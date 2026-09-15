#!/usr/bin/env python3
"""Devin Paseo Supervisor — unified ACP provider, daemon, and thin client.

Replaces the three-layer bridge (standalone-bridge, acp-proxy, acp-daemon)
with a single supervisor that supports both native ACP and print-mode fallback.

Modes:
  stdio    — Direct stdin/stdout ACP provider for Paseo (default).
  daemon   — Unix socket server with child pooling and session affinity.
  client   — Thin socket client (symlink as devin-acp-client).

Env:
  DEVIN_SUPERVISOR_MODE        native|print|auto   (default: print)
  DEVIN_SUPERVISOR_DIR         state/cache home    (default: ~/.paseo/devin-supervisor)
  DEVIN_SUPERVISOR_CONFIG      Devin CLI config    (default: ~/.config/devin/paseo-acp-config.json)
  DEVIN_SUPERVISOR_VERSIONS_DIR Devin CLI versions (default: ~/.local/share/devin/cli/_versions)
  DEVIN_SUPERVISOR_CACHE_TTL_SECONDS
  DEVIN_SUPERVISOR_SESSION_LOAD_TIMEOUT_SECONDS Long native resume timeout (default: 240)
DEVIN_SUPERVISOR_PROMPT_TIMEOUT_SECONDS   Idle timeout — no output for this long kills the turn (default: 7200)
DEVIN_SUPERVISOR_HARD_MAX_TURN_SECONDS    Absolute wall-clock ceiling; 0 = unbounded (default: 0)
DEVIN_SUPERVISOR_NUDGE_IDLE_SECONDS       Nudge threshold (default: 300)
  DEVIN_SUPERVISOR_POST_OUTPUT_STALL_GRACE_SECONDS
                                      Extra quiet grace after a nudge when output
                                      was already seen, unless a terminal command
                                      is still truly running (default: 180)
  DEVIN_SUPERVISOR_STDERR_KEEPALIVE_SECONDS
                                      If the Devin child produced stderr within
                                      this many seconds, treat as still thinking
                                      and don't stall-kill the turn (default: 300)
  DEVIN_SUPERVISOR_NETWORK_KEEPALIVE_SECONDS
                                      If the Devin child has established TCP
                                      connections within this many seconds of
                                      idle, treat as still waiting for an API
                                      response and don't stall-kill the turn.
                                      Models with large context (127k+ tokens)
                                      can take 5+ minutes between tool calls
                                      with no session/update notifications
                                      (default: 900)
  DEVIN_SUPERVISOR_NETWORK_GRACE_SECONDS
                                      After the TCP connection closes (API
                                      response received), the Devin child needs
                                      time to process the response into
                                      session/update notifications. If there
                                      was network activity within this many
                                      seconds, don't stall-kill the turn even
                                      if no TCP connections are currently
                                      established (default: 300)
  DEVIN_SUPERVISOR_CHILD_IDLE_TIMEOUT_SECONDS (default: 600)
  DEVIN_SUPERVISOR_MAX_CHILDREN             (default: 5)
"""
import collections
import contextlib
import dataclasses
import enum
import fcntl
import json
import os
import queue
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SUPERVISOR_DIR = Path(
    os.environ.get("DEVIN_SUPERVISOR_DIR", Path.home() / ".paseo" / "devin-supervisor")
)
STATE_PATH = SUPERVISOR_DIR / "state.json"
STATE_LOCK_PATH = SUPERVISOR_DIR / "state.lock"
CACHE_PATH = SUPERVISOR_DIR / "cache.json"
CACHE_LOCK_PATH = SUPERVISOR_DIR / "cache.lock"
LOG_PATH = SUPERVISOR_DIR / "supervisor.log"
EXPORT_DIR = SUPERVISOR_DIR / "exports"
HISTORY_PATH = SUPERVISOR_DIR / "history.json"  # Legacy shared file — kept for migration only
HISTORY_DIR = SUPERVISOR_DIR / "history"  # Per-session history files (no race condition)
PASEO_AGENTS_DIR = Path(os.environ.get("DEVIN_SUPERVISOR_PASEO_AGENTS_DIR", Path.home() / ".paseo" / "agents"))

DEVIN_VERSIONS_DIR = Path(
    os.environ.get("DEVIN_SUPERVISOR_VERSIONS_DIR", Path.home() / ".local" / "share" / "devin" / "cli" / "_versions")
)
DEVIN_CONFIG = os.environ.get("DEVIN_SUPERVISOR_CONFIG", str(Path.home() / ".config" / "devin" / "paseo-acp-config.json"))

SUPERVISOR_MODE = os.environ.get("DEVIN_SUPERVISOR_MODE", "native")
CACHE_TTL_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_CACHE_TTL_SECONDS", "300"))
SESSION_LOAD_TIMEOUT_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_SESSION_LOAD_TIMEOUT_SECONDS", "240"))
PROMPT_TIMEOUT_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_PROMPT_TIMEOUT_SECONDS", "7200"))
HARD_MAX_TURN_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_HARD_MAX_TURN_SECONDS", "0"))
NUDGE_IDLE_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_NUDGE_IDLE_SECONDS", "240"))
POST_OUTPUT_STALL_GRACE_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_POST_OUTPUT_STALL_GRACE_SECONDS", "180"))
PENDING_TERMINAL_STALL_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_PENDING_TERMINAL_STALL_SECONDS", "1800"))
STDERR_KEEPALIVE_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_STDERR_KEEPALIVE_SECONDS", "300"))
NETWORK_KEEPALIVE_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_NETWORK_KEEPALIVE_SECONDS", "900"))
NETWORK_GRACE_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_NETWORK_GRACE_SECONDS", "300"))
CHILD_IDLE_TIMEOUT_SECONDS = int(os.environ.get("DEVIN_SUPERVISOR_CHILD_IDLE_TIMEOUT_SECONDS", "600"))
MAX_CHILDREN = int(os.environ.get("DEVIN_SUPERVISOR_MAX_CHILDREN", "5"))
AUTO_RESUME_MAX_PER_TURN = int(os.environ.get("DEVIN_SUPERVISOR_AUTO_RESUME_MAX_PER_TURN", "10"))
PROMPT_MAX_RETRIES = int(os.environ.get("DEVIN_SUPERVISOR_PROMPT_MAX_RETRIES", str(AUTO_RESUME_MAX_PER_TURN + 1)))
PRINT_PERMISSION_MODE = "dangerous"
EXPORT_POLL_INTERVAL_SECONDS = 0.25

SOCK_PATH = Path(os.environ.get("DEVIN_SUPERVISOR_SOCK", Path.home() / ".paseo" / "devin-supervisor.sock"))
PID_PATH = Path(os.environ.get("DEVIN_SUPERVISOR_PID", Path.home() / ".paseo" / "devin-supervisor.pid"))

SESSION_LOCKS_DIR = Path(
    os.environ.get("DEVIN_SUPERVISOR_LOCKS_DIR", Path.home() / ".local" / "share" / "devin" / "cli" / "session_locks")
)

DEVIN_SESSION_DB_CANDIDATES = [
    Path.home() / ".local" / "share" / "devin-paseo-acp" / "devin" / "cli" / "sessions.db",
    Path.home() / ".local" / "share" / "devin" / "cli" / "sessions.db",
]

# Static list of known Devin models (since native ACP doesn't expose them)
DEVIN_MODELS = [
    {
        "id": "adaptive",
        "name": "Adaptive",
        "description": "Adaptive  [$0.5 / 1M Input \u00b7 $0.1 / 1M Cached input \u00b7 $2 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "claude-fable-5",
        "name": "Claude Fable 5",
        "description": "Claude Fable 5 Medium  [1M context, $10 / 1M Input \u00b7 $1 / 1M Cached input \u00b7 $50 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "claude-fable-5.1",
        "name": "Claude Fable 5.1",
        "description": "Claude Fable 5.1 Medium  [1M context, $10 / 1M Input \u00b7 $0.25 / 1M Cached input \u00b7 $50 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "claude-haiku-4.5",
        "name": "Claude Haiku 4.5",
        "description": "Claude Haiku 4.5  [200K context, $1 / 1M Input \u00b7 $0.1 / 1M Cached input \u00b7 $5 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "claude-opus-4.5",
        "name": "Claude Opus 4.5",
        "description": "Claude Opus 4.5  [200K context, $5 / 1M Input \u00b7 $0.5 / 1M Cached input \u00b7 $25 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "claude-opus-4.6",
        "name": "Claude Opus 4.6",
        "description": "Claude Opus 4.6  [200K context, $5 / 1M Input \u00b7 $0.5 / 1M Cached input \u00b7 $25 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "claude-opus-4.7",
        "name": "Claude Opus 4.7",
        "description": "Claude Opus 4.7 Medium  [1M context, $5 / 1M Input \u00b7 $0.5 / 1M Cached input \u00b7 $25 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "claude-opus-4.8",
        "name": "Claude Opus 4.8",
        "description": "Claude Opus 4.8 Medium  [1M context, $5 / 1M Input \u00b7 $0.5 / 1M Cached input \u00b7 $25 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "claude-opus-5",
        "name": "Claude Opus 5",
        "description": "Claude Opus 5 Medium  [1M context, $5 / 1M Input \u00b7 $0.5 / 1M Cached input \u00b7 $25 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "claude-sonnet-4.5",
        "name": "Claude Sonnet 4.5",
        "description": "Claude Sonnet 4.5  [200K context, $3 / 1M Input \u00b7 $0.3 / 1M Cached input \u00b7 $15 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "claude-sonnet-4.6",
        "name": "Claude Sonnet 4.6",
        "description": "Claude Sonnet 4.6  [200K context, $3 / 1M Input \u00b7 $0.3 / 1M Cached input \u00b7 $15 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "claude-sonnet-5",
        "name": "Claude Sonnet 5",
        "description": "Claude Sonnet 5 Medium  [1M context, $2 / 1M Input \u00b7 $0.2 / 1M Cached input \u00b7 $10 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "deepseek-v4-flash",
        "name": "DeepSeek V4 Flash",
        "description": "",
        "efforts": [
            "high",
            "max"
        ]
    },
    {
        "id": "deepseek-v4-pro",
        "name": "DeepSeek V4 Pro",
        "description": "",
        "efforts": [
            "high",
            "max"
        ]
    },
    {
        "id": "deepseek-v4.1-flash",
        "name": "DeepSeek V4.1 Flash",
        "description": "",
        "efforts": [
            "high",
            "max"
        ]
    },
    {
        "id": "gemini-3-flash",
        "name": "Gemini 3 Flash",
        "description": "Gemini 3 Flash Minimal  [1048576 context, $0.5 / 1M Input \u00b7 $0.05 / 1M Cached input \u00b7 $3 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "gemini-3.1-pro",
        "name": "Gemini 3.1 Pro",
        "description": "",
        "efforts": [
            "low",
            "high"
        ]
    },
    {
        "id": "gemini-3.5-flash",
        "name": "Gemini 3.5 Flash",
        "description": "Gemini 3.5 Flash Minimal  [1048576 context, $1.5 / 1M Input \u00b7 $0.15 / 1M Cached input \u00b7 $9 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high"
        ]
    },
    {
        "id": "gemini-3.6-flash",
        "name": "Gemini 3.6 Flash",
        "description": "Gemini 3.6 Flash Minimal  [1048576 context, $1.5 / 1M Input \u00b7 $0.15 / 1M Cached input \u00b7 $7.5 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high"
        ]
    },
    {
        "id": "gemini-3.7-flash",
        "name": "Gemini 3.7 Flash",
        "description": "Gemini 3.7 Flash Medium  [1048576 context, $0.75 / 1M Input \u00b7 $0.08 / 1M Cached input \u00b7 $3.75 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high"
        ]
    },
    {
        "id": "gemini-3.8-flash",
        "name": "Gemini 3.8 Flash",
        "description": "Gemini 3.8 Flash Medium  [1048576 context, $1.5 / 1M Input \u00b7 $0.15 / 1M Cached input \u00b7 $7.5 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high"
        ]
    },
    {
        "id": "glm-5.2",
        "name": "GLM-5.2",
        "description": "GLM-5.2 High  [200K context, Free — high effort only]",
        "efforts": [
            "high"
        ]
    },
    {
        "id": "glm-5.3",
        "name": "GLM-5.3",
        "description": "",
        "efforts": [
            "low",
            "high",
            "max"
        ]
    },
    {
        "id": "glm-5.3-flash",
        "name": "GLM-5.3 Flash",
        "description": "",
        "efforts": [
            "low",
            "high",
            "max"
        ]
    },
    {
        "id": "gpt-4.1",
        "name": "GPT-4.1",
        "description": "GPT-4.1  [1047576 context, $2 / 1M Input \u00b7 $0.5 / 1M Cached input \u00b7 $8 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "gpt-5.1",
        "name": "GPT-5.1",
        "description": "GPT-5.1 No Thinking  [272K context, $1.25 / 1M Input \u00b7 $0.12 / 1M Cached input \u00b7 $10 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "gpt-5.2",
        "name": "GPT-5.2",
        "description": "GPT-5.2 Low Thinking  [384K context, $1.75 / 1M Input \u00b7 $0.17 / 1M Cached input \u00b7 $14 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "gpt-5.3-codex",
        "name": "GPT-5.3-Codex",
        "description": "GPT-5.3-Codex Medium  [400K context, $1.75 / 1M Input \u00b7 $0.17 / 1M Cached input \u00b7 $14 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh"
        ]
    },
    {
        "id": "gpt-5.4",
        "name": "GPT-5.4",
        "description": "GPT-5.4 No Thinking  [272K context, $2.5 / 1M Input \u00b7 $0.25 / 1M Cached input \u00b7 $15 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh"
        ]
    },
    {
        "id": "gpt-5.4-mini",
        "name": "GPT-5.4 Mini",
        "description": "GPT-5.4 Mini Medium Thinking  [400K context, $0.75 / 1M Input \u00b7 $0.08 / 1M Cached input \u00b7 $4.5 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh"
        ]
    },
    {
        "id": "gpt-5.5",
        "name": "GPT-5.5",
        "description": "GPT-5.5 No Thinking  [272K context, $5 / 1M Input \u00b7 $0.5 / 1M Cached input \u00b7 $30 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh"
        ]
    },
    {
        "id": "gpt-5.6-luna",
        "name": "GPT-5.6 Luna",
        "description": "GPT-5.6 Luna Medium Thinking  [1M context, $0.2 / 1M Input \u00b7 $0.02 / 1M Cached input \u00b7 $1.2 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "gpt-5.6-sol",
        "name": "GPT-5.6 Sol",
        "description": "GPT-5.6 Sol Medium Thinking  [1M context, $1.2 / 1M Input \u00b7 $0.12 / 1M Cached input \u00b7 $6 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "gpt-5.6-terra",
        "name": "GPT-5.6 Terra",
        "description": "GPT-5.6 Terra No Thinking  [1M context, $2 / 1M Input \u00b7 $0.2 / 1M Cached input \u00b7 $12 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "gpt-6-astra",
        "name": "GPT-6 Astra",
        "description": "GPT-6 Astra Medium Thinking  [1M context, $10 / 1M Input \u00b7 $1 / 1M Cached input \u00b7 $50 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "grok-4.5",
        "name": "Grok 4.5",
        "description": "Grok 4.5 Medium  [500K context, $2 / 1M Input \u00b7 $0.3 / 1M Cached input \u00b7 $6 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high"
        ]
    },
    {
        "id": "grok-4.6",
        "name": "Grok 4.6",
        "description": "Grok 4.6 Medium  [500K context, $2 / 1M Input \u00b7 $0.3 / 1M Cached input \u00b7 $6 / 1M Output, beta]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh"
        ]
    },
    {
        "id": "inkling",
        "name": "Inkling",
        "description": "Inkling None  [1048576 context, $1.4 / 1M Input \u00b7 $0.26 / 1M Cached input \u00b7 $4.4 / 1M Output]",
        "efforts": [
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
        ]
    },
    {
        "id": "kimi-k2.6",
        "name": "Kimi K2.6",
        "description": "Kimi K2.6  [262144 context, $0.95 / 1M Input \u00b7 $0.16 / 1M Cached input \u00b7 $4 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "kimi-k2.7",
        "name": "Kimi K2.7",
        "description": "Kimi K2.7  [262144 context, $0.95 / 1M Input \u00b7 $0.19 / 1M Cached input \u00b7 $4 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "kimi-k3",
        "name": "Kimi K3",
        "description": "",
        "efforts": [
            "low",
            "high",
            "max"
        ]
    },
    {
        "id": "nemotron-3-ultra",
        "name": "Nemotron 3 Ultra",
        "description": "Nemotron 3 Ultra None  [1M context, $0.6 / 1M Input \u00b7 $0.12 / 1M Cached input \u00b7 $2.4 / 1M Output]",
        "efforts": [
            "medium",
            "high"
        ]
    },
    {
        "id": "swe-1.6",
        "name": "SWE-1.6",
        "description": "SWE-1.6  [200K context, $0.5 / 1M Input \u00b7 $0.2 / 1M Cached input \u00b7 $2.5 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "swe-1.6-fast",
        "name": "SWE-1.6 Fast",
        "description": "SWE-1.6 Fast  [200K context, $0.5 / 1M Input \u00b7 $0.2 / 1M Cached input \u00b7 $2.5 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "swe-1.7",
        "name": "SWE-1.7",
        "description": "SWE-1.7 Max  [262K context, Free]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "swe-1.7-lightning",
        "name": "SWE-1.7 Lightning",
        "description": "SWE-1.7 Lightning Max  [202752 context, $2.5 / 1M Input \u00b7 $1 / 1M Cached input \u00b7 $12.5 / 1M Output]",
        "efforts": [
            "medium"
        ]
    },
    {
        "id": "swe-2",
        "name": "SWE-2",
        "description": "SWE-2 Max  [262K context, Free]",
        "default_effort": "max",
        "efforts": [
            "medium",
            "high",
            "max"
        ]
    }
]

DEVIN_MODES = [
    {"id": "accept-edits", "name": "Accept Edits", "description": "Auto-accept edit suggestions"},
    {"id": "bypass", "name": "Bypass", "description": "Bypass edit suggestions"},
    {"id": "print", "name": "Print", "description": "Print mode"},
    {"id": "auto", "name": "Auto", "description": "Auto mode"},
]


def _format_duration(seconds: float) -> str:
    s = max(0, int(round(seconds)))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m {s % 60}s"


def _process_cmdline(pid: int) -> str:
    """Best-effort command line for a PID (macOS/Linux). Empty on failure."""
    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True, text=True, timeout=3,
        )
        return out.stdout.strip()
    except Exception:
        return ""


def _process_ppid(pid: int) -> Optional[int]:
    """Best-effort PPID for a PID. None on failure."""
    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "ppid="],
            capture_output=True, text=True, timeout=3,
        )
        return int(out.stdout.strip())
    except Exception:
        return None


def _is_supervisor_cmdline(cmdline: str) -> bool:
    """Return true for every known launch shape of this supervisor.

    Paseo launches the script through the devin-supervisor-stdio symlink, so
    checking only for the source filename misses live sibling supervisors.
    """
    return (
        "devin-paseo-supervisor.py" in cmdline
        or "devin-supervisor-stdio" in cmdline
    )


def _is_paseo_devin_acp_cmdline(cmdline: str) -> bool:
    """Return true only for the Devin ACP child launched by this supervisor."""
    return (
        "devin" in cmdline
        and "acp" in cmdline
        and DEVIN_CONFIG in cmdline
        and str(DEVIN_VERSIONS_DIR) in cmdline
    )


def _is_supervisor_pid(pid: Optional[int]) -> bool:
    """Best-effort check that a live PID is one of our supervisor processes."""
    if not pid:
        return False
    return _is_supervisor_cmdline(_process_cmdline(pid))


def _terminate_lock_holder(real_id: str, lock_path: Path, pid: int, reason: str) -> bool:
    """Terminate a Devin ACP lock holder and remove its lock if it still owns it."""
    _log(f"LOCK_CLEANUP {reason} {real_id} pid={pid}")
    try:
        os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    for _ in range(20):
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            break
        time.sleep(0.1)
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    try:
        current_pid = lock_path.read_text().strip() if lock_path.exists() else ""
    except Exception:
        current_pid = ""
    if current_pid in ("", str(pid)):
        lock_path.unlink(missing_ok=True)
        _log(f"LOCK_CLEANUP removed lock {real_id} pid={pid} reason={reason}")
        return True
    _log(f"LOCK_CLEANUP lock changed {real_id} old_pid={pid} current_pid={current_pid}; not removing")
    return False


def stale_lock_cleanup(real_id: str, reclaim_orphan: bool = False, reclaim_sibling: bool = False) -> bool:
    """Remove a stale Devin session lock file.

    A lock is stale and removed when:
      - the owning process is dead, OR
      - reclaim_orphan=True AND the owning process is alive but not owned by
        this supervisor or a sibling supervisor, OR
      - reclaim_sibling=True AND the owning process is a Devin ACP child of a
        sibling supervisor. This is used only after native Devin has already
        returned "already open in another process" for this exact real session,
        meaning the new supervisor cannot safely resume until the old owner is
        retired.

    Returns True if a stale lock was removed (or no lock existed)."""
    if not real_id:
        return True
    lock_path = SESSION_LOCKS_DIR / f"{real_id}.lock"
    if not lock_path.exists():
        return True
    try:
        pid_str = lock_path.read_text().strip()
        pid = int(pid_str)
        # Check if process is alive
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            # Process is dead — remove stale lock
            lock_path.unlink(missing_ok=True)
            _log(f"LOCK_CLEANUP removed stale lock {real_id} pid={pid}")
            return True

        # Process is alive.
        if not reclaim_orphan:
            _log(f"LOCK_CLEANUP skip {real_id} pid={pid} is alive")
            return False

        # Reclaim orphan: only kill if it is a devin acp process that is NOT
        # a child of this supervisor. A child of THIS process is a legitimate
        # live lock we still own.
        ppid = _process_ppid(pid)
        my_pid = os.getpid()
        if ppid == my_pid:
            _log(f"LOCK_CLEANUP skip {real_id} pid={pid} owned by this supervisor (ppid={ppid})")
            return False
        cmdline = _process_cmdline(pid)
        is_devin_acp = _is_paseo_devin_acp_cmdline(cmdline)
        if _is_supervisor_pid(ppid):
            if not reclaim_sibling:
                _log(f"LOCK_CLEANUP skip {real_id} pid={pid} owned by sibling supervisor (ppid={ppid})")
                return False
            if not is_devin_acp:
                _log(f"LOCK_CLEANUP skip sibling {real_id} pid={pid} not devin acp (cmd={cmdline!r})")
                return False
            return _terminate_lock_holder(real_id, lock_path, pid, f"reclaim sibling ppid={ppid}")
        if not is_devin_acp:
            _log(f"LOCK_CLEANUP skip {real_id} pid={pid} alive but not a devin acp process (cmd={cmdline!r}); removing lock only")
            lock_path.unlink(missing_ok=True)
            return True
        # Orphaned devin acp child from a previous supervisor — reclaim it.
        return _terminate_lock_holder(
            real_id,
            lock_path,
            pid,
            f"reclaim orphan ppid={ppid} not_child_of={my_pid}",
        )
    except Exception as exc:
        _log(f"LOCK_CLEANUP error for {real_id}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass


ensure_private_dir(SUPERVISOR_DIR)
ensure_private_dir(EXPORT_DIR)
ensure_private_dir(HISTORY_DIR)


_LOG_FH = None
_LOG_CHMODDED = False
_LOG_QUEUE: Optional[queue.Queue] = None
_LOG_THREAD_STARTED = False
_LOG_MAX_SIZE = 50 * 1024 * 1024  # 50MB cap


def _log_worker(q: queue.Queue) -> None:
    """Background thread that writes log lines. Prevents I/O from blocking
    the reader thread (which would cause stdout pipe deadlock in children)."""
    global _LOG_FH, _LOG_CHMODDED
    while True:
        try:
            line = q.get()
            if line is None:
                break
            try:
                if _LOG_FH is None or _LOG_FH.closed:
                    _LOG_FH = open(LOG_PATH, "a", encoding="utf-8")
                    if not _LOG_CHMODDED:
                        os.chmod(LOG_PATH, 0o600)
                        _LOG_CHMODDED = True
                try:
                    if _LOG_FH.tell() > _LOG_MAX_SIZE:
                        _LOG_FH.close()
                        _LOG_FH = open(LOG_PATH, "w", encoding="utf-8")
                except Exception:
                    pass
                _LOG_FH.write(line)
                _LOG_FH.flush()
            except Exception:
                try:
                    if _LOG_FH and not _LOG_FH.closed:
                        _LOG_FH.close()
                except Exception:
                    pass
                _LOG_FH = None
        except Exception:
            pass


def _log(message: str) -> None:
    global _LOG_QUEUE, _LOG_THREAD_STARTED
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {message}\n"
    if not _LOG_THREAD_STARTED:
        _LOG_QUEUE = queue.Queue(maxsize=10000)
        t = threading.Thread(target=_log_worker, args=(_LOG_QUEUE,), daemon=True)
        t.start()
        _LOG_THREAD_STARTED = True
    try:
        _LOG_QUEUE.put_nowait(line)
    except queue.Full:
        pass


def _compact_for_log(obj: Any) -> Any:
    if isinstance(obj, dict):
        compact: Dict[str, Any] = {}
        for k, v in obj.items():
            if k in ("configOptions", "availableModes", "options") and isinstance(v, list):
                compact[k] = f"<{len(v)} items>"
            else:
                compact[k] = _compact_for_log(v)
        return compact
    if isinstance(obj, list):
        return [_compact_for_log(item) for item in obj[:8]] + ([f"<{len(obj) - 8} more>"] if len(obj) > 8 else [])
    if isinstance(obj, str) and len(obj) > 800:
        return obj[:800] + "...<truncated>"
    return obj


def _load_json(path: Path, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as exc:
        _log(f"Failed to read {path}: {exc}")
        return default


# Devin 2026.8.18+ reads MCP servers from the --config file (confirmed by
# _meta.mcpConfigPath in the initialize response), NOT from the session/new
# mcpServers param. The supervisor passes --config DEVIN_CONFIG which has
# codebase_memory, agentmemory, fff, etc. So passing mcpServers: [] in
# session/new is harmless — MCPs are loaded from the config file.
# Verified 2026-06-25: codebase_memory list_projects works end-to-end through
# the Paseo→Devin bridge with this setup.
CONFIGURED_MCPSERVERS = []
_log(f"CONFIGURED_MCPSERVERS=[] (Devin 2026.8.18+ loads MCPs from --config file, not session/new param)")

DEFAULT_DEVIN_MODE = "bypass"


def _save_json(path: Path, data: Any) -> None:
    ensure_private_dir(path.parent)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
        os.chmod(path, 0o600)
    except Exception as exc:
        _log(f"Failed to write {path}: {exc}")


def _with_flock(lock_path: Path, mutator):
    ensure_private_dir(lock_path.parent)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            return mutator()
        finally:
            try:
                os.chmod(lock_path, 0o600)
            except OSError:
                pass


@contextlib.contextmanager
def _mcp_config_lock(cwd: Optional[str]):
    """Hold an exclusive flock on .devin/mcp_config.local.json for the
    write+session_init sequence.

    .devin/mcp_config.local.json is shared across all agents in the same
    cwd — each agent's paseo MCP entry carries its own callerAgentId, so
    a sibling agent's write between our write and our session_init would
    give the native child the wrong identity. Serializing the sequence
    per-cwd eliminates that race; agents in different cwds use different
    lock files and are unaffected."""
    if not cwd:
        yield
        return
    lock_path = Path(cwd) / ".devin" / "mcp_config.local.json.lock"
    ensure_private_dir(lock_path.parent)
    lock_file = open(lock_path, "a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        lock_file.close()
        try:
            os.chmod(lock_path, 0o600)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Devin binary resolution
# ---------------------------------------------------------------------------

def _version_key(path_obj: Path) -> Tuple:
    return tuple(int(part) if part.isdigit() else part for part in re.split(r"(\d+)", path_obj.parent.parent.name))


def _resolve_real_devin() -> str:
    override = os.environ.get("DEVIN_SUPERVISOR_REAL")
    if override:
        return override
    for name in ("devin-real", "devin"):
        found = shutil.which(name)
        if found:
            return found
    # Devin Desktop remote installs land under ~/.devin-server/bin/<hash>/...
    server_bins = sorted(
        Path.home().glob(".devin-server/bin/*/extensions/windsurf/devin/bin/devin"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if server_bins:
        return str(server_bins[0])
    for candidate in (Path.home() / ".local" / "bin" / "devin-real", Path.home() / ".local" / "bin" / "devin"):
        if candidate.exists():
            return str(candidate)
    current = DEVIN_VERSIONS_DIR / "current" / "bin" / "devin"
    if current.exists():
        return str(current)
    candidates = []
    if DEVIN_VERSIONS_DIR.exists():
        for child in DEVIN_VERSIONS_DIR.iterdir():
            if not child.is_dir() or child.name.startswith("_"):
                continue
            candidate = child / "bin" / "devin"
            if candidate.exists():
                candidates.append(candidate)
    if candidates:
        return str(sorted(candidates, key=_version_key)[-1])
    raise FileNotFoundError("No Devin CLI binary found (PATH, ~/.devin-server, ~/.local/bin, versions dir)")


REAL_DEVIN = _resolve_real_devin()


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state() -> dict:
    return _load_json(STATE_PATH, {"sessions": {}, "history": {}, "cache": {}})


def save_state(state: dict) -> None:
    _save_json(STATE_PATH, state)


def with_state(mutator):
    def locked():
        state = load_state()
        result = mutator(state)
        save_state(state)
        return result
    return _with_flock(STATE_LOCK_PATH, locked)


def read_state() -> dict:
    def identity(state):
        return json.loads(json.dumps(state))
    return with_state(identity)


def _write_devin_mcp_config(cwd: Optional[str], mcp_servers: Optional[list]) -> None:
    """Translate ACP session/new mcpServers into Devin's per-project MCP
    config (.devin/mcp_config.local.json). Devin ignores the ACP mcpServers
    param entirely — MCP servers must come from config files. Paseo injects
    its agent MCP as an HTTP entry carrying this agent's callerAgentId, so
    each workspace gets the correctly-scoped server entry."""
    if not cwd or not mcp_servers:
        return
    translated = {}
    for server in mcp_servers:
        if not isinstance(server, dict):
            continue
        name = server.get("name")
        if not name:
            continue
        headers = {}
        for h in server.get("headers") or []:
            if isinstance(h, dict) and h.get("name"):
                headers[h["name"]] = h.get("value", "")
        env = {}
        for e in server.get("env") or []:
            if isinstance(e, dict) and e.get("name"):
                env[e["name"]] = e.get("value", "")
        stype = server.get("type")
        if stype in ("http", "sse") and (server.get("url") or server.get("serverURL")):
            entry = {"url": server.get("url") or server.get("serverURL"),
                     "transport": stype}
            if headers:
                entry["headers"] = headers
            translated[name] = entry
        elif server.get("command"):
            entry = {"command": server["command"]}
            if server.get("args"):
                entry["args"] = server["args"]
            if env:
                entry["env"] = env
            translated[name] = entry
    if not translated:
        return
    cfg_path = Path(cwd) / ".devin" / "mcp_config.local.json"
    try:
        existing = _load_json(cfg_path, {})
        servers = existing.get("mcpServers") if isinstance(existing.get("mcpServers"), dict) else {}
        if all(servers.get(k) == v for k, v in translated.items()):
            return
        servers.update(translated)
        existing["mcpServers"] = servers
        _save_json(cfg_path, existing)
        _log(f"MCP_CONFIG_WROTE path={cfg_path} servers={list(translated)}")
    except Exception as exc:
        _log(f"MCP_CONFIG_WRITE_FAILED path={cfg_path} exc={exc}")


def _extract_caller_agent_id(params: Optional[dict]) -> Optional[str]:
    """Extract callerAgentId from Paseo's injected MCP URL, when present."""
    if not isinstance(params, dict):
        return None
    for server in params.get("mcpServers") or []:
        if not isinstance(server, dict):
            continue
        url = server.get("url") or server.get("serverURL") or ""
        match = re.search(r"(?:[?&])callerAgentId=([0-9a-fA-F-]{36})", url)
        if match:
            return match.group(1)
    return None


def _load_agent_file_by_id(agent_id: str) -> Optional[dict]:
    if not agent_id:
        return None
    try:
        matches = list(PASEO_AGENTS_DIR.glob(f"*/*{agent_id}.json"))
        if not matches:
            matches = list(PASEO_AGENTS_DIR.rglob(f"{agent_id}.json"))
        if not matches:
            return None
        with open(matches[0], "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception as exc:
        _log(f"AGENT_CONFIG_READ_FAILED agent={agent_id} exc={exc}")
        return None


def _load_agent_file_by_session_id(external_session_id: str) -> Optional[dict]:
    """Find a Paseo agent file that currently points at this ACP session."""
    if not external_session_id:
        return None
    try:
        for path in PASEO_AGENTS_DIR.rglob("*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            runtime = data.get("runtimeInfo") or {}
            persistence = data.get("persistence") or {}
            if runtime.get("sessionId") == external_session_id or persistence.get("sessionId") == external_session_id:
                return data
        return None
    except Exception as exc:
        _log(f"AGENT_CONFIG_SCAN_FAILED external={external_session_id} exc={exc}")
        return None


def _agent_requested_model(external_session_id: str, params: Optional[dict] = None) -> Optional[str]:
    """Read current Paseo model selection from the agent file.

    Paseo's ACP session/load and session/prompt requests may omit `model` after
    the user changes it in the UI. The agent JSON is the durable source of
    truth, so use it to reconcile supervisor state before native load/prompt.
    """
    if isinstance(params, dict) and params.get("model"):
        return params.get("model")
    agent = None
    caller_agent_id = _extract_caller_agent_id(params)
    if caller_agent_id:
        agent = _load_agent_file_by_id(caller_agent_id)
    if agent is None:
        agent = _load_agent_file_by_session_id(external_session_id)
    if not agent:
        return None
    for container_name in ("config", "runtimeInfo"):
        container = agent.get(container_name) or {}
        model = container.get("model")
        if isinstance(model, str) and model:
            return model
    return None


def _agent_requested_mode(params: Optional[dict] = None) -> Optional[str]:
    if not isinstance(params, dict):
        return None
    return params.get("modeId") or params.get("mode")


# ---------------------------------------------------------------------------
# Cache (models / modes / native templates)
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    return _load_json(CACHE_PATH, {})


def save_cache(cache: dict) -> None:
    _save_json(CACHE_PATH, cache)


def with_cache(mutator):
    def locked():
        cache = load_cache()
        result = mutator(cache)
        save_cache(cache)
        return result
    return _with_flock(CACHE_LOCK_PATH, locked)


def cache_valid() -> bool:
    cache = load_cache()
    ts = cache.get("cached_at", 0)
    return ts > 0 and (time.time() - ts) < CACHE_TTL_SECONDS


def get_cached_models() -> List[dict]:
    cached = load_cache().get("models", [])
    merged: List[dict] = []
    seen = set()
    # Live-fetched entries (with real variant_map/ids) take precedence over
    # the static fallback table.
    for model in list(cached) + list(DEVIN_MODELS):
        model_id = model.get("id") or model.get("modelId") or model.get("model")
        if not isinstance(model_id, str) or not model_id or model_id in seen:
            continue
        seen.add(model_id)
        merged.append(model)
    return _apply_model_effort_restrictions(merged)


# Per-model effort restrictions. Only the free "high" tier is allowed for
# GLM-5.2 — every other variant (max, none, 1m, …) is metered. Strip the
# paid variants from efforts/variant_map/ids/descs so both the Paseo UI and
# _resolve_model can never select a paid GLM-5.2 tier.
_MODEL_EFFORT_RESTRICTIONS = {
    "glm-5.2": {"high": "glm-5-2"},
}


def _apply_model_effort_restrictions(models: List[dict]) -> List[dict]:
    result = []
    for m in models:
        model_id = m.get("id") or ""
        restriction = _MODEL_EFFORT_RESTRICTIONS.get(model_id)
        if not restriction:
            result.append(m)
            continue
        m = dict(m)
        allowed_ids = set(restriction.values())
        m["efforts"] = list(restriction.keys())
        vm = m.get("variant_map") or {}
        m["variant_map"] = {k: v for k, v in vm.items() if k in restriction}
        m["ids"] = [i for i in (m.get("ids") or []) if i in allowed_ids or i == model_id]
        m["descs"] = {k: v for k, v in (m.get("descs") or {}).items() if k in allowed_ids or k == model_id}
        result.append(m)
    return result



_EFFORT_SUFFIX_RE = re.compile(r'-(none|minimal|low|medium|high|xhigh|max|thinking)$')

DEFAULT_EFFORT = "medium"
EFFORTS = ("none", "minimal", "low", "medium", "high", "xhigh", "max")

# Per-family default effort overrides. Keys are family ids (dash-normalized).
# When a family doesn't declare its own default_effort, this table is
# consulted before falling back to DEFAULT_EFFORT.
_FAMILY_DEFAULT_EFFORT: dict = {
    "swe-2": "max",
}


def _default_effort_for_family(fam: Optional[dict]) -> str:
    """Per-family default effort (e.g. swe-2 -> max). Falls back to
    DEFAULT_EFFORT when the family doesn't declare one."""
    if fam:
        de = fam.get("default_effort")
        if de and de in EFFORTS:
            return de
        fid = fam.get("id") or ""
        if fid in _FAMILY_DEFAULT_EFFORT:
            return _FAMILY_DEFAULT_EFFORT[fid]
    return DEFAULT_EFFORT

_FUSION_ID_RE = re.compile(
    r'^fusion-(?P<lead>.+?)-(?P<eff>none|minimal|low|medium|high|xhigh|max)(?P<fast>-fast)?-sidekick-(?P<sk>.+)$'
)
_SIDEKICK_PREF = [
    "swe-2-medium", "swe-2-high", "swe-1-7-medium",
    "gpt-5-6-luna-high", "gpt-5-6-sol-high", "glm-5-2",
]


def _model_family(model_id: str) -> str:
    return _EFFORT_SUFFIX_RE.sub('', model_id or '')


def _family_entry(base: str) -> Optional[dict]:
    """Find the cached family whose id is `base` (dot/dash normalized)."""
    for m in get_cached_models():
        mid = m.get("id")
        if mid == base or (mid and mid.replace(".", "-") == base):
            return m
    return None


def _family_for_model_id(model_id: str) -> Optional[dict]:
    """Find the family that owns a concrete (or family) model id."""
    if not model_id:
        return None
    for m in get_cached_models():
        if model_id == m.get("id"):
            return m
        if model_id in (m.get("variant_map") or {}).values():
            return m
        for sks in (m.get("pairs") or {}).values():
            if model_id in sks.values():
                return m
    for m in get_cached_models():
        mid = m.get("id")
        if mid and model_id.startswith(mid.replace(".", "-") + "-"):
            return m
    return None


def _default_sidekick(avail: List[str]) -> Optional[str]:
    for pref in _SIDEKICK_PREF:
        if pref in avail:
            return pref
    return sorted(avail)[0] if avail else None


def _resolve_model(base: str, effort: Optional[str], sidekick: Optional[str] = None) -> str:
    """Map (family, effort[, sidekick]) to a real Devin model id.

    Concrete ids are looked up in the family's real variant table (parsed
    from `devin models list`) — never guessed by string concat, since some
    families use private ids (MODEL_*), reversed names (claude-5-fable-*),
    or fusion lead/sidekick combos.
    """
    if not base:
        return base
    fam = _family_entry(base) or _family_for_model_id(base)
    if fam and fam.get("fusion"):
        pairs = fam.get("pairs") or {}
        _def = _default_effort_for_family(fam)
        eff = effort if effort in pairs else (_def if _def in pairs else next(iter(pairs), None))
        sks = pairs.get(eff) or {}
        sk = sidekick if sidekick in sks else _default_sidekick(list(sks))
        return sks.get(sk) or next(iter(sks.values()), base)
    vm = (fam or {}).get("variant_map") or {}
    if vm:
        # already-concrete id with no new effort requested -> keep it
        if base in vm.values() and (not effort or effort == _label_for_variant(fam, base)):
            return base
        _def = _default_effort_for_family(fam)
        eff = effort if effort in vm else (_def if _def in vm else next(iter(vm)))
        return vm.get(eff) or base
    efforts = (fam or {}).get("efforts") or []
    _def = _default_effort_for_family(fam)
    e = effort if effort in efforts else (_def if _def in efforts else (efforts[0] if efforts else None))
    if e and e in EFFORTS:
        return f"{base.replace('.', '-')}-{e}"
    return base


def _loop_family(session: dict) -> str:
    """'fusion' when the session's resolved native model is a fusion pair."""
    model = session.get("model") or ""
    if model.startswith("fusion/") or model.startswith("fusion-"):
        return "fusion"
    fam = _family_for_model_id(model)
    return "fusion" if (fam and fam.get("fusion")) else "regular"


def _label_for_variant(fam: dict, model_id: str) -> Optional[str]:
    for label, mid in (fam.get("variant_map") or {}).items():
        if mid == model_id:
            return label
    if fam.get("fusion"):
        m = _FUSION_ID_RE.match(model_id or '')
        if m:
            return m.group("eff")
    return None


def _split_model_id(model_id: str) -> Tuple[str, Optional[str]]:
    fam = _family_for_model_id(model_id)
    if fam:
        return fam["id"], _label_for_variant(fam, model_id)
    m = _EFFORT_SUFFIX_RE.search(model_id or '')
    if m:
        return model_id[:m.start()], m.group(1)
    return model_id or '', None


def _effort_label(mid: str, prefix: str, dispname: str) -> str:
    """Derive the effort label for a concrete model id."""
    if prefix and mid.startswith(prefix + "-"):
        return mid[len(prefix) + 1:]
    if mid == prefix or not prefix:
        hay = re.sub(r'[^A-Z]', '', (dispname or '').upper())
    else:
        hay = re.sub(r'[^A-Z]', '', (mid + ' ' + (dispname or '')).upper())
    for kw, label in (
        ("NOTHINKING", "none"), ("NONE", "none"), ("MINIMAL", "minimal"),
        ("XHIGH", "xhigh"), ("MEDIUM", "medium"), ("LOW", "low"),
        ("HIGH", "high"), ("MAX", "max"), ("THINKING", "thinking"),
    ):
        if kw in hay:
            return label
    return "standard"


def _effort_sort_key(label: str) -> int:
    try:
        return EFFORTS.index(label)
    except ValueError:
        return len(EFFORTS)


def _parse_models_list(text: str) -> List[dict]:
    """Parse `devin models list` output into family entries with variant maps."""
    families: List[dict] = []
    fam = None
    for raw in text.splitlines():
        if not raw.strip():
            continue
        if not raw.startswith(" "):
            m = re.match(r"^(.*?)\s*\(([^()]+)\)\s*$", raw.strip())
            fam = None
            if m and re.match(r'^[a-z0-9._-]+$', m.group(2)):
                fam = {"id": m.group(2), "name": m.group(1).strip(),
                       "description": "", "ids": [], "names": {}}
                families.append(fam)
            continue
        if fam is None:
            continue
        parts = raw.split()
        if not parts or parts[0] == "aliases:":
            continue
        mid, name = parts[0], " ".join(parts[1:]).split("[")[0].strip()
        fam["ids"].append(mid)
        fam["names"][mid] = name
        fam.setdefault("descs", {})[mid] = (
            raw.split("[", 1)[1].rstrip("]").strip() if "[" in raw else ""
        )
        if not fam["description"] and "[" in raw:
            fam["description"] = name + "  [" + raw.split("[", 1)[1].strip()
    out = []
    for fam in families:
        if not fam["ids"]:
            continue
        prefix = fam["id"].replace(".", "-")
        efforts: List[str] = []
        vmap: dict = {}
        for mid in fam["ids"]:
            label = _effort_label(mid, prefix, fam["names"].get(mid, ""))
            vmap.setdefault(label, mid)
            if not re.search(r'-(fast|priority)$', label) and label not in efforts:
                efforts.append(label)
        efforts.sort(key=_effort_sort_key)
        out.append({
            "id": fam["id"], "name": fam["name"], "description": fam["description"],
            "efforts": efforts, "variant_map": vmap, "ids": fam["ids"],
            "descs": fam.get("descs") or {},
        })
    return out


def _expand_fusion(entries: List[dict]) -> List[dict]:
    """Split the flat `fusion` family into `fusion/<lead>` pseudo-families
    with per-lead effort levels and sidekick lists."""
    fusion = next((e for e in entries if e.get("id") == "fusion"), None)
    if not fusion:
        return entries
    groups: dict = {}
    for mid in fusion.get("ids") or []:
        m = _FUSION_ID_RE.match(mid)
        if not m:
            continue
        lead, eff, fast, sk = m.group("lead"), m.group("eff"), m.group("fast"), m.group("sk")
        bucket = groups.setdefault(lead, {}).setdefault(eff, {})
        if sk not in bucket or not fast:
            bucket[sk] = mid if not fast else bucket.get(sk) or mid
    descs = fusion.get("descs") or {}
    entries = [e for e in entries if e.get("id") != "fusion"]
    for lead, effs in groups.items():
        efforts = sorted(effs, key=_effort_sort_key)
        sks = sorted({sk for m in effs.values() for sk in m})
        # Per-combo descriptions carry a "Sidekick input/output" price segment
        # only when the sidekick is billed — absence means the sidekick is free.
        sk_labels: dict = {}
        for sk in sks:
            label, detail = "Free", ""
            for mid in (m[sk] for m in effs.values() if sk in m):
                desc = descs.get(mid, "")
                m_in = re.search(r'\$([0-9.]+) / 1M Sidekick input', desc)
                m_out = re.search(r'\$([0-9.]+) / 1M Sidekick output', desc)
                if m_in:
                    label = f"+${m_in.group(1)}/1M"
                    parts = [f"Sidekick input ${m_in.group(1)}/1M"]
                    m_cached = re.search(r'\$([0-9.]+) / 1M Sidekick cached input', desc)
                    if m_cached:
                        parts.append(f"cached ${m_cached.group(1)}/1M")
                    if m_out:
                        parts.append(f"output ${m_out.group(1)}/1M")
                    detail = " · ".join(parts)
                    break
            sk_labels[sk] = {"name": f"{sk} · {label}", "description": detail}
        entries.append({
            "id": f"fusion/{lead}", "name": f"Fusion: {lead}",
            "description": f"Fusion lead {lead} + sidekick",
            "efforts": efforts, "sidekicks": sks, "pairs": effs,
            "sidekick_labels": sk_labels,
            "fusion": True, "ids": [i for m in effs.values() for i in m.values()],
        })
    return entries


def _fetch_live_models() -> Optional[List[dict]]:
    """Build the model table from `devin models list`; None on failure."""
    try:
        out = subprocess.run([REAL_DEVIN, "models", "list"], capture_output=True, text=True, timeout=25)
        if out.returncode != 0:
            _log(f"MODELS_FETCH rc={out.returncode} err={out.stderr.strip()[:200]}")
            return None
        entries = _expand_fusion(_parse_models_list(out.stdout))
        if not entries:
            return None
        _log(f"MODELS_FETCH ok families={len(entries)}")
        return entries
    except Exception as exc:
        _log(f"MODELS_FETCH failed exc={exc}")
        return None


def get_model_families() -> List[dict]:
    return get_cached_models()


def _acp_model_state(current_model: Optional[str] = None) -> dict:
    """Return an ACP-conformant SessionModelState (modelId/name/description)."""
    models = []
    for m in get_cached_models():
        mid = m.get("id") or m.get("modelId") or m.get("model")
        if not isinstance(mid, str) or not mid:
            continue
        entry = {
            "modelId": mid,
            "name": m.get("name") or mid,
            "description": m.get("description") or "",
        }
        if m.get("efforts"):
            entry["_meta"] = {"devin/efforts": m["efforts"]}
        models.append(entry)
    return {
        "availableModels": models,
        "currentModelId": current_model or (models[0]["modelId"] if models else ""),
    }


def _select_option_from_efforts(model_id: str, current_effort: Optional[str]) -> dict:
    fam = _family_for_model_id(model_id) or _family_entry(_model_family(model_id))
    efforts = (fam or {}).get("efforts") or list(EFFORTS)
    options = [{"value": e, "name": e.title(), "description": ""} for e in efforts]
    cur = current_effort or (_default_effort_for_family(fam) if _default_effort_for_family(fam) in efforts else efforts[0])
    return {
        "id": "effort",
        "name": "Effort",
        "category": "thought_level",
        "type": "select",
        "currentValue": cur,
        "options": options,
    }


def _select_option_sidekick(fam: dict, current: Optional[str]) -> dict:
    sks = (fam or {}).get("sidekicks") or []
    labels = (fam or {}).get("sidekick_labels") or {}
    cur = current if current in sks else _default_sidekick(sks)
    return {
        "id": "sidekick",
        "name": "Sidekick",
        "category": "sidekick",
        "type": "select",
        "currentValue": cur or "",
        "options": [
            {
                "value": s,
                "name": (labels.get(s) or {}).get("name", s),
                "description": (labels.get(s) or {}).get("description", ""),
            }
            for s in sks
        ],
    }

def get_cached_modes() -> List[dict]:
    return load_cache().get("modes", [])


def _select_option_from_models(models: List[dict], current_model: str) -> dict:
    options = []
    seen = set()
    for model in models:
        model_id = model.get("id") or model.get("modelId") or model.get("model")
        if not isinstance(model_id, str) or not model_id or model_id in seen:
            continue
        seen.add(model_id)
        options.append({
            "value": model_id,
            "name": model.get("name") or model_id,
            "description": model.get("description") or "",
        })
    if current_model and current_model not in seen:
        options.insert(0, {"value": current_model, "name": current_model, "description": ""})
    return {
        "id": "model",
        "name": "Model",
        "category": "model",
        "type": "select",
        "currentValue": current_model,
        "options": options,
    }


def _select_option_from_modes(modes: List[dict], current_mode: str) -> dict:
    options = []
    seen = set()
    for mode in modes:
        mode_id = mode.get("id") or mode.get("modeId")
        if not isinstance(mode_id, str) or not mode_id or mode_id in seen:
            continue
        seen.add(mode_id)
        options.append({
            "value": mode_id,
            "name": mode.get("name") or mode.get("label") or mode_id,
            "description": mode.get("description") or "",
        })
    if current_mode and current_mode not in seen:
        options.insert(0, {"value": current_mode, "name": current_mode, "description": ""})
    return {
        "id": "mode",
        "name": "Mode",
        "category": "mode",
        "type": "select",
        "currentValue": current_mode,
        "options": options,
    }


def _feature_select_options(feature_values: Optional[dict] = None) -> List[dict]:
    """Synthetic feature toggles so Paseo's composer reflects stored state."""
    fv = feature_values or {}
    return [
        {
            "id": "allow_all",
            "name": "Allow All",
            "category": "_feature",
            "type": "select",
            "currentValue": fv.get("allow_all", "on"),
            "options": [
                {"value": "on", "name": "On", "description": "Automatically approve all requests"},
                {"value": "off", "name": "Off", "description": "Ask before running tools"},
            ],
        },
    ]


def _structured_config_options(model: str, mode: str, effort: Optional[str] = None, feature_values: Optional[dict] = None, sidekick: Optional[str] = None) -> List[dict]:
    base, cur_effort = _split_model_id(model)
    if effort:
        cur_effort = effort
    options = [
        _select_option_from_modes(get_cached_modes(), mode),
        _select_option_from_models(get_cached_models(), base),
        _select_option_from_efforts(base, cur_effort),
    ]
    fam = _family_entry(base) or _family_for_model_id(model)
    if fam and fam.get("fusion"):
        options.append(_select_option_sidekick(fam, sidekick))
    options.extend(_feature_select_options(feature_values))
    return options


def _merge_structured_config_options(config_options: Optional[List[dict]], model: str, mode: str, effort: Optional[str] = None, feature_values: Optional[dict] = None, sidekick: Optional[str] = None) -> List[dict]:
    result = [
        option for option in (config_options or [])
        if not (
            isinstance(option, dict)
            and option.get("id") in ("mode", "model", "effort", "sidekick")
            and option.get("category") in (None, "mode", "model", "thought_level", "sidekick")
        )
    ]
    result.extend(_structured_config_options(model, mode, effort, feature_values, sidekick))
    return result


# ---------------------------------------------------------------------------
# History (per-session files — no concurrent write race condition)
# ---------------------------------------------------------------------------

def _session_history_path(session_id: str) -> Path:
    return HISTORY_DIR / f"{session_id}.json"


def migrate_legacy_history() -> None:
    """One-time migration: split legacy history.json into per-session files.
    Only migrates sessions that don't already have a per-session file."""
    if not HISTORY_PATH.exists():
        return
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return
        migrated = 0
        for sid, buf in data.items():
            if not isinstance(buf, list) or not buf:
                continue
            per_session_path = _session_history_path(sid)
            if per_session_path.exists():
                continue  # Don't overwrite existing per-session files
            _save_json(per_session_path, buf[-500:])
            migrated += 1
        if migrated:
            _log(f"HISTORY_MIGRATION migrated {migrated} sessions from legacy history.json to per-session files")
    except Exception as exc:
        _log(f"HISTORY_MIGRATION error: {exc}")


def load_session_history(session_id: str) -> List[dict]:
    """Load history for a single session from its per-session file."""
    path = _session_history_path(session_id)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except FileNotFoundError:
        pass
    except Exception as exc:
        _log(f"Failed to load session history {session_id}: {exc}")
    return []


def save_session_history(session_id: str, buf: List[dict]) -> None:
    """Save history for a single session to its per-session file.
    This avoids the concurrent-write race condition where multiple stdio
    processes (one per agent) would overwrite each other's data in a
    shared history.json."""
    try:
        if buf:
            _save_json(_session_history_path(session_id), buf[-500:])
    except Exception as exc:
        _log(f"Failed to save session history {session_id}: {exc}")


def delete_session_history(session_id: str) -> None:
    """Delete a session's per-session history file."""
    try:
        _session_history_path(session_id).unlink(missing_ok=True)
    except Exception as exc:
        _log(f"Failed to delete session history {session_id}: {exc}")


# Regex for log line timestamps: [2026-06-20T14:47:32Z]
_LOG_TS_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\]")


def recover_history_from_log(session_id: str) -> int:
    """Rebuild a session's per-session history file from supervisor.log.

    Used when a history file is missing (e.g. lost in the legacy shared
    history.json concurrent-write race, or created before per-session files
    existed). Scans PARENT→ session/update notifications for the session,
    deduplicates by content, and writes the most recent 500 (matching the
    supervisor's normal cap). Returns the number of notifications written."""
    try:
        if not LOG_PATH.exists():
            return 0
        recovered: List[dict] = []
        seen_keys = set()
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "PARENT→ " not in line or session_id not in line:
                    continue
                idx = line.find("PARENT→ ")
                if idx < 0:
                    continue
                payload = line[idx + 8:].strip()
                try:
                    msg = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if msg.get("method") != "session/update":
                    continue
                params = msg.get("params", {})
                if params.get("sessionId") != session_id:
                    continue
                update = params.get("update", {})
                if update.get("sessionUpdate") in ("usage_update", "current_mode_update"):
                    continue
                # Skip image blocks with truncated base64 data. The log's
                # _compact_for_log truncates strings >800 chars, so large
                # image data in the log is incomplete and would produce a
                # broken image on replay. These should have been persisted
                # to the history file directly (persist=True on user chunks).
                content = update.get("content", {})
                if isinstance(content, dict) and content.get("type") == "image":
                    data = content.get("data", "")
                    if not isinstance(data, str) or data.endswith("...<truncated>") or len(data) < 100:
                        continue
                content_hash = hash(json.dumps(update, sort_keys=True))
                if content_hash in seen_keys:
                    continue
                seen_keys.add(content_hash)
                recovered.append(msg)
        if not recovered:
            return 0
        data = recovered[-500:]
        save_session_history(session_id, data)
        _log(f"HISTORY_RECOVER external={session_id} recovered={len(recovered)} wrote={len(data)}")
        return len(data)
    except Exception as exc:
        _log(f"HISTORY_RECOVER error for {session_id}: {exc}")
        return 0


def _find_devin_db() -> Optional[Path]:
    """Find the Devin sessions.db file."""
    for candidate in DEVIN_SESSION_DB_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def recover_history_from_db(external_session_id: str, native_session_id: str) -> int:
    """Rebuild a session's history from Devin's sessions.db message_nodes table.

    This is the most complete recovery method — it extracts all user and
    assistant messages (including tool calls) from the native Devin database,
    deduplicates by message_id, and writes them as ACP session/update
    notifications with the external Paseo session ID.

    Returns the number of events written, or 0 if recovery failed."""
    try:
        db_path = _find_devin_db()
        if db_path is None:
            return 0

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT row_id, chat_message, metadata FROM message_nodes "
            "WHERE session_id = ? ORDER BY row_id",
            (native_session_id,)
        )

        seen_msg_ids: Set[str] = set()
        history: List[dict] = []

        for r in cursor.fetchall():
            cm = json.loads(r["chat_message"])
            meta_raw = r["metadata"]
            meta = json.loads(meta_raw) if meta_raw else {}
            if meta is None:
                meta = {}

            if meta.get("is_system_prefix", False):
                continue

            role = cm.get("role", "")
            if role == "system":
                continue

            mid = cm.get("message_id", "")
            if mid and mid in seen_msg_ids:
                continue
            if mid:
                seen_msg_ids.add(mid)

            content = cm.get("content", "")
            tool_calls = cm.get("tool_calls", [])

            if role == "user":
                if not isinstance(content, str) or not content.strip():
                    continue
                if content.startswith("<") and (
                    "system" in content[:80].lower()
                    or "summarize" in content[:80].lower()
                ):
                    continue
                if "Conversation to summarize" in content[:50]:
                    continue
                if content.startswith("Now summarize"):
                    continue
                if "<paseo-system>" in content:
                    continue
                history.append({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": external_session_id,
                        "update": {
                            "sessionUpdate": "user_message_chunk",
                            "content": {"type": "text", "text": content},
                            "messageId": str(uuid.uuid4()),
                        },
                    },
                })
            elif role == "assistant":
                has_text = isinstance(content, str) and content.strip()
                has_tools = bool(tool_calls)
                if not has_text and not has_tools:
                    continue
                if has_text:
                    history.append({
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": external_session_id,
                            "update": {
                                "sessionUpdate": "agent_message_chunk",
                                "content": {"type": "text", "text": content},
                            },
                        },
                    })
                for tc in tool_calls:
                    tc_id = tc.get("id", "")
                    fn = tc.get("function", {})
                    fn_name = fn.get("name", "tool")
                    fn_args = fn.get("arguments", "")
                    try:
                        args = json.loads(fn_args) if isinstance(fn_args, str) else fn_args
                        if fn_name == "exec":
                            cmd = args.get("command", "")
                            title = cmd[:80] if len(cmd) > 80 else cmd
                        elif fn_name in ("edit", "read", "write"):
                            title = args.get("file_path", fn_name)
                        else:
                            title = fn_name
                    except Exception:
                        title = fn_name
                    history.append({
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": external_session_id,
                            "update": {
                                "sessionUpdate": "tool_call",
                                "toolCallId": tc_id,
                                "title": title,
                                "kind": "execute",
                            },
                        },
                    })
                    history.append({
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": external_session_id,
                            "update": {
                                "sessionUpdate": "tool_call_update",
                                "toolCallId": tc_id,
                                "status": "completed",
                            },
                        },
                    })

        conn.close()

        if not history:
            return 0

        # Cap at 500 events to match supervisor's normal limit
        history = history[-500:]
        save_session_history(external_session_id, history)
        _log(f"HISTORY_DB_RECOVER external={external_session_id} native={native_session_id} wrote={len(history)}")
        return len(history)
    except Exception as exc:
        _log(f"HISTORY_DB_RECOVER error for external={external_session_id} native={native_session_id}: {exc}")
        return 0


# Legacy functions kept for backward compat with any external callers
def load_history() -> Dict[str, List[dict]]:
    """Load all history from per-session files (replaces legacy shared file)."""
    result = {}
    if HISTORY_DIR.exists():
        for path in HISTORY_DIR.glob("*.json"):
            sid = path.stem
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and data:
                        result[sid] = data
            except Exception:
                pass
    return result


def save_history(history: Dict[str, List[dict]]) -> None:
    """Save all session histories to per-session files."""
    for sid, buf in history.items():
        if buf:
            save_session_history(sid, buf)


# ---------------------------------------------------------------------------
# Child pool
# ---------------------------------------------------------------------------

class ChildState(enum.Enum):
    SPAWNING = "spawning"
    WARM = "warm"
    IDLE = "idle"
    BUSY = "busy"
    DYING = "dying"
    DEAD = "dead"


@dataclasses.dataclass
class NativeChild:
    proc: subprocess.Popen
    state: ChildState
    real_session_id: Optional[str] = None
    spawned_at: float = dataclasses.field(default_factory=time.time)
    last_used: float = dataclasses.field(default_factory=time.time)
    pending_rpc: Dict[Any, queue.Queue] = dataclasses.field(default_factory=dict)
    notif_handlers: List = dataclasses.field(default_factory=list)
    stdin_lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    reader_thread: Optional[threading.Thread] = None
    stderr_thread: Optional[threading.Thread] = None
    stderr_deque: collections.deque = dataclasses.field(default_factory=lambda: collections.deque(maxlen=200))
    last_stderr_at: float = dataclasses.field(default_factory=time.time)
    last_network_active_at: float = 0.0
    shutdown: bool = False


class ChildPool:
    def __init__(self):
        self.lock = threading.RLock()
        self.children: List[NativeChild] = []
        self.warm_spare: Optional[NativeChild] = None

    def _is_alive(self, child: Optional[NativeChild]) -> bool:
        if child is None:
            return False
        if child.shutdown:
            return False
        return child.proc.poll() is None

    def acquire(self, real_session_id: Optional[str] = None) -> NativeChild:
        with self.lock:
            # 1. Clean up dead children
            dead = [c for c in self.children if not self._is_alive(c)]
            for c in dead:
                _log(f"REAP dead child pid={c.proc.pid}")
                self.children.remove(c)
                if self.warm_spare is c:
                    self.warm_spare = None

            # 2. Session affinity
            if real_session_id:
                for c in self.children:
                    if self._is_alive(c) and c.real_session_id == real_session_id and c.state == ChildState.IDLE:
                        c.state = ChildState.BUSY
                        c.last_used = time.time()
                        _log(f"ACQUIRE affinity pid={c.proc.pid} real={real_session_id}")
                        return c

            # 3. Idle reuse. Do not steal a child already bound to a
            # different native session; that child has only loaded its own
            # Devin session, and stamping a new real_session_id onto it causes
            # the next session/prompt to fail with "Session not found".
            if real_session_id:
                idle = [
                    c for c in self.children
                    if self._is_alive(c) and c.state == ChildState.IDLE and c.real_session_id is None
                ]
            else:
                idle = [c for c in self.children if self._is_alive(c) and c.state == ChildState.IDLE]
            if idle:
                child = sorted(idle, key=lambda c: c.last_used, reverse=True)[0]
                child.state = ChildState.BUSY
                if real_session_id is None:
                    child.real_session_id = None
                child.last_used = time.time()
                _log(f"ACQUIRE idle pid={child.proc.pid} real={real_session_id}")
                return child

            # 4. Warm spare
            if self._is_alive(self.warm_spare):
                child = self.warm_spare
                self.warm_spare = None
                child.state = ChildState.BUSY
                child.real_session_id = None
                child.last_used = time.time()
                self.children.append(child)
                _log(f"ACQUIRE warm pid={child.proc.pid} real={real_session_id}")
                return child

            # 5. Spawn fresh
            child = self._spawn_native()
            child.state = ChildState.BUSY
            child.real_session_id = None
            self.children.append(child)
            _log(f"ACQUIRE fresh pid={child.proc.pid} real={real_session_id}")
            return child

    def release(self, child: NativeChild, real_session_id: Optional[str] = None) -> None:
        with self.lock:
            if not self._is_alive(child):
                return
            child.state = ChildState.IDLE
            if real_session_id is not None:
                child.real_session_id = real_session_id
            child.last_used = time.time()
            _log(f"RELEASE pid={child.proc.pid} real={child.real_session_id}")

    def mark_dying(self, child: NativeChild) -> None:
        should_stop = False
        with self.lock:
            if child in self.children:
                self.children.remove(child)
                should_stop = True
            if self.warm_spare is child:
                self.warm_spare = None
                should_stop = True
            if self._is_alive(child):
                should_stop = True
            child.shutdown = True
            child.real_session_id = None
            try:
                child.state = ChildState.DYING
            except Exception:
                pass
            _log(f"MARK_DYING pid={child.proc.pid}")
        if should_stop:
            self._stop_child(child)

    def reap_dead(self) -> None:
        with self.lock:
            dead = [c for c in self.children if not self._is_alive(c)]
            for c in dead:
                rc = c.proc.returncode
                real_sid = c.real_session_id
                _log(f"REAP_DEAD pid={c.proc.pid} rc={rc} real={real_sid} state={c.state.name}")
                # Log last stderr lines for diagnosis
                if c.stderr_deque:
                    tail = list(c.stderr_deque)[-5:]
                    _log(f"REAP_DEAD_STDERR pid={c.proc.pid} {' | '.join(tail)}")
                c.shutdown = True
                self.children.remove(c)
            if self.warm_spare and not self._is_alive(self.warm_spare):
                rc = self.warm_spare.proc.returncode
                _log(f"REAP_DEAD_WARM pid={self.warm_spare.proc.pid} rc={rc}")
                self.warm_spare.shutdown = True
                self.warm_spare = None

    def spawn_warm_spare(self) -> None:
        with self.lock:
            if self.warm_spare is not None and self._is_alive(self.warm_spare):
                return
            _log("SPAWN_WARM")
            self.warm_spare = self._spawn_native()

    def kill_oldest_idle_if_over_max(self) -> None:
        with self.lock:
            idle = [c for c in self.children if c.state == ChildState.IDLE]
            total = len(self.children) + (1 if self.warm_spare else 0)
            if total <= MAX_CHILDREN:
                return
            if not idle:
                return
            oldest = min(idle, key=lambda c: c.last_used)
            _log(f"KILL_OLDEST_IDLE pid={oldest.proc.pid} age={time.time() - oldest.last_used:.0f}s")
            self._stop_child(oldest)
            if oldest in self.children:
                self.children.remove(oldest)

    def _spawn_native(self) -> NativeChild:
        cmd = [REAL_DEVIN, "--config", DEVIN_CONFIG, "acp"]
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        child = NativeChild(proc=proc, state=ChildState.SPAWNING)

        def reader():
            try:
                for line in proc.stdout:
                    if child.shutdown:
                        break
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    rid = msg.get("id")
                    if rid is not None and rid in child.pending_rpc:
                        child.pending_rpc[rid].put(msg)
                    elif "method" in msg:
                        for handler in list(child.notif_handlers):
                            try:
                                handler(msg)
                            except Exception:
                                pass
            except Exception:
                pass
        child.reader_thread = threading.Thread(target=reader, daemon=True)
        child.reader_thread.start()

        def drain_stderr():
            try:
                for line in proc.stderr:
                    child.stderr_deque.append(line.rstrip())
                    child.last_stderr_at = time.time()
            except Exception:
                pass
        child.stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        child.stderr_thread.start()
        _log(f"SPAWN_NATIVE pid={proc.pid}")
        # Devin 2026.8.18+ requires initialize before any other RPC.
        init_id = f"init-{proc.pid}"
        init_req = {"jsonrpc": "2.0", "id": init_id, "method": "initialize", "params": {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": True, "writeTextFile": True}, "terminal": True, "_meta": {"cognition.ai/revert": True, "cognition.ai/subagentControl": True, "cognition.ai/chains": True}}, "clientInfo": {"name": "devin-paseo-supervisor", "version": "2.0.0"}}}
        init_q = queue.Queue()
        child.pending_rpc[init_id] = init_q
        try:
            with child.stdin_lock:
                child.proc.stdin.write(json.dumps(init_req, separators=(",", ":")) + "\n")
                child.proc.stdin.flush()
            init_res = init_q.get(timeout=30)
        except queue.Empty:
            _log(f"SPAWN_NATIVE init TIMEOUT pid={proc.pid}")
            init_res = {"error": {"message": "timeout"}}
        except (BrokenPipeError, OSError) as exc:
            _log(f"SPAWN_NATIVE init BROKEN_PIPE pid={proc.pid}: {exc}")
            init_res = {"error": {"message": str(exc)}}
        finally:
            child.pending_rpc.pop(init_id, None)
        if "error" in init_res:
            _log(f"SPAWN_NATIVE init ERROR pid={proc.pid}: {init_res['error'].get('message')}")
        else:
            _log(f"SPAWN_NATIVE init OK pid={proc.pid}")
        return child

    def _stop_child(self, child: NativeChild) -> None:
        if child.proc.poll() is not None:
            return
        child.shutdown = True
        try:
            child.proc.terminate()
            child.proc.wait(timeout=5)
        except Exception:
            try:
                child.proc.kill()
                child.proc.wait(timeout=2)
            except Exception:
                pass

    def stop_all(self) -> None:
        with self.lock:
            for c in list(self.children):
                self._stop_child(c)
            if self.warm_spare:
                self._stop_child(self.warm_spare)
            self.children.clear()
            self.warm_spare = None


# ---------------------------------------------------------------------------
# Turn state machine
# ---------------------------------------------------------------------------

class TurnPhase(enum.Enum):
    QUEUED = "queued"
    TYPING = "typing"
    ACTIVE = "active"
    COMPLETING = "completing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


@dataclasses.dataclass
class ToolResult:
    name: str
    status: str
    is_empty: bool
    output_preview: str


@dataclasses.dataclass
class Turn:
    upstream_request_id: Any
    native_request_id: Any
    external_session_id: str
    real_session_id: Optional[str]
    phase: TurnPhase
    prompt_text: str
    started_at: float
    last_update_at: float
    first_update_at: Optional[float] = None
    # Full ACP ContentBlock array from session/prompt, including image and
    # resource blocks. Forwarded verbatim to native Devin in native mode.
    # Empty list falls back to text-only behavior.
    prompt_blocks: list = dataclasses.field(default_factory=list)
    received_any_update: bool = False
    cancel_sent: bool = False
    nudge_sent: bool = False
    timeout_reason: Optional[str] = None
    latest_tool_result: Optional[ToolResult] = None
    grounding_appended: bool = False
    final_message: Optional[str] = None
    child: Optional[NativeChild] = None
    active_terminal_ids: Set[str] = dataclasses.field(default_factory=set)
    active_terminal_commands: Dict[str, str] = dataclasses.field(default_factory=dict)
    result_sent: bool = False
    failure_message: Optional[str] = None
    last_thought_update: Optional[dict] = None
    thinking_chars: int = 0
    client_message_id: Optional[str] = None
    pending_run_subagent_call: Optional[str] = None
    message_buffer: list = dataclasses.field(default_factory=list)
    flush_timer: Optional[threading.Timer] = None
    last_stall_check_at: float = 0.0
    had_agent_message: bool = False
    auto_resume_injected: bool = False
    auto_resume_count: int = 0
    # Per-turn token accumulation: sum of this turn's model requests
    # (Devin Desktop "Response statistics" semantics — totals per user
    # message, not per session).
    usage_seen: set = dataclasses.field(default_factory=set)
    usage_totals: dict = dataclasses.field(
        default_factory=lambda: {"input": 0, "output": 0, "cached": 0, "requests": 0}
    )
    # Delegated-model usage (fusion sidekick / spawned subagents), keyed by
    # subagent_context.parentAgentId — kept separate so the stats card can
    # split lead vs sidekick consumption.
    usage_sub_totals: dict = dataclasses.field(default_factory=dict)
    # Subagent ids for which we synthesized a provider_subagent card this
    # turn (fusion sidekick has no started/completed lifecycle events).
    subagent_cards: set = dataclasses.field(default_factory=set)

    def is_cancelled(self) -> bool:
        return self.cancel_sent or self.phase == TurnPhase.CANCELLED

    def mark_typing(self) -> bool:
        if self.is_cancelled():
            self.phase = TurnPhase.CANCELLED
            return False
        self.phase = TurnPhase.TYPING
        return True

    def mark_active(self) -> None:
        if self.is_cancelled():
            self.phase = TurnPhase.CANCELLED
            return
        self.last_update_at = time.time()
        self.received_any_update = True
        if self.phase in (TurnPhase.QUEUED, TurnPhase.TYPING):
            self.phase = TurnPhase.ACTIVE

    def mark_completing(self) -> None:
        if self.is_cancelled():
            self.phase = TurnPhase.CANCELLED
            return
        if self.phase == TurnPhase.ACTIVE:
            self.phase = TurnPhase.COMPLETING

    def mark_completed(self) -> None:
        if self.is_cancelled():
            self.phase = TurnPhase.CANCELLED
            return
        self.phase = TurnPhase.COMPLETED

    def mark_cancelled(self) -> None:
        self.phase = TurnPhase.CANCELLED

    def mark_failed(self) -> None:
        if self.is_cancelled():
            self.phase = TurnPhase.CANCELLED
            return
        self.phase = TurnPhase.FAILED

    def fail_with_message(self, message: str) -> None:
        self.failure_message = message
        self.mark_failed()

    def mark_timed_out(self, reason: str = "idle") -> None:
        if self.is_cancelled():
            self.phase = TurnPhase.CANCELLED
            return
        self.phase = TurnPhase.TIMED_OUT
        self.timeout_reason = reason


# ---------------------------------------------------------------------------
# Export streamer (print-mode)
# ---------------------------------------------------------------------------

class DevinExportStreamer:
    def __init__(self, session_id: str, export_path: Path, prompt_text: str, replay_history: bool = False):
        self.session_id = session_id
        self.export_path = export_path
        self.prompt_text = (prompt_text or "").strip()
        self.replay_history = replay_history
        self.stream_start: Optional[int] = 0 if replay_history else None
        self.emitted_user_steps: Set[int] = set()
        self.emitted_thought_steps: Set[int] = set()
        self.emitted_message_steps: Set[int] = set()
        self.emitted_tool_calls: Set[str] = set()
        self.emitted_tool_updates: Set[str] = set()
        self.final_message: Optional[str] = None
        self.final_message_streamed: bool = False
        self.last_replayed_user_text: Optional[str] = None
        self.title: Optional[str] = None
        self.local_session_id: Optional[str] = None

    def poll(self) -> Optional[dict]:
        transcript = self._read_export()
        if not transcript:
            return None
        self.consume(transcript)
        return transcript

    def _read_export(self) -> Optional[dict]:
        if not self.export_path.exists():
            return None
        try:
            data = self.export_path.read_text(encoding="utf-8")
            if not data:
                return None
            return json.loads(data)
        except json.JSONDecodeError:
            return None
        except Exception as exc:
            _log(f"EXPORT_READ_ERROR path={self.export_path} exc={exc}")
            return None

    def consume(self, transcript: dict) -> None:
        self.local_session_id = transcript.get("session_id") or self.local_session_id
        steps = transcript.get("steps") if isinstance(transcript, dict) else []
        if not isinstance(steps, list):
            return
        self._prime_start(steps)
        if self.stream_start is None:
            return
        for index, step in enumerate(steps[self.stream_start:], start=self.stream_start):
            if not isinstance(step, dict):
                continue
            if step.get("source") == "user":
                self._emit_user_message(index, step)
                continue
            if step.get("source") != "agent":
                continue
            self._emit_reasoning(index, step)
            self._emit_tool_calls(index, step)
            self._emit_tool_updates(index, step)
            self._emit_agent_message(index, step)

    def _prime_start(self, steps: List[dict]) -> None:
        if self.stream_start is not None:
            return
        latest_user = None
        for index, step in enumerate(steps):
            if not isinstance(step, dict) or step.get("source") != "user":
                continue
            message = step.get("message")
            if isinstance(message, str) and message.strip():
                self.title = message.strip().splitlines()[0][:120]
            if isinstance(message, str) and message.strip() == self.prompt_text:
                latest_user = index
        if latest_user is not None:
            self.stream_start = latest_user + 1
        else:
            self.stream_start = len(steps)

    def _emit_user_message(self, index: int, step: dict) -> None:
        if not self.replay_history or index in self.emitted_user_steps:
            return
        text = step.get("message")
        if not isinstance(text, str) or not text.strip():
            return
        if text == self.last_replayed_user_text:
            self.emitted_user_steps.add(index)
            return
        self.last_replayed_user_text = text
        self.emitted_user_steps.add(index)

    def _emit_reasoning(self, index: int, step: dict) -> None:
        if index in self.emitted_thought_steps:
            return
        text = step.get("reasoning_content")
        if not isinstance(text, str) or not text.strip():
            return
        self.emitted_thought_steps.add(index)

    def _emit_tool_calls(self, index: int, step: dict) -> None:
        metadata = step.get("metadata") if isinstance(step, dict) else {}
        extensions = metadata.get("extensions") if isinstance(metadata, dict) else {}
        tool_content = extensions.get("chisel/tool_call_content") if isinstance(extensions, dict) else {}
        if isinstance(tool_content, dict):
            for call_id, payload in tool_content.items():
                if call_id in self.emitted_tool_calls:
                    continue
                self.emitted_tool_calls.add(call_id)
            return
        for call in step.get("tool_calls") or []:
            if not isinstance(call, dict):
                continue
            tool_call_id = call.get("tool_call_id") or call.get("id") or f"devin-tool-{index}"
            if tool_call_id in self.emitted_tool_calls:
                continue
            self.emitted_tool_calls.add(tool_call_id)

    def _emit_tool_updates(self, index: int, step: dict) -> None:
        observation = step.get("observation")
        results = observation.get("results") if isinstance(observation, dict) else []
        if not isinstance(results, list):
            return
        for result_index, result in enumerate(results):
            if not isinstance(result, dict):
                continue
            tool_call_id = result.get("source_call_id") or result.get("tool_call_id") or result.get("id")
            if not tool_call_id:
                continue
            update_key = f"{index}:{result_index}:{tool_call_id}"
            if update_key in self.emitted_tool_updates:
                continue
            self.emitted_tool_updates.add(update_key)

    def _emit_agent_message(self, index: int, step: dict) -> None:
        if index in self.emitted_message_steps:
            return
        text = step.get("message")
        if not isinstance(text, str) or not text.strip():
            return
        self.emitted_message_steps.add(index)
        self.final_message = text
        self.final_message_streamed = True


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------

class Supervisor:
    def __init__(self):
        self.lock = threading.RLock()
        self.pool = ChildPool()
        self.sessions: Dict[str, dict] = {}
        self.turns: Dict[str, Turn] = {}  # external_session_id -> active turn
        self.history: Dict[str, List[dict]] = {}
        # Migrate legacy shared history.json to per-session files (one-time)
        migrate_legacy_history()
        # Load persisted state so sessions survive across stdio process restarts
        try:
            state = load_state()
            persisted = state.get("sessions", {})
            if persisted:
                self.sessions.update(persisted)
                _log(f"LOADED_SESSIONS count={len(self.sessions)}")
            # Load history from per-session files (lazy: only load sessions
            # we know about from state, to avoid loading hundreds of files)
            for sid in self.sessions:
                buf = load_session_history(sid)
                if buf:
                    self.history[sid] = buf
            if self.history:
                _log(f"LOADED_HISTORY count={len(self.history)}")
        except Exception as exc:
            _log(f"LOAD_STATE_ERROR {exc}")
        self._next_id = 1
        self._shutdown = False
        self._orphan_buffer: collections.deque = collections.deque(maxlen=200)
        self._retired_rpc_ids: Set[Any] = set()
        self._mode_preference: str = SUPERVISOR_MODE
        self._mode_detected: Optional[str] = None
        self._parent_write_lock = threading.Lock()
        # Track which sessions have had history replayed to paseo in this
        # process lifetime. Prevents double-replay on repeated session/load
        # or session/prompt calls.
        self._replayed_sessions: Set[str] = set()
        # Sessions we have already attempted log-based history recovery for,
        # so we don't re-scan the (large) supervisor.log on every replay.
        self._history_recover_attempted: Set[str] = set()
        # Parent responses for child-initiated ACP requests forwarded through
        # the supervisor, such as terminal/create and session/request_permission.
        self._parent_pending_rpc: Dict[Any, queue.Queue] = {}

    # -----------------------------------------------------------------------
    # IDs
    # -----------------------------------------------------------------------

    def next_id(self) -> int:
        with self.lock:
            rid = self._next_id
            self._next_id += 1
            return rid

    # -----------------------------------------------------------------------
    # Parent I/O helpers
    # -----------------------------------------------------------------------

    def write_parent(self, obj: dict) -> None:
        with self._parent_write_lock:
            line = json.dumps(obj, separators=(",", ":")) + "\n"
            sys.stdout.write(line)
            sys.stdout.flush()
            # Skip verbose logging for high-frequency streaming chunks
            method = obj.get("method", "")
            if method == "session/update":
                update = obj.get("params", {}).get("update", {})
                su = update.get("sessionUpdate", "") if isinstance(update, dict) else ""
                if su in ("agent_thought_chunk", "agent_message_chunk"):
                    return
            _log(f"PARENT→ {json.dumps(_compact_for_log(obj), separators=(',', ':'))}")

    def send_error(self, req_id: Any, code: int, message: str, external_session_id: Optional[str] = None) -> None:
        if external_session_id:
            self.write_parent({
                "jsonrpc": "2.0",
                "method": "_cognition.ai/agent_stopped",
                "params": {
                    "cause": "error",
                    "errorMessage": message,
                    "stats": {"toolCalls": 0, "filesChanged": 0, "commandsRun": 0, "modelLabel": "unknown"},
                    "sessionId": external_session_id,
                },
            })
        self.write_parent({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": code, "message": message, "data": {"cognition.ai/errorKind": "internal", "cognition.ai/retryable": code in (-32003, -32005, -32013)}},
        })

    def send_result(self, req_id: Any, result: dict) -> None:
        self.write_parent({"jsonrpc": "2.0", "id": req_id, "result": result})

    def _emit_usage_summary(self, turn: "Turn") -> None:
        """Emit a Devin-Desktop-style per-response stats block as a collapsed
        'think' tool_call timeline item, since the client UI does not render
        turn_completed.usage."""
        session = self._get_session(turn.external_session_id)
        lu = (session or {}).get("lastUsage") or {}
        meta = lu.get("_meta") or {}
        used = lu.get("used")
        size = lu.get("size")
        # Per-turn token totals (Devin Desktop semantics: sum of all model
        # requests made while processing this one user message).
        totals = turn.usage_totals
        requests = totals.get("requests")
        if requests:
            inp = totals.get("input")
            out = totals.get("output")
            cached = totals.get("cached")
        else:
            # Sessions that predate cumulative tracking: fall back to the
            # last request's usage like before.
            inp = meta.get("cognition.ai/inputTokens")
            out = meta.get("cognition.ai/outputTokens")
            cached = meta.get("cognition.ai/cachedReadTokens")
            requests = None
        if inp is None and out is None and used is None:
            return
        elapsed = max(0.0, time.time() - (turn.started_at or time.time()))
        model = (session or {}).get("model") or "unknown"
        lines = [f"Model  {model}"]
        # Devin's inputTokens includes cached reads; Devin Desktop shows the
        # non-cached portion as "Input tokens" and cached separately.
        non_cached_in = inp - cached if (inp is not None and cached is not None) else inp
        if non_cached_in is not None:
            lines.append(f"Input tokens  {non_cached_in:,}")
        if out is not None:
            lines.append(f"Output tokens  {out:,}")
        if turn.thinking_chars > 0:
            # Devin does not report reasoning tokens; estimate from streamed
            # thought text (chars/4) and mark as an estimate.
            thinking_est = max(1, round(turn.thinking_chars / 4))
            lines.append(f"Thinking tokens  ~{thinking_est:,} (est.)")
        if cached is not None:
            lines.append(f"Cached tokens  {cached:,}")
        if requests:
            lines.append(f"Requests  {requests:,}")
        # Delegated-model usage (fusion sidekick / subagents) — reported
        # separately from the lead numbers above.
        for _sid, _st in turn.usage_sub_totals.items():
            _sr = _st.get("requests")
            if not _sr:
                continue
            if _sid == "sidekick":
                _label = f"Sidekick {(session or {}).get('sidekick') or ''}".rstrip()
            else:
                _nm = ((session or {}).get("subagentNames") or {}).get(_sid) or _sid
                _label = f"Subagent {str(_nm)[:24]}"
            lines.append(
                f"{_label}  input {(_st.get('input') or 0) - (_st.get('cached') or 0):,} "
                f"· output {(_st.get('output') or 0):,} "
                f"· cached {(_st.get('cached') or 0):,} · {_sr:,} requests"
            )
        # Session-wide cumulative line (all turns since session start).
        s_totals = (session or {}).get("usageTotals") or {}
        s_requests = s_totals.get("requests")
        if s_requests:
            s_in = s_totals.get("input") or 0
            s_cached = s_totals.get("cached") or 0
            lines.append(
                "Session  "
                f"input {s_in - s_cached:,} · output {(s_totals.get('output') or 0):,} "
                f"· cached {s_cached:,} · {s_requests:,} requests"
            )
        if used is not None:
            ctx = f"{used:,} / {size:,}" if size else f"{used:,}"
            if size:
                ctx += f" ({used * 100 // size}%)"
            lines.append("Context  " + ctx + " tokens")
        if turn.first_update_at is not None:
            ttft = max(0.0, turn.first_update_at - turn.started_at)
            lines.append(f"First word  {ttft:.1f}s")
        lines.append(f"All time  {_format_duration(elapsed)}")
        self.send_notification(turn.external_session_id, {
            "sessionUpdate": "tool_call",
            "toolCallId": f"usage-{turn.upstream_request_id}",
            "title": "Response statistics",
            "kind": "think",
            "status": "completed",
            "content": [{
                "type": "content",
                "content": {"type": "text", "text": "\n".join(lines)},
            }],
        })

    def _finalize_subagent_cards(self, turn: "Turn") -> None:
        """Turn end cleanup for synthesized subagent cards: Devin sends no
        completion updates for delegated-model tool calls and no lifecycle
        events for the fusion sidekick, so close out whatever is still
        pending."""
        _pend = getattr(turn, "subagent_pending_tool", None) or {}
        for _sid, _tid in _pend.items():
            self.send_notification(turn.external_session_id, {
                "sessionUpdate": "tool_call_update",
                "toolCallId": _tid,
                "status": "completed",
                "_meta": {"paseo/subagentTimeline": _sid},
            })
        for _sid in getattr(turn, "subagent_cards", ()):
            self.send_notification(turn.external_session_id, {
                "sessionUpdate": "tool_call_update",
                "toolCallId": f"subcard-{_sid}",
                "status": "completed",
                "_meta": {"paseo/subagent": {"id": _sid, "status": "completed"}},
            })

    def _prompt_result(self, external_session_id: str) -> dict:
        """Build the session/prompt result, attaching ACP usage when the native
        child reported a usage_update during the turn."""
        result: dict = {"stopReason": "end_turn"}
        session = self._get_session(external_session_id)
        lu = (session or {}).get("lastUsage") or {}
        meta = lu.get("_meta") or {}
        inp = meta.get("cognition.ai/inputTokens")
        out = meta.get("cognition.ai/outputTokens")
        cached = meta.get("cognition.ai/cachedReadTokens")
        if inp is not None or out is not None or lu.get("used") is not None:
            usage = {
                "inputTokens": inp or 0,
                "outputTokens": out or 0,
                "totalTokens": lu.get("used") or (inp or 0) + (out or 0),
            }
            if cached is not None:
                usage["cachedReadTokens"] = cached
            usage["_meta"] = {
                "devin/contextUsed": lu.get("used"),
                "devin/contextSize": lu.get("size"),
            }
            result["usage"] = usage
        return result

    def _session_result(self, external_id: str, session: dict) -> dict:
        session["mode"] = DEFAULT_DEVIN_MODE
        model = session.get("model", "swe-2-medium")
        return {
            "sessionId": external_id,
            "modes": {"currentModeId": DEFAULT_DEVIN_MODE, "availableModes": get_cached_modes()},
            "configOptions": _structured_config_options(model, DEFAULT_DEVIN_MODE, feature_values=session.get("featureValues"), sidekick=session.get("sidekick")),
            "models": _acp_model_state(model),
        }

    def send_notification(self, external_session_id: str, update: dict) -> None:
        self.write_parent({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"sessionId": external_session_id, "update": update},
        })

    def _emit_config_options(self, external_id: str, config_options) -> None:
        # The adapter's unstable set_model path discards the response
        # payload, leaving its cached configOptions stale (e.g. missing the
        # sidekick select after switching to a fusion model) so a following
        # setFeature fails with "does not expose ACP feature". Emitting this
        # before the response guarantees the adapter applies it first.
        self.send_notification(external_id, {
            "sessionUpdate": "config_option_update",
            "configOptions": config_options or [],
        })

    def _send_parent_rpc(self, method: str, params: dict, timeout: float) -> dict:
        req_id = f"supervisor-{uuid.uuid4()}"
        q: queue.Queue = queue.Queue()
        self._parent_pending_rpc[req_id] = q
        self.write_parent({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        })
        try:
            return q.get(timeout=timeout)
        except queue.Empty:
            _log(f"PARENT_RPC_TIMEOUT id={req_id} method={method} timeout={timeout}")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": f"Parent request timeout after {timeout}s"},
            }
        finally:
            self._parent_pending_rpc.pop(req_id, None)

    def _write_child_response(self, child: NativeChild, response: dict) -> None:
        body = json.dumps(response, separators=(",", ":")) + "\n"
        with child.stdin_lock:
            child.proc.stdin.write(body)
            child.proc.stdin.flush()

    def _force_native_mode(self, child: NativeChild, real_sid: str, mode: str = DEFAULT_DEVIN_MODE) -> None:
        """Best-effort native mode enforcement.

        Paseo cannot reliably change modes for ACP-backed Devin sessions after
        creation, and Devin may still emit permission requests in accept-edits.
        Keep the native session itself on bypass whenever we create or reload it.
        """
        for method, params in (
            ("session/set_config_option", {"sessionId": real_sid, "configId": "mode", "value": mode}),
            ("session/set_mode", {"sessionId": real_sid, "modeId": mode}),
        ):
            req = {"jsonrpc": "2.0", "id": self.next_id(), "method": method, "params": params}
            try:
                res = self._send_rpc(child, req, timeout=10)
                if "error" in res:
                    _log(f"FORCE_MODE {method} error real={real_sid} mode={mode} msg={res.get('error', {}).get('message')}")
                else:
                    _log(f"FORCE_MODE {method} ok real={real_sid} mode={mode}")
            except Exception as exc:
                _log(f"FORCE_MODE {method} failed real={real_sid} mode={mode} exc={exc}")

    def _force_native_model(self, child: NativeChild, real_sid: str, model: Optional[str], effort: Optional[str] = None, sidekick: Optional[str] = None) -> None:
        if not model:
            return
        # Devin's set_config_option expects a concrete variant id
        # (e.g. "swe-2-max", "gpt-5-6-luna-max", MODEL_GPT_5_2_HIGH or a
        # fusion-...-sidekick-... combo); bare families 404.
        fam = _family_for_model_id(model)
        concrete = fam is not None and (
            model in (fam.get("variant_map") or {}).values()
            or any(model in sks.values() for sks in (fam.get("pairs") or {}).values())
        )
        if not concrete:
            model = _resolve_model(_model_family(model), _split_model_id(model)[1] or effort, sidekick)
        req = {
            "jsonrpc": "2.0",
            "id": self.next_id(),
            "method": "session/set_config_option",
            "params": {"sessionId": real_sid, "configId": "model", "value": model},
        }
        try:
            res = self._send_rpc(child, req, timeout=10)
            if "error" in res:
                _log(f"FORCE_MODEL error real={real_sid} model={model} msg={res.get('error', {}).get('message')}")
            else:
                _log(f"FORCE_MODEL ok real={real_sid} model={model}")
        except Exception as exc:
            _log(f"FORCE_MODEL failed real={real_sid} model={model} exc={exc}")

    def _reconcile_session_config(self, external_id: str, session: dict, params: Optional[dict] = None) -> None:
        requested_model = _agent_requested_model(external_id, params)
        if requested_model and requested_model != session.get("model"):
            _log(
                f"SESSION_CONFIG_RECONCILE external={external_id} "
                f"model {session.get('model')} -> {requested_model}"
            )
            # Split into base + tier; preserve the session's existing
            # effort when the agent file carries a bare family id
            # (e.g. "swe-2" without "-max"), otherwise adopt the tier
            # encoded in the requested model id.
            _base, _tier = _split_model_id(requested_model)
            _eff = _tier or session.get("effort")
            if _eff is None:
                _fam = _family_entry(_model_family(requested_model))
                _eff = _default_effort_for_family(_fam)
            session["model"] = _resolve_model(_base, _eff, session.get("sidekick"))
            session["effort"] = _eff
            # Persist the actually-resolved sidekick — _resolve_model may
            # have substituted a valid default when the stored value isn't
            # offered by this fusion family.
            _m = _FUSION_ID_RE.match(session["model"] or "")
            if _m and _m.group("sk") != session.get("sidekick"):
                session["sidekick"] = _m.group("sk")
        requested_mode = _agent_requested_mode(params)
        if requested_mode and requested_mode != DEFAULT_DEVIN_MODE:
            _log(f"SESSION_CONFIG_RECONCILE external={external_id} requested_mode={requested_mode} forced={DEFAULT_DEVIN_MODE}")
        session["mode"] = DEFAULT_DEVIN_MODE
        session["updatedAt"] = time.time()

    def _native_client_request_timeout(self, method: str) -> float:
        if method in ("terminal/wait_for_exit", "session/request_permission"):
            return float(max(PROMPT_TIMEOUT_SECONDS, 60))
        return 60.0

    def _forward_native_client_request(self, child: NativeChild, turn: "Turn", msg: dict, real_sid: str) -> None:
        method = msg.get("method")
        req_id = msg.get("id")
        params = dict(msg.get("params") or {})
        native_session_id = params.get("sessionId")
        if native_session_id and native_session_id != real_sid:
            _log(f"NATIVE_CLIENT_RPC_DROP method={method} reason=session_mismatch native={native_session_id} expected={real_sid}")
            return
        if native_session_id:
            params["sessionId"] = turn.external_session_id
        if method not in (
            "session/request_permission",
            "terminal/create",
            "terminal/output",
            "terminal/wait_for_exit",
            "terminal/release",
            "terminal/kill",
            "fs/read_text_file",
            "fs/write_text_file",
        ):
            _log(f"NATIVE_CLIENT_RPC_UNHANDLED method={method} id={req_id}")
            if req_id is not None:
                self._write_child_response(child, {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                })
            return
        if method == "session/request_permission":
            turn.mark_active()
            if req_id is not None:
                session = self.sessions.get(turn.external_session_id, {})
                _log(
                    "NATIVE_CLIENT_RPC_AUTO_ALLOW_PERMISSION "
                    f"child_id={req_id} external={turn.external_session_id} "
                    f"mode={session.get('mode')}"
                )
                self._write_child_response(child, {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {"outcome": {"outcome": "selected", "optionId": "allow_once"}},
                })
            return
        turn.mark_active()
        _log(f"NATIVE_CLIENT_RPC_FORWARD method={method} child_id={req_id} external={turn.external_session_id}")
        parent_res = self._send_parent_rpc(method, params, self._native_client_request_timeout(method))
        turn.mark_active()
        if "result" in parent_res:
            if method == "terminal/create":
                terminal_id = (parent_res.get("result") or {}).get("terminalId")
                if terminal_id:
                    turn.active_terminal_ids.add(terminal_id)
                    command = params.get("command")
                    if isinstance(command, str) and command:
                        turn.active_terminal_commands[terminal_id] = command
                    _log(f"TURN_TERMINAL_TRACK external={turn.external_session_id} terminal={terminal_id}")
            elif method == "terminal/release":
                terminal_id = params.get("terminalId")
                if terminal_id:
                    turn.active_terminal_ids.discard(terminal_id)
                    turn.active_terminal_commands.pop(terminal_id, None)
                    _log(f"TURN_TERMINAL_RELEASED external={turn.external_session_id} terminal={terminal_id}")
            elif method in ("terminal/wait_for_exit", "terminal/kill"):
                terminal_id = params.get("terminalId")
                if terminal_id:
                    result = parent_res.get("result") or {}
                    exit_code = result.get("exitCode")
                    signal_name = result.get("signal")
                    if method == "terminal/kill" or exit_code is not None or signal_name:
                        command = turn.active_terminal_commands.get(terminal_id)
                        turn.active_terminal_ids.discard(terminal_id)
                        turn.active_terminal_commands.pop(terminal_id, None)
                        _log(
                            "TURN_TERMINAL_DONE "
                            f"external={turn.external_session_id} terminal={terminal_id} "
                            f"method={method} exit={exit_code} signal={signal_name}"
                        )
                        if method == "terminal/kill" or signal_name:
                            self._kill_matching_terminal_command(command, terminal_id)
        child_res = {"jsonrpc": "2.0", "id": req_id}
        if "result" in parent_res:
            child_res["result"] = parent_res.get("result") or {}
        else:
            child_res["error"] = parent_res.get("error") or {
                "code": -32000,
                "message": "Parent request failed",
            }
        try:
            self._write_child_response(child, child_res)
            _log(f"NATIVE_CLIENT_RPC_RESPONSE method={method} child_id={req_id} ok={'result' in child_res}")
        except (BrokenPipeError, OSError) as exc:
            _log(f"NATIVE_CLIENT_RPC_RESPONSE_FAILED method={method} child_id={req_id} exc={exc}")
            child.shutdown = True

    def _extract_update_text(self, update: dict) -> Optional[str]:
        """Extract visible text from a content-chunk update."""
        content = update.get("content") if isinstance(update, dict) else None
        if not isinstance(content, dict):
            return None
        ctype = content.get("type")
        if ctype == "text":
            return content.get("text")
        if ctype == "resource_link":
            return content.get("title") or content.get("uri")
        if ctype == "resource":
            resource = content.get("resource") if isinstance(content.get("resource"), dict) else None
            if resource and "text" in resource:
                return resource["text"]
            return f"[resource:{resource.get('mimeType', 'binary') if resource else 'binary'}]"
        return None

    def _flush_last_thought(self, turn: Turn, as_message: bool = False) -> None:
        """Flush a buffered agent_thought_chunk.

        When as_message is True, retag it as agent_message_chunk so the final
        session/update before turn completion is never a thought bubble.
        """
        if not turn.last_thought_update:
            return
        update = turn.last_thought_update
        turn.last_thought_update = None
        if as_message:
            text = self._extract_update_text(update)
            if not text or not text.strip():
                # Drop empty/whitespace trailing thoughts; nothing to show.
                return
            turn.had_agent_message = True
            update = dict(update)
            update["sessionUpdate"] = "agent_message_chunk"
            update["content"] = {"type": "text", "text": text}
        notif = {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"sessionId": turn.external_session_id, "update": update},
        }
        self.write_parent(notif)
        self._append_history(turn.external_session_id, notif, persist=False)

    def _schedule_buffer_flush(self, turn: Turn) -> None:
        """Debounce-flush buffered message tokens so text reaches the client
        promptly even when no non-chunk event follows (e.g. a long markdown
        reply followed only by tool calls)."""
        if turn.flush_timer is not None and turn.flush_timer.is_alive():
            return
        def _do_flush():
            try:
                self._flush_message_buffer(turn)
            except Exception:
                pass
        turn.flush_timer = threading.Timer(0.4, _do_flush)
        turn.flush_timer.daemon = True
        turn.flush_timer.start()

    def _flush_message_buffer(self, turn: Turn) -> None:
        """Flush buffered agent_message_chunk tokens as a single consolidated notification."""
        if not turn.message_buffer:
            return
        full_text = "".join(turn.message_buffer)
        turn.message_buffer.clear()
        if not full_text.strip():
            return
        turn.had_agent_message = True
        notif = {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": turn.external_session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": full_text},
                },
            },
        }
        self.write_parent(notif)
        self._append_history(turn.external_session_id, notif, persist=False)

    # -----------------------------------------------------------------------
    # Session helpers
    # -----------------------------------------------------------------------

    def _get_session(self, external_id: str) -> Optional[dict]:
        with self.lock:
            return self.sessions.get(external_id)

    def _ensure_session(self, external_id: str, params: Optional[dict] = None) -> dict:
        with self.lock:
            session = self.sessions.get(external_id)
            if not session:
                _fam = _family_entry(_model_family((params or {}).get("model", "swe-2")))
                _eff = (params or {}).get("effort") or _default_effort_for_family(_fam)
                _resolved_model = _resolve_model(_model_family((params or {}).get("model", "swe-2")), _eff, (params or {}).get("sidekick"))
                # Extract the actually-resolved sidekick from the fusion model
                # id so session["sidekick"] always reflects what was used.
                _fm = _FUSION_ID_RE.match(_resolved_model or "")
                session = {
                    "externalId": external_id,
                    "realId": None,
                    "cwd": (params or {}).get("cwd", str(Path.home())),
                    "model": _resolved_model,
                    "effort": _eff,
                    "mode": DEFAULT_DEVIN_MODE,
                    "title": None,
                    "createdAt": time.time(),
                    "updatedAt": time.time(),
                }
                if _fm:
                    session["sidekick"] = _fm.group("sk")
                elif (params or {}).get("sidekick"):
                    session["sidekick"] = (params or {}).get("sidekick")
                self.sessions[external_id] = session
                _log(f"SESSION_CREATE external={external_id}")
            elif params:
                if params.get("cwd"):
                    session["cwd"] = params["cwd"]
                if params.get("model"):
                    _pm = params["model"] if params["model"].startswith("fusion/") else params["model"].split("/",1)[-1]
                    _base, _tier = _split_model_id(_pm)
                    # Preserve the current effort when the model switch
                    # doesn't carry an explicit tier (e.g. bare "swe-2").
                    _eff = _tier or session.get("effort")
                    if _eff is None:
                        _fam = _family_entry(_model_family(_pm))
                        _eff = _default_effort_for_family(_fam)
                    session["model"] = _resolve_model(_base, _eff, session.get("sidekick"))
                    session["effort"] = _eff
                    # Persist the actually-resolved sidekick — the stored
                    # value may not be offered by the new fusion family.
                    _m = _FUSION_ID_RE.match(session["model"] or "")
                    if _m and _m.group("sk") != session.get("sidekick"):
                        session["sidekick"] = _m.group("sk")
                session["mode"] = DEFAULT_DEVIN_MODE
                session["updatedAt"] = time.time()
            return session

    def _persist_session(self, external_id: str) -> None:
        session = self._get_session(external_id)
        if session:
            with_state(lambda state: state.setdefault("sessions", {}).update({external_id: session}))

    def _history_count(self, external_id: str) -> int:
        with self.lock:
            return len(self.history.get(external_id, []))

    def _native_message_count(self, real_id: Optional[str]) -> int:
        if not real_id:
            return 0
        total = 0
        for db_path in DEVIN_SESSION_DB_CANDIDATES:
            if not db_path.exists():
                continue
            try:
                # immutable=1 bypasses SQLite file locks — needed because devin
                # acp children hold write locks during session load, which would
                # otherwise block our read query and return 0 (false negative).
                with sqlite3.connect(f"file:{db_path}?immutable=1", uri=True, timeout=5) as conn:
                    row = conn.execute(
                        "select count(*) from message_nodes where session_id = ?",
                        (real_id,),
                    ).fetchone()
                    total += int(row[0] or 0) if row else 0
            except Exception as exc:
                _log(f"NATIVE_MESSAGE_COUNT error real={real_id} db={db_path}: {exc}")
        return total

    def _requires_real_resume(self, external_id: str, session: dict) -> bool:
        """Return True when creating a fresh native session would fake continuity."""
        if self._native_message_count(session.get("realId")) > 0:
            return True
        with self.lock:
            history = list(self.history.get(external_id, []))
        meaningful_history = []
        for event in history:
            update = event.get("params", {}).get("update", {}) if isinstance(event, dict) else {}
            if update.get("sessionUpdate") != "user_message_chunk":
                meaningful_history.append(event)
        if meaningful_history:
            return True
        if len(history) > 1:
            return True
        return False

    def _continuity_error(self, external_id: str, session: dict, reason: str) -> str:
        real_id = session.get("realId") or "<missing>"
        history_count = self._history_count(external_id)
        native_count = self._native_message_count(session.get("realId"))
        return (
            "Cannot safely resume this Devin conversation. "
            f"Paseo session {external_id} is mapped to native Devin session {real_id}, "
            f"but native resume failed ({reason}). "
            f"Refusing to create a fresh Devin session because it would detach "
            f"{history_count} saved Paseo history events and {native_count} native Devin messages "
            "from the model's real context."
        )

    # -----------------------------------------------------------------------
    # History helpers
    # -----------------------------------------------------------------------

    def _append_history(self, external_session_id: str, notification: dict, persist: bool = True) -> None:
        with self.lock:
            buf = self.history.setdefault(external_session_id, [])
            buf.append(notification)
            if len(buf) > 500:
                self.history[external_session_id] = buf[-400:]
            if persist:
                # Save only this session's history to its own file.
                # This avoids the concurrent-write race condition where
                # multiple stdio processes would overwrite each other's
                # data in a shared history.json.
                save_session_history(external_session_id, self.history[external_session_id])

    def _replay_history(self, external_session_id: str, force: bool = False) -> None:
        """Replay buffered session/update notifications to paseo.

        Only replays once per process lifetime unless force=True.
        This must be called AFTER sending the session/load result, because
        paseo drops session/update notifications that arrive before the
        session/load RPC result is returned.
        """
        if not force and external_session_id in self._replayed_sessions:
            _log(f"REPLAY_HISTORY_SKIP external={external_session_id} already replayed")
            return
        with self.lock:
            # Try in-memory cache first; fall back to per-session file
            buf = self.history.get(external_session_id)
            if buf is None:
                buf = load_session_history(external_session_id)
                if buf:
                    self.history[external_session_id] = buf
            # If still no history, auto-recover from supervisor.log. This
            # restores sessions that lost their history file to the legacy
            # shared history.json concurrent-write race. Guard with a set so
            # we only attempt the (relatively expensive) log scan once per
            # session per process lifetime.
            if not buf and external_session_id not in self._history_recover_attempted:
                self._history_recover_attempted.add(external_session_id)
                recovered = recover_history_from_log(external_session_id)
                if recovered:
                    buf = load_session_history(external_session_id)
                    if buf:
                        self.history[external_session_id] = buf
            # If still no history, try recovering from Devin's sessions.db.
            # This is the most complete method — it extracts all user and
            # assistant messages including tool calls from the native DB.
            if not buf:
                session = self.sessions.get(external_session_id, {})
                real_id = session.get("realId")
                if real_id:
                    _log(f"HISTORY_DB_RECOVER attempting external={external_session_id} real={real_id}")
                    recovered = recover_history_from_db(external_session_id, real_id)
                    if recovered:
                        buf = load_session_history(external_session_id)
                        if buf:
                            self.history[external_session_id] = buf
            for notif in buf:
                _p = notif.get("params", {})
                if isinstance(_p, dict) and _p.get("sessionId") != external_session_id:
                    notif = dict(notif)
                    notif["params"] = dict(_p)
                    notif["params"]["sessionId"] = external_session_id
                self.write_parent(notif)
            self._replayed_sessions.add(external_session_id)
            _log(f"REPLAY_HISTORY external={external_session_id} count={len(buf)}")

    # -----------------------------------------------------------------------
    # Cache refresh
    # -----------------------------------------------------------------------

    def _refresh_cache(self) -> None:
        if cache_valid():
            return
        _log("CACHE_REFRESH start")
        # Model table comes from `devin models list` (real variant ids,
        # incl. MODEL_* private names and fusion combos); static list is
        # only a fallback. Modes stay static — ACP doesn't expose them.
        models = _fetch_live_models() or DEVIN_MODELS
        modes = DEVIN_MODES
        with_cache(lambda cache: cache.update({
            "models": models,
            "modes": modes,
            "cached_at": time.time(),
        }))
        _log(f"CACHE_REFRESH ok models={len(models)} modes={len(modes)}")

    def _send_rpc(self, child: NativeChild, req: dict, timeout: float = 30.0) -> dict:
        body = json.dumps(req, separators=(",", ":")) + "\n"
        q = queue.Queue()
        child.pending_rpc[req["id"]] = q
        try:
            with child.stdin_lock:
                child.proc.stdin.write(body)
                child.proc.stdin.flush()
            return q.get(timeout=timeout)
        except queue.Empty:
            _log(f"RPC_TIMEOUT id={req['id']} method={req.get('method')} timeout={timeout}")
            return {"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32000, "message": f"Proxy request timeout after {timeout}s"}}
        except (BrokenPipeError, OSError) as exc:
            _log(f"RPC_BROKEN_PIPE id={req['id']} pid={child.proc.pid} exc={exc}")
            child.shutdown = True
            return {"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32003, "message": f"Child process died: {exc}"}}
        finally:
            child.pending_rpc.pop(req["id"], None)


    # -----------------------------------------------------------------------
    # Native prompt execution
    # -----------------------------------------------------------------------

    def _rearm_native_loop(self, external_id: str, session: dict) -> None:
        """Re-initialize the native agent loop when the model family changed
        since the loop was armed.

        Fusion's sidekick tool and pairing attach at agent-loop init
        (session/new or session/load), keyed to the model in effect at that
        moment. A mid-loop set_config_option only swaps the model id — the
        loop keeps its originally-armed toolset, so a session loaded as swe-2
        never grows a sidekick after switching to fusion (and a fusion-armed
        loop keeps stale fusion framing after switching away). Killing the
        bound child makes the load-before-prompt path below rebuild the loop
        on a fresh process; history is preserved in sessions.db.
        """
        real_sid = session.get("realId")
        if not real_sid or self._mode_preference != "native":
            return
        want = _loop_family(session)
        have = session.get("loop_family")
        if have == want:
            return
        bound = None
        with self.pool.lock:
            for c in self.pool.children:
                if c.real_session_id == real_sid and self.pool._is_alive(c):
                    bound = c
                    break
        if bound is not None:
            _log(
                f"REARM_LOOP external={external_id} real={real_sid} "
                f"pid={bound.proc.pid} family {have or 'unknown'} -> {want}"
            )
            self.pool.mark_dying(bound)
            stale_lock_cleanup(real_sid, reclaim_orphan=True, reclaim_sibling=True)
        # Force re-record at the load-before-prompt init below. When no child
        # is bound, that path initializes the loop fresh anyway.
        session["loop_family"] = None

    def _execute_native_prompt(self, turn: Turn, session: dict) -> bool:
        """Execute a prompt using native ACP mode. Returns True on success."""
        if turn.is_cancelled():
            _log(f"NATIVE_PROMPT_CANCELLED_BEFORE_START external={turn.external_session_id}")
            return False
        real_sid = session.get("realId")
        # Acquire the per-cwd MCP config lock for the write+session_init
        # sequence. .devin/mcp_config.local.json is shared across all
        # agents in the same cwd; a sibling agent's write between our
        # write and the native child's session_init read would give the
        # child the wrong callerAgentId. Holding the lock through
        # session_init eliminates that race.
        with _mcp_config_lock(session.get("cwd")):
            # Re-write this agent's MCP config before any native child
            # (re-)spawn. Writing inside the lock ensures no sibling can
            # overwrite it before our session_init reads it.
            _write_devin_mcp_config(session.get("cwd"), session.get("mcp_servers"))
            # If no realId, create a fresh native session first
            if not real_sid:
                if self._requires_real_resume(turn.external_session_id, session):
                    msg = self._continuity_error(turn.external_session_id, session, "missing native session id")
                    _log(f"NATIVE_PROMPT_CONTINUITY_REFUSED external={turn.external_session_id} reason=missing_realId")
                    turn.fail_with_message(msg)
                    return False
                _log(f"NATIVE_PROMPT no realId, creating fresh for external={turn.external_session_id}")
                child = self.pool.acquire()
                try:
                    new_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/new", "params": {"cwd": session.get("cwd", str(Path.home())), "mcpServers": []}}
                    new_res = self._send_rpc(child, new_req, timeout=30)
                    if "result" in new_res:
                        real_sid = new_res["result"]["sessionId"]
                        session["realId"] = real_sid
                        child.real_session_id = real_sid
                        self._force_native_model(child, real_sid, session.get("model"), session.get("effort"), session.get("sidekick"))
                        self._force_native_mode(child, real_sid, DEFAULT_DEVIN_MODE)
                        session["loop_family"] = _loop_family(session)
                        self._persist_session(turn.external_session_id)
                    else:
                        _log(f"NATIVE_PROMPT fresh create failed: {new_res.get('error')}")
                        self.pool.release(child)
                        turn.mark_failed()
                        return False
                    self.pool.release(child, real_sid)
                except Exception as exc:
                    _log(f"NATIVE_PROMPT fresh create error: {exc}")
                    self.pool.release(child)
                    turn.mark_failed()
                    return False
            self._rearm_native_loop(turn.external_session_id, session)
            child = self.pool.acquire(real_sid)
            if turn.is_cancelled():
                _log(f"NATIVE_PROMPT_CANCELLED_AFTER_ACQUIRE external={turn.external_session_id} pid={child.proc.pid}")
                self.pool.release(child, child.real_session_id)
                return False
            if child.real_session_id != real_sid:
                _log(
                    f"NATIVE_PROMPT_LOAD_BEFORE_PROMPT external={turn.external_session_id} "
                    f"real={real_sid} pid={child.proc.pid} previous={child.real_session_id}"
                )
                stale_lock_cleanup(real_sid, reclaim_orphan=True, reclaim_sibling=True)
                load_req = {
                    "jsonrpc": "2.0",
                    "id": self.next_id(),
                    "method": "session/load",
                    "params": {
                        "sessionId": real_sid,
                        "cwd": session.get("cwd", str(Path.home())),
                        "mcpServers": [],
                    },
                }
                load_res = self._send_rpc(child, load_req, timeout=SESSION_LOAD_TIMEOUT_SECONDS)
                if "result" in load_res:
                    child.real_session_id = real_sid
                    self._force_native_model(child, real_sid, session.get("model"), session.get("effort"), session.get("sidekick"))
                    self._force_native_mode(child, real_sid, DEFAULT_DEVIN_MODE)
                    session["loop_family"] = _loop_family(session)
                    _log(
                        f"NATIVE_PROMPT_LOAD_BEFORE_PROMPT_OK external={turn.external_session_id} "
                        f"real={real_sid} pid={child.proc.pid}"
                    )
                else:
                    err_msg = load_res.get("error", {}).get("message", "session/load failed")
                    _log(
                        f"NATIVE_PROMPT_LOAD_BEFORE_PROMPT_ERROR external={turn.external_session_id} "
                        f"real={real_sid} pid={child.proc.pid} msg={err_msg}"
                    )
                    self.pool.mark_dying(child)
                    turn.mark_failed()
                    return False
        turn.child = child
        self._force_native_model(child, real_sid, session.get("model"), session.get("effort"), session.get("sidekick"))
        self._force_native_mode(child, real_sid, DEFAULT_DEVIN_MODE)
        if turn.is_cancelled():
            _log(f"NATIVE_PROMPT_CANCELLED_AFTER_CHILD external={turn.external_session_id} pid={child.proc.pid}")
            self._cancel_turn(turn.external_session_id, turn, reason="late_cancel_child_attached")
            return False

        def notif_handler(msg: dict):
            if msg.get("method") != "session/update":
                # _cognition.ai/* notifications (compaction, agent_stopped, etc.)
                # indicate the model is actively working. Reset the idle timer
                # so the watchdog doesn't kill the turn during long compaction.
                if isinstance(msg.get("method"), str) and msg["method"].startswith("_cognition.ai/"):
                    turn.mark_active()
                    _log(f"NATIVE_ACTIVITY_SIGNAL external={turn.external_session_id} method={msg['method']}")
                threading.Thread(
                    target=self._forward_native_client_request,
                    args=(child, turn, msg, real_sid),
                    daemon=True,
                ).start()
                return
            params = msg.get("params", {})
            update = params.get("update", {})
            real_session_id = params.get("sessionId")
            if real_session_id != real_sid:
                return
            # Skip empty updates — Devin sends these periodically but Paseo
            # rejects them with schema validation errors (-32602)
            if not update or not isinstance(update, dict) or not update.get("sessionUpdate"):
                return
            turn.mark_active()
            if turn.first_update_at is None:
                turn.first_update_at = time.time()
            # Cache the latest usage_update so it can be attached to the
            # session/prompt result (ACP `usage` field) — some clients only
            # read usage from the prompt response.
            if update.get("sessionUpdate") == "usage_update":
                _sess = self._get_session(turn.external_session_id)
                if _sess is not None:
                    _sess["lastUsage"] = dict(update)
                    # Accumulate per-turn totals — Devin emits one
                    # usage_update per model call (sometimes duplicated);
                    # dedupe identical consecutive reports.
                    _meta = update.get("_meta") or {}
                    _key = (
                        _meta.get("cognition.ai/inputTokens"),
                        _meta.get("cognition.ai/outputTokens"),
                        _meta.get("cognition.ai/cachedReadTokens"),
                        update.get("used"),
                    )
                    if _key not in turn.usage_seen and any(v is not None for v in _key):
                        turn.usage_seen.add(_key)
                        # Devin tags delegated-model requests with
                        # subagent_context.parentAgentId ("sidekick" for the
                        # fusion sidekick, a hex id for spawned subagents,
                        # "root"/absent for the lead). Keep lead vs delegated
                        # consumption separate for the stats card.
                        _sub_id = (_meta.get("cognition.ai/subagent_context") or {}).get("parentAgentId")
                        _buckets = [turn.usage_totals]
                        if _sub_id and _sub_id != "root":
                            _buckets = [
                                turn.usage_sub_totals.setdefault(
                                    _sub_id,
                                    {"input": 0, "output": 0, "cached": 0, "requests": 0},
                                )
                            ]
                        _buckets.append(
                            _sess.setdefault(
                                "usageTotals",
                                {"input": 0, "output": 0, "cached": 0, "requests": 0},
                            )
                        )
                        for _totals in _buckets:
                            _totals["input"] += _key[0] or 0
                            _totals["output"] += _key[1] or 0
                            _totals["cached"] += _key[2] or 0
                            _totals["requests"] += 1
            # Track tool results for grounding guardrail.
            # Only track on terminal status (completed/failed), not in_progress,
            # which is an intermediate chunk that may carry no content.
            if update.get("sessionUpdate") == "tool_call_update":
                status = update.get("status", "")
                if status in ("completed", "failed"):
                    # Native ACP sends content as an array, not rawOutput.shortPreview.
                    # Extract text from the content array for the preview.
                    preview = ""
                    content_arr = update.get("content", [])
                    if isinstance(content_arr, list):
                        for item in content_arr:
                            if isinstance(item, dict):
                                inner = item.get("content", item)
                                if isinstance(inner, dict) and inner.get("type") == "text":
                                    preview = inner.get("text", "")
                                    break
                    turn.latest_tool_result = ToolResult(
                        name=update.get("title", "tool"),
                        status=status,
                        is_empty=not preview.strip(),
                        output_preview=preview,
                    )
            # Devin pushes config_option_update with only its own options
            # (typically 2 items) which would REPLACE the session's
            # configOptions in the Paseo adapter and drop our synthetic
            # model/mode/effort selectors — re-merge them before forwarding.
            if update.get("sessionUpdate") == "config_option_update":
                _sess = self._get_session(turn.external_session_id)
                if _sess is not None:
                    update["configOptions"] = _merge_structured_config_options(
                        update.get("configOptions"),
                        _sess.get("model", "swe-2-medium"),
                        _sess.get("mode", DEFAULT_DEVIN_MODE),
                        feature_values=_sess.get("featureValues"),
                        sidekick=_sess.get("sidekick"),
                    )
            # Buffer the last thought chunk so we can ensure the final
            # session/update before turn completion is never a thinking bubble.
            if update.get("sessionUpdate") == "agent_thought_chunk":
                self._flush_message_buffer(turn)
                self._flush_last_thought(turn, as_message=False)
                turn.last_thought_update = dict(update)
                content = update.get("content", {})
                thought_text = content.get("text", "") if isinstance(content, dict) else ""
                turn.thinking_chars += len(thought_text)
                return
            self._flush_last_thought(turn, as_message=False)
            if update.get("sessionUpdate") == "agent_message_chunk":
                # Buffer tokens; flush as one consolidated message when a
                # different event type arrives, on a short debounce timer,
                # or at turn completion.
                content = update.get("content", {})
                text = content.get("text", "") if isinstance(content, dict) else ""
                if text:
                    turn.had_agent_message = True
                    turn.message_buffer.append(text)
                    self._schedule_buffer_flush(turn)
                return
            # Non-chunk event: flush any buffered message tokens first
            self._flush_message_buffer(turn)
            # Route Devin subagent activity into a custom sessionUpdate so the
            # Paseo adapter can render provider_subagent cards instead of flat
            # tool calls. Devin marks subagent lifecycle via
            # _meta.cognition.ai/subagent_(started|completed|failed) and marks
            # the subagent's own tool calls via subagent_context.parentAgentId
            # (the subagent's id).
            meta = update.get("_meta") or {}
            sub_started = meta.get("cognition.ai/subagent_started")
            sub_completed = meta.get("cognition.ai/subagent_completed")
            sub_failed = meta.get("cognition.ai/subagent_failed")
            sub_ctx = meta.get("cognition.ai/subagent_context") or {}
            if sub_started or sub_completed or sub_failed:
                info = sub_started or sub_completed or sub_failed
                sub_id = info.get("agentId") or update.get("toolCallId")
                if sub_id:
                    _sess3 = self._get_session(turn.external_session_id)
                    if _sess3 is not None:
                        _names = _sess3.setdefault("subagentNames", {})
                        _names.setdefault(
                            sub_id,
                            info.get("title") or info.get("profile") or sub_id,
                        )
                    if sub_started:
                        status = "running"
                    elif sub_failed:
                        status = "failed"
                    else:
                        status = "completed" if info.get("success", True) else "failed"
                    if not sub_started:
                        # Subagent finished: Devin never sends completion
                        # updates for its internal tool calls, so synthesize
                        # one for the last still-running call.
                        _pend = getattr(turn, "subagent_pending_tool", None)
                        _last = _pend.pop(sub_id, None) if _pend else None
                        if _last:
                            self.write_parent({
                                "jsonrpc": "2.0",
                                "method": "session/update",
                                "params": {"sessionId": turn.external_session_id, "update": {
                                    "sessionUpdate": "tool_call_update",
                                    "toolCallId": _last,
                                    "status": "completed",
                                    "_meta": {"paseo/subagentTimeline": sub_id},
                                }},
                            })
                    payload = {
                        "id": sub_id,
                        # Paseo row-label contract: `title` is the subagent
                        # type (profile), `description` carries the task.
                        "title": info.get("profile") or info.get("title"),
                        "description": info.get("task") or info.get("title"),
                        "subtitle": info.get("summary") or info.get("model"),
                        "status": status,
                    }
                    # Bind to the most recent in-progress run_subagent tool
                    # call so the card anchors where the spawn call appears.
                    pending_spawn = getattr(turn, "pending_run_subagent_call", None)
                    if pending_spawn:
                        payload["toolCallId"] = pending_spawn
                        turn.pending_run_subagent_call = None
                    update = dict(update)
                    update_meta = dict(meta)
                    update_meta["paseo/subagent"] = payload
                    update["_meta"] = update_meta
            elif (
                (sub_ctx.get("parentAgentId") or "root") != "root"
                or meta.get("cognition.ai/sidekick")
                or str(update.get("toolCallId") or "").startswith("sk::")
            ):
                sub_id_ctx = sub_ctx.get("parentAgentId")
                if not sub_id_ctx or sub_id_ctx == "root":
                    # Fusion sidekick tool calls carry only the
                    # cognition.ai/sidekick marker (and sk::* ids), no
                    # subagent_context — attribute them to the sidekick card.
                    sub_id_ctx = "sidekick"
                # Fusion sidekick (and any subagent activity lacking a
                # started event) needs an explicit card upsert — otherwise
                # the timeline-routed tool calls have no card to live in.
                if sub_id_ctx != "root" and sub_id_ctx not in turn.subagent_cards:
                    turn.subagent_cards.add(sub_id_ctx)
                    _sess2 = self._get_session(turn.external_session_id)
                    _sk_model = (_sess2 or {}).get("sidekick") if sub_id_ctx == "sidekick" else None
                    self.send_notification(turn.external_session_id, {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": f"subcard-{sub_id_ctx}",
                        "status": "in_progress",
                        "_meta": {"paseo/subagent": {
                            "id": sub_id_ctx,
                            "title": "Sidekick" if sub_id_ctx == "sidekick" else "Subagent",
                            "description": "Delegated work" if sub_id_ctx == "sidekick" else None,
                            "subtitle": _sk_model,
                            "status": "running",
                        }},
                    })
                update = dict(update)
                update_meta = dict(meta)
                update_meta["paseo/subagentTimeline"] = sub_id_ctx
                update["_meta"] = update_meta
                # Devin streams subagent-internal tool calls live but with no
                # status and no completion updates. Show the latest call as
                # in_progress; when the next call for the same subagent
                # arrives, the previous one is implicitly finished.
                if update.get("sessionUpdate") == "tool_call":
                    _pend = getattr(turn, "subagent_pending_tool", None)
                    if _pend is None:
                        _pend = turn.subagent_pending_tool = {}
                    _tid0 = update.get("toolCallId")
                    _prev = _pend.get(sub_id_ctx)
                    if _prev and _prev != _tid0:
                        self.write_parent({
                            "jsonrpc": "2.0",
                            "method": "session/update",
                            "params": {"sessionId": turn.external_session_id, "update": {
                                "sessionUpdate": "tool_call_update",
                                "toolCallId": _prev,
                                "status": "completed",
                                "_meta": {"paseo/subagentTimeline": sub_id_ctx},
                            }},
                        })
                    update["status"] = "in_progress"
                    if _tid0:
                        _pend[sub_id_ctx] = _tid0
            # Track the in-progress run_subagent spawn call so a following
            # subagent_started can anchor to it.
            if update.get("sessionUpdate") == "tool_call" and "run_subagent" in update.get("toolCallId", ""):
                turn.pending_run_subagent_call = update.get("toolCallId")
            # Completion/other updates for a subagent's tool calls may lack
            # subagent_context — route by remembered toolCallId → subagent.
            _tid = update.get("toolCallId")
            _sub_map = getattr(turn, "subagent_tool_calls", None)
            if _sub_map is None:
                _sub_map = turn.subagent_tool_calls = {}
            if "_meta" in update and isinstance(update.get("_meta"), dict) and update["_meta"].get("paseo/subagentTimeline") and _tid:
                _sub_map[_tid] = update["_meta"]["paseo/subagentTimeline"]
            elif _tid and _tid in _sub_map and not (isinstance(update.get("_meta"), dict) and update["_meta"].get("paseo/subagent")):
                update = dict(update)
                update_meta = dict(update.get("_meta") or {})
                update_meta["paseo/subagentTimeline"] = _sub_map[_tid]
                update["_meta"] = update_meta
            # An explicit terminal update for a subagent tool call clears the
            # implicit-pending slot so we don't double-complete it later.
            if update.get("sessionUpdate") == "tool_call_update" and _tid and update.get("status") in ("completed", "failed"):
                _pend = getattr(turn, "subagent_pending_tool", None)
                if _pend:
                    for _k, _v in list(_pend.items()):
                        if _v == _tid:
                            _pend.pop(_k, None)
            # Inject supervisor-local commands into the advertised command
            # list so they appear in the client's command menu.
            if update.get("sessionUpdate") == "available_commands_update":
                update = dict(update)
                existing = {c.get("name") for c in update.get("availableCommands", []) if isinstance(c, dict)}
                for extra in (
                    {"name": "steps", "description": "List revertible steps", "input": None},
                    {"name": "revert", "description": "Rewind conversation and revert file changes to before a step", "input": {"hint": "<step> [force]"}},
                    {"name": "fork", "description": "Fork the session from a step into a new session", "input": {"hint": "<step>"}},
                ):
                    if extra["name"] not in existing:
                        update.setdefault("availableCommands", []).append(extra)
            _hm = dict(msg)
            _hp = dict(msg.get("params", {}))
            _hp["sessionId"] = turn.external_session_id
            _hp["update"] = update
            _hm["params"] = _hp
            self._append_history(turn.external_session_id, _hm, persist=False)
            # Forward to parent with external session id
            self.write_parent({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {"sessionId": turn.external_session_id, "update": update},
            })

        child.notif_handlers.append(notif_handler)
        try:
            # Forward the full ContentBlock array (text + image + resource)
            # to native Devin. Fall back to a text-only block if the upstream
            # prompt had no blocks (e.g. legacy callers).
            if turn.prompt_blocks:
                prompt_blocks = list(turn.prompt_blocks)
            else:
                prompt_blocks = [{"type": "text", "text": turn.prompt_text}]
            req = {
                "jsonrpc": "2.0",
                "id": turn.native_request_id,
                "method": "session/prompt",
                "params": {"sessionId": real_sid, "prompt": prompt_blocks},
            }
            _log(f"NATIVE_PROMPT_START id={turn.native_request_id} real={real_sid} pid={child.proc.pid}")
            if not turn.mark_typing():
                _log(f"NATIVE_PROMPT_CANCELLED_BEFORE_SEND id={turn.native_request_id} external={turn.external_session_id}")
                return False
            # Activity-aware polling: check queue every 1s so we can detect
            # child death, cancellation, or timeout promptly.
            q = queue.Queue()
            child.pending_rpc[turn.native_request_id] = q
            try:
                body = json.dumps(req, separators=(",", ":")) + "\n"
                with child.stdin_lock:
                    child.proc.stdin.write(body)
                    child.proc.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                _log(f"NATIVE_PROMPT_BROKEN_PIPE id={turn.native_request_id} exc={exc}")
                turn.mark_failed()
                return False
            result = None
            while True:
                try:
                    result = q.get(timeout=1)
                    break
                except queue.Empty:
                    if turn.phase not in (TurnPhase.TYPING, TurnPhase.ACTIVE, TurnPhase.COMPLETING):
                        _log(f"NATIVE_PROMPT_TURN_ENDED id={turn.native_request_id} phase={turn.phase.value}")
                        return False
                    if child.proc.poll() is not None:
                        _log(f"NATIVE_PROMPT_CHILD_DIED id={turn.native_request_id} rc={child.proc.returncode}")
                        turn.mark_failed()
                        return False
            if result is None:
                turn.mark_failed()
                return False
            if "error" in result:
                err = result["error"]
                msg = err.get("message", "Unknown error")
                _log(f"NATIVE_PROMPT_ERROR id={turn.native_request_id} msg={msg}")
                # Rate-limit errors are not retryable — every auto-resume
                # restart sends a new API request that hits the same limit,
                # burning through all retries in seconds with no chance of
                # success. Report the error and stop retrying.
                if "rate limit" in msg.lower():
                    turn.fail_with_message(msg)
                    return False
                # If session not found, try to resume from sessions.db. For a persisted
                # conversation, never replace the native session with a fresh one: that
                # makes Paseo history visible but unavailable to the model.
                if "not found" in msg.lower() or "unknown session" in msg.lower() or "already open" in msg.lower():
                    _log(f"NATIVE_PROMPT session lost, attempting resume for external={turn.external_session_id}")
                    self.pool.mark_dying(child)
                    # Re-acquire the per-cwd MCP config lock for the recovery
                    # session_init — a sibling agent may have overwritten the
                    # config since our initial write.
                    with _mcp_config_lock(session.get("cwd")):
                        _write_devin_mcp_config(session.get("cwd"), session.get("mcp_servers"))
                        # Try stale lock cleanup + session/load to resume from sessions.db.
                        # reclaim_orphan=True to reclaim locks held by a live devin acp
                        # child of a previous supervisor instance.
                        stale_lock_cleanup(real_sid, reclaim_orphan=True, reclaim_sibling=True)
                        resume_child = self.pool.acquire()
                        try:
                            load_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/load", "params": {"sessionId": real_sid, "cwd": session.get("cwd", str(Path.home())), "mcpServers": []}}
                            load_res = self._send_rpc(resume_child, load_req, timeout=SESSION_LOAD_TIMEOUT_SECONDS)
                            if "result" in load_res:
                                _log(f"NATIVE_PROMPT RESUMED from sessions.db for external={turn.external_session_id} real={real_sid}")
                                self._force_native_model(resume_child, real_sid, session.get("model"), session.get("effort"), session.get("sidekick"))
                                self._force_native_mode(resume_child, real_sid, DEFAULT_DEVIN_MODE)
                                session["loop_family"] = _loop_family(session)
                                self.pool.release(resume_child, real_sid)
                                turn.mark_failed()
                                return False
                            self.pool.release(resume_child)
                        except Exception as exc_resume:
                            _log(f"NATIVE_PROMPT resume attempt error: {exc_resume}")
                            self.pool.release(resume_child)
                        if not self._requires_real_resume(turn.external_session_id, session):
                            _log(f"NATIVE_PROMPT fresh retry allowed external={turn.external_session_id} old_real={real_sid} reason={msg}")
                            fresh_child = self.pool.acquire()
                            try:
                                new_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/new", "params": {"cwd": session.get("cwd", str(Path.home())), "mcpServers": []}}
                                new_res = self._send_rpc(fresh_child, new_req, timeout=30)
                                if "result" in new_res:
                                    new_real_sid = new_res["result"]["sessionId"]
                                    session["realId"] = new_real_sid
                                    fresh_child.real_session_id = new_real_sid
                                    self._force_native_model(fresh_child, new_real_sid, session.get("model"), session.get("effort"), session.get("sidekick"))
                                    self._force_native_mode(fresh_child, new_real_sid, DEFAULT_DEVIN_MODE)
                                    session["loop_family"] = _loop_family(session)
                                    self._persist_session(turn.external_session_id)
                                    self.pool.release(fresh_child, new_real_sid)
                                    turn.mark_failed()
                                    return False
                                _log(f"NATIVE_PROMPT fresh retry create failed external={turn.external_session_id}: {new_res.get('error')}")
                                self.pool.release(fresh_child)
                            except Exception as exc_fresh:
                                _log(f"NATIVE_PROMPT fresh retry error external={turn.external_session_id}: {exc_fresh}")
                                self.pool.release(fresh_child)
                    msg = self._continuity_error(turn.external_session_id, session, msg)
                    _log(f"NATIVE_PROMPT_CONTINUITY_REFUSED external={turn.external_session_id} real={real_sid}")
                    turn.fail_with_message(msg)
                    return False
                # Retryable on child death
                if "died" in msg.lower() or "broken pipe" in msg.lower() or "terminated by signal" in msg.lower():
                    turn.mark_failed()
                else:
                    turn.mark_failed()
                return False
            turn.mark_completing()
            # Flush any trailing thought as an assistant message so the iPad client
            # never sees a thinking bubble as the last item of a completed turn.
            self._flush_message_buffer(turn)
            self._flush_last_thought(turn, as_message=True)
            # Apply grounding guardrail: only fire on genuine tool failure.
            # Empty content is normal for many successful tool calls (file edits,
            # mkdir, etc.) and in native mode Devin's backend already sees full
            # tool results, so empty-but-completed is not a reliable failure signal.
            if turn.latest_tool_result and not turn.grounding_appended:
                if turn.latest_tool_result.status == "failed":
                    self.send_notification(turn.external_session_id, {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": f"[!CAUTION] The latest tool call ({turn.latest_tool_result.name}) failed. Please verify any success claims."},
                    })
                    turn.had_agent_message = True
                    turn.grounding_appended = True
            if not turn.had_agent_message:
                _log(f"NATIVE_PROMPT_EMPTY_TURN id={turn.native_request_id} external={turn.external_session_id}")
                turn.mark_failed()
                return False
            turn.mark_completed()
            _log(f"NATIVE_PROMPT_OK id={turn.native_request_id}")
            # Record clientMessageId → native userMessageId so rewind requests
            # (which carry the timeline messageId) can be resolved to a native
            # revert step via _cognition.ai/revert/listSteps.
            try:
                _meta = (result.get("result") or {}).get("_meta") or {}
                _native_umid = _meta.get("cognition.ai/userMessageId")
                if _native_umid and turn.client_message_id:
                    _s = self._get_session(turn.external_session_id)
                    if _s is not None:
                        _s.setdefault("user_msg_map", {})[turn.client_message_id] = _native_umid
                        self._persist_session(turn.external_session_id)
            except Exception:
                pass
            return True
        except Exception as exc:
            _log(f"NATIVE_PROMPT_EXCEPTION id={turn.native_request_id} exc={exc}")
            turn.mark_failed()
            return False
        finally:
            try:
                child.notif_handlers.remove(notif_handler)
            except ValueError:
                pass
            # If the turn did not complete, flush any buffered thought as
            # reasoning so it is not lost; on completion it was already flushed
            # as an assistant message.
            self._flush_message_buffer(turn)
            self._flush_last_thought(turn, as_message=False)
            # Persist history once after the turn (not on every token chunk).
            # Save only this session's history to its per-session file.
            try:
                save_session_history(turn.external_session_id, self.history.get(turn.external_session_id, []))
            except Exception:
                pass
            if turn.phase in (TurnPhase.COMPLETED, TurnPhase.CANCELLED, TurnPhase.TIMED_OUT):
                if turn.phase == TurnPhase.COMPLETED:
                    self.pool.release(child, real_sid)
                else:
                    self.pool.mark_dying(child)
            elif turn.phase == TurnPhase.FAILED:
                self.pool.mark_dying(child)

    # -----------------------------------------------------------------------
    # Print-mode prompt execution
    # -----------------------------------------------------------------------

    def _execute_print_prompt(self, turn: Turn, session: dict) -> bool:
        """Execute a prompt using print-mode (devin -p + export polling)."""
        if turn.is_cancelled():
            _log(f"PRINT_PROMPT_CANCELLED_BEFORE_START external={turn.external_session_id}")
            return False
        # Print mode uses `devin -p <text>` which cannot accept image or
        # resource content blocks. Warn if any were sent so the operator knows
        # the model will not see them; the text portion is still forwarded.
        if any(b.get("type") != "text" for b in turn.prompt_blocks):
            non_text = sum(1 for b in turn.prompt_blocks if b.get("type") != "text")
            _log(f"PRINT_PROMPT_IMAGE_DROPPED external={turn.external_session_id} non_text_blocks={non_text} (print mode cannot forward non-text content)")
        export_path = EXPORT_DIR / f"{turn.external_session_id}-{uuid.uuid4().hex}.json"
        cwd = session.get("cwd", str(Path.home()))
        cmd = [REAL_DEVIN, "--config", DEVIN_CONFIG, "--permission-mode", PRINT_PERMISSION_MODE, "--export", str(export_path)]
        if session.get("model"):
            cmd.extend(["--model", session["model"]])
        if session.get("realId"):
            cmd.extend(["-r", session["realId"]])
        cmd.extend(["-p", turn.prompt_text])
        _log(f"PRINT_PROMPT_START cmd={cmd} cwd={cwd}")
        if not turn.mark_typing():
            _log(f"PRINT_PROMPT_CANCELLED_BEFORE_SPAWN external={turn.external_session_id}")
            return False
        proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        streamer = DevinExportStreamer(turn.external_session_id, export_path, turn.prompt_text)
        stderr_deque = collections.deque(maxlen=200)

        def drain_stderr():
            try:
                for line in proc.stderr:
                    stderr_deque.append(line.rstrip())
            except Exception:
                pass
        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()

        idle_deadline = time.time() + PROMPT_TIMEOUT_SECONDS
        timed_out = False
        last_step_count = 0
        try:
            while proc.poll() is None:
                transcript = streamer.poll()
                if transcript and isinstance(transcript, dict):
                    steps = transcript.get("steps") or []
                    current_step_count = len(steps)
                    if current_step_count > last_step_count:
                        last_step_count = current_step_count
                        idle_deadline = time.time() + PROMPT_TIMEOUT_SECONDS
                        turn.mark_active()
                    # Stream new steps as notifications
                    self._stream_print_steps(turn, streamer, transcript)
                if time.time() > idle_deadline:
                    timed_out = True
                    _log(f"PRINT_PROMPT_TIMEOUT id={turn.upstream_request_id}")
                    self._stop_proc(proc)
                    break
                time.sleep(EXPORT_POLL_INTERVAL_SECONDS)
            stderr_thread.join(timeout=2)
            # Final poll
            final_transcript = streamer.poll()
            if final_transcript:
                self._stream_print_steps(turn, streamer, final_transcript)
            if timed_out:
                turn.mark_timed_out("idle")
                return False
            if proc.returncode != 0:
                stderr_text = "\n".join(stderr_deque)
                _log(f"PRINT_PROMPT_FAIL rc={proc.returncode} stderr={stderr_text[-500:]}")
                turn.mark_failed()
                return False
            if not export_path.exists():
                turn.mark_failed()
                return False
            # Extract final message and local session id
            transcript = final_transcript or _load_json(export_path, {})
            if transcript:
                local_sid = transcript.get("session_id")
                if local_sid:
                    session["realId"] = local_sid
                for step in transcript.get("steps", []):
                    if step.get("source") == "user" and isinstance(step.get("message"), str):
                        session["title"] = step["message"].strip().splitlines()[0][:120]
                    if step.get("source") == "agent" and isinstance(step.get("message"), str):
                        turn.final_message = step["message"]
            # Apply grounding guardrail
            self._apply_grounding_to_turn(turn)
            # Emit final message if not already streamed
            if turn.final_message and not streamer.final_message_streamed:
                self.send_notification(turn.external_session_id, {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": turn.final_message},
                })
            turn.mark_completed()
            _log(f"PRINT_PROMPT_OK id={turn.upstream_request_id}")
            return True
        except Exception as exc:
            _log(f"PRINT_PROMPT_EXCEPTION id={turn.upstream_request_id} exc={exc}")
            turn.mark_failed()
            return False
        finally:
            self._stop_proc(proc)

    def _stream_print_steps(self, turn: Turn, streamer: DevinExportStreamer, transcript: dict) -> None:
        """Emit ACP notifications for newly discovered steps in print-mode."""
        steps = transcript.get("steps", [])
        if not isinstance(steps, list):
            return
        start = streamer.stream_start or 0
        for index, step in enumerate(steps[start:], start=start):
            if not isinstance(step, dict):
                continue
            if step.get("source") == "user":
                text = step.get("message")
                if isinstance(text, str) and text.strip() and index not in streamer.emitted_user_steps:
                    streamer.emitted_user_steps.add(index)
            if step.get("source") != "agent":
                continue
            # Reasoning
            if index not in streamer.emitted_thought_steps:
                text = step.get("reasoning_content")
                if isinstance(text, str) and text.strip():
                    streamer.emitted_thought_steps.add(index)
                    self.send_notification(turn.external_session_id, {
                        "sessionUpdate": "agent_thought_chunk",
                        "content": {"type": "text", "text": text},
                        "messageId": f"thought-{index}",
                    })
            # Tool calls
            metadata = step.get("metadata", {})
            extensions = metadata.get("extensions", {})
            tool_content = extensions.get("chisel/tool_call_content", {})
            if isinstance(tool_content, dict):
                for call_id, payload in tool_content.items():
                    if call_id in streamer.emitted_tool_calls:
                        continue
                    streamer.emitted_tool_calls.add(call_id)
                    update = dict(payload) if isinstance(payload, dict) else {}
                    tool_call_id = update.get("toolCallId") or call_id
                    status = update.get("status", "in_progress")
                    self.send_notification(turn.external_session_id, {
                        "sessionUpdate": "tool_call",
                        "toolCallId": tool_call_id,
                        "title": update.get("title", "Devin tool"),
                        "kind": update.get("kind", "other"),
                        "status": status if status in ("in_progress", "completed", "failed") else "in_progress",
                        "messageId": f"tool-{index}-{tool_call_id}",
                    })
                    # Track for grounding
                    turn.latest_tool_result = ToolResult(
                        name=update.get("title", "tool"),
                        status=status,
                        is_empty=True,
                        output_preview="",
                    )
            # Tool updates
            observation = step.get("observation", {})
            results = observation.get("results", []) if isinstance(observation, dict) else []
            for ri, result in enumerate(results):
                if not isinstance(result, dict):
                    continue
                tool_call_id = result.get("source_call_id") or result.get("tool_call_id") or result.get("id")
                if not tool_call_id:
                    continue
                ukey = f"{index}:{ri}:{tool_call_id}"
                if ukey in streamer.emitted_tool_updates:
                    continue
                streamer.emitted_tool_updates.add(ukey)
                text = result.get("content", "")
                if not isinstance(text, str):
                    text = json.dumps(text, ensure_ascii=False)
                status = "failed" if result.get("is_error") or result.get("isError") else "completed"
                self.send_notification(turn.external_session_id, {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": tool_call_id,
                    "status": status,
                    "rawOutput": {"shortPreview": text[:500], "fullRaw": text, "isFailed": status == "failed"},
                    "content": [{"type": "content", "content": {"type": "text", "text": text}}],
                })
                # Update grounding tracker
                turn.latest_tool_result = ToolResult(
                    name="tool",
                    status=status,
                    is_empty=not text.strip(),
                    output_preview=text[:500],
                )
            # Agent message
            if index not in streamer.emitted_message_steps:
                text = step.get("message")
                if isinstance(text, str) and text.strip():
                    streamer.emitted_message_steps.add(index)
                    turn.final_message = text
                    streamer.final_message_streamed = True
                    self.send_notification(turn.external_session_id, {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": text},
                        "messageId": f"msg-{index}",
                    })

    def _apply_grounding_to_turn(self, turn: Turn) -> None:
        if not turn.latest_tool_result or turn.grounding_appended:
            return
        latest = turn.latest_tool_result
        if latest.status != "failed" and not latest.is_empty:
            return
        if not turn.final_message:
            return
        lower = turn.final_message.lower()
        claims_success = (
            ("success" in lower or "completed" in lower or "found" in lower)
            and ("error" not in lower and "fail" not in lower)
        )
        if claims_success:
            turn.grounding_appended = True
            caution = f"\n\n> [!CAUTION]\n> **Grounding Warning**: The assistant claims success, but the latest tool call (`{latest.name}`) failed or returned empty. Please verify."
            self.send_notification(turn.external_session_id, {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": caution},
            })

    def _stop_proc(self, proc: subprocess.Popen) -> None:
        if proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass


    # -----------------------------------------------------------------------
    # Method handlers
    # -----------------------------------------------------------------------

    def handle_initialize(self, req_id: Any, params: dict) -> None:
        self._refresh_cache()
        models = get_cached_models()
        modes = get_cached_modes()
        self.send_result(req_id, {
            "protocolVersion": "2025-06-01",
            "capabilities": {
                "models": models,
                "modes": modes,
                "experimental": {},
            },
            "serverInfo": {"name": "devin-paseo-supervisor", "version": "2.0.0"},
            "agentCapabilities": {
                "loadSession": True,
                # Mirror native Devin ACP prompt capabilities so Paseo sends
                # image and embedded-resource content blocks in session/prompt.
                # Native `devin acp` advertises image=true, embeddedContext=true.
                "promptCapabilities": {
                    "image": True,
                    "audio": False,
                    "embeddedContext": True,
                },
                "sessionCapabilities": {
                    "list": True,
                    "close": True,
                },
                "_meta": {
                    # Supervisor implements message-level rewind by driving the
                    # native _cognition.ai/revert/* RPCs on the Devin child.
                    "cognition.ai/revert": True,
                },
            },
        })

    def handle_session_new(self, req_id: Any, params: dict) -> None:
        external_id = str(uuid.uuid4())
        session = self._ensure_session(external_id, params)
        self._reconcile_session_config(external_id, session, params)
        session["mcp_servers"] = params.get("mcpServers")
        # In native mode, create a real session under the MCP config lock —
        # .devin/mcp_config.local.json is shared per-cwd and the native child
        # reads it during session/new, so the write must stay valid until
        # the child has consumed it.
        with _mcp_config_lock(session.get("cwd")):
            _write_devin_mcp_config(session.get("cwd"), params.get("mcpServers"))
            if self._mode_preference == "native":
                child = self.pool.acquire()
                try:
                    real_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/new", "params": {"cwd": session["cwd"], "mcpServers": params.get("mcpServers", [])}}
                    if session["mode"]:
                        real_req["params"]["mode"] = session["mode"]
                    res = self._send_rpc(child, real_req, timeout=30)
                    if "result" in res:
                        real_id = res["result"].get("sessionId")
                        session["realId"] = real_id
                        child.real_session_id = real_id
                        # Apply model/mode
                        self._force_native_model(child, real_id, session.get("model"), session.get("effort"), session.get("sidekick"))
                        self._force_native_mode(child, real_id, DEFAULT_DEVIN_MODE)
                        session["loop_family"] = _loop_family(session)
                        result = dict(res["result"])
                        result["sessionId"] = external_id
                        if isinstance(result.get("modes"), dict):
                            result["modes"]["currentModeId"] = DEFAULT_DEVIN_MODE
                        native_options = result.get("configOptions") if isinstance(result.get("configOptions"), list) else []
                        result["configOptions"] = _merge_structured_config_options(
                            native_options,
                            session.get("model", "swe-2-medium"),
                            DEFAULT_DEVIN_MODE,
                            feature_values=session.get("featureValues"),
                            sidekick=session.get("sidekick"),
                        )
                        result["models"] = _acp_model_state(session.get("model", "swe-2-medium"))
                        self._persist_session(external_id)
                        self.pool.release(child, real_id)
                        self.send_result(req_id, result)
                        return
                    else:
                        child.real_session_id = None
                        self.pool.release(child)
                        # Fall through to synthetic session
                except Exception as exc:
                    _log(f"session/new native error: {exc}")
                    self.pool.release(child)
                    # Fall through to synthetic session
        # Synthetic session (print-mode or native fallback)
        self.send_result(req_id, {
            "sessionId": external_id,
            "modes": {"currentModeId": session["mode"], "availableModes": get_cached_modes()},
            "configOptions": _structured_config_options(session["model"], session["mode"], feature_values=session.get("featureValues"), sidekick=session.get("sidekick")),
            "models": _acp_model_state(session["model"]),
        })
        self._persist_session(external_id)

    def handle_session_load(self, req_id: Any, params: dict) -> None:
        external_id = params.get("sessionId")
        session = self._ensure_session(external_id, params)
        # Imported sessions are listed with the native realId as sessionId and
        # have no supervisor record yet — bind realId so the native resume path
        # and history recovery both work.
        if not session.get("realId"):
            native = self._native_session_meta().get(external_id)
            if native:
                session["realId"] = external_id
                session["title"] = session.get("title") or native.get("title")
                if native.get("cwd") and not (params or {}).get("cwd"):
                    session["cwd"] = native["cwd"]
        self._reconcile_session_config(external_id, session, params)
        session["mcp_servers"] = params.get("mcpServers")
        # Hold the per-cwd MCP config lock through the write + session_init
        # sequence. .devin/mcp_config.local.json is shared per-cwd and the
        # native child reads it during session/load or session/new, so a
        # sibling agent's write between our write and the child's read would
        # give the child the wrong callerAgentId.
        with _mcp_config_lock(session.get("cwd")):
            _write_devin_mcp_config(session.get("cwd"), params.get("mcpServers"))
            # Replay history BEFORE the native session/load RPC. Paseo's ACP client
            # sets replayingHistory=true before calling loadSession, and collects
            # session/update notifications into persistedHistory during the wait.
            # Notifications must arrive DURING this wait to be collected. If they
            # arrive after the result, replayingHistory is false and they bypass
            # persistedHistory, causing streamHistory() to return empty.
            self._replay_history(external_id)
            # If we have a realId and native mode, try to load it
            if session.get("realId") and self._mode_preference == "native":
                # Clean up stale lock files from dead Devin processes (e.g. after reboot)
                # reclaim_orphan=True also reclaims locks held by live devin acp
                # processes that belong to a previous/replaced supervisor stdio
                # instance (Paseo does not always tear down the old stdio on reload).
                stale_lock_cleanup(session["realId"], reclaim_orphan=True)
                child = self.pool.acquire(session["realId"])
                try:
                    real_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/load", "params": {"sessionId": session["realId"], "cwd": session.get("cwd", str(Path.home())), "mcpServers": []}}
                    res = self._send_rpc(child, real_req, timeout=SESSION_LOAD_TIMEOUT_SECONDS)
                    if "result" in res:
                        _log(f"session/load OK for external={external_id} real={session['realId']}")
                        session["mode"] = DEFAULT_DEVIN_MODE
                        self._force_native_model(child, session["realId"], session.get("model"), session.get("effort"), session.get("sidekick"))
                        self._force_native_mode(child, session["realId"], DEFAULT_DEVIN_MODE)
                        session["loop_family"] = _loop_family(session)
                        self.pool.release(child, session["realId"])
                        self.send_result(req_id, self._session_result(external_id, session))
                        self._persist_session(external_id)
                        return
                    else:
                        err = res.get("error", {})
                        msg = err.get("message", "")
                        _log(f"session/load ERROR for external={external_id} real={session['realId']} msg={msg}")
                        if "already open" in msg.lower() or "another process" in msg.lower():
                            # Try removing stale lock and retrying session/load.
                            # reclaim_orphan=True to reclaim locks held by a live
                            # devin acp child of a previous supervisor instance.
                            if stale_lock_cleanup(session["realId"], reclaim_orphan=True, reclaim_sibling=True):
                                _log(f"session/load retry after lock cleanup for external={external_id}")
                                retry_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/load", "params": {"sessionId": session["realId"], "cwd": session.get("cwd", str(Path.home())), "mcpServers": []}}
                                retry_res = self._send_rpc(child, retry_req, timeout=SESSION_LOAD_TIMEOUT_SECONDS)
                                if "result" in retry_res:
                                    _log(f"session/load RESUMED from sessions.db for external={external_id} real={session['realId']}")
                                    session["mode"] = DEFAULT_DEVIN_MODE
                                    self._force_native_model(child, session["realId"], session.get("model"), session.get("effort"), session.get("sidekick"))
                                    self._force_native_mode(child, session["realId"], DEFAULT_DEVIN_MODE)
                                    session["loop_family"] = _loop_family(session)
                                    self.pool.release(child, session["realId"])
                                    self.send_result(req_id, self._session_result(external_id, session))
                                    self._persist_session(external_id)
                                    return
                                retry_err = retry_res.get("error", {}).get("message", "")
                                _log(f"session/load retry failed: {retry_err}")
                            # Fall through to fresh session creation
                        if "already open" in msg.lower() or "another process" in msg.lower() or "not found" in msg.lower() or "unknown session" in msg.lower() or "invalid params" in msg.lower() or "timeout" in msg.lower():
                            if self._requires_real_resume(external_id, session):
                                self.pool.release(child, session["realId"])
                                err_msg = self._continuity_error(external_id, session, msg or "session/load failed")
                                _log(f"session/load CONTINUITY_REFUSED external={external_id} real={session['realId']} msg={msg}")
                                self.send_error(req_id, -32013, err_msg, external_id)
                                self._persist_session(external_id)
                                return
                            # Create fresh native session
                            _log(f"session/load creating fresh for external={external_id}")
                            self.pool.release(child, session["realId"])
                            child = self.pool.acquire()
                            new_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/new", "params": {"cwd": session["cwd"], "mcpServers": params.get("mcpServers", [])}}
                            new_res = self._send_rpc(child, new_req, timeout=30)
                            if "result" in new_res:
                                new_real_id = new_res["result"]["sessionId"]
                                session["realId"] = new_real_id
                                child.real_session_id = new_real_id
                                self._force_native_model(child, new_real_id, session.get("model"), session.get("effort"), session.get("sidekick"))
                                self._force_native_mode(child, new_real_id, DEFAULT_DEVIN_MODE)
                                session["loop_family"] = _loop_family(session)
                                self.send_result(req_id, self._session_result(external_id, session))
                                self._persist_session(external_id)
                                return
                            self.pool.release(child)
                        else:
                            # Catch-all: release the child for any other error
                            # (e.g. timeout) to prevent resource leak.
                            _log(f"session/load unhandled error, releasing child for external={external_id} msg={msg}")
                            self.pool.release(child, session["realId"])
                except Exception as exc:
                    _log(f"session/load native error: {exc}")
                    self.pool.release(child)
                    if self._requires_real_resume(external_id, session):
                        err_msg = self._continuity_error(external_id, session, str(exc))
                        self.send_error(req_id, -32013, err_msg, external_id)
                        self._persist_session(external_id)
                        return
            elif not session.get("realId") and self._mode_preference == "native":
                if self._requires_real_resume(external_id, session):
                    err_msg = self._continuity_error(external_id, session, "missing native session id")
                    _log(f"session/load CONTINUITY_REFUSED external={external_id} reason=missing_realId")
                    self.send_error(req_id, -32013, err_msg, external_id)
                    self._persist_session(external_id)
                    return
                # No realId (new stdio process, state not recovered) — create a fresh native session
                _log(f"session/load no realId, creating fresh native for external={external_id}")
                child = self.pool.acquire()
                try:
                    new_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/new", "params": {"cwd": session["cwd"], "mcpServers": params.get("mcpServers", [])}}
                    new_res = self._send_rpc(child, new_req, timeout=30)
                    if "result" in new_res:
                        new_real_id = new_res["result"]["sessionId"]
                        session["realId"] = new_real_id
                        child.real_session_id = new_real_id
                        self._force_native_model(child, new_real_id, session.get("model"), session.get("effort"), session.get("sidekick"))
                        self._force_native_mode(child, new_real_id, DEFAULT_DEVIN_MODE)
                        session["loop_family"] = _loop_family(session)
                    else:
                        _log(f"session/load fresh create failed: {new_res.get('error')}")
                    self.pool.release(child, session.get("realId"))
                except Exception as exc:
                    _log(f"session/load fresh create error: {exc}")
                    self.pool.release(child)
        self.send_result(req_id, self._session_result(external_id, session))
        self._persist_session(external_id)

    def handle_session_prompt(self, req_id: Any, params: dict) -> None:
        external_id = params.get("sessionId")
        session = self._get_session(external_id)
        if not session:
            self.send_error(req_id, -32001, f"Unknown session '{external_id}'")
            return
        self._reconcile_session_config(external_id, session, params)
        # Fallback: if history hasn't been replayed yet (e.g. paseo sent
        # session/prompt without calling session/load first, or the
        # session/load replay was dropped), replay it now before the prompt.
        # The _replayed_sessions set prevents double-replay.
        self._replay_history(external_id)
        old_turn = None
        # Preserve the full ACP ContentBlock array (text, image, resource, ...)
        # so we can forward non-text blocks to native Devin. Also extract the
        # text-only representation for print-mode fallback, logging, and the
        # text portion of the user_message_chunk stream.
        raw_prompt = params.get("prompt") or []
        prompt_blocks = [item for item in raw_prompt if isinstance(item, dict)]
        prompt_text = "\n".join(
            item.get("text", "") for item in prompt_blocks if item.get("type") == "text"
        )
        image_count = sum(1 for item in prompt_blocks if item.get("type") == "image")
        if image_count:
            _log(f"PROMPT_HAS_IMAGES external={external_id} count={image_count} blocks={len(prompt_blocks)}")
        # Local commands: /steps, /revert, /fork drive the native revert RPCs
        # instead of starting a model turn.
        if self._maybe_handle_revert_command(req_id, external_id, session, prompt_text):
            return
        with self.lock:
            if external_id in self.turns and self.turns[external_id].phase in (
                TurnPhase.QUEUED, TurnPhase.TYPING, TurnPhase.ACTIVE, TurnPhase.COMPLETING
            ):
                old_turn = self.turns[external_id]
        if old_turn:
            self._cancel_turn(external_id, old_turn, reason="replaced_by_new_prompt")
        with self.lock:
            turn = Turn(
                upstream_request_id=req_id,
                native_request_id=self.next_id(),
                external_session_id=external_id,
                real_session_id=session.get("realId"),
                phase=TurnPhase.QUEUED,
                prompt_text=prompt_text,
                started_at=time.time(),
                last_update_at=time.time(),
                prompt_blocks=prompt_blocks,
                client_message_id=params.get("messageId"),
            )
            self.turns[external_id] = turn
        # Emit one user_message_chunk per ContentBlock so the Paseo UI renders
        # images alongside the text. ACP's user_message_chunk carries a single
        # ContentBlock in its `content` field.
        _mid = params.get("messageId")
        for block in prompt_blocks:
            _un = {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": external_id,
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": block,
                        "messageId": _mid,
                    },
                },
            }
            self.write_parent(_un)
            # Persist immediately so user messages (especially image blocks)
            # survive mid-turn process death. User messages are infrequent
            # (one set per prompt), so the I/O cost is negligible.
            self._append_history(external_id, _un, persist=True)
        # If the prompt had no blocks at all (empty prompt), still emit an
        # empty text chunk to preserve prior behavior.
        if not prompt_blocks:
            _un = {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": external_id,
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": {"type": "text", "text": ""},
                        "messageId": _mid,
                    },
                },
            }
            self.write_parent(_un)
            self._append_history(external_id, _un, persist=True)
        # Run in background thread
        threading.Thread(target=self._run_prompt, args=(turn, session), daemon=True).start()

    def _prepare_auto_resume_retry(self, turn: Turn, reason: str) -> None:
        text = "Continue where you left off"
        if turn.auto_resume_count < AUTO_RESUME_MAX_PER_TURN:
            turn.auto_resume_count += 1
            _log(
                f"AUTO_RESUME_INJECT external={turn.external_session_id} "
                f"reason={reason} count={turn.auto_resume_count}/{AUTO_RESUME_MAX_PER_TURN}"
            )
            msg = {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": turn.external_session_id,
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": {"type": "text", "text": text},
                        "messageId": f"auto-resume-{uuid.uuid4()}",
                    },
                },
            }
            self.write_parent(msg)
            self._append_history(turn.external_session_id, msg, persist=True)
            turn.auto_resume_injected = True
        turn.prompt_text = text
        turn.prompt_blocks = [{"type": "text", "text": text}]

    def _run_prompt(self, turn: Turn, session: dict) -> None:
        max_retries = PROMPT_MAX_RETRIES
        last_error = None
        for attempt in range(1, max_retries + 1):
            if self._shutdown:
                turn.mark_cancelled()
                break
            try:
                if self._mode_preference == "native":
                    ok = self._execute_native_prompt(turn, session)
                else:
                    ok = self._execute_print_prompt(turn, session)
                if ok:
                    break
                # Determine if retryable
                if turn.failure_message:
                    break
                retryable_phase = turn.phase == TurnPhase.FAILED or (
                    turn.phase == TurnPhase.TIMED_OUT
                    and turn.timeout_reason in ("post_output_stall", "idle")
                )
                if retryable_phase and attempt < max_retries:
                    _log(f"PROMPT_AUTO_RESUME_RETRY attempt={attempt}/{max_retries} external={turn.external_session_id} phase={turn.phase.value} timeout={turn.timeout_reason}")
                    self._prepare_auto_resume_retry(turn, turn.timeout_reason or turn.phase.value)
                    # Strip non-text blocks on retry. The image/other blocks
                    # were already forwarded on the first attempt and are also
                    # present in the loaded session history, so re-sending them
                    # would duplicate the image in the model's context on every
                    # retry (causing the agent to complain about receiving the
                    # same image repeatedly).
                    if any(b.get("type") != "text" for b in turn.prompt_blocks):
                        non_text = sum(1 for b in turn.prompt_blocks if b.get("type") != "text")
                        turn.prompt_blocks = [b for b in turn.prompt_blocks if b.get("type") == "text"]
                        _log(f"PROMPT_RETRY_STRIP_NON_TEXT external={turn.external_session_id} dropped={non_text}")
                    turn.phase = TurnPhase.QUEUED
                    turn.started_at = time.time()
                    turn.last_update_at = time.time()
                    turn.had_agent_message = False
                    turn.timeout_reason = None
                    time.sleep(1)
                    continue
                break
            except Exception as exc:
                _log(f"PROMPT_EXCEPTION attempt={attempt} external={turn.external_session_id} exc={exc}")
                last_error = exc
                turn.mark_failed()
                if attempt < max_retries:
                    self._prepare_auto_resume_retry(turn, "exception")
                    if any(b.get("type") != "text" for b in turn.prompt_blocks):
                        non_text = sum(1 for b in turn.prompt_blocks if b.get("type") != "text")
                        turn.prompt_blocks = [b for b in turn.prompt_blocks if b.get("type") == "text"]
                        _log(f"PROMPT_RETRY_STRIP_NON_TEXT external={turn.external_session_id} dropped={non_text}")
                    turn.phase = TurnPhase.QUEUED
                    turn.had_agent_message = False
                    turn.timeout_reason = None
                    time.sleep(1)
                    continue
                break
        # Send result
        if not turn.result_sent:
            turn.result_sent = True
            self._finalize_subagent_cards(turn)
            if turn.phase == TurnPhase.COMPLETED:
                self._emit_usage_summary(turn)
                self.send_result(turn.upstream_request_id, self._prompt_result(turn.external_session_id))
            elif turn.phase == TurnPhase.CANCELLED:
                self.send_error(turn.upstream_request_id, -32006, "Prompt cancelled by user", turn.external_session_id)
            elif turn.phase == TurnPhase.TIMED_OUT:
                if turn.timeout_reason == "hard":
                    timeout_msg = f"Prompt exceeded max turn duration of {HARD_MAX_TURN_SECONDS}s"
                elif turn.timeout_reason == "post_output_stall":
                    timeout_msg = (
                        f"Prompt stalled after prior output for "
                        f"{NUDGE_IDLE_SECONDS + POST_OUTPUT_STALL_GRACE_SECONDS}s"
                    )
                else:
                    timeout_msg = f"Prompt idle for {PROMPT_TIMEOUT_SECONDS}s"
                self.send_error(turn.upstream_request_id, -32005, timeout_msg, turn.external_session_id)
            else:
                msg = turn.failure_message or str(last_error) or "Prompt failed"
                self.send_error(turn.upstream_request_id, -32004, msg, turn.external_session_id)
        with self.lock:
            if self.turns.get(turn.external_session_id) is turn:
                self.turns.pop(turn.external_session_id, None)
        self._persist_session(turn.external_session_id)

    def _cancel_turn(self, external_id: str, turn: Turn, reason: str = "cancel") -> None:
        turn.cancel_sent = True
        turn.mark_cancelled()
        child = turn.child
        _log(f"CANCEL_TURN external={external_id} reason={reason} phase={turn.phase.value} child_pid={child.proc.pid if child else None}")
        self._release_turn_terminals(turn, reason)
        if not child or child.proc.poll() is not None:
            return
        self.pool.mark_dying(child)
        # Native Devin currently returns Method not found for session/cancel,
        # but keep the best-effort soft request for future-compatible ACP builds.
        try:
            cancel_req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/cancel", "params": {"sessionId": turn.real_session_id}}
            body = json.dumps(cancel_req, separators=(",", ":")) + "\n"
            with child.stdin_lock:
                child.proc.stdin.write(body)
                child.proc.stdin.flush()
            _log(f"CANCEL_SOFT external={external_id} real={turn.real_session_id}")
        except Exception as exc:
            _log(f"CANCEL_SOFT_FAILED external={external_id} exc={exc}")
        try:
            _log(f"CANCEL_SIGTERM external={external_id} reason={reason}")
            child.proc.terminate()
        except Exception as exc:
            _log(f"CANCEL_SIGTERM_FAILED external={external_id} exc={exc}")

        def kill_if_needed() -> None:
            time.sleep(5)
            if child.proc.poll() is None:
                try:
                    _log(f"CANCEL_SIGKILL external={external_id} reason={reason}")
                    child.proc.kill()
                except Exception as exc:
                    _log(f"CANCEL_SIGKILL_FAILED external={external_id} exc={exc}")

        threading.Thread(target=kill_if_needed, daemon=True).start()

    def _release_turn_terminals(self, turn: Turn, reason: str) -> None:
        terminal_ids = list(turn.active_terminal_ids)
        if not terminal_ids:
            return
        for terminal_id in terminal_ids:
            params = {"sessionId": turn.external_session_id, "terminalId": terminal_id}
            _log(f"CANCEL_TERMINAL_RELEASE external={turn.external_session_id} terminal={terminal_id} reason={reason}")
            res = self._send_parent_rpc("terminal/release", params, timeout=10)
            if "error" in res:
                _log(f"CANCEL_TERMINAL_RELEASE_FAILED external={turn.external_session_id} terminal={terminal_id} err={res.get('error')}")
                kill_res = self._send_parent_rpc("terminal/kill", params, timeout=10)
                if "error" in kill_res:
                    _log(f"CANCEL_TERMINAL_KILL_FAILED external={turn.external_session_id} terminal={terminal_id} err={kill_res.get('error')}")
                self._kill_matching_terminal_command(turn.active_terminal_commands.get(terminal_id), terminal_id)
            turn.active_terminal_ids.discard(terminal_id)
            turn.active_terminal_commands.pop(terminal_id, None)

    def _kill_matching_terminal_command(self, command: Optional[str], terminal_id: str) -> None:
        if not command or len(command.strip()) < 8:
            return
        needles = [command.strip()]
        # When a shell pipeline is killed, grandchildren can survive under
        # launchd with shorter argv strings. Match the specific long-running
        # build commands too, so tool timeouts do not leave orphaned builds.
        for marker in (
            "pnpm build:cloudflare",
            "opennextjs-cloudflare build",
            "wrangler deploy",
        ):
            if marker in command and marker not in needles:
                needles.append(marker)
        try:
            proc = subprocess.run(["ps", "-eo", "pid=,ppid=,command="], capture_output=True, text=True, timeout=5)
        except Exception as exc:
            _log(f"CANCEL_TERMINAL_OS_FALLBACK_PS_FAILED terminal={terminal_id} exc={exc}")
            return
        current_pid = os.getpid()
        candidates = []
        children_by_ppid: Dict[int, List[int]] = {}
        for line in proc.stdout.splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            try:
                pid = int(parts[0])
                ppid = int(parts[1])
            except ValueError:
                continue
            cmdline = parts[2]
            children_by_ppid.setdefault(ppid, []).append(pid)
            if pid == current_pid:
                continue
            if any(needle in cmdline for needle in needles):
                candidates.append(pid)
        if not candidates:
            return
        to_kill: Set[int] = set()
        stack = list(candidates)
        while stack:
            pid = stack.pop()
            if pid in to_kill or pid == current_pid:
                continue
            to_kill.add(pid)
            stack.extend(children_by_ppid.get(pid, []))
        if not to_kill:
            return
        _log(f"CANCEL_TERMINAL_OS_FALLBACK terminal={terminal_id} pids={sorted(to_kill)}")
        for sig in (signal.SIGTERM, signal.SIGKILL):
            for pid in sorted(to_kill, reverse=True):
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    pass
                except Exception as exc:
                    _log(f"CANCEL_TERMINAL_OS_KILL_FAILED terminal={terminal_id} pid={pid} sig={sig} exc={exc}")
            time.sleep(1)

    def handle_session_cancel(self, req_id: Any, params: dict) -> None:
        external_id = params.get("sessionId")
        with self.lock:
            turn = self.turns.get(external_id)
        if not turn:
            self.send_result(req_id, {})
            return
        self._cancel_turn(external_id, turn, reason="session_cancel")
        self.send_result(req_id, {})

    def handle_session_close(self, req_id: Any, params: dict) -> None:
        """Acknowledge Paseo's refresh/close request without deleting history.

        Paseo sends session/close while refreshing or reloading an agent. Native
        Devin currently does not need an explicit close RPC, and treating this as
        Method not found makes Paseo wait for a doomed close and report liveness
        failures. We answer immediately, then clean up any active turn in the
        background so terminal release/kill timeouts cannot block the close ACK.
        """
        external_id = params.get("sessionId")
        with self.lock:
            turn = self.turns.get(external_id)
        if turn:
            _log(f"SESSION_CLOSE external={external_id} active_turn=True")
            threading.Thread(
                target=self._cancel_turn,
                args=(external_id, turn),
                kwargs={"reason": "session_close"},
                daemon=True,
            ).start()
        else:
            _log(f"SESSION_CLOSE external={external_id} active_turn=False")
        self.send_result(req_id, {})

    def handle_session_set_mode(self, req_id: Any, params: dict) -> None:
        external_id = params.get("sessionId")
        session = self._get_session(external_id)
        if not session:
            self.send_error(req_id, -32001, f"Unknown session '{external_id}'")
            return
        # Handle both ACP format (configId/value) and legacy format (modeId/mode)
        if params.get("configId") == "mode":
            requested_mode = params.get("value") or DEFAULT_DEVIN_MODE
        else:
            requested_mode = params.get("modeId") or params.get("mode") or DEFAULT_DEVIN_MODE
        mode = DEFAULT_DEVIN_MODE
        if requested_mode != mode:
            _log(f"SET_MODE overriding requested={requested_mode} forced={mode} external={external_id}")
        session["mode"] = mode
        if session.get("realId") and self._mode_preference == "native":
            child = self.pool.acquire(session["realId"])
            try:
                req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/set_config_option", "params": {"sessionId": session["realId"], "configId": "mode", "value": mode}}
                _log(f"SET_MODE sending RPC to native child real={session['realId']} mode={mode}")
                self._send_rpc(child, req, timeout=10)
                _log(f"SET_MODE RPC success for real={session['realId']} mode={mode}")
                self.pool.release(child, session["realId"])
            except Exception as exc:
                _log(f"SET_MODE RPC failed for real={session.get('realId')} mode={mode}: {exc}")
                self.pool.release(child)
        else:
            _log(f"SET_MODE skipped: realId={session.get('realId')} mode_preference={self._mode_preference}")
        self.send_notification(external_id, {"sessionUpdate": "current_mode_update", "currentModeId": mode})
        self._emit_config_options(external_id, self._session_result(external_id, session).get("configOptions"))
        self.send_result(req_id, {})
        self._persist_session(external_id)

    def handle_session_set_model(self, req_id: Any, params: dict) -> None:
        external_id = params.get("sessionId")
        session = self._get_session(external_id)
        if not session:
            self.send_error(req_id, -32001, f"Unknown session '{external_id}'")
            return
        # Handle both ACP format (configId/value) and legacy format (modelId/model)
        if params.get("configId") == "model":
            model = params.get("value") or "swe-2"
        else:
            model = params.get("modelId") or params.get("model") or "swe-2"
        # Strip provider prefix if Paseo sends "devin/xxx"
        # ("fusion/<lead>" is a real family id — never strip that one)
        if "/" in model and not model.startswith("fusion/"):
            model = model.split("/", 1)[1]
        base, tier = _split_model_id(model)
        _fam = _family_entry(_model_family(base))
        _eff = tier or session.get("effort") or _default_effort_for_family(_fam)
        session["model"] = _resolve_model(base, _eff, session.get("sidekick"))
        session["effort"] = _eff
        # Persist the actually-resolved sidekick — the stored value may not
        # be offered by the newly-selected fusion family.
        _m = _FUSION_ID_RE.match(session["model"] or "")
        if _m and _m.group("sk") != session.get("sidekick"):
            session["sidekick"] = _m.group("sk")
        elif not _m:
            # Non-fusion model — clear the stale sidekick.
            session.pop("sidekick", None)
        model = session["model"]
        if session.get("realId") and self._mode_preference == "native":
            child = self.pool.acquire(session["realId"])
            try:
                req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/set_config_option", "params": {"sessionId": session["realId"], "configId": "model", "value": model}}
                _log(f"SET_MODEL sending RPC to native child real={session['realId']} model={model}")
                self._send_rpc(child, req, timeout=10)
                _log(f"SET_MODEL RPC success for real={session['realId']} model={model}")
                self.pool.release(child, session["realId"])
            except Exception as exc:
                _log(f"SET_MODEL RPC failed for real={session.get('realId')} model={model}: {exc}")
                self.pool.release(child)
        else:
            _log(f"SET_MODEL skipped: realId={session.get('realId')} mode_preference={self._mode_preference}")
        result = self._session_result(external_id, session)
        self._emit_config_options(external_id, result.get("configOptions"))
        self.send_result(req_id, result)
        self._persist_session(external_id)


    def handle_session_set_effort(self, req_id: Any, params: dict) -> None:
        external_id = params.get("sessionId")
        session = self._get_session(external_id)
        if not session:
            self.send_error(req_id, -32001, f"Unknown session '{external_id}'")
            return
        effort = params.get("value") or params.get("effort")
        if not effort:
            _fam = _family_for_model_id(session.get("model") or "swe-2") or _family_entry(_model_family(session.get("model") or "swe-2"))
            effort = _default_effort_for_family(_fam)
        base, _ = _split_model_id(session.get("model") or "swe-2")
        session["effort"] = effort
        session["model"] = _resolve_model(base, effort, session.get("sidekick"))
        # Persist the actually-resolved sidekick — the effort change may
        # have produced a different fusion combo.
        _m = _FUSION_ID_RE.match(session["model"] or "")
        if _m and _m.group("sk") != session.get("sidekick"):
            session["sidekick"] = _m.group("sk")
        model = session["model"]
        if session.get("realId") and self._mode_preference == "native":
            child = self.pool.acquire(session["realId"])
            try:
                req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/set_config_option", "params": {"sessionId": session["realId"], "configId": "model", "value": model}}
                self._send_rpc(child, req, timeout=10)
                self.pool.release(child, session["realId"])
            except Exception as exc:
                _log(f"SET_EFFORT RPC failed real={session.get('realId')} model={model}: {exc}")
                self.pool.release(child)
        result = self._session_result(external_id, session)
        self._emit_config_options(external_id, result.get("configOptions"))
        self.send_result(req_id, result)
        self._persist_session(external_id)


    def handle_session_set_sidekick(self, req_id: Any, params: dict) -> None:
        """Fusion sessions: pick the sidekick model, then re-resolve the
        concrete fusion-<lead>-<eff>-sidekick-<sk> id and push it."""
        external_id = params.get("sessionId")
        session = self._get_session(external_id)
        if not session:
            self.send_error(req_id, -32001, f"Unknown session '{external_id}'")
            return
        session["sidekick"] = params.get("value") or params.get("sidekick")
        base, tier = _split_model_id(session.get("model") or "")
        session["model"] = _resolve_model(base, tier or session.get("effort"), session["sidekick"])
        # Persist the actually-resolved sidekick — _resolve_model may have
        # substituted a valid default when the requested value isn't offered
        # by this fusion family.
        _m = _FUSION_ID_RE.match(session["model"] or "")
        if _m and _m.group("sk") != session["sidekick"]:
            session["sidekick"] = _m.group("sk")
        model = session["model"]
        if session.get("realId") and self._mode_preference == "native":
            child = self.pool.acquire(session["realId"])
            try:
                req = {"jsonrpc": "2.0", "id": self.next_id(), "method": "session/set_config_option", "params": {"sessionId": session["realId"], "configId": "model", "value": model}}
                self._send_rpc(child, req, timeout=10)
                self.pool.release(child, session["realId"])
            except Exception as exc:
                _log(f"SET_SIDEKICK RPC failed real={session.get('realId')} model={model}: {exc}")
                self.pool.release(child)
        result = self._session_result(external_id, session)
        self._emit_config_options(external_id, result.get("configOptions"))
        self.send_result(req_id, result)
        self._persist_session(external_id)


    def handle_session_set_feature(self, req_id: Any, params: dict, config_id: Optional[str]) -> None:
        """Handle feature/toggle config options (e.g. allow_all) that map to
        session featureValues rather than model/mode/effort."""
        external_id = params.get("sessionId")
        session = self._get_session(external_id)
        if not session:
            self.send_error(req_id, -32001, f"Unknown session '{external_id}'")
            return
        if config_id:
            session.setdefault("featureValues", {})[config_id] = params.get("value")
        result = self._session_result(external_id, session)
        self._emit_config_options(external_id, result.get("configOptions"))
        self.send_result(req_id, result)
        self._persist_session(external_id)

    # ------------------------------------------------------------------
    # Revert (rollback) support — drives Devin's _cognition.ai/revert/* RPCs
    # ------------------------------------------------------------------

    def _native_rpc(self, session: dict, method: str, params: dict, timeout: float = 30) -> dict:
        """Send an RPC to the session's native Devin child, mapping the
        external sessionId to the native realId."""
        real_id = session.get("realId")
        if not real_id or self._mode_preference != "native":
            raise RuntimeError("no native session attached")
        params = dict(params)
        params["sessionId"] = real_id
        child = self.pool.acquire(real_id)
        try:
            res = self._send_rpc(child, {"jsonrpc": "2.0", "id": self.next_id(), "method": method, "params": params}, timeout=timeout)
            self.pool.release(child, real_id)
            return res
        except Exception:
            self.pool.release(child)
            raise

    def _revert_to_message(self, session: dict, params: dict) -> dict:
        """Resolve a timeline messageId to a native revert target and execute
        the revert. Params: {sessionId, messageId, force?}."""
        client_mid = params.get("messageId")
        umsg_map = session.get("user_msg_map") or {}
        native_umid = umsg_map.get(client_mid, client_mid)
        steps_res = self._native_rpc(session, "_cognition.ai/revert/listSteps", {})
        if "error" in steps_res:
            return steps_res
        steps = (steps_res.get("result") or {}).get("steps") or []
        target = None
        for s in steps:
            if s.get("userMessageId") == native_umid or s.get("userMessageId") == client_mid:
                target = s
                break
        if target is None:
            # Fallback: if messageId looks like a step number, honor it.
            for s in steps:
                if str(s.get("stepNumber")) == str(client_mid):
                    target = s
                    break
        if target is None:
            raise RuntimeError(f"No revertible step found for message {client_mid}")
        node_id = target.get("revertTargetNodeId") or target.get("targetNodeId") or target.get("stepNumber")
        res = self._native_rpc(session, "_cognition.ai/revert/execute", {
            "targetNodeId": node_id,
            "force": bool(params.get("force", True)),
        }, timeout=120)
        return res

    def handle_native_passthrough(self, req_id: Any, method: str, params: dict) -> None:
        """Forward _cognition.ai/* (and similar extension) requests to the
        session's native Devin child."""
        external_id = (params or {}).get("sessionId")
        session = self._get_session(external_id)
        if not session or not session.get("realId"):
            self.send_error(req_id, -32601, f"Method requires a native session: {method}", external_id)
            return
        try:
            if method == "_cognition.ai/revert/toMessage":
                res = self._revert_to_message(session, params or {})
            else:
                res = self._native_rpc(session, method, params or {})
        except Exception as exc:
            self.send_error(req_id, -32004, f"Native RPC {method} failed: {exc}", external_id)
            return
        if "error" in res:
            err = res["error"]
            self.send_error(req_id, err.get("code", -32603), err.get("message", "native error"), external_id)
        else:
            if method == "_cognition.ai/revert/toMessage":
                # History was rewound natively. Truncate our recorded event
                # stream at the target user message so the next replay shows
                # the rewound timeline with full fidelity (tool calls, thinking,
                # stats blocks). Fall back to DB recovery when the message is
                # not in the recorded stream.
                with self.lock:
                    buf = self.history.get(external_id) or []
                    mid = (params or {}).get("messageId")
                    cut = None
                    for idx, ev in enumerate(buf):
                        u = (ev.get("params") or {}).get("update") or {}
                        if u.get("sessionUpdate") == "user_message_chunk" and u.get("messageId") == mid:
                            cut = idx
                            break
                    if cut is not None:
                        self.history[external_id] = buf[:cut]
                        try:
                            save_session_history(external_id, self.history[external_id])
                        except Exception:
                            pass
                    else:
                        self.history.pop(external_id, None)
                        try:
                            _session_history_path(external_id).unlink(missing_ok=True)
                        except Exception:
                            pass
                    self._replayed_sessions.discard(external_id)
            self.send_result(req_id, res.get("result") or {})

    def _maybe_handle_revert_command(self, req_id: Any, external_id: str, session: dict, prompt_text: str) -> bool:
        """Intercept /steps, /revert, /fork prompts and drive the native
        _cognition.ai/revert/* RPCs. Returns True when handled."""
        m = re.match(r"^\s*/(steps|revert|fork)\b\s*(.*)$", prompt_text or "")
        if not m:
            return False
        cmd, arg = m.group(1), m.group(2).strip()

        def reply(text: str, stop: str = "end_turn") -> None:
            self.send_notification(external_id, {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": text},
            })
            self.send_result(req_id, {"stopReason": stop})

        # Echo the user message so the command is visible in the timeline.
        self.send_notification(external_id, {
            "sessionUpdate": "user_message_chunk",
            "content": {"type": "text", "text": prompt_text},
        })

        if not session.get("realId") or self._mode_preference != "native":
            reply("Revert requires a live native Devin session.")
            return True

        try:
            if cmd == "steps" or (cmd == "revert" and not arg):
                res = self._native_rpc(session, "_cognition.ai/revert/listSteps", {})
                steps = (res.get("result") or {}).get("steps") or []
                if not steps:
                    reply("No revertible steps yet.")
                    return True
                lines = ["Revertible steps (newest first):", ""]
                for s in reversed(steps):
                    num = s.get("stepNumber")
                    kind = s.get("kind") or "step"
                    summary = (s.get("summary") or "").strip() or "(no summary)"
                    lines.append(f"{num}. [{kind}] {summary}")
                lines.append("")
                lines.append("Use /revert <step> to rewind to before that step (also reverts file changes), or /fork <step> to branch from it.")
                reply("\n".join(lines))
                return True

            # resolve numeric step arg
            res = self._native_rpc(session, "_cognition.ai/revert/listSteps", {})
            steps = (res.get("result") or {}).get("steps") or []
            force = arg.endswith(" force") or arg.endswith(" --force")
            if force:
                arg = re.sub(r"\s*(--)?force\s*$", "", arg).strip()
            target = None
            for s in steps:
                if str(s.get("stepNumber")) == arg or str(s.get("revertTargetNodeId")) == arg or str(s.get("targetNodeId")) == arg:
                    target = s
                    break
            if target is None:
                reply(f"Unknown step '{arg}'. Run /steps to list revertible steps.")
                return True
            node_id = target.get("revertTargetNodeId") or target.get("targetNodeId") or target.get("stepNumber")

            if cmd == "fork":
                res = self._native_rpc(session, "_cognition.ai/revert/forkFromStep", {"targetNodeId": node_id}, timeout=60)
                if "error" in res:
                    reply(f"Fork failed: {res['error'].get('message')}")
                    return True
                forked = (res.get("result") or {}).get("forkedSessionId")
                reply(f"Forked to a new session: {forked}\nImport it via 导入会话 (it appears with its own title).")
                return True

            # /revert: preview, then execute
            prev = self._native_rpc(session, "_cognition.ai/revert/preview", {"targetNodeId": node_id}, timeout=60)
            if "error" in prev:
                reply(f"Revert preview failed: {prev['error'].get('message')}")
                return True
            pr = prev.get("result") or {}
            conflicts = pr.get("conflicts") or []
            if conflicts and not force:
                lines = [f"Revert to step {arg} has {len(conflicts)} conflict(s):"]
                for c in conflicts[:10]:
                    if isinstance(c, dict):
                        lines.append(f"- {c.get('path') or c.get('file') or json.dumps(c)[:120]}")
                    else:
                        lines.append(f"- {c}")
                lines.append("")
                lines.append(f"Re-run with `/revert {arg} force` to override.")
                reply("\n".join(lines))
                return True
            warnings = pr.get("irreversibleWarnings") or []
            res = self._native_rpc(session, "_cognition.ai/revert/execute", {
                "targetNodeId": node_id,
                "force": force,
            }, timeout=120)
            if "error" in res:
                reply(f"Revert failed: {res['error'].get('message')}")
                return True
            # History was rewound natively — drop our cached replay so a
            # reload re-syncs from the native DB.
            with self.lock:
                self.history.pop(external_id, None)
                self._replayed_sessions.discard(external_id)
            try:
                _session_history_path(external_id).unlink(missing_ok=True)
            except Exception:
                pass
            out = res.get("result") or {}
            lines = [f"Reverted to before step {arg}."]
            if warnings:
                lines.append("Warnings: " + "; ".join(str(w)[:120] for w in warnings[:5]))
            outcomes = out.get("outcomes")
            if outcomes:
                lines.append(f"File outcomes: {len(outcomes)}")
            reply("\n".join(lines))
            return True
        except Exception as exc:
            reply(f"Revert command failed: {exc}")
            return True

    def handle_session_delete(self, req_id: Any, params: dict) -> None:
        external_id = params.get("sessionId")
        with self.lock:
            session = self.sessions.pop(external_id, None)
            self._replayed_sessions.discard(external_id)
            if session:
                real_id = session.get("realId")
                # Release child affinity
                for c in self.pool.children:
                    if c.real_session_id == real_id:
                        c.real_session_id = None
                        c.state = ChildState.IDLE
                self.history.pop(external_id, None)
        if session:
            with_state(lambda state: state.setdefault("sessions", {}).pop(external_id, None))
            delete_session_history(external_id)
            self.send_result(req_id, {"deleted": True})
        else:
            self.send_error(req_id, -32001, f"Unknown session '{external_id}'")

    def _native_session_meta(self) -> Dict[str, dict]:
        """real_id -> {title, cwd, created_at, last_activity_at} from the native
        Devin CLI sessions.db (read-only)."""
        meta: Dict[str, dict] = {}
        for db_path in DEVIN_SESSION_DB_CANDIDATES:
            if not db_path.exists():
                continue
            try:
                with sqlite3.connect(f"file:{db_path}?immutable=1", uri=True, timeout=5) as conn:
                    for row in conn.execute(
                        "SELECT id, title, working_directory, created_at, last_activity_at "
                        "FROM sessions WHERE hidden = 0"
                    ):
                        meta[row[0]] = {
                            "title": row[1],
                            "cwd": row[2],
                            "created_at": row[3] or 0,
                            "last_activity_at": row[4] or 0,
                        }
            except Exception as exc:
                _log(f"native sessions.db read failed ({db_path}): {exc}")
            if meta:
                break
        return meta

    @staticmethod
    def _iso8601(ts: float) -> Optional[str]:
        if not ts:
            return None
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))

    @staticmethod
    def _derive_title_from_history(external_id: str) -> Optional[str]:
        """Fall back to the first user prompt as a title when neither the
        supervisor state nor the native DB has one."""
        try:
            for event in load_session_history(external_id):
                update = (event.get("params") or {}).get("update") or {}
                if update.get("sessionUpdate") != "user_message_chunk":
                    continue
                content = update.get("content") or {}
                text = content.get("text") if isinstance(content, dict) else None
                if isinstance(text, str) and text.strip():
                    title = " ".join(text.split())[:60]
                    return title or None
        except Exception:
            pass
        return None

    def handle_session_list(self, req_id: Any, params: dict) -> None:
        cwd_filter = params.get("cwd")
        cursor = params.get("cursor")
        limit = params.get("limit", 100)
        native_meta = self._native_session_meta()
        with self.lock:
            all_sessions = []
            known_real_ids = set()
            for sid, sess in self.sessions.items():
                real_id = sess.get("realId")
                if real_id:
                    known_real_ids.add(real_id)
                meta = native_meta.get(real_id) or {}
                sess_cwd = sess.get("cwd") or meta.get("cwd") or "/"
                if cwd_filter and sess_cwd != cwd_filter:
                    continue
                updated_ts = sess.get("updatedAt") or sess.get("createdAt") or 0
                if meta.get("last_activity_at", 0) > updated_ts:
                    updated_ts = meta["last_activity_at"]
                title = sess.get("title") or meta.get("title") or self._derive_title_from_history(sid)
                if title and not sess.get("title"):
                    sess["title"] = title
                all_sessions.append({
                    "sessionId": sid,
                    "title": title or "Untitled",
                    "cwd": sess_cwd,
                    "updatedAt": self._iso8601(updated_ts),
                    "_sortTs": updated_ts,
                })
            # Surface native Devin CLI sessions the supervisor has never seen,
            # so they are importable. The native realId is used directly as the
            # listed sessionId; handle_session_load binds it to realId on import.
            for real_id, meta in native_meta.items():
                if real_id in known_real_ids:
                    continue
                sess_cwd = meta.get("cwd") or str(Path.home())
                if cwd_filter and sess_cwd != cwd_filter:
                    continue
                updated_ts = meta.get("last_activity_at") or meta.get("created_at") or time.time()
                all_sessions.append({
                    "sessionId": real_id,
                    "title": meta.get("title") or "Untitled",
                    "cwd": sess_cwd,
                    "updatedAt": self._iso8601(updated_ts),
                    "_sortTs": updated_ts,
                })
            all_sessions.sort(key=lambda s: s.get("_sortTs", 0), reverse=True)
            for s in all_sessions:
                s.pop("_sortTs", None)
            # Paginate
            start = 0
            if cursor:
                try:
                    start = int(cursor)
                except (ValueError, TypeError):
                    start = 0
            page = all_sessions[start:start + limit]
            next_cursor = start + limit if start + limit < len(all_sessions) else None
        self.send_result(req_id, {
            "sessions": page,
            **({"nextCursor": str(next_cursor)} if next_cursor else {}),
        })



    def handle_model_list(self, req_id: Any, params: dict) -> None:
        self.send_result(req_id, {"models": get_cached_models()})


    # -----------------------------------------------------------------------
    # Watchdog & background tasks
    # -----------------------------------------------------------------------

    def _child_has_active_network(self, child: Optional[NativeChild]) -> bool:
        """Check if the native child process has established TCP connections.

        During long model thinking time (especially with large context like
        127k+ tokens), the Devin ACP child sends no session/update notifications
        but is still alive and waiting for an API response. An established TCP
        connection to an external endpoint is a reliable signal that the model
        is still generating.
        """
        if child is None or not self.pool._is_alive(child):
            return False
        pid = child.proc.pid
        if not pid:
            return False
        try:
            # lsof works on both macOS and Linux. -i TCP -sTCP:ESTABLISHED
            # shows only established TCP connections. Redirect stderr to
            # DEVNULL to suppress "lsof: WARNING: can't stat()" on macOS.
            proc = subprocess.run(
                ["lsof", "-a", "-p", str(pid), "-i", "TCP", "-sTCP:ESTABLISHED"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
            # Any output line beyond the header means there's at least one
            # established TCP connection.
            lines = [l for l in (proc.stdout or "").splitlines() if l.strip()]
            has_conn = len(lines) > 0
            if has_conn:
                child.last_network_active_at = time.time()
            return has_conn
        except Exception as exc:
            _log(f"WATCHDOG network_check_failed external pid={pid} exc={exc}")
            # On check failure, be conservative and assume alive — don't kill
            # a potentially healthy turn just because lsof failed.
            return True

    def _child_has_recent_network(self, child: Optional[NativeChild]) -> bool:
        """Check if the child had network activity recently (within grace period).

        After the API response is received, the TCP connection closes but the
        Devin child needs time to process the response into session/update
        notifications. This grace period prevents killing the turn during
        that processing window.
        """
        if child is None or not self.pool._is_alive(child):
            return False
        if child.last_network_active_at <= 0:
            return False
        age = time.time() - child.last_network_active_at
        return age < NETWORK_GRACE_SECONDS

    def _turn_has_live_terminal_command(self, turn: Turn) -> bool:
        commands = [cmd for cmd in turn.active_terminal_commands.values() if cmd]
        if not commands:
            return False
        try:
            proc = subprocess.run(
                ["ps", "-axo", "pid=,ppid=,command="],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
        except Exception as exc:
            _log(f"WATCHDOG ps_check_failed external={turn.external_session_id} exc={exc}")
            return True
        haystack = proc.stdout or ""
        own_pid = str(os.getpid())
        for command in commands:
            needle = command.strip()
            if not needle:
                continue
            # Extract the first executable token from the command. Shell
            # commands like "sleep 300 && echo ..." are parsed by the shell
            # and "sleep 300" runs as a separate process. The full command
            # string won't appear in ps output, but the first token will.
            # Also handle shell wrappers (sh -lc, bash -c) by skipping them.
            first_token = None
            tokens = needle.split()
            for i, tok in enumerate(tokens):
                if tok in ("&&", "||", ";", "|", "&"):
                    break
                if tok in ("sudo", "nohup", "env", "time", "exec"):
                    continue
                if tok.startswith("-"):
                    continue
                first_token = tok
                break
            # Build a set of search needles: the full command (for simple
            # commands) and the first meaningful token (for compound commands
            # where the shell runs parts as separate processes).
            needles = set()
            if len(needle) < 80:
                needles.add(needle)
            if first_token and first_token not in ("sh", "bash", "zsh", "/bin/sh", "/bin/bash", "/bin/zsh"):
                needles.add(first_token)
            for line in haystack.splitlines():
                parts = line.strip().split(None, 2)
                if len(parts) < 3:
                    continue
                pid, _ppid, cmdline = parts
                if pid == own_pid:
                    continue
                for n in needles:
                    if n in cmdline:
                        return True
        return False

    def _activity_watchdog(self) -> None:
        while not self._shutdown:
            time.sleep(1)
            if self._shutdown:
                break
            now = time.time()
            with self.lock:
                turns = list(self.turns.values())
            for turn in turns:
                if turn.phase not in (TurnPhase.QUEUED, TurnPhase.TYPING, TurnPhase.ACTIVE, TurnPhase.COMPLETING):
                    continue
                elapsed = now - turn.started_at
                idle = now - turn.last_update_at
                if HARD_MAX_TURN_SECONDS > 0 and elapsed > HARD_MAX_TURN_SECONDS:
                    _log(f"WATCHDOG hard_timeout external={turn.external_session_id} elapsed={elapsed:.0f}")
                    turn.mark_timed_out("hard")
                    if turn.child:
                        self.pool.mark_dying(turn.child)
                        try:
                            turn.child.proc.kill()
                        except Exception:
                            pass
                elif NUDGE_IDLE_SECONDS > 0 and idle > NUDGE_IDLE_SECONDS and not turn.nudge_sent and turn.phase in (TurnPhase.TYPING, TurnPhase.ACTIVE):
                    turn.nudge_sent = True
                    if turn.received_any_update:
                        _log(
                            f"WATCHDOG nudge_skip external={turn.external_session_id} "
                            f"idle={idle:.0f} had_output=True"
                        )
                    else:
                        _log(
                            f"WATCHDOG nudge_kill external={turn.external_session_id} "
                            f"idle={idle:.0f} had_output=False"
                        )
                        turn.mark_failed()
                        if turn.child:
                            self.pool.mark_dying(turn.child)
                            try:
                                turn.child.proc.kill()
                            except Exception:
                                pass
                elif (
                    POST_OUTPUT_STALL_GRACE_SECONDS > 0
                    and turn.nudge_sent
                    and turn.received_any_update
                    and idle > NUDGE_IDLE_SECONDS + POST_OUTPUT_STALL_GRACE_SECONDS
                    and turn.phase in (TurnPhase.TYPING, TurnPhase.ACTIVE, TurnPhase.COMPLETING)
                ):
                    if self._turn_has_live_terminal_command(turn):
                        if now - turn.last_stall_check_at > 60:
                            turn.last_stall_check_at = now
                            _log(
                                f"WATCHDOG post_output_keepalive external={turn.external_session_id} "
                                f"idle={idle:.0f} live_terminal=True"
                            )
                    elif (
                        turn.active_terminal_commands
                        and PENDING_TERMINAL_STALL_SECONDS > 0
                        and idle < PENDING_TERMINAL_STALL_SECONDS
                    ):
                        if now - turn.last_stall_check_at > 60:
                            turn.last_stall_check_at = now
                            _log(
                                f"WATCHDOG post_output_keepalive external={turn.external_session_id} "
                                f"idle={idle:.0f} pending_terminal=True terminals={len(turn.active_terminal_commands)}"
                            )
                    elif (
                        turn.child
                        and self.pool._is_alive(turn.child)
                        and STDERR_KEEPALIVE_SECONDS > 0
                        and (now - turn.child.last_stderr_at) < STDERR_KEEPALIVE_SECONDS
                    ):
                        if now - turn.last_stall_check_at > 60:
                            turn.last_stall_check_at = now
                            _log(
                                f"WATCHDOG post_output_keepalive external={turn.external_session_id} "
                                f"idle={idle:.0f} stderr_active=True "
                                f"stderr_age={now - turn.child.last_stderr_at:.0f}s"
                            )
                    elif (
                        turn.child
                        and NETWORK_KEEPALIVE_SECONDS > 0
                        and idle < NUDGE_IDLE_SECONDS + NETWORK_KEEPALIVE_SECONDS
                        and self._child_has_active_network(turn.child)
                    ):
                        if now - turn.last_stall_check_at > 60:
                            turn.last_stall_check_at = now
                            _log(
                                f"WATCHDOG post_output_keepalive external={turn.external_session_id} "
                                f"idle={idle:.0f} network_active=True "
                                f"pid={turn.child.proc.pid}"
                            )
                    elif (
                        turn.child
                        and NETWORK_GRACE_SECONDS > 0
                        and self._child_has_recent_network(turn.child)
                    ):
                        if now - turn.last_stall_check_at > 60:
                            turn.last_stall_check_at = now
                            _log(
                                f"WATCHDOG post_output_keepalive external={turn.external_session_id} "
                                f"idle={idle:.0f} network_grace=True "
                                f"network_age={time.time() - turn.child.last_network_active_at:.0f}s"
                            )
                    else:
                        _log(
                            f"WATCHDOG post_output_stall_retry external={turn.external_session_id} "
                            f"idle={idle:.0f} live_terminal=False"
                        )
                        turn.mark_timed_out("post_output_stall")
                        if turn.child:
                            self.pool.mark_dying(turn.child)
                            try:
                                turn.child.proc.kill()
                            except Exception:
                                pass
                elif idle > PROMPT_TIMEOUT_SECONDS and turn.phase in (TurnPhase.TYPING, TurnPhase.ACTIVE, TurnPhase.COMPLETING):
                    _log(f"WATCHDOG idle_timeout external={turn.external_session_id} idle={idle:.0f}")
                    turn.mark_timed_out("idle")
                    if turn.child:
                        self.pool.mark_dying(turn.child)
                        try:
                            turn.child.proc.kill()
                        except Exception:
                            pass

    def _child_reaper(self) -> None:
        health_counter = 0
        while not self._shutdown:
            time.sleep(5)
            if self._shutdown:
                break
            self.pool.reap_dead()
            self.pool.kill_oldest_idle_if_over_max()
            self.pool.spawn_warm_spare()
            # Log pool health every 30s (every 6th iteration)
            health_counter += 1
            if health_counter >= 6:
                health_counter = 0
                with self.pool.lock:
                    alive = sum(1 for c in self.pool.children if self.pool._is_alive(c))
                    dead = sum(1 for c in self.pool.children if not self.pool._is_alive(c))
                    idle = sum(1 for c in self.pool.children if c.state == ChildState.IDLE and self.pool._is_alive(c))
                    busy = sum(1 for c in self.pool.children if c.state == ChildState.BUSY and self.pool._is_alive(c))
                    warm = 1 if self.pool._is_alive(self.pool.warm_spare) else 0
                    _log(f"POOL_HEALTH alive={alive} dead={dead} idle={idle} busy={busy} warm={warm}")

    # -----------------------------------------------------------------------
    # Orphan reaping
    # -----------------------------------------------------------------------

    def _reap_orphaned_devins(self) -> int:
        """Kill all devin ... acp processes that are NOT children of this or any
        concurrently-running sibling supervisor. Prevents the pileup of orphaned
        devin children from previous supervisor instances that Paseo did not tear
        down on reload. Returns count reaped.

        Critical: when Paseo restarts, it spawns a separate supervisor stdio
        process for each agent concurrently. Each supervisor must NOT kill
        children belonging to sibling supervisors — only truly orphaned ones
        whose PPID is not any running devin-paseo-supervisor.py process."""
        my_pid = os.getpid()
        reaped = 0
        try:
            out = subprocess.run(
                ["ps", "-eo", "pid=,ppid=,command="],
                capture_output=True, text=True, timeout=5,
            )
        except Exception as exc:
            _log(f"REAP_ORPHANS ps error: {exc}")
            return 0

        # Build set of all currently-running supervisor PIDs (including self).
        # The process may appear as either the source filename or the stdio
        # symlink that Paseo actually launches.
        supervisor_pids = {my_pid}
        for line in out.stdout.splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            try:
                pid = int(parts[0])
            except ValueError:
                continue
            cmdline = parts[2]
            if _is_supervisor_cmdline(cmdline):
                supervisor_pids.add(pid)

        for line in out.stdout.splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            try:
                pid = int(parts[0])
                ppid = int(parts[1])
            except ValueError:
                continue
            cmdline = parts[2]
            if not _is_paseo_devin_acp_cmdline(cmdline):
                continue
            if pid == my_pid:
                continue
            if ppid == my_pid:
                continue
            # Skip if PPID is a sibling supervisor — it's not an orphan
            if ppid in supervisor_pids:
                continue
            # Orphaned devin acp process — kill it
            _log(f"REAP_ORPHAN pid={pid} ppid={ppid} cmd={cmdline[:80]}")
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
            reaped += 1
        if reaped:
            _log(f"REAP_ORPHANS total={reaped}")
        return reaped

    # -----------------------------------------------------------------------
    # Stdio main loop
    # -----------------------------------------------------------------------

    def stdio_main(self) -> None:
        _log("SUPERVISOR stdio start")
        # Reap orphaned devin acp processes from previous supervisor instances
        # before starting any new work. This prevents the 28-process pileup.
        self._reap_orphaned_devins()
        threading.Thread(target=self._activity_watchdog, daemon=True).start()
        threading.Thread(target=self._child_reaper, daemon=True).start()
        for raw in sys.stdin:
            raw = raw.strip()
            if not raw:
                continue
            try:
                req = json.loads(raw)
            except json.JSONDecodeError as exc:
                _log(f"PARSE_ERROR {exc}")
                continue
            _log(f"STDIO← {json.dumps(_compact_for_log(req), separators=(',', ':'))}")
            method = req.get("method")
            req_id = req.get("id")
            params = req.get("params") or {}
            try:
                if method is None and req_id in self._parent_pending_rpc:
                    self._parent_pending_rpc[req_id].put(req)
                    continue
                if method == "initialize":
                    self.handle_initialize(req_id, params)
                elif method == "session/new":
                    self.handle_session_new(req_id, params)
                elif method == "session/load":
                    self.handle_session_load(req_id, params)
                elif method == "session/prompt":
                    self.handle_session_prompt(req_id, params)
                elif method == "session/cancel":
                    self.handle_session_cancel(req_id, params)
                elif method == "session/close":
                    self.handle_session_close(req_id, params)
                elif method == "session/set_mode":
                    self.handle_session_set_mode(req_id, params)
                elif method == "session/set_model":
                    self.handle_session_set_model(req_id, params)
                elif method == "session/set_config_option":
                    # Normalize to set_model or set_mode
                    config_id = params.get("configId") or params.get("optionId")
                    if config_id == "model":
                        self.handle_session_set_model(req_id, params)
                    elif config_id == "mode":
                        self.handle_session_set_mode(req_id, params)
                    elif config_id == "effort":
                        self.handle_session_set_effort(req_id, params)
                    elif config_id == "sidekick":
                        self.handle_session_set_sidekick(req_id, params)
                    else:
                        self.handle_session_set_feature(req_id, params, config_id)
                elif method == "session/delete":
                    self.handle_session_delete(req_id, params)
                elif method == "session/list":
                    self.handle_session_list(req_id, params)
                elif method == "model/list":
                    self.handle_model_list(req_id, params)
                elif isinstance(method, str) and (method.startswith("_cognition.ai/") or method.startswith("cognition.ai/")):
                    self.handle_native_passthrough(req_id, method, params)
                else:
                    self.send_error(req_id, -32601, f"Method not found: {method}")
            except Exception as exc:
                _log(f"HANDLER_ERROR method={method} exc={exc}\n{traceback.format_exc()}")
                self.send_error(req_id, -32000, str(exc))
        _log("SUPERVISOR stdio EOF")
        self._shutdown = True
        self.pool.stop_all()

    # -----------------------------------------------------------------------
    # Daemon main loop
    # -----------------------------------------------------------------------

    def daemon_main(self) -> None:
        _log("=" * 60)
        _log("SUPERVISOR daemon start")
        threading.Thread(target=self._activity_watchdog, daemon=True).start()
        threading.Thread(target=self._child_reaper, daemon=True).start()
        PID_PATH.write_text(str(os.getpid()))
        ensure_private_dir(SOCK_PATH.parent)
        # Remove stale socket
        if SOCK_PATH.exists():
            try:
                test = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                test.settimeout(1.0)
                test.connect(str(SOCK_PATH))
                test.close()
                _log("Daemon already running, exiting")
                sys.exit(1)
            except (socket.error, OSError):
                try:
                    SOCK_PATH.unlink()
                except OSError:
                    pass
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(SOCK_PATH))
        srv.listen(5)
        srv.settimeout(1.0)
        _log(f"Listening on {SOCK_PATH}")

        def _on_signal(signum, _frame):
            sig_name = signal.Signals(signum).name
            _log(f"Received {sig_name}, shutting down")
            self._shutdown = True
            try:
                srv.close()
            except Exception:
                pass
            self.pool.stop_all()
            for p in (SOCK_PATH, PID_PATH):
                try:
                    if p.exists():
                        p.unlink()
                except Exception:
                    pass
            sys.exit(0)

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

        while not self._shutdown:
            try:
                conn, _ = srv.accept()
                threading.Thread(target=self._handle_daemon_client, args=(conn,), daemon=True).start()
            except socket.timeout:
                continue
            except OSError:
                break
        _log("Daemon main loop exited")

    def _handle_daemon_client(self, conn: socket.socket) -> None:
        try:
            with conn.makefile("r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        req = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    method = req.get("method", "")
                    req_id = req.get("id")
                    params = req.get("params", {})
                    try:
                        if method == "initialize":
                            self.handle_initialize(req_id, params)
                        elif method == "session/new":
                            self.handle_session_new(req_id, params)
                        elif method == "session/load":
                            self.handle_session_load(req_id, params)
                        elif method == "session/prompt":
                            threading.Thread(target=self.handle_session_prompt, args=(req_id, params), daemon=True).start()
                            continue
                        elif method == "session/cancel":
                            self.handle_session_cancel(req_id, params)
                        elif method == "session/close":
                            self.handle_session_close(req_id, params)
                        elif method == "session/set_mode":
                            self.handle_session_set_mode(req_id, params)
                        elif method == "session/set_model":
                            self.handle_session_set_model(req_id, params)
                        elif method == "session/set_config_option":
                            config_id = params.get("configId") or params.get("optionId")
                            if config_id == "model":
                                self.handle_session_set_model(req_id, params)
                            elif config_id == "mode":
                                self.handle_session_set_mode(req_id, params)
                            elif config_id == "effort":
                                self.handle_session_set_effort(req_id, params)
                            elif config_id == "sidekick":
                                self.handle_session_set_sidekick(req_id, params)
                            else:
                                self.handle_session_set_feature(req_id, params, config_id)
                        elif method == "session/delete":
                            self.handle_session_delete(req_id, params)
                        elif method == "session/list":
                            self.handle_session_list(req_id, params)
                        elif method == "model/list":
                            self.handle_model_list(req_id, params)
                        elif isinstance(method, str) and (method.startswith("_cognition.ai/") or method.startswith("cognition.ai/")):
                            self.handle_native_passthrough(req_id, method, params)
                        else:
                            self.send_error(req_id, -32601, f"Method not found: {method}")
                    except Exception as exc:
                        _log(f"HANDLER_ERROR method={method} exc={exc}")
                        self.send_error(req_id, -32000, str(exc))
                    # Note: in daemon mode we need to write to the socket, not stdout
                    # This is a limitation — daemon mode needs a per-client output queue.
                    # For simplicity, daemon mode writes to stdout which is wrong.
                    # TODO: refactor to use per-client queues.
        except BrokenPipeError:
            pass
        except Exception as exc:
            _log(f"Client handler error: {exc}")
        finally:
            conn.close()

    # -----------------------------------------------------------------------
    # Thin client
    # -----------------------------------------------------------------------

    def client_main(self) -> None:
        if SOCK_PATH.exists():
            try:
                test = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                test.settimeout(1.0)
                test.connect(str(SOCK_PATH))
                test.close()
            except (socket.error, OSError):
                try:
                    SOCK_PATH.unlink()
                except OSError:
                    pass
        if not SOCK_PATH.exists():
            _log("Auto-starting daemon")
            subprocess.Popen([sys.executable, __file__, "--daemon"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(20):
                time.sleep(0.3)
                try:
                    test = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    test.settimeout(1.0)
                    test.connect(str(SOCK_PATH))
                    test.close()
                    break
                except (socket.error, OSError):
                    pass
            else:
                _log("Daemon did not start")
                sys.exit(1)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(None)
        try:
            sock.connect(str(SOCK_PATH))
        except (socket.error, OSError) as exc:
            _log(f"Failed to connect: {exc}")
            sys.exit(1)
        done = threading.Event()

        def reader():
            try:
                with sock.makefile("r") as f:
                    for line in f:
                        sys.stdout.write(line)
                        sys.stdout.flush()
            except Exception:
                pass
            finally:
                done.set()

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        try:
            for line in sys.stdin:
                sock.sendall(line.encode())
        except BrokenPipeError:
            pass
        done.wait(timeout=10.0)
        sock.close()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    supervisor = Supervisor()
    invoked_name = os.path.basename(sys.argv[0])
    if invoked_name in ("devin-acp-client", "devin-acp-client.py") or "--client" in sys.argv:
        supervisor.client_main()
    elif "--daemon" in sys.argv:
        supervisor.daemon_main()
    else:
        supervisor.stdio_main()
