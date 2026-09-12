# devin-integration

Run [Devin CLI](https://devin.ai) inside [Paseo](https://paseo.sh) with
near-native fidelity: streamed subagent cards, per-turn token statistics,
message-level rewind, and access to Paseo's agent tools from inside Devin.

## What it does

Paseo talks to Devin through a bundled **supervisor**
(`supervisor/devin-paseo-supervisor.py`) that speaks ACP on both sides:

```
Paseo daemon ──ACP──▶ devin-supervisor-stdio ──ACP──▶ devin acp
```

- **Subagent cards** — Devin's private `cognition.ai/subagent_*` metadata is
  translated into Paseo's native `provider_subagent` events, so spawned
  subagents appear in the subagent track with live tool-call timelines.
- **Response statistics** — a compact, expandable pill after each turn:
  model, input/output/cached tokens, estimated thinking tokens, context
  window usage, time-to-first-token and total time.
- **Rewind** — `/steps`, `/revert <n>`, `/fork <n>` commands drive Devin's
  `_cognition.ai/revert/*` RPCs; the adapter patch also advertises Paseo's
  native rewind affordance.
- **Paseo agent tools** — Paseo injects its agent MCP server into every
  session; the supervisor writes it into the workspace's
  `.devin/mcp_config.local.json` so Devin actually loads it (Devin ignores
  the ACP `mcpServers` param). `list_agents`, `get_agent_status`,
  `send_agent_prompt`, `create_agent`, etc. become real Devin tools.
- **History import** — existing Devin sessions are listed and replayed
  through `session/load`.

## Install

```bash
paseo plugin add cmulittlechild/devin-paseo
```

On first load the plugin:

1. Installs the supervisor to `~/.paseo/devin-supervisor/`.
2. Ensures `agents.providers.devin` exists in `~/.paseo/config.json`
   (extends the `copilot` ACP provider) and reloads the daemon if changed.
3. Idempotently patches `@getpaseo/server`'s `acp-agent.js` so
   `provider_subagent` events, usage updates, and rewind work.

Then **restart the daemon once** (`paseo daemon restart`, or the
"Devin: restart daemon" Command Center item) so the patched adapter loads.

After a Paseo upgrade replaces `acp-agent.js`, the patcher re-applies on
the next daemon start; restart once more to activate.

## Requirements

- Paseo ≥ 0.8.0 with `pluginsEnabled: true`
- Devin CLI installed and authenticated (`devin` on PATH)
- Python 3

## Files

| Path | Purpose |
| --- | --- |
| `supervisor/devin-paseo-supervisor.py` | ACP↔ACP supervisor (sessions, subagents, usage, revert, MCP bridging) |
| `server/install.ts` | Installs supervisor, ensures provider config, applies adapter patch |
| `server/patcher.ts` | Idempotent anchor-based patcher for `acp-agent.js` |
| `index.client.tsx` | Timeline transformer/renderer for the stats pill + daemon-restart command |
| `client/UsageStatsCard.tsx` | The expandable statistics pill |
