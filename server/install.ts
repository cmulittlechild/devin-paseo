import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { spawn } from "node:child_process";
import { patchAcpAdapter } from "./patcher";
import { SUPERVISOR_SOURCE_B64 } from "./supervisor-source";
import type { DevinSetupStatus } from "../shared/setup";

const SUPERVISOR_NAME = "devin-supervisor-stdio";
const PROVIDER_ID = "devin";

function paseoHome(): string {
  return process.env.PASEO_HOME ?? join(homedir(), ".paseo");
}

function supervisorTarget(): string {
  return join(paseoHome(), "devin-supervisor", SUPERVISOR_NAME);
}

/** The supervisor ships embedded in the bundle — plugin server code cannot
 * reliably locate its checkout directory at runtime. */
function bundledSupervisorSource(): string {
  return Buffer.from(SUPERVISOR_SOURCE_B64, "base64").toString("utf8");
}

/** Locate the daemon config file. */
function daemonConfigPath(): string {
  return join(paseoHome(), "config.json");
}

/**
 * Ensure agents.providers.devin points at the installed supervisor.
 * Returns true when the config was changed (daemon reload required).
 */
export function ensureProviderConfig(commandPath: string): boolean {
  const configPath = daemonConfigPath();
  let config: Record<string, unknown> = {};
  try {
    config = JSON.parse(readFileSync(configPath, "utf8"));
  } catch {
    // no config yet — create a minimal one below
  }
  const agents = (config.agents ?? {}) as Record<string, unknown>;
  const providers = (agents.providers ?? {}) as Record<string, unknown>;
  const desired = {
    extends: "copilot",
    label: "Devin",
    command: [commandPath],
    enabled: true,
  };
  const current = providers[PROVIDER_ID] as Record<string, unknown> | undefined;
  const already =
    current &&
    current.enabled === true &&
    Array.isArray(current.command) &&
    current.command[0] === commandPath;
  if (already) {
    return false;
  }
  providers[PROVIDER_ID] = desired;
  agents.providers = providers;
  config.agents = agents;
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  return true;
}

/** Install the embedded supervisor under <paseoHome>/devin-supervisor/. */
export function installSupervisor(): { path: string; changed: boolean } {
  const source = bundledSupervisorSource();
  const target = supervisorTarget();
  const changed =
    !existsSync(target) || readFileSync(target, "utf8") !== source;
  if (changed) {
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, source);
    chmodSync(target, 0o755);
  }
  return { path: target, changed };
}

/** Find the generic ACP adapter inside the installed @getpaseo/server. */
export function findAcpAdapter(): string | null {
  const rel = join("dist", "server", "server", "agent", "providers", "acp-agent.js");
  try {
    const req = createRequire(join(paseoHome(), "devin-supervisor", "probe.js"));
    const pkg = req.resolve("@getpaseo/server/package.json");
    const candidate = join(dirname(pkg), rel);
    if (existsSync(candidate)) {
      return candidate;
    }
  } catch {
    // fall through to well-known paths
  }
  // Resolve via the paseo CLI's own location — its bin symlink points into
  // the npm-global node_modules that also holds @getpaseo/server.
  for (const bin of [
    join(homedir(), ".npm-global", "bin", "paseo"),
    join(homedir(), ".local", "bin", "paseo"),
    "/usr/bin/paseo",
    "/usr/local/bin/paseo",
    process.env.PASEO_CLI ?? "",
  ]) {
    try {
      if (!bin || !existsSync(bin)) continue;
      let dir = dirname(realpathSync(bin));
      for (let i = 0; i < 6; i += 1) {
        const candidate = join(dir, "node_modules", "@getpaseo", "server", rel);
        if (existsSync(candidate)) {
          return candidate;
        }
        dir = dirname(dir);
      }
    } catch {
      // try next candidate
    }
  }
  for (const root of [
    "/usr/lib/node_modules/@getpaseo/server",
    "/usr/local/lib/node_modules/@getpaseo/server",
    join(homedir(), ".npm-global", "lib", "node_modules", "@getpaseo", "server"),
    join(homedir(), ".npm", "lib", "node_modules", "@getpaseo", "server"),
  ]) {
    const candidate = join(root, rel);
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

function paseoCli(): string {
  return process.env.PASEO_CLI ?? "paseo";
}

export function reloadDaemon(): void {
  try {
    const child = spawn(paseoCli(), ["reload", "--json"], { detached: true, stdio: "ignore" });
    child.unref();
  } catch {
    // best effort — status will report restartNeeded
  }
}

export function restartDaemon(): void {
  const child = spawn(paseoCli(), ["daemon", "restart"], { detached: true, stdio: "ignore" });
  child.unref();
}

let lastStatus: DevinSetupStatus = {
  supervisorInstalled: false,
  providerConfigured: false,
  adapterPatched: false,
  patchErrors: [],
  restartNeeded: false,
  restartReasons: [],
};

export function getSetupStatus(): DevinSetupStatus {
  return lastStatus;
}

/**
 * Install supervisor, ensure provider config, patch the ACP adapter.
 * Runs on every plugin load so a Paseo upgrade that wipes the dist patch
 * is repaired automatically (one daemon restart is then required for the
 * adapter change to take effect).
 */
export async function runDevinSetup(): Promise<DevinSetupStatus> {
  const reasons: string[] = [];
  const errors: string[] = [];

  const installed = installSupervisor();
  if (installed.changed) {
    reasons.push("supervisor updated");
  }

  const providerChanged = ensureProviderConfig(installed.path);
  if (providerChanged) {
    reasons.push("provider config updated");
    reloadDaemon();
  }

  const adapterPath = findAcpAdapter();
  let adapterPatched = false;
  if (adapterPath) {
    const result = patchAcpAdapter(adapterPath);
    adapterPatched = result.applied;
    errors.push(...result.errors);
    if (result.changed) {
      reasons.push("ACP adapter patched — restart the daemon to activate");
    }
  } else {
    errors.push("@getpaseo/server ACP adapter not found");
  }

  lastStatus = {
    supervisorInstalled: true,
    supervisorPath: installed.path,
    providerConfigured: true,
    adapterPatched,
    adapterPath: adapterPath ?? undefined,
    patchErrors: errors,
    restartNeeded: reasons.length > 0,
    restartReasons: reasons,
  };
  if (reasons.length > 0) {
    console.log(`[devin] setup applied changes: ${reasons.join(", ")}`);
  }
  if (errors.length > 0) {
    console.error(`[devin] setup problems: ${errors.join("; ")}`);
  }
  return lastStatus;
}
