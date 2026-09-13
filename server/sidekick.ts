import { existsSync, readFileSync, readdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

function paseoHome(): string {
  return process.env.PASEO_HOME ?? join(homedir(), ".paseo");
}

function supervisorDir(): string {
  return join(paseoHome(), "devin-supervisor");
}

/** Locate ~/.paseo/agents/<group>/<agentId>.json */
function findAgentFile(agentId: string): Record<string, unknown> | null {
  const agentsDir = join(paseoHome(), "agents");
  try {
    for (const group of readdirSync(agentsDir)) {
      const file = join(agentsDir, group, `${agentId}.json`);
      if (existsSync(file)) {
        return JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
      }
    }
  } catch {
    // ignore
  }
  return null;
}

function readJson(path: string): Record<string, unknown> | null {
  try {
    return JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
  } catch {
    return null;
  }
}

export function sidekickInfo(agentId: string): {
  fusion: boolean;
  sidekick: string | null;
  options: { id: string; label: string; description?: string }[];
} {
  const agent = findAgentFile(agentId);
  if (!agent) {
    return { fusion: false, sidekick: null, options: [] };
  }
  const config = (agent.config ?? {}) as Record<string, unknown>;
  const featureValues = (config.featureValues ?? {}) as Record<string, unknown>;
  const model = typeof config.model === "string" ? config.model : "";
  const runtimeInfo = (agent.runtimeInfo ?? {}) as Record<string, unknown>;
  const sessionId =
    typeof runtimeInfo.sessionId === "string" ? runtimeInfo.sessionId : null;

  // Supervisor state carries the resolved family + live sidekick choice.
  const state = readJson(join(supervisorDir(), "state.json"));
  const sessions = (state?.sessions ?? {}) as Record<string, Record<string, unknown>>;
  const session = sessionId ? sessions[sessionId] : undefined;
  const sessionModel = typeof session?.model === "string" ? session.model : "";
  const familyId = model.startsWith("fusion/")
    ? model
    : sessionModel.startsWith("fusion/")
      ? sessionModel
      : "";

  const cache = readJson(join(supervisorDir(), "cache.json"));
  const families = (cache?.models ?? []) as Array<Record<string, unknown>>;
  const family = families.find((entry) => entry.id === familyId);
  const labels = (family?.sidekick_labels ?? {}) as Record<
    string,
    { name?: string; description?: string }
  >;
  const options = Array.isArray(family?.sidekicks)
    ? (family.sidekicks as unknown[])
        .filter((v): v is string => typeof v === "string")
        .map((id) => ({
          id,
          label: labels[id]?.name ?? id,
          description: labels[id]?.description,
        }))
    : [];

  const sidekick =
    (typeof session?.sidekick === "string" ? session.sidekick : null) ??
    (typeof featureValues.sidekick === "string"
      ? (featureValues.sidekick as string)
      : null);

  return { fusion: familyId !== "", sidekick, options };
}

function daemonUrl(): string {
  const config = readJson(join(paseoHome(), "config.json"));
  const daemon = (config?.daemon ?? {}) as Record<string, unknown>;
  const listen = daemon.listen;
  if (typeof listen === "string" && listen) {
    return `ws://${listen}/ws`;
  }
  return "ws://127.0.0.1:6767/ws";
}

/**
 * Paseo's public SDK doesn't expose feature writes, so we speak the daemon
 * wire protocol directly: raw `hello`, then a session-enveloped
 * `set_agent_feature_request` — the same call the app UI issues.
 */
export function setSidekick(
  agentId: string,
  sidekick: string,
): Promise<{ ok: boolean; error?: string }> {
  const requestId = `devin-plugin-${Date.now()}`;
  return new Promise((resolve) => {
    let settled = false;
    const done = (result: { ok: boolean; error?: string }) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        ws.close();
      } catch {
        // ignore
      }
      resolve(result);
    };
    // `any` keeps the plugin boundary checker off the DOM WebSocket type graph.
    const WS = (globalThis as Record<string, unknown>).WebSocket as any;
    const ws = new WS(daemonUrl()) as any;
    const timer = setTimeout(
      () => done({ ok: false, error: "daemon timeout" }),
      15000,
    );
    ws.onopen = () => {
      ws.send(
        JSON.stringify({
          type: "hello",
          clientId: "devin-integration-plugin",
          clientType: "cli",
          protocolVersion: 1,
          capabilities: {},
        }),
      );
    };
    ws.onmessage = (event: { data?: unknown }) => {
      let message: Record<string, unknown>;
      try {
        const env = JSON.parse(String(event.data)) as Record<string, unknown>;
        message = (env.message ?? env) as Record<string, unknown>;
      } catch {
        return;
      }
      const payload = (message.payload ?? {}) as Record<string, unknown>;
      if (message.type === "status" && payload.status === "server_info") {
        ws.send(
          JSON.stringify({
            type: "session",
            message: {
              type: "set_agent_feature_request",
              agentId,
              featureId: "sidekick",
              value: sidekick,
              requestId,
            },
          }),
        );
      } else if (message.type === "set_agent_feature_response") {
        done(
          payload.accepted === true
            ? { ok: true }
            : { ok: false, error: String(payload.error ?? "rejected") },
        );
      } else if (message.type === "rpc_error") {
        done({ ok: false, error: String(payload.error ?? "rpc_error") });
      }
    };
    ws.onerror = () => done({ ok: false, error: "daemon connection failed" });
  });
}
