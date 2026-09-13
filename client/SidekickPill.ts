import type { PluginClientContext } from "@getpaseo/plugin/client";
import type { PluginButtonRegistration } from "@getpaseo/plugin/client";
import { devinSetSidekick, devinSidekickInfo } from "../shared/sidekick";

interface SidekickFeature {
  value: string | null;
  options: { id: string; label?: string }[];
}

/**
 * Devin fusion sessions get a second composer dropdown — the sidekick model —
 * mirroring Devin Desktop's lead/sidekick pair pickers. The menu is populated
 * from the sidekick select feature inside `agent_update` upserts when present,
 * otherwise via the `devin.sidekick_info` plugin RPC. Writes go through
 * `devin.set_sidekick` (daemon `set_agent_feature` -> supervisor).
 */
export function registerSidekickPills(client: PluginClientContext): () => void {
  const pills = new Map<string, PluginButtonRegistration>();

  function findSidekickFeature(agent: { features?: unknown }): SidekickFeature | null {
    const features = Array.isArray(agent.features) ? agent.features : [];
    for (const feature of features) {
      if (
        feature &&
        typeof feature === "object" &&
        (feature as { type?: unknown }).type === "select" &&
        (feature as { id?: unknown }).id === "sidekick"
      ) {
        const f = feature as { value?: unknown; options?: unknown };
        const options = Array.isArray(f.options) ? f.options : [];
        return {
          value: typeof f.value === "string" ? f.value : null,
          options: options
            .filter(
              (o): o is { id: string; label?: string } =>
                !!o && typeof o === "object" && typeof (o as { id?: unknown }).id === "string",
            )
            .map((o) => ({ id: o.id, label: typeof o.label === "string" ? o.label : o.id })),
        };
      }
    }
    return null;
  }

  function isDevinFusion(agent: { provider?: unknown; model?: unknown }): boolean {
    return (
      agent.provider === "devin" &&
      typeof agent.model === "string" &&
      (agent.model.startsWith("fusion/") || agent.model.startsWith("fusion-"))
    );
  }

  function applyMenu(agentId: string, feature: SidekickFeature) {
    const registration = pills.get(agentId);
    if (!registration) return;
    const current = feature.value;
    registration.update({
      label: current ?? "Sidekick",
      behavior: {
        kind: "menu",
        items:
          feature.options.length > 0
            ? feature.options.map((option) => ({
                kind: "item" as const,
                id: option.id,
                title: option.label ?? option.id,
                icon: option.id === current ? "Check" : undefined,
                behavior: {
                  kind: "action" as const,
                  onPress: async () => {
                    try {
                      const res = await client.rpc(devinSetSidekick, {
                        agentId,
                        sidekick: option.id,
                      });
                      if (!res.ok) {
                        console.error(`[devin] setSidekick failed: ${res.error}`);
                      }
                    } catch (error) {
                      console.error(`[devin] setSidekick failed: ${String(error)}`);
                    }
                  },
                },
              }))
            : [
                {
                  kind: "item" as const,
                  id: "unavailable",
                  title: "No sidekick options reported",
                  disabled: true,
                  behavior: { kind: "action" as const, onPress: () => {} },
                },
              ],
      },
    });
  }

  async function refreshViaRpc(agentId: string) {
    const registration = pills.get(agentId);
    if (!registration) return;
    try {
      const info = await client.rpc(devinSidekickInfo, { agentId });
      if (!pills.has(agentId)) return;
      if (!info.fusion) {
        registration.remove();
        pills.delete(agentId);
        return;
      }
      applyMenu(agentId, {
        value: info.sidekick,
        options: info.options.map((id) => ({ id })),
      });
    } catch (error) {
      if (!pills.has(agentId)) return;
      registration.update({
        behavior: {
          kind: "menu",
          items: [
            {
              kind: "item",
              id: "error",
              title: `Sidekick unavailable`,
              disabled: true,
              behavior: { kind: "action", onPress: () => {} },
            },
          ],
        },
      });
      console.error(`[devin] sidekickInfo failed: ${String(error)}`);
    }
  }

  function ensurePill(agent: {
    id: string;
    workspaceId?: string | null;
    provider?: unknown;
    model?: unknown;
    features?: unknown;
  }) {
    if (!agent.workspaceId || !isDevinFusion(agent)) return;
    let registration = pills.get(agent.id);
    if (!registration) {
      registration = client.addComposerPill({
        id: "devin-sidekick",
        workspaceId: agent.workspaceId,
        agentId: agent.id,
        button: {
          title: "Sidekick",
          icon: "Bot",
          label: "Sidekick",
          behavior: { kind: "menu", items: [] },
        },
      });
      pills.set(agent.id, registration);
    }
    const feature = findSidekickFeature(agent);
    if (feature) {
      applyMenu(agent.id, feature);
    } else {
      void refreshViaRpc(agent.id);
    }
  }

  // Agents already open before this plugin (re)loaded never emit an upsert —
  // enumerate them once so their pills exist immediately.
  void client.paseo.agents
    .list({})
    .then((result) => {
      const agents = Array.isArray(result)
        ? result
        : ((result as { agents?: unknown[] }).agents ?? []);
      for (const agent of agents) {
        ensurePill(agent as Parameters<typeof ensurePill>[0]);
      }
    })
    .catch((error) => {
      console.error(`[devin] agents.list failed: ${String(error)}`);
    });

  const unsubscribe = client.paseo.agents.subscribe((update) => {
    if (update.kind !== "upsert") return;
    const agent = update.agent;
    if (isDevinFusion(agent)) {
      ensurePill(agent);
    } else {
      const existing = pills.get(agent.id);
      if (existing) {
        existing.remove();
        pills.delete(agent.id);
      }
    }
  });

  return () => {
    unsubscribe();
    for (const registration of pills.values()) registration.remove();
    pills.clear();
  };
}
