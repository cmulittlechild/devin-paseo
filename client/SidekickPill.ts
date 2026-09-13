import type { PluginClientContext } from "@getpaseo/plugin/client";
import type { PluginButtonRegistration } from "@getpaseo/plugin/client";
import { devinSetSidekick } from "../shared/sidekick";

interface SidekickFeature {
  value: string | null;
  options: { id: string; label?: string }[];
}

/**
 * Devin fusion sessions get a second composer dropdown — the sidekick model —
 * mirroring Devin Desktop's lead/sidekick pair pickers. The daemon already
 * publishes the sidekick select inside `agent_update` upserts (`features`),
 * so the menu is built from that payload directly; only writes go through the
 * plugin RPC (daemon `set_agent_feature` -> supervisor `set_config_option`).
 */
export function registerSidekickPills(client: PluginClientContext): () => void {
  const pills = new Map<string, PluginButtonRegistration>();

  function findSidekickFeature(agent: {
    features?: unknown;
  }): SidekickFeature | null {
    const features = Array.isArray(agent.features) ? agent.features : [];
    for (const feature of features) {
      if (
        feature &&
        typeof feature === "object" &&
        (feature as { type?: unknown }).type === "select" &&
        (feature as { id?: unknown }).id === "sidekick"
      ) {
        const f = feature as {
          value?: unknown;
          options?: unknown;
        };
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

  const unsubscribe = client.paseo.agents.subscribe((update) => {
    if (update.kind !== "upsert") return;
    const agent = update.agent;
    if (!agent.workspaceId) return;
    const isDevinFusion =
      agent.provider === "devin" &&
      typeof agent.model === "string" &&
      (agent.model.startsWith("fusion/") || agent.model.startsWith("fusion-"));
    const feature = isDevinFusion ? findSidekickFeature(agent) : null;

    if (!feature) {
      if (!isDevinFusion) {
        const existing = pills.get(agent.id);
        if (existing) {
          existing.remove();
          pills.delete(agent.id);
        }
      }
      return;
    }

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

    const current = feature.value;
    const agentId = agent.id;
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
  });

  return () => {
    unsubscribe();
    for (const registration of pills.values()) registration.remove();
    pills.clear();
  };
}
