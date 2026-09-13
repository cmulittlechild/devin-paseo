import type { PluginClientContext } from "@getpaseo/plugin/client";
import type { PluginButtonRegistration } from "@getpaseo/plugin/client";
import { devinSetSidekick, devinSidekickInfo } from "../shared/sidekick";

/**
 * Devin fusion sessions get a second composer dropdown — the sidekick model —
 * mirroring Devin Desktop's lead/sidekick pair pickers. Paseo's built-in
 * feature-select UI doesn't render dynamic options, so we draw our own pill
 * and push the choice through `set_agent_feature` on the daemon.
 */
export function registerSidekickPills(client: PluginClientContext): () => void {
  const pills = new Map<string, PluginButtonRegistration>();

  async function refresh(agentId: string) {
    const registration = pills.get(agentId);
    if (!registration) return;
    try {
      const info = await client.rpc(devinSidekickInfo, { agentId });
      if (!info.fusion) {
        registration.remove();
        pills.delete(agentId);
        return;
      }
      const current = info.sidekick;
      registration.update({
        label: current ?? "Sidekick",
        behavior: {
          kind: "menu",
          items: info.options.map((option) => ({
            kind: "item" as const,
            id: option,
            title: option,
            icon: option === current ? "Check" : undefined,
            behavior: {
              kind: "action" as const,
              onPress: async () => {
                registration.update({ label: option });
                const res = await client.rpc(devinSetSidekick, {
                  agentId,
                  sidekick: option,
                });
                if (!res.ok) {
                  console.error(`[devin] setSidekick failed: ${res.error}`);
                }
                await refresh(agentId);
              },
            },
          })),
        },
      });
    } catch (error) {
      console.error(`[devin] sidekickInfo failed: ${String(error)}`);
    }
  }

  const unsubscribe = client.paseo.agents.subscribe((update) => {
    if (update.kind !== "upsert") return;
    const agent = update.agent;
    if (!agent.workspaceId) return;
    const fusion =
      agent.provider === "devin" &&
      typeof agent.model === "string" &&
      agent.model.startsWith("fusion/");
    if (!fusion) return;
    if (!pills.has(agent.id)) {
      pills.set(
        agent.id,
        client.addComposerPill({
          id: "devin-sidekick",
          workspaceId: agent.workspaceId,
          agentId: agent.id,
          button: {
            title: "Sidekick",
            icon: "Bot",
            label: "Sidekick",
            behavior: { kind: "menu", items: [] },
          },
        }),
      );
    }
    void refresh(agent.id);
  });

  return () => {
    unsubscribe();
    for (const registration of pills.values()) registration.remove();
    pills.clear();
  };
}
