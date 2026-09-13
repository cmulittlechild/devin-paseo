import type { PluginServerContext } from "@getpaseo/plugin/server";
import { devinRestartDaemon, devinStatus } from "./shared/setup";
import {
  DEFAULT_SIDEKICK,
  devinSetSidekick,
  devinSidekickInfo,
} from "./shared/sidekick";
import {
  getSetupStatus,
  restartDaemon,
  runDevinSetup,
} from "./server/install";
import { setSidekick, sidekickInfo } from "./server/sidekick";

export default function contribute(server: PluginServerContext) {
  void runDevinSetup().catch((error) => {
    console.error(`[devin] setup failed: ${String(error)}`);
  });
  server.handle(devinStatus, async () => getSetupStatus());
  server.handle(devinRestartDaemon, async () => {
    restartDaemon();
    return { ok: true };
  });
  server.handle(devinSidekickInfo, async ({ agentId }) => sidekickInfo(agentId));
  server.handle(devinSetSidekick, async ({ agentId, sidekick }) =>
    setSidekick(agentId, sidekick),
  );
  // Fusion agents need featureValues.sidekick in the create config — the
  // adapter applies it via applyConfiguredOverrides once the session is up.
  server.before("agent.create", ({ request }) => {
    const config = request.config;
    if (!config || String(config.provider) !== "devin") return;
    const model = typeof config.model === "string" ? config.model : "";
    if (!model.startsWith("fusion/") && !model.startsWith("fusion-")) return;
    const featureValues = { ...(config.featureValues ?? {}) };
    if (featureValues.sidekick === undefined) {
      featureValues.sidekick = DEFAULT_SIDEKICK;
    }
    return { ...request, config: { ...config, featureValues } };
  });
  return () => {};
}
