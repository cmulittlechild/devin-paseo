import type { PluginServerContext } from "@getpaseo/plugin/server";
import { devinRestartDaemon, devinStatus } from "./shared/setup";
import { devinSetSidekick, devinSidekickInfo } from "./shared/sidekick";
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
  return () => {};
}
