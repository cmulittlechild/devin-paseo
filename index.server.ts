import type { PluginServerContext } from "@getpaseo/plugin/server";
import { devinRestartDaemon, devinStatus } from "./shared/setup";
import {
  getSetupStatus,
  restartDaemon,
  runDevinSetup,
} from "./server/install";

export default function contribute(server: PluginServerContext) {
  void runDevinSetup().catch((error) => {
    console.error(`[devin] setup failed: ${String(error)}`);
  });
  server.handle(devinStatus, async () => getSetupStatus());
  server.handle(devinRestartDaemon, async () => {
    restartDaemon();
    return { ok: true };
  });
  return () => {};
}
