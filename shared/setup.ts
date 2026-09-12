import { defineRpc } from "@getpaseo/plugin";
import { z } from "zod";

export const devinSetupStatus = z.object({
  supervisorInstalled: z.boolean(),
  supervisorPath: z.string().optional(),
  providerConfigured: z.boolean(),
  adapterPatched: z.boolean(),
  adapterPath: z.string().optional(),
  patchErrors: z.array(z.string()),
  restartNeeded: z.boolean(),
  restartReasons: z.array(z.string()),
});
export type DevinSetupStatus = z.infer<typeof devinSetupStatus>;

export const devinStatus = defineRpc({
  name: "devin.status",
  input: z.object({}),
  output: devinSetupStatus,
});

export const devinRestartDaemon = defineRpc({
  name: "devin.restart_daemon",
  input: z.object({}),
  output: z.object({ ok: z.boolean() }),
});
