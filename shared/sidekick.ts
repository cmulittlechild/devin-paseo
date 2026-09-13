import { defineRpc } from "@getpaseo/plugin";
import { z } from "zod";

/** Current sidekick + available options for a fusion Devin agent. */
export const devinSidekickInfo = defineRpc({
  name: "devin.sidekick_info",
  input: z.object({ agentId: z.string() }),
  output: z.object({
    fusion: z.boolean(),
    sidekick: z.string().nullable(),
    options: z.array(z.string()),
  }),
});

export const devinSetSidekick = defineRpc({
  name: "devin.set_sidekick",
  input: z.object({ agentId: z.string(), sidekick: z.string() }),
  output: z.object({ ok: z.boolean(), error: z.string().optional() }),
});
