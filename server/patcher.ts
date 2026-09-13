import { readFileSync, writeFileSync } from "node:fs";

/**
 * Idempotent patcher for Paseo's generic ACP adapter
 * (server/agent/providers/acp-agent.js).
 *
 * The plugin API cannot reach the provider event pipeline, so the Devin
 * supervisor marks subagent traffic with `_meta["paseo/subagent"]` and
 * `_meta["paseo/subagentTimeline"]` on valid ACP updates, and this patch
 * teaches the adapter to translate them into native `provider_subagent`
 * events. It also enables usage_update forwarding and the rewind
 * capability the supervisor exposes via `cognition.ai/revert`.
 *
 * Every patch is a literal find/replace against the unpatched 0.8.x dist
 * build. If an anchor is missing (Paseo upgrade changed the code), the
 * patch reports the failure instead of corrupting the file.
 */

const MARKER = "paseo/subagent";

interface Patch {
  id: string;
  find: string;
  replace: string;
  /** apply to every occurrence */
  all?: boolean;
}

const PATCHES: Patch[] = [
  {
    // Forward Devin's context-window usage fields.
    id: "usage-meta",
    find: `    return {
        inputTokens: usage.inputTokens ?? undefined,
        outputTokens: usage.outputTokens ?? undefined,
        cachedInputTokens: usage.cachedReadTokens ?? undefined,
    };
}`,
    replace: `    const meta = usage._meta ?? {};
    return {
        inputTokens: usage.inputTokens ?? undefined,
        outputTokens: usage.outputTokens ?? undefined,
        cachedInputTokens: usage.cachedReadTokens ?? undefined,
        contextWindowUsedTokens: usage.contextWindowUsedTokens ?? meta["devin/contextUsed"] ?? undefined,
        contextWindowMaxTokens: usage.contextWindowMaxTokens ?? meta["devin/contextSize"] ?? undefined,
    };
}`,
  },
  {
    id: "subagent-store-init",
    find: `        this.terminalEntries = new Map();
        this.persistedHistory = [];`,
    replace: `        this.terminalEntries = new Map();
        this.persistedHistory = [];
        this.persistedSubagentEvents = [];`,
  },
  {
    // Advertise Paseo's rewind UI when the agent supports cognition.ai/revert.
    id: "rewind-capability",
    all: true,
    find: `            this.agentCapabilities = spawned.initialize.agentCapabilities ?? null;`,
    replace: `            this.agentCapabilities = spawned.initialize.agentCapabilities ?? null;
            if (this.agentCapabilities?._meta?.["cognition.ai/revert"]) {
                this.capabilities = { ...this.capabilities, supportsRewindBoth: true };
            }`,
  },
  {
    // Replay persisted provider_subagent events after timeline history.
    id: "subagent-replay",
    find: `        const history = [...this.persistedHistory];
        this.persistedHistory.length = 0;
        this.historyPending = false;
        for (const item of history) {
            yield { type: "timeline", provider: this.provider, item };
        }
    }`,
    replace: `        const history = [...this.persistedHistory];
        const subagentEvents = [...this.persistedSubagentEvents];
        this.persistedHistory.length = 0;
        this.persistedSubagentEvents.length = 0;
        this.historyPending = false;
        for (const item of history) {
            yield { type: "timeline", provider: this.provider, item };
        }
        for (const event of subagentEvents) {
            yield event;
        }
    }`,
  },
  {
    // Message-level rewind: drive the agent's _cognition.ai/revert/toMessage
    // then re-hydrate history via session/load.
    id: "revert-both",
    find: `    async interrupt() {`,
    replace: `    async revertBoth(input) {
        if (!this.connection || !this.sessionId) {
            throw new Error("ACP session is not connected");
        }
        await this.runACPRequest(() => this.connection.extMethod("_cognition.ai/revert/toMessage", {
            sessionId: this.sessionId,
            messageId: input.messageId,
            force: true,
        }));
        this.persistedHistory = [];
        this.persistedSubagentEvents = [];
        this.toolCalls.clear();
        this.terminalEntries.clear();
        try {
            this.replayingHistory = true;
            await this.runACPRequest(() => this.connection.loadSession({
                sessionId: this.sessionId,
                cwd: this.config.cwd,
                mcpServers: this.acpMcpServers(),
            }));
            this.deliverTranslatedEvents(this.flushPendingUserMessage());
        }
        finally {
            this.replayingHistory = false;
        }
        this.historyPending = this.persistedHistory.length > 0;
    }
    async interrupt() {`,
  },
  {
    // Translate paseo/subagent and paseo/subagentTimeline _meta markers into
    // provider_subagent upsert/timeline events.
    id: "subagent-translate",
    find: `        if (params.sessionId !== this.sessionId) {
            return;
        }
        const events = this.translateSessionUpdate(params.update);`,
    replace: `        if (params.sessionId !== this.sessionId) {
            return;
        }
        const subMarker = params.update?._meta?.["paseo/subagent"];
        if (subMarker && subMarker.id) {
            this.deliverTranslatedEvents([{
                type: "provider_subagent",
                provider: this.provider,
                event: {
                    type: "upsert",
                    id: subMarker.id,
                    ...(subMarker.title ? { title: subMarker.title } : {}),
                    ...(subMarker.description ? { description: subMarker.description } : {}),
                    ...(subMarker.subtitle ? { subtitle: subMarker.subtitle } : {}),
                    ...(subMarker.status ? { status: subMarker.status } : {}),
                    ...(subMarker.toolCallId ? { toolCallId: subMarker.toolCallId } : {}),
                },
            }]);
            return;
        }
        const subTimelineId = params.update?._meta?.["paseo/subagentTimeline"];
        const events = this.translateSessionUpdate(params.update);
        if (subTimelineId) {
            const wrapped = events.map((ev) => ev.type === "timeline"
                ? { type: "provider_subagent", provider: this.provider, event: { type: "timeline", id: subTimelineId, item: ev.item } }
                : ev);
            this.deliverTranslatedEvents(wrapped);
            return;
        }`,
  },
  {
    id: "subagent-persist",
    find: `                if (event.type === "timeline") {
                    this.persistedHistory.push(event.item);
                }`,
    replace: `                if (event.type === "timeline") {
                    this.persistedHistory.push(event.item);
                }
                else if (event.type === "provider_subagent") {
                    this.persistedSubagentEvents.push(event);
                }`,
  },
  {
    // Forward ACP usage_update notifications as usage_updated events.
    id: "usage-update",
    find: `    handleUsageUpdate(update) {
        void update;
    }`,
    replace: `    handleUsageUpdate(update) {
        const meta = update?._meta ?? {};
        const num = (v) => (typeof v === "number" && Number.isFinite(v) ? v : undefined);
        const usage = {
            contextWindowUsedTokens: num(update.used) ?? num(update.usedTokens),
            contextWindowMaxTokens: num(update.size) ?? num(update.contextWindow),
            inputTokens: num(update.inputTokens) ?? num(meta["cognition.ai/inputTokens"]),
            outputTokens: num(update.outputTokens) ?? num(meta["cognition.ai/outputTokens"]),
            cachedInputTokens: num(update.cachedReadTokens) ?? num(meta["cognition.ai/cachedReadTokens"]),
        };
        if (Object.values(usage).every((v) => v === undefined)) {
            return;
        }
        this.pushEvent({
            type: "usage_updated",
            provider: this.provider,
            usage,
        });
    }`,
  },
];

export interface PatchResult {
  applied: boolean;
  alreadyApplied: boolean;
  errors: string[];
  changed: boolean;
}

const COPILOT_MARKER = "devin/sidekick";

const COPILOT_PATCHES: Patch[] = [
  {
    id: "sidekick-feature-option",
    // Declare a dynamic "Sidekick" feature select. Paseo only renders
    // config options that are declared per-provider as featureOptions —
    // generic categories never reach the UI.
    find: `export const COPILOT_AGENT_FEATURE_OPTION = {`,
    replace: `// devin/sidekick: extra feature select for Devin fusion sessions
export const DEVIN_SIDEKICK_FEATURE_OPTION = {
    id: "sidekick",
    configId: "sidekick",
    category: "sidekick",
    label: "Sidekick",
    description: "Fusion sidekick model",
    tooltip: "Select fusion sidekick model",
    emptyOptionLabel: "Default",
};
export const COPILOT_AGENT_FEATURE_OPTION = {`,
  },
  {
    id: "sidekick-feature-register",
    find: `            configFeatureOptions: [COPILOT_AGENT_FEATURE_OPTION],`,
    replace: `            configFeatureOptions: [COPILOT_AGENT_FEATURE_OPTION, DEVIN_SIDEKICK_FEATURE_OPTION],`,
  },
];

export function patchCopilotAdapter(adapterPath: string): PatchResult {
  let source: string;
  try {
    source = readFileSync(adapterPath, "utf8");
  } catch (error) {
    return {
      applied: false,
      alreadyApplied: false,
      changed: false,
      errors: [`cannot read ${adapterPath}: ${String(error)}`],
    };
  }
  if (source.includes(COPILOT_MARKER)) {
    return { applied: true, alreadyApplied: true, changed: false, errors: [] };
  }
  const errors: string[] = [];
  let next = source;
  for (const patch of COPILOT_PATCHES) {
    const idx = next.indexOf(patch.find);
    if (idx < 0) {
      errors.push(`${patch.id}: anchor not found`);
      continue;
    }
    next = next.slice(0, idx) + patch.replace + next.slice(idx + patch.find.length);
  }
  if (errors.length > 0) {
    return { applied: false, alreadyApplied: false, changed: false, errors };
  }
  try {
    writeFileSync(adapterPath, next);
  } catch (error) {
    return {
      applied: false,
      alreadyApplied: false,
      changed: false,
      errors: [`cannot write ${adapterPath}: ${String(error)}`],
    };
  }
  return { applied: true, alreadyApplied: false, changed: true, errors: [] };
}

export function patchAcpAdapter(adapterPath: string): PatchResult {
  let source: string;
  try {
    source = readFileSync(adapterPath, "utf8");
  } catch (error) {
    return {
      applied: false,
      alreadyApplied: false,
      changed: false,
      errors: [`cannot read ${adapterPath}: ${String(error)}`],
    };
  }
  if (source.includes(MARKER)) {
    return { applied: true, alreadyApplied: true, changed: false, errors: [] };
  }
  const errors: string[] = [];
  let next = source;
  for (const patch of PATCHES) {
    if (patch.all) {
      if (!next.includes(patch.find)) {
        errors.push(`${patch.id}: anchor not found`);
        continue;
      }
      next = next.split(patch.find).join(patch.replace);
    } else {
      const idx = next.indexOf(patch.find);
      if (idx < 0) {
        errors.push(`${patch.id}: anchor not found`);
        continue;
      }
      next = next.slice(0, idx) + patch.replace + next.slice(idx + patch.find.length);
    }
  }
  if (errors.length > 0) {
    return { applied: false, alreadyApplied: false, changed: false, errors };
  }
  try {
    writeFileSync(adapterPath, next);
  } catch (error) {
    return {
      applied: false,
      alreadyApplied: false,
      changed: false,
      errors: [`cannot write ${adapterPath}: ${String(error)}`],
    };
  }
  return { applied: true, alreadyApplied: false, changed: true, errors: [] };
}
