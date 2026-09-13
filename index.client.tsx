import type { PluginClientContext } from "@getpaseo/plugin/client";
import { UsageStatsCard } from "./client/UsageStatsCard";
import { devinRestartDaemon } from "./shared/setup";
import {
  USAGE_STATS_TITLE,
  parseUsageStatsText,
  usageStatsData,
} from "./shared/usageStats";

export default function contribute(client: PluginClientContext) {
  client.addTimelineTransformer({
    id: "devin-usage-stats",
    query: { itemType: "tool_call" },
    transform({ item }) {
      const title =
        typeof item.metadata?.title === "string" ? item.metadata.title : undefined;
      if (item.name !== "think" || title !== USAGE_STATS_TITLE) {
        return undefined;
      }
      if (item.detail.type !== "plain_text" || !item.detail.text) {
        return undefined;
      }
      const data = parseUsageStatsText(item.detail.text);
      if (!data) {
        return undefined;
      }
      return {
        items: [
          { type: "plugin", kind: "devin-usage-stats", version: 1, data },
        ],
      };
    },
  });
  client.addTimelineRenderer({
    kind: "devin-usage-stats",
    version: 1,
    schema: usageStatsData,
    Component: UsageStatsCard,
  });
  client.addCommandCenterItem({
    id: "devin-restart-daemon",
    title: "Devin: restart daemon to apply integration updates",
    icon: "RefreshCw",
    context: "global",
    async onSelect({ rpc }) {
      await rpc(devinRestartDaemon, {});
    },
  });
  return () => {};
}
