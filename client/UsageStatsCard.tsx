import type { PluginTimelineItemProps } from "@getpaseo/plugin/client";
import { useMemo, useState } from "react";
import { Pressable, Text, View } from "react-native";
import type { UsageStatsData } from "../shared/usageStats";

function formatTokens(n: number | undefined): string {
  if (n === undefined) return "—";
  return n.toLocaleString("en-US");
}

function StatCell({
  label,
  value,
  mutedColor,
  fgColor,
}: {
  label: string;
  value: string;
  mutedColor: string;
  fgColor: string;
}) {
  return (
    <View style={{ flex: 1, minWidth: 110, gap: 4 }}>
      <Text style={{ color: mutedColor, fontSize: 12 }}>{label}</Text>
      <Text style={{ color: fgColor, fontSize: 14, fontWeight: "500" }}>{value}</Text>
    </View>
  );
}

export function UsageStatsCard({
  item,
  theme,
  layout,
}: PluginTimelineItemProps<UsageStatsData>) {
  const [expanded, setExpanded] = useState(false);
  const d = item.data;
  const styles = useMemo(
    () => ({
      card: {
        borderRadius: 10,
        borderWidth: 1,
        borderColor: theme.colors.border,
        backgroundColor: theme.colors.surface1,
        paddingHorizontal: layout.compact ? 12 : 14,
        paddingVertical: 10,
        gap: 10,
        alignSelf: "flex-start" as const,
        minWidth: 280,
      },
      pill: {
        flexDirection: "row" as const,
        alignItems: "center" as const,
        gap: 6,
        borderRadius: 14,
        borderWidth: 1,
        borderColor: theme.colors.border,
        backgroundColor: theme.colors.surface1,
        paddingHorizontal: 10,
        paddingVertical: 5,
        alignSelf: "flex-start" as const,
      },
      pillText: {
        color: theme.colors.foregroundMuted,
        fontSize: 11,
        fontWeight: "600" as const,
        letterSpacing: 0.5,
      },
      headerRow: {
        flexDirection: "row" as const,
        justifyContent: "space-between" as const,
        alignItems: "center" as const,
      },
      headerLeft: {
        flexDirection: "row" as const,
        alignItems: "center" as const,
        gap: 8,
      },
      chevron: { color: theme.colors.foregroundMuted, fontSize: 12 },
      header: {
        color: theme.colors.foregroundMuted,
        fontSize: 11,
        fontWeight: "600" as const,
        letterSpacing: 0.8,
      },
      model: { color: theme.colors.foregroundMuted, fontSize: 12 },
      modelName: { color: theme.colors.foreground, fontSize: 12 },
      divider: { height: 1, backgroundColor: theme.colors.border },
      statsRow: {
        flexDirection: "row" as const,
        flexWrap: "wrap" as const,
        gap: 12,
        rowGap: 10,
      },
      footer: { color: theme.colors.foregroundMuted, fontSize: 12 },
    }),
    [theme, layout.compact],
  );

  const footerParts: string[] = [];
  if (d.contextUsed !== undefined) {
    footerParts.push(
      `Context ${formatTokens(d.contextUsed)} / ${formatTokens(d.contextSize)}` +
        (d.contextPercent !== undefined ? ` (${d.contextPercent}%)` : "") +
        " tokens",
    );
  }
  if (d.firstWordSeconds !== undefined) {
    footerParts.push(`First word ${d.firstWordSeconds}s`);
  }
  if (d.allTimeSeconds !== undefined) {
    footerParts.push(`All time ${d.allTimeSeconds}s`);
  }

  if (!expanded) {
    const summaryParts: string[] = [];
    if (d.inputTokens !== undefined) {
      summaryParts.push(`${formatTokens(d.inputTokens)} in`);
    }
    if (d.outputTokens !== undefined) {
      summaryParts.push(`${formatTokens(d.outputTokens)} out`);
    }
    if (d.cachedTokens !== undefined) {
      summaryParts.push(`${formatTokens(d.cachedTokens)} cached`);
    }
    return (
      <View style={{ alignItems: "flex-start", alignSelf: "stretch" }}>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Expand response statistics"
          onPress={() => setExpanded(true)}
          style={styles.pill}
        >
          <Text style={styles.chevron}>▸</Text>
          <Text style={styles.pillText}>
            {summaryParts.join(" / ") || "STATS"}
          </Text>
        </Pressable>
      </View>
    );
  }

  return (
    <View style={styles.card}>
      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Collapse response statistics"
        onPress={() => setExpanded(false)}
        style={styles.headerRow}
      >
        <View style={styles.headerLeft}>
          <Text style={styles.chevron}>▾</Text>
          <Text style={styles.header}>RESPONSE STATISTICS</Text>
        </View>
        {d.model ? (
          <Text style={styles.model}>
            Model <Text style={styles.modelName}>{d.model}</Text>
          </Text>
        ) : null}
      </Pressable>
      <View style={styles.divider} />
      <View style={styles.statsRow}>
            <StatCell
              label="Input tokens"
              value={formatTokens(d.inputTokens)}
              mutedColor={theme.colors.foregroundMuted}
              fgColor={theme.colors.foreground}
            />
            <StatCell
              label="Output tokens"
              value={formatTokens(d.outputTokens)}
              mutedColor={theme.colors.foregroundMuted}
              fgColor={theme.colors.foreground}
            />
            {d.thinkingTokens !== undefined ? (
              <StatCell
                label="Thinking tokens (est.)"
                value={`~${formatTokens(d.thinkingTokens)}`}
                mutedColor={theme.colors.foregroundMuted}
                fgColor={theme.colors.foreground}
              />
            ) : null}
            <StatCell
              label="Cached tokens"
              value={formatTokens(d.cachedTokens)}
              mutedColor={theme.colors.foregroundMuted}
              fgColor={theme.colors.foreground}
            />
            {d.requests !== undefined ? (
              <StatCell
                label="Requests"
                value={formatTokens(d.requests)}
                mutedColor={theme.colors.foregroundMuted}
                fgColor={theme.colors.foreground}
              />
            ) : null}
          </View>
      {footerParts.length > 0 ? (
        <>
          <View style={styles.divider} />
          <Text style={styles.footer}>{footerParts.join("   ·   ")}</Text>
        </>
      ) : null}
    </View>
  );
}
