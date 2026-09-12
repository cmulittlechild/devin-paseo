import { z } from "zod";

export const USAGE_STATS_TITLE = "Response statistics";

export const usageStatsData = z.object({
  model: z.string().optional(),
  inputTokens: z.number().optional(),
  outputTokens: z.number().optional(),
  thinkingTokens: z.number().optional(),
  cachedTokens: z.number().optional(),
  requests: z.number().optional(),
  contextUsed: z.number().optional(),
  contextSize: z.number().optional(),
  contextPercent: z.number().optional(),
  firstWordSeconds: z.number().optional(),
  allTimeSeconds: z.number().optional(),
});

export type UsageStatsData = z.infer<typeof usageStatsData>;

function parseCount(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const n = Number(value.replace(/,/g, ""));
  return Number.isFinite(n) ? n : undefined;
}

/**
 * Parse the plain-text stats lines emitted by the devin-paseo-supervisor
 * "Response statistics" tool_call into structured data.
 */
export function parseUsageStatsText(text: string): UsageStatsData | undefined {
  const data: UsageStatsData = {};
  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    let m = line.match(/^Model\s+(.+)$/);
    if (m) {
      data.model = m[1].trim();
      continue;
    }
    m = line.match(/^Input tokens\s+([\d,]+)/);
    if (m) {
      data.inputTokens = parseCount(m[1]);
      continue;
    }
    m = line.match(/^Output tokens\s+([\d,]+)/);
    if (m) {
      data.outputTokens = parseCount(m[1]);
      continue;
    }
    m = line.match(/^Thinking tokens\s+~?([\d,]+)/);
    if (m) {
      data.thinkingTokens = parseCount(m[1]);
      continue;
    }
    m = line.match(/^Cached tokens\s+([\d,]+)/);
    if (m) {
      data.cachedTokens = parseCount(m[1]);
      continue;
    }
    m = line.match(/^Requests\s+([\d,]+)/);
    if (m) {
      data.requests = parseCount(m[1]);
      continue;
    }
    m = line.match(/^Context\s+([\d,]+)\s*\/\s*([\d,]+)\s*\((\d+)%\)/);
    if (m) {
      data.contextUsed = parseCount(m[1]);
      data.contextSize = parseCount(m[2]);
      data.contextPercent = parseCount(m[3]);
      continue;
    }
    m = line.match(/^Context\s+([\d,]+)\s*tokens?/);
    if (m) {
      data.contextUsed = parseCount(m[1]);
      continue;
    }
    m = line.match(/^First word\s+([\d.]+)s/);
    if (m) {
      data.firstWordSeconds = Number(m[1]);
      continue;
    }
    m = line.match(/^All time\s+(.+)$/);
    if (m) {
      data.allTimeSeconds = parseDuration(m[1]);
      continue;
    }
  }
  return Object.keys(data).length > 0 ? data : undefined;
}

/** Parse "12h 5m", "3m 20s", "42s" into seconds. */
export function parseDuration(text: string): number | undefined {
  const t = text.trim();
  let total = 0;
  let matched = false;
  const re = /(\d+(?:\.\d+)?)\s*(h|m|s)\b/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(t)) !== null) {
    matched = true;
    const v = Number(m[1]);
    total += m[2] === "h" ? v * 3600 : m[2] === "m" ? v * 60 : v;
  }
  return matched ? total : undefined;
}

/** Format seconds as "12h 5m", "3m 20s", or "42s". */
export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}
