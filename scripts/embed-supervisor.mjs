#!/usr/bin/env node
// Regenerate server/supervisor-source.ts from supervisor/devin-paseo-supervisor.py.
import { readFileSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const src = readFileSync(join(root, "supervisor", "devin-paseo-supervisor.py"));
const b64 = src.toString("base64");
let out =
  "// Generated from supervisor/devin-paseo-supervisor.py — do not edit.\n" +
  "// Regenerate: npm run sync-supervisor\n" +
  "export const SUPERVISOR_SOURCE_B64 =\n";
for (let i = 0; i < b64.length; i += 120) {
  out += `  ${JSON.stringify(b64.slice(i, i + 120))} +\n`;
}
out += '  "";\n';
writeFileSync(join(root, "server", "supervisor-source.ts"), out);
console.log(`embedded ${src.length} bytes`);
