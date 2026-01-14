import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const REQUIRED_ENV = ["SUPA_URL", "SUPA_SERVICE_KEY"] as const;
const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(", ")}`);
  process.exit(1);
}

const SUPA_URL = process.env.SUPA_URL as string;
const SUPA_SERVICE_KEY = process.env.SUPA_SERVICE_KEY as string;
const PAGE_SIZE = Number.parseInt(process.env.INSPECT_PAGE_SIZE ?? "1000", 10);
const MAX_ROWS = Number.parseInt(process.env.INSPECT_MAX_ROWS ?? "0", 10); // 0 = all
const DISTINCT_SAMPLE_LIMIT = Number.parseInt(process.env.INSPECT_DISTINCT_LIMIT ?? "15", 10);

const client = createClient(SUPA_URL, SUPA_SERVICE_KEY, {
  auth: { persistSession: false },
});

type ColumnSpec = {
  name: string;
  type: "text" | "integer" | "numeric" | "timestamptz";
  required?: boolean;
  pattern?: RegExp;
};

const COLUMNS: ColumnSpec[] = [
  { name: "projnr", type: "text", required: true, pattern: /^HIVE\d{4}\d{7}$/ },
  { name: "projbezeichnung", type: "text" },
  { name: "statusse", type: "text" },
  { name: "projadr", type: "text" },
  { name: "rechadr", type: "text" },
  { name: "bauhradr", type: "text" },
  { name: "abtnr", type: "integer" },
  { name: "sachbearb", type: "text" },
  { name: "auftragssumme", type: "numeric" },
  { name: "beginn", type: "timestamptz" },
  { name: "projinfos", type: "text" },
  { name: "rechinfos", type: "text" },
  { name: "bauhrinfos", type: "text" },
  { name: "vorname", type: "text" },
  { name: "name", type: "text" },
  { name: "strasse", type: "text" },
  { name: "ort", type: "text" },
  { name: "plz", type: "text", pattern: /^\d{5}$/ },
  { name: "rechnungsmail", type: "text", pattern: /^[^\s@]+@[^\s@]+\.[^\s@]+$/ },
];

type ColumnStats = {
  name: string;
  type: ColumnSpec["type"];
  required: boolean;
  total: number;
  nulls: number;
  empties: number;
  invalids: number;
  minLength?: number;
  maxLength?: number;
  distinctSample: string[];
  minValue?: string | number;
  maxValue?: string | number;
};

function initStats(spec: ColumnSpec): ColumnStats {
  return {
    name: spec.name,
    type: spec.type,
    required: !!spec.required,
    total: 0,
    nulls: 0,
    empties: 0,
    invalids: 0,
    minLength: spec.type === "text" ? Number.POSITIVE_INFINITY : undefined,
    maxLength: spec.type === "text" ? 0 : undefined,
    distinctSample: [],
    minValue: undefined,
    maxValue: undefined,
  };
}

function addDistinct(stats: ColumnStats, value: string) {
  if (!value) return;
  if (stats.distinctSample.includes(value)) return;
  if (stats.distinctSample.length >= DISTINCT_SAMPLE_LIMIT) return;
  stats.distinctSample.push(value);
}

function updateStats(spec: ColumnSpec, stats: ColumnStats, value: unknown) {
  stats.total += 1;

  if (value === null || value === undefined) {
    stats.nulls += 1;
    if (spec.required) stats.invalids += 1;
    return;
  }

  if (spec.type === "text") {
    const text = String(value);
    if (text.length === 0) stats.empties += 1;
    if (spec.pattern && !spec.pattern.test(text)) stats.invalids += 1;
    if (stats.minLength !== undefined) stats.minLength = Math.min(stats.minLength, text.length);
    if (stats.maxLength !== undefined) stats.maxLength = Math.max(stats.maxLength, text.length);
    addDistinct(stats, text.trim());
    return;
  }

  if (spec.type === "integer" || spec.type === "numeric") {
    const num = typeof value === "number" ? value : Number(value);
    if (!Number.isFinite(num)) {
      stats.invalids += 1;
      return;
    }
    if (stats.minValue === undefined || num < Number(stats.minValue)) stats.minValue = num;
    if (stats.maxValue === undefined || num > Number(stats.maxValue)) stats.maxValue = num;
    return;
  }

  if (spec.type === "timestamptz") {
    const date = value instanceof Date ? value : new Date(String(value));
    if (Number.isNaN(date.getTime())) {
      stats.invalids += 1;
      return;
    }
    const iso = date.toISOString();
    if (!stats.minValue || iso < String(stats.minValue)) stats.minValue = iso;
    if (!stats.maxValue || iso > String(stats.maxValue)) stats.maxValue = iso;
  }
}

async function fetchCount(): Promise<number> {
  const { count, error } = await client
    .from("projekt")
    .select("projnr", { count: "exact", head: true });
  if (error) throw error;
  return count ?? 0;
}

async function fetchBatch(from: number, to: number) {
  const { data, error } = await client.from("projekt").select("*").range(from, to);
  if (error) throw error;
  return data ?? [];
}

async function run() {
  const total = await fetchCount();
  const statsMap = new Map<string, ColumnStats>();
  COLUMNS.forEach((spec) => statsMap.set(spec.name, initStats(spec)));

  let processed = 0;
  for (let offset = 0; offset < total; offset += PAGE_SIZE) {
    if (MAX_ROWS > 0 && processed >= MAX_ROWS) break;
    const limit = MAX_ROWS > 0 ? Math.min(PAGE_SIZE, MAX_ROWS - processed) : PAGE_SIZE;
    const rows = await fetchBatch(offset, offset + limit - 1);
    if (!rows.length) break;

    for (const row of rows as Record<string, unknown>[]) {
      for (const spec of COLUMNS) {
        const stats = statsMap.get(spec.name);
        if (!stats) continue;
        updateStats(spec, stats, row[spec.name]);
      }
    }

    processed += rows.length;
    if (rows.length < limit) break;
  }

  console.log("\nProjekt table inspection");
  console.log(`Rows total: ${total}`);
  console.log(`Rows scanned: ${processed}${MAX_ROWS > 0 ? ` (limit ${MAX_ROWS})` : ""}`);

  const output = Array.from(statsMap.values()).map((s) => ({
    column: s.name,
    type: s.type,
    required: s.required,
    nulls: s.nulls,
    empties: s.empties,
    invalids: s.invalids,
    minLength: s.minLength === Number.POSITIVE_INFINITY ? 0 : s.minLength,
    maxLength: s.maxLength,
    minValue: s.minValue,
    maxValue: s.maxValue,
    distinctSample: s.distinctSample.join(" | "),
  }));

  console.table(output);
}

run().catch((error) => {
  console.error("Inspection failed:", error?.message ?? error);
  process.exit(1);
});
