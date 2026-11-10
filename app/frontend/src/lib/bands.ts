export type Band = "Active" | "Busy" | "Crowded";

// One meaning per colour, used identically in charts, badges and gauges.
export const BAND_COLORS: Record<Band, string> = { Active: "#2E7D32", Busy: "#F9A825", Crowded: "#C62828" };
export const NEUTRAL = "#1565C0";
export const LOW_SAMPLE = "#9E9E9E";
export const GEDWIN = { busy: 2.06, crowded: 2.34 };
export const EDWIN = { busy: 1.5, crowded: 2 };
const NEDOCS_LABELS = ["Not busy", "Busy", "Extremely busy", "Overcrowded", "Severely overcrowded", "Dangerously overcrowded"];

type Num = number | null | undefined;

export function gedwinBand(g: Num): Band {
  if (g === null || g === undefined) return "Active"; // census 0
  return g < GEDWIN.busy ? "Active" : g <= GEDWIN.crowded ? "Busy" : "Crowded";
}

export function edwinBand(e: Num): Band | null {
  if (e === null || e === undefined) return null;
  return e < EDWIN.busy ? "Active" : e <= EDWIN.crowded ? "Busy" : "Crowded";
}

export function nedocsLabel(level: Num): string {
  return level ? NEDOCS_LABELS[level - 1] ?? "—" : "—";
}

export function isLowSample(n: Num): boolean {
  return n === null || n === undefined || n < 5;
}

export function fmt(v: Num, digits = 1, suffix = ""): string {
  return v === null || v === undefined || Number.isNaN(v) ? "—" : `${v.toFixed(digits)}${suffix}`;
}

export function pct(v: Num): string {
  return v === null || v === undefined ? "—" : `${Math.round(v * 100)}%`;
}

export function trendArrow(delta: Num): string {
  if (delta === null || delta === undefined) return "";
  return delta > 0.05 ? "▲" : delta < -0.05 ? "▼" : "▶";
}

export function localHour(iso: string, tz: string): string {
  return new Intl.DateTimeFormat("en-GB", { timeZone: tz, hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).format(new Date(iso));
}
