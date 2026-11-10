import { BAND_COLORS, type Band } from "./bands";

/** Cockpit look: everything grey by default; colour only marks Busy (amber) and Crowded (red). */
export const BAR_GREY = "#C8CCD0";
export const ACCENT_TEAL = "#5E9C91";
export const ACCENT_PURPLE = "#8A8CC4";

export type SortKey = "gedwin" | "census" | "d2p_median_min" | "boarders_n";
export const SORT_OPTIONS: [SortKey, string][] = [
  ["gedwin", "gEDWIN"], ["census", "Census"], ["d2p_median_min", "Door-to-provider"], ["boarders_n", "Boarders"],
];
export const RANGES = [6, 24, 168] as const;
export type Range = (typeof RANGES)[number];

type Named = { facility_id: string; facility_name: string };

export function sortFacilities<T extends Record<K, number | null>, K extends string>(rows: T[], key: K): T[] {
  return [...rows].sort((a, b) => {
    const av = a[key], bv = b[key];
    if (av === null || av === undefined) return bv === null || bv === undefined ? 0 : 1;
    if (bv === null || bv === undefined) return -1;
    return bv - av;
  });
}

export function topN<T extends Named & Record<K, number | null>, K extends string>(rows: T[], key: K, n = 5) {
  return sortFacilities(rows.filter((r) => r[key] !== null && r[key] !== undefined), key)
    .slice(0, n)
    .map((r) => ({ facility_id: r.facility_id, facility_name: r.facility_name, value: r[key] as number }));
}

export function barColor(band: Band): string {
  return band === "Active" ? BAR_GREY : BAND_COLORS[band];
}

/** Bar width in percent of the [lo, hi] range; non-null values keep a 3% floor so they stay visible. */
export function barPct(v: number | null | undefined, lo: number, hi: number): number {
  if (v === null || v === undefined) return 0;
  if (hi <= lo) return 100;
  return Math.max(3, Math.min(100, ((v - lo) / (hi - lo)) * 100));
}

export function crowdedCount(rows: { gedwin_band: Band }[]): number {
  return rows.filter((r) => r.gedwin_band === "Crowded").length;
}

export function rangeLabel(hours: Range): string {
  return hours === 168 ? "last 7 days" : `last ${hours} hours`;
}
