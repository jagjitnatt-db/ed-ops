import { describe, expect, it } from "vitest";
import { BAR_GREY, barColor, barPct, crowdedCount, rangeLabel, sortFacilities, topN } from "./cockpit";
import { BAND_COLORS } from "./bands";

type Row = { facility_id: string; facility_name: string; gedwin: number | null; gedwin_band: "Active" | "Busy" | "Crowded";
  census: number; d2p_median_min: number | null; boarders_n: number };
const row = (id: string, gedwin: number | null, band: Row["gedwin_band"], census: number, d2p: number | null, boarders: number): Row =>
  ({ facility_id: id, facility_name: `${id} Hospital`, gedwin, gedwin_band: band, census, d2p_median_min: d2p, boarders_n: boarders });
const ROWS = [row("A", 1.2, "Active", 10, 20, 0), row("B", 2.9, "Crowded", 30, null, 5), row("C", null, "Active", 0, 35, 1),
  row("D", 2.2, "Busy", 20, 50, 2)];

describe("cockpit helpers", () => {
  it("sorts facilities descending with nulls last", () => {
    expect(sortFacilities(ROWS, "gedwin").map((r) => r.facility_id)).toEqual(["B", "D", "A", "C"]);
    expect(sortFacilities(ROWS, "d2p_median_min").map((r) => r.facility_id)).toEqual(["D", "C", "A", "B"]);
    expect(ROWS.map((r) => r.facility_id)).toEqual(["A", "B", "C", "D"]); // input untouched
  });

  it("takes the top N non-null values", () => {
    expect(topN(ROWS, "d2p_median_min", 2)).toEqual([
      { facility_id: "D", facility_name: "D Hospital", value: 50 },
      { facility_id: "C", facility_name: "C Hospital", value: 35 },
    ]);
  });

  it("colours bars grey unless Busy or Crowded", () => {
    expect([barColor("Active"), barColor("Busy"), barColor("Crowded")]).toEqual([BAR_GREY, BAND_COLORS.Busy, BAND_COLORS.Crowded]);
  });

  it("scales bars between the range minimum and maximum with a visible floor", () => {
    expect(barPct(2.9, -1, 2.9)).toBe(100);
    expect(barPct(-1, -1, 2.9)).toBe(3);
    expect(barPct(null, 0, 10)).toBe(0);
    expect(barPct(5, 5, 5)).toBe(100); // flat range
  });

  it("counts crowded facilities and labels ranges", () => {
    expect(crowdedCount(ROWS)).toBe(1);
    expect([rangeLabel(6), rangeLabel(24), rangeLabel(168)]).toEqual(["last 6 hours", "last 24 hours", "last 7 days"]);
  });
});
