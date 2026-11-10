import { describe, expect, it } from "vitest";
import { edwinBand, fmt, gedwinBand, isLowSample, localHour, nedocsLabel, pct, trendArrow } from "./bands";

describe("bands", () => {
  it("classifies gEDWIN with the paper's cut-offs and treats null (empty ED) as Active", () => {
    expect([gedwinBand(2.05), gedwinBand(2.06), gedwinBand(2.34), gedwinBand(2.35), gedwinBand(null)]).toEqual([
      "Active", "Busy", "Busy", "Crowded", "Active",
    ]);
  });
  it("classifies EDWIN", () => {
    expect([edwinBand(1.49), edwinBand(1.5), edwinBand(2), edwinBand(2.01), edwinBand(null)]).toEqual([
      "Active", "Busy", "Busy", "Crowded", null,
    ]);
  });
  it("labels NEDOCS levels", () => {
    expect(nedocsLabel(1)).toBe("Not busy");
    expect(nedocsLabel(6)).toBe("Dangerously overcrowded");
    expect(nedocsLabel(null)).toBe("—");
  });
  it("flags low samples and formats values", () => {
    expect([isLowSample(4), isLowSample(5), isLowSample(null)]).toEqual([true, false, true]);
    expect([fmt(null), fmt(2.345, 2), fmt(45, 0, " min")]).toEqual(["—", "2.35", "45 min"]);
    expect(pct(0.034)).toBe("3%");
    expect([trendArrow(0.2), trendArrow(-0.2), trendArrow(0.01), trendArrow(null)]).toEqual(["▲", "▼", "▶", ""]);
  });
  it("formats local hours in the facility time zone", () => {
    expect(localHour("2026-11-10T18:00:00Z", "America/Chicago")).toBe("12:00");
  });
});
