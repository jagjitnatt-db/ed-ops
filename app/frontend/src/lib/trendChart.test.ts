import { describe, expect, it } from "vitest";
import { TREND_SERIES, fmt2, trendLineOptions } from "./trendChart";

describe("trend chart", () => {
  it("formats minutes and gEDWIN with 2 decimals and nulls as a dash", () => {
    expect([fmt2(34.5678), fmt2(2), fmt2(null), fmt2(undefined)]).toEqual(["34.57", "2.00", "—", "—"]);
  });

  it("uses 2-decimal formatters on both y-axes and in the tooltip", () => {
    const o = trendLineOptions(24);
    const axes = o.yaxis as { labels: { formatter: (v: number) => string } }[];
    expect(axes.map((a) => a.labels.formatter(1.23456))).toEqual(["1.23", "1.23"]);
    const tip = o.tooltip as { y: { formatter: (v: number | null) => string } };
    expect([tip.y.formatter(75.016), tip.y.formatter(null)]).toEqual(["75.02", "—"]);
  });

  it("maps both door-to-provider series to the minutes axis and gEDWIN to its own axis", () => {
    const axes = trendLineOptions(24).yaxis as { seriesName: string | string[]; opposite?: boolean }[];
    expect(axes[0].seriesName).toEqual([TREND_SERIES.median, TREND_SERIES.p90]);
    expect(axes[1].seriesName).toEqual([TREND_SERIES.gedwin]);
    expect(axes[1].opposite).toBe(true);
  });
});
