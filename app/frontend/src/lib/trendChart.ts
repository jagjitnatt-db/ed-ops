import type { ApexOptions } from "apexcharts";
import { BAND_COLORS, GEDWIN, NEUTRAL } from "./bands";

export const TREND_SERIES = { median: "D2P median", p90: "D2P 90th pct", gedwin: "gEDWIN" } as const;

/** Two decimals for minutes and gEDWIN on the trends chart; null (no sample) shows a dash. */
export function fmt2(v: number | null | undefined): string {
  return v === null || v === undefined || Number.isNaN(v) ? "—" : v.toFixed(2);
}

export function trendLineOptions(hours: number): ApexOptions {
  return {
    chart: { type: "line", toolbar: { show: false } },
    stroke: { width: 2 },
    colors: [NEUTRAL, "#6A1B9A", BAND_COLORS.Crowded],
    xaxis: { type: "datetime", labels: { datetimeUTC: false, format: hours > 24 ? "dd MMM" : "HH:mm" } },
    // seriesName pins each series to its axis; by position alone the 90th percentile would land on the gEDWIN axis.
    yaxis: [
      { seriesName: [TREND_SERIES.median, TREND_SERIES.p90], title: { text: "minutes" }, labels: { formatter: fmt2 } },
      { seriesName: [TREND_SERIES.gedwin], opposite: true, title: { text: "gEDWIN" }, labels: { formatter: fmt2 } },
    ],
    annotations: {
      yaxis: [
        { y: GEDWIN.busy, yAxisIndex: 1, borderColor: BAND_COLORS.Busy, label: { text: "Busy" } },
        { y: GEDWIN.crowded, yAxisIndex: 1, borderColor: BAND_COLORS.Crowded, label: { text: "Crowded" } },
      ],
    },
    tooltip: { x: { format: "dd MMM HH:mm" }, y: { formatter: fmt2 } },
  };
}
