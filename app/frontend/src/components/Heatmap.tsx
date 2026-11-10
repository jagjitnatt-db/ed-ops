import type { ApexOptions } from "apexcharts";
import Chart from "react-apexcharts";
import type { Cell } from "../api";
import { BAND_COLORS, GEDWIN, localHour } from "../lib/bands";
import { BAR_GREY } from "../lib/cockpit";

export default function Heatmap({ cells, tz }: { cells: Cell[]; tz: string }) {
  const byFacility = new Map<string, Cell[]>();
  cells.forEach((c) => byFacility.set(c.facility_name, [...(byFacility.get(c.facility_name) ?? []), c]));
  const series = [...byFacility.entries()].map(([name, rows]) => ({
    name,
    data: rows.map((c) => ({ x: localHour(c.hour, tz), y: c.gedwin ?? 0 })),
  }));
  const options: ApexOptions = {
    chart: { type: "heatmap", toolbar: { show: false } },
    dataLabels: { enabled: false },
    plotOptions: { heatmap: { colorScale: { ranges: [
      { from: -50, to: GEDWIN.busy - 0.0001, color: BAR_GREY, name: "Active" },
      { from: GEDWIN.busy, to: GEDWIN.crowded, color: BAND_COLORS.Busy, name: "Busy" },
      { from: GEDWIN.crowded + 0.0001, to: 50, color: BAND_COLORS.Crowded, name: "Crowded" },
    ] } } },
    tooltip: { y: { formatter: (v) => `gEDWIN ${v.toFixed(2)}` } },
  };
  return (
    <div className="panel heat">
      <h2>Last 24 hours by local hour (peak gEDWIN band)</h2>
      <Chart type="heatmap" height={Math.max(300, series.length * 16)} options={options} series={series} />
    </div>
  );
}
