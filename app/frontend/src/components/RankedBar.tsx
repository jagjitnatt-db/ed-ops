import type { ApexOptions } from "apexcharts";
import Chart from "react-apexcharts";
import type { Facility } from "../api";
import { BAND_COLORS, GEDWIN, gedwinBand } from "../lib/bands";

export default function RankedBar({ facilities, onSelect }: { facilities: Facility[]; onSelect: (id: string) => void }) {
  const rows = [...facilities].sort((a, b) => (b.gedwin ?? -99) - (a.gedwin ?? -99));
  const options: ApexOptions = {
    chart: { type: "bar", toolbar: { show: false }, events: { dataPointSelection: (_e, _c, cfg) => onSelect(rows[cfg.dataPointIndex].facility_id) } },
    plotOptions: { bar: { horizontal: true, distributed: true, barHeight: "80%" } },
    colors: rows.map((r) => BAND_COLORS[gedwinBand(r.gedwin)]),
    legend: { show: false },
    dataLabels: { enabled: false },
    xaxis: { categories: rows.map((r) => r.facility_name), title: { text: "gEDWIN" } },
    annotations: {
      xaxis: [
        { x: GEDWIN.busy, borderColor: BAND_COLORS.Busy, label: { text: "Busy 2.06" } },
        { x: GEDWIN.crowded, borderColor: BAND_COLORS.Crowded, label: { text: "Crowded 2.34" } },
      ],
    },
    tooltip: { y: { formatter: (v) => (v === null ? "empty ED" : v.toFixed(2)) } },
  };
  return (
    <div className="card">
      <h2>Crowding ranking (gEDWIN)</h2>
      <Chart type="bar" height={Math.max(320, rows.length * 18)} options={options} series={[{ name: "gEDWIN", data: rows.map((r) => r.gedwin ?? 0) }]} />
    </div>
  );
}
