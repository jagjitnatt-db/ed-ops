import type { ApexOptions } from "apexcharts";
import Chart from "react-apexcharts";
import type { Facility } from "../api";
import { BAND_COLORS, type Band, gedwinBand } from "../lib/bands";

const BANDS: Band[] = ["Crowded", "Busy", "Active"];

export default function BubbleChart({ facilities, onSelect }: { facilities: Facility[]; onSelect: (id: string) => void }) {
  const groups = BANDS.map((b) => facilities.filter((f) => gedwinBand(f.gedwin) === b && f.d2p_median_min !== null));
  const options: ApexOptions = {
    chart: { type: "bubble", toolbar: { show: false },
      events: { dataPointSelection: (_e, _c, cfg) => onSelect(groups[cfg.seriesIndex][cfg.dataPointIndex].facility_id) } },
    colors: BANDS.map((b) => BAND_COLORS[b]),
    dataLabels: { enabled: false },
    xaxis: { type: "numeric", title: { text: "Occupancy (%)" }, min: 0 },
    yaxis: { title: { text: "Door-to-provider median (min)" }, min: 0 },
    tooltip: { custom: ({ seriesIndex, dataPointIndex }) => {
      const f = groups[seriesIndex][dataPointIndex];
      return `<div class="tip"><b>${f.facility_name}</b><br/>Occupancy ${Math.round((f.occupancy_ratio ?? 0) * 100)}%<br/>` +
        `D2P median ${f.d2p_median_min?.toFixed(0)} min<br/>Census ${f.census}</div>`;
    } },
  };
  const series = BANDS.map((b, i) => ({
    name: b,
    data: groups[i].map((f) => ({ x: Math.round((f.occupancy_ratio ?? 0) * 100), y: Math.round(f.d2p_median_min ?? 0), z: Math.max(1, f.census) })),
  }));
  return (
    <div className="card">
      <h2>Where are we doing well? Occupancy vs. door-to-provider</h2>
      <Chart type="bubble" height={420} options={options} series={series} />
    </div>
  );
}
