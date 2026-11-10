import type { ApexOptions } from "apexcharts";
import { useState } from "react";
import Chart from "react-apexcharts";
import type { Facility } from "../api";
import { BAND_COLORS, LOW_SAMPLE, NEUTRAL, fmt, gedwinBand, isLowSample, nedocsLabel, trendArrow } from "../lib/bands";

type Key = "facility_name" | "gedwin" | "census" | "d2p_median_min" | "boarders_n" | "nedocs";
const COLUMNS: [Key, string][] = [["facility_name", "Facility"], ["gedwin", "gEDWIN"], ["census", "Census"],
  ["d2p_median_min", "Door-to-provider"], ["boarders_n", "Boarders"], ["nedocs", "NEDOCS"]];
const spark: ApexOptions = { chart: { type: "line", sparkline: { enabled: true } }, stroke: { width: 2 }, colors: [NEUTRAL], tooltip: { enabled: false } };

export default function FacilityTable({ facilities, onSelect }: { facilities: Facility[]; onSelect: (id: string) => void }) {
  const [sort, setSort] = useState<{ key: Key; desc: boolean }>({ key: "gedwin", desc: true });
  const rows = [...facilities].sort((a, b) => {
    const av = a[sort.key] ?? -1, bv = b[sort.key] ?? -1;
    const cmp = typeof av === "string" ? String(av).localeCompare(String(bv)) : Number(av) - Number(bv);
    return sort.desc ? -cmp : cmp;
  });
  return (
    <div className="card">
      <h2>All facilities</h2>
      <table className="table">
        <thead>
          <tr>
            {COLUMNS.map(([k, label]) => (
              <th key={k} onClick={() => setSort({ key: k, desc: sort.key === k ? !sort.desc : true })}>{label}</th>
            ))}
            <th>Last 6 h</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((f) => {
            const band = gedwinBand(f.gedwin);
            const low = isLowSample(f.d2p_sample_n);
            return (
              <tr key={f.facility_id} onClick={() => onSelect(f.facility_id)}>
                <td>{f.facility_name}</td>
                <td><span className="badge" style={{ background: BAND_COLORS[band] }}>{band}</span> {fmt(f.gedwin, 2)} {trendArrow(f.gedwin_delta_1h)}</td>
                <td>{f.census}</td>
                <td style={low ? { color: LOW_SAMPLE } : undefined}>{fmt(f.d2p_median_min, 0, " min")}{low && <span className="low"> low sample</span>}</td>
                <td>{f.boarders_n}</td>
                <td>{nedocsLabel(f.nedocs_level)}</td>
                <td className="spark">
                  <Chart type="line" height={32} width={120} options={spark}
                    series={[{ name: "gEDWIN", data: (f.sparkline ?? []).map(([, g]) => g ?? 0) }]} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
