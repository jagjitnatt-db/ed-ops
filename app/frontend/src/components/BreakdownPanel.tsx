import type { ApexOptions } from "apexcharts";
import Chart from "react-apexcharts";
import type { Facility, NetworkBucket, Point } from "../api";
import { fmt, pct } from "../lib/bands";
import { ACCENT_PURPLE, BAR_GREY, barPct, topN } from "../lib/cockpit";

type MetricKey = "d2p_median_min" | "longest_current_wait_min" | "boarders_n" | "lwbs_rate_24h";
type Metric = { key: MetricKey; label: string; unit: string; format: (v: number | null | undefined) => string;
  network: (rows: Facility[]) => number | null };

const median = (xs: number[]) => {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b), m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const values = (rows: Facility[], k: MetricKey) => rows.map((r) => r[k]).filter((v): v is number => v !== null && v !== undefined);

const METRICS: Metric[] = [
  { key: "d2p_median_min", label: "Door-to-provider (median)", unit: "min", format: (v) => fmt(v, 0),
    network: (rows) => median(values(rows, "d2p_median_min")) },
  { key: "longest_current_wait_min", label: "Longest current wait", unit: "min", format: (v) => fmt(v, 0),
    network: (rows) => (values(rows, "longest_current_wait_min").length ? Math.max(...values(rows, "longest_current_wait_min")) : null) },
  { key: "boarders_n", label: "Boarding patients", unit: "", format: (v) => fmt(v, 0),
    network: (rows) => values(rows, "boarders_n").reduce((a, b) => a + b, 0) },
  { key: "lwbs_rate_24h", label: "Left without being seen (24 h)", unit: "", format: (v) => pct(v),
    network: (rows) => { const v = values(rows, "lwbs_rate_24h"); return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; } },
];

const spark: ApexOptions = {
  chart: { type: "line", sparkline: { enabled: true }, animations: { enabled: false } },
  stroke: { width: 1.5, curve: "straight" }, colors: ["#374151"], tooltip: { enabled: false },
};

/** "Wait & Flow Breakdown": network (or selected facility) figure, trend sparkline and the network's top 5. */
export default function BreakdownPanel({ facilities, selected, network, points, onClear }: {
  facilities: Facility[]; selected: Facility | null; network: NetworkBucket[]; points: Point[]; onClear: () => void;
}) {
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2><b>Wait &amp; Flow Breakdown</b> by Facility</h2>
          <p>{selected ? <>{selected.facility_name} <button className="link" onClick={onClear}>show all</button></> : "All facilities"}</p>
        </div>
      </header>
      {METRICS.map((m) => {
        const value = selected ? selected[m.key] : m.network(facilities);
        const trend = (selected ? points.map((p) => p[m.key]) : network.map((b) => b[m.key])).map((v) => v ?? 0);
        const top = topN(facilities, m.key, 5);
        const hi = Math.max(...top.map((t) => t.value), 0.0001);
        return (
          <div className="breakdown-row" key={m.key}>
            <div className="breakdown-figure">
              <div className="big">{m.format(value)}{m.unit && <span className="unit"> {m.unit}</span>}</div>
              <div className="label">{m.label}</div>
              <div className="accent-rule" style={{ background: ACCENT_PURPLE }} />
            </div>
            <div className="breakdown-spark">
              {trend.length > 1 ? <Chart type="line" height={70} options={spark} series={[{ name: m.label, data: trend }]} /> : <span className="muted">—</span>}
            </div>
            <ol className="top5">
              {top.map((t, i) => (
                <li key={t.facility_id} className={selected?.facility_id === t.facility_id ? "selected" : undefined}>
                  <span className="rank">{i + 1}</span>
                  <span className="name" title={t.facility_name}>{t.facility_name}</span>
                  <span className="bar-cell">
                    <span className="bar" style={{ width: `${barPct(t.value, 0, hi)}%`, background: BAR_GREY }} />
                    <span className="bar-val">{m.format(t.value)}</span>
                  </span>
                </li>
              ))}
            </ol>
          </div>
        );
      })}
    </section>
  );
}
