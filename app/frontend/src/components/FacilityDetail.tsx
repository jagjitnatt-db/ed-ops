import type { ApexOptions } from "apexcharts";
import { useState } from "react";
import Chart from "react-apexcharts";
import { api, usePolling } from "../api";
import { BAND_COLORS, LOW_SAMPLE, edwinBand, fmt, gedwinBand, isLowSample, nedocsLabel, pct } from "../lib/bands";
import { TREND_SERIES, trendLineOptions } from "../lib/trendChart";

function Gauge({ label, value, max, color, text }: { label: string; value: number | null; max: number; color: string; text: string }) {
  const options: ApexOptions = {
    chart: { type: "radialBar", sparkline: { enabled: true } },
    colors: [color],
    // Only the value sits inside the arc; the score name is a caption below the gauge so it never overlaps the arc.
    plotOptions: { radialBar: { startAngle: -110, endAngle: 110, dataLabels: { name: { show: false }, value: { offsetY: 0, fontSize: "22px", formatter: () => text } } } },
    labels: [label],
  };
  const percent = value === null ? 0 : Math.max(0, Math.min(100, (value / max) * 100));
  return (
    <div className="gauge">
      <Chart type="radialBar" height={200} options={options} series={[percent]} />
      <div className="gauge-label" style={{ color }}>{label}</div>
    </div>
  );
}

export default function FacilityDetail({ id, tz, onBack }: { id: string; tz: string; onBack: () => void }) {
  const [hours, setHours] = useState(24);
  const detail = usePolling(() => api.facility(id), 30_000, [id]);
  const history = usePolling(() => api.history(id, hours), 60_000, [id, hours]);
  const board = usePolling(() => api.board(id), 30_000, [id]);
  const f = detail.data?.facility;
  if (detail.error) return <main><button onClick={onBack}>← Network</button><p className="error">{detail.error}</p></main>;
  if (!f) return <main><p>Loading…</p></main>;
  const points = history.data?.points ?? [];
  const band = gedwinBand(f.gedwin);
  const low = isLowSample(f.d2p_sample_n);
  const time = { type: "datetime" as const, labels: { datetimeUTC: false, format: hours > 24 ? "dd MMM" : "HH:mm" } };
  const lineOptions = trendLineOptions(hours);
  const ts = (p: { snapshot_ts: string }) => new Date(p.snapshot_ts).getTime();
  const pt = (p: { snapshot_ts: string }, y: number | null): [number, number | null] => [ts(p), y];
  const areaOptions: ApexOptions = {
    chart: { type: "area", stacked: true, toolbar: { show: false } }, xaxis: time, dataLabels: { enabled: false },
    colors: ["#B71C1C", "#E65100", "#F9A825", "#7CB342", "#2E7D32", LOW_SAMPLE], stroke: { width: 1 },
  };
  const esiKeys = ["n_esi1", "n_esi2", "n_esi3", "n_esi4", "n_esi5", "n_untriaged"] as const;
  const esiNames = ["ESI 1", "ESI 2", "ESI 3", "ESI 4", "ESI 5", "Untriaged"];
  const n = detail.data?.narrative;
  const eb = edwinBand(f.edwin);
  return (
    <main>
      <button onClick={onBack}>← Network</button>
      <h2 className="title">{f.facility_name} <span className="badge" style={{ background: BAND_COLORS[band] }}>{band}</span></h2>
      <p className="muted">{f.facility_type.replace(/_/g, " ")} · {f.region} · {f.ed_beds ?? "?"} ED beds · {f.attendings_on_duty ?? 0} attendings on duty</p>
      <section className="gauges">
        <Gauge label="gEDWIN" value={f.gedwin} max={4} color={BAND_COLORS[band]} text={fmt(f.gedwin, 2)} />
        <Gauge label="EDWIN" value={f.edwin} max={4} color={eb ? BAND_COLORS[eb] : LOW_SAMPLE} text={fmt(f.edwin, 2)} />
        <Gauge label={`NEDOCS · ${nedocsLabel(f.nedocs_level)}`} value={f.nedocs} max={200}
          color={f.nedocs_level >= 4 ? BAND_COLORS.Crowded : f.nedocs_level === 3 ? BAND_COLORS.Busy : BAND_COLORS.Active} text={fmt(f.nedocs, 0)} />
      </section>
      <section className="tiles">
        {[["Door-to-provider mean", fmt(f.d2p_mean_min, 0, " min")], ["Median", fmt(f.d2p_median_min, 0, " min")],
          ["90th percentile", fmt(f.d2p_p90_min, 0, " min")], ["Greeted (last hour)", String(f.d2p_sample_n)],
          ["Waiting now", String(f.waiting_now_n)], ["Longest wait", fmt(f.longest_current_wait_min, 0, " min")],
          ["Boarders", `${f.boarders_n} (longest ${fmt(f.longest_boarder_h, 1, " h")})`], ["LWBS (24 h)", pct(f.lwbs_rate_24h)]].map(([label, value]) => (
          <div className="tile" key={label} style={low && label !== "Waiting now" && label.startsWith("D") ? { color: LOW_SAMPLE } : undefined}>
            <div className="tile-value">{value}</div><div className="tile-label">{label}</div>
          </div>
        ))}
      </section>
      {low && <p className="low">Door-to-provider figures are low-sample: fewer than 5 patients were greeted in the last hour.</p>}
      {n && (
        <div className="card narrative-card">
          <h3>{n.headline}</h3><p>{n.explanation}</p>
          {n.recommended_actions?.length ? <ul>{n.recommended_actions.map((a) => <li key={a}>{a}</li>)}</ul> : null}
          {f.top_drivers?.length ? <p className="muted">Drivers: {f.top_drivers.map((d) => `${d.driver.replace(/_/g, " ")} ${fmt(d.value, 2)} vs ${fmt(d.baseline, 2)} (z ${fmt(d.z, 1)})`).join(" · ")}</p> : null}
        </div>
      )}
      <div className="card">
        <h2>Trends <button onClick={() => setHours(hours === 24 ? 168 : 24)}>{hours === 24 ? "Show 7 days" : "Show 24 hours"}</button></h2>
        {points.length === 0 ? <p className="muted">No history yet.</p> : (
          <Chart type="line" height={300} options={lineOptions} series={[
            { name: TREND_SERIES.median, data: points.map((p) => pt(p, p.d2p_median_min)) },
            { name: TREND_SERIES.p90, data: points.map((p) => pt(p, p.d2p_p90_min)) },
            { name: TREND_SERIES.gedwin, data: points.map((p) => pt(p, p.gedwin)) },
          ]} />
        )}
        {points.length > 0 && (
          <Chart type="area" height={240} options={areaOptions}
            series={esiKeys.map((k, i) => ({ name: esiNames[i], data: points.map((p) => pt(p, p[k])) }))} />
        )}
      </div>
      <div className="card">
        <h2>ED board (de-identified)</h2>
        <table className="table">
          <thead><tr><th>Visit</th><th>Status</th><th>ESI</th><th>AI ESI</th><th>Complaint</th><th>In ED</th><th>Waiting</th><th>Arrival</th></tr></thead>
          <tbody>
            {(board.data?.patients ?? []).map((p) => (
              <tr key={p.visit_id} className={p.undertriage_flag ? "undertriage" : undefined}>
                <td>{p.visit_id}</td><td>{p.status}{p.on_vent ? " · ventilated" : ""}</td><td>{p.esi_level ?? "—"}</td>
                <td>{p.suggested_esi ?? "—"}{p.undertriage_flag ? " ⚠" : ""}</td><td>{p.complaint_category ?? "—"}</td>
                <td>{p.minutes_in_ed} min</td><td>{p.minutes_waiting} min</td><td>{p.arrival_mode.replace("_", " ")}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="muted">⚠ AI-suggested ESI is at least 2 levels more acute than the nurse ESI (decision support only). Times in {tz}.</p>
      </div>
    </main>
  );
}
