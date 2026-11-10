import { useState } from "react";
import { api, usePolling, type Facility, type Summary } from "../api";
import { fmt, pct } from "../lib/bands";
import { ACCENT_PURPLE, ACCENT_TEAL, RANGES, type Range, crowdedCount, rangeLabel } from "../lib/cockpit";
import BreakdownPanel from "./BreakdownPanel";
import Heatmap from "./Heatmap";
import KpiCard from "./KpiCard";
import SummaryPanel from "./SummaryPanel";

const POLL_MS = 30_000;

export default function Cockpit({ summary, facilities, tz, onOpen }: {
  summary: Summary | null; facilities: Facility[]; tz: string; onOpen: (id: string) => void;
}) {
  const [range, setRange] = useState<Range>(24);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const network = usePolling(() => api.networkHistory(range), POLL_MS * 2, [range]);
  const facilityHistory = usePolling(() => (selectedId ? api.history(selectedId, 24) : Promise.resolve({ points: [] })), POLL_MS * 2, [selectedId]);
  const heatmap = usePolling(api.heatmap, POLL_MS);
  const buckets = network.data?.buckets ?? [];
  const at = (b: { bucket: string }) => new Date(b.bucket).getTime();
  const timeFormat = range === 168 ? "dd MMM" : "HH:mm";
  const selected = facilities.find((f) => f.facility_id === selectedId) ?? null;
  const asOf = summary?.as_of
    ? new Intl.DateTimeFormat("en-US", { timeZone: tz, dateStyle: "medium", timeStyle: "short" }).format(new Date(summary.as_of))
    : "—";
  const lwbs = facilities.length ? facilities.reduce((a, f) => a + (f.lwbs_rate_24h ?? 0), 0) / facilities.length : null;
  const waiting = facilities.reduce((a, f) => a + f.waiting_now_n, 0);

  return (
    <main className="cockpit">
      <header className="cockpit-head">
        <div>
          <div className="eyebrow">Emergency Departments - Crowding &amp; Wait Time Dashboard</div>
          <h1>NETWORK COCKPIT</h1>
          <div className="as-of">data as of: <i>{asOf}</i> · trends: <i>{rangeLabel(range)}</i></div>
        </div>
        <div className="range">
          {RANGES.map((r) => (
            <button key={r} className={r === range ? "active" : undefined} onClick={() => setRange(r)}>
              {r === 168 ? "7D" : `${r}H`}
            </button>
          ))}
        </div>
      </header>

      {network.error && <p className="error">Trends unavailable: {network.error}</p>}
      <div className="kpi-row">
        <KpiCard accent={ACCENT_TEAL} barName="Patients in EDs" format={(v) => fmt(v, 0)} timeFormat={timeFormat}
          bars={buckets.map((b) => [at(b), b.census])}>
          <div className="kpi-label">Patients in EDs</div>
          <div className="kpi-big">{summary ? summary.census.toLocaleString() : "—"}</div>
          <div className="kpi-sub">across {summary?.facilities ?? "—"} facilities</div>
          <div className="kpi-label spaced">Facilities crowded</div>
          <div className="kpi-big">{crowdedCount(facilities)}<span className="unit"> / {facilities.length}</span></div>
          <div className="kpi-sub">{summary ? `${summary.counts.Busy} busy · ${summary.counts.Active} active` : ""}</div>
        </KpiCard>
        <KpiCard accent={ACCENT_PURPLE} barName="Door-to-provider median" format={(v) => fmt(v, 0)} timeFormat={timeFormat}
          bars={buckets.map((b) => [at(b), b.d2p_median_min === null ? null : Math.round(b.d2p_median_min * 100) / 100])}>
          <div className="kpi-label">Median door-to-provider</div>
          <div className="kpi-big">{fmt(summary?.network_d2p_median_min, 0)}<span className="unit"> min</span></div>
          <div className="kpi-sub">median of facility medians, last hour</div>
          <div className="kpi-trio">
            <div><b>{waiting}</b><span>Waiting now</span></div>
            <div><b>{summary?.boarders ?? "—"}</b><span>Boarders</span></div>
            <div><b>{pct(lwbs)}</b><span>LWBS (24 h)</span></div>
          </div>
        </KpiCard>
      </div>

      <div className="panel-row">
        <SummaryPanel facilities={facilities} selected={selectedId} onSelect={setSelectedId} onOpen={onOpen} />
        <BreakdownPanel facilities={facilities} selected={selected} network={buckets} points={facilityHistory.data?.points ?? []}
          onClear={() => setSelectedId(null)} />
      </div>

      <Heatmap cells={heatmap.data?.cells ?? []} tz={tz} />
    </main>
  );
}
