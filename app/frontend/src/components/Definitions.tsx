import { api, usePolling } from "../api";

export default function Definitions() {
  const { data, error } = usePolling(api.definitions, 3_600_000);
  if (error) return <main><p className="error">{error}</p></main>;
  if (!data) return <main><p>Loading…</p></main>;
  return (
    <main className="definitions">
      <div className="card">
        <h2>Formulas</h2>
        <dl>{Object.entries(data.formulas).map(([k, v]) => <div key={k}><dt>{k.replace(/_/g, " ")}</dt><dd>{v}</dd></div>)}</dl>
        <h2>Bands</h2>
        <p>gEDWIN (headline): Active &lt; {data.bands.gedwin.busy} ≤ Busy ≤ {data.bands.gedwin.crowded} &lt; Crowded.
          EDWIN: Active &lt; {data.bands.edwin.busy} ≤ Busy ≤ {data.bands.edwin.crowded} &lt; Crowded.
          NEDOCS: levels 1-6 at 20 / 60 / 100 / 140 / 180.</p>
        <h2>Assumptions</h2>
        <ul>{data.assumptions.map((a) => <li key={a}>{a}</li>)}</ul>
        <h2>Sources</h2>
        <ul>{data.citations.map((c) => <li key={c}>{c}</li>)}</ul>
      </div>
    </main>
  );
}
