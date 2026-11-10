import { useState } from "react";
import { api, usePolling } from "./api";
import Banner from "./components/Banner";
import BubbleChart from "./components/BubbleChart";
import Definitions from "./components/Definitions";
import FacilityDetail from "./components/FacilityDetail";
import FacilityTable from "./components/FacilityTable";
import GeniePanel from "./components/GeniePanel";
import Heatmap from "./components/Heatmap";
import KpiTiles from "./components/KpiTiles";
import RankedBar from "./components/RankedBar";

export const TZ = "America/Chicago";
const POLL_MS = 30_000;
type View = { page: "overview" } | { page: "facility"; id: string } | { page: "definitions" };

export default function App() {
  const [view, setView] = useState<View>({ page: "overview" });
  const [genieOpen, setGenieOpen] = useState(false);
  const health = usePolling(api.health, POLL_MS);
  const summary = usePolling(api.summary, POLL_MS);
  const facilities = usePolling(api.facilities, POLL_MS);
  const heatmap = usePolling(api.heatmap, POLL_MS);
  const open = (id: string) => setView({ page: "facility", id });
  const rows = facilities.data?.facilities ?? [];

  return (
    <div className="app">
      <header className="topbar">
        <h1 onClick={() => setView({ page: "overview" })}>ED Crowding Command Center</h1>
        <nav>
          <button onClick={() => setView({ page: "overview" })}>Network</button>
          <button onClick={() => setView({ page: "definitions" })}>Definitions</button>
          <button className="primary" onClick={() => setGenieOpen(true)}>Ask Genie</button>
        </nav>
      </header>
      <Banner health={health.data} healthError={health.error} summary={summary.data} />
      {view.page === "overview" && (
        <main>
          <KpiTiles summary={summary.data} />
          {facilities.error && <p className="error">Facilities unavailable: {facilities.error}</p>}
          <section className="grid2">
            <RankedBar facilities={rows} onSelect={open} />
            <BubbleChart facilities={rows} onSelect={open} />
          </section>
          <Heatmap cells={heatmap.data?.cells ?? []} tz={TZ} />
          <FacilityTable facilities={rows} onSelect={open} />
        </main>
      )}
      {view.page === "facility" && <FacilityDetail id={view.id} tz={TZ} onBack={() => setView({ page: "overview" })} />}
      {view.page === "definitions" && <Definitions />}
      <GeniePanel open={genieOpen} onClose={() => setGenieOpen(false)} />
    </div>
  );
}
