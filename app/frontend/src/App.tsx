import { useState } from "react";
import { api, usePolling } from "./api";
import Banner from "./components/Banner";
import Cockpit from "./components/Cockpit";
import Definitions from "./components/Definitions";
import FacilityDetail from "./components/FacilityDetail";
import GeniePanel from "./components/GeniePanel";

export const TZ = "America/Chicago";
const POLL_MS = 30_000;
type View = { page: "overview" } | { page: "facility"; id: string } | { page: "definitions" };

export default function App() {
  const [view, setView] = useState<View>({ page: "overview" });
  const [genieOpen, setGenieOpen] = useState(false);
  const health = usePolling(api.health, POLL_MS);
  const summary = usePolling(api.summary, POLL_MS);
  const facilities = usePolling(api.facilities, POLL_MS);
  const open = (id: string) => setView({ page: "facility", id });

  return (
    <div className="shell">
      <nav className="rail" aria-label="Main">
        <div className="rail-logo" title="ED Ops">ED</div>
        <button className={view.page !== "definitions" ? "active" : undefined} onClick={() => setView({ page: "overview" })} title="Network">
          <span className="rail-icon">▦</span><span>Network</span>
        </button>
        <button className={view.page === "definitions" ? "active" : undefined} onClick={() => setView({ page: "definitions" })} title="Definitions">
          <span className="rail-icon">?</span><span>Definitions</span>
        </button>
        <button onClick={() => setGenieOpen(true)} title="Ask Genie">
          <span className="rail-icon">✦</span><span>Ask Genie</span>
        </button>
      </nav>
      <div className="content">
        <Banner health={health.data} healthError={health.error} summary={summary.data} />
        {facilities.error && view.page === "overview" && <p className="error">Facilities unavailable: {facilities.error}</p>}
        {view.page === "overview" && (
          <Cockpit summary={summary.data} facilities={facilities.data?.facilities ?? []} tz={TZ} onOpen={open} />
        )}
        {view.page === "facility" && <FacilityDetail id={view.id} tz={TZ} onBack={() => setView({ page: "overview" })} />}
        {view.page === "definitions" && <Definitions />}
      </div>
      <GeniePanel open={genieOpen} onClose={() => setGenieOpen(false)} />
    </div>
  );
}
