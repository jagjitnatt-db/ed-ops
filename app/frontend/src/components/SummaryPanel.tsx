import { useState } from "react";
import type { Facility } from "../api";
import { fmt, gedwinBand, isLowSample } from "../lib/bands";
import { ACCENT_TEAL, BAR_GREY, SORT_OPTIONS, type SortKey, barColor, barPct, crowdedCount, sortFacilities } from "../lib/cockpit";

/** "Crowding Summary by Facility": sortable list with inline census and gEDWIN bars; click filters the breakdown. */
export default function SummaryPanel({ facilities, selected, onSelect, onOpen }: {
  facilities: Facility[]; selected: string | null; onSelect: (id: string | null) => void; onOpen: (id: string) => void;
}) {
  const [sort, setSort] = useState<SortKey>("gedwin");
  const rows = sortFacilities(facilities, sort);
  const maxCensus = Math.max(1, ...facilities.map((f) => f.census));
  const g = facilities.map((f) => f.gedwin).filter((v): v is number => v !== null);
  const gLo = Math.min(0, ...g), gHi = Math.max(0.01, ...g);
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2><b>Crowding Summary</b> by Facility</h2>
          <p>Click a <b>facility</b> to filter the breakdown</p>
        </div>
        <label className="sort">Sort by
          <select value={sort} onChange={(e) => setSort(e.target.value as SortKey)}>
            {SORT_OPTIONS.map(([k, label]) => <option key={k} value={k}>{label}</option>)}
          </select>
        </label>
      </header>
      <div className="strip">
        <span className="strip-big" style={{ color: ACCENT_TEAL }}>{crowdedCount(facilities)}</span> of{" "}
        <span className="strip-big">{facilities.length}</span> facilities are <b>Crowded</b>
      </div>
      <div className="summary-cols"><span /><span>Census</span><span>gEDWIN</span><span>Door-to-provider</span></div>
      <div className="summary-rows">
        {rows.map((f) => {
          const band = gedwinBand(f.gedwin);
          return (
            <div key={f.facility_id} className={`summary-row${selected === f.facility_id ? " selected" : ""}`}
              onClick={() => onSelect(selected === f.facility_id ? null : f.facility_id)}>
              <span className="name" title={f.facility_name}>{f.facility_name}</span>
              <span className="bar-cell">
                <span className="bar" style={{ width: `${barPct(f.census, 0, maxCensus)}%`, background: BAR_GREY }} />
                <span className="bar-val">{f.census}</span>
              </span>
              <span className="bar-cell">
                <span className="bar" style={{ width: `${barPct(f.gedwin, gLo, gHi)}%`, background: barColor(band) }} />
                <span className="bar-val">{fmt(f.gedwin, 2)}</span>
              </span>
              <span className={isLowSample(f.d2p_sample_n) ? "low" : undefined}>
                {fmt(f.d2p_median_min, 0, " min")}
                <button className="link" onClick={(e) => { e.stopPropagation(); onOpen(f.facility_id); }}>details →</button>
              </span>
            </div>
          );
        })}
      </div>
    </section>
  );
}
