import type { Summary } from "../api";
import { BAND_COLORS, fmt } from "../lib/bands";

export default function KpiTiles({ summary }: { summary: Summary | null }) {
  const c = summary?.counts ?? { Crowded: 0, Busy: 0, Active: 0 };
  const tiles = [
    { label: "Crowded", value: String(c.Crowded), color: BAND_COLORS.Crowded },
    { label: "Busy", value: String(c.Busy), color: BAND_COLORS.Busy },
    { label: "Active", value: String(c.Active), color: BAND_COLORS.Active },
    { label: "Network median door-to-provider", value: fmt(summary?.network_d2p_median_min, 0, " min") },
    { label: "Patients in EDs", value: summary ? String(summary.census) : "—" },
    { label: "Boarding patients", value: summary ? String(summary.boarders) : "—" },
  ];
  return (
    <section className="tiles">
      {tiles.map((t) => (
        <div className="tile" key={t.label} style={t.color ? { borderTopColor: t.color } : undefined}>
          <div className="tile-value">{t.value}</div>
          <div className="tile-label">{t.label}</div>
        </div>
      ))}
    </section>
  );
}
