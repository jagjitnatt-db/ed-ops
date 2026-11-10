import type { Health, Summary } from "../api";

export default function Banner({ health, healthError, summary }: { health: Health | null; healthError: string | null; summary: Summary | null }) {
  const delayed = healthError !== null || (health !== null && health.status !== "ok");
  const n = summary?.narrative;
  return (
    <>
      {delayed && (
        <div className="banner warn" role="alert">
          Data delayed{health?.data_age_seconds ? `: last update ${Math.round(health.data_age_seconds / 60)} min ago` : " - Lakebase is unavailable"}.
          Figures may be stale.
        </div>
      )}
      {n && (
        <div className="banner narrative">
          <strong>{n.headline}</strong>
          <span>{n.explanation}</span>
        </div>
      )}
    </>
  );
}
