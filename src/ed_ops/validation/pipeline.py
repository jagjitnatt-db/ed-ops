"""End-to-end pipeline assertions for ed_validate (spec 16.3)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ed_ops.config import Config

MIN_NOTES_OK_SHARE = 0.9


def pipeline_metrics(spark: SparkSession, cfg: Config) -> dict:
    def t(logical: str, name: str):
        return spark.table(cfg.table(logical, name))

    snaps = t("gold", "facility_snapshot_5min")
    board_in_ed = t("gold", "ed_board_current").groupBy("facility_id").agg(F.count_if("in_ed").alias("n"))
    mismatches = t("gold", "facility_current").join(board_in_ed, "facility_id", "left").filter(
        F.coalesce(F.col("n"), F.lit(0)) != F.col("census")
    ).count()
    notes = t("silver", "triage_notes").agg(
        F.count_if("ai_enriched").alias("enriched"), F.count_if(F.col("ai_status") == "ok").alias("ok")
    ).collect()[0]
    legacy = snaps.select(F.explode_outer("top_drivers").alias("d")).filter(F.col("d.driver") == "arrivals_1h").count()
    return {
        "bronze_events": t("bronze", "ed_events").count(),
        "silver_visits": t("silver", "ed_visits").count(),
        "quarantine": t("silver", "ed_events_quarantine").count(),
        "facility_current": t("gold", "facility_current").count(),
        "snapshot_facilities": snaps.select("facility_id").distinct().count(),
        "snapshot_rows": snaps.count(),
        "board_census_mismatches": mismatches,
        "notes_enriched": notes.enriched,
        "notes_ok_share": (notes.ok / notes.enriched) if notes.enriched else 0.0,
        "facility_narratives": t("gold", "facility_narrative").count(),
        "network_narratives": t("gold", "network_narrative").count(),
        "legacy_driver_entries": legacy,
    }


def evaluate_pipeline(metrics: dict, cfg: Config) -> list[str]:
    n = cfg.facility_count
    min_snapshots = int(n * 288 * cfg.backfill_days * 0.95)
    rules = [
        (metrics["bronze_events"] > 0, "bronze has no events"),
        (metrics["silver_visits"] > 0, "silver has no visits"),
        (metrics["quarantine"] > 0 or cfg.malformed_rate == 0, "quarantine is empty although malformed_rate > 0"),
        (metrics["facility_current"] == n, f"facility_current has {metrics['facility_current']} rows, expected {n}"),
        (metrics["snapshot_facilities"] == n, f"snapshots cover {metrics['snapshot_facilities']} facilities, expected {n}"),
        (metrics["snapshot_rows"] >= min_snapshots, f"{metrics['snapshot_rows']} snapshot rows < {min_snapshots}"),
        (metrics["board_census_mismatches"] == 0,
         f"{metrics['board_census_mismatches']} facilities where board in_ed != census"),
        (metrics["notes_enriched"] > 0, "no AI-enriched triage notes"),
        (metrics["notes_ok_share"] >= MIN_NOTES_OK_SHARE,
         f"AI ok share {metrics['notes_ok_share']:.0%} < {MIN_NOTES_OK_SHARE:.0%}"),
        (metrics["facility_narratives"] == n, f"{metrics['facility_narratives']} facility narratives, expected {n}"),
        (metrics["network_narratives"] == 1, f"{metrics['network_narratives']} network narratives, expected 1"),
        (metrics["legacy_driver_entries"] == 0,
         f"{metrics['legacy_driver_entries']} top_drivers entries still named arrivals_1h"),
    ]
    return [message for ok, message in rules if not ok]
