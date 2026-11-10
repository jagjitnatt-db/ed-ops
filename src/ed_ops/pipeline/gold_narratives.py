"""AI narratives grounded only in computed gold metrics (spec 9.2). Rows whose reply fails to parse are dropped,
so the MERGE keeps the previous narrative for that facility."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.ai import AiFunctions, DatabricksAi
from ed_ops.config import Config
from ed_ops.delta import merge_into
from ed_ops.pipeline.silver_notes_ai import parse_json_reply

FACILITY_SCHEMA = "headline STRING, explanation STRING, recommended_actions ARRAY<STRING>"
NETWORK_SCHEMA = ("headline STRING, explanation STRING, crowded_facilities ARRAY<STRING>, "
                  "doing_well_facilities ARRAY<STRING>, recommended_actions ARRAY<STRING>")
FACILITY_PROMPT = (
    "You are an operations analyst for a single emergency department. Using ONLY the numbers in the JSON below, "
    "write a status update for the ED charge nurse. Return ONLY a JSON object, no prose, no code fences, matching "
    '{"headline": <string, max 12 words>, "explanation": <string, max 60 words>, '
    '"recommended_actions": [<up to 3 short strings>]}. Do not invent numbers or facts. '
    "gEDWIN bands: Active < 2.06 <= Busy <= 2.34 < Crowded. NEDOCS levels: 1 not busy, 2 busy, 3 extremely busy, "
    "4 overcrowded, 5 severely overcrowded, 6 dangerously overcrowded. top_drivers lists metrics furthest from this "
    "ED's normal for this hour (z = standard deviations). Data: "
)
NETWORK_PROMPT = (
    "You are an operations analyst for a health system with many emergency departments. Using ONLY the JSON below, "
    "summarise which facilities are crowded and why, and which are doing well. Return ONLY a JSON object, no prose, "
    'no code fences, matching {"headline": <string, max 15 words>, "explanation": <string, max 80 words>, '
    '"crowded_facilities": [<facility names>], "doing_well_facilities": [<facility names>], '
    '"recommended_actions": [<up to 3 short strings>]}. List a facility as crowded only if its gedwin_band is '
    "Crowded. Do not invent numbers. Data: "
)
_ROUND = {"gedwin": 2, "gedwin_delta_1h": 2, "edwin": 2, "nedocs": 1, "occupancy_ratio": 2, "d2p_median_min": 1,
          "d2p_p90_min": 1, "longest_current_wait_min": 1, "longest_boarder_h": 1, "lwbs_rate_24h": 3,
          "patients_per_attending": 1, "arrival_surge_ratio": 2}
_FIELDS = (
    "facility_id", "facility_name", "facility_type", "region", "gedwin", "gedwin_band", "gedwin_delta_1h", "edwin",
    "edwin_band", "nedocs", "nedocs_level", "census", "occupancy_ratio", "d2p_median_min", "d2p_p90_min",
    "d2p_sample_n", "waiting_now_n", "longest_current_wait_min", "boarders_n", "longest_boarder_h", "lwbs_rate_24h",
    "attendings_on_duty", "patients_per_attending", "arrival_surge_ratio", "top_drivers",
)


def _field(name: str) -> Column:
    c = F.col(name)
    return (F.round(c, _ROUND[name]) if name in _ROUND else c).alias(name)


def _ask(df: DataFrame, prompt: str, ai: AiFunctions, schema: str) -> DataFrame:
    df = df.withColumn("_prompt", F.concat(F.lit(prompt), F.col("input_json"))).withColumn("_reply", ai.query("_prompt"))
    return df.withColumn("_n", parse_json_reply(F.col("_reply.result"), schema)).filter(
        F.col("_n.headline").isNotNull() & F.col("_n.explanation").isNotNull()
    )


def _capped(name: str, n: int = 3) -> Column:
    return F.slice(F.coalesce(F.col(f"_n.{name}"), F.array().cast("array<string>")), 1, n)


def facility_narratives(current: DataFrame, ai: AiFunctions) -> DataFrame:
    df = current.withColumn("input_json", F.to_json(F.struct(*[_field(f) for f in _FIELDS])))
    return _ask(df, FACILITY_PROMPT, ai, FACILITY_SCHEMA).select(
        "facility_id", "snapshot_ts", "gedwin_band",
        F.col("_n.headline").alias("headline"),
        F.col("_n.explanation").alias("explanation"),
        _capped("recommended_actions").alias("recommended_actions"),
        "top_drivers", "input_json",
        F.lit(ai.endpoint).alias("model"),
        F.current_timestamp().alias("generated_at"),
    )


def network_narrative(current: DataFrame, ai: AiFunctions) -> DataFrame:
    facility = F.struct(
        F.col("facility_name"), F.col("facility_id"), F.col("gedwin_band"), F.round("gedwin", 2).alias("gedwin"),
        F.round("nedocs", 1).alias("nedocs"), F.round("d2p_median_min", 1).alias("d2p_median_min"),
        F.try_element_at(F.col("top_drivers"), F.lit(1))["driver"].alias("top_driver"),
    )
    summary = current.agg(
        F.max("snapshot_ts").alias("snapshot_ts"),
        F.count_if(F.col("gedwin_band") == "Crowded").cast("int").alias("crowded_n"),
        F.count_if(F.col("gedwin_band") == "Busy").cast("int").alias("busy_n"),
        F.count_if(F.col("gedwin_band") == "Active").cast("int").alias("active_n"),
        F.collect_list(facility).alias("facilities"),
    ).withColumn("input_json", F.to_json(F.struct("crowded_n", "busy_n", "active_n", "facilities")))
    return _ask(summary, NETWORK_PROMPT, ai, NETWORK_SCHEMA).select(
        F.lit("NETWORK").alias("network_id"), "snapshot_ts", "crowded_n", "busy_n", "active_n",
        F.col("_n.headline").alias("headline"),
        F.col("_n.explanation").alias("explanation"),
        _capped("crowded_facilities", 50).alias("crowded_facilities"),
        _capped("doing_well_facilities", 50).alias("doing_well_facilities"),
        _capped("recommended_actions").alias("recommended_actions"),
        "input_json",
        F.lit(ai.endpoint).alias("model"),
        F.current_timestamp().alias("generated_at"),
    )


def narrative_tick(spark: SparkSession, cfg: Config, ai: AiFunctions) -> None:
    current = spark.table(cfg.table("gold", "facility_current"))
    merge_into(facility_narratives(current, ai), cfg, "gold", "facility_narrative")
    merge_into(network_narrative(current, ai), cfg, "gold", "network_narrative")


def start_gold_narratives(spark: SparkSession, cfg: Config) -> StreamingQuery:
    endpoint = cfg.llm_endpoint
    return (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .writeStream.foreachBatch(lambda df, _batch_id: narrative_tick(df.sparkSession, cfg, DatabricksAi(endpoint)))
        .trigger(processingTime=f"{cfg.narrative_minutes} minutes")
        .option("checkpointLocation", cfg.checkpoint("gold_narratives"))
        .start()
    )
