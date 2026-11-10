"""Automated governance checks (spec 11.5) over UC information_schema and the data itself.
Tags are regular UC tags; every key carries cfg.tag_prefix."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ed_ops.config import LOGICAL_SCHEMAS, Config
from ed_ops.ddl.metadata import load_catalog_def
from ed_ops.generator.population import FIRST_NAMES, LAST_NAMES
from ed_ops.pipeline.silver_notes_ai import phi_hit

REQUIRED_TABLE_TAGS = ("layer", "domain", "data_classification", "contains_phi", "data_origin")
RAW_PHI_LAYERS = ("bronze", "sim")
NO_PHI_TAG_LAYERS = ("gold", "ref", "serving")
SCAN_LAYERS = ("silver", "gold", "ref")
MAX_AI_MASK_MISS_RATE = 0.02
NAME_PATTERN = r"\b(" + "|".join(FIRST_NAMES) + r")\s+(" + "|".join(LAST_NAMES) + r")\b"


@dataclass(frozen=True)
class Violation:
    check: str
    object: str
    detail: str


def _layer(cfg: Config, schema: str) -> str | None:
    if not schema.startswith(cfg.schema_prefix):
        return None
    logical = schema[len(cfg.schema_prefix):]
    return logical if logical in LOGICAL_SCHEMAS else None


def check_comments(tables: DataFrame, columns: DataFrame) -> list[Violation]:
    def blank(c):
        return F.col(c).isNull() | (F.trim(F.col(c)) == "")

    out = [Violation("comment", f"{r.table_schema}.{r.table_name}", "table comment missing")
           for r in tables.filter(blank("comment")).collect()]
    out += [Violation("comment", f"{r.table_schema}.{r.table_name}.{r.column_name}", "column comment missing")
            for r in columns.filter(blank("comment")).collect()]
    return out


def check_table_tags(tables: DataFrame, table_tags: DataFrame, cfg: Config) -> list[Violation]:
    present: dict[tuple[str, str], set[str]] = defaultdict(set)
    for r in table_tags.collect():
        present[(r.schema_name, r.table_name)].add(r.tag_name)
    required = [cfg.tag_prefix + k for k in REQUIRED_TABLE_TAGS]
    out = []
    for r in tables.collect():
        missing = [k for k in required if k not in present[(r.table_schema, r.table_name)]]
        if missing:
            out.append(Violation("table_tags", f"{r.table_schema}.{r.table_name}", f"missing tags {missing}"))
    return out


def check_phi_tags(column_tags: DataFrame, cfg: Config) -> list[Violation]:
    tags: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for r in column_tags.collect():
        tags[(r.schema_name, r.table_name, r.column_name)].add(r.tag_name)
    phi, deid = cfg.tag_prefix + "phi_type", cfg.tag_prefix + "deid_method"
    out = []
    for (schema, table, column), names in sorted(tags.items()):
        if phi not in names:
            continue
        layer, obj = _layer(cfg, schema), f"{schema}.{table}.{column}"
        if layer in NO_PHI_TAG_LAYERS:
            out.append(Violation("phi_tag_in_serving_layer", obj, f"{phi} tag found in {layer}"))
        elif layer not in RAW_PHI_LAYERS and deid not in names:
            out.append(Violation("raw_phi_outside_bronze_sim", obj, f"{phi} without {deid}"))
    return out


def phi_match_counts(df: DataFrame, columns: list[str]) -> dict[str, int]:
    if not columns:
        return {}
    row = df.agg(*[F.count_if(phi_hit(F.col(c))).alias(c) for c in columns]).collect()[0]
    return {c: int(row[c] or 0) for c in columns}


def ai_mask_miss_rate(notes: DataFrame) -> float | None:
    row = notes.agg(
        F.count("note_masked").alias("total"),
        F.count_if(F.col("note_masked").rlike(NAME_PATTERN)).alias("misses"),
    ).collect()[0]
    return None if not row.total else row.misses / row.total


def run_checks(spark: SparkSession, cfg: Config, sample_rows: int = 20000) -> list[Violation]:
    schemas = [cfg.schema_prefix + logical for logical in LOGICAL_SCHEMAS]
    info = f"{cfg.catalog}.information_schema"

    def scoped(view: str, schema_col: str, *cols: str) -> DataFrame:
        return spark.table(f"{info}.{view}").filter(F.col(schema_col).isin(schemas)).select(*cols)

    tables = scoped("tables", "table_schema", "table_schema", "table_name", "comment")
    columns = scoped("columns", "table_schema", "table_schema", "table_name", "column_name", "comment")
    table_tags = scoped("table_tags", "schema_name", "schema_name", "table_name", "tag_name")
    column_tags = scoped("column_tags", "schema_name", "schema_name", "table_name", "column_name", "tag_name")
    violations = check_comments(tables, columns) + check_table_tags(tables, table_tags, cfg) + check_phi_tags(column_tags, cfg)
    for t in load_catalog_def().tables:
        if t.schema not in SCAN_LAYERS:
            continue
        strings = [c.name for c in t.columns if c.type.upper() == "STRING"]
        counts = phi_match_counts(spark.table(cfg.table(t.schema, t.name)).limit(sample_rows), strings)
        violations += [Violation("phi_content", f"{cfg.table(t.schema, t.name)}.{c}", f"{n} sampled values match a PHI pattern")
                       for c, n in counts.items() if n]
    rate = ai_mask_miss_rate(spark.table(cfg.table("silver", "triage_notes")))
    if rate is not None and rate > MAX_AI_MASK_MISS_RATE:
        violations.append(Violation("ai_mask_miss_rate", cfg.table("silver", "triage_notes"),
                                    f"{rate:.2%} > {MAX_AI_MASK_MISS_RATE:.0%}"))
    print(f"governance: {len(violations)} violation(s); ai_mask name miss rate = {rate}")
    return violations
