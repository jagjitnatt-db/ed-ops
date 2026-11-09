"""Turn metadata definitions into idempotent Databricks SQL statements."""

from __future__ import annotations

from ed_ops.config import Config
from ed_ops.ddl.metadata import CatalogDef, TableDef, VolumeDef


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _tag_pairs(tags: dict[str, str], prefix: str) -> str:
    return ", ".join(f"{sql_string(prefix + k)} = {sql_string(v)}" for k, v in tags.items())


def create_schema_sql(cfg: Config, logical: str, comment: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(logical)} COMMENT {sql_string(comment)}"


def create_volume_sql(cfg: Config, volume: VolumeDef) -> str:
    return f"CREATE VOLUME IF NOT EXISTS {cfg.schema(volume.schema)}.{volume.name} COMMENT {sql_string(volume.comment)}"


def create_table_sql(cfg: Config, t: TableDef) -> str:
    lines = [
        f"{c.name} {c.type}{'' if c.nullable else ' NOT NULL'} COMMENT {sql_string(c.comment)}" for c in t.columns
    ]
    if t.primary_key:
        lines.append(f"CONSTRAINT {t.name}_pk PRIMARY KEY ({', '.join(t.primary_key)})")
    body = ",\n  ".join(lines)
    sql = f"CREATE TABLE IF NOT EXISTS {cfg.table(t.schema, t.name)} (\n  {body}\n)"
    if t.cluster_by:
        sql += f"\nCLUSTER BY ({', '.join(t.cluster_by)})"
    sql += f"\nCOMMENT {sql_string(t.comment)}"
    if t.cdf:
        sql += "\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    return sql


def comment_sqls(cfg: Config, t: TableDef) -> list[str]:
    fq = cfg.table(t.schema, t.name)
    stmts = [f"COMMENT ON TABLE {fq} IS {sql_string(t.comment)}"]
    stmts += [f"ALTER TABLE {fq} ALTER COLUMN {c.name} COMMENT {sql_string(c.comment)}" for c in t.columns]
    return stmts


def tag_sqls(cfg: Config, t: TableDef) -> list[str]:
    fq = cfg.table(t.schema, t.name)
    stmts = [f"ALTER TABLE {fq} SET TAGS ({_tag_pairs(t.tags, cfg.tag_prefix)})"]
    stmts += [f"ALTER TABLE {fq} ALTER COLUMN {c.name} SET TAGS ({_tag_pairs(c.tags, cfg.tag_prefix)})" for c in t.columns if c.tags]
    return stmts


def ddl_statements(cfg: Config, cat: CatalogDef) -> list[str]:
    stmts = [create_schema_sql(cfg, logical, comment) for logical, comment in cat.schemas.items()]
    stmts += [create_volume_sql(cfg, v) for v in cat.volumes]
    stmts += [create_table_sql(cfg, t) for t in cat.tables]
    for t in cat.tables:
        stmts += comment_sqls(cfg, t)
        stmts += tag_sqls(cfg, t)
    return stmts
