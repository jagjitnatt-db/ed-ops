"""Load and validate src/ed_ops/metadata/tables.yml into typed definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path

import yaml

REQUIRED_TABLE_TAGS = ("layer", "domain", "data_classification", "contains_phi", "data_origin")


class MetadataError(ValueError):
    pass


@dataclass(frozen=True)
class ColumnDef:
    name: str
    type: str
    comment: str
    nullable: bool = True
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TableDef:
    schema: str
    name: str
    comment: str
    columns: tuple[ColumnDef, ...]
    cluster_by: tuple[str, ...]
    primary_key: tuple[str, ...]
    cdf: bool
    tags: dict[str, str]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> ColumnDef:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.schema}.{self.name} has no column {name!r}")

    def spark_ddl(self) -> str:
        return ", ".join(f"{c.name} {c.type}" for c in self.columns)


@dataclass(frozen=True)
class VolumeDef:
    schema: str
    name: str
    comment: str


@dataclass(frozen=True)
class CatalogDef:
    schemas: dict[str, str]
    volumes: tuple[VolumeDef, ...]
    tables: tuple[TableDef, ...]

    def table(self, schema: str, name: str) -> TableDef:
        for t in self.tables:
            if t.schema == schema and t.name == name:
                return t
        raise KeyError(f"no table {schema}.{name} in metadata")


def _tag_value(value: object) -> str:
    return str(value).lower() if isinstance(value, bool) else str(value)


def _validate(t: TableDef, schemas: dict[str, str]) -> None:
    where = f"{t.schema}.{t.name}"
    if t.schema not in schemas:
        raise MetadataError(f"{where}: schema {t.schema!r} not declared under 'schemas'")
    if not t.comment.strip():
        raise MetadataError(f"{where}: table comment is required")
    names = t.column_names
    if not names or len(names) != len(set(names)):
        raise MetadataError(f"{where}: columns must be non-empty and unique")
    for c in t.columns:
        if not c.comment.strip():
            raise MetadataError(f"{where}.{c.name}: column comment is required")
    for key in t.cluster_by:
        if key not in names:
            raise MetadataError(f"{where}: cluster key {key!r} is not a column")
    for key in t.primary_key:
        if key not in names:
            raise MetadataError(f"{where}: primary key {key!r} is not a column")
        if t.column(key).nullable:
            raise MetadataError(f"{where}: primary key column {key!r} must be nullable: false")
    for key in REQUIRED_TABLE_TAGS:
        if key not in t.tags:
            raise MetadataError(f"{where}: required table tag {key!r} missing")


def parse_catalog_def(doc: dict) -> CatalogDef:
    schemas = dict(doc["schemas"])
    default_tags = doc.get("defaults", {}).get("table_tags", {})
    tables: list[TableDef] = []
    by_key: dict[tuple[str, str], TableDef] = {}
    for raw in doc["tables"]:
        columns: list[ColumnDef] = []
        if "inherit_columns" in raw:
            parent_key = (raw["inherit_columns"]["schema"], raw["inherit_columns"]["name"])
            if parent_key not in by_key:
                raise MetadataError(f"{raw['schema']}.{raw['name']}: inherit_columns {parent_key} must be defined earlier")
            columns.extend(by_key[parent_key].columns)
        for c in raw.get("columns", []):
            columns.append(
                ColumnDef(
                    name=c["name"],
                    type=c["type"],
                    comment=c.get("comment", ""),
                    nullable=bool(c.get("nullable", True)),
                    tags={k: _tag_value(v) for k, v in c.get("tags", {}).items()},
                )
            )
        tags = {"layer": raw["schema"], **default_tags, **raw.get("tags", {})}
        table = TableDef(
            schema=raw["schema"],
            name=raw["name"],
            comment=raw.get("comment", ""),
            columns=tuple(columns),
            cluster_by=tuple(raw.get("cluster_by", [])),
            primary_key=tuple(raw.get("primary_key", [])),
            cdf=bool(raw.get("cdf", False)),
            tags={k: _tag_value(v) for k, v in tags.items()},
        )
        _validate(table, schemas)
        by_key[(table.schema, table.name)] = table
        tables.append(table)
    volumes = tuple(VolumeDef(**v) for v in doc.get("volumes", []))
    return CatalogDef(schemas=schemas, volumes=volumes, tables=tuple(tables))


def load_catalog_def(path: str | Path | None = None) -> CatalogDef:
    if path is not None:
        text = Path(path).read_text()
    else:
        text = resources.files("ed_ops.metadata").joinpath("tables.yml").read_text()
    return parse_catalog_def(yaml.safe_load(text))
