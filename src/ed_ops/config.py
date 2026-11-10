"""Runtime configuration shared by every job task. Values come from bundle variables via CLI flags."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, fields

LOGICAL_SCHEMAS = ("bronze", "silver", "gold", "ref", "sim", "serving")


@dataclass(frozen=True)
class Config:
    catalog: str = "hls_jagjit_lb_playground_catalog"
    schema_prefix: str = "ed_"
    facility_count: int = 50
    backfill_days: int = 14
    tick_seconds: int = 60
    dup_rate: float = 0.01
    late_rate: float = 0.02
    malformed_rate: float = 0.005
    live_surge_offset_min: int = 30
    live_surge_duration_min: int = 90
    timezone: str = "America/Chicago"
    seed: int = 42
    checkpoint_version: int = 1
    patient_pool_size: int = 50000
    secret_scope: str = "ed_ops"
    tag_prefix: str = "ed_"  # namespaces UC tag keys so they never collide with existing governed tag policies
    ai_backfill_days: int = 2
    llm_endpoint: str = "databricks-claude-sonnet-5-5"
    narrative_minutes: int = 15
    ai_retry_minutes: int = 15

    def __post_init__(self) -> None:
        if self.tick_seconds <= 0 or self.tick_seconds % 60 != 0:
            raise ValueError("tick_seconds must be a positive multiple of 60 (visit_id has minute precision)")

    def schema(self, logical: str) -> str:
        if logical not in LOGICAL_SCHEMAS:
            raise ValueError(f"unknown logical schema {logical!r}; expected one of {LOGICAL_SCHEMAS}")
        return f"{self.catalog}.{self.schema_prefix}{logical}"

    def table(self, logical: str, name: str) -> str:
        return f"{self.schema(logical)}.{name}"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema_prefix}bronze/landing/events"

    def checkpoint(self, stream: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema_prefix}ref/checkpoints/{stream}/v{self.checkpoint_version}"


def parse_config(argv: list[str]) -> tuple[Config, list[str]]:
    """Parse --field-name / --field_name flags (with or without '=') into a Config; return leftovers."""
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    for f in fields(Config):
        dashed = "--" + f.name.replace("_", "-")
        underscored = "--" + f.name
        flags = [dashed] if dashed == underscored else [dashed, underscored]
        parser.add_argument(*flags, dest=f.name, type=type(f.default), default=f.default)
    namespace, rest = parser.parse_known_args(argv)
    return Config(**vars(namespace)), rest
