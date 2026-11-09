"""Generator progress watermarks (stores are added in Task 13)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class GeneratorState:
    generator_id: str
    backfill_start_ts: datetime
    last_planned_ts: datetime
    last_emitted_ts: datetime
    live_started_ts: datetime | None = None
