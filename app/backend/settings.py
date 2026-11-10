"""Runtime settings from the app's environment (set in the bundle's app config)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    schema: str
    lakebase_endpoint: str
    genie_space_title: str
    local_tz: str
    cache_seconds: float
    stale_after_seconds: int


def load_settings() -> Settings:
    return Settings(
        schema=os.environ.get("SERVING_SCHEMA", "ed_serving"),
        lakebase_endpoint=os.environ.get("LAKEBASE_ENDPOINT", ""),
        genie_space_title=os.environ.get("GENIE_SPACE_TITLE", "ED Ops Crowding Assistant"),
        local_tz=os.environ.get("LOCAL_TZ", "America/Chicago"),
        cache_seconds=float(os.environ.get("CACHE_SECONDS", "15")),
        stale_after_seconds=int(os.environ.get("STALE_AFTER_SECONDS", "300")),
    )
