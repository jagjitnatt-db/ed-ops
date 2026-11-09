# ED Ops — Plan 1: Core Data Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the synthetic patient-journey generator and the bronze → silver → gold streaming pipeline that computes EDWIN, gEDWIN, NEDOCS and door-to-provider metrics for 50 EDs. The plan ends with live gold tables running on the dev workspace.

**Architecture:**
- **Generator:** a Python package `ed_ops` simulates ED visits with load feedback (pure pandas/numpy, using reference data and a patient population built with dbldatagen). It writes JSON events to a Unity Catalog volume.
- **Bronze and silver:** Auto Loader lands the events in bronze. A `foreachBatch` stream validates, de-identifies and folds them into one row per visit in silver.
- **Gold:** a 1-minute "ticker" stream recomputes point-in-time metrics from silver state with a single `compute_snapshots` function, which the 14-day backfill also uses.
- **Deployment:** everything ships as one wheel through a Databricks Asset Bundle with two jobs, `ed_setup` (serverless) and `ed_streaming` (continuous, classic cluster).

**Tech Stack:**
- **Runtime:** Python 3.12, PySpark 4.1 (local Homebrew Spark 4.1.2 for tests; DBR 18 LTS on the workspace), Delta Lake, Auto Loader, Structured Streaming.
- **Libraries:** dbldatagen 0.4.0, pandas, numpy, PyYAML.
- **Tooling:** pytest, hatchling, uv, Databricks Asset Bundles (Databricks CLI).

**Spec:** `docs/superpowers/specs/2026-11-09-ed-crowding-design.md`. Executors should read it; this plan implements §§2–8, §10, §11.2 and §15–17 for the core platform.

**This is Plan 1 of 3:**
- **Plan 2 (AI and governance):** `silver_notes_ai`, `ai_retry_sweep`, narratives, governed tag policies, governance checks and `ed_validate`.
- **Plan 3 (serving and experience):** Lakebase, synced tables, the app, Genie and the runbook.

Plan 1 creates the `silver.triage_notes` table empty so gold can already left-join it.

## Global Constraints

- **Toolchain:**
  - Python **3.12** (`requires-python = ">=3.12"`).
  - Local Spark is the Homebrew install at `/opt/homebrew/Cellar/apache-spark/4.1.2/libexec` with Java 17 at `/opt/homebrew/opt/openjdk@17`. **Never `pip install pyspark`.**
  - Delta is **not** available locally (Maven Central is blocked), so MERGE statements are tested as SQL strings locally and exercised on the workspace in Task 16.
- **Workspace and catalog:**
  - Dev workspace `https://fevm-hls-jagjit-lb-playground.cloud.databricks.com`, CLI profile `fevm-playground`.
  - Catalog `hls_jagjit_lb_playground_catalog` (exists; **never create or drop a catalog**).
  - Schemas are `{schema_prefix}{logical}` with `schema_prefix` default `ed_`. Logical schemas: `bronze`, `silver`, `gold`, `ref`, `sim`, `serving`.
- **Datetimes:**
  - **All Python datetimes are naive UTC.** Spark session time zone is `UTC`.
  - Local-time logic uses `zoneinfo.ZoneInfo(cfg.timezone)` (default `America/Chicago`).
- **Streaming compute:** `ed_streaming` runs on a **classic job cluster**: DBR **18 LTS** (`18.x-scala2.13`), **standard access mode** (`USER_ISOLATION`). `ed_setup` runs on serverless.
- **Tables:**
  - Every table is liquid-clustered (`CLUSTER BY`) on its MERGE keys, or on its main query keys if append-only.
  - CDF is enabled on tables later synced to Lakebase.
  - Every table and column has a comment.
  - All of this comes from `src/ed_ops/metadata/tables.yml` (spec §10; moved inside the package so it ships in the wheel).
- **No deletes anywhere.** `gold.ed_board_current` is insert/update only, with `in_ed`.
- **No Unity Catalog column masks or ABAC.** Structured PHI is de-identified deterministically in `silver_visits` (spec §11.2). Raw PHI stays only in `bronze` and `sim`.
- **Metric definitions** are exactly as in spec §7, including:
  - gEDWIN natural log;
  - untriaged patients weighted t = 3;
  - occupancy clamp [0.01, 0.99];
  - EDWIN bands 1.5 / 2;
  - gEDWIN bands 2.06 / 2.34;
  - NEDOCS clamp 0–200 with cuts 20/60/100/140/180.
- **Event `visit_id` format:** `{facility_id}-{arrival yyyyMMddHHmm}-{seq:03d}`. `tick_seconds` must be a multiple of 60.
- **Arithmetic:** Spark runs with ANSI mode on (the Spark 4 / DBR 18 default). Use `F.try_divide` for every division whose denominator can be 0.
- **Commits:** every commit message ends with a blank line followed by `Co-authored-by: Isaac <no-reply@databricks.com>`.

**Deviations from spec, decided in this plan:**
1. `silver_visits` deduplicates `event_id` **within each micro-batch** and relies on the idempotent `COALESCE` MERGE for duplicates across batches. It does **not** use `dropDuplicatesWithinWatermark`: that adds streaming state plus watermark-based dropping of legitimately late backfill events, and buys no correctness the MERGE doesn't already give.
2. The spec's `impossible_order` quarantine reason needs cross-event state. Instead, gold ignores negative intervals (for example, greet before arrival) when computing waits. Quarantine reason codes are: `unparseable`, `missing_event_ts`, `missing_facility_id`, `missing_visit_id`, `invalid_event_type`, `invalid_esi`, `unknown_facility`.
3. Thin per-command console scripts in the wheel replace the spec's `jobs/` directory.
4. dbldatagen builds a 50k-patient **population** table (`sim.patient_pool`) once at setup, and each arrival samples a patient from it. This avoids one Spark job per generator tick.
5. Governed tag **policies** move to Plan 2. Plan 1 applies the tag key/values with `SET TAGS`.

## Review Focus

1. **Generator restart after hours of downtime**: one `advance()` call must catch up in hourly chunks with unique visit IDs and one landing file per chunk (test in Task 13).
2. **Visits crossing midnight or the backfill day-chunk boundary**: a patient who arrived at 23:30 must count in the census at 00:05 the next day (test in Task 4).
3. **Out-of-order events in one micro-batch** (DEPARTURE before ARRIVAL): folding must give the same visit row as in-order events (test in Task 14).
4. **DST change (2026-11-01, America/Chicago)**: the roster must cover every UTC hour exactly once across the fall-back night (test in Task 11).
5. **A facility with no visits and no roster rows**: snapshots must still produce a row with census 0, `gedwin_band = 'Active'`, null EDWIN and the `no_attending_on_duty` flag rather than dropping the facility (test in Task 4).

---

## File Structure

```
pyproject.toml                         package, deps, console scripts, pytest config
databricks.yml                         bundle: artifacts, variables, dev target
resources/jobs.yml                     ed_setup (serverless) + ed_streaming (continuous, classic)
scripts/test-env.sh                    exports JAVA_HOME/SPARK_HOME/PYTHONPATH for local Spark
scripts/pytest.sh                      runs pytest inside the env
scripts/run_sql.py                     runs SQL on the dev warehouse via the CLI (smoke checks)
scripts/teardown.sh                    drops {prefix}* schemas (never the catalog)
src/ed_ops/__init__.py
src/ed_ops/config.py                   Config dataclass, naming helpers, CLI arg parsing
src/ed_ops/timeutil.py                 naive-UTC helpers (floor, 5-min marks, local time)
src/ed_ops/events_contract.py          EVENT_TYPES, payload fields, bronze EVENT_SCHEMA_DDL
src/ed_ops/metadata/__init__.py
src/ed_ops/metadata/tables.yml         single source: schemas, volumes, tables, columns, comments, tags
src/ed_ops/ddl/__init__.py
src/ed_ops/ddl/metadata.py             YAML -> dataclasses + validation
src/ed_ops/ddl/generate.py             dataclasses -> SQL statements
src/ed_ops/ddl/apply.py                run statements on Spark
src/ed_ops/delta.py                    conform(), merge_sql(), merge_into(), append_into()
src/ed_ops/metrics/__init__.py
src/ed_ops/metrics/formulas.py         EDWIN/gEDWIN/NEDOCS Spark Column expressions
src/ed_ops/metrics/snapshots.py        compute_snapshots() point-in-time + window metrics
src/ed_ops/metrics/drivers.py          compute_baselines(), add_drivers(), with_null_drivers()
src/ed_ops/metrics/board.py            board_rows() for gold.ed_board_current
src/ed_ops/generator/__init__.py
src/ed_ops/generator/curves.py         arrival curves, ESI mixes, duration medians
src/ed_ops/generator/scenarios.py      flu + live surge windows
src/ed_ops/generator/notes_templates.py  36 triage-note templates with PHI slots
src/ed_ops/generator/journey.py        plan_visit(), plan_arrivals(), census_at()
src/ed_ops/generator/events.py         events_in_window() with chaos, LandingWriter
src/ed_ops/generator/facilities.py     build_facilities() (dbldatagen + rules)
src/ed_ops/generator/roster.py         build_roster()
src/ed_ops/generator/population.py     build_patient_pool() (dbldatagen)
src/ed_ops/generator/reference.py      ReferenceData, load_reference()
src/ed_ops/generator/state.py          GeneratorState (progress watermarks)
src/ed_ops/generator/stores.py         InMemoryStore (tests), DeltaStore (sim.* tables)
src/ed_ops/generator/tick.py           advance(): plan + emit in hourly chunks
src/ed_ops/governance/__init__.py
src/ed_ops/governance/deid.py          deidentify(), get_salt(), ensure_salt()
src/ed_ops/pipeline/__init__.py
src/ed_ops/pipeline/bronze.py          Auto Loader stream
src/ed_ops/pipeline/silver_visits.py   validate_events(), fold_events(), visits_merge_sql(), stream
src/ed_ops/pipeline/gold_metrics.py    gold_tick(), backfill_gold(), stream
src/ed_ops/setup.py                    ed_setup task functions
src/ed_ops/streaming.py                ed_streaming task functions
src/ed_ops/cli.py                      console-script entry points
tests/conftest.py                      local SparkSession fixture
tests/factories.py                     Spark test-data builders (visits, roster, facilities, scenario)
tests/gen_factories.py                 pure-pandas reference data for generator tests
docs/runbook-plan1.md                  operator notes for Plan 1
tests/unit/test_*.py                   one file per module
```

---

### Task 1: Repository scaffold, local Spark test harness, config and time helpers

**Files:**
- Create: `pyproject.toml`, `scripts/test-env.sh`, `scripts/pytest.sh`, `src/ed_ops/__init__.py`, `src/ed_ops/config.py`, `src/ed_ops/timeutil.py`, `tests/__init__.py`, `tests/unit/__init__.py`, `tests/conftest.py`
- Test: `tests/unit/test_config.py`, `tests/unit/test_timeutil.py`

**Interfaces:**
- Produces:
  - `Config` (frozen dataclass; fields and defaults below)
  - `Config.schema(logical) -> str`, `Config.table(logical, name) -> str`
  - `Config.landing_path: str`, `Config.checkpoint(stream) -> str`
  - `parse_config(argv: list[str]) -> tuple[Config, list[str]]`
  - `timeutil.utcnow() -> datetime`
  - `timeutil.floor_to(dt, seconds) -> datetime`
  - `timeutil.five_minute_marks(start_exclusive, end_inclusive) -> list[datetime]`
  - `timeutil.to_local(dt, tz_name) -> datetime` (aware)
  - `timeutil.epoch_seconds(dt) -> int`
  - the pytest fixture `spark`

- [ ] **Step 1: Create the package skeleton and tooling files**

`pyproject.toml`:
```toml
[build-system]
requires = ["hatchling>=1.25"]
build-backend = "hatchling.build"

[project]
name = "ed_ops"
version = "0.1.0"
description = "ED crowding command center: synthetic ED patient-flow simulator and streaming medallion pipeline"
requires-python = ">=3.12"
dependencies = [
  "dbldatagen>=0.4.0",
  "pyyaml>=6.0",
  "jmespath>=1.0",
  "pyparsing>=3.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "pandas>=2.2", "numpy>=1.26", "pyarrow>=15.0"]

[project.scripts]
ed-ops-create-objects = "ed_ops.cli:create_objects"
ed-ops-seed-reference = "ed_ops.cli:seed_reference"
ed-ops-backfill-generate = "ed_ops.cli:backfill_generate"
ed-ops-backfill-ingest = "ed_ops.cli:backfill_ingest"
ed-ops-backfill-gold = "ed_ops.cli:backfill_gold"
ed-ops-stream-generator = "ed_ops.cli:stream_generator"
ed-ops-stream-bronze = "ed_ops.cli:stream_bronze"
ed-ops-stream-silver-visits = "ed_ops.cli:stream_silver_visits"
ed-ops-stream-gold-metrics = "ed_ops.cli:stream_gold_metrics"

[tool.hatch.build.targets.wheel]
packages = ["src/ed_ops"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q"
```

`scripts/test-env.sh`:
```bash
#!/usr/bin/env bash
# Source from the repo root: wires the local Homebrew Spark 4.1.2 into the project venv.
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/4.1.2/libexec
PY4J_ZIP="$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip)"
export PYTHONPATH="$SPARK_HOME/python/lib/pyspark.zip:$PY4J_ZIP:$(pwd)/src"
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python"
export _ZO_DOCTOR=0
```

`scripts/pytest.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/test-env.sh
exec .venv/bin/python -m pytest "$@"
```

`src/ed_ops/__init__.py`:
```python
"""ED crowding command center: synthetic ED patient flow, streaming medallion pipeline, crowding metrics."""

__version__ = "0.1.0"
```

`tests/__init__.py` and `tests/unit/__init__.py`: empty files.

`tests/conftest.py`:
```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ed_ops_tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
```

Run:
```bash
chmod +x scripts/*.sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e ".[dev]"
```
Expected: the venv is created and `dbldatagen`, `pandas`, `pytest` and the other dependencies install without errors.

- [ ] **Step 2: Write the failing tests**

`tests/unit/test_config.py`:
```python
import pytest

from ed_ops.config import Config, parse_config


def test_schema_and_table_names_use_prefix():
    cfg = Config()
    assert cfg.schema("silver") == "hls_jagjit_lb_playground_catalog.ed_silver"
    assert cfg.table("gold", "facility_current") == "hls_jagjit_lb_playground_catalog.ed_gold.facility_current"


def test_unknown_logical_schema_rejected():
    with pytest.raises(ValueError, match="unknown logical schema"):
        Config().schema("platinum")


def test_paths():
    cfg = Config(schema_prefix="ed_smoke_", checkpoint_version=3)
    assert cfg.landing_path == "/Volumes/hls_jagjit_lb_playground_catalog/ed_smoke_bronze/landing/events"
    assert cfg.checkpoint("gold_metrics") == (
        "/Volumes/hls_jagjit_lb_playground_catalog/ed_smoke_ref/checkpoints/gold_metrics/v3"
    )


def test_parse_config_accepts_dash_underscore_and_equals_forms():
    cfg, rest = parse_config(["--facility-count", "5", "--schema_prefix=ed_smoke_", "--seed=7", "--extra", "x"])
    assert cfg.facility_count == 5
    assert cfg.schema_prefix == "ed_smoke_"
    assert cfg.seed == 7
    assert rest == ["--extra", "x"]


def test_parse_config_floats():
    cfg, _ = parse_config(["--late-rate", "0.5"])
    assert cfg.late_rate == 0.5


def test_tick_seconds_must_be_multiple_of_60():
    with pytest.raises(ValueError, match="multiple of 60"):
        Config(tick_seconds=30)
```

`tests/unit/test_timeutil.py`:
```python
from datetime import datetime

from ed_ops import timeutil


def test_floor_to_minute_and_hour():
    t = datetime(2026, 11, 9, 12, 34, 56)
    assert timeutil.floor_to(t, 60) == datetime(2026, 11, 9, 12, 34)
    assert timeutil.floor_to(t, 3600) == datetime(2026, 11, 9, 12, 0)


def test_five_minute_marks_are_exclusive_start_inclusive_end():
    marks = timeutil.five_minute_marks(datetime(2026, 11, 9, 12, 0), datetime(2026, 11, 9, 12, 17))
    assert marks == [datetime(2026, 11, 9, 12, 5), datetime(2026, 11, 9, 12, 10), datetime(2026, 11, 9, 12, 15)]


def test_to_local_chicago_standard_time():
    local = timeutil.to_local(datetime(2026, 11, 9, 18, 0), "America/Chicago")
    assert (local.hour, local.utcoffset().total_seconds()) == (12, -6 * 3600)


def test_utcnow_is_naive():
    assert timeutil.utcnow().tzinfo is None


def test_epoch_seconds():
    assert timeutil.epoch_seconds(datetime(1970, 1, 1, 0, 1)) == 60
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_config.py tests/unit/test_timeutil.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.config'`.

- [ ] **Step 4: Implement `config.py` and `timeutil.py`**

`src/ed_ops/config.py`:
```python
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
```

`src/ed_ops/timeutil.py`:
```python
"""Time helpers. Project convention: every Python datetime is naive UTC."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def epoch_seconds(dt: datetime) -> int:
    return int(dt.replace(tzinfo=UTC).timestamp())


def floor_to(dt: datetime, seconds: int) -> datetime:
    epoch = epoch_seconds(dt)
    return datetime.fromtimestamp(epoch - epoch % seconds, UTC).replace(tzinfo=None)


def five_minute_marks(start_exclusive: datetime, end_inclusive: datetime) -> list[datetime]:
    marks: list[datetime] = []
    t = floor_to(end_inclusive, 300)
    while t > start_exclusive:
        marks.append(t)
        t -= timedelta(minutes=5)
    return sorted(marks)


def to_local(dt: datetime, tz_name: str) -> datetime:
    return dt.replace(tzinfo=UTC).astimezone(ZoneInfo(tz_name))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_config.py tests/unit/test_timeutil.py -v`
Expected: 11 passed.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml scripts/test-env.sh scripts/pytest.sh src/ed_ops/__init__.py src/ed_ops/config.py src/ed_ops/timeutil.py tests/
git commit -m "feat: scaffold ed_ops package, local Spark test harness, config and time helpers" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Table metadata (single source) and DDL generation

**Files:**
- Create: `src/ed_ops/metadata/__init__.py`, `src/ed_ops/metadata/tables.yml`, `src/ed_ops/ddl/__init__.py`, `src/ed_ops/ddl/metadata.py`, `src/ed_ops/ddl/generate.py`, `src/ed_ops/ddl/apply.py`, `src/ed_ops/events_contract.py`
- Test: `tests/unit/test_ddl.py`

**Interfaces:**
- Consumes: `Config` (Task 1).
- Produces:
  - `ColumnDef(name, type, comment, nullable, tags)`
  - `TableDef(schema, name, comment, columns, cluster_by, primary_key, cdf, tags)` with `.column_names`, `.column(name)` and `.spark_ddl() -> str` (`"a STRING, b TIMESTAMP"`, no NOT NULL)
  - `VolumeDef(schema, name, comment)`
  - `CatalogDef(schemas, volumes, tables)` with `.table(schema, name)`
  - `load_catalog_def(path=None) -> CatalogDef`, `parse_catalog_def(doc: dict) -> CatalogDef`, `MetadataError`
  - `generate.sql_string(s)`, `create_schema_sql`, `create_volume_sql`, `create_table_sql`, `comment_sqls`, `tag_sqls`
  - `ddl_statements(cfg, cat) -> list[str]`, `apply.apply_ddl(spark, cfg, cat=None) -> int`
  - `events_contract.EVENT_TYPES`, `events_contract.PAYLOAD_DDL`, `events_contract.EVENT_SCHEMA_DDL`

- [ ] **Step 1: Write the event contract** (shared by generator and bronze; the bronze table columns must match it)

`src/ed_ops/events_contract.py`:
```python
"""The landing-event contract shared by the generator (writer) and bronze ingestion (reader)."""

EVENT_TYPES = (
    "ARRIVAL",
    "TRIAGE",
    "BED_ASSIGNED",
    "PROVIDER_GREET",
    "VENT_START",
    "VENT_END",
    "DISPOSITION",
    "LWBS",
    "DEPARTURE",
)

PAYLOAD_DDL = (
    "STRUCT<arrival_mode: STRING, patient_name: STRING, mrn: STRING, dob: DATE, sex: STRING, phone: STRING, "
    "address: STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>, chief_complaint_text: STRING, "
    "esi_level: INT, triage_note: STRING, bed_id: STRING, provider_id: STRING, disposition: STRING>"
)

EVENT_SCHEMA_DDL = (
    "event_id STRING, event_type STRING, event_ts TIMESTAMP, emitted_ts TIMESTAMP, "
    f"facility_id STRING, visit_id STRING, payload {PAYLOAD_DDL}"
)
```

- [ ] **Step 2: Write `tables.yml`**

`src/ed_ops/metadata/__init__.py`: empty file.

`src/ed_ops/metadata/tables.yml`:
```yaml
# Single source of truth for every schema, volume, table, column, comment, PK, clustering key and tag.
# DDL is generated from this file (ed_ops.ddl.generate). Never hand-edit tables in the workspace.
defaults:
  table_tags:
    domain: emergency_department
    data_origin: synthetic

schemas:
  bronze: "Raw landing events from the ED patient-journey simulator. Contains raw PHI; restricted access."
  silver: "Validated, de-identified ED visit state and enriched triage notes."
  gold: "Point-in-time ED crowding metrics (EDWIN, gEDWIN, NEDOCS, door-to-provider) and the live ED board."
  ref: "Reference data (facilities, physician roster) and stream checkpoints."
  sim: "Simulator internals (visit plans, patient population, generator state). Contains raw PHI; not for consumers."
  serving: "Lakebase synced tables that serve the Databricks App (populated in Plan 3)."

volumes:
  - schema: bronze
    name: landing
    comment: "Landing zone for simulator JSON event files, partitioned by UTC yyyy/mm/dd/hh."
  - schema: ref
    name: checkpoints
    comment: "Structured Streaming checkpoints, one versioned folder per stream."

tables:
  - schema: bronze
    name: ed_events
    comment: "Raw ED events ingested by Auto Loader from the landing volume. Append-only; may contain duplicates, late and malformed events by design."
    cluster_by: [facility_id, event_ts]
    tags: {data_classification: phi, contains_phi: "true"}
    columns:
      - {name: event_id, type: STRING, comment: "Deterministic UUIDv5 of visit_id and event_type; identical for duplicate re-emissions."}
      - {name: event_type, type: STRING, comment: "ARRIVAL, TRIAGE, BED_ASSIGNED, PROVIDER_GREET, VENT_START, VENT_END, DISPOSITION, LWBS or DEPARTURE."}
      - {name: event_ts, type: TIMESTAMP, comment: "When the event happened (UTC)."}
      - {name: emitted_ts, type: TIMESTAMP, comment: "When the simulator wrote the event (UTC); later than event_ts for late events."}
      - {name: facility_id, type: STRING, comment: "Facility identifier (FACnnn); unknown or null for some malformed test events."}
      - {name: visit_id, type: STRING, comment: "Visit identifier {facility_id}-{arrival yyyyMMddHHmm}-{seq}; null for some malformed test events."}
      - name: payload
        type: "STRUCT<arrival_mode: STRING, patient_name: STRING, mrn: STRING, dob: DATE, sex: STRING, phone: STRING, address: STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>, chief_complaint_text: STRING, esi_level: INT, triage_note: STRING, bed_id: STRING, provider_id: STRING, disposition: STRING>"
        comment: "Event-type-specific attributes. Contains raw PHI: name, MRN, DOB, phone, address and free-text triage notes and chief complaints."
        tags: {data_classification: phi, phi_type: free_text}
      - {name: _rescued_data, type: STRING, comment: "JSON of fields that did not match the expected schema (Auto Loader rescued data); non-null rows are quarantined in silver."}
      - {name: _source_file, type: STRING, comment: "Landing file the event was read from."}
      - {name: _ingest_ts, type: TIMESTAMP, comment: "When Auto Loader ingested the event (UTC)."}
      - {name: ingest_date, type: DATE, comment: "UTC date of _ingest_ts."}

  - schema: silver
    name: ed_visits
    comment: "One row per ED visit with the latest known timestamp of each journey stage, built by MERGE from bronze events. Structured PHI is de-identified before write."
    primary_key: [facility_id, visit_id]
    cluster_by: [facility_id, visit_id]
    cdf: true
    tags: {data_classification: confidential, contains_phi: "false"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier (FACnnn); joins to ref.facility."}
      - {name: visit_id, type: STRING, nullable: false, comment: "Visit identifier {facility_id}-{arrival yyyyMMddHHmm}-{seq}."}
      - {name: arrival_ts, type: TIMESTAMP, comment: "Arrival (door) time, UTC."}
      - {name: arrival_mode, type: STRING, comment: "walk_in or ambulance."}
      - {name: esi_level, type: INT, comment: "Nurse-assigned Emergency Severity Index 1 (most acute) to 5."}
      - {name: triage_ts, type: TIMESTAMP, comment: "Triage completion time, UTC."}
      - {name: bed_assigned_ts, type: TIMESTAMP, comment: "Treatment bed assignment time, UTC."}
      - {name: greet_ts, type: TIMESTAMP, comment: "Provider greet time, UTC; door-to-provider = greet_ts - arrival_ts."}
      - {name: vent_start_ts, type: TIMESTAMP, comment: "Mechanical ventilation start time, UTC."}
      - {name: vent_end_ts, type: TIMESTAMP, comment: "Mechanical ventilation end time, UTC."}
      - {name: disposition, type: STRING, comment: "DISCHARGE, ADMIT or TRANSFER."}
      - {name: disposition_ts, type: TIMESTAMP, comment: "Disposition decision time, UTC; for ADMIT this is the admit decision that starts boarding."}
      - {name: lwbs_ts, type: TIMESTAMP, comment: "Time the patient left without being seen, UTC."}
      - {name: departure_ts, type: TIMESTAMP, comment: "Time the patient left the ED, UTC: the DEPARTURE event, or lwbs_ts for patients who left without being seen."}
      - {name: patient_name_redacted, type: STRING, comment: "Constant 'REDACTED' when a name was recorded; the raw name exists only in bronze.", tags: {data_classification: confidential, phi_type: name, deid_method: redacted}}
      - {name: mrn_hash, type: STRING, comment: "SHA-256 of MRN plus a secret salt; join and count patients without seeing the MRN.", tags: {data_classification: confidential, phi_type: mrn, deid_method: hashed}}
      - {name: birth_year, type: INT, comment: "Year of birth (date of birth generalised to year).", tags: {data_classification: confidential, phi_type: dob, deid_method: generalized}}
      - {name: age_at_arrival, type: INT, comment: "Age in whole years at arrival_ts.", tags: {data_classification: confidential, phi_type: dob, deid_method: generalized}}
      - {name: sex, type: STRING, comment: "Recorded sex: F or M."}
      - {name: phone_last4, type: STRING, comment: "Last four digits of the contact phone number.", tags: {data_classification: confidential, phi_type: contact, deid_method: generalized}}
      - {name: city, type: STRING, comment: "City of residence (street and ZIP removed).", tags: {data_classification: confidential, phi_type: address, deid_method: generalized}}
      - {name: state, type: STRING, comment: "State of residence.", tags: {data_classification: confidential, phi_type: address, deid_method: generalized}}
      - {name: last_event_ts, type: TIMESTAMP, comment: "Latest event_ts merged into this visit, UTC."}
      - {name: updated_at, type: TIMESTAMP, comment: "When the row was last merged, UTC."}

  - schema: silver
    name: ed_events_quarantine
    comment: "Events rejected by silver validation, with a reason code. Holds envelope fields only (no PHI)."
    cluster_by: [ingest_date, facility_id]
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: event_id, type: STRING, comment: "event_id of the rejected event."}
      - {name: event_type, type: STRING, comment: "event_type of the rejected event (may be invalid)."}
      - {name: event_ts, type: TIMESTAMP, comment: "event_ts of the rejected event, UTC."}
      - {name: emitted_ts, type: TIMESTAMP, comment: "emitted_ts of the rejected event, UTC."}
      - {name: facility_id, type: STRING, comment: "facility_id of the rejected event (may be unknown or null)."}
      - {name: visit_id, type: STRING, comment: "visit_id of the rejected event (may be null)."}
      - {name: reason_code, type: STRING, comment: "unparseable, missing_event_ts, missing_facility_id, missing_visit_id, invalid_event_type, invalid_esi or unknown_facility."}
      - {name: esi_level, type: INT, comment: "payload.esi_level of the rejected event, kept to diagnose invalid_esi."}
      - {name: source_file, type: STRING, comment: "Landing file of the rejected event."}
      - {name: quarantined_at, type: TIMESTAMP, comment: "When the event was quarantined, UTC."}
      - {name: ingest_date, type: DATE, comment: "Bronze ingest date of the rejected event."}

  - schema: silver
    name: triage_notes
    comment: "Triage notes and chief complaints masked with ai_mask plus a regex post-check, with AI-derived category, vitals and suggested ESI. Populated by Plan 2."
    primary_key: [facility_id, visit_id]
    cluster_by: [facility_id, visit_id]
    tags: {data_classification: confidential, contains_phi: "false"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier (FACnnn)."}
      - {name: visit_id, type: STRING, nullable: false, comment: "Visit identifier; joins to silver.ed_visits."}
      - {name: triage_ts, type: TIMESTAMP, comment: "Triage time of the note, UTC."}
      - {name: esi_level, type: INT, comment: "Nurse-assigned ESI recorded with the note."}
      - {name: note_masked, type: STRING, comment: "Triage note with PHI masked by ai_mask and a deterministic regex pass.", tags: {data_classification: confidential, phi_type: free_text, deid_method: ai_mask}}
      - {name: chief_complaint_masked, type: STRING, comment: "Chief complaint with PHI masked by ai_mask and a deterministic regex pass.", tags: {data_classification: confidential, phi_type: free_text, deid_method: ai_mask}}
      - {name: residual_phi_flag, type: BOOLEAN, comment: "True when the regex post-check found and redacted PHI that ai_mask missed."}
      - {name: complaint_category, type: STRING, comment: "ai_classify category: cardiac, respiratory, neurological, trauma, gastrointestinal, infectious, psychiatric, obstetric, musculoskeletal or other."}
      - {name: heart_rate, type: INT, comment: "Heart rate (beats/min) extracted by ai_extract."}
      - {name: bp_systolic, type: INT, comment: "Systolic blood pressure (mmHg) extracted by ai_extract."}
      - {name: bp_diastolic, type: INT, comment: "Diastolic blood pressure (mmHg) extracted by ai_extract."}
      - {name: respiratory_rate, type: INT, comment: "Respiratory rate (breaths/min) extracted by ai_extract."}
      - {name: spo2, type: INT, comment: "Oxygen saturation (%) extracted by ai_extract."}
      - {name: temperature_c, type: DOUBLE, comment: "Temperature in Celsius extracted by ai_extract (Fahrenheit converted)."}
      - {name: pain_score, type: INT, comment: "Pain score 0-10 extracted by ai_extract."}
      - {name: symptom_onset, type: STRING, comment: "Symptom onset as free text extracted by ai_extract."}
      - {name: suggested_esi, type: INT, comment: "ESI 1-5 suggested by ai_query from the masked note (demo decision support, not clinical advice)."}
      - {name: esi_rationale, type: STRING, comment: "Short ai_query rationale for suggested_esi."}
      - {name: undertriage_flag, type: BOOLEAN, comment: "True when esi_level - suggested_esi >= 2."}
      - {name: ai_enriched, type: BOOLEAN, comment: "True when the AI chain ran; false for backfill notes older than ai_backfill_days."}
      - {name: ai_status, type: STRING, comment: "ok, error or parse_error."}
      - {name: ai_error, type: STRING, comment: "Last AI error message, if any."}
      - {name: ai_attempts, type: INT, comment: "Number of AI chain attempts."}
      - {name: processed_at, type: TIMESTAMP, comment: "When the row was last written, UTC."}

  - schema: gold
    name: facility_snapshot_5min
    comment: "ED crowding metrics per facility every 5 minutes (UTC), recomputed for the trailing 30 minutes each tick so late events self-correct. History for trends and Genie."
    primary_key: [facility_id, snapshot_ts]
    cluster_by: [facility_id, snapshot_ts]
    cdf: true
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier (FACnnn); joins to ref.facility."}
      - {name: snapshot_ts, type: TIMESTAMP, nullable: false, comment: "Point in time (UTC) the metrics describe."}
      - {name: census, type: INT, comment: "Patients in the ED at snapshot_ts (arrived and not yet departed or left without being seen), including the waiting room; the paper's B_A."}
      - {name: n_esi1, type: INT, comment: "Census patients triaged ESI 1 at snapshot_ts."}
      - {name: n_esi2, type: INT, comment: "Census patients triaged ESI 2 at snapshot_ts."}
      - {name: n_esi3, type: INT, comment: "Census patients triaged ESI 3 at snapshot_ts."}
      - {name: n_esi4, type: INT, comment: "Census patients triaged ESI 4 at snapshot_ts."}
      - {name: n_esi5, type: INT, comment: "Census patients triaged ESI 5 at snapshot_ts."}
      - {name: n_untriaged, type: INT, comment: "Census patients not yet triaged at snapshot_ts (weighted as ESI 3 in acuity)."}
      - {name: waiting_now_n, type: INT, comment: "Census patients not yet greeted by a provider."}
      - {name: in_bed_n, type: INT, comment: "Census patients assigned a treatment bed."}
      - {name: longest_current_wait_min, type: DOUBLE, comment: "Minutes since arrival of the longest-waiting census patient not yet greeted by a provider."}
      - {name: d2p_mean_min, type: DOUBLE, comment: "Door-to-provider mean, minutes, over patients greeted in the 60 minutes ending snapshot_ts. Null when d2p_sample_n = 0."}
      - {name: d2p_median_min, type: DOUBLE, comment: "Door-to-provider median (exact percentile), minutes, over patients greeted in the 60 minutes ending snapshot_ts."}
      - {name: d2p_p90_min, type: DOUBLE, comment: "Door-to-provider 90th percentile (exact), minutes, over patients greeted in the 60 minutes ending snapshot_ts."}
      - {name: d2p_sample_n, type: INT, comment: "Patients greeted in the 60 minutes ending snapshot_ts; door-to-provider statistics are low-sample below 5."}
      - {name: occupancy_ratio, type: DOUBLE, comment: "census / ed_beds (OR); may exceed 1 when patients outnumber treatment spaces."}
      - {name: acuity_ratio, type: DOUBLE, comment: "Mean triage weight of census patients, sum(n_i * t_i) / census with t_i = 6 - ESI and untriaged = 3 (AR). Null when census = 0."}
      - {name: attendings_on_duty, type: INT, comment: "Attending physicians on duty at snapshot_ts from ref.physician_roster (N_a)."}
      - {name: edwin, type: DOUBLE, comment: "EDWIN = (1/N_a) * OR_c/(1-OR_c) * AR with OR_c = occupancy clamped to [0.01, 0.99] (gEDWIN paper eq. 6). 0 when census = 0; null when no attending is on duty."}
      - {name: edwin_band, type: STRING, comment: "EDWIN band: Active (< 1.5), Busy (1.5 to 2), Crowded (> 2)."}
      - {name: gedwin, type: DOUBLE, comment: "gEDWIN = ln(OR_c/(1-OR_c)) + ln(AR); natural log assumed (the paper does not state the base). Null when census = 0."}
      - {name: gedwin_band, type: STRING, comment: "Headline crowding status from gEDWIN: Active (< 2.06), Busy (2.06 to 2.34), Crowded (> 2.34). Active when census = 0."}
      - {name: gedwin_delta_1h, type: DOUBLE, comment: "gEDWIN at snapshot_ts minus gEDWIN one hour earlier; positive means deteriorating."}
      - {name: boarders_n, type: INT, comment: "Census patients with an ADMIT disposition still in the ED (boarders)."}
      - {name: vents_n, type: INT, comment: "Census patients on a ventilator at snapshot_ts."}
      - {name: longest_boarder_h, type: DOUBLE, comment: "Hours since admit decision of the longest-waiting boarder; null when there are no boarders."}
      - {name: last_bedded_wait_h, type: DOUBLE, comment: "Door-to-bed hours of the most recent patient assigned a bed in the 24 hours ending snapshot_ts."}
      - {name: nedocs, type: DOUBLE, comment: "NEDOCS = -20 + 85.8*(census/ed_beds) + 600*(boarders/inpatient_beds) + 13.4*vents + 0.93*longest_boarder_h + 5.64*last_bedded_wait_h, clamped to 0-200."}
      - {name: nedocs_level, type: INT, comment: "NEDOCS level: 1 not busy (<= 20), 2 busy (<= 60), 3 extremely busy (<= 100), 4 overcrowded (<= 140), 5 severely overcrowded (<= 180), 6 dangerously overcrowded."}
      - {name: lwbs_rate_24h, type: DOUBLE, comment: "Share of departures in the 24 hours ending snapshot_ts that left without being seen."}
      - {name: los_median_h_24h, type: DOUBLE, comment: "Median length of stay, hours, of discharged patients who departed in the 24 hours ending snapshot_ts."}
      - {name: los_p90_h_24h, type: DOUBLE, comment: "90th percentile length of stay, hours, of discharged patients who departed in the 24 hours ending snapshot_ts."}
      - {name: boarding_median_h_24h, type: DOUBLE, comment: "Median hours from admit decision to departure for admitted patients who departed in the 24 hours ending snapshot_ts."}
      - {name: admit_rate_24h, type: DOUBLE, comment: "Share of dispositions in the 24 hours ending snapshot_ts that were admissions."}
      - {name: arrivals_1h, type: INT, comment: "Arrivals in the 60 minutes ending snapshot_ts."}
      - {name: ambulance_arrivals_1h, type: INT, comment: "Ambulance arrivals in the 60 minutes ending snapshot_ts."}
      - {name: arrival_surge_ratio, type: DOUBLE, comment: "arrivals_1h divided by the facility's mean arrivals_1h for the same local hour over the baseline history."}
      - {name: patients_per_attending, type: DOUBLE, comment: "census / attendings_on_duty."}
      - {name: boarder_share, type: DOUBLE, comment: "boarders_n / census."}
      - {name: high_acuity_share, type: DOUBLE, comment: "(n_esi1 + n_esi2) / census."}
      - {name: undertriage_flags_n, type: INT, comment: "Census patients whose AI-suggested ESI is at least 2 levels more acute than the nurse ESI (0 until AI enrichment runs)."}
      - {name: top_drivers, type: "ARRAY<STRUCT<driver: STRING, value: DOUBLE, baseline: DOUBLE, z: DOUBLE>>", comment: "Up to 3 crowding drivers deviating most (|z| >= 1) from the facility's baseline for the same local hour, most extreme first. Drivers: occupancy_ratio, boarder_share, patients_per_attending, arrivals_1h, high_acuity_share."}
      - {name: dq_flags, type: "ARRAY<STRING>", comment: "Data-quality flags: no_attending_on_duty, census_exceeds_beds."}
      - {name: computed_at, type: TIMESTAMP, comment: "When the row was computed, UTC."}

  - schema: gold
    name: facility_current
    comment: "Latest crowding metrics, one row per facility, refreshed every minute by the gold ticker. Headline table for the app and Genie."
    inherit_columns: {schema: gold, name: facility_snapshot_5min}
    primary_key: [facility_id]
    cluster_by: [facility_id]
    cdf: true
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: facility_name, type: STRING, comment: "Facility display name."}
      - {name: facility_type, type: STRING, comment: "level_1_trauma, community, rural_critical_access or pediatric."}
      - {name: region, type: STRING, comment: "Health-system region of the facility."}

  - schema: gold
    name: ed_board_current
    comment: "De-identified live ED board: patients currently in the ED plus those who departed in the last 30 minutes. Insert/update only; filter in_ed = true for the current board."
    primary_key: [facility_id, visit_id]
    cluster_by: [facility_id, visit_id]
    cdf: true
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier (FACnnn)."}
      - {name: visit_id, type: STRING, nullable: false, comment: "Visit identifier."}
      - {name: in_ed, type: BOOLEAN, comment: "True while the patient is in the ED as of as_of_ts."}
      - {name: status, type: STRING, comment: "waiting (not yet greeted by a provider), in_bed (greeted or in a treatment bed), boarding (admitted, awaiting an inpatient bed) or departed."}
      - {name: esi_level, type: INT, comment: "Nurse ESI if triaged by as_of_ts."}
      - {name: suggested_esi, type: INT, comment: "AI-suggested ESI (Plan 2)."}
      - {name: undertriage_flag, type: BOOLEAN, comment: "AI under-triage flag (Plan 2)."}
      - {name: complaint_category, type: STRING, comment: "AI complaint category (Plan 2)."}
      - {name: arrival_ts, type: TIMESTAMP, comment: "Arrival time, UTC."}
      - {name: departed_ts, type: TIMESTAMP, comment: "Departure time when status is departed, UTC."}
      - {name: minutes_in_ed, type: INT, comment: "Minutes from arrival to as_of_ts (or to departure once departed)."}
      - {name: minutes_waiting, type: INT, comment: "Minutes from arrival to provider greet (or to as_of_ts/departure if never greeted)."}
      - {name: arrival_mode, type: STRING, comment: "walk_in or ambulance."}
      - {name: on_vent, type: BOOLEAN, comment: "True when on a ventilator at as_of_ts."}
      - {name: as_of_ts, type: TIMESTAMP, comment: "Gold tick time the row describes, UTC."}
      - {name: computed_at, type: TIMESTAMP, comment: "When the row was computed, UTC."}

  - schema: ref
    name: facility
    comment: "The 50 simulated emergency departments with capacity and scenario profile."
    primary_key: [facility_id]
    cluster_by: [facility_id]
    cdf: true
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier FAC001-FAC050."}
      - {name: facility_name, type: STRING, comment: "Synthetic facility display name."}
      - {name: region, type: STRING, comment: "Health-system region (Central, Metro, North, South, West)."}
      - {name: city, type: STRING, comment: "City of the facility."}
      - {name: state, type: STRING, comment: "US state of the facility."}
      - {name: latitude, type: DOUBLE, comment: "Latitude (WGS84)."}
      - {name: longitude, type: DOUBLE, comment: "Longitude (WGS84)."}
      - {name: facility_type, type: STRING, comment: "level_1_trauma, community, rural_critical_access or pediatric."}
      - {name: ed_beds, type: INT, comment: "ED treatment spaces (B_T)."}
      - {name: inpatient_beds, type: INT, comment: "Hospital inpatient beds (NEDOCS denominator)."}
      - {name: baseline_daily_visits, type: INT, comment: "Expected ED visits per day before time-of-day, day-of-week and scenario factors."}
      - {name: scenario_profile, type: STRING, comment: "normal, chronic_boarding, understaffed, surge_prone or high_performer."}

  - schema: ref
    name: physician_roster
    comment: "Attending physicians on duty per facility per 8-hour shift (07-15, 15-23, 23-07 local time)."
    primary_key: [facility_id, shift_start]
    cluster_by: [facility_id, shift_start]
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier (FACnnn)."}
      - {name: shift_start, type: TIMESTAMP, nullable: false, comment: "Shift start, UTC."}
      - {name: shift_end, type: TIMESTAMP, comment: "Shift end (exclusive), UTC."}
      - {name: shift_name, type: STRING, comment: "day, evening or night."}
      - {name: attendings_on_duty, type: INT, comment: "Attending physicians on duty for the shift (N_a)."}

  - schema: sim
    name: visit_plan
    comment: "Simulator plan: every visit's full future journey decided at arrival. Events are emitted from it as their time passes. Contains raw PHI in free text."
    primary_key: [facility_id, visit_id]
    cluster_by: [facility_id, visit_id]
    tags: {data_classification: phi, contains_phi: "true"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier."}
      - {name: visit_id, type: STRING, nullable: false, comment: "Visit identifier."}
      - {name: pool_id, type: BIGINT, comment: "Patient from sim.patient_pool."}
      - {name: arrival_ts, type: TIMESTAMP, comment: "Planned arrival, UTC."}
      - {name: arrival_mode, type: STRING, comment: "walk_in or ambulance."}
      - {name: true_esi, type: INT, comment: "True acuity used for disposition and note text."}
      - {name: nurse_esi, type: INT, comment: "ESI the nurse assigns (differs from true_esi for deliberate under-triage cases)."}
      - {name: archetype, type: STRING, comment: "Complaint archetype used to pick the note template."}
      - {name: triage_ts, type: TIMESTAMP, comment: "Planned triage time, UTC."}
      - {name: bed_assigned_ts, type: TIMESTAMP, comment: "Planned bed time, UTC."}
      - {name: greet_ts, type: TIMESTAMP, comment: "Planned provider greet, UTC."}
      - {name: vent_start_ts, type: TIMESTAMP, comment: "Planned ventilation start, UTC."}
      - {name: vent_end_ts, type: TIMESTAMP, comment: "Planned ventilation end, UTC."}
      - {name: disposition, type: STRING, comment: "Planned disposition."}
      - {name: disposition_ts, type: TIMESTAMP, comment: "Planned disposition time, UTC."}
      - {name: lwbs_ts, type: TIMESTAMP, comment: "Planned left-without-being-seen time, UTC."}
      - {name: departure_ts, type: TIMESTAMP, comment: "Planned departure (equals lwbs_ts for LWBS), UTC."}
      - {name: triage_note, type: STRING, comment: "Raw triage note with PHI.", tags: {data_classification: phi, phi_type: free_text}}
      - {name: chief_complaint_text, type: STRING, comment: "Raw chief complaint, may contain PHI.", tags: {data_classification: phi, phi_type: free_text}}
      - {name: planned_at, type: TIMESTAMP, comment: "Simulated time the visit was planned (window end), UTC."}

  - schema: sim
    name: patient_pool
    comment: "Synthetic patient population generated by dbldatagen; arrivals sample patients from it. Raw synthetic PHI."
    primary_key: [pool_id]
    cluster_by: [pool_id]
    tags: {data_classification: phi, contains_phi: "true"}
    columns:
      - {name: pool_id, type: BIGINT, nullable: false, comment: "Patient index 0..N-1."}
      - {name: first_name, type: STRING, comment: "Synthetic first name.", tags: {data_classification: phi, phi_type: name}}
      - {name: last_name, type: STRING, comment: "Synthetic last name.", tags: {data_classification: phi, phi_type: name}}
      - {name: dob, type: DATE, comment: "Synthetic date of birth.", tags: {data_classification: phi, phi_type: dob}}
      - {name: sex, type: STRING, comment: "F or M."}
      - {name: phone, type: STRING, comment: "Synthetic phone number.", tags: {data_classification: phi, phi_type: contact}}
      - {name: street, type: STRING, comment: "Synthetic street address.", tags: {data_classification: phi, phi_type: address}}
      - {name: zip, type: STRING, comment: "Synthetic ZIP code.", tags: {data_classification: phi, phi_type: address}}
      - {name: mrn, type: STRING, comment: "Synthetic medical record number MRN-nnnnnnn.", tags: {data_classification: phi, phi_type: mrn}}

  - schema: sim
    name: generator_state
    comment: "Simulator progress watermarks; one row per generator."
    primary_key: [generator_id]
    cluster_by: [generator_id]
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: generator_id, type: STRING, nullable: false, comment: "Generator identifier (main)."}
      - {name: backfill_start_ts, type: TIMESTAMP, comment: "Start of the simulated history, UTC."}
      - {name: last_planned_ts, type: TIMESTAMP, comment: "Arrivals are planned up to (excluding) this time, UTC."}
      - {name: last_emitted_ts, type: TIMESTAMP, comment: "Events are emitted up to (including) this time, UTC."}
      - {name: live_started_ts, type: TIMESTAMP, comment: "First live (streaming) tick, UTC; anchors the live surge scenario."}
      - {name: updated_at, type: TIMESTAMP, comment: "When the state was last saved, UTC."}
```

- [ ] **Step 3: Write the failing tests**

`tests/unit/test_ddl.py`:
```python
import pytest

from ed_ops.config import Config
from ed_ops.ddl import generate
from ed_ops.ddl.metadata import MetadataError, load_catalog_def, parse_catalog_def
from ed_ops.events_contract import EVENT_SCHEMA_DDL


def _minimal_doc(**table_overrides):
    table = {
        "schema": "gold",
        "name": "t",
        "comment": "A table.",
        "primary_key": ["id"],
        "cluster_by": ["id"],
        "cdf": True,
        "tags": {"data_classification": "internal", "contains_phi": "false"},
        "columns": [
            {"name": "id", "type": "STRING", "nullable": False, "comment": "Key."},
            {"name": "v", "type": "INT", "comment": "It's a value.", "tags": {"unit": "count"}},
        ],
    }
    table.update(table_overrides)
    return {"defaults": {"table_tags": {"domain": "ed", "data_origin": "synthetic"}}, "schemas": {"gold": "Gold."}, "tables": [table]}


def test_packaged_metadata_loads_and_is_complete():
    cat = load_catalog_def()
    names = {(t.schema, t.name) for t in cat.tables}
    assert ("silver", "ed_visits") in names
    assert ("gold", "facility_current") in names
    current = cat.table("gold", "facility_current")
    assert current.column_names[:2] == ["facility_id", "snapshot_ts"]
    assert current.column_names[-3:] == ["facility_name", "facility_type", "region"]
    for t in cat.tables:
        for key in ("layer", "domain", "data_classification", "contains_phi", "data_origin"):
            assert key in t.tags, (t.name, key)


def test_bronze_columns_match_event_contract(spark):
    bronze = load_catalog_def().table("bronze", "ed_events")
    contract = spark.createDataFrame([], EVENT_SCHEMA_DDL).schema
    assert bronze.column_names[: len(contract.fields)] == [f.name for f in contract.fields]
    table_schema = spark.createDataFrame([], bronze.spark_ddl()).schema
    assert table_schema["payload"].dataType == contract["payload"].dataType
    assert bronze.column_names[len(contract.fields):] == ["_rescued_data", "_source_file", "_ingest_ts", "ingest_date"]


def test_missing_column_comment_rejected():
    doc = _minimal_doc()
    doc["tables"][0]["columns"][1]["comment"] = ""
    with pytest.raises(MetadataError, match="comment"):
        parse_catalog_def(doc)


def test_nullable_primary_key_rejected():
    doc = _minimal_doc()
    doc["tables"][0]["columns"][0]["nullable"] = True
    with pytest.raises(MetadataError, match="primary key"):
        parse_catalog_def(doc)


def test_cluster_key_must_exist():
    with pytest.raises(MetadataError, match="cluster"):
        parse_catalog_def(_minimal_doc(cluster_by=["nope"]))


def test_create_table_sql_has_pk_clustering_cdf_and_escaped_comments():
    cfg = Config()
    t = parse_catalog_def(_minimal_doc()).table("gold", "t")
    sql = generate.create_table_sql(cfg, t)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS hls_jagjit_lb_playground_catalog.ed_gold.t (")
    assert "id STRING NOT NULL COMMENT 'Key.'" in sql
    assert "v INT COMMENT 'It\\'s a value.'" in sql
    assert "CONSTRAINT t_pk PRIMARY KEY (id)" in sql
    assert "CLUSTER BY (id)" in sql
    assert "'delta.enableChangeDataFeed' = 'true'" in sql


def test_tag_and_comment_statements():
    cfg = Config()
    t = parse_catalog_def(_minimal_doc()).table("gold", "t")
    tags = generate.tag_sqls(cfg, t)
    assert tags[0] == (
        "ALTER TABLE hls_jagjit_lb_playground_catalog.ed_gold.t SET TAGS "
        "('layer' = 'gold', 'domain' = 'ed', 'data_origin' = 'synthetic', 'data_classification' = 'internal', 'contains_phi' = 'false')"
    )
    assert tags[1] == "ALTER TABLE hls_jagjit_lb_playground_catalog.ed_gold.t ALTER COLUMN v SET TAGS ('unit' = 'count')"
    comments = generate.comment_sqls(cfg, t)
    assert comments[0] == "COMMENT ON TABLE hls_jagjit_lb_playground_catalog.ed_gold.t IS 'A table.'"
    assert comments[1] == "ALTER TABLE hls_jagjit_lb_playground_catalog.ed_gold.t ALTER COLUMN id COMMENT 'Key.'"


def test_ddl_statements_order_schemas_volumes_tables():
    stmts = generate.ddl_statements(Config(), load_catalog_def())
    first_volume = next(i for i, s in enumerate(stmts) if s.startswith("CREATE VOLUME"))
    first_table = next(i for i, s in enumerate(stmts) if s.startswith("CREATE TABLE"))
    last_schema = max(i for i, s in enumerate(stmts) if s.startswith("CREATE SCHEMA"))
    assert last_schema < first_volume < first_table
    assert sum(s.startswith("CREATE SCHEMA") for s in stmts) == 6


def test_spark_ddl_parses(spark):
    t = load_catalog_def().table("gold", "facility_snapshot_5min")
    df = spark.createDataFrame([], t.spark_ddl())
    assert df.columns == t.column_names
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_ddl.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.ddl'`.

- [ ] **Step 5: Implement metadata loading, DDL generation and apply**

`src/ed_ops/ddl/__init__.py`: empty file.

`src/ed_ops/ddl/metadata.py`:
```python
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
```

`src/ed_ops/ddl/generate.py`:
```python
"""Turn metadata definitions into idempotent Databricks SQL statements."""

from __future__ import annotations

from ed_ops.config import Config
from ed_ops.ddl.metadata import CatalogDef, TableDef, VolumeDef


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _tag_pairs(tags: dict[str, str]) -> str:
    return ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in tags.items())


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
    stmts = [f"ALTER TABLE {fq} SET TAGS ({_tag_pairs(t.tags)})"]
    stmts += [f"ALTER TABLE {fq} ALTER COLUMN {c.name} SET TAGS ({_tag_pairs(c.tags)})" for c in t.columns if c.tags]
    return stmts


def ddl_statements(cfg: Config, cat: CatalogDef) -> list[str]:
    stmts = [create_schema_sql(cfg, logical, comment) for logical, comment in cat.schemas.items()]
    stmts += [create_volume_sql(cfg, v) for v in cat.volumes]
    stmts += [create_table_sql(cfg, t) for t in cat.tables]
    for t in cat.tables:
        stmts += comment_sqls(cfg, t)
        stmts += tag_sqls(cfg, t)
    return stmts
```

`src/ed_ops/ddl/apply.py`:
```python
from __future__ import annotations

from pyspark.sql import SparkSession

from ed_ops.config import Config
from ed_ops.ddl.generate import ddl_statements
from ed_ops.ddl.metadata import CatalogDef, load_catalog_def


def apply_ddl(spark: SparkSession, cfg: Config, cat: CatalogDef | None = None) -> int:
    statements = ddl_statements(cfg, cat or load_catalog_def())
    for stmt in statements:
        try:
            spark.sql(stmt)
        except Exception as exc:
            raise RuntimeError(f"DDL statement failed:\n{stmt}") from exc
    return len(statements)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_ddl.py -v`
Expected: 9 passed.

- [ ] **Step 7: Commit**

```bash
git add src/ed_ops/metadata src/ed_ops/ddl src/ed_ops/events_contract.py tests/unit/test_ddl.py
git commit -m "feat: table metadata single source and DDL generation" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: Crowding formulas (EDWIN, gEDWIN, NEDOCS)

**Files:**
- Create: `src/ed_ops/metrics/__init__.py`, `src/ed_ops/metrics/formulas.py`
- Test: `tests/unit/test_formulas.py`

**Interfaces:**
- Produces: `formulas.triage_weight(esi)`, `clamp(x, lo, hi)`, `occupancy_clamped(census, ed_beds)`, `acuity_ratio(weight_sum, census)`, `edwin(census, ed_beds, attendings, ar)`, `gedwin(census, ed_beds, ar)`, `edwin_band(e)`, `gedwin_band(g, census)`, `nedocs(census, ed_beds, boarders, inpatient_beds, vents, longest_boarder_h, last_bedded_wait_h)` and `nedocs_level(n)`. All take and return `pyspark.sql.Column`. Also exports the constants `OR_FLOOR`, `OR_CAP`, `EDWIN_BUSY`, `EDWIN_CROWDED`, `GEDWIN_BUSY` and `GEDWIN_CROWDED`.

- [ ] **Step 1: Write the failing tests** (expected values are written independently from the paper's formulas)

`tests/unit/test_formulas.py`:
```python
import math

import pytest
from pyspark.sql import functions as F

from ed_ops.metrics import formulas as fx

DDL = "census INT, ed_beds INT, attendings INT, weight_sum INT, boarders INT, inpatient INT, vents INT, lb DOUBLE, lbw DOUBLE"


def _eval(spark, row):
    df = spark.createDataFrame([row], DDL)
    c = F.col
    ar = fx.acuity_ratio(c("weight_sum"), c("census"))
    df = df.withColumn("ar", ar)
    df = df.withColumn("edwin", fx.edwin(c("census"), c("ed_beds"), c("attendings"), c("ar")))
    df = df.withColumn("gedwin", fx.gedwin(c("census"), c("ed_beds"), c("ar")))
    df = df.withColumn("edwin_band", fx.edwin_band(c("edwin")))
    df = df.withColumn("gedwin_band", fx.gedwin_band(c("gedwin"), c("census")))
    df = df.withColumn("nedocs", fx.nedocs(c("census"), c("ed_beds"), c("boarders"), c("inpatient"), c("vents"), c("lb"), c("lbw")))
    df = df.withColumn("nedocs_level", fx.nedocs_level(c("nedocs")))
    return df.collect()[0].asDict()


def test_typical_case(spark):
    # 20 beds, 15 patients: ESI1 x1, ESI2 x3, ESI3 x6, ESI4 x4, ESI5 x1 -> sum(n*t) = 5+12+18+8+1 = 44
    r = _eval(spark, (15, 20, 2, 44, 0, 300, 0, None, None))
    assert r["ar"] == pytest.approx(44 / 15)
    assert r["edwin"] == pytest.approx((1 / 2) * (0.75 / 0.25) * (44 / 15))  # 4.4
    assert r["edwin_band"] == "Crowded"
    assert r["gedwin"] == pytest.approx(math.log(0.75 / 0.25) + math.log(44 / 15))  # 2.1748
    assert r["gedwin_band"] == "Busy"


def test_full_ed_caps_occupancy_at_099(spark):
    r = _eval(spark, (12, 10, 1, 36, 0, 300, 0, None, None))  # OR 1.2, all ESI 3 -> AR 3
    assert r["edwin"] == pytest.approx(99 * 3)
    assert r["gedwin"] == pytest.approx(math.log(99) + math.log(3))
    assert r["gedwin_band"] == "Crowded"


def test_empty_ed(spark):
    r = _eval(spark, (0, 10, 2, 0, 0, 300, 0, None, None))
    assert r["ar"] is None
    assert r["edwin"] == 0.0
    assert r["edwin_band"] == "Active"
    assert r["gedwin"] is None
    assert r["gedwin_band"] == "Active"


def test_no_attending_makes_edwin_null_but_not_gedwin(spark):
    r = _eval(spark, (5, 10, 0, 15, 0, 300, 0, None, None))
    assert r["edwin"] is None and r["edwin_band"] is None
    assert r["gedwin"] == pytest.approx(math.log(0.5 / 0.5) + math.log(3))


def test_null_attending_treated_like_zero(spark):
    r = _eval(spark, (5, 10, None, 15, 0, 300, 0, None, None))
    assert r["edwin"] is None


def test_low_occupancy_floor(spark):
    r = _eval(spark, (1, 200, 1, 3, 0, 300, 0, None, None))  # OR 0.005 -> clamped to 0.01
    assert r["gedwin"] == pytest.approx(math.log(0.01 / 0.99) + math.log(3))


@pytest.mark.parametrize(
    "value,band",
    [(1.49, "Active"), (1.5, "Busy"), (2.0, "Busy"), (2.01, "Crowded")],
)
def test_edwin_band_boundaries(spark, value, band):
    df = spark.createDataFrame([(value,)], "e DOUBLE").select(fx.edwin_band(F.col("e")).alias("b"))
    assert df.collect()[0]["b"] == band


@pytest.mark.parametrize(
    "value,band",
    [(2.05, "Active"), (2.06, "Busy"), (2.34, "Busy"), (2.35, "Crowded")],
)
def test_gedwin_band_boundaries(spark, value, band):
    df = spark.createDataFrame([(value, 5)], "g DOUBLE, census INT").select(fx.gedwin_band(F.col("g"), F.col("census")).alias("b"))
    assert df.collect()[0]["b"] == band


def test_nedocs_worked_example(spark):
    # -20 + 85.8*(30/25) + 600*(6/300) + 13.4*1 + 0.93*5 + 5.64*1.5 = 121.47 -> level 4
    r = _eval(spark, (30, 25, 3, 90, 6, 300, 1, 5.0, 1.5))
    assert r["nedocs"] == pytest.approx(121.47)
    assert r["nedocs_level"] == 4


def test_nedocs_clamps(spark):
    assert _eval(spark, (0, 25, 3, 0, 0, 300, 0, None, None))["nedocs"] == 0.0
    huge = _eval(spark, (100, 10, 3, 300, 90, 100, 5, 40.0, 10.0))
    assert huge["nedocs"] == 200.0 and huge["nedocs_level"] == 6


@pytest.mark.parametrize("score,level", [(20, 1), (20.5, 2), (60, 2), (100, 3), (140, 4), (180, 5), (180.1, 6)])
def test_nedocs_level_cuts(spark, score, level):
    df = spark.createDataFrame([(float(score),)], "n DOUBLE").select(fx.nedocs_level(F.col("n")).alias("l"))
    assert df.collect()[0]["l"] == level


def test_triage_weight(spark):
    df = spark.createDataFrame([(1,), (5,), (None,)], "esi INT").select(fx.triage_weight(F.col("esi")).alias("w"))
    assert [r["w"] for r in df.collect()] == [5, 1, 3]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_formulas.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.metrics'`.

- [ ] **Step 3: Implement the formulas**

`src/ed_ops/metrics/__init__.py`: empty file.

`src/ed_ops/metrics/formulas.py`:
```python
"""Crowding index formulas as Spark Column expressions.

Source: gEDWIN: a simple and practical index for real-time monitoring of ED crowding,
BMC Emergency Medicine 2025 (PMC12648796); NEDOCS: Weiss et al., Acad Emerg Med 2004.
Assumptions (spec section 7): natural log for gEDWIN; untriaged patients weighted t = 3;
occupancy clamped to [0.01, 0.99] (0.99 cap from the paper, 0.01 floor ours).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

OR_FLOOR, OR_CAP = 0.01, 0.99
EDWIN_BUSY, EDWIN_CROWDED = 1.5, 2.0
GEDWIN_BUSY, GEDWIN_CROWDED = 2.06, 2.34


def triage_weight(esi: Column) -> Column:
    return F.when(esi.isNull(), F.lit(3)).otherwise(F.lit(6) - esi)


def clamp(x: Column, lo: float, hi: float) -> Column:
    return F.least(F.greatest(x, F.lit(lo)), F.lit(hi))


def occupancy_clamped(census: Column, ed_beds: Column) -> Column:
    return clamp(F.try_divide(census.cast("double"), ed_beds.cast("double")), OR_FLOOR, OR_CAP)


def acuity_ratio(weight_sum: Column, census: Column) -> Column:
    return F.when(census > 0, F.try_divide(weight_sum.cast("double"), census.cast("double")))


def edwin(census: Column, ed_beds: Column, attendings: Column, ar: Column) -> Column:
    orc = occupancy_clamped(census, ed_beds)
    return (
        F.when(attendings.isNull() | (attendings <= 0), F.lit(None).cast("double"))
        .when(census == 0, F.lit(0.0))
        .otherwise(F.try_divide(orc / (F.lit(1.0) - orc) * ar, attendings.cast("double")))
    )


def gedwin(census: Column, ed_beds: Column, ar: Column) -> Column:
    orc = occupancy_clamped(census, ed_beds)
    return F.when(census > 0, F.log(orc / (F.lit(1.0) - orc)) + F.log(ar))


def edwin_band(e: Column) -> Column:
    return (
        F.when(e.isNull(), F.lit(None).cast("string"))
        .when(e < EDWIN_BUSY, F.lit("Active"))
        .when(e <= EDWIN_CROWDED, F.lit("Busy"))
        .otherwise(F.lit("Crowded"))
    )


def gedwin_band(g: Column, census: Column) -> Column:
    return (
        F.when(census == 0, F.lit("Active"))
        .when(g.isNull(), F.lit(None).cast("string"))
        .when(g < GEDWIN_BUSY, F.lit("Active"))
        .when(g <= GEDWIN_CROWDED, F.lit("Busy"))
        .otherwise(F.lit("Crowded"))
    )


def nedocs(
    census: Column,
    ed_beds: Column,
    boarders: Column,
    inpatient_beds: Column,
    vents: Column,
    longest_boarder_h: Column,
    last_bedded_wait_h: Column,
) -> Column:
    raw = (
        F.lit(-20.0)
        + F.lit(85.8) * F.try_divide(census.cast("double"), ed_beds.cast("double"))
        + F.lit(600.0) * F.try_divide(boarders.cast("double"), inpatient_beds.cast("double"))
        + F.lit(13.4) * vents.cast("double")
        + F.lit(0.93) * F.coalesce(longest_boarder_h, F.lit(0.0))
        + F.lit(5.64) * F.coalesce(last_bedded_wait_h, F.lit(0.0))
    )
    return clamp(raw, 0.0, 200.0)


def nedocs_level(n: Column) -> Column:
    return (
        F.when(n <= 20, F.lit(1))
        .when(n <= 60, F.lit(2))
        .when(n <= 100, F.lit(3))
        .when(n <= 140, F.lit(4))
        .when(n <= 180, F.lit(5))
        .otherwise(F.lit(6))
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_formulas.py -v`
Expected: 24 passed (the parametrized cases count individually).

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/metrics/__init__.py src/ed_ops/metrics/formulas.py tests/unit/test_formulas.py
git commit -m "feat: EDWIN, gEDWIN and NEDOCS formulas as Spark expressions" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: Point-in-time snapshot metrics (`compute_snapshots`)

**Files:**
- Create: `src/ed_ops/metrics/snapshots.py`, `tests/factories.py`
- Test: `tests/unit/test_snapshots.py`

**Interfaces:**
- Consumes: `ed_ops.metrics.formulas` (Task 3).
- Produces:
  - `snapshots.VISIT_COLUMNS: tuple[str, ...]`: the input contract for visits; the gold job builds it from `silver.ed_visits` left-joined to `silver.triage_notes`.
  - `snapshots.METRIC_COLUMNS: tuple[str, ...]`
  - `compute_snapshots(visits, roster, facilities, as_of) -> DataFrame`. Arguments:
    - `visits` has `VISIT_COLUMNS`;
    - `roster` has `facility_id, shift_start, shift_end, attendings_on_duty`;
    - `facilities` has at least `facility_id, ed_beds, inpatient_beds`;
    - `as_of` has a `snapshot_ts` column.

    It returns one row per (facility, requested snapshot_ts) with `METRIC_COLUMNS`. That is every column of `gold.facility_snapshot_5min` **except** `arrival_surge_ratio` and `top_drivers`, which Task 5 adds.
- Produces (tests): `tests/factories.py` with `T`, `m(minutes)`, `VISIT_DDL`, `visit(...)`, `visits_df`, `facilities_df`, `roster_df`, `as_of_df` and `SCENARIO_VISITS`.

- [ ] **Step 1: Write the test factories**

`tests/factories.py`:
```python
"""Test-data builders. All datetimes are naive UTC."""

from datetime import datetime, timedelta

T = datetime(2026, 11, 9, 12, 0)


def m(minutes: float) -> datetime:
    """T shifted by `minutes` (negative = before T)."""
    return T + timedelta(minutes=minutes)


VISIT_DDL = (
    "facility_id STRING, visit_id STRING, arrival_ts TIMESTAMP, arrival_mode STRING, esi_level INT, "
    "triage_ts TIMESTAMP, bed_assigned_ts TIMESTAMP, greet_ts TIMESTAMP, vent_start_ts TIMESTAMP, "
    "vent_end_ts TIMESTAMP, disposition STRING, disposition_ts TIMESTAMP, lwbs_ts TIMESTAMP, "
    "departure_ts TIMESTAMP, suggested_esi INT, undertriage_flag BOOLEAN, complaint_category STRING"
)


def visit(visit_id, arrival, *, facility_id="FAC001", esi=None, triage=None, bed=None, greet=None,
          vent_start=None, vent_end=None, disposition=None, disposition_ts=None, lwbs=None,
          departure=None, mode="walk_in", undertriage=None):
    return (facility_id, visit_id, arrival, mode, esi, triage, bed, greet, vent_start, vent_end,
            disposition, disposition_ts, lwbs, departure, None, undertriage, None)


def visits_df(spark, rows):
    return spark.createDataFrame(rows, VISIT_DDL)


def facilities_df(spark, rows):
    """rows: (facility_id, ed_beds, inpatient_beds)."""
    full = [(fid, f"{fid} Hospital", "community", "Central", beds, inpatient) for fid, beds, inpatient in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, facility_name STRING, facility_type STRING, region STRING, ed_beds INT, inpatient_beds INT"
    )


def roster_df(spark, rows):
    """rows: (facility_id, shift_start, shift_end, attendings_on_duty)."""
    full = [(fid, s, e, "day", n) for fid, s, e, n in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, shift_start TIMESTAMP, shift_end TIMESTAMP, shift_name STRING, attendings_on_duty INT"
    )


def as_of_df(spark, *times):
    return spark.createDataFrame([(t,) for t in times], "snapshot_ts TIMESTAMP")


# Hand-built scenario for FAC001 at T (20 ED beds, 300 inpatient beds, 2 attendings 07:00-15:00 UTC).
SCENARIO_VISITS = [
    # v1: ESI1, bedded, greeted, ventilated, admitted at T-60 and still boarding.
    visit("v1", m(-120), esi=1, triage=m(-118), bed=m(-115), greet=m(-110), vent_start=m(-100),
          disposition="ADMIT", disposition_ts=m(-60)),
    # v2: ESI3 ambulance, bedded T-30, greeted T-20 (d2p 30 min), flagged under-triaged.
    visit("v2", m(-50), esi=3, triage=m(-45), bed=m(-30), greet=m(-20), mode="ambulance", undertriage=True),
    # v3: ESI4 waiting since T-40.
    visit("v3", m(-40), esi=4, triage=m(-35)),
    # v4: arrived T-10, not triaged yet.
    visit("v4", m(-10)),
    # v5: ESI5 greeted T-30 (d2p 60 min), discharged, departed T-5 (LOS 85 min).
    visit("v5", m(-90), esi=5, triage=m(-85), greet=m(-30), disposition="DISCHARGE", disposition_ts=m(-15),
          departure=m(-5)),
    # v6: ESI3 left without being seen at T-200.
    visit("v6", m(-300), esi=3, triage=m(-295), lwbs=m(-200), departure=m(-200)),
    # v7: ESI2 admitted T-150, departed T-30 (boarding 2 h); greet outside the 1 h window.
    visit("v7", m(-200), esi=2, triage=m(-195), greet=m(-190), bed=m(-180), disposition="ADMIT",
          disposition_ts=m(-150), departure=m(-30)),
    # v8: arrives in the future.
    visit("v8", m(10), esi=3, triage=m(12)),
    # v9: arrived T-3, triaged only at T+2 (unknown ESI at T), flag must not count yet.
    visit("v9", m(-3), esi=2, triage=m(2), undertriage=True),
]
```

- [ ] **Step 2: Write the failing tests**

`tests/unit/test_snapshots.py`:
```python
import math
from datetime import datetime

import pytest

from ed_ops.metrics.snapshots import METRIC_COLUMNS, compute_snapshots
from tests.factories import (
    SCENARIO_VISITS, T, as_of_df, facilities_df, m, roster_df, visit, visits_df,
)


def _rows(df):
    return {(r["facility_id"], r["snapshot_ts"]): r.asDict() for r in df.collect()}


@pytest.fixture(scope="module")
def scenario(spark):
    visits = visits_df(spark, SCENARIO_VISITS)
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)])
    facilities = facilities_df(spark, [("FAC001", 20, 300), ("FAC002", 10, 100)])
    return _rows(compute_snapshots(visits, roster, facilities, as_of_df(spark, T)))


def test_columns(spark, scenario):
    assert set(next(iter(scenario.values())).keys()) == set(METRIC_COLUMNS)


def test_census_is_point_in_time(scenario):
    r = scenario[("FAC001", T)]
    assert r["census"] == 5  # v1, v2, v3, v4, v9
    assert (r["n_esi1"], r["n_esi2"], r["n_esi3"], r["n_esi4"], r["n_esi5"]) == (1, 0, 1, 1, 0)
    assert r["n_untriaged"] == 2  # v4 and v9 (triaged after T)
    assert r["waiting_now_n"] == 3  # v3, v4, v9
    assert r["in_bed_n"] == 2
    assert r["longest_current_wait_min"] == pytest.approx(40.0)
    assert r["boarders_n"] == 1 and r["longest_boarder_h"] == pytest.approx(1.0)
    assert r["vents_n"] == 1
    assert r["undertriage_flags_n"] == 1  # v2 only; v9 not yet triaged


def test_door_to_provider_window(scenario):
    r = scenario[("FAC001", T)]
    assert r["d2p_sample_n"] == 2  # v2 (30 min) and v5 (60 min)
    assert r["d2p_mean_min"] == pytest.approx(45.0)
    assert r["d2p_median_min"] == pytest.approx(45.0)
    assert r["d2p_p90_min"] == pytest.approx(57.0)


def test_flow_windows(scenario):
    r = scenario[("FAC001", T)]
    assert r["arrivals_1h"] == 4 and r["ambulance_arrivals_1h"] == 1
    assert r["last_bedded_wait_h"] == pytest.approx(20 / 60)  # v2: arrival T-50, bed T-30
    assert r["lwbs_rate_24h"] == pytest.approx(1 / 3)
    assert r["los_median_h_24h"] == pytest.approx(85 / 60)
    assert r["boarding_median_h_24h"] == pytest.approx(2.0)
    assert r["admit_rate_24h"] == pytest.approx(2 / 3)


def test_indices(scenario):
    r = scenario[("FAC001", T)]
    assert r["attendings_on_duty"] == 2
    assert r["occupancy_ratio"] == pytest.approx(0.25)
    assert r["acuity_ratio"] == pytest.approx(16 / 5)  # 5 + 3 + 2 + 3 + 3
    assert r["edwin"] == pytest.approx(0.5 * (0.25 / 0.75) * 3.2)
    assert r["edwin_band"] == "Active"
    assert r["gedwin"] == pytest.approx(math.log(0.25 / 0.75) + math.log(3.2))
    assert r["gedwin_band"] == "Active"
    assert r["nedocs"] == pytest.approx(-20 + 85.8 * 0.25 + 600 / 300 + 13.4 + 0.93 * 1.0 + 5.64 * (20 / 60))  # 19.66
    assert r["nedocs_level"] == 1
    assert r["patients_per_attending"] == pytest.approx(2.5)
    assert r["boarder_share"] == pytest.approx(0.2)
    assert r["high_acuity_share"] == pytest.approx(0.2)
    assert r["dq_flags"] == []


def test_gedwin_delta_one_hour(scenario):
    # At T-60: v1 (ESI1, boarder), v5 (ESI5), v7 (ESI2) -> census 3, AR = 10/3, OR = 0.15
    prior = math.log(0.15 / 0.85) + math.log(10 / 3)
    r = scenario[("FAC001", T)]
    assert r["gedwin_delta_1h"] == pytest.approx(r["gedwin"] - prior)


def test_facility_without_visits_or_roster_still_reported(scenario):
    r = scenario[("FAC002", T)]
    assert r["census"] == 0
    assert r["edwin"] is None and r["edwin_band"] is None
    assert r["gedwin"] is None and r["gedwin_band"] == "Active"
    assert r["nedocs"] == 0.0 and r["nedocs_level"] == 1
    assert r["dq_flags"] == ["no_attending_on_duty"]
    assert r["d2p_sample_n"] == 0 and r["d2p_mean_min"] is None


def test_over_capacity_is_capped_and_flagged(spark):
    visits = visits_df(spark, [visit(f"c{i}", m(-30), facility_id="FAC003", esi=3, triage=m(-25)) for i in range(6)])
    roster = roster_df(spark, [("FAC003", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 1)])
    r = _rows(compute_snapshots(visits, roster, facilities_df(spark, [("FAC003", 4, 50)]), as_of_df(spark, T)))[("FAC003", T)]
    assert r["occupancy_ratio"] == pytest.approx(1.5)
    assert r["edwin"] == pytest.approx(99 * 3)
    assert r["gedwin"] == pytest.approx(math.log(99) + math.log(3))
    assert r["gedwin_band"] == "Crowded"
    assert r["dq_flags"] == ["census_exceeds_beds"]


def test_visit_crossing_midnight_counts_after_midnight(spark):
    late = datetime(2026, 11, 9, 23, 30)
    snap = datetime(2026, 11, 10, 0, 5)
    visits = visits_df(spark, [visit("mid", late, esi=3, triage=datetime(2026, 11, 9, 23, 40))])
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 23), datetime(2026, 11, 10, 7), 1)])
    r = _rows(compute_snapshots(visits, roster, facilities_df(spark, [("FAC001", 20, 300)]), as_of_df(spark, snap)))[("FAC001", snap)]
    assert r["census"] == 1
    assert r["arrivals_1h"] == 1


def test_negative_door_to_provider_ignored(spark):
    bad = visit("neg", m(-20), esi=3, triage=m(-19), greet=m(-25), disposition="DISCHARGE", disposition_ts=m(-10), departure=m(-2))
    r = _rows(compute_snapshots(visits_df(spark, [bad]), roster_df(spark, []), facilities_df(spark, [("FAC001", 20, 300)]), as_of_df(spark, T)))[("FAC001", T)]
    assert r["d2p_sample_n"] == 0
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_snapshots.py -v`
Expected: FAIL with `ImportError: cannot import name 'compute_snapshots'` (or a module-not-found error).

- [ ] **Step 4: Implement `snapshots.py`**

`src/ed_ops/metrics/snapshots.py`:
```python
"""Point-in-time ED metrics for any set of (facility, snapshot_ts) pairs.

Every window join is turned into an equi-join on (facility_id, hour bucket) by exploding each visit or
event into the hour buckets it can affect, then filtering on the exact condition. This keeps the 14-day
backfill (thousands of snapshot times) tractable without relying on range-join hints.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ed_ops.metrics import formulas as fx

VISIT_COLUMNS = (
    "facility_id", "visit_id", "arrival_ts", "arrival_mode", "esi_level", "triage_ts", "bed_assigned_ts",
    "greet_ts", "vent_start_ts", "vent_end_ts", "disposition", "disposition_ts", "lwbs_ts", "departure_ts",
    "suggested_esi", "undertriage_flag", "complaint_category",
)

METRIC_COLUMNS = (
    "facility_id", "snapshot_ts", "census", "n_esi1", "n_esi2", "n_esi3", "n_esi4", "n_esi5", "n_untriaged",
    "waiting_now_n", "in_bed_n", "longest_current_wait_min", "d2p_mean_min", "d2p_median_min", "d2p_p90_min",
    "d2p_sample_n", "occupancy_ratio", "acuity_ratio", "attendings_on_duty", "edwin", "edwin_band", "gedwin",
    "gedwin_band", "gedwin_delta_1h", "boarders_n", "vents_n", "longest_boarder_h", "last_bedded_wait_h",
    "nedocs", "nedocs_level", "lwbs_rate_24h", "los_median_h_24h", "los_p90_h_24h", "boarding_median_h_24h",
    "admit_rate_24h", "arrivals_1h", "ambulance_arrivals_1h", "patients_per_attending", "boarder_share",
    "high_acuity_share", "undertriage_flags_n", "dq_flags", "computed_at",
)

_COUNT_COLUMNS = (
    "census", "n_esi1", "n_esi2", "n_esi3", "n_esi4", "n_esi5", "n_untriaged", "waiting_now_n", "in_bed_n",
    "boarders_n", "vents_n", "undertriage_flags_n", "d2p_sample_n", "arrivals_1h", "ambulance_arrivals_1h",
    "_weight_sum",
)

HOUR = F.expr("INTERVAL 1 HOUR")
MAX_STAY = F.expr("INTERVAL 3 DAYS")
WIN_1H = F.expr("INTERVAL 60 MINUTES")
WIN_24H = F.expr("INTERVAL 24 HOURS")
T = F.col("snapshot_ts")
KEYS = ["facility_id", "snapshot_ts"]


def _seconds(later: Column, earlier: Column) -> Column:
    return (F.unix_timestamp(later) - F.unix_timestamp(earlier)).cast("double")


def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0)))


def _candidates(df: DataFrame, start: Column, end: Column) -> DataFrame:
    return df.withColumn("_bucket", F.explode(F.sequence(F.date_trunc("hour", start), F.date_trunc("hour", end), HOUR)))


def _pairs(grid_b: DataFrame, candidates: DataFrame, condition: Column) -> DataFrame:
    return grid_b.join(candidates, ["facility_id", "_bucket"]).filter(condition)


def _window_pairs(grid_b: DataFrame, visits: DataFrame, ts_col: str, window: Column) -> DataFrame:
    ts = F.col(ts_col)
    candidates = _candidates(visits.filter(ts.isNotNull()), ts, ts + window)
    return _pairs(grid_b, candidates, (ts > T - window) & (ts <= T))


def _in_ed_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    arrival, departure = F.col("arrival_ts"), F.col("departure_ts")
    end = F.least(F.coalesce(departure, arrival + MAX_STAY), arrival + MAX_STAY)
    pairs = _pairs(
        grid_b,
        _candidates(visits.filter(arrival.isNotNull()), arrival, end),
        (arrival <= T) & (arrival > T - MAX_STAY) & (departure.isNull() | (departure > T)),
    )
    esi_t = F.when(F.col("triage_ts") <= T, F.col("esi_level"))
    greeted = F.coalesce(F.col("greet_ts") <= T, F.lit(False))
    bedded = F.coalesce(F.col("bed_assigned_ts") <= T, F.lit(False))
    boarder = F.coalesce((F.col("disposition") == "ADMIT") & (F.col("disposition_ts") <= T), F.lit(False))
    on_vent = F.coalesce(
        (F.col("vent_start_ts") <= T) & (F.col("vent_end_ts").isNull() | (F.col("vent_end_ts") > T)), F.lit(False)
    )
    flagged = F.coalesce(F.col("undertriage_flag") & (F.col("triage_ts") <= T), F.lit(False))
    pairs = pairs.withColumn("_esi_t", esi_t)
    return pairs.groupBy(*KEYS).agg(
        F.count(F.lit(1)).alias("census"),
        *[_count_if(F.col("_esi_t") == i).alias(f"n_esi{i}") for i in range(1, 6)],
        _count_if(F.col("_esi_t").isNull()).alias("n_untriaged"),
        F.sum(fx.triage_weight(F.col("_esi_t"))).alias("_weight_sum"),
        _count_if(~greeted).alias("waiting_now_n"),
        _count_if(bedded).alias("in_bed_n"),
        F.max(F.when(~greeted, _seconds(T, arrival) / 60.0)).alias("longest_current_wait_min"),
        _count_if(boarder).alias("boarders_n"),
        _count_if(on_vent).alias("vents_n"),
        F.max(F.when(boarder, _seconds(T, F.col("disposition_ts")) / 3600.0)).alias("longest_boarder_h"),
        _count_if(flagged).alias("undertriage_flags_n"),
    )


def _d2p_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    valid = visits.filter(F.col("greet_ts") >= F.col("arrival_ts"))
    pairs = _window_pairs(grid_b, valid, "greet_ts", WIN_1H)
    pairs = pairs.withColumn("_d2p", _seconds(F.col("greet_ts"), F.col("arrival_ts")) / 60.0)
    return pairs.groupBy(*KEYS).agg(
        F.avg("_d2p").alias("d2p_mean_min"),
        F.percentile("_d2p", 0.5).alias("d2p_median_min"),
        F.percentile("_d2p", 0.9).alias("d2p_p90_min"),
        F.count(F.lit(1)).alias("d2p_sample_n"),
    )


def _arrival_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "arrival_ts", WIN_1H)
    return pairs.groupBy(*KEYS).agg(
        F.count(F.lit(1)).alias("arrivals_1h"),
        _count_if(F.col("arrival_mode") == "ambulance").alias("ambulance_arrivals_1h"),
    )


def _last_bedded(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "bed_assigned_ts", WIN_24H)
    wait_h = _seconds(F.col("bed_assigned_ts"), F.col("arrival_ts")) / 3600.0
    return pairs.groupBy(*KEYS).agg(F.max_by(wait_h, F.col("bed_assigned_ts")).alias("last_bedded_wait_h"))


def _departure_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "departure_ts", WIN_24H)
    los_h = F.when(F.col("disposition") == "DISCHARGE", _seconds(F.col("departure_ts"), F.col("arrival_ts")) / 3600.0)
    boarding_h = F.when(F.col("disposition") == "ADMIT", _seconds(F.col("departure_ts"), F.col("disposition_ts")) / 3600.0)
    return pairs.groupBy(*KEYS).agg(
        F.try_divide(_count_if(F.col("lwbs_ts").isNotNull()).cast("double"), F.count(F.lit(1)).cast("double")).alias("lwbs_rate_24h"),
        F.percentile(los_h, 0.5).alias("los_median_h_24h"),
        F.percentile(los_h, 0.9).alias("los_p90_h_24h"),
        F.percentile(boarding_h, 0.5).alias("boarding_median_h_24h"),
    )


def _disposition_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "disposition_ts", WIN_24H)
    return pairs.groupBy(*KEYS).agg(
        F.try_divide(_count_if(F.col("disposition") == "ADMIT").cast("double"), F.count(F.lit(1)).cast("double")).alias("admit_rate_24h")
    )


def _attendings(grid_b: DataFrame, roster: DataFrame) -> DataFrame:
    start, end = F.col("shift_start"), F.col("shift_end")
    pairs = _pairs(grid_b, _candidates(roster, start, end), (start <= T) & (end > T))
    return pairs.groupBy(*KEYS).agg(F.sum("attendings_on_duty").cast("int").alias("attendings_on_duty"))


def _metrics_at(visits: DataFrame, roster: DataFrame, facilities: DataFrame, times: DataFrame) -> DataFrame:
    grid = facilities.select("facility_id", "ed_beds", "inpatient_beds").crossJoin(F.broadcast(times))
    grid_b = grid.withColumn("_bucket", F.date_trunc("hour", T))
    df = grid
    for part in (
        _in_ed_stats(grid_b, visits),
        _d2p_stats(grid_b, visits),
        _arrival_stats(grid_b, visits),
        _last_bedded(grid_b, visits),
        _departure_stats(grid_b, visits),
        _disposition_stats(grid_b, visits),
        _attendings(grid_b, roster),
    ):
        df = df.join(part, KEYS, "left")
    df = df.fillna(0, subset=list(_COUNT_COLUMNS))
    census, beds, attendings = F.col("census"), F.col("ed_beds"), F.col("attendings_on_duty")
    df = (
        df.withColumn("occupancy_ratio", F.try_divide(census.cast("double"), beds.cast("double")))
        .withColumn("acuity_ratio", fx.acuity_ratio(F.col("_weight_sum"), census))
        .withColumn("edwin", fx.edwin(census, beds, attendings, F.col("acuity_ratio")))
        .withColumn("edwin_band", fx.edwin_band(F.col("edwin")))
        .withColumn("gedwin", fx.gedwin(census, beds, F.col("acuity_ratio")))
        .withColumn("gedwin_band", fx.gedwin_band(F.col("gedwin"), census))
        .withColumn(
            "nedocs",
            fx.nedocs(census, beds, F.col("boarders_n"), F.col("inpatient_beds"), F.col("vents_n"),
                      F.col("longest_boarder_h"), F.col("last_bedded_wait_h")),
        )
        .withColumn("nedocs_level", fx.nedocs_level(F.col("nedocs")))
        .withColumn("patients_per_attending", F.try_divide(census.cast("double"), attendings.cast("double")))
        .withColumn("boarder_share", F.try_divide(F.col("boarders_n").cast("double"), census.cast("double")))
        .withColumn(
            "high_acuity_share", F.try_divide((F.col("n_esi1") + F.col("n_esi2")).cast("double"), census.cast("double"))
        )
        .withColumn(
            "dq_flags",
            F.array_compact(
                F.array(
                    F.when(attendings.isNull() | (attendings <= 0), F.lit("no_attending_on_duty")),
                    F.when(F.col("occupancy_ratio") > 1, F.lit("census_exceeds_beds")),
                )
            ),
        )
        .withColumn("computed_at", F.current_timestamp())
    )
    return df


def compute_snapshots(visits: DataFrame, roster: DataFrame, facilities: DataFrame, as_of: DataFrame) -> DataFrame:
    requested = as_of.select("snapshot_ts").distinct()
    times = requested.unionByName(requested.select((T - HOUR).alias("snapshot_ts"))).distinct()
    base = _metrics_at(visits, roster, facilities, times)
    prior = base.select("facility_id", (T + HOUR).alias("snapshot_ts"), F.col("gedwin").alias("_gedwin_prior"))
    return (
        base.join(F.broadcast(requested), "snapshot_ts")
        .join(prior, KEYS, "left")
        .withColumn("gedwin_delta_1h", F.col("gedwin") - F.col("_gedwin_prior"))
        .select(*METRIC_COLUMNS)
    )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_snapshots.py -v`
Expected: 10 passed.

- [ ] **Step 6: Commit**

```bash
git add src/ed_ops/metrics/snapshots.py tests/factories.py tests/unit/test_snapshots.py
git commit -m "feat: point-in-time ED snapshot metrics with bucketed window joins" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: Baselines and crowding drivers

**Files:**
- Create: `src/ed_ops/metrics/drivers.py`
- Test: `tests/unit/test_drivers.py`

**Interfaces:**
- Consumes: snapshot rows with `facility_id, snapshot_ts` and the driver columns (Task 4).
- Produces:
  - `drivers.DRIVER_COLUMNS = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrivals_1h", "high_acuity_share")`
  - `drivers.TOP_DRIVERS_TYPE: str`
  - `compute_baselines(snapshots, tz) -> DataFrame` with `facility_id, local_hour` and `{d}_mean`, `{d}_std` for each driver
  - `add_drivers(snapshots, baselines, tz) -> DataFrame`: input plus `arrival_surge_ratio` and `top_drivers`
  - `with_null_drivers(df) -> DataFrame`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_drivers.py`:
```python
from datetime import datetime

import pytest

from ed_ops.metrics.drivers import add_drivers, compute_baselines, with_null_drivers

DDL = "facility_id STRING, snapshot_ts TIMESTAMP, occupancy_ratio DOUBLE, boarder_share DOUBLE, patients_per_attending DOUBLE, arrivals_1h INT, high_acuity_share DOUBLE"
TZ = "America/Chicago"


def test_top_drivers_ranked_by_absolute_z(spark):
    # Three history points at 18:00 UTC = 12:00 Chicago (CST) on different days.
    history = spark.createDataFrame(
        [
            ("FAC001", datetime(2026, 11, 2, 18), 0.4, 0.1, 4.0, 10, 0.1),
            ("FAC001", datetime(2026, 11, 3, 18), 0.5, 0.1, 5.0, 12, 0.2),
            ("FAC001", datetime(2026, 11, 4, 18), 0.6, 0.1, 6.0, 14, 0.3),
        ],
        DDL,
    )
    baselines = compute_baselines(history, TZ)
    current = spark.createDataFrame(
        [
            ("FAC001", datetime(2026, 11, 9, 18), 0.8, 0.5, 5.5, 16, 0.35),  # z: 3.0, null(std 0), 0.5, 2.0, 1.5
            ("FAC001", datetime(2026, 11, 9, 3), 0.8, 0.5, 5.5, 16, 0.35),  # no baseline at this local hour
        ],
        DDL,
    )
    out = {r["snapshot_ts"]: r for r in add_drivers(current, baselines, TZ).collect()}
    top = out[datetime(2026, 11, 9, 18)]["top_drivers"]
    assert [d["driver"] for d in top] == ["occupancy_ratio", "arrivals_1h", "high_acuity_share"]
    assert top[0]["z"] == pytest.approx(3.0)
    assert top[0]["baseline"] == pytest.approx(0.5) and top[0]["value"] == pytest.approx(0.8)
    assert out[datetime(2026, 11, 9, 18)]["arrival_surge_ratio"] == pytest.approx(16 / 12)
    assert out[datetime(2026, 11, 9, 3)]["top_drivers"] == []
    assert out[datetime(2026, 11, 9, 3)]["arrival_surge_ratio"] is None


def test_add_drivers_replaces_existing_driver_columns(spark):
    history = spark.createDataFrame([("FAC001", datetime(2026, 11, 2, 18), 0.4, 0.1, 4.0, 10, 0.1)], DDL)
    current = with_null_drivers(spark.createDataFrame([("FAC001", datetime(2026, 11, 9, 18), 0.8, 0.5, 5.5, 16, 0.35)], DDL))
    out = add_drivers(current, compute_baselines(history, TZ), TZ)
    assert out.columns.count("top_drivers") == 1 and out.columns.count("arrival_surge_ratio") == 1


def test_with_null_drivers_types(spark):
    df = with_null_drivers(spark.createDataFrame([("FAC001", datetime(2026, 11, 9, 18), 0.8, 0.5, 5.5, 16, 0.35)], DDL))
    assert dict(df.dtypes)["top_drivers"] == "array<struct<driver:string,value:double,baseline:double,z:double>>"
    assert dict(df.dtypes)["arrival_surge_ratio"] == "double"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_drivers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.metrics.drivers'`.

- [ ] **Step 3: Implement `drivers.py`**

`src/ed_ops/metrics/drivers.py`:
```python
"""Per-facility local-hour baselines and the top deviating crowding drivers (spec section 7.4)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DRIVER_COLUMNS = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrivals_1h", "high_acuity_share")
TOP_DRIVERS_TYPE = "ARRAY<STRUCT<driver: STRING, value: DOUBLE, baseline: DOUBLE, z: DOUBLE>>"


def _local_hour(tz: str):
    return F.hour(F.from_utc_timestamp(F.col("snapshot_ts"), tz))


def compute_baselines(snapshots: DataFrame, tz: str) -> DataFrame:
    aggs = []
    for d in DRIVER_COLUMNS:
        aggs.append(F.avg(F.col(d).cast("double")).alias(f"{d}_mean"))
        aggs.append(F.stddev_samp(F.col(d).cast("double")).alias(f"{d}_std"))
    return snapshots.withColumn("local_hour", _local_hour(tz)).groupBy("facility_id", "local_hour").agg(*aggs)


def add_drivers(snapshots: DataFrame, baselines: DataFrame, tz: str) -> DataFrame:
    df = snapshots.drop("top_drivers", "arrival_surge_ratio").withColumn("_local_hour", _local_hour(tz))
    df = df.join(baselines.withColumnRenamed("local_hour", "_local_hour"), ["facility_id", "_local_hour"], "left")
    candidates = F.array(
        *[
            F.struct(
                F.lit(d).alias("driver"),
                F.col(d).cast("double").alias("value"),
                F.col(f"{d}_mean").cast("double").alias("baseline"),
                F.try_divide(F.col(d).cast("double") - F.col(f"{d}_mean"), F.col(f"{d}_std")).alias("z"),
            )
            for d in DRIVER_COLUMNS
        ]
    )
    significant = F.filter(candidates, lambda s: F.abs(s["z"]) >= F.lit(1.0))
    ordered = F.array_sort(
        significant,
        lambda a, b: F.when(F.abs(a["z"]) > F.abs(b["z"]), F.lit(-1))
        .when(F.abs(a["z"]) < F.abs(b["z"]), F.lit(1))
        .otherwise(F.lit(0)),
    )
    df = df.withColumn("top_drivers", F.slice(ordered, 1, 3)).withColumn(
        "arrival_surge_ratio", F.try_divide(F.col("arrivals_1h").cast("double"), F.col("arrivals_1h_mean"))
    )
    baseline_cols = [f"{d}_{s}" for d in DRIVER_COLUMNS for s in ("mean", "std")]
    return df.drop("_local_hour", *baseline_cols)


def with_null_drivers(df: DataFrame) -> DataFrame:
    return df.withColumn("arrival_surge_ratio", F.lit(None).cast("double")).withColumn(
        "top_drivers", F.lit(None).cast(TOP_DRIVERS_TYPE)
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_drivers.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/metrics/drivers.py tests/unit/test_drivers.py
git commit -m "feat: local-hour baselines and top crowding drivers" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: De-identified ED board rows

**Files:**
- Create: `src/ed_ops/metrics/board.py`
- Test: `tests/unit/test_board.py`

**Interfaces:**
- Consumes: visits with `snapshots.VISIT_COLUMNS` (Task 4).
- Produces: `board.board_rows(visits: DataFrame, as_of: datetime) -> DataFrame` with exactly the `gold.ed_board_current` columns:
  - `facility_id, visit_id, in_ed, status, esi_level, suggested_esi, undertriage_flag, complaint_category`
  - `arrival_ts, departed_ts, minutes_in_ed, minutes_waiting, arrival_mode, on_vent, as_of_ts, computed_at`

  Rows are patients in the ED at `as_of`, plus those who departed in `(as_of − 30 min, as_of]`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_board.py`:
```python
from ed_ops.metrics.board import board_rows
from tests.factories import SCENARIO_VISITS, T, visits_df


def test_board_statuses_and_minutes(spark):
    rows = {r["visit_id"]: r.asDict() for r in board_rows(visits_df(spark, SCENARIO_VISITS), T).collect()}
    assert set(rows) == {"v1", "v2", "v3", "v4", "v9", "v5"}  # v6/v7 departed > 30 min ago, v8 future
    assert rows["v1"]["status"] == "boarding" and rows["v1"]["on_vent"] is True
    assert rows["v2"]["status"] == "in_bed" and rows["v2"]["minutes_waiting"] == 30
    assert rows["v3"]["status"] == "waiting" and rows["v3"]["minutes_waiting"] == 40
    assert rows["v5"]["status"] == "departed" and rows["v5"]["in_ed"] is False
    assert rows["v5"]["minutes_in_ed"] == 85 and rows["v5"]["departed_ts"] is not None
    assert rows["v1"]["minutes_in_ed"] == 120 and rows["v1"]["in_ed"] is True
    assert rows["v9"]["esi_level"] is None  # triaged after T
    assert rows["v1"]["as_of_ts"] == T


def test_board_columns_match_table(spark):
    from ed_ops.ddl.metadata import load_catalog_def

    expected = load_catalog_def().table("gold", "ed_board_current").column_names
    assert board_rows(visits_df(spark, SCENARIO_VISITS), T).columns == expected
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_board.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.metrics.board'`.

- [ ] **Step 3: Implement `board.py`**

`src/ed_ops/metrics/board.py`:
```python
"""De-identified live ED board for gold.ed_board_current (insert/update only; no deletes)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DEPARTED_LOOKBACK = F.expr("INTERVAL 30 MINUTES")


def board_rows(visits: DataFrame, as_of: datetime) -> DataFrame:
    t = F.lit(as_of).cast("timestamp")
    arrival, departure = F.col("arrival_ts"), F.col("departure_ts")
    in_ed = (arrival <= t) & (departure.isNull() | (departure > t))
    departed_recently = (departure > t - DEPARTED_LOOKBACK) & (departure <= t)
    greeted = F.coalesce(F.col("greet_ts") <= t, F.lit(False))
    bedded = F.coalesce(F.col("bed_assigned_ts") <= t, F.lit(False))
    boarder = F.coalesce((F.col("disposition") == "ADMIT") & (F.col("disposition_ts") <= t), F.lit(False))
    on_vent = F.coalesce(
        (F.col("vent_start_ts") <= t) & (F.col("vent_end_ts").isNull() | (F.col("vent_end_ts") > t)), F.lit(False)
    )
    df = visits.filter(arrival.isNotNull() & (in_ed | departed_recently)).withColumn("in_ed", in_ed)
    end = F.when(F.col("in_ed"), t).otherwise(departure)
    waited_until = F.when(greeted, F.col("greet_ts")).otherwise(end)
    minutes = lambda later, earlier: F.floor((F.unix_timestamp(later) - F.unix_timestamp(earlier)) / 60).cast("int")
    return df.select(
        "facility_id",
        "visit_id",
        "in_ed",
        F.when(~F.col("in_ed"), F.lit("departed"))
        .when(boarder, F.lit("boarding"))
        .when(greeted | bedded, F.lit("in_bed"))
        .otherwise(F.lit("waiting"))
        .alias("status"),
        F.when(F.col("triage_ts") <= t, F.col("esi_level")).alias("esi_level"),
        "suggested_esi",
        "undertriage_flag",
        "complaint_category",
        "arrival_ts",
        F.when(~F.col("in_ed"), departure).alias("departed_ts"),
        minutes(end, arrival).alias("minutes_in_ed"),
        minutes(waited_until, arrival).alias("minutes_waiting"),
        "arrival_mode",
        F.when(F.col("in_ed"), on_vent).otherwise(F.lit(False)).alias("on_vent"),
        t.alias("as_of_ts"),
        F.current_timestamp().alias("computed_at"),
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_board.py tests/unit/test_ddl.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/metrics/board.py tests/unit/test_board.py
git commit -m "feat: de-identified live ED board rows" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: Delta write helpers (conform, MERGE, append)

**Files:**
- Create: `src/ed_ops/delta.py`
- Test: `tests/unit/test_delta.py`

**Interfaces:**
- Consumes: `load_catalog_def`, `TableDef` (Task 2); `Config` (Task 1).
- Produces:
  - `delta.table_def(logical, name) -> TableDef` (cached)
  - `delta.conform(df, t: TableDef) -> DataFrame`: selects and casts columns in table order; raises `ValueError` listing missing columns
  - `delta.merge_sql(target, source_view, keys, columns) -> str`
  - `delta.merge_into(df, cfg, logical, name, keys=None) -> None` (keys default to the table PK)
  - `delta.append_into(df, cfg, logical, name) -> None`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_delta.py`:
```python
import pytest

from ed_ops.delta import conform, merge_sql, table_def


def test_merge_sql_updates_non_keys_and_inserts_all():
    sql = merge_sql("c.s.t", "src_view", ["facility_id"], ["facility_id", "census", "gedwin"])
    assert sql == (
        "MERGE INTO c.s.t AS t USING src_view AS s\n"
        "ON t.facility_id = s.facility_id\n"
        "WHEN MATCHED THEN UPDATE SET t.census = s.census, t.gedwin = s.gedwin\n"
        "WHEN NOT MATCHED THEN INSERT (facility_id, census, gedwin) VALUES (s.facility_id, s.census, s.gedwin)"
    )


def test_conform_orders_and_casts(spark):
    t = table_def("ref", "physician_roster")
    df = spark.createDataFrame(
        [(3, "FAC001", "day")], "attendings_on_duty BIGINT, facility_id STRING, shift_name STRING"
    ).selectExpr("*", "timestamp'2026-11-09 07:00:00' AS shift_start", "timestamp'2026-11-09 15:00:00' AS shift_end")
    out = conform(df, t)
    assert out.columns == t.column_names
    assert dict(out.dtypes)["attendings_on_duty"] == "int"


def test_conform_rejects_missing_columns(spark):
    df = spark.createDataFrame([("FAC001",)], "facility_id STRING")
    with pytest.raises(ValueError, match="missing columns"):
        conform(df, table_def("ref", "physician_roster"))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_delta.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.delta'`.

- [ ] **Step 3: Implement `delta.py`**

`src/ed_ops/delta.py`:
```python
"""Typed writes into metadata-defined Delta tables."""

from __future__ import annotations

import uuid
from functools import lru_cache

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ed_ops.config import Config
from ed_ops.ddl.metadata import TableDef, load_catalog_def


@lru_cache(maxsize=None)
def table_def(logical: str, name: str) -> TableDef:
    return load_catalog_def().table(logical, name)


def conform(df: DataFrame, t: TableDef) -> DataFrame:
    missing = [c for c in t.column_names if c not in df.columns]
    if missing:
        raise ValueError(f"{t.schema}.{t.name}: DataFrame is missing columns {missing}")
    return df.select(*[F.col(c.name).cast(c.type).alias(c.name) for c in t.columns])


def merge_sql(target: str, source_view: str, keys: list[str], columns: list[str]) -> str:
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    updates = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in keys)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s\n"
        f"ON {on}\n"
        f"WHEN MATCHED THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def merge_into(df: DataFrame, cfg: Config, logical: str, name: str, keys: list[str] | None = None) -> None:
    t = table_def(logical, name)
    keys = list(keys or t.primary_key)
    if not keys:
        raise ValueError(f"{logical}.{name} has no primary key; pass keys explicitly")
    view = f"src_{name}_{uuid.uuid4().hex[:8]}"
    conform(df, t).createOrReplaceTempView(view)
    spark = df.sparkSession
    try:
        spark.sql(merge_sql(cfg.table(logical, name), view, keys, t.column_names))
    finally:
        spark.catalog.dropTempView(view)


def append_into(df: DataFrame, cfg: Config, logical: str, name: str) -> None:
    conform(df, table_def(logical, name)).write.mode("append").saveAsTable(cfg.table(logical, name))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_delta.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/delta.py tests/unit/test_delta.py
git commit -m "feat: typed Delta MERGE and append helpers" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: Generator curves, generator state and scenario windows

**Files:**
- Create: `src/ed_ops/generator/__init__.py`, `src/ed_ops/generator/curves.py`, `src/ed_ops/generator/state.py`, `src/ed_ops/generator/scenarios.py`
- Test: `tests/unit/test_curves_scenarios.py`

**Interfaces:**
- Consumes: `Config` (Task 1).
- Produces:
  - `curves`:
    - `HOURLY: tuple[float, ...]` (24 values, mean 1) and `DOW: tuple[float, ...]` (7 values, Monday = 0, mean 1)
    - `esi_mix(facility_type, acuity_shift) -> np.ndarray` (5 probabilities)
    - `arrivals_per_minute(baseline_daily, local_hour, local_weekday) -> float`
    - the dicts `AMBULANCE_P`, `ADMIT_P`, `ADMIT_TYPE_FACTOR`, `VENT_P`, `D2T_MEDIAN`, `D2P_MEDIAN` and `TREAT_MEDIAN`
    - the floats `BED_WAIT_MEDIAN`, `PATIENCE_MEDIAN`, `DISCHARGE_TO_DEPART_MEDIAN` and `TRANSFER_TO_DEPART_MEDIAN`
    - `PROFILE_SPEED: dict[str, float]` and `boarding_median(profile) -> float`
  - `state.GeneratorState` (frozen dataclass: `generator_id, backfill_start_ts, last_planned_ts, last_emitted_ts, live_started_ts=None`)
  - `scenarios`:
    - `ScenarioWindow(name, start, end, facility_ids, multiplier, acuity_shift)` with `.active(facility_id, ts)`
    - `build_scenarios(facilities_pdf, state, cfg) -> list[ScenarioWindow]`
    - `arrival_multiplier(windows, facility_id, ts) -> float`
    - `acuity_shift_active(windows, facility_id, ts) -> bool`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_curves_scenarios.py`:
```python
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from ed_ops.config import Config
from ed_ops.generator import curves
from ed_ops.generator.scenarios import acuity_shift_active, arrival_multiplier, build_scenarios
from ed_ops.generator.state import GeneratorState


def test_curves_are_normalised():
    assert len(curves.HOURLY) == 24 and np.mean(curves.HOURLY) == pytest.approx(1.0)
    assert len(curves.DOW) == 7 and np.mean(curves.DOW) == pytest.approx(1.0)
    assert curves.HOURLY[13] > curves.HOURLY[4]  # afternoon busier than 4am


def test_week_of_arrivals_matches_baseline():
    per_week = sum(curves.arrivals_per_minute(240, h, d) * 60 for d in range(7) for h in range(24))
    assert per_week == pytest.approx(240 * 7)


@pytest.mark.parametrize("ftype", ["level_1_trauma", "community", "rural_critical_access", "pediatric"])
def test_esi_mix_is_a_distribution(ftype):
    for shift in (False, True):
        p = curves.esi_mix(ftype, shift)
        assert p.shape == (5,) and p.sum() == pytest.approx(1.0) and (p > 0).all()


def test_flu_shift_moves_mass_to_esi_2_and_3():
    base, flu = curves.esi_mix("community", False), curves.esi_mix("community", True)
    assert flu[1] + flu[2] > base[1] + base[2]


def _facilities():
    return pd.DataFrame(
        {
            "facility_id": ["FAC001", "FAC002", "FAC003", "FAC004"],
            "region": ["North", "North", "South", "South"],
            "scenario_profile": ["normal", "surge_prone", "surge_prone", "normal"],
        }
    )


def test_flu_window_covers_surge_prone_and_their_region():
    start = datetime(2026, 10, 26)
    state = GeneratorState("main", start, start, start)
    windows = build_scenarios(_facilities(), state, Config())
    flu = [w for w in windows if w.name == "flu_surge"][0]
    assert flu.facility_ids == frozenset({"FAC001", "FAC002", "FAC003"})  # region of FAC002 = North
    assert flu.start == start + timedelta(days=8) and flu.end == start + timedelta(days=11)
    assert all(w.name != "live_surge" for w in windows)
    assert arrival_multiplier(windows, "FAC001", start + timedelta(days=9)) == pytest.approx(1.4)
    assert arrival_multiplier(windows, "FAC004", start + timedelta(days=9)) == pytest.approx(1.0)
    assert acuity_shift_active(windows, "FAC002", start + timedelta(days=9))
    assert not acuity_shift_active(windows, "FAC002", start + timedelta(days=12))


def test_live_surge_starts_after_offset_for_two_surge_prone_facilities():
    start = datetime(2026, 10, 26)
    live = datetime(2026, 11, 9, 12)
    windows = build_scenarios(_facilities(), GeneratorState("main", start, live, live, live), Config(live_surge_offset_min=30, live_surge_duration_min=90))
    surge = [w for w in windows if w.name == "live_surge"][0]
    assert surge.facility_ids == frozenset({"FAC002", "FAC003"})
    assert arrival_multiplier(windows, "FAC002", live + timedelta(minutes=29)) == 1.0
    assert arrival_multiplier(windows, "FAC002", live + timedelta(minutes=30)) == pytest.approx(2.0)
    assert arrival_multiplier(windows, "FAC002", live + timedelta(minutes=120)) == 1.0


def test_no_surge_prone_means_no_scenarios():
    f = _facilities().assign(scenario_profile="normal")
    s = datetime(2026, 10, 26)
    assert build_scenarios(f, GeneratorState("main", s, s, s), Config()) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_curves_scenarios.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.generator'`.

- [ ] **Step 3: Implement curves, state and scenarios**

`src/ed_ops/generator/__init__.py`: empty file.

`src/ed_ops/generator/curves.py`:
```python
"""Arrival curves, acuity mixes and journey duration medians (minutes) for the simulator."""

from __future__ import annotations

import numpy as np

_HOURLY_RAW = (0.55, 0.45, 0.38, 0.33, 0.30, 0.32, 0.45, 0.70, 0.95, 1.15, 1.30, 1.38,
               1.40, 1.38, 1.35, 1.33, 1.32, 1.30, 1.25, 1.15, 1.02, 0.90, 0.78, 0.65)
HOURLY = tuple(h / (sum(_HOURLY_RAW) / 24) for h in _HOURLY_RAW)

_DOW_RAW = (1.12, 1.03, 1.00, 0.98, 1.00, 0.95, 0.92)  # Monday = 0
DOW = tuple(d / (sum(_DOW_RAW) / 7) for d in _DOW_RAW)

ESI_MIX = {
    "level_1_trauma": (0.03, 0.18, 0.47, 0.25, 0.07),
    "community": (0.01, 0.12, 0.45, 0.32, 0.10),
    "rural_critical_access": (0.005, 0.08, 0.38, 0.40, 0.135),
    "pediatric": (0.005, 0.07, 0.35, 0.43, 0.145),
}
FLU_SHIFT = np.array((0.0, 0.04, 0.06, -0.07, -0.03))

AMBULANCE_P = {1: 0.85, 2: 0.45, 3: 0.20, 4: 0.06, 5: 0.03}
ADMIT_P = {1: 0.80, 2: 0.55, 3: 0.30, 4: 0.06, 5: 0.01}
ADMIT_TYPE_FACTOR = {"level_1_trauma": 1.1, "community": 1.0, "rural_critical_access": 0.8, "pediatric": 0.6}
VENT_P = {1: 0.35, 2: 0.04}

D2T_MEDIAN = {1: 1.0, 2: 4.0, 3: 8.0, 4: 10.0, 5: 10.0}
D2P_MEDIAN = {1: 2.0, 2: 8.0, 3: 22.0, 4: 32.0, 5: 38.0}
TREAT_MEDIAN = {1: 120.0, 2: 180.0, 3: 200.0, 4: 110.0, 5: 70.0}
BED_WAIT_MEDIAN = 10.0
PATIENCE_MEDIAN = 160.0
DISCHARGE_TO_DEPART_MEDIAN = 20.0
TRANSFER_TO_DEPART_MEDIAN = 150.0
PROFILE_SPEED = {
    "normal": 1.0,
    "chronic_boarding": 1.0,
    "understaffed": 1.25,
    "surge_prone": 1.0,
    "high_performer": 0.6,
}


def boarding_median(profile: str) -> float:
    return 540.0 if profile == "chronic_boarding" else 150.0


def esi_mix(facility_type: str, acuity_shift: bool) -> np.ndarray:
    p = np.array(ESI_MIX[facility_type], dtype=float)
    if acuity_shift:
        p = p + FLU_SHIFT
    p = np.clip(p, 0.001, None)
    return p / p.sum()


def arrivals_per_minute(baseline_daily: float, local_hour: int, local_weekday: int) -> float:
    return baseline_daily / 1440.0 * HOURLY[local_hour] * DOW[local_weekday]
```

`src/ed_ops/generator/state.py`:
```python
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
```

`src/ed_ops/generator/scenarios.py`:
```python
"""Scenario windows that raise arrivals (and acuity) for chosen facilities (spec section 6)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from ed_ops.config import Config
from ed_ops.generator.state import GeneratorState

FLU_START_DAY, FLU_END_DAY, FLU_MULTIPLIER = 8, 11, 1.4  # backfill days 9-11
LIVE_SURGE_MULTIPLIER = 2.0


@dataclass(frozen=True)
class ScenarioWindow:
    name: str
    start: datetime
    end: datetime
    facility_ids: frozenset[str]
    multiplier: float
    acuity_shift: bool

    def active(self, facility_id: str, ts: datetime) -> bool:
        return facility_id in self.facility_ids and self.start <= ts < self.end


def build_scenarios(facilities: pd.DataFrame, state: GeneratorState, cfg: Config) -> list[ScenarioWindow]:
    surge_prone = sorted(facilities.loc[facilities["scenario_profile"] == "surge_prone", "facility_id"])
    if not surge_prone:
        return []
    flu_region = facilities.loc[facilities["facility_id"] == surge_prone[0], "region"].iloc[0]
    flu_ids = frozenset(surge_prone) | frozenset(facilities.loc[facilities["region"] == flu_region, "facility_id"])
    windows = [
        ScenarioWindow(
            "flu_surge",
            state.backfill_start_ts + timedelta(days=FLU_START_DAY),
            state.backfill_start_ts + timedelta(days=FLU_END_DAY),
            flu_ids,
            FLU_MULTIPLIER,
            True,
        )
    ]
    if state.live_started_ts is not None:
        start = state.live_started_ts + timedelta(minutes=cfg.live_surge_offset_min)
        windows.append(
            ScenarioWindow(
                "live_surge",
                start,
                start + timedelta(minutes=cfg.live_surge_duration_min),
                frozenset(surge_prone[:2]),
                LIVE_SURGE_MULTIPLIER,
                False,
            )
        )
    return windows


def arrival_multiplier(windows: list[ScenarioWindow], facility_id: str, ts: datetime) -> float:
    result = 1.0
    for w in windows:
        if w.active(facility_id, ts):
            result *= w.multiplier
    return result


def acuity_shift_active(windows: list[ScenarioWindow], facility_id: str, ts: datetime) -> bool:
    return any(w.acuity_shift and w.active(facility_id, ts) for w in windows)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_curves_scenarios.py -v`
Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/generator/__init__.py src/ed_ops/generator/curves.py src/ed_ops/generator/state.py src/ed_ops/generator/scenarios.py tests/unit/test_curves_scenarios.py
git commit -m "feat: simulator arrival curves, acuity mixes and scenario windows" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Triage-note templates with embedded PHI

**Files:**
- Create: `src/ed_ops/generator/notes_templates.py`
- Test: `tests/unit/test_notes_templates.py`

**Interfaces:**
- Produces:
  - `ARCHETYPES: dict[str, dict]`. The keys are archetype names; each value has the keys `category`, `weight`, `complaints`, `high`, `mid` and `low` (each of the last three is a template string).
  - `acuity_bucket(esi) -> str`
  - `pick_archetype(rng, sex) -> str`
  - `render_note(rng, archetype, true_esi, patient: dict, arrival: datetime) -> tuple[str, str]` returning `(triage_note, chief_complaint_text)`.

  `patient` has the keys `first_name, last_name, dob (date), sex, phone, street, zip, mrn, city, state`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_notes_templates.py`:
```python
import re
from datetime import date, datetime

import numpy as np
import pytest

from ed_ops.generator.notes_templates import ARCHETYPES, acuity_bucket, pick_archetype, render_note

PATIENT = {
    "first_name": "Ava", "last_name": "Carter", "dob": date(1958, 3, 14), "sex": "F", "phone": "312-555-0147",
    "street": "42 Elm St", "zip": "62704", "mrn": "MRN-7000042", "city": "Springfield", "state": "IL",
}
PHI_SLOTS = ("{dob}", "{mrn}", "{phone}", "{street}")


def test_archetype_catalogue_complete():
    assert len(ARCHETYPES) == 12
    assert sum(a["weight"] for a in ARCHETYPES.values()) == pytest.approx(1.0)
    for name, a in ARCHETYPES.items():
        assert a["category"] in {"cardiac", "respiratory", "neurological", "trauma", "gastrointestinal",
                                 "infectious", "psychiatric", "obstetric", "musculoskeletal", "other"}
        assert a["complaints"]
        for bucket in ("high", "mid", "low"):
            template = a[bucket]
            assert "{name}" in template, (name, bucket)
            assert any(slot in template for slot in PHI_SLOTS), (name, bucket)


def test_acuity_bucket():
    assert [acuity_bucket(e) for e in (1, 2, 3, 4, 5)] == ["high", "high", "mid", "low", "low"]


def test_render_high_acuity_note_contains_phi_and_abnormal_vitals():
    rng = np.random.default_rng(1)
    note, complaint = render_note(rng, "chest_pain", 1, PATIENT, datetime(2026, 11, 9, 12))
    assert "Ava Carter" in note
    assert "{" not in note and "}" not in note
    hr = int(re.search(r"HR (\d+)", note).group(1))
    spo2 = int(re.search(r"SpO2 (\d+)%", note).group(1))
    assert hr >= 110 and spo2 <= 90
    assert complaint


def test_render_low_acuity_note_has_normal_vitals():
    rng = np.random.default_rng(2)
    note, _ = render_note(rng, "back_pain", 5, PATIENT, datetime(2026, 11, 9, 12))
    assert int(re.search(r"SpO2 (\d+)%", note).group(1)) >= 97


def test_male_patients_never_get_pregnancy_archetype():
    rng = np.random.default_rng(3)
    assert "pregnancy_concern" not in {pick_archetype(rng, "M") for _ in range(3000)}
    assert "pregnancy_concern" in {pick_archetype(rng, "F") for _ in range(3000)}


def test_render_is_deterministic():
    a = render_note(np.random.default_rng(9), "fever_infection", 3, PATIENT, datetime(2026, 11, 9, 12))
    b = render_note(np.random.default_rng(9), "fever_infection", 3, PATIENT, datetime(2026, 11, 9, 12))
    assert a == b
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_notes_templates.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.generator.notes_templates'`.

- [ ] **Step 3: Implement the templates**

`src/ed_ops/generator/notes_templates.py`:
```python
"""Triage-note templates. Each archetype has one template per acuity bucket; every template embeds PHI
(name plus at least one of DOB, MRN, phone, street) so ai_mask has something real to find (Plan 2)."""

from __future__ import annotations

from datetime import datetime

import numpy as np

V = "HR {hr}, BP {sbp}/{dbp}, RR {rr}, SpO2 {spo2}%, T {temp}C"

ARCHETYPES: dict[str, dict] = {
    "chest_pain": {
        "category": "cardiac", "weight": 0.10,
        "complaints": ["chest pain", "chest tightness", "CP radiating to arm"],
        "high": "{name} ({age}{sex_short}), DOB {dob}, {mrn}. BIBA c/o crushing substernal chest pain radiating to L arm x {onset}, diaphoretic, nauseated. " + V + ". Pain {pain}/10. Callback {phone}.",
        "mid": "Pt {name}, {age}yo {sex_word}, {mrn}, reports intermittent chest tightness x {onset}, worse with exertion. " + V + ", pain {pain}/10. Hx HTN. Contact {phone}.",
        "low": "{name} DOB {dob} c/o sharp L chest wall pain after lifting boxes x {onset}, reproducible on palpation. " + V + ", pain {pain}/10. {mrn}.",
    },
    "shortness_of_breath": {
        "category": "respiratory", "weight": 0.10,
        "complaints": ["shortness of breath", "SOB", "difficulty breathing"],
        "high": "BIBA {name}, {age}{sex_short}, {mrn}: severe SOB x {onset}, 2-3 word sentences, accessory muscle use, cyanotic lips. " + V + " on NRB. Lives at {street}, {city}.",
        "mid": "{name} ({mrn}) c/o SOB and productive cough x {onset}, mild wheeze. " + V + ", pain {pain}/10. DOB {dob}.",
        "low": "Pt {name}, DOB {dob}, mild SOB with seasonal allergies x {onset}, full sentences, no distress. " + V + ". Phone {phone}.",
    },
    "stroke_symptoms": {
        "category": "neurological", "weight": 0.04,
        "complaints": ["weakness one side", "slurred speech", "facial droop"],
        "high": "STROKE ALERT. {name}, {age}{sex_short}, {mrn}, sudden R-sided weakness and slurred speech, LKW {onset} ago, facial droop. " + V + ". Daughter callback {phone}.",
        "mid": "{name} DOB {dob} reports transient L hand numbness x {onset}, now resolved, no weakness. " + V + ". {mrn}.",
        "low": "Pt {name} ({mrn}) c/o dizziness when standing x {onset}, no focal deficits, ambulatory. " + V + ", pain {pain}/10.",
    },
    "headache": {
        "category": "neurological", "weight": 0.07,
        "complaints": ["headache", "migraine", "head pain"],
        "high": "{name}, {age}{sex_short}, {mrn}: worst headache of life, thunderclap onset {onset} ago, vomiting, neck stiffness. " + V + ", pain {pain}/10. DOB {dob}.",
        "mid": "Pt {name} c/o migraine x {onset} unrelieved by home meds, photophobia. " + V + ", pain {pain}/10. {mrn}, ph {phone}.",
        "low": "{name} DOB {dob} mild tension-type headache x {onset}, alert, ambulatory. " + V + ", pain {pain}/10.",
    },
    "abdominal_pain": {
        "category": "gastrointestinal", "weight": 0.14,
        "complaints": ["abdominal pain", "stomach pain", "abd pain with vomiting"],
        "high": "{name}, {age}{sex_short}, {mrn}, severe diffuse abd pain x {onset}, rigid abdomen, pale and clammy. " + V + ", pain {pain}/10. Lives {street}, {city}.",
        "mid": "Pt {name} (DOB {dob}) RLQ pain x {onset} with nausea, anorexia. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} c/o mild epigastric discomfort after meals x {onset}, tolerating PO. " + V + ", pain {pain}/10. Callback {phone}.",
    },
    "fever_infection": {
        "category": "infectious", "weight": 0.10,
        "complaints": ["fever", "fever and chills", "possible infection"],
        "high": "{name}, {age}{sex_short}, {mrn}: fever, rigors and confusion x {onset}, mottled skin, suspected sepsis. " + V + ". Family ph {phone}.",
        "mid": "Pt {name} DOB {dob} fever and dysuria x {onset}, flank pain. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} ({mrn}) sore throat and low-grade fever x {onset}, eating and drinking. " + V + ", pain {pain}/10.",
    },
    "fall_injury": {
        "category": "trauma", "weight": 0.10,
        "complaints": ["fall", "injury after fall", "hip pain after fall"],
        "high": "BIBA {name}, {age}{sex_short}, {mrn}, fall from ladder ~12 ft {onset} ago, GCS 13, obvious L femur deformity. " + V + ", pain {pain}/10. DOB {dob}.",
        "mid": "{name} DOB {dob} mechanical fall at home {onset} ago, R wrist swelling, no LOC. " + V + ", pain {pain}/10. {mrn}, lives {street}.",
        "low": "Pt {name} rolled R ankle {onset} ago, ambulatory with limp. " + V + ", pain {pain}/10. Ph {phone}.",
    },
    "laceration": {
        "category": "trauma", "weight": 0.07,
        "complaints": ["laceration", "cut", "bleeding wound"],
        "high": "{name}, {age}{sex_short}, {mrn}: deep L forearm laceration with pulsatile bleeding x {onset}, tourniquet applied by EMS. " + V + ", pain {pain}/10.",
        "mid": "{name} DOB {dob} 4 cm scalp laceration after fall {onset} ago, bleeding controlled. " + V + ", pain {pain}/10. {mrn}.",
        "low": "Pt {name} ({mrn}) small finger laceration from kitchen knife {onset} ago, bleeding controlled. " + V + ", pain {pain}/10. Callback {phone}.",
    },
    "back_pain": {
        "category": "musculoskeletal", "weight": 0.09,
        "complaints": ["back pain", "low back pain", "back injury"],
        "high": "{name}, {age}{sex_short}, {mrn}: sudden tearing back pain radiating to abdomen x {onset}, diaphoretic. " + V + ", pain {pain}/10. DOB {dob}.",
        "mid": "Pt {name} DOB {dob} low back pain with new leg numbness x {onset}. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} ({mrn}) chronic low back pain flare x {onset}, no red flags, ambulatory. " + V + ", pain {pain}/10. Ph {phone}.",
    },
    "behavioral_crisis": {
        "category": "psychiatric", "weight": 0.06,
        "complaints": ["anxiety", "depression", "psychiatric evaluation"],
        "high": "{name}, {age}{sex_short}, {mrn}, brought by police, active suicidal ideation with plan, agitated, escalating x {onset}. " + V + ". Emergency contact {phone}.",
        "mid": "Pt {name} DOB {dob} anxiety and panic attacks x {onset}, passive SI without plan. " + V + ". {mrn}.",
        "low": "{name} ({mrn}) requesting medication refill, insomnia x {onset}, calm and cooperative. " + V + ".",
    },
    "pregnancy_concern": {
        "category": "obstetric", "weight": 0.03,
        "complaints": ["pregnancy bleeding", "pregnant with abdominal pain", "pregnancy concern"],
        "high": "{name}, {age}F, {mrn}, 34 wks pregnant, heavy vaginal bleeding and severe abd pain x {onset}. " + V + ", pain {pain}/10. Partner {phone}.",
        "mid": "Pt {name} DOB {dob}, 10 wks pregnant, light spotting and cramping x {onset}. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} ({mrn}), 20 wks pregnant, mild nausea x {onset}, tolerating fluids. " + V + ", pain {pain}/10.",
    },
    "allergic_reaction": {
        "category": "other", "weight": 0.10,
        "complaints": ["allergic reaction", "rash", "hives"],
        "high": "{name}, {age}{sex_short}, {mrn}: throat swelling and stridor after peanut exposure {onset} ago, diffuse hives, EpiPen x1. " + V + ". DOB {dob}.",
        "mid": "Pt {name} DOB {dob} facial swelling and hives after new medication x {onset}, no stridor. " + V + ". {mrn}.",
        "low": "{name} ({mrn}) localized rash on arm x {onset}, itchy, no swelling. " + V + ", pain {pain}/10. Ph {phone}.",
    },
}

VITALS = {
    "high": {"hr": (110, 140), "sbp": (78, 100), "dbp": (45, 62), "rr": (24, 32), "spo2": (82, 90), "temp": (36.0, 39.8), "pain": (8, 10)},
    "mid": {"hr": (90, 110), "sbp": (105, 150), "dbp": (65, 95), "rr": (18, 22), "spo2": (93, 96), "temp": (36.5, 38.6), "pain": (5, 7)},
    "low": {"hr": (64, 92), "sbp": (112, 138), "dbp": (68, 88), "rr": (12, 18), "spo2": (97, 100), "temp": (36.4, 37.4), "pain": (1, 4)},
}
COMPLAINT_PHI_P = 0.3
_NAMES = tuple(ARCHETYPES)


def acuity_bucket(esi: int) -> str:
    return "high" if esi <= 2 else "mid" if esi == 3 else "low"


def pick_archetype(rng: np.random.Generator, sex: str) -> str:
    names = [n for n in _NAMES if not (sex == "M" and ARCHETYPES[n]["category"] == "obstetric")]
    weights = np.array([ARCHETYPES[n]["weight"] for n in names])
    return names[int(rng.choice(len(names), p=weights / weights.sum()))]


def _onset(rng: np.random.Generator, bucket: str) -> str:
    if bucket == "high":
        return f"{int(rng.integers(10, 90))} min"
    if bucket == "mid":
        return f"{int(rng.integers(2, 24))} hours"
    return f"{int(rng.integers(1, 7))} days"


def render_note(rng: np.random.Generator, archetype: str, true_esi: int, patient: dict, arrival: datetime) -> tuple[str, str]:
    bucket = acuity_bucket(true_esi)
    v = VITALS[bucket]
    dob = patient["dob"]
    age = arrival.year - dob.year - ((arrival.month, arrival.day) < (dob.month, dob.day))
    slots = {
        "name": f"{patient['first_name']} {patient['last_name']}",
        "age": age,
        "sex_short": patient["sex"],
        "sex_word": "female" if patient["sex"] == "F" else "male",
        "dob": dob.strftime("%m/%d/%Y"),
        "mrn": patient["mrn"],
        "phone": patient["phone"],
        "street": patient["street"],
        "city": patient["city"],
        "hr": int(rng.integers(v["hr"][0], v["hr"][1] + 1)),
        "sbp": int(rng.integers(v["sbp"][0], v["sbp"][1] + 1)),
        "dbp": int(rng.integers(v["dbp"][0], v["dbp"][1] + 1)),
        "rr": int(rng.integers(v["rr"][0], v["rr"][1] + 1)),
        "spo2": int(rng.integers(v["spo2"][0], v["spo2"][1] + 1)),
        "temp": f"{rng.uniform(*v['temp']):.1f}",
        "pain": int(rng.integers(v["pain"][0], v["pain"][1] + 1)),
        "onset": _onset(rng, bucket),
    }
    note = ARCHETYPES[archetype][bucket].format(**slots)
    complaints = ARCHETYPES[archetype]["complaints"]
    complaint = complaints[int(rng.integers(0, len(complaints)))]
    if rng.random() < COMPLAINT_PHI_P:
        complaint = f"{complaint} - pt {slots['name']}, cb {patient['phone']}"
    return note, complaint
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_notes_templates.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/generator/notes_templates.py tests/unit/test_notes_templates.py
git commit -m "feat: triage-note templates with embedded PHI and acuity-matched vitals" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 10: Reference data (facilities, roster, patient population)

**Files:**
- Create: `src/ed_ops/generator/facilities.py`, `src/ed_ops/generator/roster.py`, `src/ed_ops/generator/population.py`, `src/ed_ops/generator/reference.py`
- Test: `tests/unit/test_reference.py`

**Interfaces:**
- Consumes: `Config` (Task 1).
- Produces:
  - `facilities.FACILITY_COLUMNS`, `facilities.allocate(shares, n) -> list[str]`
  - `facilities.build_facilities(spark, cfg) -> pd.DataFrame`, with columns equal to `ref.facility`
  - `roster.ROSTER_COLUMNS`, `roster.attendings_for(baseline_daily_visits, profile, shift_name, factor) -> int`
  - `roster.build_roster(facilities_pdf, start: date, days: int, tz_name) -> pd.DataFrame`, with columns equal to `ref.physician_roster`
  - `population.build_patient_pool(spark, size, seed) -> DataFrame`, with columns equal to `sim.patient_pool`
  - `reference.ReferenceData(facilities, roster, pool)` with `.pool_size`, `.patient(pool_id) -> dict` (pool fields only), `.facility(facility_id)` (a namedtuple row) and `.attendings_at(facility_id, ts) -> int` (0 if no shift)
  - `reference.load_reference(spark, cfg) -> ReferenceData`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_reference.py`:
```python
from collections import Counter
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from ed_ops.config import Config
from ed_ops.generator.facilities import FACILITY_COLUMNS, allocate, build_facilities
from ed_ops.generator.population import build_patient_pool
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.roster import attendings_for, build_roster


def test_allocate_proportional_with_minimum_one():
    shares = {"normal": 0.6, "chronic_boarding": 0.1, "understaffed": 0.1, "surge_prone": 0.1, "high_performer": 0.1}
    assert Counter(allocate(shares, 50)) == {"normal": 30, "chronic_boarding": 5, "understaffed": 5, "surge_prone": 5, "high_performer": 5}
    assert Counter(allocate(shares, 5)) == {k: 1 for k in shares}
    assert len(allocate(shares, 3)) == 3


@pytest.fixture(scope="module")
def facilities(spark):
    return build_facilities(spark, Config())


def test_facilities_shape_and_mix(facilities):
    assert list(facilities.columns) == list(FACILITY_COLUMNS)
    assert list(facilities.facility_id) == [f"FAC{i:03d}" for i in range(1, 51)]
    assert facilities.facility_name.is_unique
    assert Counter(facilities.scenario_profile) == {"normal": 30, "chronic_boarding": 5, "understaffed": 5, "surge_prone": 5, "high_performer": 5}
    assert Counter(facilities.facility_type) == {"community": 28, "rural_critical_access": 12, "level_1_trauma": 6, "pediatric": 4}


def test_facility_sizes_follow_type(facilities):
    trauma = facilities[facilities.facility_type == "level_1_trauma"]
    rural = facilities[facilities.facility_type == "rural_critical_access"]
    assert trauma.ed_beds.between(50, 80).all() and trauma.inpatient_beds.between(300, 600).all()
    assert rural.ed_beds.between(8, 15).all() and rural.inpatient_beds.between(25, 60).all()
    assert (facilities.baseline_daily_visits > 0).all()


def test_facilities_deterministic(spark, facilities):
    pd.testing.assert_frame_equal(facilities, build_facilities(spark, Config()))


def test_small_network_has_every_profile(spark):
    small = build_facilities(spark, Config(facility_count=5))
    assert set(small.scenario_profile) == {"normal", "chronic_boarding", "understaffed", "surge_prone", "high_performer"}


def test_roster_covers_every_hour_once_across_dst(facilities):
    one = facilities.head(1)
    roster = build_roster(one, date(2026, 10, 30), 5, "America/Chicago").sort_values("shift_start")
    starts, ends = list(roster.shift_start), list(roster.shift_end)
    assert all(ends[i] == starts[i + 1] for i in range(len(starts) - 1))  # contiguous, no gaps/overlaps
    night = roster[(roster.shift_name == "night") & (roster.shift_start == datetime(2026, 11, 1, 4))]
    assert len(night) == 1 and night.iloc[0].shift_end == datetime(2026, 11, 1, 13)  # 23:00 CDT -> 07:00 CST = 9 h
    assert len(roster) == 15


def test_attendings_rules():
    assert attendings_for(24, "normal", "night", 0.6) == 1  # minimum one
    assert attendings_for(200, "understaffed", "night", 0.6) == attendings_for(200, "normal", "night", 0.6) - 1
    assert attendings_for(200, "high_performer", "day", 1.0) == attendings_for(200, "normal", "day", 1.0) + 1


def test_patient_pool(spark):
    pool = build_patient_pool(spark, 200, 42).toPandas()
    assert list(pool.columns) == ["pool_id", "first_name", "last_name", "dob", "sex", "phone", "street", "zip", "mrn"]
    assert sorted(pool.pool_id) == list(range(200))
    assert pool.mrn.str.fullmatch(r"MRN-\d{7}").all() and pool.mrn.is_unique
    assert set(pool.sex) <= {"F", "M"}
    assert pool.phone.str.fullmatch(r"\d{3}-555-\d{4}").all()


def test_reference_data_lookups(spark, facilities):
    pool = build_patient_pool(spark, 10, 42).toPandas()
    roster = build_roster(facilities.head(2), date(2026, 11, 9), 1, "America/Chicago")
    ref = ReferenceData(facilities.head(2), roster, pool)
    assert ref.pool_size == 10
    assert ref.patient(3)["mrn"] == "MRN-7000003"
    assert ref.facility("FAC001").facility_id == "FAC001"
    noon_local = datetime(2026, 11, 9, 18)  # 12:00 CST -> day shift
    assert ref.attendings_at("FAC001", noon_local) == int(
        roster[(roster.facility_id == "FAC001") & (roster.shift_name == "day")].attendings_on_duty.iloc[0]
    )
    assert ref.attendings_at("FAC001", noon_local + timedelta(days=5)) == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_reference.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.generator.facilities'`.

- [ ] **Step 3: Implement facilities, roster, population and reference**

`src/ed_ops/generator/facilities.py`:
```python
"""The simulated ED network: dbldatagen draws the random attributes, fixed rules make sizes coherent."""

from __future__ import annotations

import math
from collections import Counter

import dbldatagen as dg
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from ed_ops.config import Config

FACILITY_COLUMNS = (
    "facility_id", "facility_name", "region", "city", "state", "latitude", "longitude", "facility_type",
    "ed_beds", "inpatient_beds", "baseline_daily_visits", "scenario_profile",
)

CITIES = (
    ("Springfield", "IL", 39.80, -89.64, "Central"), ("Peoria", "IL", 40.69, -89.59, "Central"),
    ("Champaign", "IL", 40.12, -88.24, "Central"), ("Bloomington", "IL", 40.48, -88.99, "Central"),
    ("Rockford", "IL", 42.27, -89.09, "North"), ("Madison", "WI", 43.07, -89.40, "North"),
    ("Milwaukee", "WI", 43.04, -87.91, "North"), ("Green Bay", "WI", 44.51, -88.01, "North"),
    ("Naperville", "IL", 41.75, -88.15, "Metro"), ("Joliet", "IL", 41.53, -88.08, "Metro"),
    ("Evanston", "IL", 42.05, -87.69, "Metro"), ("Aurora", "IL", 41.76, -88.32, "Metro"),
    ("Kankakee", "IL", 41.12, -87.86, "Metro"), ("St. Louis", "MO", 38.63, -90.20, "South"),
    ("Columbia", "MO", 38.95, -92.33, "South"), ("Carbondale", "IL", 37.73, -89.22, "South"),
    ("Davenport", "IA", 41.52, -90.58, "West"), ("Cedar Rapids", "IA", 41.98, -91.67, "West"),
    ("Des Moines", "IA", 41.59, -93.62, "West"), ("Quincy", "IL", 39.94, -91.41, "West"),
)
TYPE_SUFFIXES = {
    "level_1_trauma": ("Regional Medical Center", "University Hospital"),
    "community": ("Memorial Hospital", "Community Hospital", "General Hospital"),
    "rural_critical_access": ("Area Hospital", "Critical Access Hospital"),
    "pediatric": ("Children's Hospital",),
}
TYPE_SHARES = {"community": 0.56, "rural_critical_access": 0.24, "level_1_trauma": 0.12, "pediatric": 0.08}
PROFILE_SHARES = {"normal": 0.6, "chronic_boarding": 0.1, "understaffed": 0.1, "surge_prone": 0.1, "high_performer": 0.1}
SIZE = {
    "level_1_trauma": ((50, 80), (300, 600)),
    "community": ((18, 45), (100, 300)),
    "rural_critical_access": ((8, 15), (25, 60)),
    "pediatric": ((20, 40), (80, 200)),
}
VISITS_PER_BED = {"level_1_trauma": 2.4, "community": 2.4, "rural_critical_access": 1.8, "pediatric": 2.2}


def allocate(shares: dict[str, float], n: int) -> list[str]:
    """Deterministic largest-remainder allocation; every label gets >= 1 when n >= len(shares)."""
    keys = list(shares)
    minimum = 1 if n >= len(keys) else 0
    counts = {k: max(minimum, math.floor(shares[k] * n)) for k in keys}
    while sum(counts.values()) > n:
        largest = max(keys, key=lambda k: (counts[k], -keys.index(k)))
        counts[largest] -= 1
    while sum(counts.values()) < n:
        best = max(keys, key=lambda k: (shares[k] * n - counts[k], -keys.index(k)))
        counts[best] += 1
    return [k for k in keys for _ in range(counts[k])]


def build_facilities(spark: SparkSession, cfg: Config) -> pd.DataFrame:
    n = cfg.facility_count
    spec = (
        dg.DataGenerator(spark, name="facilities", rows=n, partitions=1, randomSeedMethod="hash_fieldname", randomSeed=cfg.seed)
        .withIdOutput()
        .withColumn("city_idx", "int", minValue=0, maxValue=len(CITIES) - 1, random=True)
        .withColumn("suffix_idx", "int", minValue=0, maxValue=11, random=True)
        .withColumn("lat_jitter_milli", "int", minValue=-60, maxValue=60, random=True)
        .withColumn("lon_jitter_milli", "int", minValue=-60, maxValue=60, random=True)
        .withColumn("size_pct", "int", minValue=0, maxValue=100, random=True)
        .withColumn("inpatient_pct", "int", minValue=0, maxValue=100, random=True)
        .withColumn("visit_jitter_pct", "int", minValue=-10, maxValue=10, random=True)
    )
    raw = spec.build().orderBy("id").toPandas()
    rng = np.random.default_rng(cfg.seed)
    types = [str(t) for t in rng.permutation(allocate(TYPE_SHARES, n))]
    profiles = [str(p) for p in rng.permutation(allocate(PROFILE_SHARES, n))]
    seen: Counter[str] = Counter()
    rows = []
    for i, r in enumerate(raw.itertuples(index=False)):
        ftype = types[i]
        city, state, lat, lon, region = CITIES[int(r.city_idx)]
        suffixes = TYPE_SUFFIXES[ftype]
        base = f"{city} {suffixes[int(r.suffix_idx) % len(suffixes)]}"
        seen[base] += 1
        name = base if seen[base] == 1 else f"{base} - Campus {seen[base]}"
        (bed_lo, bed_hi), (inp_lo, inp_hi) = SIZE[ftype]
        ed_beds = bed_lo + round((bed_hi - bed_lo) * int(r.size_pct) / 100)
        inpatient = inp_lo + round((inp_hi - inp_lo) * int(r.inpatient_pct) / 100)
        visits = round(ed_beds * VISITS_PER_BED[ftype] * (1 + int(r.visit_jitter_pct) / 100))
        rows.append(
            {
                "facility_id": f"FAC{i + 1:03d}",
                "facility_name": name,
                "region": region,
                "city": city,
                "state": state,
                "latitude": round(lat + int(r.lat_jitter_milli) / 1000, 4),
                "longitude": round(lon + int(r.lon_jitter_milli) / 1000, 4),
                "facility_type": ftype,
                "ed_beds": int(ed_beds),
                "inpatient_beds": int(inpatient),
                "baseline_daily_visits": int(visits),
                "scenario_profile": profiles[i],
            }
        )
    return pd.DataFrame(rows, columns=list(FACILITY_COLUMNS))
```

`src/ed_ops/generator/roster.py`:
```python
"""Attending physician roster: three 8-hour local shifts per day, converted to naive UTC."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

ROSTER_COLUMNS = ("facility_id", "shift_start", "shift_end", "shift_name", "attendings_on_duty")
SHIFTS = (("day", 7, 15, 1.0), ("evening", 15, 23, 1.1), ("night", 23, 7, 0.6))
VISITS_PER_ATTENDING_SHIFT = 45.0


def attendings_for(baseline_daily_visits: int, profile: str, shift_name: str, factor: float) -> int:
    n = round(baseline_daily_visits / VISITS_PER_ATTENDING_SHIFT * factor)
    if profile == "understaffed" and shift_name in ("evening", "night"):
        n -= 1
    if profile == "high_performer":
        n += 1
    return max(1, n)


def _utc(aware: datetime) -> datetime:
    return aware.astimezone(UTC).replace(tzinfo=None)


def build_roster(facilities: pd.DataFrame, start: date, days: int, tz_name: str) -> pd.DataFrame:
    tz = ZoneInfo(tz_name)
    rows = []
    for fac in facilities.itertuples(index=False):
        for d in range(days):
            day = start + timedelta(days=d)
            for name, start_hour, end_hour, factor in SHIFTS:
                end_day = day + timedelta(days=1) if end_hour <= start_hour else day
                rows.append(
                    (
                        fac.facility_id,
                        _utc(datetime.combine(day, time(start_hour), tzinfo=tz)),
                        _utc(datetime.combine(end_day, time(end_hour), tzinfo=tz)),
                        name,
                        attendings_for(int(fac.baseline_daily_visits), fac.scenario_profile, name, factor),
                    )
                )
    return pd.DataFrame(rows, columns=list(ROSTER_COLUMNS))
```

`src/ed_ops/generator/population.py`:
```python
"""Synthetic patient population built with dbldatagen; arrivals sample patients from it."""

from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

FIRST_NAMES = (
    "Ava", "Liam", "Mia", "Noah", "Emma", "Oliver", "Sophia", "Elijah", "Isabella", "James", "Amelia", "Lucas",
    "Harper", "Mason", "Evelyn", "Ethan", "Abigail", "Logan", "Emily", "Aiden", "Ella", "Jacob", "Grace", "Wyatt",
    "Chloe", "Henry", "Nora", "Daniel", "Lily", "Samuel", "Zoe", "Carter", "Layla", "Owen", "Riley", "Julian",
    "Aria", "Levi", "Maya", "Isaac",
)
LAST_NAMES = (
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
)
STREETS = (
    "Elm St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine St", "Washington Ave", "Lincoln Rd", "Lake St",
    "Hill Rd", "Park Ave", "Main St", "Church St", "River Rd", "Prairie Ln", "Sunset Blvd", "Highland Ave",
    "Grand Ave", "Jefferson St", "Madison Ave", "Walnut St",
)


def build_patient_pool(spark: SparkSession, size: int, seed: int) -> DataFrame:
    spec = (
        dg.DataGenerator(spark, name="patient_pool", rows=size, partitions=4, randomSeedMethod="hash_fieldname", randomSeed=seed)
        .withIdOutput()
        .withColumn("first_name", "string", values=list(FIRST_NAMES), random=True)
        .withColumn("last_name", "string", values=list(LAST_NAMES), random=True)
        .withColumn("dob", "date", data_range=dg.DateRange("1930-01-01 00:00:00", "2025-12-31 00:00:00", "days=1"), random=True)
        .withColumn("sex", "string", values=["F", "M"], weights=[52, 48], random=True)
        .withColumn("phone", "string", template=r"ddd-555-dddd")
        .withColumn("street_no", "int", minValue=10, maxValue=9999, random=True)
        .withColumn("street_name", "string", values=list(STREETS), random=True)
        .withColumn("zip", "string", template=r"6dddd")
    )
    return (
        spec.build()
        .withColumnRenamed("id", "pool_id")
        .withColumn("street", F.concat_ws(" ", F.col("street_no").cast("string"), F.col("street_name")))
        .withColumn("mrn", F.concat(F.lit("MRN-"), (F.col("pool_id") + F.lit(7000000)).cast("string")))
        .select("pool_id", "first_name", "last_name", "dob", "sex", "phone", "street", "zip", "mrn")
    )
```

`src/ed_ops/generator/reference.py`:
```python
"""In-memory reference data for the simulator (pandas; small enough for the driver)."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from ed_ops.config import Config


class ReferenceData:
    def __init__(self, facilities: pd.DataFrame, roster: pd.DataFrame, pool: pd.DataFrame) -> None:
        self.facilities = facilities.reset_index(drop=True)
        self.roster = roster.reset_index(drop=True)
        self.pool = pool.sort_values("pool_id").reset_index(drop=True)
        if not np.array_equal(self.pool["pool_id"].to_numpy(), np.arange(len(self.pool))):
            raise ValueError("patient pool ids must be contiguous 0..N-1")
        self._patients = self.pool.to_dict("records")
        self._facilities = {f.facility_id: f for f in self.facilities.itertuples(index=False)}
        self._shifts: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
        for fid, group in self.roster.groupby("facility_id"):
            self._shifts[fid] = (
                group["shift_start"].to_numpy(dtype="datetime64[ns]"),
                group["shift_end"].to_numpy(dtype="datetime64[ns]"),
                group["attendings_on_duty"].to_numpy(dtype=int),
            )

    @property
    def pool_size(self) -> int:
        return len(self._patients)

    def patient(self, pool_id: int) -> dict:
        return self._patients[int(pool_id)]

    def facility(self, facility_id: str):
        return self._facilities[facility_id]

    def attendings_at(self, facility_id: str, ts: datetime) -> int:
        if facility_id not in self._shifts:
            return 0
        starts, ends, counts = self._shifts[facility_id]
        t = np.datetime64(ts, "ns")
        return int(counts[(starts <= t) & (ends > t)].sum())


def load_reference(spark: SparkSession, cfg: Config) -> ReferenceData:
    return ReferenceData(
        spark.table(cfg.table("ref", "facility")).toPandas(),
        spark.table(cfg.table("ref", "physician_roster")).toPandas(),
        spark.table(cfg.table("sim", "patient_pool")).toPandas(),
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_reference.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/generator/facilities.py src/ed_ops/generator/roster.py src/ed_ops/generator/population.py src/ed_ops/generator/reference.py tests/unit/test_reference.py
git commit -m "feat: simulated facility network, physician roster and patient population" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 11: Patient-journey planner with load feedback

**Files:**
- Create: `src/ed_ops/generator/journey.py`, `tests/gen_factories.py`
- Test: `tests/unit/test_journey.py`

**Interfaces:**
- Consumes: `curves`, `scenarios` (Task 8); `notes_templates` (Task 9); `ReferenceData` (Task 10); `timeutil.to_local` (Task 1).
- Produces:
  - `journey.PLAN_COLUMNS` (equal to the `sim.visit_plan` columns) and `journey.TS_COLUMNS`
  - `empty_plan() -> pd.DataFrame`, `normalize_plan(df) -> pd.DataFrame`
  - `census_at(plan, ts) -> dict[str, int]`
  - `load_factor(occupancy) -> float`, `staffing_factor(patients_per_attending) -> float`
  - `plan_visit(rng, fac, visit_id, arrival, occupancy, patients_per_attending, acuity_shift, patient, pool_id, planned_at) -> dict`
  - `plan_arrivals(rng, ref, plan, start, end, scenarios, tz_name) -> pd.DataFrame`, which plans arrivals strictly inside `(start, end)` at whole seconds
- Produces (tests): `tests/gen_factories.py` with `make_reference(facilities=None, pool_size=100)`, `FACILITIES` and `plan_hours(ref, start, hours, seed=42)`.

- [ ] **Step 1: Write the generator test factories**

`tests/gen_factories.py`:
```python
"""Pure-pandas reference data for generator tests (no Spark needed)."""

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from ed_ops.generator.journey import empty_plan, plan_arrivals
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.roster import build_roster

FACILITIES = pd.DataFrame(
    [
        ("FAC001", "Springfield Memorial Hospital", "Central", "Springfield", "IL", 39.8, -89.6, "community", 30, 200, 72, "normal"),
        ("FAC002", "Peoria Regional Medical Center", "Central", "Peoria", "IL", 40.7, -89.6, "level_1_trauma", 60, 450, 144, "chronic_boarding"),
        ("FAC003", "Madison Community Hospital", "North", "Madison", "WI", 43.1, -89.4, "community", 25, 150, 60, "surge_prone"),
    ],
    columns=["facility_id", "facility_name", "region", "city", "state", "latitude", "longitude", "facility_type",
             "ed_beds", "inpatient_beds", "baseline_daily_visits", "scenario_profile"],
)


def make_pool(size: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "pool_id": np.arange(size, dtype="int64"),
            "first_name": [f"Pat{i}" for i in range(size)],
            "last_name": ["Tester"] * size,
            "dob": [date(1950 + i % 50, 1 + i % 12, 1 + i % 28) for i in range(size)],
            "sex": ["F" if i % 2 == 0 else "M" for i in range(size)],
            "phone": [f"312-555-{i:04d}" for i in range(size)],
            "street": [f"{i + 10} Main St" for i in range(size)],
            "zip": ["60601"] * size,
            "mrn": [f"MRN-{7000000 + i}" for i in range(size)],
        }
    )


def make_reference(facilities: pd.DataFrame | None = None, pool_size: int = 100) -> ReferenceData:
    fac = FACILITIES if facilities is None else facilities
    roster = build_roster(fac, date(2026, 10, 20), 40, "America/Chicago")
    return ReferenceData(fac, roster, make_pool(pool_size))


def plan_hours(ref: ReferenceData, start: datetime, hours: int, seed: int = 42) -> pd.DataFrame:
    plan = empty_plan()
    for h in range(hours):
        s, e = start + timedelta(hours=h), start + timedelta(hours=h + 1)
        rng = np.random.default_rng([seed, h])
        new = plan_arrivals(rng, ref, plan, s, e, [], "America/Chicago")
        plan = pd.concat([plan, new], ignore_index=True) if not new.empty else plan
    return plan
```

- [ ] **Step 2: Write the failing tests**

`tests/unit/test_journey.py`:
```python
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from ed_ops.generator.journey import (
    PLAN_COLUMNS, census_at, load_factor, plan_arrivals, plan_visit, staffing_factor,
)
from tests.gen_factories import FACILITIES, make_reference, plan_hours

START = datetime(2026, 11, 2, 6)  # Monday 00:00 Chicago


@pytest.fixture(scope="module")
def day_plan():
    return plan_hours(make_reference(), START, 24)


def _le(a, b):
    return pd.isna(a) or pd.isna(b) or a <= b


def test_plan_columns_and_types(day_plan):
    from ed_ops.ddl.metadata import load_catalog_def

    assert list(PLAN_COLUMNS) == load_catalog_def().table("sim", "visit_plan").column_names
    assert list(day_plan.columns) == list(PLAN_COLUMNS)
    assert str(day_plan["arrival_ts"].dtype).startswith("datetime64")
    assert len(day_plan) > 100


def test_journey_ordering_invariants(day_plan):
    for r in day_plan.itertuples(index=False):
        assert r.arrival_ts < r.triage_ts
        if not pd.isna(r.lwbs_ts):
            assert pd.isna(r.greet_ts) and pd.isna(r.bed_assigned_ts) and pd.isna(r.disposition_ts)
            assert r.lwbs_ts > r.triage_ts and r.departure_ts == r.lwbs_ts
            assert r.nurse_esi >= 3
            continue
        assert r.triage_ts < r.greet_ts and r.triage_ts <= r.bed_assigned_ts
        assert max(r.greet_ts, r.bed_assigned_ts) <= r.disposition_ts < r.departure_ts
        assert r.disposition in {"DISCHARGE", "ADMIT", "TRANSFER"}
        if r.disposition == "ADMIT":
            assert r.departure_ts - r.disposition_ts <= timedelta(hours=48, minutes=1)
        if not pd.isna(r.vent_start_ts):
            assert r.true_esi <= 2
            assert r.bed_assigned_ts <= r.vent_start_ts <= r.vent_end_ts <= r.departure_ts


def test_visit_ids_unique_and_match_arrival_minute(day_plan):
    assert day_plan.visit_id.is_unique
    for r in day_plan.itertuples(index=False):
        assert re.fullmatch(r"FAC\d{3}-\d{12}-\d{3}", r.visit_id)
        assert r.visit_id.split("-")[1] == r.arrival_ts.strftime("%Y%m%d%H%M")


def test_arrivals_strictly_inside_window():
    ref = make_reference()
    s, e = datetime(2026, 11, 2, 18), datetime(2026, 11, 2, 18, 1)
    total = 0
    for i in range(200):
        new = plan_arrivals(np.random.default_rng([1, i]), ref, pd.DataFrame(columns=PLAN_COLUMNS), s, e, [], "America/Chicago")
        assert ((new.arrival_ts > s) & (new.arrival_ts < e)).all()
        total += len(new)
    assert total > 0


def test_arrival_volume_matches_baseline_over_a_week():
    ref = make_reference(FACILITIES.head(1))  # 72 visits/day
    week = plan_hours(ref, START, 24 * 7, seed=7)
    assert abs(len(week) - 72 * 7) / (72 * 7) < 0.08


def test_load_feedback_increases_door_to_provider():
    ref = make_reference()
    fac = ref.facility("FAC001")
    patient = {**ref.patient(0), "city": fac.city, "state": fac.state}

    def median_d2p(occupancy):
        rng = np.random.default_rng(11)
        waits = []
        for i in range(400):
            v = plan_visit(rng, fac, f"FAC001-202611021200-{i:03d}", datetime(2026, 11, 2, 18), occupancy, 4.0, False, patient, 0, datetime(2026, 11, 2, 19))
            if v["greet_ts"] is not None:
                waits.append((v["greet_ts"] - v["arrival_ts"]).total_seconds() / 60)
        return float(np.median(waits))

    assert median_d2p(1.3) > 1.5 * median_d2p(0.4)
    assert load_factor(0.5) == 1.0 and load_factor(1.2) > load_factor(0.9) > 1.0
    assert staffing_factor(3.0) == 1.0 and staffing_factor(12.0) > 1.0


def test_undertriage_share_about_three_percent(day_plan):
    week = plan_hours(make_reference(), START, 72, seed=3)
    share = ((week.nurse_esi - week.true_esi) >= 2).mean()
    assert 0.015 < share < 0.05


def test_census_at(day_plan):
    t = START + timedelta(hours=12)
    census = census_at(day_plan, t)
    expected = day_plan[(day_plan.arrival_ts <= t) & (day_plan.departure_ts > t)].groupby("facility_id").size().to_dict()
    assert census == expected and census
    assert census_at(pd.DataFrame(columns=PLAN_COLUMNS), t) == {}


def test_plan_is_deterministic():
    a = plan_hours(make_reference(), START, 3, seed=5)
    b = plan_hours(make_reference(), START, 3, seed=5)
    pd.testing.assert_frame_equal(a, b)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_journey.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.generator.journey'`.

- [ ] **Step 4: Implement `journey.py`**

`src/ed_ops/generator/journey.py`:
```python
"""Plan each visit's full journey at arrival, with durations driven by current load (spec section 6)."""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from ed_ops.generator import curves
from ed_ops.generator.notes_templates import pick_archetype, render_note
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.scenarios import ScenarioWindow, acuity_shift_active, arrival_multiplier
from ed_ops.timeutil import to_local

PLAN_COLUMNS = (
    "facility_id", "visit_id", "pool_id", "arrival_ts", "arrival_mode", "true_esi", "nurse_esi", "archetype",
    "triage_ts", "bed_assigned_ts", "greet_ts", "vent_start_ts", "vent_end_ts", "disposition", "disposition_ts",
    "lwbs_ts", "departure_ts", "triage_note", "chief_complaint_text", "planned_at",
)
TS_COLUMNS = (
    "arrival_ts", "triage_ts", "bed_assigned_ts", "greet_ts", "vent_start_ts", "vent_end_ts", "disposition_ts",
    "lwbs_ts", "departure_ts", "planned_at",
)
INT_COLUMNS = ("pool_id", "true_esi", "nurse_esi")
UNDERTRIAGE_P = 0.03
MAX_BOARDING_MIN = 48 * 60.0


def normalize_plan(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reindex(columns=list(PLAN_COLUMNS))
    for c in TS_COLUMNS:
        df[c] = pd.to_datetime(df[c])
    for c in INT_COLUMNS:
        df[c] = df[c].astype("int64")
    return df.reset_index(drop=True)


def empty_plan() -> pd.DataFrame:
    return normalize_plan(pd.DataFrame(columns=list(PLAN_COLUMNS)))


def census_at(plan: pd.DataFrame, ts: datetime) -> dict[str, int]:
    if plan.empty:
        return {}
    mask = (plan["arrival_ts"] <= ts) & (plan["departure_ts"] > ts)
    return {k: int(v) for k, v in plan.loc[mask].groupby("facility_id").size().items()}


def load_factor(occupancy: float) -> float:
    return 1.0 + 3.0 * max(0.0, occupancy - 0.7) + 10.0 * max(0.0, occupancy - 1.0)


def staffing_factor(patients_per_attending: float) -> float:
    return max(1.0, patients_per_attending / 6.0) ** 0.7


def _lognormal(rng: np.random.Generator, median: float, sigma: float) -> float:
    return float(median * math.exp(sigma * rng.standard_normal()))


def _mins(m: float) -> timedelta:
    return timedelta(minutes=m)


def _pick_disposition(rng: np.random.Generator, facility_type: str, true_esi: int) -> str:
    p_admit = min(0.95, curves.ADMIT_P[true_esi] * curves.ADMIT_TYPE_FACTOR[facility_type])
    p_transfer = 0.10 if (facility_type == "rural_critical_access" and true_esi <= 2) else 0.01
    u = rng.random()
    if u < p_admit:
        return "ADMIT"
    if u < p_admit + p_transfer:
        return "TRANSFER"
    return "DISCHARGE"


def plan_visit(rng, fac, visit_id: str, arrival: datetime, occupancy: float, patients_per_attending: float,
               acuity_shift: bool, patient: dict, pool_id: int, planned_at: datetime) -> dict:
    true_esi = int(rng.choice(5, p=curves.esi_mix(fac.facility_type, acuity_shift))) + 1
    nurse_esi = true_esi
    if rng.random() < UNDERTRIAGE_P:
        true_esi = int(rng.choice([1, 2], p=[0.25, 0.75]))
        nurse_esi = int(rng.choice([4, 5], p=[0.7, 0.3]))
    archetype = pick_archetype(rng, patient["sex"])
    arrival_mode = "ambulance" if rng.random() < curves.AMBULANCE_P[true_esi] else "walk_in"
    note, complaint = render_note(rng, archetype, true_esi, patient, arrival)

    speed = curves.PROFILE_SPEED[fac.scenario_profile]
    lf, sf = load_factor(occupancy), staffing_factor(patients_per_attending)
    d2t = _lognormal(rng, curves.D2T_MEDIAN[nurse_esi] * (0.5 if arrival_mode == "ambulance" else 1.0), 0.5)
    triage = arrival + _mins(max(0.5, d2t))
    d2p = _lognormal(rng, curves.D2P_MEDIAN[nurse_esi] * lf * sf * speed, 0.6)
    greet = max(triage + _mins(1), arrival + _mins(d2p))
    patience = _lognormal(rng, curves.PATIENCE_MEDIAN, 0.6)

    row = {c: None for c in PLAN_COLUMNS}
    row.update(
        facility_id=fac.facility_id, visit_id=visit_id, pool_id=int(pool_id), arrival_ts=arrival,
        arrival_mode=arrival_mode, true_esi=true_esi, nurse_esi=nurse_esi, archetype=archetype,
        triage_ts=triage, triage_note=note, chief_complaint_text=complaint, planned_at=planned_at,
    )
    if nurse_esi >= 3 and (greet - arrival).total_seconds() / 60 > patience:
        lwbs = max(triage + _mins(5), arrival + _mins(patience))
        row.update(lwbs_ts=lwbs, departure_ts=lwbs)
        return _whole_seconds(row)

    bed = triage + _mins(_lognormal(rng, curves.BED_WAIT_MEDIAN * lf * speed, 0.7))
    treat = _lognormal(rng, curves.TREAT_MEDIAN[true_esi] * (1.0 + 0.3 * max(0.0, occupancy - 0.8)), 0.5)
    disposition_ts = max(greet, bed) + _mins(treat)
    disposition = _pick_disposition(rng, fac.facility_type, true_esi)
    if disposition == "DISCHARGE":
        after = _lognormal(rng, curves.DISCHARGE_TO_DEPART_MEDIAN, 0.6)
    elif disposition == "TRANSFER":
        after = _lognormal(rng, curves.TRANSFER_TO_DEPART_MEDIAN, 0.5)
    else:
        after = min(_lognormal(rng, curves.boarding_median(fac.scenario_profile), 0.7), MAX_BOARDING_MIN)
    departure = disposition_ts + _mins(max(1.0, after))
    vent_start = vent_end = None
    if rng.random() < curves.VENT_P.get(true_esi, 0.0):
        start = bed + _mins(float(rng.uniform(5, 30)))
        end = departure - _mins(5)
        if end > start:
            vent_start, vent_end = start, end
    row.update(
        bed_assigned_ts=bed, greet_ts=greet, disposition=disposition, disposition_ts=disposition_ts,
        departure_ts=departure, vent_start_ts=vent_start, vent_end_ts=vent_end,
    )
    return _whole_seconds(row)


def _whole_seconds(row: dict) -> dict:
    for c in TS_COLUMNS:
        if row[c] is not None:
            row[c] = row[c].replace(microsecond=0)
    return row


def plan_arrivals(rng: np.random.Generator, ref: ReferenceData, plan: pd.DataFrame, start: datetime, end: datetime,
                  scenarios: list[ScenarioWindow], tz_name: str) -> pd.DataFrame:
    seconds = int((end - start).total_seconds())
    if seconds < 2:
        return empty_plan()
    mid = start + (end - start) / 2
    local = to_local(mid, tz_name)
    census = census_at(plan, start)
    rows = []
    for fac in ref.facilities.itertuples(index=False):
        lam = (
            curves.arrivals_per_minute(fac.baseline_daily_visits, local.hour, local.weekday())
            * arrival_multiplier(scenarios, fac.facility_id, mid)
            * seconds / 60.0
        )
        n = int(rng.poisson(lam))
        if n == 0:
            continue
        in_ed = census.get(fac.facility_id, 0)
        occupancy = in_ed / fac.ed_beds
        ppa = in_ed / max(1, ref.attendings_at(fac.facility_id, start))
        shift = acuity_shift_active(scenarios, fac.facility_id, mid)
        seq: Counter[str] = Counter()
        for offset in np.sort(rng.integers(1, seconds, n)):
            arrival = start + timedelta(seconds=int(offset))
            key = arrival.strftime("%Y%m%d%H%M")
            seq[key] += 1
            pool_id = int(rng.integers(0, ref.pool_size))
            patient = {**ref.patient(pool_id), "city": fac.city, "state": fac.state}
            rows.append(
                plan_visit(rng, fac, f"{fac.facility_id}-{key}-{seq[key]:03d}", arrival, occupancy, ppa, shift,
                           patient, pool_id, end)
            )
    if not rows:
        return empty_plan()
    return normalize_plan(pd.DataFrame(rows, columns=list(PLAN_COLUMNS)))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_journey.py -v`
Expected: 9 passed. If `test_arrival_volume_matches_baseline_over_a_week` fails by a small margin, check the seed and the Poisson math before touching tolerances. The expected total is exactly `72 × 7` because `HOURLY` and `DOW` are mean-normalised.

- [ ] **Step 6: Commit**

```bash
git add src/ed_ops/generator/journey.py tests/gen_factories.py tests/unit/test_journey.py
git commit -m "feat: patient-journey planner with occupancy and staffing feedback" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 12: Event emission with chaos, and the landing writer

**Files:**
- Create: `src/ed_ops/generator/events.py`
- Test: `tests/unit/test_events.py`

**Interfaces:**
- Consumes: `PLAN_COLUMNS` and the plan DataFrame (Task 11); `ReferenceData` (Task 10); `Config` (Task 1); `events_contract.EVENT_SCHEMA_DDL` (Task 2); `timeutil.epoch_seconds`.
- Produces:
  - `events.STAGES`, `events.MAX_EMIT_DELAY`
  - `event_id(visit_id, event_type, variant="") -> str`
  - `schedule(row, ref, cfg) -> list[tuple[datetime, dict]]`
  - `events_in_window(plan, ref, start_exclusive, end_inclusive, cfg) -> list[dict]`. Event dicts have the keys `event_id, event_type, event_ts, emitted_ts, facility_id, visit_id, payload`, with timestamps as `YYYY-MM-DDTHH:MM:SSZ`.
  - `LandingWriter(root)`: callable `(events, window_end) -> path`, which writes JSON lines to `{root}/yyyy/mm/dd/hh/tick_<epoch>.json`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_events.py`:
```python
import json
from collections import Counter
from datetime import datetime, timedelta

import pandas as pd
import pytest

from ed_ops.config import Config
from ed_ops.events_contract import EVENT_SCHEMA_DDL
from ed_ops.generator.events import LandingWriter, events_in_window
from tests.gen_factories import make_reference, plan_hours

START = datetime(2026, 11, 2, 18)
CLEAN = Config(dup_rate=0.0, late_rate=0.0, malformed_rate=0.0)


@pytest.fixture(scope="module")
def world():
    ref = make_reference()
    return ref, plan_hours(ref, START, 3)


def _sweep(plan, ref, cfg, hours=72):
    out, t = [], START
    while t < START + timedelta(hours=hours):
        out += events_in_window(plan, ref, t, t + timedelta(minutes=60), cfg)
        t += timedelta(minutes=60)
    return out


def test_every_stage_emitted_exactly_once(world):
    ref, plan = world
    events = _sweep(plan, ref, CLEAN)
    ids = Counter(e["event_id"] for e in events)
    assert max(ids.values()) == 1
    by_visit = Counter(e["visit_id"] for e in events)
    optional = ("bed_assigned_ts", "greet_ts", "vent_start_ts", "vent_end_ts", "disposition_ts", "lwbs_ts")
    for r in plan.itertuples(index=False):
        expected = 2  # ARRIVAL + TRIAGE
        expected += sum(0 if pd.isna(getattr(r, c)) else 1 for c in optional)
        expected += 1 if pd.isna(r.lwbs_ts) else 0  # DEPARTURE unless the patient left without being seen
        assert by_visit[r.visit_id] == expected, r.visit_id


def test_emitted_ts_is_window_end_and_not_before_event(world):
    ref, plan = world
    end = START + timedelta(minutes=60)
    for e in events_in_window(plan, ref, START, end, CLEAN):
        assert e["emitted_ts"] == "2026-11-02T19:00:00Z"
        assert e["event_ts"] <= e["emitted_ts"]


def test_arrival_payload_has_phi_and_triage_has_esi(world):
    ref, plan = world
    events = _sweep(plan, ref, CLEAN, hours=4)
    arrival = next(e for e in events if e["event_type"] == "ARRIVAL")
    p = arrival["payload"]
    assert p["patient_name"] and p["mrn"].startswith("MRN-") and p["address"]["city"]
    triage = next(e for e in events if e["event_type"] == "TRIAGE")
    assert 1 <= triage["payload"]["esi_level"] <= 5 and triage["payload"]["triage_note"]


def test_late_events_are_held_back(world):
    ref, plan = world
    events = _sweep(plan, ref, Config(dup_rate=0.0, late_rate=1.0, malformed_rate=0.0), hours=6)
    for e in events:
        delay = datetime.fromisoformat(e["emitted_ts"][:-1]) - datetime.fromisoformat(e["event_ts"][:-1])
        assert delay >= timedelta(minutes=5)


def test_duplicates_reuse_event_id(world):
    ref, plan = world
    events = _sweep(plan, ref, Config(dup_rate=1.0, late_rate=0.0, malformed_rate=0.0))
    counts = Counter(e["event_id"] for e in events)
    assert set(counts.values()) == {2}


def test_malformed_events_are_extra_and_corrupted(world):
    ref, plan = world
    clean = _sweep(plan, ref, CLEAN, hours=6)
    dirty = _sweep(plan, ref, Config(dup_rate=0.0, late_rate=0.0, malformed_rate=1.0), hours=6)
    assert len(dirty) == 2 * len(clean)
    bad = [e for e in dirty if e["event_id"] not in {c["event_id"] for c in clean}]
    for e in bad:
        assert e["visit_id"] is None or e["facility_id"] == "FAC999" or e["payload"].get("esi_level") == 9


def test_deterministic(world):
    ref, plan = world
    cfg = Config()
    assert _sweep(plan, ref, cfg, hours=6) == _sweep(plan, ref, cfg, hours=6)


def test_landing_file_parses_with_bronze_contract(spark, world, tmp_path):
    ref, plan = world
    end = START + timedelta(minutes=60)
    events = events_in_window(plan, ref, START, end, CLEAN)
    path = LandingWriter(str(tmp_path))(events, end)
    assert path == f"{tmp_path}/2026/11/02/19/tick_{int((end - datetime(1970, 1, 1)).total_seconds())}.json"
    assert len(open(path).read().splitlines()) == len(events)
    df = spark.read.schema(EVENT_SCHEMA_DDL).json(path)
    assert df.count() == len(events)
    rows = df.filter("event_type = 'ARRIVAL'").select("event_ts", "payload.address.city", "payload.dob").collect()
    assert rows and all(r["city"] and r["dob"] for r in rows)
    assert df.filter("event_ts IS NULL").count() == 0
    first = json.loads(open(path).readline())
    assert first["event_ts"].endswith("Z")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_events.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.generator.events'`.

- [ ] **Step 3: Implement `events.py`**

`src/ed_ops/generator/events.py`:
```python
"""Turn planned journeys into landing events. Chaos (late, duplicate, malformed) is a deterministic
function of (seed, visit_id, event_type), so replays emit identical files."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta

import pandas as pd

from ed_ops.config import Config
from ed_ops.generator.reference import ReferenceData
from ed_ops.timeutil import epoch_seconds

NAMESPACE = uuid.UUID("6f1c2b8e-4d0a-4f7e-9a51-3e1d5c7b2a90")
STAGES = (
    ("ARRIVAL", "arrival_ts"),
    ("TRIAGE", "triage_ts"),
    ("BED_ASSIGNED", "bed_assigned_ts"),
    ("PROVIDER_GREET", "greet_ts"),
    ("VENT_START", "vent_start_ts"),
    ("VENT_END", "vent_end_ts"),
    ("DISPOSITION", "disposition_ts"),
    ("LWBS", "lwbs_ts"),
    ("DEPARTURE", "departure_ts"),
)
MAX_EMIT_DELAY = timedelta(minutes=60)  # late (<= 20 min) + duplicate (<= 30 min) re-emission


def _u(seed: int, *parts: object) -> float:
    digest = hashlib.blake2b("|".join(str(p) for p in (seed, *parts)).encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def event_id(visit_id: str, event_type: str, variant: str = "") -> str:
    return str(uuid.uuid5(NAMESPACE, f"{visit_id}|{event_type}|{variant}"))


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _opt(value):
    if value is None or pd.isna(value):
        return None
    return value.to_pydatetime() if isinstance(value, pd.Timestamp) else value


def _payload(event_type: str, row, ref: ReferenceData) -> dict:
    fac = ref.facility(row.facility_id)
    if event_type == "ARRIVAL":
        p = ref.patient(row.pool_id)
        return {
            "arrival_mode": row.arrival_mode,
            "patient_name": f"{p['first_name']} {p['last_name']}",
            "mrn": p["mrn"],
            "dob": p["dob"].isoformat(),
            "sex": p["sex"],
            "phone": p["phone"],
            "address": {"street": p["street"], "city": fac.city, "state": fac.state, "zip": p["zip"]},
            "chief_complaint_text": row.chief_complaint_text,
        }
    if event_type == "TRIAGE":
        return {"esi_level": int(row.nurse_esi), "triage_note": row.triage_note, "chief_complaint_text": row.chief_complaint_text}
    if event_type == "BED_ASSIGNED":
        return {"bed_id": f"{row.facility_id}-B{1 + int(_u(0, row.visit_id, 'bed') * fac.ed_beds):02d}"}
    if event_type == "PROVIDER_GREET":
        return {"provider_id": f"{row.facility_id}-MD{1 + int(_u(0, row.visit_id, 'md') * 9)}"}
    if event_type == "DISPOSITION":
        return {"disposition": row.disposition}
    return {}


def _corrupt(event: dict, kind: int) -> dict:
    bad = copy.deepcopy(event)
    bad["event_id"] = event_id(str(event["visit_id"]), event["event_type"], "malformed")
    if kind == 1:
        bad["facility_id"] = "FAC999"
    elif kind == 2 and bad["event_type"] == "TRIAGE":
        bad["payload"]["esi_level"] = 9
    else:
        bad["visit_id"] = None
    return bad


def schedule(row, ref: ReferenceData, cfg: Config) -> list[tuple[datetime, dict]]:
    lwbs = _opt(row.lwbs_ts)
    out: list[tuple[datetime, dict]] = []
    for etype, column in STAGES:
        ts = _opt(getattr(row, column))
        if ts is None or (etype == "DEPARTURE" and lwbs is not None):
            continue
        event = {
            "event_id": event_id(row.visit_id, etype),
            "event_type": etype,
            "event_ts": _iso(ts),
            "facility_id": row.facility_id,
            "visit_id": row.visit_id,
            "payload": _payload(etype, row, ref),
        }
        emit_at = ts
        if _u(cfg.seed, row.visit_id, etype, "late") < cfg.late_rate:
            emit_at = ts + timedelta(minutes=5 + int(_u(cfg.seed, row.visit_id, etype, "late_min") * 16))
        out.append((emit_at, event))
        if _u(cfg.seed, row.visit_id, etype, "dup") < cfg.dup_rate:
            out.append((emit_at + timedelta(minutes=1 + int(_u(cfg.seed, row.visit_id, etype, "dup_min") * 29)), event))
        if _u(cfg.seed, row.visit_id, etype, "bad") < cfg.malformed_rate:
            out.append((emit_at, _corrupt(event, int(_u(cfg.seed, row.visit_id, etype, "bad_kind") * 3))))
    return out


def events_in_window(plan: pd.DataFrame, ref: ReferenceData, start_exclusive: datetime, end_inclusive: datetime,
                     cfg: Config) -> list[dict]:
    if plan.empty:
        return []
    candidates = plan[(plan["arrival_ts"] <= end_inclusive) & (plan["departure_ts"] >= start_exclusive - MAX_EMIT_DELAY)]
    emitted = _iso(end_inclusive)
    events = []
    for row in candidates.itertuples(index=False):
        for emit_at, event in schedule(row, ref, cfg):
            if start_exclusive < emit_at <= end_inclusive:
                events.append({**event, "emitted_ts": emitted})
    events.sort(key=lambda e: (e["event_ts"], e["event_type"], e["event_id"]))
    return events


class LandingWriter:
    """Writes one JSON-lines file per window. Files on UC Volumes become visible only once closed."""

    def __init__(self, root: str) -> None:
        self.root = root.rstrip("/")

    def __call__(self, events: list[dict], window_end: datetime) -> str:
        directory = f"{self.root}/{window_end:%Y/%m/%d/%H}"
        os.makedirs(directory, exist_ok=True)
        path = f"{directory}/tick_{epoch_seconds(window_end)}.json"
        with open(path, "w") as fh:
            for e in events:
                fh.write(json.dumps(e, separators=(",", ":")) + "\n")
        return path
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_events.py -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/generator/events.py tests/unit/test_events.py
git commit -m "feat: deterministic event emission with late, duplicate and malformed chaos" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 13: Generator stores and the `advance()` loop

**Files:**
- Create: `src/ed_ops/generator/stores.py`, `src/ed_ops/generator/tick.py`
- Test: `tests/unit/test_tick.py`

**Interfaces:**
- Consumes: `GeneratorState` and `build_scenarios` (Task 8); `plan_arrivals`, `empty_plan` and `normalize_plan` (Task 11); `events_in_window` (Task 12); `merge_into` and `table_def` (Task 7); `timeutil`.
- Produces:
  - `stores.InMemoryStore(state=None)` and `stores.DeltaStore(spark, cfg)`. Both implement `load_state() -> GeneratorState | None`, `save_state(state)`, `load_plan(since) -> pd.DataFrame` and `upsert_plan(df)`.
  - `tick.CHUNK = timedelta(hours=1)`, `tick.PLAN_LOOKBACK = timedelta(days=3)`
  - `tick.initial_state(now, backfill_days) -> GeneratorState`
  - `tick.advance(store, ref, cfg, now, *, live, writer, persist_each_chunk) -> list[str]`, which returns the paths written.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_tick.py`:
```python
import json
from collections import Counter
from datetime import datetime, timedelta

import pytest

from ed_ops.config import Config
from ed_ops.generator.stores import InMemoryStore
from ed_ops.generator.tick import advance, initial_state
from tests.gen_factories import make_reference

T0 = datetime(2026, 11, 2, 12)
CLEAN = Config(dup_rate=0.0, late_rate=0.0, malformed_rate=0.0)


class MemoryWriter:
    def __init__(self):
        self.files: list[tuple[datetime, list[dict]]] = []

    def __call__(self, events, window_end):
        self.files.append((window_end, events))
        return f"mem://{window_end.isoformat()}"


def _store():
    return InMemoryStore(initial_state(T0 + timedelta(days=1), 1))  # backfill_start = T0


def test_initial_state_is_hour_aligned():
    s = initial_state(datetime(2026, 11, 9, 12, 34), 14)
    assert s.backfill_start_ts == s.last_planned_ts == s.last_emitted_ts == datetime(2026, 10, 26, 12)
    assert s.live_started_ts is None


def test_backfill_writes_one_file_per_hour_and_persists_once():
    store, writer = _store(), MemoryWriter()
    paths = advance(store, make_reference(), CLEAN, T0 + timedelta(hours=6), live=False, writer=writer, persist_each_chunk=False)
    assert len(paths) == 6 and [w for w, _ in writer.files] == [T0 + timedelta(hours=h) for h in range(1, 7)]
    state = store.load_state()
    assert state.last_planned_ts == state.last_emitted_ts == T0 + timedelta(hours=6)
    assert store.plan.visit_id.is_unique and len(store.plan) > 50


def test_replay_to_same_time_is_a_noop():
    store, writer = _store(), MemoryWriter()
    advance(store, make_reference(), CLEAN, T0 + timedelta(hours=2), live=False, writer=writer, persist_each_chunk=False)
    assert advance(store, make_reference(), CLEAN, T0 + timedelta(hours=2), live=False, writer=writer, persist_each_chunk=False) == []


def test_catch_up_after_downtime_chunks_hourly_without_duplicates():
    store, writer = _store(), MemoryWriter()
    ref = make_reference()
    advance(store, ref, CLEAN, T0 + timedelta(minutes=60), live=True, writer=writer, persist_each_chunk=True)
    advance(store, ref, CLEAN, T0 + timedelta(hours=6, minutes=7), live=True, writer=writer, persist_each_chunk=True)
    assert [w for w, _ in writer.files][-1] == T0 + timedelta(hours=6, minutes=7)
    assert len(writer.files) == 7  # 1 + five full hours + one 7-minute chunk
    assert store.plan.visit_id.is_unique
    ids = Counter(e["event_id"] for _, evs in writer.files for e in evs)
    assert max(ids.values()) == 1


def test_live_sets_live_started_once():
    store = _store()
    ref = make_reference()
    advance(store, ref, CLEAN, T0 + timedelta(minutes=1), live=True, writer=MemoryWriter(), persist_each_chunk=True)
    first = store.load_state().live_started_ts
    advance(store, ref, CLEAN, T0 + timedelta(minutes=2), live=True, writer=MemoryWriter(), persist_each_chunk=True)
    assert first == T0 + timedelta(minutes=1) == store.load_state().live_started_ts


def test_deterministic_across_fresh_stores():
    def run():
        w = MemoryWriter()
        advance(_store(), make_reference(), Config(), T0 + timedelta(hours=3), live=False, writer=w, persist_each_chunk=False)
        return json.dumps(w.files, default=str)

    assert run() == run()


def test_missing_state_raises():
    with pytest.raises(RuntimeError, match="generator state missing"):
        advance(InMemoryStore(None), make_reference(), CLEAN, T0, live=True, writer=MemoryWriter(), persist_each_chunk=True)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_tick.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.generator.stores'`.

- [ ] **Step 3: Implement the stores and `advance()`**

`src/ed_ops/generator/stores.py`:
```python
"""Persistence for generator state and visit plans: in-memory (tests) and Delta (workspace)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ed_ops.config import Config
from ed_ops.delta import merge_into, table_def
from ed_ops.generator.journey import empty_plan, normalize_plan
from ed_ops.generator.state import GeneratorState
from ed_ops.timeutil import utcnow

GENERATOR_ID = "main"


class InMemoryStore:
    def __init__(self, state: GeneratorState | None = None) -> None:
        self.state = state
        self.plan = empty_plan()

    def load_state(self) -> GeneratorState | None:
        return self.state

    def save_state(self, state: GeneratorState) -> None:
        self.state = state

    def load_plan(self, since: datetime) -> pd.DataFrame:
        return self.plan[self.plan["arrival_ts"] >= since].reset_index(drop=True)

    def upsert_plan(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        merged = pd.concat([self.plan, df], ignore_index=True) if not self.plan.empty else df
        self.plan = normalize_plan(merged.drop_duplicates(subset=["facility_id", "visit_id"], keep="last"))


class DeltaStore:
    def __init__(self, spark: SparkSession, cfg: Config) -> None:
        self.spark = spark
        self.cfg = cfg

    def load_state(self) -> GeneratorState | None:
        rows = (
            self.spark.table(self.cfg.table("sim", "generator_state"))
            .filter(F.col("generator_id") == GENERATOR_ID)
            .collect()
        )
        if not rows:
            return None
        r = rows[0]
        return GeneratorState(r.generator_id, r.backfill_start_ts, r.last_planned_ts, r.last_emitted_ts, r.live_started_ts)

    def save_state(self, state: GeneratorState) -> None:
        df = self.spark.createDataFrame(
            [(state.generator_id, state.backfill_start_ts, state.last_planned_ts, state.last_emitted_ts,
              state.live_started_ts, utcnow())],
            table_def("sim", "generator_state").spark_ddl(),
        )
        merge_into(df, self.cfg, "sim", "generator_state")

    def load_plan(self, since: datetime) -> pd.DataFrame:
        pdf = (
            self.spark.table(self.cfg.table("sim", "visit_plan"))
            .filter(F.col("arrival_ts") >= F.lit(since).cast("timestamp"))
            .toPandas()
        )
        return normalize_plan(pdf)

    def upsert_plan(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        merge_into(self.spark.createDataFrame(df, table_def("sim", "visit_plan").spark_ddl()), self.cfg, "sim", "visit_plan")
```

`src/ed_ops/generator/tick.py`:
```python
"""Advance the simulator to `now`: plan arrivals and emit due events in hourly chunks."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from typing import Callable

import numpy as np
import pandas as pd

from ed_ops.config import Config
from ed_ops.generator.events import events_in_window
from ed_ops.generator.journey import empty_plan, plan_arrivals
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.scenarios import build_scenarios
from ed_ops.generator.state import GeneratorState
from ed_ops.timeutil import epoch_seconds, floor_to

CHUNK = timedelta(hours=1)
PLAN_LOOKBACK = timedelta(days=3)
Writer = Callable[[list[dict], datetime], str]


def initial_state(now: datetime, backfill_days: int) -> GeneratorState:
    start = floor_to(now - timedelta(days=backfill_days), 3600)
    return GeneratorState("main", start, start, start, None)


def advance(store, ref: ReferenceData, cfg: Config, now: datetime, *, live: bool, writer: Writer,
            persist_each_chunk: bool) -> list[str]:
    state = store.load_state()
    if state is None:
        raise RuntimeError("generator state missing; run the backfill-generate task first")
    if now <= state.last_planned_ts:
        return []
    if live and state.live_started_ts is None:
        state = replace(state, live_started_ts=now)
    windows = build_scenarios(ref.facilities, state, cfg)
    plan = store.load_plan(since=state.last_planned_ts - PLAN_LOOKBACK)
    pending: list[pd.DataFrame] = []
    paths: list[str] = []
    t = state.last_planned_ts
    while t < now:
        end = min(t + CHUNK, now)
        rng = np.random.default_rng([cfg.seed, epoch_seconds(end)])
        new = plan_arrivals(rng, ref, plan, t, end, windows, cfg.timezone)
        if not new.empty:
            plan = pd.concat([plan, new], ignore_index=True) if not plan.empty else new
        events = events_in_window(plan, ref, state.last_emitted_ts, end, cfg)
        if events:
            paths.append(writer(events, end))
        state = replace(state, last_planned_ts=end, last_emitted_ts=end)
        if persist_each_chunk:
            store.upsert_plan(new)
            store.save_state(state)
        else:
            pending.append(new)
        t = end
    if not persist_each_chunk:
        non_empty = [p for p in pending if not p.empty]
        store.upsert_plan(pd.concat(non_empty, ignore_index=True) if non_empty else empty_plan())
        store.save_state(state)
    return paths
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_tick.py -v`
Expected: 7 passed.

- [ ] **Step 5: Run the whole suite**

Run: `scripts/pytest.sh -v`
Expected: all tests from Tasks 1–13 pass.

- [ ] **Step 6: Commit**

```bash
git add src/ed_ops/generator/stores.py src/ed_ops/generator/tick.py tests/unit/test_tick.py
git commit -m "feat: generator stores and hourly-chunked advance loop with catch-up" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 14: Bronze ingestion, PHI de-identification and the silver visits stream

**Files:**
- Create: `src/ed_ops/governance/__init__.py`, `src/ed_ops/governance/deid.py`, `src/ed_ops/pipeline/__init__.py`, `src/ed_ops/pipeline/bronze.py`, `src/ed_ops/pipeline/silver_visits.py`
- Test: `tests/unit/test_silver.py`

**Interfaces:**
- Consumes: `EVENT_SCHEMA_DDL`, `EVENT_TYPES` (Task 2); `append_into`, `table_def` (Task 7); `Config` (Task 1).
- Produces:
  - `deid.DEID_COLUMNS`, `deid.deidentify(events, salt) -> DataFrame` (adds the de-identified columns and replaces `payload` with a PHI-free struct `arrival_mode, esi_level, bed_id, provider_id, disposition`)
  - `deid.ensure_salt(cfg)`, `deid.get_salt(cfg) -> str`
  - `bronze.add_ingest_metadata(df) -> DataFrame`, `bronze.start_bronze(spark, cfg, available_now=False) -> StreamingQuery`
  - `silver_visits`:
    - `REASON_CODES`
    - `validate_events(events, facility_ids) -> tuple[DataFrame, DataFrame]` (valid, rejected-with-`reason_code`)
    - `quarantine_rows(rejected) -> DataFrame`
    - `FOLD_COLUMNS`, `fold_events(events) -> DataFrame`
    - `visits_merge_sql(target, source_view) -> str`
    - `process_batch(batch_df, cfg, salt) -> None`
    - `start_silver_visits(spark, cfg, salt, available_now=False) -> StreamingQuery`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_silver.py`:
```python
import hashlib
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from pyspark.sql import functions as F

from ed_ops.config import Config
from ed_ops.delta import conform, table_def
from ed_ops.events_contract import EVENT_SCHEMA_DDL
from ed_ops.generator.events import LandingWriter, events_in_window
from ed_ops.governance.deid import deidentify
from ed_ops.pipeline.bronze import add_ingest_metadata
from ed_ops.pipeline.silver_visits import (
    FOLD_COLUMNS, fold_events, quarantine_rows, validate_events, visits_merge_sql,
)
from tests.gen_factories import make_reference, plan_hours

BRONZE_DDL = EVENT_SCHEMA_DDL + ", _rescued_data STRING, _source_file STRING, _ingest_ts TIMESTAMP, ingest_date DATE"
T = datetime(2026, 11, 9, 12)
SALT = "test-salt"
VID = "FAC001-202611091100-001"


def payload(arrival_mode=None, patient_name=None, mrn=None, dob=None, sex=None, phone=None, address=None,
            chief=None, esi=None, note=None, bed=None, provider=None, disposition=None):
    return (arrival_mode, patient_name, mrn, dob, sex, phone, address, chief, esi, note, bed, provider, disposition)


def ev(event_id, etype, ts, visit_id=VID, facility_id="FAC001", pl=None, rescued=None):
    return (event_id, etype, ts, ts, facility_id, visit_id, pl or payload(), rescued, "f.json", ts, ts.date() if ts else None)


ARRIVAL_PAYLOAD = payload("ambulance", "Ava Carter", "MRN-7000042", date(1958, 3, 14), "F", "312-555-0147",
                          ("42 Elm St", "Springfield", "IL", "62704"), "chest pain")
JOURNEY = [
    ev("e1", "ARRIVAL", T - timedelta(minutes=60), pl=ARRIVAL_PAYLOAD),
    ev("e2", "TRIAGE", T - timedelta(minutes=55), pl=payload(esi=3, note="note", chief="chest pain")),
    ev("e3", "BED_ASSIGNED", T - timedelta(minutes=40), pl=payload(bed="FAC001-B03")),
    ev("e4", "PROVIDER_GREET", T - timedelta(minutes=30), pl=payload(provider="FAC001-MD2")),
    ev("e5", "DISPOSITION", T - timedelta(minutes=10), pl=payload(disposition="ADMIT")),
    ev("e6", "DEPARTURE", T - timedelta(minutes=1)),
]


def _fold(spark, rows):
    df = spark.createDataFrame(rows, BRONZE_DDL)
    return {r["visit_id"]: r.asDict() for r in fold_events(deidentify(df, SALT)).collect()}


def test_validate_assigns_reason_codes(spark):
    rows = [
        ev("ok", "ARRIVAL", T, pl=ARRIVAL_PAYLOAD),
        ev("esi", "TRIAGE", T, pl=payload(esi=9)),
        ev("novisit", "TRIAGE", T, visit_id=None, pl=payload(esi=3)),
        ev("nofac", "TRIAGE", T, facility_id=None, pl=payload(esi=3)),
        ev("unknown", "TRIAGE", T, facility_id="FAC999", pl=payload(esi=3)),
        ev("type", "FOO", T),
        ev("rescued", "TRIAGE", T, pl=payload(esi=3), rescued='{"esi_level":"high"}'),
        ev("nots", "TRIAGE", None, pl=payload(esi=3)),
    ]
    valid, rejected = validate_events(spark.createDataFrame(rows, BRONZE_DDL), spark.createDataFrame([("FAC001",)], "facility_id STRING"))
    assert [r.event_id for r in valid.collect()] == ["ok"]
    reasons = {r.event_id: r.reason_code for r in rejected.collect()}
    assert reasons == {
        "esi": "invalid_esi", "novisit": "missing_visit_id", "nofac": "missing_facility_id",
        "unknown": "unknown_facility", "type": "invalid_event_type", "rescued": "unparseable",
        "nots": "missing_event_ts",
    }
    q = quarantine_rows(rejected)
    conform(q, table_def("silver", "ed_events_quarantine"))  # raises if the shape drifts
    assert {r.reason_code for r in q.collect()} == set(reasons.values())


def test_deidentify_structured_phi(spark):
    df = deidentify(spark.createDataFrame([JOURNEY[0], JOURNEY[1]], BRONZE_DDL), SALT)
    a = df.filter("event_type = 'ARRIVAL'").collect()[0]
    assert a.patient_name_redacted == "REDACTED"
    assert a.mrn_hash == hashlib.sha256(("MRN-7000042" + SALT).encode()).hexdigest()
    assert (a.birth_year, a.age_at_arrival, a.sex, a.phone_last4, a.city, a.state) == (1958, 68, "F", "0147", "Springfield", "IL")
    assert df.schema["payload"].dataType.fieldNames() == ["arrival_mode", "esi_level", "bed_id", "provider_id", "disposition"]
    t = df.filter("event_type = 'TRIAGE'").collect()[0]
    assert t.mrn_hash is None and t.payload.esi_level == 3


def test_fold_builds_visit_row(spark):
    v = _fold(spark, JOURNEY)[VID]
    assert v["arrival_ts"] == T - timedelta(minutes=60) and v["arrival_mode"] == "ambulance"
    assert v["esi_level"] == 3 and v["disposition"] == "ADMIT"
    assert v["greet_ts"] == T - timedelta(minutes=30) and v["departure_ts"] == T - timedelta(minutes=1)
    assert v["last_event_ts"] == T - timedelta(minutes=1)
    assert v["mrn_hash"] and v["patient_name_redacted"] == "REDACTED"


def test_fold_is_order_independent(spark):
    assert _fold(spark, JOURNEY) == _fold(spark, list(reversed(JOURNEY)))


def test_fold_lwbs_sets_departure(spark):
    rows = [JOURNEY[0], JOURNEY[1], ev("e9", "LWBS", T - timedelta(minutes=5))]
    v = _fold(spark, rows)[VID]
    assert v["lwbs_ts"] == v["departure_ts"] == T - timedelta(minutes=5)


def test_merge_sql_coalesces_every_stage():
    sql = visits_merge_sql("c.ed_silver.ed_visits", "src")
    for c in FOLD_COLUMNS:
        assert f"t.{c} = coalesce(t.{c}, s.{c})" in sql
    assert "t.last_event_ts = greatest(t.last_event_ts, s.last_event_ts)" in sql
    insert_cols = sql.split("INSERT (")[1].split(")")[0].split(", ")
    assert insert_cols == table_def("silver", "ed_visits").column_names


def test_generator_to_silver_contract_end_to_end(spark, tmp_path):
    ref = make_reference()
    start = datetime(2026, 11, 2, 18)
    plan = plan_hours(ref, start, 2)
    cfg = Config(dup_rate=0.0, late_rate=0.0, malformed_rate=0.0)
    writer = LandingWriter(str(tmp_path))
    t = start
    while t < start + timedelta(hours=72):
        events = events_in_window(plan, ref, t, t + timedelta(hours=1), cfg)
        if events:
            writer(events, t + timedelta(hours=1))
        t += timedelta(hours=1)
    raw = spark.read.schema(EVENT_SCHEMA_DDL).json(f"{tmp_path}/*/*/*/*/*.json")
    bronze = add_ingest_metadata(raw).withColumn("_rescued_data", F.lit(None).cast("string"))
    conform(bronze, table_def("bronze", "ed_events"))
    facility_ids = spark.createDataFrame([(f,) for f in ref.facilities.facility_id], "facility_id STRING")
    valid, rejected = validate_events(bronze, facility_ids)
    assert rejected.count() == 0
    visits = {r.visit_id: r for r in fold_events(deidentify(valid, SALT)).collect()}
    assert len(visits) == len(plan)
    for p in plan.itertuples(index=False):
        v = visits[p.visit_id]
        assert v.arrival_ts == p.arrival_ts.to_pydatetime()
        assert v.departure_ts == p.departure_ts.to_pydatetime()
        assert v.esi_level == p.nurse_esi
        assert (v.lwbs_ts is None) == pd.isna(p.lwbs_ts)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_silver.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.governance'`.

- [ ] **Step 3: Implement de-identification**

`src/ed_ops/governance/__init__.py`: empty file.

`src/ed_ops/governance/deid.py`:
```python
"""Deterministic de-identification of structured PHI before anything is written to silver (spec 11.2).
No Unity Catalog column masks or ABAC are used; free text is masked by ai_mask in Plan 2."""

from __future__ import annotations

import secrets

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ed_ops.config import Config

SALT_KEY = "mrn_salt"
DEID_COLUMNS = ("patient_name_redacted", "mrn_hash", "birth_year", "age_at_arrival", "sex", "phone_last4", "city", "state")


def deidentify(events: DataFrame, salt: str) -> DataFrame:
    p = F.col("payload")
    arrival = F.col("event_type") == "ARRIVAL"
    return (
        events.withColumn("patient_name_redacted", F.when(arrival & p["patient_name"].isNotNull(), F.lit("REDACTED")))
        .withColumn("mrn_hash", F.when(arrival & p["mrn"].isNotNull(), F.sha2(F.concat(p["mrn"], F.lit(salt)), 256)))
        .withColumn("birth_year", F.when(arrival, F.year(p["dob"])))
        .withColumn("age_at_arrival", F.when(arrival, F.floor(F.months_between(F.col("event_ts"), p["dob"]) / 12).cast("int")))
        .withColumn("sex", F.when(arrival, p["sex"]))
        .withColumn("phone_last4", F.when(arrival, F.substring(F.regexp_replace(p["phone"], "[^0-9]", ""), -4, 4)))
        .withColumn("city", F.when(arrival, p["address"]["city"]))
        .withColumn("state", F.when(arrival, p["address"]["state"]))
        .withColumn(
            "payload",
            F.struct(
                p["arrival_mode"].alias("arrival_mode"),
                p["esi_level"].alias("esi_level"),
                p["bed_id"].alias("bed_id"),
                p["provider_id"].alias("provider_id"),
                p["disposition"].alias("disposition"),
            ),
        )
    )


def ensure_salt(cfg: Config) -> None:
    """Create the secret scope and a random MRN salt once (idempotent)."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    if cfg.secret_scope not in {s.name for s in w.secrets.list_scopes()}:
        w.secrets.create_scope(scope=cfg.secret_scope)
    if SALT_KEY not in {k.key for k in w.secrets.list_secrets(scope=cfg.secret_scope)}:
        w.secrets.put_secret(scope=cfg.secret_scope, key=SALT_KEY, string_value=secrets.token_hex(32))


def get_salt(cfg: Config) -> str:
    from databricks.sdk.runtime import dbutils

    return dbutils.secrets.get(scope=cfg.secret_scope, key=SALT_KEY)
```

- [ ] **Step 4: Implement bronze and silver**

`src/ed_ops/pipeline/__init__.py`: empty file.

`src/ed_ops/pipeline/bronze.py`:
```python
"""Auto Loader ingestion of landing JSON into bronze.ed_events (append-only)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.config import Config
from ed_ops.events_contract import EVENT_SCHEMA_DDL


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("_ingest_ts")))
    )


def start_bronze(spark: SparkSession, cfg: Config, available_now: bool = False) -> StreamingQuery:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(EVENT_SCHEMA_DDL)
        .load(cfg.landing_path)
    )
    writer = add_ingest_metadata(stream).writeStream.option("checkpointLocation", cfg.checkpoint("bronze"))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="30 seconds")
    return writer.toTable(cfg.table("bronze", "ed_events"))
```

`src/ed_ops/pipeline/silver_visits.py`:
```python
"""bronze.ed_events -> silver.ed_visits: validate, de-identify, fold per visit, idempotent COALESCE MERGE.
Duplicates are removed within each micro-batch; across batches the MERGE is idempotent (plan deviation 1)."""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.config import Config
from ed_ops.delta import append_into
from ed_ops.events_contract import EVENT_TYPES
from ed_ops.governance.deid import DEID_COLUMNS, deidentify

REASON_CODES = (
    "unparseable", "missing_event_ts", "missing_facility_id", "missing_visit_id", "invalid_event_type",
    "invalid_esi", "unknown_facility",
)
STAGE_COLUMNS = (
    "arrival_ts", "arrival_mode", "esi_level", "triage_ts", "bed_assigned_ts", "greet_ts", "vent_start_ts",
    "vent_end_ts", "disposition", "disposition_ts", "lwbs_ts", "departure_ts",
)
FOLD_COLUMNS = STAGE_COLUMNS + DEID_COLUMNS


def validate_events(events: DataFrame, facility_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
    known = F.broadcast(facility_ids.select("facility_id").distinct().withColumn("_known_facility", F.lit(True)))
    df = events.join(known, "facility_id", "left")
    esi = F.col("payload.esi_level")
    event_type = F.col("event_type")
    reason = (
        F.when(F.col("_rescued_data").isNotNull(), F.lit("unparseable"))
        .when(F.col("event_ts").isNull(), F.lit("missing_event_ts"))
        .when(F.col("facility_id").isNull(), F.lit("missing_facility_id"))
        .when(F.col("visit_id").isNull(), F.lit("missing_visit_id"))
        .when(event_type.isNull() | ~event_type.isin(*EVENT_TYPES), F.lit("invalid_event_type"))
        .when((event_type == "TRIAGE") & (esi.isNull() | ~esi.between(1, 5)), F.lit("invalid_esi"))
        .when(F.col("_known_facility").isNull(), F.lit("unknown_facility"))
    )
    df = df.withColumn("reason_code", reason)
    valid = df.filter(F.col("reason_code").isNull()).drop("_known_facility", "reason_code")
    rejected = df.filter(F.col("reason_code").isNotNull()).drop("_known_facility")
    return valid, rejected


def quarantine_rows(rejected: DataFrame) -> DataFrame:
    return rejected.select(
        "event_id", "event_type", "event_ts", "emitted_ts", "facility_id", "visit_id", "reason_code",
        F.col("payload.esi_level").alias("esi_level"),
        F.col("_source_file").alias("source_file"),
        F.current_timestamp().alias("quarantined_at"),
        "ingest_date",
    )


def _first(event_type: str) -> F.Column:
    return F.min(F.when(F.col("event_type") == event_type, F.col("event_ts")))


def _value(event_type: str, column: str) -> F.Column:
    return F.max(F.when(F.col("event_type") == event_type, F.col(column)))


def fold_events(events: DataFrame) -> DataFrame:
    """One partial visit row per (facility_id, visit_id); expects deidentify() output."""
    return (
        events.groupBy("facility_id", "visit_id")
        .agg(
            _first("ARRIVAL").alias("arrival_ts"),
            _value("ARRIVAL", "payload.arrival_mode").alias("arrival_mode"),
            _value("TRIAGE", "payload.esi_level").alias("esi_level"),
            _first("TRIAGE").alias("triage_ts"),
            _first("BED_ASSIGNED").alias("bed_assigned_ts"),
            _first("PROVIDER_GREET").alias("greet_ts"),
            _first("VENT_START").alias("vent_start_ts"),
            _first("VENT_END").alias("vent_end_ts"),
            _value("DISPOSITION", "payload.disposition").alias("disposition"),
            _first("DISPOSITION").alias("disposition_ts"),
            _first("LWBS").alias("lwbs_ts"),
            _first("DEPARTURE").alias("_departure_ts"),
            *[F.max(c).alias(c) for c in DEID_COLUMNS],
            F.max("event_ts").alias("last_event_ts"),
        )
        .withColumn("departure_ts", F.coalesce(F.col("_departure_ts"), F.col("lwbs_ts")))
        .select("facility_id", "visit_id", *FOLD_COLUMNS, "last_event_ts")
    )


def visits_merge_sql(target: str, source_view: str) -> str:
    sets = ",\n  ".join(f"t.{c} = coalesce(t.{c}, s.{c})" for c in FOLD_COLUMNS)
    columns = ["facility_id", "visit_id", *FOLD_COLUMNS, "last_event_ts", "updated_at"]
    values = ", ".join("current_timestamp()" if c == "updated_at" else f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s\n"
        "ON t.facility_id = s.facility_id AND t.visit_id = s.visit_id\n"
        f"WHEN MATCHED THEN UPDATE SET\n  {sets},\n"
        "  t.last_event_ts = greatest(t.last_event_ts, s.last_event_ts),\n"
        "  t.updated_at = current_timestamp()\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})\n"
        f"VALUES ({values})"
    )


def process_batch(batch_df: DataFrame, cfg: Config, salt: str) -> None:
    spark = batch_df.sparkSession
    events = batch_df.dropDuplicates(["event_id"])
    facility_ids = spark.table(cfg.table("ref", "facility")).select("facility_id")
    valid, rejected = validate_events(events, facility_ids)
    if not rejected.isEmpty():
        append_into(quarantine_rows(rejected), cfg, "silver", "ed_events_quarantine")
    view = f"silver_visits_src_{uuid.uuid4().hex[:8]}"
    fold_events(deidentify(valid, salt)).createOrReplaceTempView(view)
    try:
        spark.sql(visits_merge_sql(cfg.table("silver", "ed_visits"), view))
    finally:
        spark.catalog.dropTempView(view)


def start_silver_visits(spark: SparkSession, cfg: Config, salt: str, available_now: bool = False) -> StreamingQuery:
    writer = (
        spark.readStream.table(cfg.table("bronze", "ed_events"))
        .writeStream.foreachBatch(lambda df, _batch_id: process_batch(df, cfg, salt))
        .option("checkpointLocation", cfg.checkpoint("silver_visits"))
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="30 seconds")
    return writer.start()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_silver.py -v`
Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
git add src/ed_ops/governance src/ed_ops/pipeline/__init__.py src/ed_ops/pipeline/bronze.py src/ed_ops/pipeline/silver_visits.py tests/unit/test_silver.py
git commit -m "feat: bronze Auto Loader, deterministic PHI de-identification and silver visit folding" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 15: Gold metrics job (live ticker, backfill and gap fill)

**Files:**
- Create: `src/ed_ops/pipeline/gold_metrics.py`
- Test: `tests/unit/test_gold_metrics.py`

**Interfaces:**
- Consumes:
  - `compute_snapshots`, `VISIT_COLUMNS` (Task 4)
  - `compute_baselines`, `add_drivers`, `with_null_drivers` (Task 5)
  - `board_rows` (Task 6)
  - `merge_into` (Task 7)
  - `timeutil.five_minute_marks`, `floor_to`, `utcnow`
- Produces:
  - `gold_metrics.TickFrames(current, history, board)`
  - `compute_tick_frames(visits, roster, facilities, baselines, now, tz) -> TickFrames`
  - `day_chunks(start, end) -> list[tuple[datetime, datetime]]`
  - `load_visits(spark, cfg)`, `load_roster(spark, cfg)`, `load_facilities(spark, cfg)`
  - `BaselineCache().get(spark, cfg, now) -> DataFrame`
  - `gold_tick(spark, cfg, now, cache) -> None`
  - `backfill_snapshots(spark, cfg, start, end) -> None`
  - `fill_gap(spark, cfg, now) -> None`
  - `start_gold_metrics(spark, cfg) -> StreamingQuery`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_gold_metrics.py`:
```python
from datetime import datetime, timedelta

from ed_ops.delta import conform, table_def
from ed_ops.metrics.drivers import compute_baselines
from ed_ops.pipeline.gold_metrics import compute_tick_frames, day_chunks
from tests.factories import SCENARIO_VISITS, T, facilities_df, roster_df, visits_df

BASELINE_INPUT_DDL = (
    "facility_id STRING, snapshot_ts TIMESTAMP, occupancy_ratio DOUBLE, boarder_share DOUBLE, "
    "patients_per_attending DOUBLE, arrivals_1h INT, high_acuity_share DOUBLE"
)


def test_day_chunks_cover_range_exactly():
    start, end = datetime(2026, 11, 1, 6), datetime(2026, 11, 3, 13, 25)
    chunks = day_chunks(start, end)
    assert chunks[0][0] == start and chunks[-1][1] == end
    assert all(a[1] == b[0] for a, b in zip(chunks, chunks[1:]))
    assert all(e - s <= timedelta(days=1) for s, e in chunks)
    assert day_chunks(end, end) == []


def test_tick_frames_shapes_match_gold_tables(spark):
    facilities = facilities_df(spark, [("FAC001", 20, 300), ("FAC002", 10, 100)])
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)])
    baselines = compute_baselines(spark.createDataFrame([], BASELINE_INPUT_DDL), "America/Chicago")
    frames = compute_tick_frames(visits_df(spark, SCENARIO_VISITS), roster, facilities, baselines, T, "America/Chicago")

    current = conform(frames.current, table_def("gold", "facility_current")).collect()
    assert len(current) == 2 and {r.facility_name for r in current} == {"FAC001 Hospital", "FAC002 Hospital"}
    assert all(r.snapshot_ts == T for r in current)

    history = conform(frames.history, table_def("gold", "facility_snapshot_5min")).collect()
    assert sorted({r.snapshot_ts for r in history}) == [T - timedelta(minutes=m) for m in (25, 20, 15, 10, 5, 0)]
    assert len(history) == 12

    board = conform(frames.board, table_def("gold", "ed_board_current")).collect()
    assert {r.visit_id for r in board} == {"v1", "v2", "v3", "v4", "v5", "v9"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_gold_metrics.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.pipeline.gold_metrics'`.

- [ ] **Step 3: Implement `gold_metrics.py`**

`src/ed_ops/pipeline/gold_metrics.py`:
```python
"""Gold ticker: every minute recompute facility_current, the trailing 30 minutes of 5-minute history and
the ED board from silver state; the same compute path backfills history (spec sections 7.5 and 8)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.config import Config
from ed_ops.delta import merge_into
from ed_ops.metrics.board import board_rows
from ed_ops.metrics.drivers import add_drivers, compute_baselines, with_null_drivers
from ed_ops.metrics.snapshots import VISIT_COLUMNS, compute_snapshots
from ed_ops.timeutil import five_minute_marks, floor_to, utcnow

HISTORY_RECOMPUTE = timedelta(minutes=30)
BASELINE_WINDOW = timedelta(days=14)
MAX_STAY = timedelta(days=3)


@dataclass
class TickFrames:
    current: DataFrame
    history: DataFrame
    board: DataFrame


def _materialize(df: DataFrame) -> DataFrame:
    return df.sparkSession.createDataFrame(df.collect(), df.schema)


def _times(spark: SparkSession, times: list[datetime]) -> DataFrame:
    return spark.createDataFrame([(t,) for t in times], "snapshot_ts TIMESTAMP")


def day_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    chunks, t = [], start
    while t < end:
        nxt = min(t + timedelta(days=1), end)
        chunks.append((t, nxt))
        t = nxt
    return chunks


def load_visits(spark: SparkSession, cfg: Config) -> DataFrame:
    notes = spark.table(cfg.table("silver", "triage_notes")).select(
        "facility_id", "visit_id", "suggested_esi", "undertriage_flag", "complaint_category"
    )
    return spark.table(cfg.table("silver", "ed_visits")).join(notes, ["facility_id", "visit_id"], "left").select(*VISIT_COLUMNS)


def load_roster(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.table(cfg.table("ref", "physician_roster"))


def load_facilities(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.table(cfg.table("ref", "facility"))


def compute_tick_frames(visits: DataFrame, roster: DataFrame, facilities: DataFrame, baselines: DataFrame,
                        now: datetime, tz: str) -> TickFrames:
    spark = visits.sparkSession
    marks = five_minute_marks(now - HISTORY_RECOMPUTE, now)
    snaps = compute_snapshots(visits, roster, facilities, _times(spark, sorted(set(marks) | {now})))
    snaps = _materialize(add_drivers(snaps, baselines, tz))
    current = snaps.filter(F.col("snapshot_ts") == F.lit(now)).join(
        facilities.select("facility_id", "facility_name", "facility_type", "region"), "facility_id"
    )
    history = snaps.filter(F.col("snapshot_ts").isin(marks))
    return TickFrames(current=current, history=history, board=board_rows(visits, now))


class BaselineCache:
    """Baselines change slowly; recompute at most once per hour."""

    def __init__(self) -> None:
        self._hour: datetime | None = None
        self._df: DataFrame | None = None

    def get(self, spark: SparkSession, cfg: Config, now: datetime) -> DataFrame:
        hour = floor_to(now, 3600)
        if self._df is None or self._hour != hour:
            history = spark.table(cfg.table("gold", "facility_snapshot_5min")).filter(
                F.col("snapshot_ts") > F.lit(now - BASELINE_WINDOW)
            )
            self._df = _materialize(compute_baselines(history, cfg.timezone))
            self._hour = hour
        return self._df


def gold_tick(spark: SparkSession, cfg: Config, now: datetime, cache: BaselineCache) -> None:
    visits = load_visits(spark, cfg).filter(
        (F.col("arrival_ts") > F.lit(now - MAX_STAY))
        & (F.col("departure_ts").isNull() | (F.col("departure_ts") > F.lit(now - timedelta(hours=25))))
    )
    roster = load_roster(spark, cfg).filter(F.col("shift_end") > F.lit(now - timedelta(days=2)))
    frames = compute_tick_frames(visits, roster, load_facilities(spark, cfg), cache.get(spark, cfg, now), now, cfg.timezone)
    merge_into(frames.current, cfg, "gold", "facility_current")
    merge_into(frames.history, cfg, "gold", "facility_snapshot_5min")
    merge_into(frames.board, cfg, "gold", "ed_board_current")


def backfill_snapshots(spark: SparkSession, cfg: Config, start: datetime, end: datetime) -> None:
    """Compute 5-minute snapshots in (start, end] day by day, then add drivers from the stored history."""
    visits, roster, facilities = load_visits(spark, cfg), load_roster(spark, cfg), load_facilities(spark, cfg)
    for chunk_start, chunk_end in day_chunks(start, end):
        marks = five_minute_marks(chunk_start, chunk_end)
        if not marks:
            continue
        chunk_visits = visits.filter(
            (F.col("arrival_ts") > F.lit(chunk_start - MAX_STAY)) & (F.col("arrival_ts") <= F.lit(chunk_end))
        )
        snaps = with_null_drivers(compute_snapshots(chunk_visits, roster, facilities, _times(spark, marks)))
        merge_into(snaps, cfg, "gold", "facility_snapshot_5min")
    table = spark.table(cfg.table("gold", "facility_snapshot_5min"))
    baselines = _materialize(compute_baselines(table.filter(F.col("snapshot_ts") > F.lit(end - BASELINE_WINDOW)), cfg.timezone))
    window = table.filter((F.col("snapshot_ts") > F.lit(start)) & (F.col("snapshot_ts") <= F.lit(end)))
    merge_into(add_drivers(window, baselines, cfg.timezone), cfg, "gold", "facility_snapshot_5min")


def fill_gap(spark: SparkSession, cfg: Config, now: datetime) -> None:
    """After setup or downtime, backfill history older than the ticker's 30-minute recompute window."""
    last = spark.table(cfg.table("gold", "facility_snapshot_5min")).agg(F.max("snapshot_ts")).collect()[0][0]
    gap_end = floor_to(now - HISTORY_RECOMPUTE, 300)
    if last is not None and last < gap_end:
        backfill_snapshots(spark, cfg, last, gap_end)


def start_gold_metrics(spark: SparkSession, cfg: Config) -> StreamingQuery:
    fill_gap(spark, cfg, floor_to(utcnow(), 60))
    cache = BaselineCache()

    def tick(batch_df: DataFrame, _batch_id: int) -> None:
        gold_tick(batch_df.sparkSession, cfg, floor_to(utcnow(), 60), cache)

    return (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .writeStream.foreachBatch(tick)
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", cfg.checkpoint("gold_metrics"))
        .start()
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_gold_metrics.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/pipeline/gold_metrics.py tests/unit/test_gold_metrics.py
git commit -m "feat: gold ticker with trailing-history recompute, backfill and gap fill" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 16: Job entry points, Databricks Asset Bundle and dev scripts

**Files:**
- Create: `src/ed_ops/setup.py`, `src/ed_ops/streaming.py`, `src/ed_ops/cli.py`, `databricks.yml`, `resources/jobs.yml`, `scripts/run_sql.py`, `scripts/teardown.sh`
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `setup.create_objects`, `setup.seed_reference`, `setup.backfill_generate`, `setup.backfill_ingest`, `setup.backfill_gold`
  - `streaming.run_generator`, `streaming.run_bronze`, `streaming.run_silver_visits`, `streaming.run_gold_metrics`, `streaming.ensure_roster_horizon(spark, cfg, now) -> ReferenceData`

  Every task function has the signature `(spark, cfg) -> None` (except `ensure_roster_horizon`).
  - `cli.<name>()`: one zero-argument function per console script in `pyproject.toml`.
  - Bundle jobs `ed_setup` (serverless; five chained tasks) and `ed_streaming` (continuous; four tasks on one classic cluster).

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_cli.py`:
```python
import importlib
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_every_console_script_resolves():
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert len(scripts) == 9
    for name, target in scripts.items():
        module, attr = target.split(":")
        assert callable(getattr(importlib.import_module(module), attr)), name


def test_bundle_tasks_reference_existing_entry_points():
    scripts = set(tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"])
    jobs = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text())["resources"]["jobs"]
    used = {t["python_wheel_task"]["entry_point"] for job in jobs.values() for t in job["tasks"]}
    assert used == scripts
    assert [t["task_key"] for t in jobs["ed_setup"]["tasks"]] == [
        "create_objects", "seed_reference", "backfill_generate", "backfill_ingest", "backfill_gold",
    ]
    streaming = jobs["ed_streaming"]
    assert streaming["job_clusters"][0]["new_cluster"]["spark_version"] == "18.x-scala2.13"
    assert streaming["job_clusters"][0]["new_cluster"]["data_security_mode"] == "USER_ISOLATION"
    params = jobs["ed_setup"]["tasks"][0]["python_wheel_task"]["named_parameters"]
    from dataclasses import fields

    from ed_ops.config import Config

    assert set(params) <= {f.name for f in fields(Config)}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.cli'`.

- [ ] **Step 3: Implement setup, streaming and CLI**

`src/ed_ops/setup.py`:
```python
"""ed_setup job tasks (serverless). Each task is idempotent and safe to re-run."""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import SparkSession

from ed_ops.config import Config
from ed_ops.ddl.apply import apply_ddl
from ed_ops.delta import merge_into, table_def
from ed_ops.generator.events import LandingWriter
from ed_ops.generator.facilities import build_facilities
from ed_ops.generator.population import build_patient_pool
from ed_ops.generator.reference import load_reference
from ed_ops.generator.roster import build_roster
from ed_ops.generator.stores import DeltaStore
from ed_ops.generator.tick import advance, initial_state
from ed_ops.governance.deid import ensure_salt, get_salt
from ed_ops.pipeline import gold_metrics
from ed_ops.pipeline.bronze import start_bronze
from ed_ops.pipeline.silver_visits import start_silver_visits
from ed_ops.timeutil import floor_to, utcnow


def create_objects(spark: SparkSession, cfg: Config) -> None:
    n = apply_ddl(spark, cfg)
    ensure_salt(cfg)
    print(f"applied {n} DDL statements; salt present in secret scope {cfg.secret_scope}")


def seed_reference(spark: SparkSession, cfg: Config) -> None:
    now = utcnow()
    facilities = build_facilities(spark, cfg)
    merge_into(spark.createDataFrame(facilities, table_def("ref", "facility").spark_ddl()), cfg, "ref", "facility")
    roster = build_roster(facilities, (now - timedelta(days=cfg.backfill_days + 1)).date(), cfg.backfill_days + 9, cfg.timezone)
    merge_into(spark.createDataFrame(roster, table_def("ref", "physician_roster").spark_ddl()), cfg, "ref", "physician_roster")
    merge_into(build_patient_pool(spark, cfg.patient_pool_size, cfg.seed), cfg, "sim", "patient_pool")
    print(f"seeded {len(facilities)} facilities, {len(roster)} shifts, {cfg.patient_pool_size} patients")


def backfill_generate(spark: SparkSession, cfg: Config) -> None:
    store = DeltaStore(spark, cfg)
    now = floor_to(utcnow(), 60)
    if store.load_state() is None:
        store.save_state(initial_state(now, cfg.backfill_days))
    paths = advance(store, load_reference(spark, cfg), cfg, now, live=False,
                    writer=LandingWriter(cfg.landing_path), persist_each_chunk=False)
    print(f"wrote {len(paths)} landing files up to {now}")


def backfill_ingest(spark: SparkSession, cfg: Config) -> None:
    start_bronze(spark, cfg, available_now=True).awaitTermination()
    start_silver_visits(spark, cfg, get_salt(cfg), available_now=True).awaitTermination()


def backfill_gold(spark: SparkSession, cfg: Config) -> None:
    state = DeltaStore(spark, cfg).load_state()
    if state is None:
        raise RuntimeError("generator state missing; run backfill-generate first")
    end = floor_to(state.last_emitted_ts, 300)
    gold_metrics.backfill_snapshots(spark, cfg, state.backfill_start_ts, end)
    gold_metrics.gold_tick(spark, cfg, end, gold_metrics.BaselineCache())
    print(f"gold history computed for ({state.backfill_start_ts}, {end}]")
```

`src/ed_ops/streaming.py`:
```python
"""ed_streaming job tasks (classic cluster, continuous). Each task runs one long-lived query."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from ed_ops.config import Config
from ed_ops.delta import merge_into, table_def
from ed_ops.generator.events import LandingWriter
from ed_ops.generator.reference import ReferenceData, load_reference
from ed_ops.generator.roster import build_roster
from ed_ops.generator.stores import DeltaStore
from ed_ops.generator.tick import advance
from ed_ops.governance.deid import get_salt
from ed_ops.pipeline.bronze import start_bronze
from ed_ops.pipeline.gold_metrics import start_gold_metrics
from ed_ops.pipeline.silver_visits import start_silver_visits
from ed_ops.timeutil import floor_to, utcnow

ROSTER_HORIZON = timedelta(days=2)


def ensure_roster_horizon(spark: SparkSession, cfg: Config, now: datetime) -> ReferenceData:
    ref = load_reference(spark, cfg)
    horizon = ref.roster["shift_end"].max() if not ref.roster.empty else None
    if horizon is None or pd.isna(horizon) or horizon < now + ROSTER_HORIZON:
        roster = build_roster(ref.facilities, (now - timedelta(days=1)).date(), 9, cfg.timezone)
        merge_into(spark.createDataFrame(roster, table_def("ref", "physician_roster").spark_ddl()), cfg, "ref", "physician_roster")
        ref = load_reference(spark, cfg)
    return ref


class _ReferenceCache:
    def __init__(self) -> None:
        self.ref: ReferenceData | None = None
        self.hour: datetime | None = None

    def get(self, spark: SparkSession, cfg: Config, now: datetime) -> ReferenceData:
        hour = floor_to(now, 3600)
        if self.ref is None or self.hour != hour:
            self.ref = ensure_roster_horizon(spark, cfg, now)
            self.hour = hour
        return self.ref


def run_generator(spark: SparkSession, cfg: Config) -> None:
    cache = _ReferenceCache()

    def tick(batch_df: DataFrame, _batch_id: int) -> None:
        session = batch_df.sparkSession
        now = floor_to(utcnow(), cfg.tick_seconds)
        advance(DeltaStore(session, cfg), cache.get(session, cfg, now), cfg, now, live=True,
                writer=LandingWriter(cfg.landing_path), persist_each_chunk=True)

    (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .writeStream.foreachBatch(tick)
        .trigger(processingTime=f"{cfg.tick_seconds} seconds")
        .option("checkpointLocation", cfg.checkpoint("generator"))
        .start()
        .awaitTermination()
    )


def run_bronze(spark: SparkSession, cfg: Config) -> None:
    start_bronze(spark, cfg).awaitTermination()


def run_silver_visits(spark: SparkSession, cfg: Config) -> None:
    start_silver_visits(spark, cfg, get_salt(cfg)).awaitTermination()


def run_gold_metrics(spark: SparkSession, cfg: Config) -> None:
    start_gold_metrics(spark, cfg).awaitTermination()
```

`src/ed_ops/cli.py`:
```python
"""Console-script entry points used by the bundle's python_wheel_task (named parameters -> --key=value)."""

from __future__ import annotations

import sys
from typing import Callable

from pyspark.sql import SparkSession

from ed_ops import setup, streaming
from ed_ops.config import Config, parse_config


def _run(task: Callable[[SparkSession, Config], None]) -> None:
    cfg, rest = parse_config(sys.argv[1:])
    if rest:
        raise SystemExit(f"unknown arguments: {rest}")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    task(spark, cfg)


def create_objects() -> None:
    _run(setup.create_objects)


def seed_reference() -> None:
    _run(setup.seed_reference)


def backfill_generate() -> None:
    _run(setup.backfill_generate)


def backfill_ingest() -> None:
    _run(setup.backfill_ingest)


def backfill_gold() -> None:
    _run(setup.backfill_gold)


def stream_generator() -> None:
    _run(streaming.run_generator)


def stream_bronze() -> None:
    _run(streaming.run_bronze)


def stream_silver_visits() -> None:
    _run(streaming.run_silver_visits)


def stream_gold_metrics() -> None:
    _run(streaming.run_gold_metrics)
```

- [ ] **Step 4: Write the bundle**

`databricks.yml`:
```yaml
bundle:
  name: ed-ops

include:
  - resources/*.yml

artifacts:
  ed_ops_wheel:
    type: whl
    build: uv build --wheel
    path: .

variables:
  catalog:
    description: Existing Unity Catalog catalog. The bundle never creates or drops a catalog.
    default: hls_jagjit_lb_playground_catalog
  schema_prefix:
    description: Prefix of every schema the solution creates ({prefix}bronze, {prefix}silver, ...).
    default: ed_
  facility_count:
    description: Number of simulated emergency departments.
    default: "50"
  backfill_days:
    description: Days of simulated history generated by ed_setup.
    default: "14"
  tick_seconds:
    description: Generator tick interval in seconds (multiple of 60).
    default: "60"
  timezone:
    description: Local time zone of the health system.
    default: America/Chicago
  seed:
    description: Simulator random seed.
    default: "42"
  checkpoint_version:
    description: Bump to give every stream a fresh checkpoint.
    default: "1"
  secret_scope:
    description: Secret scope that holds the MRN hashing salt.
    default: ed_ops
  streaming_node_type:
    description: Node type of the classic streaming job cluster.
    default: m5d.xlarge
  streaming_pause_status:
    description: PAUSED or UNPAUSED for the continuous ed_streaming job.
    default: PAUSED

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://fevm-hls-jagjit-lb-playground.cloud.databricks.com
    presets:
      trigger_pause_status: UNPAUSED
    variables:
      secret_scope: ed_ops_${workspace.current_user.short_name}
```

`resources/jobs.yml`:
```yaml
resources:
  jobs:
    ed_setup:
      name: ed-ops setup
      description: >-
        One-off, idempotent setup: DDL with comments and tags, reference data, patient population,
        14-day patient-journey backfill, and bronze/silver/gold history.
      max_concurrent_runs: 1
      environments:
        - environment_key: default
          spec:
            environment_version: "4"
            dependencies:
              - ../dist/*.whl
      tasks:
        - task_key: create_objects
          environment_key: default
          python_wheel_task:
            package_name: ed_ops
            entry_point: ed-ops-create-objects
            named_parameters: &common
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              facility_count: ${var.facility_count}
              backfill_days: ${var.backfill_days}
              tick_seconds: ${var.tick_seconds}
              timezone: ${var.timezone}
              seed: ${var.seed}
              checkpoint_version: ${var.checkpoint_version}
              secret_scope: ${var.secret_scope}
        - task_key: seed_reference
          depends_on: [{task_key: create_objects}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-seed-reference, named_parameters: *common}
        - task_key: backfill_generate
          depends_on: [{task_key: seed_reference}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-generate, named_parameters: *common}
        - task_key: backfill_ingest
          depends_on: [{task_key: backfill_generate}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-ingest, named_parameters: *common}
        - task_key: backfill_gold
          depends_on: [{task_key: backfill_ingest}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-gold, named_parameters: *common}

    ed_streaming:
      name: ed-ops streaming
      description: >-
        Always-on simulator and medallion streams on a classic DBR 18 LTS standard-access cluster:
        generator -> landing volume -> bronze (Auto Loader) -> silver visits -> gold crowding metrics.
      continuous:
        pause_status: ${var.streaming_pause_status}
      job_clusters:
        - job_cluster_key: streaming
          new_cluster:
            spark_version: 18.x-scala2.13
            node_type_id: ${var.streaming_node_type}
            num_workers: 1
            data_security_mode: USER_ISOLATION
            spark_conf:
              spark.sql.session.timeZone: UTC
            custom_tags:
              project: ed-ops
      tasks:
        - task_key: generator
          job_cluster_key: streaming
          libraries: [{whl: ../dist/*.whl}]
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-stream-generator, named_parameters: *common}
        - task_key: bronze_ingest
          job_cluster_key: streaming
          libraries: [{whl: ../dist/*.whl}]
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-stream-bronze, named_parameters: *common}
        - task_key: silver_visits
          job_cluster_key: streaming
          libraries: [{whl: ../dist/*.whl}]
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-stream-silver-visits, named_parameters: *common}
        - task_key: gold_metrics
          job_cluster_key: streaming
          libraries: [{whl: ../dist/*.whl}]
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-stream-gold-metrics, named_parameters: *common}
```

- [ ] **Step 5: Write the dev scripts**

`scripts/run_sql.py`:
```python
#!/usr/bin/env python3
"""Run SQL on a Databricks SQL warehouse through the Databricks CLI (dev tooling for smoke checks).

Usage: scripts/run_sql.py "SELECT 1" ["SELECT 2" ...]
Env:   DATABRICKS_PROFILE (default fevm-playground), WAREHOUSE_ID (default abef38163a09f120)
"""

import json
import os
import subprocess
import sys
import time

PROFILE = os.environ.get("DATABRICKS_PROFILE", "fevm-playground")
WAREHOUSE = os.environ.get("WAREHOUSE_ID", "abef38163a09f120")


def _api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "-p", PROFILE, "api", method, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    return json.loads(subprocess.check_output(cmd))


def run(statement: str) -> list[list]:
    r = _api("post", "/api/2.0/sql/statements",
             {"warehouse_id": WAREHOUSE, "statement": statement, "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = _api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"FAILED: {statement}\n{r['status'].get('error', {}).get('message', '')}")
    return r.get("result", {}).get("data_array", []) or []


if __name__ == "__main__":
    for stmt in sys.argv[1:]:
        print(f">> {stmt}")
        for row in run(stmt):
            print("  ", row)
```

`scripts/teardown.sh`:
```bash
#!/usr/bin/env bash
# Drop every {prefix}* schema of the solution (CASCADE: tables, volumes, checkpoints, landing files).
# Never drops the catalog. Plan 3 extends this with Lakebase synced tables, the app and the Genie space.
set -euo pipefail
PREFIX="${1:?usage: scripts/teardown.sh <schema_prefix> --yes [catalog]}"
[[ "${2:-}" == "--yes" ]] || { echo "refusing to drop ${PREFIX}* schemas without --yes" >&2; exit 2; }
CATALOG="${3:-hls_jagjit_lb_playground_catalog}"
cd "$(dirname "$0")/.."
for logical in bronze silver gold ref sim serving; do
  python3 scripts/run_sql.py "DROP SCHEMA IF EXISTS ${CATALOG}.${PREFIX}${logical} CASCADE"
done
```

Run: `chmod +x scripts/run_sql.py scripts/teardown.sh`

- [ ] **Step 6: Run the tests and validate the bundle**

Run: `scripts/pytest.sh -v`
Expected: every test passes, including `test_cli.py` (2 tests).

Run: `databricks bundle validate -t dev -p fevm-playground`
Expected: `Validation OK!`. If it reports an unknown field under `environments[].spec`, check the field name with `databricks bundle schema | python3 -c "import json,sys; d=json.load(sys.stdin); print([k for k in d['$defs'] if 'Environment' in k])"` and use whichever environment-version field that release supports.

Run:
```bash
databricks bundle validate -t dev -p fevm-playground -o json | python3 -c "import json,sys; j=json.load(sys.stdin)['resources']['jobs']; print(j['ed_streaming']['continuous'], len(j['ed_streaming']['tasks']), j['ed_setup']['tasks'][0]['python_wheel_task']['named_parameters'])"
databricks bundle validate -t dev -p fevm-playground --var streaming_pause_status=UNPAUSED -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['continuous'])"
```
Expected:
- The first command prints `{'pause_status': 'PAUSED'}`, then `4`, then the named parameters with `catalog: hls_jagjit_lb_playground_catalog` and `secret_scope: ed_ops_<your short name>`.
- The second command prints `{'pause_status': 'UNPAUSED'}`.

If development mode forces `PAUSED` in the second case, remove the `presets` block and start the job with `databricks bundle run ed_streaming` in Task 17 instead.

- [ ] **Step 7: Commit**

```bash
git add src/ed_ops/setup.py src/ed_ops/streaming.py src/ed_ops/cli.py databricks.yml resources/jobs.yml scripts/run_sql.py scripts/teardown.sh tests/unit/test_cli.py
git commit -m "feat: job entry points, asset bundle (serverless setup + classic continuous streaming) and dev scripts" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 17: Workspace smoke run, then the full 50-facility run

**Files:**
- Create: `docs/runbook-plan1.md` (operator notes: deploy, run, check, reset and tear down; Plan 3 folds it into the final runbook)

**Interfaces:**
- Consumes: the deployed bundle (Task 16).
- Produces: live `{catalog}.ed_gold.*` tables refreshed every minute. These are the inputs for Plans 2 and 3.

Each check below is a command with an expected result. If a check fails, fix the code with a failing unit test first wherever the logic can be reproduced locally, then redeploy.

- [ ] **Step 1: Smoke deploy (5 facilities, 1-day backfill, isolated `ed_smoke_` schemas)**

```bash
SMOKE="--var schema_prefix=ed_smoke_ --var facility_count=5 --var backfill_days=1 --var checkpoint_version=1"
databricks bundle deploy -t dev -p fevm-playground $SMOKE
databricks bundle run ed_setup -t dev -p fevm-playground $SMOKE
```
Expected: all five `ed_setup` tasks succeed (about 10–20 minutes on serverless).

- [ ] **Step 2: Smoke checks after setup**

```bash
C=hls_jagjit_lb_playground_catalog
scripts/run_sql.py \
  "SELECT count(*) FROM $C.ed_smoke_bronze.ed_events" \
  "SELECT reason_code, count(*) FROM $C.ed_smoke_silver.ed_events_quarantine GROUP BY 1 ORDER BY 1" \
  "SELECT count(*), count(arrival_ts), count(departure_ts), count_if(patient_name_redacted <> 'REDACTED') FROM $C.ed_smoke_silver.ed_visits" \
  "SELECT count(*), count(DISTINCT facility_id), min(snapshot_ts), max(snapshot_ts) FROM $C.ed_smoke_gold.facility_snapshot_5min" \
  "SELECT facility_id, gedwin_band, census, d2p_median_min, d2p_sample_n, nedocs_level, computed_at FROM $C.ed_smoke_gold.facility_current ORDER BY 1" \
  "SELECT count_if(size(top_drivers) > 0), count(*) FROM $C.ed_smoke_gold.facility_snapshot_5min" \
  "SELECT count_if(mrn_hash RLIKE '^MRN-'), count_if(phone_last4 RLIKE '-') FROM $C.ed_smoke_silver.ed_visits"
```
Expected:
1. Bronze count > 1,000.
2. Quarantine has rows for `missing_visit_id`, `unknown_facility` and `invalid_esi` (malformed rate 0.5% of about 2–3k events gives roughly 10–15 rows).
3. Visits: `count(arrival_ts)` ≈ `count(*)` (a few visits only have late events). `count_if(... <> 'REDACTED')` = 0.
4. Snapshots: 5 facilities and about 1,440–1,500 rows (288–300 five-minute marks each).
5. `facility_current` has 5 rows with `computed_at` from this run.
6. Some snapshots have non-empty `top_drivers`.
7. `0, 0` (no raw MRN or phone in silver).

- [ ] **Step 3: Smoke streaming for 10 minutes**

```bash
databricks bundle deploy -t dev -p fevm-playground $SMOKE --var streaming_pause_status=UNPAUSED
```
Wait about 10 minutes after the job's cluster is running, then:
```bash
scripts/run_sql.py \
  "SELECT live_started_ts, last_emitted_ts, updated_at FROM $C.ed_smoke_sim.generator_state" \
  "SELECT max(computed_at), current_timestamp() FROM $C.ed_smoke_gold.facility_current" \
  "SELECT max(snapshot_ts) FROM $C.ed_smoke_gold.facility_snapshot_5min" \
  "SELECT count_if(in_ed), count(*) FROM $C.ed_smoke_gold.ed_board_current" \
  "SELECT max(_ingest_ts) FROM $C.ed_smoke_bronze.ed_events"
```
Expected:
- `live_started_ts` is set and `last_emitted_ts` is within 2 minutes of now.
- `max(computed_at)` is within 2 minutes of `current_timestamp()`.
- The latest snapshot is within 6 minutes of now.
- The board has in-ED rows.
- Bronze ingest is within 2 minutes of now.

- [ ] **Step 4: Stop and tear down the smoke run**

```bash
databricks bundle deploy -t dev -p fevm-playground $SMOKE --var streaming_pause_status=PAUSED
scripts/teardown.sh ed_smoke_ --yes
```
Expected: the job shows as paused, and `scripts/run_sql.py "SHOW SCHEMAS IN $C LIKE 'ed_smoke_*'"` prints no rows.

- [ ] **Step 5: Full deploy and setup (50 facilities, 14 days)**

```bash
databricks bundle deploy -t dev -p fevm-playground
databricks bundle run ed_setup -t dev -p fevm-playground
```
Expected: all tasks succeed. `backfill_generate` and `backfill_gold` take longest; the whole run takes about 30–60 minutes.

- [ ] **Step 6: Full-run acceptance checks**

```bash
scripts/run_sql.py \
  "SELECT count(*), count(DISTINCT facility_id) FROM $C.ed_gold.facility_snapshot_5min" \
  "SELECT gedwin_band, round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct FROM $C.ed_gold.facility_snapshot_5min GROUP BY 1 ORDER BY 1" \
  "SELECT f.scenario_profile, round(avg(s.nedocs), 1), round(avg(s.gedwin), 2), round(avg(s.d2p_median_min), 1) FROM $C.ed_gold.facility_snapshot_5min s JOIN $C.ed_ref.facility f USING (facility_id) GROUP BY 1 ORDER BY 1" \
  "SELECT round(corr(occupancy_ratio, d2p_median_min), 2) FROM $C.ed_gold.facility_snapshot_5min WHERE d2p_sample_n >= 3" \
  "SELECT count(*), count(DISTINCT visit_id) FROM $C.ed_silver.ed_visits"
```
Expected:
1. About 201,600 rows (50 × 288 × 14, ± one day's worth) and 50 facilities.
2. Band mix: `Crowded` between 3% and 50%, and `Active` at least 30%.
3. `chronic_boarding` has the highest average NEDOCS, `high_performer` the lowest average door-to-provider median, and `understaffed` an average door-to-provider median above `normal`.
4. Correlation > 0.3. This shows crowding emerging from load feedback.
5. About 100k visits, all distinct.

**If check 2 fails:** tune `VISITS_PER_BED` in `generator/facilities.py`: lower it if too many snapshots are Crowded, raise it if too few. Then start over from a clean state:
```bash
scripts/teardown.sh ed_ --yes
databricks bundle deploy -t dev -p fevm-playground
databricks bundle run ed_setup -t dev -p fevm-playground
```

- [ ] **Step 7: Start live streaming and check freshness**

```bash
databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=UNPAUSED
```
Wait 15 minutes after the cluster is running, then run the Step 3 queries against the `ed_` schemas.
Expected: the same freshness results as Step 3. In addition, about 30 minutes after `live_started_ts`, the two live-surge facilities (the first two `surge_prone` IDs) show a rising `arrivals_1h` and `gedwin_delta_1h > 0`:
```bash
scripts/run_sql.py "SELECT c.facility_id, c.arrivals_1h, round(c.gedwin, 2), round(c.gedwin_delta_1h, 2), c.gedwin_band FROM $C.ed_gold.facility_current c JOIN $C.ed_ref.facility f USING (facility_id) WHERE f.scenario_profile = 'surge_prone' ORDER BY 1"
```

- [ ] **Step 8: Write the operator notes and commit**

`docs/runbook-plan1.md`:
```markdown
# ED Ops: core data platform runbook (Plan 1)

Workspace: https://fevm-hls-jagjit-lb-playground.cloud.databricks.com (CLI profile `fevm-playground`).
Catalog: `hls_jagjit_lb_playground_catalog` (existing; never created or dropped). Schemas: `ed_bronze`, `ed_silver`, `ed_gold`, `ed_ref`, `ed_sim`, `ed_serving`.

## Deploy and set up
    databricks bundle deploy -t dev -p fevm-playground
    databricks bundle run ed_setup -t dev -p fevm-playground        # ~30-60 min: DDL, reference data, 14-day backfill

## Start / stop live streaming (classic DBR 18 LTS cluster, costs money while running)
    databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=UNPAUSED
    databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=PAUSED

## Check freshness
    scripts/run_sql.py "SELECT max(computed_at), current_timestamp() FROM hls_jagjit_lb_playground_catalog.ed_gold.facility_current"

## Reset streams (fresh checkpoints, keep data)
    databricks bundle deploy -t dev -p fevm-playground --var checkpoint_version=2 --var streaming_pause_status=UNPAUSED

## Tear down (drops the ed_* schemas only)
    databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=PAUSED
    scripts/teardown.sh ed_ --yes
    databricks bundle destroy -t dev -p fevm-playground
```

```bash
git add docs/runbook-plan1.md src/ed_ops/generator/facilities.py
git commit -m "docs: Plan 1 operator runbook; tuned simulator volume after the full run" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```
(If `facilities.py` didn't change, commit only the runbook.)

---

## Self-Review Notes (completed by the plan author)

**Spec coverage:**

| Spec section | Where this plan covers it |
|---|---|
| §3 layout | Tasks 1–2 |
| §4 reference data | Task 10 |
| §5 events | Tasks 2 and 12 |
| §6 generator | Tasks 8–13 |
| §7 metrics | Tasks 3–6 |
| §7.5 late events | Task 15 (30-minute recompute) |
| §8 tasks | `silver_notes_ai` and `ai_retry_sweep` → Plan 2; `gold_narratives` → Plan 2; the rest → Tasks 14–16 |
| §10 tables, clustering, CDF, PK | Task 2 |
| §11.2 de-identification | Task 14 |
| §11.3–11.5 tag policies and checks | Plan 2 (Plan 1 applies tag values) |
| §12–14 | Plan 3 |
| §15 error handling | Tasks 12–16 (quarantine, idempotent MERGEs, replay-safe files, checkpoints, gap fill) |
| §16.1 unit tests | Tasks 1–16 |
| §16.3 `ed_validate` | Plan 2 (Task 17 does the equivalent manual checks for Plan 1) |
| §17 bundle and variables | Task 16 |

Plan 2 adds the remaining variables: `ai_backfill_days`, `llm_endpoint`, `warehouse_id` and the Lakebase/sync variables. The live surge offset and duration variables use their `Config` defaults; Plan 3 exposes them in the bundle.

**Deviations** (listed under Global Constraints): in-batch deduplication instead of watermark deduplication; gold-side handling instead of the `impossible_order` quarantine reason; console scripts instead of a `jobs/` directory; a dbldatagen patient population; tag policies deferred to Plan 2.
