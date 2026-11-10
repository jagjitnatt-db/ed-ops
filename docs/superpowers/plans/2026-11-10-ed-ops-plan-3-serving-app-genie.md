# ED Ops — Plan 3: Lakebase Serving, Databricks App and Genie Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the gold crowding data through Lakebase Autoscaling to a FastAPI + React (ApexCharts) Databricks App with an embedded Genie assistant, add the `gold.visit_facts` view and a Genie space with example SQL and benchmarks, and finish the customer-handover packaging (customer bundle target, `run_as`, consolidated runbook, teardown).

**Architecture:**
- **Lakebase:** the bundle declares a Lakebase Autoscaling project; its default `production` branch, `primary` endpoint and `databricks_postgres` database are created with it. A new `serving_setup` task in `ed_setup` creates the synced tables through the SDK (Continuous for gold, Snapshot for `ref.facility`), waits until they are online, and grants the app's Postgres role `SELECT` on the serving schema.
- **App:** a FastAPI backend reads only Lakebase, through a psycopg pool that refreshes its OAuth token. It proxies the Genie Conversation API with the user's forwarded token (on-behalf-of-user), falling back to the app service principal. The React app is built locally and served by FastAPI as static files.
- **Genie:** Genie validates that its tables exist, so the space is created or updated from an `ed_setup` task (`genie_space`) after the tables exist, never by `bundle deploy`.

**Tech Stack:**
- **Data:** Databricks Lakebase Autoscaling (Postgres 17), synced tables, Databricks SDK for Python 0.164.
- **Backend:** FastAPI, uvicorn, psycopg 3 with `psycopg_pool`.
- **Frontend:** React 18, Vite 6, TypeScript 5, `react-apexcharts` / ApexCharts 4, vitest.
- **Platform:** Databricks Apps, Genie, Asset Bundles.
- **Tests:** pytest and FastAPI `TestClient`.

**Spec:** `docs/superpowers/specs/2026-11-09-ed-crowding-design.md`. This plan implements:
- §10 `gold.visit_facts`;
- §12 Lakebase;
- §13 the app;
- §14 Genie;
- §17 the customer target, `run_as` and teardown;
- the remaining §18 docs.

**Builds on** Plans 1 and 2, both merged to `main`.

## Global Constraints

- **Everything in the Plan 1 and Plan 2 Global Constraints still holds**, in particular:
  - naive-UTC datetimes;
  - the existing catalog `hls_jagjit_lb_playground_catalog`, never created or dropped;
  - `{schema_prefix}{logical}` schemas;
  - regular `ed_`-prefixed tags;
  - no deletes;
  - no UC column masks;
  - the classic-cluster spec (on-demand, Photon, `r7gd.2xlarge`, `spark_version: 18.x-scala2.13`);
  - the commit trailer `Co-authored-by: Isaac <no-reply@databricks.com>`.
- **Lakebase Autoscaling:**
  - project id from bundle variable `lakebase_project_id`, default `ed-ops-${bundle.target}`;
  - branch `production`, endpoint `primary`, Postgres database `databricks_postgres`;
  - synced tables live in UC schema `{prefix}serving`, which is also their Postgres schema;
  - live tables sync `CONTINUOUS`, `ref.facility` syncs `SNAPSHOT`.
- **App connections:**
  - The app connects using only the `PG*` variables injected by its `postgres` resource, plus an OAuth token from `WorkspaceClient().postgres.generate_database_credential(endpoint=...)`.
  - The app's Postgres role is its service principal client id.
- **Genie:**
  - `serialized_space` uses version 2: `data_sources.tables[].identifier`, `instructions.text_instructions[]` and `instructions.example_question_sqls[]`, with deterministic 32-character hex ids.
  - Benchmarks are not part of `serialized_space`; they run as a separate job.
- **The app only shows de-identified gold/ref data.** Genie only sees PHI-free tables and the masked `silver.triage_notes`.
- **Frontend build:**
  - `app/frontend/dist` is git-ignored and synced to the workspace through `sync.include`.
  - Always deploy with `scripts/deploy.sh`, which builds the frontend first.
- **Colours** (one meaning each; the same in charts, badges and gauges):
  - Active `#2E7D32`, Busy `#F9A825`, Crowded `#C62828`;
  - neutral series `#1565C0`;
  - low-sample grey `#9E9E9E`.

**Deviations from spec, decided in this plan:**
1. The Genie space is created by an `ed_setup` task through the SDK, not by a bundle `genie_spaces` resource. Verified on the dev workspace: Genie rejects a space whose tables don't exist yet, and on a fresh customer deploy the tables only exist after `ed_setup`. The app finds the space by title (`GENIE_SPACE_TITLE`) rather than through a static app resource.
2. The Genie definition lives in `src/ed_ops/genie/space.yml` (inside the wheel) instead of `genie/space.json`, so `ed_setup` can render catalog-specific identifiers.
3. The Lakebase database is the default `databricks_postgres`, not a new `ed_ops` database. It exists the moment the project does, so the app's `postgres` resource can reference it on the first deploy.

## Review Focus

1. **The Lakebase OAuth token expires (one-hour lifetime) while the app is running.** New pool connections must fetch a fresh token, and a long-running app must not start failing after an hour (test in Task 4).
2. **The Genie space exists but the user has no access, or the title matches nothing.** The Genie endpoints must return a clear 503/502 JSON error, and the rest of the app must keep working (test in Task 4).
3. **A facility has no history rows, or a null gEDWIN (census 0).** Every endpoint must return valid JSON with nulls rather than crash, and the band must default to `Active` (tests in Tasks 4–5).
4. **Lakebase is unreachable or still suspended.** `/api/health` must report `unavailable` with a 503 within a bounded time, and the UI must show the data-delayed banner (tests in Tasks 4–5).
5. **Re-running `serving_setup` and `genie_space`.** Both must be idempotent: no duplicate synced tables, no second Genie space, and grants re-applied (test in Tasks 2–3).

---

## File Structure

```
src/ed_ops/config.py                     + warehouse_id, lakebase_*, sync_mode_*, app_name, genie_space_title
src/ed_ops/metadata/tables.yml           + views: gold.visit_facts; refreshed triage_notes comments
src/ed_ops/ddl/metadata.py               + ViewDef, CatalogDef.views
src/ed_ops/ddl/generate.py               + create_view_sql, view tags; views after tables
src/ed_ops/serving/__init__.py
src/ed_ops/serving/lakebase.py           synced-table specs, ensure/wait, app role, SELECT grants
src/ed_ops/serving/genie_space.py        serialized_space builder, upsert by title, CAN_RUN grant
src/ed_ops/serving/genie_benchmarks.py   run benchmark questions through Genie, compare with expected SQL
src/ed_ops/genie/__init__.py
src/ed_ops/genie/space.yml               tables, instructions, example SQL
src/ed_ops/genie/benchmarks.yml          benchmark questions + expected SQL
src/ed_ops/setup.py                      + serving_setup, genie_space tasks
src/ed_ops/cli.py, pyproject.toml        + 3 entry points (22 total), psycopg dependency
app/requirements.txt
app/backend/{__init__,settings,cache,db,repository,genie,definitions,main}.py
app/frontend/{package.json,tsconfig.json,vite.config.ts,index.html}
app/frontend/src/{main.tsx,App.tsx,api.ts,styles.css}
app/frontend/src/lib/{bands.ts,bands.test.ts}
app/frontend/src/components/{Banner,KpiTiles,RankedBar,BubbleChart,Heatmap,FacilityTable,FacilityDetail,GeniePanel,Definitions}.tsx
databricks.yml                           + variables, sync include, customer target
resources/{app.yml,lakebase.yml}         + app, Lakebase project; jobs.yml gets new tasks + ed_genie_benchmarks
scripts/deploy.sh                        build frontend, then bundle deploy
scripts/teardown.sh, scripts/teardown_serving.py   + synced tables, Genie space
tests/unit/test_{views,lakebase,genie_space,genie_benchmarks}.py, tests/app/{conftest,test_api,test_genie,test_db}.py
README.md, docs/runbook.md, docs/architecture.md
```

---

### Task 1: Config, the `gold.visit_facts` view and refreshed comments

**Files:**
- Modify: `src/ed_ops/config.py`, `src/ed_ops/ddl/metadata.py`, `src/ed_ops/ddl/generate.py`, `src/ed_ops/metadata/tables.yml`
- Test: `tests/unit/test_views.py`

**Interfaces:**
- Produces:
  - `Config` fields:
    - `warehouse_id: str = ""`
    - `lakebase_project_id: str = "ed-ops-dev"`
    - `lakebase_branch: str = "production"`
    - `lakebase_database: str = "databricks_postgres"`
    - `sync_mode_live: str = "CONTINUOUS"`
    - `sync_mode_ref: str = "SNAPSHOT"`
    - `app_name: str = ""`
    - `genie_space_title: str = "ED Ops Crowding Assistant"`
  - `ViewDef(schema, name, comment, sql, columns, tags)`; `CatalogDef.views` and `CatalogDef.view(schema, name)`
  - `generate.create_view_sql(cfg, v) -> str`, `generate.view_tag_sqls(cfg, v) -> list[str]`
  - `ddl_statements` emits views (and their tags) after all tables

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_views.py`:
```python
import pytest

from ed_ops.config import LOGICAL_SCHEMAS, Config
from ed_ops.ddl import generate
from ed_ops.ddl.metadata import load_catalog_def


def test_serving_config_defaults():
    cfg = Config()
    assert (cfg.lakebase_project_id, cfg.lakebase_branch, cfg.lakebase_database) == ("ed-ops-dev", "production", "databricks_postgres")
    assert (cfg.sync_mode_live, cfg.sync_mode_ref, cfg.genie_space_title) == ("CONTINUOUS", "SNAPSHOT", "ED Ops Crowding Assistant")


def test_visit_facts_view_metadata():
    v = load_catalog_def().view("gold", "visit_facts")
    assert v.tags["data_classification"] == "internal" and v.tags["contains_phi"] == "false" and v.tags["layer"] == "gold"
    names = [c.name for c in v.columns]
    assert names[:5] == ["facility_id", "facility_name", "facility_type", "region", "visit_id"]
    assert {"d2p_min", "los_h", "boarding_h", "left_without_being_seen", "complaint_category"} <= set(names)
    assert not ({"patient_name_redacted", "mrn_hash", "phone_last4", "city", "state", "note_masked"} & set(names))
    assert all(c.comment.strip() for c in v.columns)


def test_create_view_sql_and_ordering():
    cfg = Config()
    cat = load_catalog_def()
    sql = generate.create_view_sql(cfg, cat.view("gold", "visit_facts"))
    assert sql.startswith("CREATE OR REPLACE VIEW hls_jagjit_lb_playground_catalog.ed_gold.visit_facts (")
    assert "d2p_min COMMENT" in sql and "FROM hls_jagjit_lb_playground_catalog.ed_silver.ed_visits v" in sql
    assert "{" not in sql
    stmts = generate.ddl_statements(cfg, cat)
    last_table = max(i for i, s in enumerate(stmts) if s.startswith("CREATE TABLE"))
    view_at = next(i for i, s in enumerate(stmts) if s.startswith("CREATE OR REPLACE VIEW"))
    assert view_at > last_table
    assert any(s.startswith("ALTER VIEW hls_jagjit_lb_playground_catalog.ed_gold.visit_facts SET TAGS ('ed_layer' = 'gold'") for s in stmts)


def test_view_select_matches_declared_columns(spark):
    """Run the view's SELECT on local Spark over empty source tables: the output columns must equal the declared ones."""
    cat = load_catalog_def()
    cfg = Config(catalog="spark_catalog", schema_prefix="tv_")
    for logical in ("silver", "ref"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS tv_{logical}")
    try:
        for logical, name in (("silver", "ed_visits"), ("silver", "triage_notes"), ("ref", "facility")):
            t = cat.table(logical, name)
            spark.createDataFrame([], t.spark_ddl()).write.mode("overwrite").saveAsTable(f"tv_{logical}.{name}")
        v = cat.view("gold", "visit_facts")
        select = v.sql.format(**{l: cfg.schema(l) for l in LOGICAL_SCHEMAS})
        cols = spark.sql(select).columns
        names = [c.name for c in v.columns]
        # Derived columns come back under their expression text locally, so compare count plus the leading plain columns.
        assert len(cols) == len(names) and cols[:21] == names[:21]
    finally:
        for logical in ("silver", "ref"):
            spark.sql(f"DROP DATABASE IF EXISTS tv_{logical} CASCADE")


@pytest.mark.parametrize("column,needle", [
    ("ai_status", "skipped"), ("ai_error", "500 characters"), ("ai_attempts", "skipped"),
])
def test_triage_notes_comments_are_current(column, needle):
    assert needle in load_catalog_def().table("silver", "triage_notes").column(column).comment
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_views.py -v`
Expected:
- an `AttributeError` for `lakebase_project_id` and for `view`;
- the comment tests fail because the current comments lack `skipped` and `500 characters`.

- [ ] **Step 3: Implement**

`src/ed_ops/config.py`: add after `streaming_job_id`:
```python
    warehouse_id: str = ""
    lakebase_project_id: str = "ed-ops-dev"
    lakebase_branch: str = "production"
    lakebase_database: str = "databricks_postgres"
    sync_mode_live: str = "CONTINUOUS"
    sync_mode_ref: str = "SNAPSHOT"
    app_name: str = ""
    genie_space_title: str = "ED Ops Crowding Assistant"
```

`src/ed_ops/ddl/metadata.py`:

Add after `VolumeDef`:
```python
@dataclass(frozen=True)
class ViewDef:
    schema: str
    name: str
    comment: str
    sql: str  # SELECT with {bronze}/{silver}/{gold}/{ref}/{sim}/{serving} schema placeholders
    columns: tuple[ColumnDef, ...]
    tags: dict[str, str]
```

In `CatalogDef`, add the field `views: tuple[ViewDef, ...] = ()` after `tables`, and this method:
```python
    def view(self, schema: str, name: str) -> ViewDef:
        for v in self.views:
            if v.schema == schema and v.name == name:
                return v
        raise KeyError(f"no view {schema}.{name} in metadata")
```

In `parse_catalog_def`, before the `return`, add:
```python
    views = []
    for raw in doc.get("views", []):
        view = ViewDef(
            schema=raw["schema"],
            name=raw["name"],
            comment=raw.get("comment", ""),
            sql=raw["sql"].strip(),
            columns=tuple(ColumnDef(name=c["name"], type="", comment=c.get("comment", "")) for c in raw["columns"]),
            tags={k: _tag_value(v) for k, v in {"layer": raw["schema"], **default_tags, **raw.get("tags", {})}.items()},
        )
        where = f"{view.schema}.{view.name}"
        if view.schema not in schemas or not view.comment.strip():
            raise MetadataError(f"{where}: view needs a declared schema and a comment")
        if any(not c.comment.strip() for c in view.columns):
            raise MetadataError(f"{where}: every view column needs a comment")
        for key in REQUIRED_TABLE_TAGS:
            if key not in view.tags:
                raise MetadataError(f"{where}: required tag {key!r} missing")
        views.append(view)
```
Change the return to `return CatalogDef(schemas=schemas, volumes=volumes, tables=tuple(tables), views=tuple(views))`.

`src/ed_ops/ddl/generate.py`:

Change `from ed_ops.config import Config` to `from ed_ops.config import LOGICAL_SCHEMAS, Config`, and the metadata import to `from ed_ops.ddl.metadata import CatalogDef, TableDef, ViewDef, VolumeDef`.

Add before `ddl_statements`:
```python
def create_view_sql(cfg: Config, v: ViewDef) -> str:
    cols = ",\n  ".join(f"{c.name} COMMENT {sql_string(c.comment)}" for c in v.columns)
    select = v.sql.format(**{logical: cfg.schema(logical) for logical in LOGICAL_SCHEMAS})
    return f"CREATE OR REPLACE VIEW {cfg.table(v.schema, v.name)} (\n  {cols}\n)\nCOMMENT {sql_string(v.comment)}\nAS {select}"


def view_tag_sqls(cfg: Config, v: ViewDef) -> list[str]:
    return [f"ALTER VIEW {cfg.table(v.schema, v.name)} SET TAGS ({_tag_pairs(v.tags, cfg.tag_prefix)})"]
```

In `ddl_statements`, add before `return stmts`:
```python
    for v in cat.views:
        stmts.append(create_view_sql(cfg, v))
        stmts += view_tag_sqls(cfg, v)
```

`src/ed_ops/metadata/tables.yml`, comment replacements:
- `ai_status`: replace the comment with `"ok, error, parse_error or skipped (older than ai_backfill_days: no AI ran and no text is stored)."`
- `ai_error`: replace with `"Last AI error message; for parse_error, the first 500 characters of the model reply."`
- `ai_attempts`: replace with `"Number of AI chain attempts (1 on first processing, including skipped rows)."`

Then append to the end of the file:
```yaml

views:
  - schema: gold
    name: visit_facts
    comment: "One row per ED visit for analysis and Genie: journey timestamps, derived waits and lengths of stay, AI complaint category and suggested ESI. De-identified (no names, MRNs, phones or addresses)."
    tags: {data_classification: internal, contains_phi: "false"}
    sql: |
      SELECT
        v.facility_id, f.facility_name, f.facility_type, f.region, v.visit_id, v.arrival_ts, v.arrival_mode, v.esi_level,
        n.suggested_esi, n.undertriage_flag, n.complaint_category, n.ai_status,
        v.triage_ts, v.bed_assigned_ts, v.greet_ts, v.disposition, v.disposition_ts, v.lwbs_ts, v.departure_ts,
        v.age_at_arrival, v.sex,
        CASE WHEN v.greet_ts >= v.arrival_ts THEN (unix_timestamp(v.greet_ts) - unix_timestamp(v.arrival_ts)) / 60.0 END,
        CASE WHEN v.departure_ts >= v.arrival_ts THEN (unix_timestamp(v.departure_ts) - unix_timestamp(v.arrival_ts)) / 3600.0 END,
        CASE WHEN v.disposition = 'ADMIT' AND v.departure_ts >= v.disposition_ts
             THEN (unix_timestamp(v.departure_ts) - unix_timestamp(v.disposition_ts)) / 3600.0 END,
        v.lwbs_ts IS NOT NULL
      FROM {silver}.ed_visits v
      LEFT JOIN {silver}.triage_notes n ON n.facility_id = v.facility_id AND n.visit_id = v.visit_id
      LEFT JOIN {ref}.facility f ON f.facility_id = v.facility_id
    columns:
      - {name: facility_id, comment: "Facility identifier (FACnnn)."}
      - {name: facility_name, comment: "Facility display name."}
      - {name: facility_type, comment: "level_1_trauma, community, rural_critical_access or pediatric."}
      - {name: region, comment: "Health-system region."}
      - {name: visit_id, comment: "Visit identifier."}
      - {name: arrival_ts, comment: "Arrival (door) time, UTC."}
      - {name: arrival_mode, comment: "walk_in or ambulance."}
      - {name: esi_level, comment: "Nurse-assigned ESI 1 (most acute) to 5."}
      - {name: suggested_esi, comment: "AI-suggested ESI from the masked triage note (decision support only)."}
      - {name: undertriage_flag, comment: "True when the nurse ESI is at least 2 levels less acute than the AI-suggested ESI."}
      - {name: complaint_category, comment: "AI complaint category (cardiac, respiratory, neurological, trauma, ...)."}
      - {name: ai_status, comment: "AI enrichment status: ok, error, parse_error or skipped."}
      - {name: triage_ts, comment: "Triage time, UTC."}
      - {name: bed_assigned_ts, comment: "Treatment bed assignment time, UTC."}
      - {name: greet_ts, comment: "Provider greet time, UTC."}
      - {name: disposition, comment: "DISCHARGE, ADMIT or TRANSFER."}
      - {name: disposition_ts, comment: "Disposition decision time, UTC (admit decision for ADMIT)."}
      - {name: lwbs_ts, comment: "Left-without-being-seen time, UTC."}
      - {name: departure_ts, comment: "Time the patient left the ED, UTC (lwbs_ts for LWBS)."}
      - {name: age_at_arrival, comment: "Age in whole years at arrival."}
      - {name: sex, comment: "Recorded sex: F or M."}
      - {name: d2p_min, comment: "Door-to-provider minutes (greet_ts - arrival_ts); null if never greeted."}
      - {name: los_h, comment: "Length of stay in hours (departure_ts - arrival_ts); null while still in the ED."}
      - {name: boarding_h, comment: "Hours from admit decision to departure for admitted patients."}
      - {name: left_without_being_seen, comment: "True when the patient left without being seen."}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_views.py tests/unit/test_ddl.py -v`
Expected: all pass (7 views + 11 ddl).

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/config.py src/ed_ops/ddl src/ed_ops/metadata/tables.yml tests/unit/test_views.py
git commit -m "feat: gold.visit_facts view, view DDL support, serving config and refreshed triage_notes comments" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Lakebase synced tables and app grants

**Files:**
- Create: `src/ed_ops/serving/__init__.py`, `src/ed_ops/serving/lakebase.py`
- Modify: `pyproject.toml`; add `"psycopg[binary]>=3.2"` to `dependencies`
- Test: `tests/unit/test_lakebase.py`

**Interfaces:**
- Consumes: `Config` (Task 1); `table_def` (Plan 1).
- Produces:
  - `SYNCED_TABLES: tuple[tuple[str, str, bool], ...]`: `(logical, name, live)`
  - `branch_name(cfg)`, `endpoint_name(cfg)`
  - `synced_table_specs(cfg) -> list[tuple[str, dict]]`: `(uc_name, spec)`
  - `grant_statements(schema, role) -> list[str]`
  - `ensure_synced_tables(w, cfg) -> list[str]`: the tables it created; idempotent
  - `wait_until_online(w, cfg, timeout_s=1800, poll_s=20)`
  - `app_role(w, cfg) -> str`
  - `apply_grants(w, cfg, role)`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_lakebase.py`:
```python
from types import SimpleNamespace

import pytest

from ed_ops.config import Config
from ed_ops.serving import lakebase

CFG = Config(lakebase_project_id="ed-ops-dev")


def test_names():
    assert lakebase.branch_name(CFG) == "projects/ed-ops-dev/branches/production"
    assert lakebase.endpoint_name(CFG) == "projects/ed-ops-dev/branches/production/endpoints/primary"


def test_synced_table_specs():
    specs = dict(lakebase.synced_table_specs(CFG))
    assert set(specs) == {
        f"hls_jagjit_lb_playground_catalog.ed_serving.{n}"
        for n in ("facility_current", "facility_snapshot_5min", "ed_board_current", "facility_narrative", "network_narrative", "facility")
    }
    cur = specs["hls_jagjit_lb_playground_catalog.ed_serving.facility_current"]
    assert cur["source_table_full_name"] == "hls_jagjit_lb_playground_catalog.ed_gold.facility_current"
    assert cur["primary_key_columns"] == ["facility_id"] and cur["scheduling_policy"] == "CONTINUOUS"
    assert cur["branch"] == "projects/ed-ops-dev/branches/production" and cur["postgres_database"] == "databricks_postgres"
    assert cur["new_pipeline_spec"] == {"storage_catalog": "hls_jagjit_lb_playground_catalog", "storage_schema": "ed_serving"}
    assert specs["hls_jagjit_lb_playground_catalog.ed_serving.facility_snapshot_5min"]["primary_key_columns"] == ["facility_id", "snapshot_ts"]
    assert specs["hls_jagjit_lb_playground_catalog.ed_serving.facility"]["scheduling_policy"] == "SNAPSHOT"


def test_grant_statements_quote_the_role():
    stmts = lakebase.grant_statements("ed_serving", "5f1c-abc")
    assert stmts == [
        'GRANT USAGE ON SCHEMA "ed_serving" TO "5f1c-abc"',
        'GRANT SELECT ON ALL TABLES IN SCHEMA "ed_serving" TO "5f1c-abc"',
    ]


class FakePostgres:
    def __init__(self, existing):
        self.existing = set(existing)
        self.created = []

    def get_synced_table(self, name):
        if name.removeprefix("synced_tables/") not in self.existing:
            raise lakebase.NotFound("missing")
        return SimpleNamespace(status=SimpleNamespace(detailed_state="SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE"))

    def create_synced_table(self, synced_table, synced_table_id):
        self.created.append(synced_table_id)
        self.existing.add(synced_table_id)


def test_ensure_synced_tables_is_idempotent():
    already = "hls_jagjit_lb_playground_catalog.ed_serving.facility_current"
    w = SimpleNamespace(postgres=FakePostgres({already}))
    created = lakebase.ensure_synced_tables(w, CFG)
    assert already not in created and len(created) == 5
    assert lakebase.ensure_synced_tables(w, CFG) == []  # second run creates nothing
    lakebase.wait_until_online(w, CFG, timeout_s=1, poll_s=0)


def test_wait_until_online_times_out():
    class Stuck(FakePostgres):
        def get_synced_table(self, name):
            return SimpleNamespace(status=SimpleNamespace(detailed_state="SYNCED_TABLE_PROVISIONING_PIPELINE_RESOURCES"))

    with pytest.raises(TimeoutError, match="not online"):
        lakebase.wait_until_online(SimpleNamespace(postgres=Stuck(set())), CFG, timeout_s=0, poll_s=0)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_lakebase.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.serving'`.

- [ ] **Step 3: Implement**

`src/ed_ops/serving/__init__.py`: empty file.

`src/ed_ops/serving/lakebase.py`:
```python
"""Lakebase Autoscaling serving (spec 12): synced tables from gold/ref into Postgres and SELECT grants for the app.
The project, its production branch, primary endpoint and databricks_postgres database come from the bundle."""

from __future__ import annotations

import time

from databricks.sdk.errors import NotFound

from ed_ops.config import Config
from ed_ops.delta import table_def

# (logical schema, table, live?) - live tables sync CONTINUOUS, reference data SNAPSHOT.
SYNCED_TABLES = (
    ("gold", "facility_current", True),
    ("gold", "facility_snapshot_5min", True),
    ("gold", "ed_board_current", True),
    ("gold", "facility_narrative", True),
    ("gold", "network_narrative", True),
    ("ref", "facility", False),
)
__all__ = ["NotFound", "SYNCED_TABLES", "branch_name", "endpoint_name", "synced_table_specs", "grant_statements",
           "ensure_synced_tables", "wait_until_online", "app_role", "apply_grants"]


def branch_name(cfg: Config) -> str:
    return f"projects/{cfg.lakebase_project_id}/branches/{cfg.lakebase_branch}"


def endpoint_name(cfg: Config) -> str:
    return f"{branch_name(cfg)}/endpoints/primary"


def synced_table_specs(cfg: Config) -> list[tuple[str, dict]]:
    out = []
    for logical, name, live in SYNCED_TABLES:
        out.append((
            cfg.table("serving", name),
            {
                "branch": branch_name(cfg),
                "postgres_database": cfg.lakebase_database,
                "source_table_full_name": cfg.table(logical, name),
                "primary_key_columns": list(table_def(logical, name).primary_key),
                "scheduling_policy": cfg.sync_mode_live if live else cfg.sync_mode_ref,
                "create_database_objects_if_missing": True,
                "new_pipeline_spec": {"storage_catalog": cfg.catalog, "storage_schema": f"{cfg.schema_prefix}serving"},
            },
        ))
    return out


def grant_statements(schema: str, role: str) -> list[str]:
    return [
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"',
    ]


def ensure_synced_tables(w, cfg: Config) -> list[str]:
    from databricks.sdk.service.postgres import SyncedTable

    created = []
    for uc_name, spec in synced_table_specs(cfg):
        try:
            w.postgres.get_synced_table(name=f"synced_tables/{uc_name}")
        except NotFound:
            w.postgres.create_synced_table(synced_table=SyncedTable.from_dict({"spec": spec}), synced_table_id=uc_name)
            created.append(uc_name)
    print(f"synced tables: created {created or 'none'}")
    return created


def _state(st) -> str:
    state = st.status.detailed_state
    return str(getattr(state, "value", state))


def wait_until_online(w, cfg: Config, timeout_s: int = 1800, poll_s: int = 20) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        pending = [uc for uc, _ in synced_table_specs(cfg)
                   if "ONLINE" not in _state(w.postgres.get_synced_table(name=f"synced_tables/{uc}"))]
        if not pending:
            print("synced tables: all online")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"synced tables not online after {timeout_s}s: {pending}")
        time.sleep(poll_s)


def app_role(w, cfg: Config) -> str:
    """The app's Postgres role is its service principal client id (created by the app's postgres resource)."""
    return w.apps.get(name=cfg.app_name).service_principal_client_id


def apply_grants(w, cfg: Config, role: str) -> None:
    import psycopg

    endpoint = w.postgres.get_endpoint(name=endpoint_name(cfg))
    token = w.postgres.generate_database_credential(endpoint=endpoint_name(cfg)).token
    with psycopg.connect(host=endpoint.status.hosts.host, dbname=cfg.lakebase_database,
                         user=w.current_user.me().user_name, password=token, sslmode="require") as conn:
        for stmt in grant_statements(f"{cfg.schema_prefix}serving", role):
            conn.execute(stmt)
    print(f"granted SELECT on {cfg.schema_prefix}serving to app role {role}")
```

In `pyproject.toml`, add `"psycopg[binary]>=3.2",` to `dependencies`, and add `"databricks-sdk>=0.60"` to the `dev` extra (the tests import `databricks.sdk.errors`; on Databricks the runtime provides the SDK). Then run `uv pip install --python .venv/bin/python -e ".[dev]"`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_lakebase.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/serving pyproject.toml tests/unit/test_lakebase.py
git commit -m "feat: Lakebase synced-table specs, idempotent creation, online wait and app SELECT grants" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: Genie space definition, upsert and benchmarks

**Files:**
- Create: `src/ed_ops/genie/__init__.py`, `src/ed_ops/genie/space.yml`, `src/ed_ops/genie/benchmarks.yml`, `src/ed_ops/serving/genie_space.py`, `src/ed_ops/serving/genie_benchmarks.py`
- Test: `tests/unit/test_genie_space.py`, `tests/unit/test_genie_benchmarks.py`

**Interfaces:**
- Consumes: `Config` (Task 1); `load_catalog_def` (Plan 1); the `ddl.generate` schema mapping.
- Produces in `genie_space`:
  - `load_space_doc() -> dict`
  - `build_serialized_space(cfg, doc) -> dict`
  - `list_spaces(w) -> list[dict]`, `find_space_id(w, title) -> str | None`
  - `upsert_genie_space(w, cfg, serialized) -> str` (the space id)
  - `grant_can_run(w, space_id, application_id)`
- Produces in `genie_benchmarks`:
  - `MIN_PASS_RATE = 0.6`
  - `norm(value) -> str`, `rows_match(expected, actual) -> bool`
  - `ask_genie(w, space_id, question) -> tuple[list[list], str | None]`
  - `run_benchmarks(spark, cfg) -> dict`

- [ ] **Step 1: Write the Genie definition files**

`src/ed_ops/genie/__init__.py`: empty file.

`src/ed_ops/genie/space.yml`:
```yaml
# Genie space definition (spec 14). Identifiers and SQL use {bronze} {silver} {gold} {ref} {sim} {serving} schema
# placeholders and {timezone}; ed_setup renders them per catalog/prefix and creates or updates the space by title.
description: >-
  Ask about emergency-department crowding across the health system: current status, trends, door-to-provider waits,
  boarding, AI triage insights and AI-written explanations. All data is synthetic and de-identified.
tables:
  - gold.facility_current
  - gold.facility_snapshot_5min
  - gold.visit_facts
  - gold.facility_narrative
  - gold.network_narrative
  - silver.triage_notes
  - ref.facility
  - ref.physician_roster
instructions: |
  Glossary: census = patients in the ED now (including the waiting room). Door-to-provider (D2P) = minutes from arrival to provider greet.
  Boarding = an admitted patient still in the ED waiting for an inpatient bed. LWBS = left without being seen.
  ESI = Emergency Severity Index, 1 (most acute) to 5 (least acute).
  Crowding indices: gEDWIN is the headline score (bands: Active < 2.06, Busy 2.06-2.34, Crowded > 2.34); EDWIN bands: Active < 1.5,
  Busy 1.5-2, Crowded > 2; NEDOCS is 0-200 with levels 1 (not busy) to 6 (dangerously overcrowded).
  "Crowded" means gedwin_band = 'Crowded' unless the user explicitly asks about EDWIN or NEDOCS.
  Use facility_current for questions about now; facility_snapshot_5min for history and trends (one row per facility every 5 minutes);
  visit_facts for per-visit questions (waits, length of stay, dispositions); triage_notes for complaint categories, vitals and AI triage
  (exclude ai_status = 'skipped'); facility_narrative and network_narrative for plain-English explanations.
  All timestamps are UTC; convert with from_utc_timestamp(ts, '{timezone}') for local-time questions such as hour of day.
  top_drivers is an array of structs (driver, value, baseline, z); use explode(top_drivers) to analyse drivers.
  suggested_esi and undertriage_flag are AI decision support, not clinical decisions.
  Show facility_name rather than facility_id in answers and round scores to 2 decimals and minutes to 1 decimal.
examples:
  - question: Which facilities are crowded right now?
    sql: |
      SELECT facility_name, round(gedwin, 2) AS gedwin, census, round(d2p_median_min, 1) AS d2p_median_min, boarders_n
      FROM {gold}.facility_current WHERE gedwin_band = 'Crowded' ORDER BY gedwin DESC
  - question: What share of the last 24 hours did each facility spend crowded?
    sql: |
      SELECT f.facility_name, round(100 * avg(CASE WHEN s.gedwin_band = 'Crowded' THEN 1 ELSE 0 END), 1) AS pct_crowded_24h
      FROM {gold}.facility_snapshot_5min s JOIN {ref}.facility f ON f.facility_id = s.facility_id
      WHERE s.snapshot_ts > current_timestamp() - INTERVAL 24 HOURS
      GROUP BY f.facility_name ORDER BY pct_crowded_24h DESC
  - question: Show the hourly 90th percentile door-to-provider time for FAC001 over the last 24 hours
    sql: |
      SELECT date_trunc('hour', from_utc_timestamp(snapshot_ts, '{timezone}')) AS local_hour, round(avg(d2p_p90_min), 1) AS d2p_p90_min
      FROM {gold}.facility_snapshot_5min
      WHERE facility_id = 'FAC001' AND snapshot_ts > current_timestamp() - INTERVAL 24 HOURS
      GROUP BY 1 ORDER BY 1
  - question: Where is boarding worst right now?
    sql: |
      SELECT facility_name, boarders_n, round(longest_boarder_h, 1) AS longest_boarder_h, nedocs_level
      FROM {gold}.facility_current ORDER BY boarders_n DESC, longest_boarder_h DESC LIMIT 10
  - question: Which facilities had the most AI-flagged under-triage in the last 7 days?
    sql: |
      SELECT facility_name, count(*) AS undertriaged_visits
      FROM {gold}.visit_facts WHERE undertriage_flag AND arrival_ts > current_timestamp() - INTERVAL 7 DAYS
      GROUP BY facility_name ORDER BY undertriaged_visits DESC
  - question: Where do EDWIN and gEDWIN disagree most on the crowding ranking?
    sql: |
      WITH r AS (SELECT facility_name, rank() OVER (ORDER BY edwin DESC) AS edwin_rank, rank() OVER (ORDER BY gedwin DESC) AS gedwin_rank
                 FROM {gold}.facility_current)
      SELECT facility_name, edwin_rank, gedwin_rank, abs(edwin_rank - gedwin_rank) AS rank_gap FROM r ORDER BY rank_gap DESC LIMIT 10
  - question: What is the LWBS rate by facility type?
    sql: |
      SELECT facility_type, round(100 * avg(lwbs_rate_24h), 1) AS lwbs_pct_24h
      FROM {gold}.facility_current GROUP BY facility_type ORDER BY lwbs_pct_24h DESC
  - question: What is the busiest local hour of the day for arrivals?
    sql: |
      SELECT hour(from_utc_timestamp(arrival_ts, '{timezone}')) AS local_hour, count(*) AS arrivals
      FROM {gold}.visit_facts GROUP BY 1 ORDER BY arrivals DESC
  - question: Which facilities are deteriorating fastest in the last hour?
    sql: |
      SELECT facility_name, round(gedwin, 2) AS gedwin, round(gedwin_delta_1h, 2) AS gedwin_change_1h, gedwin_band
      FROM {gold}.facility_current WHERE gedwin_delta_1h IS NOT NULL ORDER BY gedwin_delta_1h DESC LIMIT 10
  - question: What is the complaint mix at FAC001 in the last 24 hours?
    sql: |
      SELECT complaint_category, count(*) AS notes
      FROM {silver}.triage_notes WHERE facility_id = 'FAC001' AND ai_status = 'ok' AND triage_ts > current_timestamp() - INTERVAL 24 HOURS
      GROUP BY complaint_category ORDER BY notes DESC
```

`src/ed_ops/genie/benchmarks.yml`:
```yaml
# Genie benchmark questions with expected SQL (spec 14). Answers are stable reference data or long-run aggregates,
# so live streaming does not change them between the Genie query and the expected query.
- question: How many facilities are there of each facility type?
  sql: SELECT facility_type, count(*) FROM {ref}.facility GROUP BY facility_type
- question: Which facility has the most ED beds?
  sql: SELECT facility_name, ed_beds FROM {ref}.facility ORDER BY ed_beds DESC LIMIT 1
- question: How many Level I trauma centers are in the network?
  sql: SELECT count(*) FROM {ref}.facility WHERE facility_type = 'level_1_trauma'
- question: Which region has the most inpatient beds in total?
  sql: SELECT region, sum(inpatient_beds) FROM {ref}.facility GROUP BY region ORDER BY 2 DESC LIMIT 1
- question: What is the average number of ED beds by facility type?
  sql: SELECT facility_type, round(avg(ed_beds), 1) FROM {ref}.facility GROUP BY facility_type
- question: How many facilities have the chronic_boarding scenario profile?
  sql: SELECT count(*) FROM {ref}.facility WHERE scenario_profile = 'chronic_boarding'
- question: How many distinct regions are there?
  sql: SELECT count(DISTINCT region) FROM {ref}.facility
- question: What is the facility type of FAC001?
  sql: SELECT facility_type FROM {ref}.facility WHERE facility_id = 'FAC001'
- question: What is the average number of attending physicians on the night shift at FAC001?
  sql: SELECT round(avg(attendings_on_duty), 1) FROM {ref}.physician_roster WHERE facility_id = 'FAC001' AND shift_name = 'night'
- question: Which 3 facilities have the highest average NEDOCS score across all stored history?
  sql: |
    SELECT f.facility_name FROM {gold}.facility_snapshot_5min s JOIN {ref}.facility f ON f.facility_id = s.facility_id
    GROUP BY f.facility_name ORDER BY avg(s.nedocs) DESC LIMIT 3
- question: Which facility has the lowest average door-to-provider median across all stored history?
  sql: |
    SELECT f.facility_name FROM {gold}.facility_snapshot_5min s JOIN {ref}.facility f ON f.facility_id = s.facility_id
    GROUP BY f.facility_name ORDER BY avg(s.d2p_median_min) ASC LIMIT 1
- question: How many pediatric facilities are there?
  sql: SELECT count(*) FROM {ref}.facility WHERE facility_type = 'pediatric'
```

- [ ] **Step 2: Write the failing tests**

`tests/unit/test_genie_space.py`:
```python
import json
import re
from types import SimpleNamespace

from ed_ops.config import Config
from ed_ops.ddl.metadata import load_catalog_def
from ed_ops.serving.genie_space import build_serialized_space, grant_can_run, load_space_doc, upsert_genie_space

CFG = Config(warehouse_id="wh1", genie_space_title="ED Ops Crowding Assistant (dev)")


def test_serialized_space_shape():
    s = build_serialized_space(CFG, load_space_doc())
    assert s["version"] == 2
    ids = [t["identifier"] for t in s["data_sources"]["tables"]]
    assert ids == sorted(ids) and "hls_jagjit_lb_playground_catalog.ed_gold.visit_facts" in ids
    cat = load_catalog_def()
    known = {CFG.table(t.schema, t.name) for t in cat.tables} | {CFG.table(v.schema, v.name) for v in cat.views}
    assert set(ids) <= known
    assert not any("_bronze." in i or "_sim." in i or "ed_visits" in i for i in ids)  # no raw-PHI tables
    text = s["instructions"]["text_instructions"]
    assert len(text) == 1 and "America/Chicago" in "".join(text[0]["content"])
    examples = s["instructions"]["example_question_sqls"]
    assert len(examples) == 10
    for e in examples + text:
        assert re.fullmatch(r"[0-9a-f]{32}", e["id"])
    for e in examples:
        sql = "".join(e["sql"])
        assert "{" not in sql and "hls_jagjit_lb_playground_catalog.ed_" in sql
    assert len({e["id"] for e in examples}) == 10
    assert build_serialized_space(CFG, load_space_doc()) == s  # deterministic ids


class FakeApi:
    def __init__(self, spaces):
        self.spaces = spaces
        self.calls = []

    def do(self, method, path, query=None, body=None):
        self.calls.append((method, path, body))
        if method == "GET":
            return {"spaces": self.spaces}
        if method == "POST":
            self.spaces.append({"space_id": "new123", "title": body["title"]})
            return {"space_id": "new123"}
        return {}


def test_upsert_creates_then_updates_by_title():
    api = FakeApi([{"space_id": "other", "title": "Something else"}])
    w = SimpleNamespace(api_client=api)
    serialized = build_serialized_space(CFG, load_space_doc())
    assert upsert_genie_space(w, CFG, serialized) == "new123"
    post = [c for c in api.calls if c[0] == "POST"][0]
    assert post[1] == "/api/2.0/genie/spaces" and post[2]["warehouse_id"] == "wh1"
    assert json.loads(post[2]["serialized_space"]) == serialized
    assert upsert_genie_space(w, CFG, serialized) == "new123"  # second run updates, no new space
    assert [c[0] for c in api.calls if c[0] in ("POST", "PATCH")] == ["POST", "PATCH"]
    assert [c for c in api.calls if c[0] == "PATCH"][0][1] == "/api/2.0/genie/spaces/new123"


def test_grant_can_run():
    api = FakeApi([])
    grant_can_run(SimpleNamespace(api_client=api), "sp1", "app-sp-uuid")
    method, path, body = api.calls[0]
    assert (method, path) == ("PATCH", "/api/2.0/permissions/genie/sp1")
    assert body == {"access_control_list": [{"service_principal_name": "app-sp-uuid", "permission_level": "CAN_RUN"}]}
```

`tests/unit/test_genie_benchmarks.py`:
```python
from types import SimpleNamespace

import pytest

from ed_ops.serving.genie_benchmarks import ask_genie, norm, rows_match


@pytest.mark.parametrize("value,expected", [(3, "3"), (3.0, "3"), ("3.04", "3"), (2.26, "2.3"), ("Peoria", "peoria"), (None, ""), (True, "true")])
def test_norm(value, expected):
    assert norm(value) == expected


def test_rows_match_allows_extra_columns_and_any_order():
    expected = [("community", 28), ("rural_critical_access", 12)]
    actual = [["12", "rural_critical_access", "x"], ["28.0", "community", "y"]]
    assert rows_match(expected, actual)
    assert not rows_match(expected, actual[:1])  # row count differs
    assert not rows_match([("community", 29)], [["28", "community"]])


def test_ask_genie_reads_query_attachment():
    att = SimpleNamespace(attachment_id="a1", query=SimpleNamespace(query="SELECT 1"), text=None)
    msg = SimpleNamespace(status=SimpleNamespace(value="COMPLETED"), conversation_id="c1", message_id="m1", id="m1",
                          attachments=[att], error=None)
    result = SimpleNamespace(statement_response=SimpleNamespace(result=SimpleNamespace(data_array=[["28", "community"]])))
    genie = SimpleNamespace(start_conversation_and_wait=lambda space_id, content: msg,
                            get_message_attachment_query_result=lambda **kw: result)
    rows, err = ask_genie(SimpleNamespace(genie=genie), "s1", "q")
    assert rows == [["28", "community"]] and err is None


def test_ask_genie_reports_failures():
    msg = SimpleNamespace(status=SimpleNamespace(value="FAILED"), conversation_id="c", message_id="m", id="m",
                          attachments=[], error=SimpleNamespace(error="boom"))
    genie = SimpleNamespace(start_conversation_and_wait=lambda space_id, content: msg)
    rows, err = ask_genie(SimpleNamespace(genie=genie), "s1", "q")
    assert rows == [] and "FAILED" in err
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_genie_space.py tests/unit/test_genie_benchmarks.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.serving.genie_space'`.

- [ ] **Step 4: Implement**

`src/ed_ops/serving/genie_space.py`:
```python
"""Genie space (spec 14): rendered from src/ed_ops/genie/space.yml and created or updated by title from ed_setup
(Genie rejects spaces whose tables do not exist yet, so bundle deploy cannot own it)."""

from __future__ import annotations

import json
import uuid
from importlib import resources

import yaml

from ed_ops.config import LOGICAL_SCHEMAS, Config

NAMESPACE = uuid.UUID("2b7f9a4e-91c3-4d6a-8e15-0c4f7d3a6b21")


def load_space_doc() -> dict:
    return yaml.safe_load(resources.files("ed_ops.genie").joinpath("space.yml").read_text())


def _id(*parts: str) -> str:
    return uuid.uuid5(NAMESPACE, "|".join(parts)).hex


def _render(text: str, cfg: Config) -> str:
    return text.format(**{logical: cfg.schema(logical) for logical in LOGICAL_SCHEMAS}, timezone=cfg.timezone)


def build_serialized_space(cfg: Config, doc: dict) -> dict:
    tables = sorted(cfg.table(*entry.split(".", 1)) for entry in doc["tables"])
    return {
        "version": 2,
        "data_sources": {"tables": [{"identifier": t} for t in tables]},
        "instructions": {
            "text_instructions": [{"id": _id("text", "0"), "content": [_render(doc["instructions"], cfg)]}],
            "example_question_sqls": [
                {"id": _id("example", e["question"]), "question": [e["question"]], "sql": [_render(e["sql"], cfg).strip()]}
                for e in doc["examples"]
            ],
        },
    }


def list_spaces(w) -> list[dict]:
    spaces, token = [], None
    while True:
        query = {"page_size": 100, **({"page_token": token} if token else {})}
        page = w.api_client.do("GET", "/api/2.0/genie/spaces", query=query) or {}
        spaces += page.get("spaces", [])
        token = page.get("next_page_token")
        if not token:
            return spaces


def find_space_id(w, title: str) -> str | None:
    return next((s["space_id"] for s in list_spaces(w) if s.get("title") == title), None)


def upsert_genie_space(w, cfg: Config, serialized: dict) -> str:
    body = {
        "title": cfg.genie_space_title,
        "description": load_space_doc()["description"],
        "warehouse_id": cfg.warehouse_id,
        "serialized_space": json.dumps(serialized),
    }
    space_id = find_space_id(w, cfg.genie_space_title)
    if space_id:
        w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
        print(f"genie: updated space {space_id} ({cfg.genie_space_title})")
        return space_id
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
    print(f"genie: created space {space_id} ({cfg.genie_space_title})")
    return space_id


def grant_can_run(w, space_id: str, application_id: str) -> None:
    w.api_client.do(
        "PATCH",
        f"/api/2.0/permissions/genie/{space_id}",
        body={"access_control_list": [{"service_principal_name": application_id, "permission_level": "CAN_RUN"}]},
    )
    print(f"genie: granted CAN_RUN on {space_id} to {application_id}")
```

`src/ed_ops/serving/genie_benchmarks.py`:
```python
"""Genie benchmarks (spec 14): ask each benchmark question through the Conversation API and compare Genie's result
with the expected SQL's result (values normalised, extra columns and row order tolerated)."""

from __future__ import annotations

from collections import Counter
from importlib import resources

import yaml
from pyspark.sql import SparkSession

from ed_ops.config import LOGICAL_SCHEMAS, Config
from ed_ops.serving.genie_space import find_space_id

MIN_PASS_RATE = 0.6


def norm(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    try:
        number = round(float(value), 1)
        return str(int(number)) if number == int(number) else str(number)
    except (TypeError, ValueError):
        return str(value).strip().lower()


def rows_match(expected: list[tuple], actual: list[list]) -> bool:
    if len(expected) != len(actual):
        return False
    remaining = [Counter(norm(v) for v in row) for row in actual]
    for row in expected:
        want = Counter(norm(v) for v in row)
        hit = next((i for i, have in enumerate(remaining) if not want - have), None)
        if hit is None:
            return False
        remaining.pop(hit)
    return True


def ask_genie(w, space_id: str, question: str) -> tuple[list[list], str | None]:
    msg = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
    status = str(getattr(msg.status, "value", msg.status))
    if status != "COMPLETED":
        return [], f"{status}: {getattr(msg.error, 'error', msg.error)}"
    for att in msg.attachments or []:
        if getattr(att, "query", None) is not None:
            res = w.genie.get_message_attachment_query_result(
                space_id=space_id, conversation_id=msg.conversation_id, message_id=msg.message_id or msg.id,
                attachment_id=att.attachment_id,
            )
            return list(res.statement_response.result.data_array or []), None
    return [], "no query attachment in Genie's answer"


def run_benchmarks(spark: SparkSession, cfg: Config) -> dict:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    space_id = find_space_id(w, cfg.genie_space_title)
    if not space_id:
        raise RuntimeError(f"Genie space {cfg.genie_space_title!r} not found; run ed_setup first")
    items = yaml.safe_load(resources.files("ed_ops.genie").joinpath("benchmarks.yml").read_text())
    schemas = {logical: cfg.schema(logical) for logical in LOGICAL_SCHEMAS}
    results = []
    for item in items:
        expected = [tuple(r) for r in spark.sql(item["sql"].format(**schemas)).collect()]
        actual, error = ask_genie(w, space_id, item["question"])
        passed = error is None and rows_match(expected, actual)
        results.append({"question": item["question"], "passed": passed, "error": error})
        print(f"{'PASS' if passed else 'FAIL'}  {item['question']}" + (f"  ({error})" if error else ""))
    rate = sum(r["passed"] for r in results) / len(results)
    print(f"genie benchmarks: {rate:.0%} passed ({sum(r['passed'] for r in results)}/{len(results)})")
    if rate < MIN_PASS_RATE:
        raise RuntimeError(f"Genie benchmark pass rate {rate:.0%} < {MIN_PASS_RATE:.0%}")
    return {"pass_rate": rate, "results": results}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_genie_space.py tests/unit/test_genie_benchmarks.py -v`
Expected: 13 passed (3 in `test_genie_space.py`; 10 in `test_genie_benchmarks.py`, 7 of them parametrized `test_norm` cases).

- [ ] **Step 6: Commit**

```bash
git add src/ed_ops/genie src/ed_ops/serving/genie_space.py src/ed_ops/serving/genie_benchmarks.py tests/unit/test_genie_space.py tests/unit/test_genie_benchmarks.py
git commit -m "feat: Genie space definition, idempotent upsert by title, CAN_RUN grant and benchmark runner" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: FastAPI backend

**Files:**
- Create: `app/requirements.txt`, `app/backend/__init__.py`, `app/backend/settings.py`, `app/backend/cache.py`, `app/backend/db.py`, `app/backend/repository.py`, `app/backend/genie.py`, `app/backend/definitions.py`, `app/backend/main.py`, `tests/app/__init__.py`, `tests/app/conftest.py`
- Modify: `pyproject.toml`; add `"fastapi>=0.115"`, `"httpx>=0.27"`, `"psycopg[binary,pool]>=3.2"` and `"databricks-sdk>=0.60"` to the `dev` extra
- Test: `tests/app/test_api.py`, `tests/app/test_genie.py`, `tests/app/test_db.py`

**Interfaces:**
- Produces:
  - `settings.Settings` (`schema, lakebase_endpoint, genie_space_title, local_tz, cache_seconds, stale_after_seconds`) and `load_settings()`
  - `cache.TtlCache(seconds)` with `.get(key, fn)`
  - `db.TokenProvider(endpoint, client_factory, refresh_seconds=2400)` with `.token()`, and `db.make_pool(settings)`
  - `repository.Repository` (protocol) and `LakebaseRepository(pool, schema)`
  - `genie.GenieService(space_title, client_factory)` with `.ask(token, question, conversation_id=None)` and `.poll(token, conversation_id, message_id)`
  - `definitions.DEFINITIONS`, `band(gedwin)`
  - `main.create_app(repository=None, genie=None, settings=None, static_dir=None) -> FastAPI`, and `main.app`

  **HTTP contract (consumed by Task 5):**

  | Method and path | Response |
  |---|---|
  | `GET /api/health` | `{status: ok/delayed/unavailable, data_age_seconds}`, with 503 when unavailable |
  | `GET /api/network/summary` | `{counts, facilities, census, boarders, network_d2p_median_min, as_of, narrative}` |
  | `GET /api/facilities` | `{facilities: [... + sparkline: [[ts, gedwin]]]}` |
  | `GET /api/facilities/{id}` | `{facility, narrative}`, or 404 |
  | `GET /api/facilities/{id}/history?hours=1..168` | `{points}` |
  | `GET /api/facilities/{id}/board` | `{patients}` |
  | `GET /api/heatmap` | `{cells: [{facility_id, facility_name, hour, gedwin, band}]}` |
  | `GET /api/definitions` | `DEFINITIONS` |
  | `POST /api/genie/messages {question, conversation_id?}` | `{conversation_id, message_id}` |
  | `GET /api/genie/conversations/{c}/messages/{m}` | `{done, status, text, sql, columns, rows, error}` |

- [ ] **Step 1: Write the app's requirements and the test scaffolding**

`app/requirements.txt`:
```
fastapi>=0.115
uvicorn[standard]>=0.30
psycopg[binary,pool]>=3.2
databricks-sdk>=0.60
```

`tests/app/__init__.py`: empty file.

`tests/app/conftest.py`:
```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "app"))
```

In `pyproject.toml`, add to `[project.optional-dependencies] dev`: `"fastapi>=0.115"`, `"httpx>=0.27"`, `"psycopg[binary,pool]>=3.2"`, `"databricks-sdk>=0.60"`. Then run `uv pip install --python .venv/bin/python -e ".[dev]"`.

- [ ] **Step 2: Write the failing tests**

`tests/app/test_api.py`:
```python
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from backend.main import create_app
from backend.settings import Settings

NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)


def fac(fid, band, gedwin, census, d2p=None, boarders=0):
    return {"facility_id": fid, "facility_name": f"{fid} Hospital", "facility_type": "community", "region": "Central",
            "snapshot_ts": NOW, "census": census, "gedwin": gedwin, "gedwin_band": band, "gedwin_delta_1h": 0.1,
            "edwin": 1.0, "edwin_band": "Active", "nedocs": 40.0, "nedocs_level": 2, "occupancy_ratio": 0.5,
            "d2p_mean_min": d2p, "d2p_median_min": d2p, "d2p_p90_min": d2p, "d2p_sample_n": 6, "waiting_now_n": 2,
            "longest_current_wait_min": 30.0, "boarders_n": boarders, "longest_boarder_h": 1.0, "lwbs_rate_24h": 0.02,
            "los_median_h_24h": 3.5, "attendings_on_duty": 2, "patients_per_attending": 4.0, "arrival_surge_ratio": 1.1,
            "undertriage_flags_n": 0, "top_drivers": [], "computed_at": NOW}


class FakeRepository:
    def __init__(self, age=30.0, fail=False):
        self.age, self.fail, self.calls = age, fail, 0
        self.rows = [fac("FAC001", "Crowded", 2.9, 30, 50.0, 4), fac("FAC002", "Active", None, 0, None, 0),
                     fac("FAC003", "Busy", 2.2, 12, 20.0, 1)]

    def _call(self):
        self.calls += 1
        if self.fail:
            raise ConnectionError("lakebase down")

    def facilities(self):
        self._call()
        return [dict(r) for r in self.rows]

    def facility(self, facility_id):
        self._call()
        return next((dict(r) for r in self.rows if r["facility_id"] == facility_id), None)

    def narrative(self, facility_id):
        self._call()
        return {"facility_id": facility_id, "headline": "h", "explanation": "e", "recommended_actions": ["a"]}

    def network(self):
        self._call()
        return {"network_id": "NETWORK", "headline": "17 crowded"}

    def history(self, facility_id, hours):
        self._call()
        return [] if facility_id == "FAC002" else [{"snapshot_ts": NOW - timedelta(minutes=5), "gedwin": 2.8}]

    def board(self, facility_id):
        self._call()
        return [{"visit_id": "v1", "status": "waiting", "esi_level": 3}]

    def sparklines(self, hours):
        self._call()
        return {"FAC001": [[NOW, 2.9]]}

    def heatmap(self, hours):
        self._call()
        return [{"facility_id": "FAC001", "facility_name": "FAC001 Hospital", "hour": NOW, "gedwin": 2.9},
                {"facility_id": "FAC002", "facility_name": "FAC002 Hospital", "hour": NOW, "gedwin": None}]

    def data_age_seconds(self):
        self._call()
        return self.age


class FakeGenie:
    def __init__(self):
        self.tokens = []

    def ask(self, token, question, conversation_id=None):
        self.tokens.append(token)
        return {"conversation_id": conversation_id or "c1", "message_id": "m1"}

    def poll(self, token, conversation_id, message_id):
        self.tokens.append(token)
        return {"done": True, "status": "COMPLETED", "text": "answer", "sql": "SELECT 1", "columns": ["x"], "rows": [["1"]], "error": None}


def client(repo=None, genie=None, cache_seconds=0.0):
    settings = Settings(schema="ed_serving", lakebase_endpoint="e", genie_space_title="t", local_tz="America/Chicago",
                        cache_seconds=cache_seconds, stale_after_seconds=300)
    return TestClient(create_app(repository=repo or FakeRepository(), genie=genie or FakeGenie(), settings=settings, static_dir=None))


def test_summary_counts_and_kpis():
    body = client().get("/api/network/summary").json()
    assert body["counts"] == {"Crowded": 1, "Busy": 1, "Active": 1}
    assert body["census"] == 42 and body["boarders"] == 5 and body["facilities"] == 3
    assert body["network_d2p_median_min"] == 35.0  # median of the non-null facility medians (50, 20)
    assert body["narrative"]["headline"] == "17 crowded"


def test_facilities_include_sparklines_and_nulls():
    rows = {r["facility_id"]: r for r in client().get("/api/facilities").json()["facilities"]}
    assert len(rows["FAC001"]["sparkline"]) == 1 and rows["FAC001"]["sparkline"][0][1] == 2.9
    assert rows["FAC002"]["sparkline"] == [] and rows["FAC002"]["gedwin"] is None


def test_facility_detail_404_and_history_validation():
    c = client()
    assert c.get("/api/facilities/FAC999").status_code == 404
    assert c.get("/api/facilities/FAC001").json()["narrative"]["headline"] == "h"
    assert c.get("/api/facilities/FAC002/history?hours=24").json() == {"points": []}
    assert c.get("/api/facilities/FAC001/history?hours=0").status_code == 422
    assert c.get("/api/facilities/FAC001/history?hours=169").status_code == 422
    assert c.get("/api/facilities/FAC001/board").json()["patients"][0]["visit_id"] == "v1"


def test_heatmap_bands_default_to_active_for_null():
    cells = client().get("/api/heatmap").json()["cells"]
    assert [c["band"] for c in cells] == ["Crowded", "Active"]


@pytest.mark.parametrize("age,status,code", [(30.0, "ok", 200), (900.0, "delayed", 200), (None, "delayed", 200)])
def test_health_reports_freshness(age, status, code):
    r = client(FakeRepository(age=age)).get("/api/health")
    assert (r.status_code, r.json()["status"]) == (code, status)


def test_health_unavailable_when_lakebase_down():
    r = client(FakeRepository(fail=True)).get("/api/health")
    assert r.status_code == 503 and r.json()["status"] == "unavailable"


def test_data_endpoints_return_503_json_when_lakebase_down():
    r = client(FakeRepository(fail=True)).get("/api/facilities")
    assert r.status_code == 503 and "unavailable" in r.json()["detail"]


def test_cache_avoids_repeat_queries():
    repo = FakeRepository()
    c = client(repo, cache_seconds=60)
    c.get("/api/facilities")
    first = repo.calls
    c.get("/api/facilities")
    assert repo.calls == first


def test_genie_endpoints_forward_user_token():
    genie = FakeGenie()
    c = client(genie=genie)
    r = c.post("/api/genie/messages", json={"question": "Which EDs are crowded?"}, headers={"x-forwarded-access-token": "user-tok"})
    assert r.json() == {"conversation_id": "c1", "message_id": "m1"}
    assert c.get("/api/genie/conversations/c1/messages/m1").json()["rows"] == [["1"]]
    assert genie.tokens == ["user-tok", None]


def test_genie_errors_are_json():
    class Broken(FakeGenie):
        def ask(self, token, question, conversation_id=None):
            raise LookupError("Genie space 't' not found or not shared with you")

    r = client(genie=Broken()).post("/api/genie/messages", json={"question": "q"})
    assert r.status_code == 503 and "not found" in r.json()["detail"]
    assert client().post("/api/genie/messages", json={"question": " "}).status_code == 422


def test_definitions():
    d = client().get("/api/definitions").json()
    assert d["bands"]["gedwin"] == {"busy": 2.06, "crowded": 2.34}
    assert any("natural log" in a for a in d["assumptions"])
```

`tests/app/test_genie.py`:
```python
from types import SimpleNamespace

import pytest

from backend.genie import GenieService


class FakeGenieApi:
    def start_conversation(self, space_id, content):
        return SimpleNamespace(response=SimpleNamespace(conversation_id="c1", message_id="m1"))

    def create_message(self, space_id, conversation_id, content):
        return SimpleNamespace(response=SimpleNamespace(conversation_id=conversation_id, message_id="m2", id="m2"))

    def get_message(self, space_id, conversation_id, message_id):
        if message_id == "m1":
            att = [SimpleNamespace(attachment_id="a1", text=SimpleNamespace(content="Two EDs."),
                                   query=SimpleNamespace(query="SELECT 1", description="d"))]
            return SimpleNamespace(status=SimpleNamespace(value="COMPLETED"), attachments=att, error=None)
        return SimpleNamespace(status=SimpleNamespace(value="EXECUTING_QUERY"), attachments=[], error=None)

    def get_message_attachment_query_result(self, space_id, conversation_id, message_id, attachment_id):
        cols = [SimpleNamespace(name="facility"), SimpleNamespace(name="gedwin")]
        sr = SimpleNamespace(manifest=SimpleNamespace(schema=SimpleNamespace(columns=cols)),
                             result=SimpleNamespace(data_array=[["A", "2.9"]] * 300))
        return SimpleNamespace(statement_response=sr)


def make(spaces):
    calls = []

    def factory(token):
        calls.append(token)
        api = SimpleNamespace(do=lambda method, path, query=None, body=None: {"spaces": spaces})
        return SimpleNamespace(genie=FakeGenieApi(), api_client=api)

    return GenieService("ED Ops Crowding Assistant (dev)", factory), calls


def test_ask_and_poll_with_user_token():
    svc, calls = make([{"space_id": "s1", "title": "ED Ops Crowding Assistant (dev)"}])
    assert svc.ask("tok", "q") == {"conversation_id": "c1", "message_id": "m1"}
    assert svc.ask("tok", "follow-up", conversation_id="c1") == {"conversation_id": "c1", "message_id": "m2"}
    done = svc.poll("tok", "c1", "m1")
    assert done["done"] and done["text"] == "Two EDs." and done["sql"] == "SELECT 1"
    assert done["columns"] == ["facility", "gedwin"] and len(done["rows"]) == 200  # capped
    pending = svc.poll("tok", "c1", "m2")
    assert pending["done"] is False and pending["status"] == "EXECUTING_QUERY"
    assert set(calls) == {"tok"}


def test_missing_space_raises_lookup_error():
    svc, _ = make([{"space_id": "s1", "title": "Something else"}])
    with pytest.raises(LookupError, match="not found"):
        svc.ask(None, "q")
```

`tests/app/test_db.py`:
```python
from types import SimpleNamespace

from backend.db import TokenProvider


def test_token_provider_caches_and_refreshes():
    issued = []

    def factory():
        def generate(endpoint):
            issued.append(endpoint)
            return SimpleNamespace(token=f"t{len(issued)}")

        return SimpleNamespace(postgres=SimpleNamespace(generate_database_credential=generate))

    clock = [1000.0]
    tp = TokenProvider("projects/p/branches/production/endpoints/primary", factory, refresh_seconds=2400, clock=lambda: clock[0])
    assert tp.token() == "t1" and tp.token() == "t1"
    clock[0] += 2401  # past the refresh window (tokens live one hour): a new connection gets a fresh token
    assert tp.token() == "t2"
    assert issued == ["projects/p/branches/production/endpoints/primary"] * 2
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/app -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backend'`.

- [ ] **Step 4: Implement the backend**

`app/backend/__init__.py`: empty file.

`app/backend/settings.py`:
```python
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
```

`app/backend/cache.py`:
```python
"""A tiny in-process TTL cache that protects Lakebase when many users have the app open."""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class TtlCache:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.seconds, self.clock = seconds, clock
        self._items: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, fn: Callable[[], Any]) -> Any:
        if self.seconds <= 0:
            return fn()
        now = self.clock()
        with self._lock:
            hit = self._items.get(key)
            if hit and now - hit[0] < self.seconds:
                return hit[1]
        value = fn()
        with self._lock:
            self._items[key] = (now, value)
        return value
```

`app/backend/db.py`:
```python
"""Lakebase connection pool. Host, port, database, user and SSL mode come from the PG* variables the app's postgres
resource injects; the password is an OAuth token refreshed well before its one-hour expiry, so every new pool
connection authenticates with a valid token."""

from __future__ import annotations

import threading
import time
from typing import Callable

from backend.settings import Settings


class TokenProvider:
    def __init__(self, endpoint: str, client_factory: Callable[[], object], refresh_seconds: int = 2400,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.endpoint, self.client_factory, self.refresh_seconds, self.clock = endpoint, client_factory, refresh_seconds, clock
        self._token: str | None = None
        self._issued = 0.0
        self._lock = threading.Lock()

    def token(self) -> str:
        with self._lock:
            if self._token is None or self.clock() - self._issued >= self.refresh_seconds:
                cred = self.client_factory().postgres.generate_database_credential(endpoint=self.endpoint)
                self._token, self._issued = cred.token, self.clock()
            return self._token


def make_pool(settings: Settings):
    import psycopg
    from databricks.sdk import WorkspaceClient
    from psycopg_pool import ConnectionPool

    provider = TokenProvider(settings.lakebase_endpoint, WorkspaceClient)

    class OAuthConnection(psycopg.Connection):
        @classmethod
        def connect(cls, conninfo: str = "", **kwargs):
            kwargs["password"] = provider.token()
            return super().connect(conninfo, **kwargs)

    return ConnectionPool(conninfo="", connection_class=OAuthConnection, min_size=1, max_size=8,
                          max_lifetime=2400, timeout=10, kwargs={"connect_timeout": 10}, open=True)
```

`app/backend/repository.py`:
```python
"""Read-only queries against the Lakebase synced tables (Postgres schema = the UC serving schema)."""

from __future__ import annotations

from typing import Protocol

FACILITY_COLUMNS = (
    "facility_id, facility_name, facility_type, region, snapshot_ts, census, gedwin, gedwin_band, gedwin_delta_1h, "
    "edwin, edwin_band, nedocs, nedocs_level, occupancy_ratio, d2p_mean_min, d2p_median_min, d2p_p90_min, d2p_sample_n, "
    "waiting_now_n, longest_current_wait_min, boarders_n, longest_boarder_h, lwbs_rate_24h, los_median_h_24h, "
    "attendings_on_duty, patients_per_attending, arrival_surge_ratio, undertriage_flags_n, "
    "to_jsonb(top_drivers) AS top_drivers, computed_at"
)


class Repository(Protocol):
    def facilities(self) -> list[dict]: ...
    def facility(self, facility_id: str) -> dict | None: ...
    def narrative(self, facility_id: str) -> dict | None: ...
    def network(self) -> dict | None: ...
    def history(self, facility_id: str, hours: int) -> list[dict]: ...
    def board(self, facility_id: str) -> list[dict]: ...
    def sparklines(self, hours: int) -> dict[str, list]: ...
    def heatmap(self, hours: int) -> list[dict]: ...
    def data_age_seconds(self) -> float | None: ...


class LakebaseRepository:
    def __init__(self, pool, schema: str) -> None:
        self.pool = pool
        self.s = '"' + schema.replace('"', '""') + '"'

    def _all(self, query: str, params: tuple = ()) -> list[dict]:
        from psycopg.rows import dict_row

        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return list(cur.fetchall())

    def _one(self, query: str, params: tuple = ()) -> dict | None:
        rows = self._all(query, params)
        return rows[0] if rows else None

    def facilities(self) -> list[dict]:
        return self._all(f"SELECT {FACILITY_COLUMNS} FROM {self.s}.facility_current ORDER BY gedwin DESC NULLS LAST")

    def facility(self, facility_id: str) -> dict | None:
        return self._one(
            # Subquery first so FACILITY_COLUMNS stays unqualified without clashing with ref columns of the same name.
            f"SELECT c.*, f.ed_beds, f.inpatient_beds, f.city, f.state "
            f"FROM (SELECT {FACILITY_COLUMNS} FROM {self.s}.facility_current WHERE facility_id = %s) c "
            f"JOIN {self.s}.facility f ON f.facility_id = c.facility_id",
            (facility_id,),
        )

    def narrative(self, facility_id: str) -> dict | None:
        return self._one(
            f"SELECT facility_id, snapshot_ts, gedwin_band, headline, explanation, to_jsonb(recommended_actions) AS recommended_actions, "
            f"to_jsonb(top_drivers) AS top_drivers, generated_at FROM {self.s}.facility_narrative WHERE facility_id = %s",
            (facility_id,),
        )

    def network(self) -> dict | None:
        return self._one(
            "SELECT snapshot_ts, crowded_n, busy_n, active_n, headline, explanation, to_jsonb(crowded_facilities) AS crowded_facilities, "
            "to_jsonb(doing_well_facilities) AS doing_well_facilities, to_jsonb(recommended_actions) AS recommended_actions, "
            f"generated_at FROM {self.s}.network_narrative WHERE network_id = 'NETWORK'"
        )

    def history(self, facility_id: str, hours: int) -> list[dict]:
        return self._all(
            "SELECT snapshot_ts, gedwin, edwin, nedocs, d2p_median_min, d2p_p90_min, d2p_sample_n, census, n_esi1, n_esi2, "
            "n_esi3, n_esi4, n_esi5, n_untriaged, boarders_n, occupancy_ratio "
            f"FROM {self.s}.facility_snapshot_5min WHERE facility_id = %s AND snapshot_ts > now() - make_interval(hours => %s) "
            "ORDER BY snapshot_ts",
            (facility_id, hours),
        )

    def board(self, facility_id: str) -> list[dict]:
        return self._all(
            "SELECT visit_id, status, esi_level, suggested_esi, undertriage_flag, complaint_category, arrival_ts, minutes_in_ed, "
            f"minutes_waiting, arrival_mode, on_vent FROM {self.s}.ed_board_current WHERE facility_id = %s AND in_ed "
            "ORDER BY esi_level NULLS LAST, minutes_waiting DESC",
            (facility_id,),
        )

    def sparklines(self, hours: int) -> dict[str, list]:
        out: dict[str, list] = {}
        for r in self._all(
            f"SELECT facility_id, snapshot_ts, gedwin FROM {self.s}.facility_snapshot_5min "
            "WHERE snapshot_ts > now() - make_interval(hours => %s) ORDER BY facility_id, snapshot_ts",
            (hours,),
        ):
            out.setdefault(r["facility_id"], []).append([r["snapshot_ts"], r["gedwin"]])
        return out

    def heatmap(self, hours: int) -> list[dict]:
        return self._all(
            "SELECT s.facility_id, f.facility_name, date_trunc('hour', s.snapshot_ts) AS hour, max(s.gedwin) AS gedwin "
            f"FROM {self.s}.facility_snapshot_5min s JOIN {self.s}.facility f USING (facility_id) "
            "WHERE s.snapshot_ts > now() - make_interval(hours => %s) GROUP BY 1, 2, 3 ORDER BY 2, 3",
            (hours,),
        )

    def data_age_seconds(self) -> float | None:
        row = self._one(f"SELECT EXTRACT(EPOCH FROM now() - max(computed_at))::float AS age FROM {self.s}.facility_current")
        return row["age"] if row else None
```

`app/backend/genie.py`:
```python
"""Genie Conversation API proxy. Calls run with the user's forwarded token (on-behalf-of-user) when present, so the
user's Unity Catalog permissions apply; otherwise with the app service principal. The space is found by title."""

from __future__ import annotations

from typing import Callable

MAX_ROWS = 200
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}


class GenieService:
    def __init__(self, space_title: str, client_factory: Callable[[str | None], object]) -> None:
        self.space_title, self.client_factory = space_title, client_factory
        self._space_id: str | None = None

    def _space(self, w) -> str:
        if self._space_id:
            return self._space_id
        token, spaces = None, []
        while True:
            page = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 100, **({"page_token": token} if token else {})}) or {}
            spaces += page.get("spaces", [])
            token = page.get("next_page_token")
            if not token:
                break
        match = next((s["space_id"] for s in spaces if s.get("title") == self.space_title), None)
        if not match:
            raise LookupError(f"Genie space {self.space_title!r} not found or not shared with you")
        self._space_id = match
        return match

    def ask(self, token: str | None, question: str, conversation_id: str | None = None) -> dict:
        w = self.client_factory(token)
        space = self._space(w)
        if conversation_id:
            r = w.genie.create_message(space_id=space, conversation_id=conversation_id, content=question).response
            return {"conversation_id": conversation_id, "message_id": getattr(r, "message_id", None) or r.id}
        r = w.genie.start_conversation(space_id=space, content=question).response
        return {"conversation_id": r.conversation_id, "message_id": r.message_id}

    def poll(self, token: str | None, conversation_id: str, message_id: str) -> dict:
        w = self.client_factory(token)
        space = self._space(w)
        msg = w.genie.get_message(space_id=space, conversation_id=conversation_id, message_id=message_id)
        status = str(getattr(msg.status, "value", msg.status))
        out = {"done": status in TERMINAL, "status": status, "text": None, "sql": None, "columns": [], "rows": [],
               "error": str(getattr(msg.error, "error", msg.error)) if msg.error else None}
        for att in msg.attachments or []:
            if getattr(att, "text", None) is not None and out["text"] is None:
                out["text"] = att.text.content
            if getattr(att, "query", None) is not None and status == "COMPLETED":
                out["sql"] = att.query.query
                out["text"] = out["text"] or att.query.description
                res = w.genie.get_message_attachment_query_result(
                    space_id=space, conversation_id=conversation_id, message_id=message_id, attachment_id=att.attachment_id
                ).statement_response
                out["columns"] = [c.name for c in res.manifest.schema.columns]
                out["rows"] = list(res.result.data_array or [])[:MAX_ROWS]
        return out


def workspace_client(token: str | None):
    from databricks.sdk import WorkspaceClient

    if token:
        import os

        return WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=token, auth_type="pat")
    return WorkspaceClient()
```

`app/backend/definitions.py`:
```python
"""Metric definitions, bands, assumptions and citations shown on the Definitions page (spec 13.2)."""

GEDWIN_BUSY, GEDWIN_CROWDED = 2.06, 2.34


def band(gedwin: float | None) -> str:
    if gedwin is None:
        return "Active"  # census 0
    return "Active" if gedwin < GEDWIN_BUSY else "Busy" if gedwin <= GEDWIN_CROWDED else "Crowded"


DEFINITIONS = {
    "bands": {
        "gedwin": {"busy": GEDWIN_BUSY, "crowded": GEDWIN_CROWDED},
        "edwin": {"busy": 1.5, "crowded": 2.0},
        "nedocs": {"levels": [20, 60, 100, 140, 180],
                   "labels": ["Not busy", "Busy", "Extremely busy", "Overcrowded", "Severely overcrowded", "Dangerously overcrowded"]},
    },
    "formulas": {
        "occupancy": "OR = census / ED beds; OR_c = OR clamped to [0.01, 0.99]",
        "acuity": "AR = sum(n_i * t_i) / census, t_i = 6 - ESI, untriaged = 3",
        "edwin": "EDWIN = (1 / attendings on duty) * OR_c / (1 - OR_c) * AR",
        "gedwin": "gEDWIN = ln(OR_c / (1 - OR_c)) + ln(AR)",
        "nedocs": "NEDOCS = -20 + 85.8*(census/ED beds) + 600*(boarders/inpatient beds) + 13.4*ventilators "
                  "+ 0.93*longest boarder hours + 5.64*last door-to-bed hours, clamped 0-200",
        "door_to_provider": "Minutes from arrival to provider greet, over patients greeted in the last 60 minutes",
    },
    "assumptions": [
        "gEDWIN uses the natural log (the paper does not state the base; its analysis used R, where log() is natural).",
        "Untriaged patients are weighted as ESI 3 (t = 3).",
        "Occupancy is floored at 0.01 (the paper only caps at 0.99).",
        "Door-to-provider uses a 1-hour window; LWBS, length of stay and boarding use 24-hour windows.",
        "All facilities share one time zone; timestamps are stored in UTC.",
        "Door-to-provider statistics are marked low-sample when fewer than 5 patients were greeted.",
        "AI-suggested ESI is decision support for the demo, not clinical advice. All data is synthetic.",
    ],
    "citations": [
        "gEDWIN: a simple and practical index for real-time monitoring of emergency department crowding. BMC Emergency Medicine, 2025.",
        "Weiss SJ et al. Estimating the degree of emergency department overcrowding in academic medical centers (NEDOCS). Acad Emerg Med, 2004.",
    ],
}
```

`app/backend/main.py`:
```python
"""FastAPI backend: reads only Lakebase, proxies Genie, serves the built React app (spec 13)."""

from __future__ import annotations

import statistics
from pathlib import Path
from typing import Callable

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator

from backend.cache import TtlCache
from backend.definitions import DEFINITIONS, band
from backend.settings import Settings, load_settings

DEFAULT_STATIC = Path(__file__).resolve().parents[1] / "frontend" / "dist"


class AskBody(BaseModel):
    question: str
    conversation_id: str | None = None

    @field_validator("question")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("question must not be blank")
        return v.strip()


def _lazy(factory: Callable[[], object]) -> Callable[[], object]:
    box: list = []

    def get():
        if not box:
            box.append(factory())
        return box[0]

    return get


def create_app(repository=None, genie=None, settings: Settings | None = None, static_dir: Path | None = DEFAULT_STATIC) -> FastAPI:
    settings = settings or load_settings()
    cache = TtlCache(settings.cache_seconds)

    def build_repo():
        from backend.db import make_pool
        from backend.repository import LakebaseRepository

        return LakebaseRepository(make_pool(settings), settings.schema)

    def build_genie():
        from backend.genie import GenieService, workspace_client

        return GenieService(settings.genie_space_title, workspace_client)

    repo = (lambda: repository) if repository is not None else _lazy(build_repo)
    genie_service = (lambda: genie) if genie is not None else _lazy(build_genie)
    app = FastAPI(title="ED Ops Crowding Command Center")

    def data(key: str, fn: Callable[[], object]):
        try:
            return cache.get(key, fn)
        except Exception as exc:  # Lakebase unreachable, suspended or misconfigured
            raise HTTPException(status_code=503, detail=f"data unavailable: {type(exc).__name__}") from exc

    @app.get("/api/health")
    def health():
        try:
            age = repo().data_age_seconds()
        except Exception:
            return JSONResponse({"status": "unavailable", "data_age_seconds": None}, status_code=503)
        status = "ok" if age is not None and age <= settings.stale_after_seconds else "delayed"
        return {"status": status, "data_age_seconds": age}

    @app.get("/api/network/summary")
    def summary():
        def build():
            rows = repo().facilities()
            counts = {"Crowded": 0, "Busy": 0, "Active": 0}
            for r in rows:
                counts[r.get("gedwin_band") or band(r.get("gedwin"))] += 1
            medians = [r["d2p_median_min"] for r in rows if r.get("d2p_median_min") is not None]
            return {
                "counts": counts,
                "facilities": len(rows),
                "census": sum(r.get("census") or 0 for r in rows),
                "boarders": sum(r.get("boarders_n") or 0 for r in rows),
                "network_d2p_median_min": statistics.median(medians) if medians else None,
                "as_of": max((r["snapshot_ts"] for r in rows), default=None),
                "narrative": repo().network(),
            }

        return data("summary", build)

    @app.get("/api/facilities")
    def facilities():
        def build():
            rows, lines = repo().facilities(), repo().sparklines(6)
            for r in rows:
                r["sparkline"] = lines.get(r["facility_id"], [])
            return {"facilities": rows}

        return data("facilities", build)

    @app.get("/api/facilities/{facility_id}")
    def facility(facility_id: str):
        row = data(f"facility:{facility_id}", lambda: repo().facility(facility_id))
        if row is None:
            raise HTTPException(status_code=404, detail=f"unknown facility {facility_id}")
        return {"facility": row, "narrative": data(f"narrative:{facility_id}", lambda: repo().narrative(facility_id))}

    @app.get("/api/facilities/{facility_id}/history")
    def history(facility_id: str, hours: int = Query(24, ge=1, le=168)):
        return {"points": data(f"history:{facility_id}:{hours}", lambda: repo().history(facility_id, hours))}

    @app.get("/api/facilities/{facility_id}/board")
    def board(facility_id: str):
        return {"patients": data(f"board:{facility_id}", lambda: repo().board(facility_id))}

    @app.get("/api/heatmap")
    def heatmap():
        def build():
            return {"cells": [{**c, "band": band(c.get("gedwin"))} for c in repo().heatmap(24)]}

        return data("heatmap", build)

    @app.get("/api/definitions")
    def definitions():
        return DEFINITIONS

    @app.post("/api/genie/messages")
    def ask(body: AskBody, request: Request):
        try:
            return genie_service().ask(request.headers.get("x-forwarded-access-token"), body.question, body.conversation_id)
        except LookupError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Genie request failed: {exc}") from exc

    @app.get("/api/genie/conversations/{conversation_id}/messages/{message_id}")
    def poll(conversation_id: str, message_id: str, request: Request):
        try:
            return genie_service().poll(request.headers.get("x-forwarded-access-token"), conversation_id, message_id)
        except LookupError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Genie request failed: {exc}") from exc

    if static_dir is not None and (static_dir / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str):
            if path.startswith("api/"):
                raise HTTPException(status_code=404)
            return FileResponse(static_dir / "index.html")

    return app


app = create_app()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/app -v`
Expected: 16 passed (13 api including 3 parametrized health cases, 2 genie, 1 db).

- [ ] **Step 6: Commit**

```bash
git add app/requirements.txt app/backend tests/app pyproject.toml
git commit -m "feat: FastAPI backend over Lakebase with token-refreshing pool, TTL cache and Genie OBO proxy" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: React frontend (Vite + TypeScript + ApexCharts)

**Files:**
- Create:
  - `app/frontend/package.json`, `app/frontend/tsconfig.json`, `app/frontend/vite.config.ts`, `app/frontend/index.html`
  - `app/frontend/src/main.tsx`, `app/frontend/src/App.tsx`, `app/frontend/src/api.ts`, `app/frontend/src/styles.css`
  - `app/frontend/src/lib/bands.ts`
  - `app/frontend/src/components/Banner.tsx`, `KpiTiles.tsx`, `RankedBar.tsx`, `BubbleChart.tsx`, `Heatmap.tsx`, `FacilityTable.tsx`, `FacilityDetail.tsx`, `GeniePanel.tsx`, `Definitions.tsx`
- Test: `app/frontend/src/lib/bands.test.ts`

**Interfaces:**
- Consumes: the HTTP contract from Task 4.
- Produces: `app/frontend/dist/` (`index.html` + `assets/`), served by FastAPI.
- Exported from `bands.ts`:
  - `Band`, `BAND_COLORS`, `NEUTRAL`, `LOW_SAMPLE`
  - `gedwinBand`, `edwinBand`, `nedocsLabel`
  - `isLowSample`, `fmt`, `pct`, `trendArrow`, `localHour`

- [ ] **Step 1: Write the project files and the failing unit tests**

`app/frontend/package.json`:
```json
{
  "name": "ed-ops-frontend",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit && vite build",
    "test": "vitest run"
  },
  "dependencies": {
    "apexcharts": "^4.5.0",
    "react": "^18.3.1",
    "react-apexcharts": "^1.7.0",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@types/react": "^18.3.12",
    "@types/react-dom": "^18.3.1",
    "@vitejs/plugin-react": "^4.3.4",
    "typescript": "^5.6.3",
    "vite": "^6.0.0",
    "vitest": "^2.1.8"
  }
}
```

`app/frontend/tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "skipLibCheck": true,
    "noEmit": true,
    "isolatedModules": true,
    "esModuleInterop": true
  },
  "include": ["src"]
}
```

`app/frontend/vite.config.ts`:
```ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/api": "http://localhost:8000" } },
  build: { outDir: "dist", emptyOutDir: true },
});
```

`app/frontend/index.html`:
```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>ED Ops Crowding Command Center</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`app/frontend/src/lib/bands.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import { edwinBand, fmt, gedwinBand, isLowSample, localHour, nedocsLabel, pct, trendArrow } from "./bands";

describe("bands", () => {
  it("classifies gEDWIN with the paper's cut-offs and treats null (empty ED) as Active", () => {
    expect([gedwinBand(2.05), gedwinBand(2.06), gedwinBand(2.34), gedwinBand(2.35), gedwinBand(null)]).toEqual([
      "Active", "Busy", "Busy", "Crowded", "Active",
    ]);
  });
  it("classifies EDWIN", () => {
    expect([edwinBand(1.49), edwinBand(1.5), edwinBand(2), edwinBand(2.01), edwinBand(null)]).toEqual([
      "Active", "Busy", "Busy", "Crowded", null,
    ]);
  });
  it("labels NEDOCS levels", () => {
    expect(nedocsLabel(1)).toBe("Not busy");
    expect(nedocsLabel(6)).toBe("Dangerously overcrowded");
    expect(nedocsLabel(null)).toBe("—");
  });
  it("flags low samples and formats values", () => {
    expect([isLowSample(4), isLowSample(5), isLowSample(null)]).toEqual([true, false, true]);
    expect([fmt(null), fmt(2.345, 2), fmt(45, 0, " min")]).toEqual(["—", "2.35", "45 min"]);
    expect(pct(0.034)).toBe("3%");
    expect([trendArrow(0.2), trendArrow(-0.2), trendArrow(0.01), trendArrow(null)]).toEqual(["▲", "▼", "▶", ""]);
  });
  it("formats local hours in the facility time zone", () => {
    expect(localHour("2026-11-10T18:00:00Z", "America/Chicago")).toBe("12:00");
  });
});
```

Run `npm --prefix app/frontend install`. This creates `package-lock.json`, which you commit.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm --prefix app/frontend test`
Expected: FAIL with `Failed to resolve import "./bands"`.

- [ ] **Step 3: Implement the frontend**

`app/frontend/src/lib/bands.ts`:
```ts
export type Band = "Active" | "Busy" | "Crowded";

// One meaning per colour, used identically in charts, badges and gauges.
export const BAND_COLORS: Record<Band, string> = { Active: "#2E7D32", Busy: "#F9A825", Crowded: "#C62828" };
export const NEUTRAL = "#1565C0";
export const LOW_SAMPLE = "#9E9E9E";
export const GEDWIN = { busy: 2.06, crowded: 2.34 };
export const EDWIN = { busy: 1.5, crowded: 2 };
const NEDOCS_LABELS = ["Not busy", "Busy", "Extremely busy", "Overcrowded", "Severely overcrowded", "Dangerously overcrowded"];

type Num = number | null | undefined;

export function gedwinBand(g: Num): Band {
  if (g === null || g === undefined) return "Active"; // census 0
  return g < GEDWIN.busy ? "Active" : g <= GEDWIN.crowded ? "Busy" : "Crowded";
}

export function edwinBand(e: Num): Band | null {
  if (e === null || e === undefined) return null;
  return e < EDWIN.busy ? "Active" : e <= EDWIN.crowded ? "Busy" : "Crowded";
}

export function nedocsLabel(level: Num): string {
  return level ? NEDOCS_LABELS[level - 1] ?? "—" : "—";
}

export function isLowSample(n: Num): boolean {
  return n === null || n === undefined || n < 5;
}

export function fmt(v: Num, digits = 1, suffix = ""): string {
  return v === null || v === undefined || Number.isNaN(v) ? "—" : `${v.toFixed(digits)}${suffix}`;
}

export function pct(v: Num): string {
  return v === null || v === undefined ? "—" : `${Math.round(v * 100)}%`;
}

export function trendArrow(delta: Num): string {
  if (delta === null || delta === undefined) return "";
  return delta > 0.05 ? "▲" : delta < -0.05 ? "▼" : "▶";
}

export function localHour(iso: string, tz: string): string {
  return new Intl.DateTimeFormat("en-GB", { timeZone: tz, hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).format(new Date(iso));
}
```

`app/frontend/src/api.ts`:
```ts
import { useEffect, useState } from "react";
import type { Band } from "./lib/bands";

export type Driver = { driver: string; value: number | null; baseline: number | null; z: number | null };
export type Facility = {
  facility_id: string; facility_name: string; facility_type: string; region: string; snapshot_ts: string;
  census: number; gedwin: number | null; gedwin_band: Band; gedwin_delta_1h: number | null; edwin: number | null;
  edwin_band: Band | null; nedocs: number; nedocs_level: number; occupancy_ratio: number | null;
  d2p_mean_min: number | null; d2p_median_min: number | null; d2p_p90_min: number | null; d2p_sample_n: number;
  waiting_now_n: number; longest_current_wait_min: number | null; boarders_n: number; longest_boarder_h: number | null;
  lwbs_rate_24h: number | null; los_median_h_24h: number | null; attendings_on_duty: number | null;
  patients_per_attending: number | null; arrival_surge_ratio: number | null; undertriage_flags_n: number;
  top_drivers: Driver[] | null; computed_at: string; sparkline?: [string, number | null][];
  ed_beds?: number; inpatient_beds?: number;
};
export type Narrative = { headline: string; explanation: string; recommended_actions: string[] | null; top_drivers?: Driver[] | null;
  crowded_facilities?: string[] | null; doing_well_facilities?: string[] | null; generated_at?: string };
export type Summary = { counts: Record<Band, number>; facilities: number; census: number; boarders: number;
  network_d2p_median_min: number | null; as_of: string | null; narrative: Narrative | null };
export type Health = { status: "ok" | "delayed" | "unavailable"; data_age_seconds: number | null };
export type Point = { snapshot_ts: string; gedwin: number | null; edwin: number | null; nedocs: number | null;
  d2p_median_min: number | null; d2p_p90_min: number | null; d2p_sample_n: number; census: number;
  n_esi1: number; n_esi2: number; n_esi3: number; n_esi4: number; n_esi5: number; n_untriaged: number };
export type Patient = { visit_id: string; status: string; esi_level: number | null; suggested_esi: number | null;
  undertriage_flag: boolean | null; complaint_category: string | null; minutes_in_ed: number; minutes_waiting: number;
  arrival_mode: string; on_vent: boolean };
export type Cell = { facility_id: string; facility_name: string; hour: string; gedwin: number | null; band: Band };
export type GenieAnswer = { done: boolean; status: string; text: string | null; sql: string | null; columns: string[];
  rows: string[][]; error: string | null };
export type Definitions = { formulas: Record<string, string>; assumptions: string[]; citations: string[];
  bands: { gedwin: { busy: number; crowded: number }; edwin: { busy: number; crowded: number } } };

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, init);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(body.detail ?? `${r.status} ${r.statusText}`);
  }
  return r.json() as Promise<T>;
}

export const api = {
  health: () => request<Health>("/api/health"),
  summary: () => request<Summary>("/api/network/summary"),
  facilities: () => request<{ facilities: Facility[] }>("/api/facilities"),
  facility: (id: string) => request<{ facility: Facility; narrative: Narrative | null }>(`/api/facilities/${id}`),
  history: (id: string, hours: number) => request<{ points: Point[] }>(`/api/facilities/${id}/history?hours=${hours}`),
  board: (id: string) => request<{ patients: Patient[] }>(`/api/facilities/${id}/board`),
  heatmap: () => request<{ cells: Cell[] }>("/api/heatmap"),
  definitions: () => request<Definitions>("/api/definitions"),
  ask: (question: string, conversation_id?: string) =>
    request<{ conversation_id: string; message_id: string }>("/api/genie/messages", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question, conversation_id }),
    }),
  poll: (c: string, m: string) => request<GenieAnswer>(`/api/genie/conversations/${c}/messages/${m}`),
};

/** Fetch now and every `ms` milliseconds (the data refreshes about once a minute; 30 s polling is enough). */
export function usePolling<T>(fn: () => Promise<T>, ms: number, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    const load = () =>
      fn().then((d) => alive && (setData(d), setError(null))).catch((e: Error) => alive && setError(e.message));
    load();
    const id = setInterval(load, ms);
    return () => { alive = false; clearInterval(id); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return { data, error };
}
```

`app/frontend/src/main.tsx`:
```tsx
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
```

`app/frontend/src/App.tsx`:
```tsx
import { useState } from "react";
import { api, usePolling } from "./api";
import Banner from "./components/Banner";
import BubbleChart from "./components/BubbleChart";
import Definitions from "./components/Definitions";
import FacilityDetail from "./components/FacilityDetail";
import FacilityTable from "./components/FacilityTable";
import GeniePanel from "./components/GeniePanel";
import Heatmap from "./components/Heatmap";
import KpiTiles from "./components/KpiTiles";
import RankedBar from "./components/RankedBar";

export const TZ = "America/Chicago";
const POLL_MS = 30_000;
type View = { page: "overview" } | { page: "facility"; id: string } | { page: "definitions" };

export default function App() {
  const [view, setView] = useState<View>({ page: "overview" });
  const [genieOpen, setGenieOpen] = useState(false);
  const health = usePolling(api.health, POLL_MS);
  const summary = usePolling(api.summary, POLL_MS);
  const facilities = usePolling(api.facilities, POLL_MS);
  const heatmap = usePolling(api.heatmap, POLL_MS);
  const open = (id: string) => setView({ page: "facility", id });
  const rows = facilities.data?.facilities ?? [];

  return (
    <div className="app">
      <header className="topbar">
        <h1 onClick={() => setView({ page: "overview" })}>ED Crowding Command Center</h1>
        <nav>
          <button onClick={() => setView({ page: "overview" })}>Network</button>
          <button onClick={() => setView({ page: "definitions" })}>Definitions</button>
          <button className="primary" onClick={() => setGenieOpen(true)}>Ask Genie</button>
        </nav>
      </header>
      <Banner health={health.data} healthError={health.error} summary={summary.data} />
      {view.page === "overview" && (
        <main>
          <KpiTiles summary={summary.data} />
          {facilities.error && <p className="error">Facilities unavailable: {facilities.error}</p>}
          <section className="grid2">
            <RankedBar facilities={rows} onSelect={open} />
            <BubbleChart facilities={rows} onSelect={open} />
          </section>
          <Heatmap cells={heatmap.data?.cells ?? []} tz={TZ} />
          <FacilityTable facilities={rows} onSelect={open} />
        </main>
      )}
      {view.page === "facility" && <FacilityDetail id={view.id} tz={TZ} onBack={() => setView({ page: "overview" })} />}
      {view.page === "definitions" && <Definitions />}
      <GeniePanel open={genieOpen} onClose={() => setGenieOpen(false)} />
    </div>
  );
}
```

`app/frontend/src/components/Banner.tsx`:
```tsx
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
```

`app/frontend/src/components/KpiTiles.tsx`:
```tsx
import type { Summary } from "../api";
import { BAND_COLORS, fmt } from "../lib/bands";

export default function KpiTiles({ summary }: { summary: Summary | null }) {
  const c = summary?.counts ?? { Crowded: 0, Busy: 0, Active: 0 };
  const tiles = [
    { label: "Crowded", value: String(c.Crowded), color: BAND_COLORS.Crowded },
    { label: "Busy", value: String(c.Busy), color: BAND_COLORS.Busy },
    { label: "Active", value: String(c.Active), color: BAND_COLORS.Active },
    { label: "Network median door-to-provider", value: fmt(summary?.network_d2p_median_min, 0, " min") },
    { label: "Patients in EDs", value: summary ? String(summary.census) : "—" },
    { label: "Boarding patients", value: summary ? String(summary.boarders) : "—" },
  ];
  return (
    <section className="tiles">
      {tiles.map((t) => (
        <div className="tile" key={t.label} style={t.color ? { borderTopColor: t.color } : undefined}>
          <div className="tile-value">{t.value}</div>
          <div className="tile-label">{t.label}</div>
        </div>
      ))}
    </section>
  );
}
```

`app/frontend/src/components/RankedBar.tsx`:
```tsx
import type { ApexOptions } from "apexcharts";
import Chart from "react-apexcharts";
import type { Facility } from "../api";
import { BAND_COLORS, GEDWIN, gedwinBand } from "../lib/bands";

export default function RankedBar({ facilities, onSelect }: { facilities: Facility[]; onSelect: (id: string) => void }) {
  const rows = [...facilities].sort((a, b) => (b.gedwin ?? -99) - (a.gedwin ?? -99));
  const options: ApexOptions = {
    chart: { type: "bar", toolbar: { show: false }, events: { dataPointSelection: (_e, _c, cfg) => onSelect(rows[cfg.dataPointIndex].facility_id) } },
    plotOptions: { bar: { horizontal: true, distributed: true, barHeight: "80%" } },
    colors: rows.map((r) => BAND_COLORS[gedwinBand(r.gedwin)]),
    legend: { show: false },
    dataLabels: { enabled: false },
    xaxis: { categories: rows.map((r) => r.facility_name), title: { text: "gEDWIN" } },
    annotations: {
      xaxis: [
        { x: GEDWIN.busy, borderColor: BAND_COLORS.Busy, label: { text: "Busy 2.06" } },
        { x: GEDWIN.crowded, borderColor: BAND_COLORS.Crowded, label: { text: "Crowded 2.34" } },
      ],
    },
    tooltip: { y: { formatter: (v) => (v === null ? "empty ED" : v.toFixed(2)) } },
  };
  return (
    <div className="card">
      <h2>Crowding ranking (gEDWIN)</h2>
      <Chart type="bar" height={Math.max(320, rows.length * 18)} options={options} series={[{ name: "gEDWIN", data: rows.map((r) => r.gedwin ?? 0) }]} />
    </div>
  );
}
```

`app/frontend/src/components/BubbleChart.tsx`:
```tsx
import type { ApexOptions } from "apexcharts";
import Chart from "react-apexcharts";
import type { Facility } from "../api";
import { BAND_COLORS, type Band, gedwinBand } from "../lib/bands";

const BANDS: Band[] = ["Crowded", "Busy", "Active"];

export default function BubbleChart({ facilities, onSelect }: { facilities: Facility[]; onSelect: (id: string) => void }) {
  const groups = BANDS.map((b) => facilities.filter((f) => gedwinBand(f.gedwin) === b && f.d2p_median_min !== null));
  const options: ApexOptions = {
    chart: { type: "bubble", toolbar: { show: false },
      events: { dataPointSelection: (_e, _c, cfg) => onSelect(groups[cfg.seriesIndex][cfg.dataPointIndex].facility_id) } },
    colors: BANDS.map((b) => BAND_COLORS[b]),
    dataLabels: { enabled: false },
    xaxis: { type: "numeric", title: { text: "Occupancy (%)" }, min: 0 },
    yaxis: { title: { text: "Door-to-provider median (min)" }, min: 0 },
    tooltip: { custom: ({ seriesIndex, dataPointIndex }) => {
      const f = groups[seriesIndex][dataPointIndex];
      return `<div class="tip"><b>${f.facility_name}</b><br/>Occupancy ${Math.round((f.occupancy_ratio ?? 0) * 100)}%<br/>` +
        `D2P median ${f.d2p_median_min?.toFixed(0)} min<br/>Census ${f.census}</div>`;
    } },
  };
  const series = BANDS.map((b, i) => ({
    name: b,
    data: groups[i].map((f) => ({ x: Math.round((f.occupancy_ratio ?? 0) * 100), y: Math.round(f.d2p_median_min ?? 0), z: Math.max(1, f.census) })),
  }));
  return (
    <div className="card">
      <h2>Where are we doing well? Occupancy vs. door-to-provider</h2>
      <Chart type="bubble" height={420} options={options} series={series} />
    </div>
  );
}
```

`app/frontend/src/components/Heatmap.tsx`:
```tsx
import type { ApexOptions } from "apexcharts";
import Chart from "react-apexcharts";
import type { Cell } from "../api";
import { BAND_COLORS, GEDWIN, localHour } from "../lib/bands";

export default function Heatmap({ cells, tz }: { cells: Cell[]; tz: string }) {
  const byFacility = new Map<string, Cell[]>();
  cells.forEach((c) => byFacility.set(c.facility_name, [...(byFacility.get(c.facility_name) ?? []), c]));
  const series = [...byFacility.entries()].map(([name, rows]) => ({
    name,
    data: rows.map((c) => ({ x: localHour(c.hour, tz), y: c.gedwin ?? 0 })),
  }));
  const options: ApexOptions = {
    chart: { type: "heatmap", toolbar: { show: false } },
    dataLabels: { enabled: false },
    plotOptions: { heatmap: { colorScale: { ranges: [
      { from: -50, to: GEDWIN.busy - 0.0001, color: BAND_COLORS.Active, name: "Active" },
      { from: GEDWIN.busy, to: GEDWIN.crowded, color: BAND_COLORS.Busy, name: "Busy" },
      { from: GEDWIN.crowded + 0.0001, to: 50, color: BAND_COLORS.Crowded, name: "Crowded" },
    ] } } },
    tooltip: { y: { formatter: (v) => `gEDWIN ${v.toFixed(2)}` } },
  };
  return (
    <div className="card">
      <h2>Last 24 hours by local hour (peak gEDWIN band)</h2>
      <Chart type="heatmap" height={Math.max(300, series.length * 16)} options={options} series={series} />
    </div>
  );
}
```

`app/frontend/src/components/FacilityTable.tsx`:
```tsx
import type { ApexOptions } from "apexcharts";
import { useState } from "react";
import Chart from "react-apexcharts";
import type { Facility } from "../api";
import { BAND_COLORS, LOW_SAMPLE, NEUTRAL, fmt, gedwinBand, isLowSample, nedocsLabel, trendArrow } from "../lib/bands";

type Key = "facility_name" | "gedwin" | "census" | "d2p_median_min" | "boarders_n" | "nedocs";
const COLUMNS: [Key, string][] = [["facility_name", "Facility"], ["gedwin", "gEDWIN"], ["census", "Census"],
  ["d2p_median_min", "Door-to-provider"], ["boarders_n", "Boarders"], ["nedocs", "NEDOCS"]];
const spark: ApexOptions = { chart: { type: "line", sparkline: { enabled: true } }, stroke: { width: 2 }, colors: [NEUTRAL], tooltip: { enabled: false } };

export default function FacilityTable({ facilities, onSelect }: { facilities: Facility[]; onSelect: (id: string) => void }) {
  const [sort, setSort] = useState<{ key: Key; desc: boolean }>({ key: "gedwin", desc: true });
  const rows = [...facilities].sort((a, b) => {
    const av = a[sort.key] ?? -1, bv = b[sort.key] ?? -1;
    const cmp = typeof av === "string" ? String(av).localeCompare(String(bv)) : Number(av) - Number(bv);
    return sort.desc ? -cmp : cmp;
  });
  return (
    <div className="card">
      <h2>All facilities</h2>
      <table className="table">
        <thead>
          <tr>
            {COLUMNS.map(([k, label]) => (
              <th key={k} onClick={() => setSort({ key: k, desc: sort.key === k ? !sort.desc : true })}>{label}</th>
            ))}
            <th>Last 6 h</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((f) => {
            const band = gedwinBand(f.gedwin);
            const low = isLowSample(f.d2p_sample_n);
            return (
              <tr key={f.facility_id} onClick={() => onSelect(f.facility_id)}>
                <td>{f.facility_name}</td>
                <td><span className="badge" style={{ background: BAND_COLORS[band] }}>{band}</span> {fmt(f.gedwin, 2)} {trendArrow(f.gedwin_delta_1h)}</td>
                <td>{f.census}</td>
                <td style={low ? { color: LOW_SAMPLE } : undefined}>{fmt(f.d2p_median_min, 0, " min")}{low && <span className="low"> low sample</span>}</td>
                <td>{f.boarders_n}</td>
                <td>{nedocsLabel(f.nedocs_level)}</td>
                <td className="spark">
                  <Chart type="line" height={32} width={120} options={spark}
                    series={[{ name: "gEDWIN", data: (f.sparkline ?? []).map(([, g]) => g ?? 0) }]} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
```

`app/frontend/src/components/FacilityDetail.tsx`:
```tsx
import type { ApexOptions } from "apexcharts";
import { useState } from "react";
import Chart from "react-apexcharts";
import { api, usePolling } from "../api";
import { BAND_COLORS, EDWIN, GEDWIN, LOW_SAMPLE, NEUTRAL, edwinBand, fmt, gedwinBand, isLowSample, nedocsLabel, pct } from "../lib/bands";

function Gauge({ label, value, max, color, text }: { label: string; value: number | null; max: number; color: string; text: string }) {
  const options: ApexOptions = {
    chart: { type: "radialBar", sparkline: { enabled: true } },
    colors: [color],
    plotOptions: { radialBar: { startAngle: -110, endAngle: 110, dataLabels: { name: { show: true, offsetY: 20 }, value: { offsetY: -15, formatter: () => text } } } },
    labels: [label],
  };
  const percent = value === null ? 0 : Math.max(0, Math.min(100, (value / max) * 100));
  return <div className="gauge"><Chart type="radialBar" height={200} options={options} series={[percent]} /></div>;
}

export default function FacilityDetail({ id, tz, onBack }: { id: string; tz: string; onBack: () => void }) {
  const [hours, setHours] = useState(24);
  const detail = usePolling(() => api.facility(id), 30_000, [id]);
  const history = usePolling(() => api.history(id, hours), 60_000, [id, hours]);
  const board = usePolling(() => api.board(id), 30_000, [id]);
  const f = detail.data?.facility;
  if (detail.error) return <main><button onClick={onBack}>← Network</button><p className="error">{detail.error}</p></main>;
  if (!f) return <main><p>Loading…</p></main>;
  const points = history.data?.points ?? [];
  const band = gedwinBand(f.gedwin);
  const low = isLowSample(f.d2p_sample_n);
  const time = { type: "datetime" as const, labels: { datetimeUTC: false, format: hours > 24 ? "dd MMM" : "HH:mm" } };
  const lineOptions: ApexOptions = {
    chart: { type: "line", toolbar: { show: false } }, stroke: { width: 2 }, colors: [NEUTRAL, "#6A1B9A", BAND_COLORS.Crowded],
    xaxis: time, yaxis: [{ title: { text: "minutes" } }, { opposite: true, title: { text: "gEDWIN" } }],
    annotations: { yaxis: [
      { y: GEDWIN.busy, yAxisIndex: 1, borderColor: BAND_COLORS.Busy, label: { text: "Busy" } },
      { y: GEDWIN.crowded, yAxisIndex: 1, borderColor: BAND_COLORS.Crowded, label: { text: "Crowded" } },
    ] },
    tooltip: { x: { format: "dd MMM HH:mm" } },
  };
  const ts = (p: { snapshot_ts: string }) => new Date(p.snapshot_ts).getTime();
  const areaOptions: ApexOptions = {
    chart: { type: "area", stacked: true, toolbar: { show: false } }, xaxis: time, dataLabels: { enabled: false },
    colors: ["#B71C1C", "#E65100", "#F9A825", "#7CB342", "#2E7D32", LOW_SAMPLE], stroke: { width: 1 },
  };
  const esiKeys = ["n_esi1", "n_esi2", "n_esi3", "n_esi4", "n_esi5", "n_untriaged"] as const;
  const esiNames = ["ESI 1", "ESI 2", "ESI 3", "ESI 4", "ESI 5", "Untriaged"];
  const n = detail.data?.narrative;
  const eb = edwinBand(f.edwin);
  return (
    <main>
      <button onClick={onBack}>← Network</button>
      <h2 className="title">{f.facility_name} <span className="badge" style={{ background: BAND_COLORS[band] }}>{band}</span></h2>
      <p className="muted">{f.facility_type.replace(/_/g, " ")} · {f.region} · {f.ed_beds ?? "?"} ED beds · {f.attendings_on_duty ?? 0} attendings on duty</p>
      <section className="gauges">
        <Gauge label="gEDWIN" value={f.gedwin} max={4} color={BAND_COLORS[band]} text={fmt(f.gedwin, 2)} />
        <Gauge label="EDWIN" value={f.edwin} max={4} color={eb ? BAND_COLORS[eb] : LOW_SAMPLE} text={fmt(f.edwin, 2)} />
        <Gauge label={`NEDOCS · ${nedocsLabel(f.nedocs_level)}`} value={f.nedocs} max={200}
          color={f.nedocs_level >= 4 ? BAND_COLORS.Crowded : f.nedocs_level === 3 ? BAND_COLORS.Busy : BAND_COLORS.Active} text={fmt(f.nedocs, 0)} />
      </section>
      <section className="tiles">
        {[["Door-to-provider mean", fmt(f.d2p_mean_min, 0, " min")], ["Median", fmt(f.d2p_median_min, 0, " min")],
          ["90th percentile", fmt(f.d2p_p90_min, 0, " min")], ["Greeted (last hour)", String(f.d2p_sample_n)],
          ["Waiting now", String(f.waiting_now_n)], ["Longest wait", fmt(f.longest_current_wait_min, 0, " min")],
          ["Boarders", `${f.boarders_n} (longest ${fmt(f.longest_boarder_h, 1, " h")})`], ["LWBS (24 h)", pct(f.lwbs_rate_24h)]].map(([label, value]) => (
          <div className="tile" key={label} style={low && label !== "Waiting now" && label.startsWith("D") ? { color: LOW_SAMPLE } : undefined}>
            <div className="tile-value">{value}</div><div className="tile-label">{label}</div>
          </div>
        ))}
      </section>
      {low && <p className="low">Door-to-provider figures are low-sample: fewer than 5 patients were greeted in the last hour.</p>}
      {n && (
        <div className="card narrative-card">
          <h3>{n.headline}</h3><p>{n.explanation}</p>
          {n.recommended_actions?.length ? <ul>{n.recommended_actions.map((a) => <li key={a}>{a}</li>)}</ul> : null}
          {f.top_drivers?.length ? <p className="muted">Drivers: {f.top_drivers.map((d) => `${d.driver.replace(/_/g, " ")} ${fmt(d.value, 2)} vs ${fmt(d.baseline, 2)} (z ${fmt(d.z, 1)})`).join(" · ")}</p> : null}
        </div>
      )}
      <div className="card">
        <h2>Trends <button onClick={() => setHours(hours === 24 ? 168 : 24)}>{hours === 24 ? "Show 7 days" : "Show 24 hours"}</button></h2>
        {points.length === 0 ? <p className="muted">No history yet.</p> : (
          <Chart type="line" height={300} options={lineOptions} series={[
            { name: "D2P median", data: points.map((p) => [ts(p), p.d2p_median_min]) },
            { name: "D2P 90th pct", data: points.map((p) => [ts(p), p.d2p_p90_min]) },
            { name: "gEDWIN", data: points.map((p) => [ts(p), p.gedwin]) },
          ]} />
        )}
        {points.length > 0 && (
          <Chart type="area" height={240} options={areaOptions}
            series={esiKeys.map((k, i) => ({ name: esiNames[i], data: points.map((p) => [ts(p), p[k]]) }))} />
        )}
      </div>
      <div className="card">
        <h2>ED board (de-identified)</h2>
        <table className="table">
          <thead><tr><th>Visit</th><th>Status</th><th>ESI</th><th>AI ESI</th><th>Complaint</th><th>In ED</th><th>Waiting</th><th>Arrival</th></tr></thead>
          <tbody>
            {(board.data?.patients ?? []).map((p) => (
              <tr key={p.visit_id} className={p.undertriage_flag ? "undertriage" : undefined}>
                <td>{p.visit_id}</td><td>{p.status}{p.on_vent ? " · ventilated" : ""}</td><td>{p.esi_level ?? "—"}</td>
                <td>{p.suggested_esi ?? "—"}{p.undertriage_flag ? " ⚠" : ""}</td><td>{p.complaint_category ?? "—"}</td>
                <td>{p.minutes_in_ed} min</td><td>{p.minutes_waiting} min</td><td>{p.arrival_mode.replace("_", " ")}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="muted">⚠ AI-suggested ESI is at least 2 levels more acute than the nurse ESI (decision support only). Times in {tz}.</p>
      </div>
    </main>
  );
}
```

`app/frontend/src/components/GeniePanel.tsx`:
```tsx
import { useState } from "react";
import { api, type GenieAnswer } from "../api";

type Turn = { question: string; answer: GenieAnswer | null; error: string | null };

export default function GeniePanel({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [question, setQuestion] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [conversation, setConversation] = useState<string | undefined>();
  const [busy, setBusy] = useState(false);

  async function send() {
    const q = question.trim();
    if (!q || busy) return;
    setBusy(true);
    setQuestion("");
    setTurns((t) => [...t, { question: q, answer: null, error: null }]);
    const update = (patch: Partial<Turn>) => setTurns((t) => t.map((x, i) => (i === t.length - 1 ? { ...x, ...patch } : x)));
    try {
      const { conversation_id, message_id } = await api.ask(q, conversation);
      setConversation(conversation_id);
      for (let attempt = 0; attempt < 120; attempt++) {
        const answer = await api.poll(conversation_id, message_id);
        update({ answer });
        if (answer.done) break;
        await new Promise((r) => setTimeout(r, 1500));
      }
    } catch (e) {
      update({ error: (e as Error).message });
    } finally {
      setBusy(false);
    }
  }

  if (!open) return null;
  return (
    <aside className="genie">
      <header><h2>Ask Genie</h2><button onClick={onClose}>✕</button></header>
      <div className="turns">
        {turns.length === 0 && <p className="muted">Try: "Which facilities are crowded right now?" or "Where is boarding worst?"</p>}
        {turns.map((t, i) => (
          <div className="turn" key={i}>
            <p className="q">{t.question}</p>
            {t.error && <p className="error">{t.error}</p>}
            {t.answer && !t.answer.done && <p className="muted">Genie is working… ({t.answer.status.toLowerCase().replace(/_/g, " ")})</p>}
            {t.answer?.done && (
              <>
                {t.answer.error && <p className="error">{t.answer.error}</p>}
                {t.answer.text && <p>{t.answer.text}</p>}
                {t.answer.sql && <details><summary>SQL</summary><pre>{t.answer.sql}</pre></details>}
                {t.answer.rows.length > 0 && (
                  <table className="table">
                    <thead><tr>{t.answer.columns.map((c) => <th key={c}>{c}</th>)}</tr></thead>
                    <tbody>{t.answer.rows.map((r, j) => <tr key={j}>{r.map((v, k) => <td key={k}>{v}</td>)}</tr>)}</tbody>
                  </table>
                )}
              </>
            )}
          </div>
        ))}
      </div>
      <footer>
        <input value={question} placeholder="Ask about ED crowding…" onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && send()} disabled={busy} />
        <button className="primary" onClick={send} disabled={busy}>Ask</button>
      </footer>
    </aside>
  );
}
```

`app/frontend/src/components/Definitions.tsx`:
```tsx
import { api, usePolling } from "../api";

export default function Definitions() {
  const { data, error } = usePolling(api.definitions, 3_600_000);
  if (error) return <main><p className="error">{error}</p></main>;
  if (!data) return <main><p>Loading…</p></main>;
  return (
    <main className="definitions">
      <div className="card">
        <h2>Formulas</h2>
        <dl>{Object.entries(data.formulas).map(([k, v]) => <div key={k}><dt>{k.replace(/_/g, " ")}</dt><dd>{v}</dd></div>)}</dl>
        <h2>Bands</h2>
        <p>gEDWIN (headline): Active &lt; {data.bands.gedwin.busy} ≤ Busy ≤ {data.bands.gedwin.crowded} &lt; Crowded.
          EDWIN: Active &lt; {data.bands.edwin.busy} ≤ Busy ≤ {data.bands.edwin.crowded} &lt; Crowded.
          NEDOCS: levels 1-6 at 20 / 60 / 100 / 140 / 180.</p>
        <h2>Assumptions</h2>
        <ul>{data.assumptions.map((a) => <li key={a}>{a}</li>)}</ul>
        <h2>Sources</h2>
        <ul>{data.citations.map((c) => <li key={c}>{c}</li>)}</ul>
      </div>
    </main>
  );
}
```

`app/frontend/src/styles.css`:
```css
:root { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; color: #1f2933; background: #f5f7fa; }
* { box-sizing: border-box; }
body { margin: 0; }
.app { max-width: 1440px; margin: 0 auto; padding: 0 16px 48px; }
.topbar { display: flex; align-items: center; justify-content: space-between; padding: 12px 0; }
.topbar h1 { font-size: 20px; margin: 0; cursor: pointer; }
nav button, button { border: 1px solid #cbd2d9; background: #fff; border-radius: 6px; padding: 6px 12px; cursor: pointer; margin-left: 6px; }
button.primary { background: #1565c0; border-color: #1565c0; color: #fff; }
.banner { border-radius: 8px; padding: 10px 14px; margin-bottom: 10px; display: flex; gap: 10px; flex-wrap: wrap; }
.banner.warn { background: #fff4e5; border: 1px solid #f9a825; }
.banner.narrative { background: #e8f0fe; border: 1px solid #c3d6f8; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin: 12px 0; }
.tile { background: #fff; border-radius: 8px; padding: 12px; border-top: 4px solid #cbd2d9; }
.tile-value { font-size: 24px; font-weight: 600; }
.tile-label { font-size: 12px; color: #52606d; }
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 1000px) { .grid2 { grid-template-columns: 1fr; } }
.card { background: #fff; border-radius: 8px; padding: 12px 16px; margin-bottom: 12px; }
.card h2 { font-size: 16px; margin: 4px 0 8px; }
.table { width: 100%; border-collapse: collapse; font-size: 13px; }
.table th { text-align: left; border-bottom: 1px solid #e4e7eb; padding: 6px; cursor: pointer; }
.table td { border-bottom: 1px solid #f0f2f4; padding: 6px; }
.table tbody tr { cursor: pointer; }
.table tr.undertriage { background: #fdecea; }
.badge { color: #fff; border-radius: 10px; padding: 1px 8px; font-size: 12px; }
.low { color: #9e9e9e; font-size: 12px; }
.muted { color: #616e7c; font-size: 13px; }
.error { color: #c62828; }
.gauges { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
.gauge { background: #fff; border-radius: 8px; }
.title { display: flex; gap: 10px; align-items: center; }
.spark { width: 130px; }
.genie { position: fixed; top: 0; right: 0; width: 460px; max-width: 100vw; height: 100vh; background: #fff; box-shadow: -4px 0 16px rgba(0,0,0,.15); display: flex; flex-direction: column; z-index: 10; }
.genie header, .genie footer { display: flex; gap: 8px; align-items: center; justify-content: space-between; padding: 10px 14px; border-bottom: 1px solid #e4e7eb; }
.genie footer { border-top: 1px solid #e4e7eb; border-bottom: none; }
.genie footer input { flex: 1; padding: 8px; border: 1px solid #cbd2d9; border-radius: 6px; }
.turns { flex: 1; overflow-y: auto; padding: 10px 14px; }
.turn .q { font-weight: 600; }
.turn pre { white-space: pre-wrap; background: #f5f7fa; padding: 8px; border-radius: 6px; font-size: 12px; }
.tip { padding: 6px 8px; font-size: 12px; }
.definitions dt { font-weight: 600; text-transform: capitalize; }
.definitions dd { margin: 0 0 8px; font-family: ui-monospace, monospace; font-size: 13px; }
```

- [ ] **Step 4: Run the tests and the production build**

Run: `npm --prefix app/frontend test`
Expected: 5 passed.

Run: `npm --prefix app/frontend run build`
Expected: `tsc --noEmit` reports no errors, then Vite writes `app/frontend/dist/index.html` and `app/frontend/dist/assets/*.js`.

Run: `scripts/pytest.sh tests/app -v`
Expected: still 16 passed. The backend now finds `dist/` and mounts it; the `/api/*` routes are unchanged.

- [ ] **Step 5: Commit**

```bash
git add app/frontend/package.json app/frontend/package-lock.json app/frontend/tsconfig.json app/frontend/vite.config.ts app/frontend/index.html app/frontend/src
git commit -m "feat: React + ApexCharts frontend - network overview, facility detail, heatmap, Genie panel, definitions" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: Setup tasks, entry points, bundle resources, customer target and scripts

**Files:**
- Modify: `src/ed_ops/setup.py`, `src/ed_ops/cli.py`, `pyproject.toml`, `databricks.yml`, `resources/jobs.yml`, `scripts/teardown.sh`, `tests/unit/test_cli.py`
- Create: `resources/app.yml`, `resources/lakebase.yml`, `scripts/deploy.sh`, `scripts/teardown_serving.py`

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces:
  - setup tasks `serving_setup` and `genie_space`;
  - console scripts `ed-ops-serving-setup`, `ed-ops-genie-space` and `ed-ops-genie-benchmarks` (22 in total);
  - bundle resources `postgres_projects.ed_ops`, `apps.ed_ops` and `jobs.ed_genie_benchmarks`;
  - the `customer` target with `run_as`.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/test_cli.py`:
- change `assert len(scripts) == 19` to `assert len(scripts) == 22`;
- change the `_jobs()` loop to read `("jobs.yml", "validate.yml")` exactly as now;
- add this test:
```python


def test_serving_resources_and_customer_target():
    jobs = _jobs()
    setup_keys = [t["task_key"] for t in jobs["ed_setup"]["tasks"]]
    assert setup_keys[-2:] == ["serving_setup", "genie_space"]
    assert [t["task_key"] for t in jobs["ed_genie_benchmarks"]["tasks"]] == ["genie_benchmarks"]
    common = jobs["ed_setup"]["tasks"][0]["python_wheel_task"]["named_parameters"]
    assert common["app_name"] == "${resources.apps.ed_ops.name}" and common["warehouse_id"] == "${var.warehouse_id}"
    app = yaml.safe_load((ROOT / "resources" / "app.yml").read_text())["resources"]["apps"]["ed_ops"]
    assert app["source_code_path"] == "../app" and set(app["user_api_scopes"]) == {"dashboards.genie", "sql"}
    kinds = {next(k for k in r if k not in ("name", "description")) for r in app["resources"]}
    assert kinds == {"postgres", "sql_warehouse"}
    env = {e["name"]: e["value"] for e in app["config"]["env"]}
    assert {"SERVING_SCHEMA", "LAKEBASE_ENDPOINT", "GENIE_SPACE_TITLE", "LOCAL_TZ"} <= set(env)
    project = yaml.safe_load((ROOT / "resources" / "lakebase.yml").read_text())["resources"]["postgres_projects"]["ed_ops"]
    assert project["project_id"] == "${var.lakebase_project_id}"
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "app/frontend/dist/**" in bundle["sync"]["include"]
    customer = bundle["targets"]["customer"]
    assert customer["mode"] == "production" and customer["run_as"] == {"service_principal_name": "${var.run_as_service_principal}"}
    assert bundle["targets"]["dev"]["variables"]["warehouse_id"] == "abef38163a09f120"
    assert (ROOT / "scripts" / "deploy.sh").stat().st_mode & 0o111
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_cli.py -v`
Expected: FAIL. The script count is 19, not 22, and there is no `serving_setup` task yet.

- [ ] **Step 3: Implement the setup tasks and entry points**

Append to `src/ed_ops/setup.py`:
```python


def serving_setup(spark: SparkSession, cfg: Config) -> None:
    """Create the Lakebase synced tables (idempotent), wait until online, grant the app's role SELECT."""
    from databricks.sdk import WorkspaceClient

    from ed_ops.serving import lakebase

    w = WorkspaceClient()
    lakebase.ensure_synced_tables(w, cfg)
    lakebase.wait_until_online(w, cfg)
    lakebase.apply_grants(w, cfg, lakebase.app_role(w, cfg))


def genie_space(spark: SparkSession, cfg: Config) -> None:
    """Create or update the Genie space by title (after its tables exist) and let the app service principal run it."""
    from databricks.sdk import WorkspaceClient

    from ed_ops.serving.genie_space import build_serialized_space, grant_can_run, load_space_doc, upsert_genie_space

    w = WorkspaceClient()
    space_id = upsert_genie_space(w, cfg, build_serialized_space(cfg, load_space_doc()))
    grant_can_run(w, space_id, w.apps.get(name=cfg.app_name).service_principal_client_id)
```

Append to `src/ed_ops/cli.py`:
```python


def serving_setup() -> None:
    _run(setup.serving_setup)


def genie_space() -> None:
    _run(setup.genie_space)


def genie_benchmarks() -> None:
    from ed_ops.serving.genie_benchmarks import run_benchmarks

    _run(lambda spark, cfg: run_benchmarks(spark, cfg) and None)
```

In `pyproject.toml`, add to `[project.scripts]`:
```toml
ed-ops-serving-setup = "ed_ops.cli:serving_setup"
ed-ops-genie-space = "ed_ops.cli:genie_space"
ed-ops-genie-benchmarks = "ed_ops.cli:genie_benchmarks"
```

- [ ] **Step 4: Bundle resources, the customer target and variables**

`resources/lakebase.yml`:
```yaml
resources:
  postgres_projects:
    ed_ops:
      project_id: ${var.lakebase_project_id}
      display_name: ED Ops serving (${bundle.target})
      pg_version: 17
      default_endpoint_settings:
        autoscaling_limit_min_cu: 0.5
        autoscaling_limit_max_cu: 2
        suspend_timeout_duration: 600s
```

`resources/app.yml`:
```yaml
resources:
  apps:
    ed_ops:
      name: ${var.app_name}
      description: ED crowding command center - live EDWIN/gEDWIN/NEDOCS, waits, boarding, AI narratives and Genie.
      source_code_path: ../app
      user_api_scopes: [dashboards.genie, sql]
      resources:
        - name: lakebase
          postgres:
            branch: projects/${var.lakebase_project_id}/branches/${var.lakebase_branch}
            database: projects/${var.lakebase_project_id}/branches/${var.lakebase_branch}/databases/databricks-postgres
            permission: CAN_CONNECT_AND_CREATE
        - name: warehouse
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: CAN_USE
      config:
        command: [uvicorn, backend.main:app, --host, 0.0.0.0, --port, "8000"]
        env:
          - {name: SERVING_SCHEMA, value: "${var.schema_prefix}serving"}
          - {name: LAKEBASE_ENDPOINT, value: "projects/${var.lakebase_project_id}/branches/${var.lakebase_branch}/endpoints/primary"}
          - {name: GENIE_SPACE_TITLE, value: "${var.genie_space_title}"}
          - {name: LOCAL_TZ, value: "${var.timezone}"}
```

In `resources/jobs.yml`, add these lines to the `&common` named parameters, after `ai_retry_minutes: ${var.ai_retry_minutes}`:
```yaml
              warehouse_id: ${var.warehouse_id}
              lakebase_project_id: ${var.lakebase_project_id}
              lakebase_branch: ${var.lakebase_branch}
              lakebase_database: ${var.lakebase_database}
              sync_mode_live: ${var.sync_mode_live}
              sync_mode_ref: ${var.sync_mode_ref}
              app_name: ${resources.apps.ed_ops.name}
              genie_space_title: ${var.genie_space_title}
```
Append these tasks to `ed_setup`, after `governance_checks`:
```yaml
        - task_key: serving_setup
          depends_on: [{task_key: governance_checks}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-serving-setup, named_parameters: *common}
        - task_key: genie_space
          depends_on: [{task_key: serving_setup}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-genie-space, named_parameters: *common}
```
Append this job at the end of `resources/jobs.yml`, under `resources.jobs`, at the same indentation as `ed_streaming`:
```yaml

    ed_genie_benchmarks:
      name: ed-ops genie benchmarks
      description: Asks every benchmark question through the Genie Conversation API and compares with the expected SQL.
      max_concurrent_runs: 1
      environments:
        - environment_key: default
          spec:
            environment_version: "4"
            dependencies:
              - ../dist/*.whl
      tasks:
        - task_key: genie_benchmarks
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-genie-benchmarks, named_parameters: *common}
```

In `databricks.yml`:
- add under `variables:`, before `streaming_node_type:`:
```yaml
  warehouse_id:
    description: SQL warehouse for Genie and the app (required for every target).
    default: ""
  lakebase_project_id:
    description: Lakebase Autoscaling project id (lowercase letters, digits, hyphens).
    default: ed-ops-${bundle.target}
  lakebase_branch:
    description: Lakebase branch serving the app.
    default: production
  lakebase_database:
    description: Postgres database holding the synced tables.
    default: databricks_postgres
  sync_mode_live:
    description: Sync mode for live gold tables (CONTINUOUS or TRIGGERED).
    default: CONTINUOUS
  sync_mode_ref:
    description: Sync mode for reference tables.
    default: SNAPSHOT
  app_name:
    description: Databricks App name.
    default: ed-ops-${bundle.target}
  genie_space_title:
    description: Title of the Genie space (the app finds the space by this title).
    default: ED Ops Crowding Assistant (${bundle.target})
  run_as_service_principal:
    description: Application id of the service principal that runs the jobs in the customer target.
    default: ""
```
- add a top-level `sync` block after `include:`:
```yaml

sync:
  include:
    - app/frontend/dist/**
```
- under `targets.dev.variables`, add `warehouse_id: abef38163a09f120`;
- append this target under `targets:`:
```yaml
  customer:
    # Template for the customer workspace: authenticate with a CLI profile for that workspace and pass
    # --var catalog=<existing catalog> --var warehouse_id=<warehouse> --var run_as_service_principal=<application id>.
    mode: production
    run_as:
      service_principal_name: ${var.run_as_service_principal}
```

- [ ] **Step 5: Deploy and teardown scripts**

`scripts/deploy.sh`:
```bash
#!/usr/bin/env bash
# Build the React frontend, then deploy the bundle (the dist/ folder is git-ignored but synced via sync.include).
# Usage: scripts/deploy.sh -t dev -p fevm-playground [--var name=value ...]
set -euo pipefail
cd "$(dirname "$0")/.."
npm --prefix app/frontend ci
npm --prefix app/frontend run build
databricks bundle deploy "$@"
```

`scripts/teardown_serving.py`:
```python
#!/usr/bin/env python3
"""Delete the Lakebase synced tables and the Genie space (run before dropping the schemas).
Usage: scripts/teardown_serving.py --prefix ed_ --catalog <catalog> --genie-title "<title>" [--profile fevm-playground]"""

import argparse
import json
import subprocess

TABLES = ("facility_current", "facility_snapshot_5min", "ed_board_current", "facility_narrative", "network_narrative", "facility")


def cli(profile: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["databricks", "-p", profile, *args], capture_output=True, text=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--prefix", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--genie-title", required=True)
    p.add_argument("--profile", default="fevm-playground")
    a = p.parse_args()
    for t in TABLES:
        name = f"synced_tables/{a.catalog}.{a.prefix}serving.{t}"
        r = cli(a.profile, "postgres", "delete-synced-table", name)
        print(f"{name}: {'deleted' if r.returncode == 0 else r.stderr.strip()[:120]}")
    spaces = json.loads(cli(a.profile, "api", "get", "/api/2.0/genie/spaces?page_size=100").stdout or "{}").get("spaces", [])
    for s in spaces:
        if s.get("title") == a.genie_title:
            cli(a.profile, "api", "delete", f"/api/2.0/genie/spaces/{s['space_id']}")
            print(f"deleted Genie space {s['space_id']} ({a.genie_title})")


if __name__ == "__main__":
    main()
```

Replace the body of `scripts/teardown.sh` with:
```bash
#!/usr/bin/env bash
# Delete the synced tables and the Genie space, then drop every {prefix}* schema (CASCADE: tables, volumes,
# checkpoints, landing files). Never drops the catalog. Run `databricks bundle destroy` afterwards for jobs, the app
# and the Lakebase project.
set -euo pipefail
PREFIX="${1:?usage: scripts/teardown.sh <schema_prefix> --yes [catalog] [genie title]}"
[[ "${2:-}" == "--yes" ]] || { echo "refusing to drop ${PREFIX}* schemas without --yes" >&2; exit 2; }
CATALOG="${3:-hls_jagjit_lb_playground_catalog}"
TITLE="${4:-ED Ops Crowding Assistant (dev)}"
cd "$(dirname "$0")/.."
python3 scripts/teardown_serving.py --prefix "$PREFIX" --catalog "$CATALOG" --genie-title "$TITLE"
for logical in bronze silver gold ref sim serving; do
  python3 scripts/run_sql.py "DROP SCHEMA IF EXISTS ${CATALOG}.${PREFIX}${logical} CASCADE"
done
```

Run: `chmod +x scripts/deploy.sh scripts/teardown_serving.py`

- [ ] **Step 6: Run the tests and validate the bundle**

Run: `scripts/pytest.sh -v`
Expected: the whole suite passes.

Run: `databricks bundle validate -t dev -p fevm-playground`
Expected: `Validation OK!`. If a field name is rejected (for example under `postgres_projects` or the app's `postgres` resource), check it against `databricks bundle schema` and use the name that release accepts.

- [ ] **Step 7: Commit**

```bash
git add src/ed_ops/setup.py src/ed_ops/cli.py pyproject.toml databricks.yml resources scripts tests/unit/test_cli.py
git commit -m "feat: Lakebase project, app and Genie wiring in the bundle; serving/genie setup tasks; customer target; deploy and teardown scripts" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: Workspace rollout, verification and handover docs

**Files:**
- Create: `README.md`, `docs/runbook.md`, `docs/architecture.md`
- Delete: `docs/runbook-plan1.md`, `docs/runbook-plan2.md` (their content folds into `docs/runbook.md`)

**Interfaces:**
- Consumes: the bundle from Task 6.
- Produces:
  - a running app on Lakebase;
  - an online Genie space;
  - a green benchmark run;
  - the handover documentation.

Each check gives a command and its expected result. If a check fails, reproduce it in a unit test first wherever possible, then fix and redeploy.

- [ ] **Step 1: Pause streaming, deploy, run setup, start the app, resume streaming**

```bash
databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=PAUSED
JOB=$(databricks bundle summary -t dev -p fevm-playground -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['id'])")
databricks jobs cancel-all-runs --job-id $JOB -p fevm-playground
scripts/deploy.sh -t dev -p fevm-playground --var streaming_pause_status=PAUSED
databricks bundle run ed_setup -t dev -p fevm-playground
databricks bundle run ed_ops -t dev -p fevm-playground
scripts/deploy.sh -t dev -p fevm-playground --var streaming_pause_status=UNPAUSED
```
Expected:
- The first `deploy.sh` creates the Lakebase project `ed-ops-dev`, the app, and the `ed_genie_benchmarks` job.
- `ed_setup` succeeds with 11 tasks: `create_objects` creates `gold.visit_facts`; `serving_setup` prints `synced tables: all online` and the grant; `genie_space` prints `created space …` and `granted CAN_RUN …`.
- `bundle run ed_ops` deploys the app and reports it RUNNING.

- [ ] **Step 2: Check the synced tables and the app API**

The curl checks need a short-lived OAuth token from `databricks auth token`. **Ask the user before minting it.** If they decline, open the app URL in a browser and check the same endpoints there; the expected results are the same.

```bash
URL=$(databricks apps get $(databricks bundle summary -t dev -p fevm-playground -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['apps']['ed_ops']['name'])") -p fevm-playground -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['url'])")
TOKEN=$(databricks auth token -p fevm-playground | python3 -c "import json,sys;print(json.load(sys.stdin)['access_token'])")
for p in /api/health /api/network/summary /api/facilities /api/heatmap "/api/facilities/FAC001" "/api/facilities/FAC001/history?hours=24" "/api/facilities/FAC001/board"; do
  echo "== $p"; curl -s -H "Authorization: Bearer $TOKEN" "$URL$p" | python3 -c "import json,sys;d=json.load(sys.stdin);print({k:(len(v) if isinstance(v,list) else v) for k,v in d.items()} if isinstance(d,dict) else d)" | cut -c1-300
done
curl -s -o /dev/null -w "index.html %{http_code}\n" -H "Authorization: Bearer $TOKEN" "$URL/"
```
Expected:
- `/api/health` returns `status: ok`, with `data_age_seconds` under 120.
- The summary has `facilities: 50` and a narrative.
- `/api/facilities` returns 50 rows.
- `/api/heatmap` returns about 1,200 cells (50 × 24 or 25).
- The FAC001 history has about 288 points, and its board is non-empty.
- `/` returns `index.html` with status 200.

- [ ] **Step 3: Check Genie through the app and run the benchmarks**

```bash
R=$(curl -s -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"question":"Which facilities are crowded right now?"}' "$URL/api/genie/messages"); echo "$R"
C=$(echo "$R" | python3 -c "import json,sys;print(json.load(sys.stdin)['conversation_id'])"); M=$(echo "$R" | python3 -c "import json,sys;print(json.load(sys.stdin)['message_id'])")
for i in $(seq 1 30); do A=$(curl -s -H "Authorization: Bearer $TOKEN" "$URL/api/genie/conversations/$C/messages/$M"); echo "$A" | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['status'], len(d['rows']), (d['sql'] or '')[:120])"; echo "$A" | grep -q '"done":true' && break; sleep 3; done
databricks bundle run ed_genie_benchmarks -t dev -p fevm-playground
```
Expected:
- The Genie answer reaches `COMPLETED`, with SQL over `ed_gold.facility_current` and rows for the Crowded facilities.
- The benchmark job prints one PASS or FAIL line per question and `genie benchmarks: N% passed`, with N at least 60.

If the pass rate is below 60%, refine the space instructions and examples in `src/ed_ops/genie/space.yml` (never the benchmarks), re-run `--only genie_space`, then re-run the benchmarks.

- [ ] **Step 4: Write the handover docs and remove the per-plan runbooks**

`README.md`:
```markdown
# ED Crowding Command Center (Databricks reference solution)

Real-time emergency-department crowding for a 50-facility health system, built entirely on Databricks:
a synthetic patient-journey simulator, a Structured Streaming medallion pipeline, EDWIN / gEDWIN / NEDOCS crowding
scores, AI Functions on triage notes, AI-written narratives, Lakebase serving, a React + ApexCharts Databricks App and a
Genie space. All data is synthetic and de-identified.

## Deploy (about 1 hour end to end)

    # prerequisites: Databricks CLI authenticated to the workspace, Node 20+, uv, an existing UC catalog and SQL warehouse
    uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e ".[dev]"
    scripts/pytest.sh                                                   # local tests (Spark 4.1 + Java 17)
    scripts/deploy.sh -t dev -p <profile>                               # builds the frontend, deploys jobs, app, Lakebase
    databricks bundle run ed_setup -t dev -p <profile>                  # DDL, reference data, 14-day backfill, AI, serving, Genie
    databricks bundle run ed_ops -t dev -p <profile>                    # start the app
    scripts/deploy.sh -t dev -p <profile> --var streaming_pause_status=UNPAUSED   # go live
    databricks bundle run ed_validate -t dev -p <profile>               # end-to-end proof in ed_validate_* schemas

For a customer workspace use `-t customer` with `--var catalog=... --var warehouse_id=... --var run_as_service_principal=...`.

See `docs/architecture.md` for the design and `docs/runbook.md` for operations, costs and teardown.
The design spec is `docs/superpowers/specs/2026-11-09-ed-crowding-design.md`.
```

`docs/architecture.md`:
```markdown
# Architecture

    generator (rate stream, 1-min ticks) -> UC volume (JSON events)
      -> bronze.ed_events (Auto Loader)
      -> silver.ed_visits (validate, quarantine, de-identify, COALESCE MERGE)    silver.triage_notes (ai_mask -> regex -> ai_classify / ai_extract / ai_query)
      -> gold.facility_current / facility_snapshot_5min / ed_board_current (1-min ticker: EDWIN, gEDWIN, NEDOCS, waits, drivers)
      -> gold.facility_narrative / network_narrative (ai_query every 15 min, grounded in gold metrics)
      -> Lakebase Autoscaling synced tables ({prefix}serving, Continuous) -> FastAPI + React app
      -> Genie space over gold + ref + masked notes (Conversation API proxied by the app on behalf of the user)

- Jobs: ed_setup (serverless, idempotent), ed_streaming (continuous, classic on-demand Photon r7gd.2xlarge, DBR 18 LTS),
  ed_validate (serverless, end-to-end in ed_validate_* schemas), ed_genie_benchmarks (serverless).
- Metrics: one implementation (`ed_ops.metrics.snapshots.compute_snapshots`) for live ticks, backfill and validation.
  Definitions and assumptions are on the app's Definitions page (`app/backend/definitions.py`).
- PHI: raw only in bronze and sim; structured fields de-identified deterministically; free text masked with ai_mask plus a
  regex backstop; gold, ref, Lakebase, the app and Genie are PHI-free. No UC column masks or ABAC.
- Governance: every table, view and column commented and tagged (regular tags, keys prefixed `ed_`), enforced by
  `governance_checks` (comments, tags, tag values, raw-PHI tags, PHI content scan, ai_mask miss rate).
```

`docs/runbook.md`:
```markdown
# Runbook

Workspace: dev = https://fevm-hls-jagjit-lb-playground.cloud.databricks.com (profile fevm-playground).
Catalog: hls_jagjit_lb_playground_catalog (existing; never created or dropped). Schemas: ed_bronze, ed_silver, ed_gold,
ed_ref, ed_sim, ed_serving. Tag keys are prefixed ed_.

## Start / stop live streaming (the classic cluster bills while running)
    scripts/deploy.sh -t dev -p fevm-playground --var streaming_pause_status=UNPAUSED
    scripts/deploy.sh -t dev -p fevm-playground --var streaming_pause_status=PAUSED
    JOB=$(databricks bundle summary -t dev -p fevm-playground -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['id'])")
    databricks jobs cancel-all-runs --job-id $JOB -p fevm-playground      # pausing alone leaves the active run up

## Re-run setup (safe; pause streaming first - AI/driver/narrative tasks refuse while it runs)
    databricks bundle run ed_setup -t dev -p fevm-playground
    databricks bundle run ed_setup --only genie_space -t dev -p fevm-playground    # refresh Genie instructions only

## App
    databricks bundle run ed_ops -t dev -p fevm-playground                 # (re)deploy and start the app
    databricks apps logs <app name> -p fevm-playground --tail-lines 100

## Health checks
    scripts/run_sql.py "SELECT max(computed_at), current_timestamp() FROM hls_jagjit_lb_playground_catalog.ed_gold.facility_current"
    scripts/run_sql.py "SELECT ai_status, count(*) FROM hls_jagjit_lb_playground_catalog.ed_silver.triage_notes GROUP BY 1"
    databricks bundle run ed_validate -t dev -p fevm-playground            # end-to-end, cleans up after itself
    databricks bundle run ed_genie_benchmarks -t dev -p fevm-playground    # Genie accuracy (>= 60% to pass)

## Costs (all knobs are bundle variables)
- ed_streaming: one on-demand Photon r7gd.2xlarge driver + 1 worker, always on while UNPAUSED.
- AI: ~2.4 triage notes/min x 5 AI calls; narratives 51 ai_query calls every narrative_minutes (15).
- Lakebase: 0.5-2 CU autoscaling; Continuous sync keeps it awake while streaming.
- Volume: tune facility_count, backfill_days, ai_backfill_days, tick_seconds.

## Reset streams (fresh checkpoints, keep data; note bronze re-ingests landing files)
    scripts/deploy.sh -t dev -p fevm-playground --var checkpoint_version=2 --var streaming_pause_status=UNPAUSED

## Tear down
    scripts/deploy.sh -t dev -p fevm-playground --var streaming_pause_status=PAUSED   # then cancel-all-runs as above
    scripts/teardown.sh ed_ --yes                      # synced tables, Genie space, then the ed_* schemas
    databricks bundle destroy -t dev -p fevm-playground   # jobs, app, Lakebase project
```

```bash
git rm -q docs/runbook-plan1.md docs/runbook-plan2.md
git add README.md docs/runbook.md docs/architecture.md
git commit -m "docs: README, consolidated runbook and architecture for customer handover" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Self-Review Notes (completed by the plan author)

**Spec coverage:**

| Spec section | Where this plan covers it |
|---|---|
| §10 `gold.visit_facts` | Task 1 |
| §12 Lakebase: project, synced tables with PKs and modes, app grants, reserved write-back via the app role's `CREATE` | Tasks 2 and 6 |
| §13.1 backend: endpoints, pool with token refresh, cache, Genie OBO, data-delayed health | Task 4 |
| §13.2 frontend: overview (narrative banner, KPI tiles, ranked bar with thresholds, bubble, heatmap, sortable table with sparklines), facility detail (three gauges, D2P with low-sample, trends 24 h / 7 d with bands, ESI area, narrative and drivers, board with under-triage), Genie panel, Definitions | Task 5 |
| §14 Genie: tables, instructions, 10 examples, benchmarks (12) | Task 3 |
| §17 bundle: app, Lakebase, customer target, `run_as`, variables, teardown | Task 6 |
| §18 docs | Task 7 |

**Not covered by any plan:** a geographic map (a spec non-goal) and user write-back (spec "for now: read-only").
