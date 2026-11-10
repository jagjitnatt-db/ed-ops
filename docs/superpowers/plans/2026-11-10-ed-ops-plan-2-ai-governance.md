# ED Ops — Plan 2: AI Enrichment, Narratives, Governance and Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add AI Functions enrichment of triage notes (masking, classification, vitals extraction and suggested ESI, with a retry sweep), AI-written crowding narratives per facility and for the whole network, automated governance checks, and an `ed_validate` job that proves the system end to end on the workspace.

**Architecture:**
- **AI calls** go through a small `AiFunctions` interface. `DatabricksAi` builds the real `ai_mask` / `ai_classify` / `ai_extract` / `ai_query` SQL expressions; tests use a local `FakeAi` that returns outputs in the same shapes. That makes every transform around the LLM calls testable on local Spark.
- **New streams** join the existing `ed_streaming` job: `silver_notes_ai`, `ai_retry_sweep` and `gold_narratives`.
- **Setup:** `ed_setup` gains notes backfill, driver refresh, narratives and governance-check tasks, guarded so re-running it never fights the live streams.
- **Validation:** `ed_validate` runs the whole pipeline at small scale in `ed_validate_*` schemas, checks the golden scenario on the workspace runtime, runs the governance and pipeline assertions, then tears its schemas down.

**Tech Stack:** Python 3.12, PySpark 4.1 (local Spark 4.1.2; DBR 18 LTS Photon on workspace), Databricks AI Functions on `databricks-claude-sonnet-5-5`, Delta Lake, Structured Streaming, Unity Catalog `information_schema`, Databricks Asset Bundles, pytest.

**Spec:** `docs/superpowers/specs/2026-11-09-ed-crowding-design.md`. This plan implements:
- §7.4 driver naming (`arrival_surge_ratio`);
- §8 (`silver_notes_ai`, `ai_retry_sweep`, `gold_narratives`);
- §9 AI functions;
- §10 narrative tables;
- §11.3–11.5 tags and checks;
- §16.3 `ed_validate`.

**Builds on Plan 1** (merged to `main`): `docs/superpowers/plans/2026-11-09-ed-ops-plan-1-core-data-platform.md`. Plan 3 covers Lakebase, the app, Genie, the `gold.visit_facts` view and the customer bundle target.

## Global Constraints

- **Everything in Plan 1's Global Constraints still holds**, including:
  - naive-UTC datetimes;
  - the existing catalog `<catalog>`, never created or dropped;
  - `{schema_prefix}{logical}` schemas;
  - liquid clustering on MERGE keys;
  - no deletes;
  - no UC column masks or ABAC;
  - ANSI-safe divisions;
  - the commit trailer `Co-authored-by: Isaac <no-reply@databricks.com>`.
- **Run tests with `scripts/pytest.sh`.** It pins `TZ=UTC` and wires the local Homebrew Spark 4.1.2.
- **Classic clusters:** on-demand (`aws_attributes.availability: ON_DEMAND`), Photon (`runtime_engine: PHOTON`), `r7gd.2xlarge` for driver and workers, and `spark_version: 18.x-scala2.13` (never the `-aarch64-` key). `ed_setup` and `ed_validate` run on serverless.
- **Tags are regular UC tags only, with no tag policies.** Keys carry `cfg.tag_prefix` (default `ed_`); checks look up `prefix + logical_key`.
- **LLM endpoint:** bundle variable `llm_endpoint`, default `databricks-claude-sonnet-5-5`.
  - **Never pass `responseFormat`:** Claude 5.x rejects it. Prompts ask for a JSON object only, and replies are parsed with `from_json(regexp_extract(result, '(?s)(\{.*\})', 1), schema)`.
  - **Always call `ai_query` with `failOnError => false`**, so it returns `struct<result, errorMessage>`.
- **`ai_extract` returns strings with units** (`"89% RA"`, `"9/10"`, `"37.1C"`, `"92/58"`). Convert them to typed columns with regex plus `try_cast`.
- **Order of AI calls:** `ai_mask` runs first, then the deterministic regex post-check. Every other AI call sees only masked text. Raw note text never reaches silver.
- **Notes older than `ai_backfill_days`** (default 2) are stored with `note_masked`/`chief_complaint_masked` null, `ai_enriched = false`, `ai_status = 'skipped'` (spec §9.1 as amended).
- **A successful AI result is never overwritten:** the notes MERGE only updates target rows whose `ai_status` isn't `'ok'`.
- **Retries:** at most 3 attempts (`MAX_AI_ATTEMPTS`), on rows with `ai_status IN ('error', 'parse_error')`.

**Deviations from spec, decided in this plan:**
1. `ed_validate` checks the golden scenario by running `compute_snapshots` on the workspace runtime (serverless Photon) over the in-memory golden DataFrames. It does not load a test facility into the validate tables. The goal is the same (runtime parity with the hand-calculated values) without mixing a test facility into pipeline tables.
2. `ed_validate` runs the pipeline in batch (`availableNow`) mode and skips the spec's "about 10 live ticks". The live path is already verified on `ed_streaming` (Plan 1, Task 17), and keeping validation batch-only makes it deterministic and cheap.
3. The `top_drivers` entry for arrivals is named `arrival_surge_ratio`. Its `value` is the ratio, its `baseline` is `1.0`, and its `z` is computed from `arrivals_1h` against its local-hour baseline. That keeps the spec's name and lets narratives say "1.5× normal arrivals".
4. Plan 1's deferred minor "quarantine append not idempotent on replay" is fixed here, because `ed_validate` asserts on quarantine counts.
5. Plan 1's deferred minor "`ed_setup` unsafe while streaming" is fixed here, because Plan 2 re-runs `ed_setup` against the live dev deployment.

## Review Focus

1. **A note whose `ai_mask` misses a phone number or MRN** must still store redacted text and set `residual_phi_flag = true` (test in Task 4).
2. **An LLM reply wrapped in prose or code fences, or spread across several lines**, must still parse. Garbage must give `parse_error`, never a crash (test in Task 4).
3. **A retry that succeeds after earlier failures** must flip the row to `ok` and must never downgrade an already-`ok` row (test in Task 5, via the MERGE SQL).
4. **A narrative refresh where one facility's reply fails to parse** must keep that facility's previous narrative and update the others (test in Task 6).
5. **Re-running `ed_setup` on a deployment whose generator has gone live** must skip generate, ingest and gold, and refuse the notes and narratives backfills while streaming is active (test in Task 9).

---

## File Structure

```
src/ed_ops/config.py                    + ai_backfill_days, llm_endpoint, narrative_minutes, ai_retry_minutes
src/ed_ops/metrics/drivers.py           arrival driver renamed to arrival_surge_ratio
src/ed_ops/metadata/tables.yml          + gold.facility_narrative, gold.network_narrative; top_drivers comment
src/ed_ops/ai.py                        AiFunctions protocol + DatabricksAi (SQL expression builders)
src/ed_ops/pipeline/silver_notes_ai.py  note masking/enrichment transforms, notes stream, retry sweep
src/ed_ops/pipeline/gold_narratives.py  facility + network narrative transforms and stream
src/ed_ops/pipeline/gold_metrics.py     + refresh_drivers()
src/ed_ops/pipeline/silver_visits.py    idempotent quarantine append (txnAppId/txnVersion)
src/ed_ops/delta.py                     append_into(..., txn=None)
src/ed_ops/governance/checks.py         comment/tag/PHI-tag checks, PHI content scan, ai_mask miss rate
src/ed_ops/validation/__init__.py
src/ed_ops/validation/golden.py         golden visit scenario + hand-calculated expectations
src/ed_ops/validation/pipeline.py       pipeline assertions for ed_validate
src/ed_ops/setup.py                     + new tasks, live-deployment guards
src/ed_ops/streaming.py                 + run_notes_ai, run_ai_retry, run_narratives
src/ed_ops/cli.py                       + 10 entry points
pyproject.toml                          + 10 console scripts
databricks.yml                          + AI/narrative variables
resources/jobs.yml                      ed_setup + ed_streaming tasks
resources/validate.yml                  ed_validate job
tests/fake_ai.py                        local FakeAi with Databricks-shaped outputs
tests/factories.py                      scenario re-exported from validation/golden.py
tests/unit/test_*.py                    new and updated tests
docs/runbook-plan2.md                   operator notes for AI, narratives and validation
```

---

### Task 1: Config additions, the `arrival_surge_ratio` driver, and `refresh_drivers`

**Files:**
- Modify: `src/ed_ops/config.py`, `src/ed_ops/metrics/drivers.py`, `src/ed_ops/pipeline/gold_metrics.py`, `src/ed_ops/metadata/tables.yml`
- Test: `tests/unit/test_config.py`, `tests/unit/test_drivers.py`

**Interfaces:**
- Produces:
  - `Config.ai_backfill_days: int = 2`, `Config.llm_endpoint: str = "databricks-claude-sonnet-5-5"`, `Config.narrative_minutes: int = 15`, `Config.ai_retry_minutes: int = 15`
  - `drivers.DRIVER_NAMES = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrival_surge_ratio", "high_acuity_share")`
  - `gold_metrics.refresh_drivers(spark, cfg) -> None`: recomputes `top_drivers` and `arrival_surge_ratio` for all of `facility_snapshot_5min` and `facility_current`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_config.py`:
```python


def test_ai_and_narrative_defaults():
    cfg = Config()
    assert (cfg.ai_backfill_days, cfg.llm_endpoint, cfg.narrative_minutes, cfg.ai_retry_minutes) == (
        2, "databricks-claude-sonnet-5-5", 15, 15
    )
    parsed, _ = parse_config(["--llm-endpoint=databricks-claude-haiku-5-5", "--ai_backfill_days=1"])
    assert parsed.llm_endpoint == "databricks-claude-haiku-5-5" and parsed.ai_backfill_days == 1
```

In `tests/unit/test_drivers.py`, replace these four lines in `test_top_drivers_ranked_by_absolute_z`:
```python
    assert [d["driver"] for d in top] == ["occupancy_ratio", "arrivals_1h", "high_acuity_share"]
    assert top[0]["z"] == pytest.approx(3.0)
    assert top[0]["baseline"] == pytest.approx(0.5) and top[0]["value"] == pytest.approx(0.8)
    assert out[datetime(2026, 11, 9, 18)]["arrival_surge_ratio"] == pytest.approx(16 / 12)
```
with:
```python
    assert [d["driver"] for d in top] == ["occupancy_ratio", "arrival_surge_ratio", "high_acuity_share"]
    assert top[0]["z"] == pytest.approx(3.0)
    assert top[0]["baseline"] == pytest.approx(0.5) and top[0]["value"] == pytest.approx(0.8)
    surge = top[1]
    assert surge["value"] == pytest.approx(16 / 12) and surge["baseline"] == 1.0 and surge["z"] == pytest.approx(2.0)
    assert out[datetime(2026, 11, 9, 18)]["arrival_surge_ratio"] == pytest.approx(16 / 12)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_config.py tests/unit/test_drivers.py -v`
Expected:
- `test_ai_and_narrative_defaults` fails with `AttributeError: 'Config' object has no attribute 'ai_backfill_days'`.
- `test_top_drivers_ranked_by_absolute_z` fails because the driver list contains `'arrivals_1h'`.

- [ ] **Step 3: Implement**

In `src/ed_ops/config.py`, add after the `tag_prefix` field:
```python
    ai_backfill_days: int = 2
    llm_endpoint: str = "databricks-claude-sonnet-5-5"
    narrative_minutes: int = 15
    ai_retry_minutes: int = 15
```

In `src/ed_ops/metrics/drivers.py`, replace the line
```python
DRIVER_COLUMNS = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrivals_1h", "high_acuity_share")
```
with:
```python
# Snapshot columns that get a local-hour baseline (mean/std) ...
DRIVER_COLUMNS = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrivals_1h", "high_acuity_share")
# ... and the driver names reported in top_drivers (spec 7.4): arrivals are reported as arrival_surge_ratio.
DRIVER_NAMES = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrival_surge_ratio", "high_acuity_share")
```
Then replace the whole `candidates = F.array(...)` statement in `add_drivers`:
```python
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
```
with:
```python
    surge = F.try_divide(F.col("arrivals_1h").cast("double"), F.col("arrivals_1h_mean"))

    def z(d: str):
        return F.try_divide(F.col(d).cast("double") - F.col(f"{d}_mean"), F.col(f"{d}_std"))

    def entry(name: str, value, baseline, zscore):
        return F.struct(F.lit(name).alias("driver"), value.cast("double").alias("value"),
                        baseline.cast("double").alias("baseline"), zscore.alias("z"))

    candidates = F.array(
        *[
            entry("arrival_surge_ratio", surge, F.lit(1.0), z(d)) if d == "arrivals_1h"
            else entry(d, F.col(d), F.col(f"{d}_mean"), z(d))
            for d in DRIVER_COLUMNS
        ]
    )
```
and replace
```python
    df = df.withColumn("top_drivers", F.slice(ordered, 1, 3)).withColumn(
        "arrival_surge_ratio", F.try_divide(F.col("arrivals_1h").cast("double"), F.col("arrivals_1h_mean"))
    )
```
with:
```python
    df = df.withColumn("top_drivers", F.slice(ordered, 1, 3)).withColumn("arrival_surge_ratio", surge)
```

In `src/ed_ops/pipeline/gold_metrics.py`, add after `backfill_snapshots`:
```python
def refresh_drivers(spark: SparkSession, cfg: Config) -> None:
    """Recompute top_drivers / arrival_surge_ratio over all stored history and the current rows (idempotent)."""
    history = spark.table(cfg.table("gold", "facility_snapshot_5min"))
    baselines = _materialize(compute_baselines(history, cfg.timezone))
    merge_into(add_drivers(history, baselines, cfg.timezone), cfg, "gold", "facility_snapshot_5min")
    current = spark.table(cfg.table("gold", "facility_current"))
    merge_into(add_drivers(current, baselines, cfg.timezone), cfg, "gold", "facility_current")
```

In `src/ed_ops/metadata/tables.yml`, in the `top_drivers` column of `gold.facility_snapshot_5min`, replace the comment sentence `Drivers: occupancy_ratio, boarder_share, patients_per_attending, arrivals_1h, high_acuity_share.` with `Drivers: occupancy_ratio, boarder_share, patients_per_attending, arrival_surge_ratio (value = arrivals_1h / local-hour mean, baseline 1.0, z from arrivals_1h), high_acuity_share.`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_config.py tests/unit/test_drivers.py tests/unit/test_ddl.py -v`
Expected: all pass (7 config + 3 drivers + 10 ddl = 20 passed).

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/config.py src/ed_ops/metrics/drivers.py src/ed_ops/pipeline/gold_metrics.py src/ed_ops/metadata/tables.yml tests/unit/test_config.py tests/unit/test_drivers.py
git commit -m "feat: AI/narrative config, arrival_surge_ratio driver and refresh_drivers" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Narrative tables in the metadata

**Files:**
- Modify: `src/ed_ops/metadata/tables.yml`
- Test: `tests/unit/test_ddl.py`

**Interfaces:**
- Produces: tables `gold.facility_narrative` (PK `facility_id`) and `gold.network_narrative` (PK `network_id`). Their column lists, below, are the contract for Task 6.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_ddl.py`:
```python


def test_narrative_tables_defined():
    cat = load_catalog_def()
    fac = cat.table("gold", "facility_narrative")
    net = cat.table("gold", "network_narrative")
    assert fac.primary_key == ("facility_id",) and fac.cdf and fac.cluster_by == ("facility_id",)
    assert net.primary_key == ("network_id",) and net.cdf
    assert fac.column_names == [
        "facility_id", "snapshot_ts", "gedwin_band", "headline", "explanation", "recommended_actions",
        "top_drivers", "input_json", "model", "generated_at",
    ]
    assert net.column_names == [
        "network_id", "snapshot_ts", "crowded_n", "busy_n", "active_n", "headline", "explanation",
        "crowded_facilities", "doing_well_facilities", "recommended_actions", "input_json", "model", "generated_at",
    ]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `scripts/pytest.sh tests/unit/test_ddl.py::test_narrative_tables_defined -v`
Expected: FAIL with `KeyError: 'no table gold.facility_narrative in metadata'`.

- [ ] **Step 3: Add the tables**

Append to the end of `src/ed_ops/metadata/tables.yml`, at the same indentation as the other `tables:` entries:
```yaml

  - schema: gold
    name: facility_narrative
    comment: "AI-written plain-English crowding status per facility. ai_query sees only computed metrics and drivers; refreshed every narrative_minutes. A refresh that fails to parse keeps the previous narrative."
    primary_key: [facility_id]
    cluster_by: [facility_id]
    cdf: true
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: facility_id, type: STRING, nullable: false, comment: "Facility identifier (FACnnn)."}
      - {name: snapshot_ts, type: TIMESTAMP, comment: "facility_current snapshot the narrative describes, UTC."}
      - {name: gedwin_band, type: STRING, comment: "Headline crowding band at snapshot_ts."}
      - {name: headline, type: STRING, comment: "One-line status (at most 12 words)."}
      - {name: explanation, type: STRING, comment: "Why the ED is in this state (at most 60 words), citing only computed metrics."}
      - {name: recommended_actions, type: "ARRAY<STRING>", comment: "Up to 3 suggested operational actions."}
      - {name: top_drivers, type: "ARRAY<STRUCT<driver: STRING, value: DOUBLE, baseline: DOUBLE, z: DOUBLE>>", comment: "Drivers given to the model (copy of facility_current.top_drivers)."}
      - {name: input_json, type: STRING, comment: "Exact JSON sent to the model, so every claim in the narrative can be audited."}
      - {name: model, type: STRING, comment: "Model serving endpoint that wrote the narrative."}
      - {name: generated_at, type: TIMESTAMP, comment: "When the narrative was generated, UTC."}

  - schema: gold
    name: network_narrative
    comment: "AI-written network-wide summary of which facilities are crowded and why, and which are doing well. One row (network_id = 'NETWORK'); the previous summary is kept when a refresh fails to parse."
    primary_key: [network_id]
    cluster_by: [network_id]
    cdf: true
    tags: {data_classification: internal, contains_phi: "false"}
    columns:
      - {name: network_id, type: STRING, nullable: false, comment: "Constant NETWORK (single row)."}
      - {name: snapshot_ts, type: TIMESTAMP, comment: "Latest facility_current snapshot summarised, UTC."}
      - {name: crowded_n, type: INT, comment: "Facilities with gedwin_band = Crowded."}
      - {name: busy_n, type: INT, comment: "Facilities with gedwin_band = Busy."}
      - {name: active_n, type: INT, comment: "Facilities with gedwin_band = Active."}
      - {name: headline, type: STRING, comment: "One-line network status (at most 15 words)."}
      - {name: explanation, type: STRING, comment: "Which facilities are crowded and why, and which are doing well (at most 80 words)."}
      - {name: crowded_facilities, type: "ARRAY<STRING>", comment: "Facility names the model lists as crowded (only gEDWIN Crowded facilities qualify)."}
      - {name: doing_well_facilities, type: "ARRAY<STRING>", comment: "Facility names the model lists as doing well."}
      - {name: recommended_actions, type: "ARRAY<STRING>", comment: "Up to 3 network-level actions."}
      - {name: input_json, type: STRING, comment: "Exact JSON sent to the model."}
      - {name: model, type: STRING, comment: "Model serving endpoint that wrote the summary."}
      - {name: generated_at, type: TIMESTAMP, comment: "When the summary was generated, UTC."}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_ddl.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/metadata/tables.yml tests/unit/test_ddl.py
git commit -m "feat: facility and network narrative tables" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: The AI Functions interface and a local fake

**Files:**
- Create: `src/ed_ops/ai.py`, `tests/fake_ai.py`
- Test: `tests/unit/test_ai.py`

**Interfaces:**
- Produces:
  - `ai.MASK_LABELS`, `ai.CATEGORIES`, `ai.VITAL_LABELS` (tuples of strings)
  - `ai.AiFunctions` (Protocol), with `endpoint: str` and four methods:
    - `mask(column: str) -> Column`
    - `classify(column: str) -> Column`
    - `extract(column: str) -> Column`: a struct of strings, with fields named as `VITAL_LABELS`
    - `query(column: str) -> Column`: `struct<result: string, errorMessage: string>`

    Each method takes a **column name**.
  - `ai.DatabricksAi(endpoint)` implementing it with `F.expr`
  - `tests/fake_ai.FakeAi(miss_phone=False, fail_marker="FAILME", garbage_marker="GARBAGE")`, which uses deterministic Spark expressions with the same output shapes

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_ai.py`:
```python
from pyspark.sql import functions as F

from ed_ops.ai import CATEGORIES, MASK_LABELS, VITAL_LABELS, DatabricksAi
from tests.fake_ai import FakeAi


def test_databricks_expressions(spark):
    ai = DatabricksAi("databricks-claude-sonnet-5-5")
    assert "ai_mask(`note`, array('person'" in str(ai.mask("note"))
    assert "'medical record number'" in str(ai.mask("note"))
    assert "ai_classify(`note_masked`, array('cardiac'" in str(ai.classify("note_masked"))
    assert "ai_extract(`note_masked`, array('heart_rate'" in str(ai.extract("note_masked"))
    q = str(ai.query("_prompt"))
    assert "ai_query('databricks-claude-sonnet-5-5', `_prompt`" in q and "failOnError" in q
    assert "responseFormat" not in q
    assert len(CATEGORIES) == 10 and "person" in MASK_LABELS and VITAL_LABELS[0] == "heart_rate"


def test_fake_ai_output_shapes(spark):
    df = spark.createDataFrame([("Pt Pat1 Tester MRN-7000001 ph 312-555-0001 crushing chest pain HR 118, BP 92/58, RR 24, SpO2 89% RA, T 37.1C. Pain 9/10.",)], "note STRING")
    ai = FakeAi()
    row = df.select(
        ai.mask("note").alias("m"), ai.classify("note").alias("c"), ai.extract("note").alias("x"),
        ai.query("note").alias("q"),
    ).collect()[0]
    assert "Pat1" not in row.m and "MRN-7000001" not in row.m and "[MASKED]" in row.m
    assert row.c == "cardiac"
    assert row.x.heart_rate == "118" and row.x.spo2 == "89% RA" and row.x.pain_score == "9/10"
    assert set(row.q.asDict()) == {"result", "errorMessage"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_ai.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.ai'`.

- [ ] **Step 3: Implement the interface and the fake**

`src/ed_ops/ai.py`:
```python
"""Databricks AI Functions behind a small interface so every transform around them is testable locally.

Methods take a column *name* (AI functions are SQL functions; ai_query needs the named argument
failOnError => false, which the DataFrame API cannot express). Never pass responseFormat: Claude 5.x
endpoints reject it; callers ask for JSON in the prompt and parse with from_json.
"""

from __future__ import annotations

from typing import Protocol

from pyspark.sql import Column
from pyspark.sql import functions as F

from ed_ops.ddl.generate import sql_string

MASK_LABELS = ("person", "phone number", "date of birth", "address", "medical record number", "email")
CATEGORIES = (
    "cardiac", "respiratory", "neurological", "trauma", "gastrointestinal", "infectious", "psychiatric",
    "obstetric", "musculoskeletal", "other",
)
VITAL_LABELS = ("heart_rate", "blood_pressure", "respiratory_rate", "spo2", "temperature", "pain_score", "symptom_onset")


def _array(values: tuple[str, ...]) -> str:
    return "array(" + ", ".join(sql_string(v) for v in values) + ")"


class AiFunctions(Protocol):
    endpoint: str

    def mask(self, column: str) -> Column: ...

    def classify(self, column: str) -> Column: ...

    def extract(self, column: str) -> Column: ...

    def query(self, column: str) -> Column: ...


class DatabricksAi:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    def mask(self, column: str) -> Column:
        return F.expr(f"ai_mask(`{column}`, {_array(MASK_LABELS)})")

    def classify(self, column: str) -> Column:
        return F.expr(f"ai_classify(`{column}`, {_array(CATEGORIES)})")

    def extract(self, column: str) -> Column:
        return F.expr(f"ai_extract(`{column}`, {_array(VITAL_LABELS)})")

    def query(self, column: str) -> Column:
        return F.expr(f"ai_query({sql_string(self.endpoint)}, `{column}`, failOnError => false)")
```

`tests/fake_ai.py`:
```python
"""Local stand-in for Databricks AI Functions: deterministic Spark expressions with the same output shapes."""

from pyspark.sql import Column
from pyspark.sql import functions as F

from ed_ops.ai import VITAL_LABELS

NAME = r"Pat\d+ Tester"
MRN = r"MRN-\d+"
PHONE = r"\d{3}-555-\d{4}"
DOB = r"\d{2}/\d{2}/\d{4}"

ESI_MARKER = "Emergency Severity Index"
FACILITY_MARKER = "single emergency department"
NETWORK_MARKER = "health system"


class FakeAi:
    endpoint = "fake-endpoint"

    def __init__(self, miss_phone: bool = False, fail_marker: str | None = "FAILME", garbage_marker: str | None = "GARBAGE"):
        self.miss_phone = miss_phone
        self.fail_marker = fail_marker
        self.garbage_marker = garbage_marker

    def mask(self, column: str) -> Column:
        c = F.col(column)
        for pattern in (NAME, MRN, DOB) + (() if self.miss_phone else (PHONE,)):
            c = F.regexp_replace(c, pattern, "[MASKED]")
        return c

    def classify(self, column: str) -> Column:
        c = F.col(column)
        return F.when(c.rlike("(?i)chest"), F.lit("cardiac")).when(c.rlike("(?i)SOB|breath"), F.lit("respiratory")).otherwise(F.lit("other"))

    def extract(self, column: str) -> Column:
        c = F.col(column)
        patterns = {
            "heart_rate": r"HR (\d+)",
            "blood_pressure": r"BP (\d+/\d+)",
            "respiratory_rate": r"RR (\d+)",
            "spo2": r"SpO2 (\d+% ?[A-Za-z]*)",
            "temperature": r"T ([\d.]+[CF])",
            "pain_score": r"(\d+/10)",
            "symptom_onset": r"x (\d+ \w+)",
        }
        return F.struct(*[F.nullif(F.regexp_extract(c, patterns[k], 1), F.lit("")).alias(k) for k in VITAL_LABELS])

    def _reply(self, prompt: Column) -> Column:
        esi = F.concat(
            F.lit('Sure! ```json\n{"suggested_esi": '),
            F.when(prompt.rlike("(?i)crushing|stroke|sepsis"), F.lit("1")).otherwise(F.lit("3")),
            F.lit(',\n "rationale": "fake rationale"}\n```'),
        )
        facility = F.lit('{"headline": "Fake facility status", "explanation": "Census and waits from the input.", '
                         '"recommended_actions": ["a", "b", "c", "d"]}')
        network = F.lit('{"headline": "Fake network status", "explanation": "Two EDs crowded.", '
                        '"crowded_facilities": ["X"], "doing_well_facilities": ["Y"], "recommended_actions": ["z"]}')
        return (
            F.when(prompt.contains(ESI_MARKER), esi)
            .when(prompt.contains(FACILITY_MARKER), facility)
            .when(prompt.contains(NETWORK_MARKER), network)
            .otherwise(F.lit("{}"))
        )

    def query(self, column: str) -> Column:
        prompt = F.col(column)
        failing = prompt.contains(self.fail_marker) if self.fail_marker else F.lit(False)
        garbage = prompt.contains(self.garbage_marker) if self.garbage_marker else F.lit(False)
        result = F.when(failing, F.lit(None).cast("string")).when(garbage, F.lit("I cannot help with that.")).otherwise(self._reply(prompt))
        error = F.when(failing, F.lit("fake endpoint error")).otherwise(F.lit(None).cast("string"))
        return F.struct(result.alias("result"), error.alias("errorMessage"))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_ai.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/ai.py tests/fake_ai.py tests/unit/test_ai.py
git commit -m "feat: AI Functions interface with Databricks expressions and a local fake" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: Triage-note masking and enrichment transforms

**Files:**
- Create: `src/ed_ops/pipeline/silver_notes_ai.py`, `tests/bronze_factories.py`
- Test: `tests/unit/test_notes_ai.py`

**Interfaces:**
- Consumes:
  - `AiFunctions` (Task 3)
  - `conform`, `table_def` (Plan 1 `delta.py`)
  - `validate_events` (Plan 1 `silver_visits.py`)
  - `EVENT_SCHEMA_DDL` (Plan 1)
- Produces in `silver_notes_ai`:
  - `PHI_PATTERNS`, `REDACTED = "[REDACTED]"`, `ESI_PROMPT`, `ESI_SCHEMA`, `MAX_AI_ATTEMPTS = 3`, `NOTE_KEYS`
  - `phi_hit(text) -> Column`, `regex_redact(text) -> Column`, `parse_json_reply(result, schema) -> Column`
  - `parse_vitals(vitals_struct) -> dict[str, Column]`
  - `triage_events(batch_df, facility_ids) -> DataFrame`: valid, deduplicated TRIAGE events
  - `mask_stage(events, ai) -> DataFrame`, with columns `facility_id, visit_id, triage_ts, esi_level, note_masked, chief_complaint_masked, residual_phi_flag`
  - `enrich_stage(masked, ai) -> DataFrame`
  - `skipped_rows(events) -> DataFrame`
  - `notes_rows(events, ai, enrich_since) -> DataFrame`: exactly the `silver.triage_notes` columns, with `ai_attempts = 1`
- Produces in `tests/bronze_factories.py`: `BRONZE_DDL`, `payload(...)`, `ev(...)`, `triage(...)`

- [ ] **Step 1: Write the bronze-event test factory**

`tests/bronze_factories.py`:
```python
"""Builders for bronze.ed_events-shaped rows (shared by silver tests)."""

from ed_ops.events_contract import EVENT_SCHEMA_DDL

BRONZE_DDL = EVENT_SCHEMA_DDL + ", _rescued_data STRING, _source_file STRING, _ingest_ts TIMESTAMP, ingest_date DATE"


def payload(arrival_mode=None, patient_name=None, mrn=None, dob=None, sex=None, phone=None, address=None,
            chief=None, esi=None, note=None, bed=None, provider=None, disposition=None):
    return (arrival_mode, patient_name, mrn, dob, sex, phone, address, chief, esi, note, bed, provider, disposition)


def ev(event_id, etype, ts, visit_id="FAC001-202611091100-001", facility_id="FAC001", pl=None, rescued=None):
    return (event_id, etype, ts, ts, facility_id, visit_id, pl or payload(), rescued, "f.json", ts, ts.date() if ts else None)


def triage(event_id, ts, note, esi=3, chief="chest pain", visit_id=None, facility_id="FAC001"):
    return ev(event_id, "TRIAGE", ts, visit_id=visit_id or f"{facility_id}-{event_id}", facility_id=facility_id,
              pl=payload(esi=esi, note=note, chief=chief))
```

- [ ] **Step 2: Write the failing tests**

`tests/unit/test_notes_ai.py`:
```python
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from ed_ops.delta import table_def
from ed_ops.pipeline.silver_notes_ai import (
    notes_rows, parse_json_reply, parse_vitals, regex_redact, triage_events,
)
from tests.bronze_factories import BRONZE_DDL, ev, payload, triage
from tests.fake_ai import FakeAi

NOW = datetime(2026, 11, 9, 12)
SINCE = NOW - timedelta(days=2)
NOTE = ("Pat1 Tester (68F), DOB 03/14/1958, MRN-7000001. BIBA c/o crushing substernal chest pain x 40 min, "
        "diaphoretic. HR 118, BP 92/58, RR 24, SpO2 89% RA, T 37.1C. Pain 9/10. Callback 312-555-0001.")


def _rows(spark, events, ai=None):
    df = spark.createDataFrame(events, BRONZE_DDL)
    return {r.visit_id: r for r in notes_rows(df, ai or FakeAi(), SINCE).collect()}


def test_recent_note_is_masked_then_enriched(spark):
    r = _rows(spark, [triage("a", NOW - timedelta(minutes=5), NOTE, esi=4, chief="cp - pt Pat1 Tester, cb 312-555-0001")])["FAC001-a"]
    assert "Pat1" not in r.note_masked and "MRN-7000001" not in r.note_masked and "312-555-0001" not in r.note_masked
    assert "Pat1" not in r.chief_complaint_masked
    assert r.residual_phi_flag is False
    assert r.complaint_category == "cardiac"
    assert (r.heart_rate, r.bp_systolic, r.bp_diastolic, r.respiratory_rate, r.spo2, r.temperature_c, r.pain_score) == (
        118, 92, 58, 24, 89, 37.1, 9
    )
    assert r.ai_status == "ok" and r.suggested_esi == 1 and r.esi_rationale == "fake rationale"
    assert r.undertriage_flag is True  # nurse ESI 4 vs suggested 1
    assert r.ai_enriched is True and r.ai_attempts == 1 and r.ai_error is None


def test_residual_phi_missed_by_ai_mask_is_redacted_and_flagged(spark):
    r = _rows(spark, [triage("b", NOW, NOTE)], FakeAi(miss_phone=True))["FAC001-b"]
    assert r.residual_phi_flag is True
    assert "312-555-0001" not in r.note_masked and "[REDACTED]" in r.note_masked


def test_endpoint_error_and_unparseable_reply(spark):
    rows = _rows(spark, [triage("c", NOW, NOTE + " FAILME"), triage("d", NOW, NOTE + " GARBAGE")])
    assert rows["FAC001-c"].ai_status == "error" and rows["FAC001-c"].ai_error == "fake endpoint error"
    assert rows["FAC001-c"].suggested_esi is None and rows["FAC001-c"].undertriage_flag is None
    assert rows["FAC001-d"].ai_status == "parse_error" and "cannot help" in rows["FAC001-d"].ai_error
    assert rows["FAC001-d"].note_masked is not None  # masked text kept so the retry sweep can re-run enrichment


def test_old_notes_are_skipped_and_store_no_text(spark):
    r = _rows(spark, [triage("e", SINCE - timedelta(hours=1), NOTE)])["FAC001-e"]
    assert r.ai_status == "skipped" and r.ai_enriched is False
    assert r.note_masked is None and r.chief_complaint_masked is None and r.complaint_category is None


def test_output_matches_triage_notes_table(spark):
    df = spark.createDataFrame([triage("f", NOW, NOTE), triage("g", SINCE - timedelta(days=1), NOTE)], BRONZE_DDL)
    assert notes_rows(df, FakeAi(), SINCE).columns == table_def("silver", "triage_notes").column_names


def test_parse_vitals_units(spark):
    v = spark.createDataFrame(
        [("101.3F", "120 / 80", "9/10", "93%", "HR 102", "18", "2 days")],
        "temperature STRING, blood_pressure STRING, pain_score STRING, spo2 STRING, heart_rate STRING, respiratory_rate STRING, symptom_onset STRING",
    ).select(F.struct("*").alias("v"))
    out = v.select(*[c.alias(k) for k, c in parse_vitals(F.col("v")).items()]).collect()[0]
    assert (out.temperature_c, out.bp_systolic, out.bp_diastolic, out.pain_score, out.spo2, out.heart_rate) == (38.5, 120, 80, 9, 93, 102)
    assert out.respiratory_rate == 18 and out.symptom_onset == "2 days"


@pytest.mark.parametrize(
    "reply,esi",
    [
        ('{"suggested_esi": 2, "rationale": "x"}', 2),
        ('Here you go:\n```json\n{\n  "suggested_esi": 3,\n  "rationale": "y"\n}\n```', 3),
        ("no json here", None),
    ],
)
def test_parse_json_reply_tolerates_prose_and_fences(spark, reply, esi):
    r = spark.createDataFrame([(reply,)], "r STRING").select(parse_json_reply(F.col("r"), "suggested_esi INT, rationale STRING").alias("p")).collect()[0]
    assert (r.p.suggested_esi if r.p else None) == esi


def test_regex_redact_keeps_vitals(spark):
    text = "BP 120/80, pain 9/10, DOB 03/14/1958, ph 312.555.0199, MRN-7000123, a.b@x.org"
    out = spark.createDataFrame([(text,)], "t STRING").select(regex_redact(F.col("t")).alias("o")).collect()[0].o
    assert "120/80" in out and "9/10" in out
    assert "03/14/1958" not in out and "312.555.0199" not in out and "MRN-7000123" not in out and "a.b@x.org" not in out


def test_triage_events_keeps_valid_deduplicated_triage_only(spark):
    rows = [
        triage("t1", NOW, NOTE, visit_id="FAC001-v1"),
        triage("t1", NOW, NOTE, visit_id="FAC001-v1"),  # duplicate event
        triage("t2", NOW, NOTE, esi=9, visit_id="FAC001-v2"),  # invalid ESI
        triage("t3", NOW, NOTE, facility_id="FAC999", visit_id="FAC999-v3"),  # unknown facility
        ev("a1", "ARRIVAL", NOW, visit_id="FAC001-v1", pl=payload(arrival_mode="walk_in")),
    ]
    df = triage_events(spark.createDataFrame(rows, BRONZE_DDL), spark.createDataFrame([("FAC001",)], "facility_id STRING"))
    assert [r.visit_id for r in df.collect()] == ["FAC001-v1"]
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_notes_ai.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.pipeline.silver_notes_ai'`.

- [ ] **Step 4: Implement the transforms**

`src/ed_ops/pipeline/silver_notes_ai.py`:
```python
"""bronze TRIAGE events -> silver.triage_notes (spec 9.1).

ai_mask runs first, followed by a deterministic regex post-check; classification, vitals extraction and the
suggested ESI only ever see masked text. Raw note text never reaches silver. Notes older than
ai_backfill_days are stored without text (ai_status = 'skipped'): regex alone cannot remove names.
"""

from __future__ import annotations

from datetime import datetime
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ed_ops.ai import AiFunctions
from ed_ops.delta import conform, table_def
from ed_ops.pipeline.silver_visits import validate_events

PHI_PATTERNS = (
    r"\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b",  # phone
    r"MRN-?\d{5,}",  # medical record number
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",  # full date (DOB); vitals like 120/80 and 9/10 have one slash
    r"[\w.+-]+@[\w-]+\.[A-Za-z]{2,}",  # email
)
REDACTED = "[REDACTED]"
ESI_SCHEMA = "suggested_esi INT, rationale STRING"
ESI_PROMPT = (
    "You are an emergency department triage assistant. Assign an Emergency Severity Index (ESI) level from "
    "1 (most acute) to 5 (least acute) for this triage note. Return ONLY a JSON object, no prose, no code fences, "
    'matching {"suggested_esi": <integer 1-5>, "rationale": <string, max 30 words>}. Triage note: '
)
MAX_AI_ATTEMPTS = 3
NOTE_KEYS = ["facility_id", "visit_id"]
VITAL_COLUMNS = ("heart_rate", "bp_systolic", "bp_diastolic", "respiratory_rate", "spo2", "temperature_c",
                 "pain_score", "symptom_onset")


def phi_hit(text: Column) -> Column:
    return F.coalesce(reduce(lambda a, b: a | b, [text.rlike(p) for p in PHI_PATTERNS]), F.lit(False))


def regex_redact(text: Column) -> Column:
    return reduce(lambda c, p: F.regexp_replace(c, p, REDACTED), PHI_PATTERNS, text)


def parse_json_reply(result: Column, schema: str) -> Column:
    return F.from_json(F.regexp_extract(result, r"(?s)(\{.*\})", 1), schema)


def _match(text: Column, pattern: str) -> Column:
    return F.nullif(F.regexp_extract(text, pattern, 1), F.lit(""))


def parse_vitals(v: Column) -> dict[str, Column]:
    number = r"(\d+(?:\.\d+)?)"
    temp = _match(v["temperature"], number).try_cast("double")
    return {
        "heart_rate": _match(v["heart_rate"], number).try_cast("int"),
        "bp_systolic": _match(v["blood_pressure"], r"(\d{2,3})\s*/\s*\d{2,3}").try_cast("int"),
        "bp_diastolic": _match(v["blood_pressure"], r"\d{2,3}\s*/\s*(\d{2,3})").try_cast("int"),
        "respiratory_rate": _match(v["respiratory_rate"], number).try_cast("int"),
        "spo2": _match(v["spo2"], number).try_cast("int"),
        "temperature_c": F.when(temp > 50, F.round((temp - 32) * 5 / 9, 1)).otherwise(temp),
        "pain_score": F.coalesce(_match(v["pain_score"], r"(\d+)\s*/\s*10"), _match(v["pain_score"], number)).try_cast("int"),
        "symptom_onset": v["symptom_onset"],
    }


def _typed(values: dict[str, Column]) -> list[Column]:
    """Every triage_notes column except ai_attempts/processed_at, typed; unspecified columns are typed nulls."""
    cols = [c for c in table_def("silver", "triage_notes").columns if c.name not in ("ai_attempts", "processed_at")]
    return [values.get(c.name, F.lit(None)).cast(c.type).alias(c.name) for c in cols]


def triage_events(batch_df: DataFrame, facility_ids: DataFrame) -> DataFrame:
    valid, _ = validate_events(batch_df.dropDuplicates(["event_id"]), facility_ids)
    return valid.filter(F.col("event_type") == "TRIAGE").dropDuplicates(NOTE_KEYS)


def mask_stage(events: DataFrame, ai: AiFunctions) -> DataFrame:
    df = events.select(
        *NOTE_KEYS,
        F.col("event_ts").alias("triage_ts"),
        F.col("payload.esi_level").alias("esi_level"),
        F.col("payload.triage_note").alias("_note"),
        F.col("payload.chief_complaint_text").alias("_complaint"),
    )
    df = df.withColumn("_m_note", ai.mask("_note")).withColumn("_m_complaint", ai.mask("_complaint"))
    return df.select(
        *NOTE_KEYS, "triage_ts", "esi_level",
        regex_redact(F.col("_m_note")).alias("note_masked"),
        regex_redact(F.col("_m_complaint")).alias("chief_complaint_masked"),
        (phi_hit(F.col("_m_note")) | phi_hit(F.col("_m_complaint"))).alias("residual_phi_flag"),
    )


def enrich_stage(masked: DataFrame, ai: AiFunctions) -> DataFrame:
    df = (
        masked.withColumn("complaint_category", ai.classify("note_masked"))
        .withColumn("_vitals", ai.extract("note_masked"))
        .withColumn("_prompt", F.concat(F.lit(ESI_PROMPT), F.col("note_masked")))
        .withColumn("_reply", ai.query("_prompt"))
    )
    df = df.withColumn("_esi", parse_json_reply(F.col("_reply.result"), ESI_SCHEMA))
    status = (
        F.when(F.col("_reply.errorMessage").isNotNull(), F.lit("error"))
        .when(F.col("_esi.suggested_esi").between(1, 5), F.lit("ok"))
        .otherwise(F.lit("parse_error"))
    )
    df = df.withColumn("ai_status", status)
    ok = F.col("ai_status") == "ok"
    vitals = parse_vitals(F.col("_vitals"))
    return df.select(
        *_typed(
            {
                "facility_id": F.col("facility_id"),
                "visit_id": F.col("visit_id"),
                "triage_ts": F.col("triage_ts"),
                "esi_level": F.col("esi_level"),
                "note_masked": F.col("note_masked"),
                "chief_complaint_masked": F.col("chief_complaint_masked"),
                "residual_phi_flag": F.col("residual_phi_flag"),
                "complaint_category": F.col("complaint_category"),
                **vitals,
                "suggested_esi": F.when(ok, F.col("_esi.suggested_esi")),
                "esi_rationale": F.when(ok, F.col("_esi.rationale")),
                "undertriage_flag": F.when(ok, (F.col("esi_level") - F.col("_esi.suggested_esi")) >= 2),
                "ai_enriched": F.lit(True),
                "ai_status": F.col("ai_status"),
                "ai_error": F.when(F.col("ai_status") == "error", F.col("_reply.errorMessage")).when(
                    F.col("ai_status") == "parse_error", F.substring(F.coalesce(F.col("_reply.result"), F.lit("")), 1, 500)
                ),
            }
        )
    )


def skipped_rows(events: DataFrame) -> DataFrame:
    return events.select(
        *_typed(
            {
                "facility_id": F.col("facility_id"),
                "visit_id": F.col("visit_id"),
                "triage_ts": F.col("event_ts"),
                "esi_level": F.col("payload.esi_level"),
                "residual_phi_flag": F.lit(False),
                "ai_enriched": F.lit(False),
                "ai_status": F.lit("skipped"),
            }
        )
    )


def notes_rows(events: DataFrame, ai: AiFunctions, enrich_since: datetime) -> DataFrame:
    since = F.lit(enrich_since).cast("timestamp")
    recent = enrich_stage(mask_stage(events.filter(F.col("event_ts") >= since), ai), ai)
    old = skipped_rows(events.filter(F.col("event_ts") < since))
    rows = recent.unionByName(old).withColumn("ai_attempts", F.lit(1)).withColumn("processed_at", F.current_timestamp())
    return conform(rows, table_def("silver", "triage_notes"))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_notes_ai.py -v`
Expected: 11 passed.

- [ ] **Step 6: Commit**

```bash
git add src/ed_ops/pipeline/silver_notes_ai.py tests/bronze_factories.py tests/unit/test_notes_ai.py
git commit -m "feat: triage-note masking (ai_mask + regex post-check) and AI enrichment transforms" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: Notes stream, idempotent notes MERGE and the retry sweep

**Files:**
- Modify: `src/ed_ops/pipeline/silver_notes_ai.py`
- Test: `tests/unit/test_notes_ai.py`

**Interfaces:**
- Consumes: Task 4 transforms; `DatabricksAi` (Task 3); `utcnow` (Plan 1).
- Produces:
  - `notes_merge_sql(target, source_view) -> str`
  - `merge_notes(rows, cfg) -> None`
  - `process_notes_batch(batch_df, cfg, ai) -> None`
  - `start_silver_notes_ai(spark, cfg, available_now=False) -> StreamingQuery` (checkpoint `silver_notes_ai`)
  - `retry_candidates(notes) -> DataFrame`, `retry_rows(candidates, ai) -> DataFrame`
  - `ai_retry_tick(spark, cfg, ai) -> None`
  - `start_ai_retry(spark, cfg) -> StreamingQuery` (checkpoint `ai_retry_sweep`)

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_notes_ai.py`:
```python


def test_notes_merge_never_overwrites_ok_and_counts_attempts():
    from ed_ops.pipeline.silver_notes_ai import notes_merge_sql

    sql = notes_merge_sql("c.ed_silver.triage_notes", "src")
    assert "WHEN MATCHED AND (t.ai_status IS NULL OR t.ai_status <> 'ok') THEN UPDATE SET" in sql
    assert "t.ai_attempts = coalesce(t.ai_attempts, 0) + 1" in sql
    assert "t.ai_attempts = s.ai_attempts" not in sql
    insert_cols = sql.split("INSERT (")[1].split(")")[0].split(", ")
    assert insert_cols == table_def("silver", "triage_notes").column_names


def test_retry_candidates_and_successful_retry(spark):
    from ed_ops.pipeline.silver_notes_ai import retry_candidates, retry_rows

    df = spark.createDataFrame([triage("ok", NOW, NOTE), triage("err", NOW, NOTE + " FAILME"),
                                triage("bad", NOW, NOTE + " GARBAGE"), triage("old", SINCE - timedelta(days=1), NOTE)], BRONZE_DDL)
    notes = notes_rows(df, FakeAi(), SINCE)
    exhausted = notes.withColumn("ai_attempts", F.when(F.col("visit_id") == "FAC001-bad", F.lit(3)).otherwise(F.col("ai_attempts")))
    cands = retry_candidates(exhausted)
    assert [r.visit_id for r in cands.collect()] == ["FAC001-err"]  # ok, skipped and exhausted rows are not retried
    # The endpoint recovers: retrying the masked text now succeeds. (The FAILME marker is in the masked note too.)
    retried = {r.visit_id: r for r in retry_rows(cands, FakeAi(fail_marker=None)).collect()}
    assert retried["FAC001-err"].ai_status == "ok" and retried["FAC001-err"].suggested_esi == 1
    assert retry_rows(cands, FakeAi()).columns == table_def("silver", "triage_notes").column_names
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_notes_ai.py -v`
Expected: the two new tests FAIL with `ImportError: cannot import name 'notes_merge_sql'` and `ImportError: cannot import name 'retry_candidates'`.

- [ ] **Step 3: Implement the stream, the MERGE and the sweep**

Add to the imports of `src/ed_ops/pipeline/silver_notes_ai.py`:
```python
import uuid
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from ed_ops.ai import DatabricksAi
from ed_ops.config import Config
from ed_ops.timeutil import utcnow
```
Append to `src/ed_ops/pipeline/silver_notes_ai.py`:
```python


def notes_merge_sql(target: str, source_view: str) -> str:
    """Insert new notes; update existing ones only while they are not 'ok' (a success is never overwritten)."""
    cols = table_def("silver", "triage_notes").column_names
    updates = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in NOTE_KEYS and c != "ai_attempts")
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s\n"
        "ON t.facility_id = s.facility_id AND t.visit_id = s.visit_id\n"
        "WHEN MATCHED AND (t.ai_status IS NULL OR t.ai_status <> 'ok') THEN UPDATE SET "
        f"{updates}, t.ai_attempts = coalesce(t.ai_attempts, 0) + 1\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"
    )


def merge_notes(rows: DataFrame, cfg: Config) -> None:
    spark = rows.sparkSession
    view = f"triage_notes_src_{uuid.uuid4().hex[:8]}"
    rows.createOrReplaceTempView(view)
    try:
        spark.sql(notes_merge_sql(cfg.table("silver", "triage_notes"), view))
    finally:
        spark.catalog.dropTempView(view)


def process_notes_batch(batch_df: DataFrame, cfg: Config, ai: AiFunctions) -> None:
    spark = batch_df.sparkSession
    events = triage_events(batch_df, spark.table(cfg.table("ref", "facility")).select("facility_id"))
    if events.isEmpty():
        return
    merge_notes(notes_rows(events, ai, utcnow() - timedelta(days=cfg.ai_backfill_days)), cfg)


def start_silver_notes_ai(spark: SparkSession, cfg: Config, available_now: bool = False) -> StreamingQuery:
    endpoint = cfg.llm_endpoint
    writer = (
        spark.readStream.table(cfg.table("bronze", "ed_events"))
        .writeStream.foreachBatch(lambda df, _batch_id: process_notes_batch(df, cfg, DatabricksAi(endpoint)))
        .option("checkpointLocation", cfg.checkpoint("silver_notes_ai"))
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="60 seconds")
    return writer.start()


def retry_candidates(notes: DataFrame) -> DataFrame:
    return notes.filter(
        F.col("ai_status").isin("error", "parse_error")
        & (F.col("ai_attempts") < MAX_AI_ATTEMPTS)
        & F.col("note_masked").isNotNull()
    ).select(*NOTE_KEYS, "triage_ts", "esi_level", "note_masked", "chief_complaint_masked", "residual_phi_flag")


def retry_rows(candidates: DataFrame, ai: AiFunctions) -> DataFrame:
    rows = enrich_stage(candidates, ai).withColumn("ai_attempts", F.lit(1)).withColumn("processed_at", F.current_timestamp())
    return conform(rows, table_def("silver", "triage_notes"))


def ai_retry_tick(spark: SparkSession, cfg: Config, ai: AiFunctions) -> None:
    candidates = retry_candidates(spark.table(cfg.table("silver", "triage_notes")))
    if candidates.isEmpty():
        return
    merge_notes(retry_rows(candidates, ai), cfg)


def start_ai_retry(spark: SparkSession, cfg: Config) -> StreamingQuery:
    endpoint = cfg.llm_endpoint
    return (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .writeStream.foreachBatch(lambda df, _batch_id: ai_retry_tick(df.sparkSession, cfg, DatabricksAi(endpoint)))
        .trigger(processingTime=f"{cfg.ai_retry_minutes} minutes")
        .option("checkpointLocation", cfg.checkpoint("ai_retry_sweep"))
        .start()
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_notes_ai.py -v`
Expected: 13 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/pipeline/silver_notes_ai.py tests/unit/test_notes_ai.py
git commit -m "feat: silver_notes_ai stream, success-preserving notes MERGE and AI retry sweep" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: Facility and network narratives

**Files:**
- Create: `src/ed_ops/pipeline/gold_narratives.py`
- Test: `tests/unit/test_narratives.py`

**Interfaces:**
- Consumes:
  - `AiFunctions`, `DatabricksAi` (Task 3)
  - `parse_json_reply` (Task 4)
  - `merge_into` (Plan 1)
  - `compute_tick_frames` (Plan 1; used by the tests to build a `facility_current`-shaped frame)
- Produces:
  - `FACILITY_PROMPT`, `NETWORK_PROMPT`, `FACILITY_SCHEMA`, `NETWORK_SCHEMA`
  - `facility_narratives(current, ai) -> DataFrame`: `gold.facility_narrative` columns, parsed rows only
  - `network_narrative(current, ai) -> DataFrame`: `gold.network_narrative` columns; 0 rows if the reply doesn't parse
  - `narrative_tick(spark, cfg, ai) -> None`
  - `start_gold_narratives(spark, cfg) -> StreamingQuery` (checkpoint `gold_narratives`)

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_narratives.py`:
```python
import json
from datetime import datetime

import pytest

from ed_ops.delta import conform, table_def
from ed_ops.metrics.drivers import compute_baselines
from ed_ops.pipeline.gold_metrics import compute_tick_frames
from ed_ops.pipeline.gold_narratives import facility_narratives, network_narrative
from tests.factories import SCENARIO_VISITS, T, facilities_df, roster_df, visits_df
from tests.fake_ai import FakeAi

BASELINE_DDL = ("facility_id STRING, snapshot_ts TIMESTAMP, occupancy_ratio DOUBLE, boarder_share DOUBLE, "
                "patients_per_attending DOUBLE, arrivals_1h INT, high_acuity_share DOUBLE")


@pytest.fixture(scope="module")
def current(spark):
    facilities = facilities_df(spark, [("FAC001", 20, 300), ("FAC002", 10, 100)])
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)])
    baselines = compute_baselines(spark.createDataFrame([], BASELINE_DDL), "America/Chicago")
    return compute_tick_frames(visits_df(spark, SCENARIO_VISITS), roster, facilities, baselines, T, "America/Chicago").current


def test_facility_narratives_keep_only_parsed_rows(spark, current):
    out = facility_narratives(current, FakeAi(garbage_marker="FAC002"))
    rows = {r.facility_id: r for r in conform(out, table_def("gold", "facility_narrative")).collect()}
    assert set(rows) == {"FAC001"}  # FAC002's reply did not parse -> previous narrative is kept (no row to merge)
    r = rows["FAC001"]
    assert r.headline == "Fake facility status" and r.recommended_actions == ["a", "b", "c"]  # capped at 3
    assert r.model == "fake-endpoint" and r.gedwin_band == "Active" and r.snapshot_ts == T
    payload = json.loads(r.input_json)
    assert payload["facility_id"] == "FAC001" and payload["census"] == 5 and "top_drivers" in payload


def test_network_narrative_single_row(spark, current):
    rows = conform(network_narrative(current, FakeAi()), table_def("gold", "network_narrative")).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.network_id == "NETWORK" and (r.crowded_n, r.busy_n, r.active_n) == (0, 0, 2)
    assert r.crowded_facilities == ["X"] and r.doing_well_facilities == ["Y"]
    assert len(json.loads(r.input_json)["facilities"]) == 2


def test_network_narrative_unparseable_yields_no_row(spark, current):
    assert network_narrative(current, FakeAi(garbage_marker="FAC001")).count() == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_narratives.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.pipeline.gold_narratives'`.

- [ ] **Step 3: Implement the narratives**

`src/ed_ops/pipeline/gold_narratives.py`:
```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_narratives.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/pipeline/gold_narratives.py tests/unit/test_narratives.py
git commit -m "feat: grounded facility and network crowding narratives" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: Governance checks

**Files:**
- Create: `src/ed_ops/governance/checks.py`
- Test: `tests/unit/test_governance_checks.py`

**Interfaces:**
- Consumes:
  - `Config`, `LOGICAL_SCHEMAS` (Plan 1)
  - `load_catalog_def` (Plan 1)
  - `FIRST_NAMES`, `LAST_NAMES` (Plan 1 `population.py`)
  - `phi_hit` (Task 4)
- Produces:
  - `Violation(check, object, detail)` (frozen dataclass)
  - `check_comments(tables, columns) -> list[Violation]`
  - `check_table_tags(tables, table_tags, cfg) -> list[Violation]`
  - `check_phi_tags(column_tags, cfg) -> list[Violation]`
  - `phi_match_counts(df, columns) -> dict[str, int]`
  - `ai_mask_miss_rate(notes) -> float | None`
  - `run_checks(spark, cfg, sample_rows=20000) -> list[Violation]`

  The check functions take `information_schema`-shaped DataFrames:
  - `tables`: `table_schema, table_name, comment`
  - `columns`: `table_schema, table_name, column_name, comment`
  - `table_tags`: `schema_name, table_name, tag_name`
  - `column_tags`: `schema_name, table_name, column_name, tag_name`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_governance_checks.py`:
```python
from ed_ops.config import Config
from ed_ops.governance.checks import (
    ai_mask_miss_rate, check_comments, check_phi_tags, check_table_tags, phi_match_counts,
)

CFG = Config()


def test_missing_table_and_column_comments(spark):
    tables = spark.createDataFrame([("ed_gold", "a", "ok"), ("ed_gold", "b", " ")], "table_schema STRING, table_name STRING, comment STRING")
    columns = spark.createDataFrame([("ed_gold", "a", "x", "fine"), ("ed_gold", "a", "y", None)],
                                    "table_schema STRING, table_name STRING, column_name STRING, comment STRING")
    v = check_comments(tables, columns)
    assert {(x.check, x.object) for x in v} == {("comment", "ed_gold.b"), ("comment", "ed_gold.a.y")}


def test_required_table_tags_use_prefix(spark):
    tables = spark.createDataFrame([("ed_gold", "a", "c"), ("ed_gold", "b", "c")], "table_schema STRING, table_name STRING, comment STRING")
    full = [("ed_gold", "a", f"ed_{k}") for k in ("layer", "domain", "data_classification", "contains_phi", "data_origin")]
    tags = spark.createDataFrame(full + [("ed_gold", "b", "layer")], "schema_name STRING, table_name STRING, tag_name STRING")
    v = check_table_tags(tables, tags, CFG)
    assert {x.object for x in v} == {"ed_gold.b"}  # an unprefixed 'layer' tag does not count
    assert "ed_layer" in v[0].detail


def test_phi_tags_raw_phi_only_in_bronze_and_sim(spark):
    rows = [
        ("ed_bronze", "ed_events", "payload", "ed_phi_type"),  # raw PHI in bronze: allowed
        ("ed_silver", "triage_notes", "note_masked", "ed_phi_type"),
        ("ed_silver", "triage_notes", "note_masked", "ed_deid_method"),  # de-identified: allowed
        ("ed_silver", "ed_visits", "city", "ed_phi_type"),  # no deid_method: violation
        ("ed_gold", "facility_current", "x", "ed_phi_type"),  # any phi tag in gold: violation
        ("ed_gold", "facility_current", "x", "ed_deid_method"),
    ]
    v = check_phi_tags(spark.createDataFrame(rows, "schema_name STRING, table_name STRING, column_name STRING, tag_name STRING"), CFG)
    assert {(x.check, x.object) for x in v} == {
        ("raw_phi_outside_bronze_sim", "ed_silver.ed_visits.city"),
        ("phi_tag_in_serving_layer", "ed_gold.facility_current.x"),
    }


def test_phi_content_scan_counts_matches(spark):
    df = spark.createDataFrame(
        [("call 312-555-0101", "ok", "BP 120/80"), ("MRN-7000001", "fine", "DOB 01/02/1950"), (None, "x", "9/10")],
        "a STRING, b STRING, c STRING",
    )
    assert phi_match_counts(df, ["a", "b", "c"]) == {"a": 2, "b": 0, "c": 1}


def test_ai_mask_miss_rate(spark):
    notes = spark.createDataFrame([("pt Ava Smith arrived",), ("pt [MASKED] arrived",), (None,), ("[MASKED] ok",)], "note_masked STRING")
    assert ai_mask_miss_rate(notes) == 1 / 3
    assert ai_mask_miss_rate(spark.createDataFrame([(None,)], "note_masked STRING")) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_governance_checks.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.governance.checks'`.

- [ ] **Step 3: Implement the checks**

`src/ed_ops/governance/checks.py`:
```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh tests/unit/test_governance_checks.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/governance/checks.py tests/unit/test_governance_checks.py
git commit -m "feat: governance checks - comments, prefixed tags, raw-PHI tags, PHI content scan, ai_mask miss rate" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: Golden scenario and pipeline assertions

**Files:**
- Create: `src/ed_ops/validation/__init__.py`, `src/ed_ops/validation/golden.py`, `src/ed_ops/validation/pipeline.py`
- Modify: `tests/factories.py`, which re-exports the scenario from `golden.py` so unit tests and `ed_validate` share one dataset
- Test: `tests/unit/test_validation.py`

**Interfaces:**
- Consumes: `compute_snapshots` (Plan 1).
- Produces:
  - `golden`:
    - `T`, `m(minutes)`, `VISIT_DDL`, `visit(...)`, `SCENARIO_VISITS`
    - `visits_frame(spark, rows)`, `facilities_frame(spark, rows)`, `roster_frame(spark, rows)`, `as_of_frame(spark, *times)`
    - `GOLDEN_FACILITIES`, `GOLDEN_ROSTER`, `EXPECTED: dict[tuple[str, str], object]`
    - `check_golden(spark, expected=EXPECTED) -> list[str]`
  - `pipeline`:
    - `MIN_NOTES_OK_SHARE = 0.9`
    - `pipeline_metrics(spark, cfg) -> dict`
    - `evaluate_pipeline(metrics, cfg) -> list[str]`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_validation.py`:
```python
from ed_ops.config import Config
from ed_ops.validation.golden import EXPECTED, check_golden
from ed_ops.validation.pipeline import evaluate_pipeline


def test_golden_scenario_matches_hand_calculation(spark):
    assert check_golden(spark) == []


def test_golden_reports_mismatches(spark):
    wrong = dict(EXPECTED)
    wrong[("FAC001", "census")] = 99
    assert check_golden(spark, wrong) == ["FAC001.census: expected 99, got 5"]


GOOD = {
    "bronze_events": 2600, "silver_visits": 480, "quarantine": 10, "facility_current": 5, "snapshot_facilities": 5,
    "snapshot_rows": 1465, "board_census_mismatches": 0, "notes_enriched": 120, "notes_ok_share": 0.97,
    "facility_narratives": 5, "network_narratives": 1, "legacy_driver_entries": 0,
}
SMALL = Config(facility_count=5, backfill_days=1)


def test_evaluate_pipeline_passes_on_healthy_metrics():
    assert evaluate_pipeline(GOOD, SMALL) == []


def test_evaluate_pipeline_names_each_failure():
    bad = dict(GOOD, quarantine=0, snapshot_rows=100, board_census_mismatches=2, notes_ok_share=0.5,
               network_narratives=0, legacy_driver_entries=3)
    failures = evaluate_pipeline(bad, SMALL)
    assert len(failures) == 6
    assert any("quarantine" in f for f in failures) and any("arrivals_1h" in f for f in failures)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_validation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ed_ops.validation'`.

- [ ] **Step 3: Implement the golden scenario and pipeline assertions**

`src/ed_ops/validation/__init__.py`: empty file.

`src/ed_ops/validation/golden.py`:
```python
"""Hand-calculated golden scenario (FAC001 busy, FAC002 empty, at T). Unit tests use it, and ed_validate re-runs
it on the workspace runtime (serverless Photon) to prove the metric formulas give identical results there."""

from __future__ import annotations

import math
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

from ed_ops.metrics.snapshots import compute_snapshots

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
    # v9: arrived T-3, triaged only at T+2 (unknown ESI at T); its flag must not count yet.
    visit("v9", m(-3), esi=2, triage=m(2), undertriage=True),
]
GOLDEN_FACILITIES = [("FAC001", 20, 300), ("FAC002", 10, 100)]
GOLDEN_ROSTER = [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)]

_GEDWIN = math.log(0.25 / 0.75) + math.log(3.2)
_FAC001 = {
    "census": 5, "n_esi1": 1, "n_esi3": 1, "n_esi4": 1, "n_untriaged": 2, "waiting_now_n": 3, "in_bed_n": 2,
    "longest_current_wait_min": 40.0, "boarders_n": 1, "longest_boarder_h": 1.0, "vents_n": 1,
    "undertriage_flags_n": 1, "d2p_sample_n": 2, "d2p_mean_min": 45.0, "d2p_median_min": 45.0, "d2p_p90_min": 57.0,
    "arrivals_1h": 4, "ambulance_arrivals_1h": 1, "last_bedded_wait_h": 20 / 60, "lwbs_rate_24h": 1 / 3,
    "los_median_h_24h": 85 / 60, "boarding_median_h_24h": 2.0, "admit_rate_24h": 2 / 3, "attendings_on_duty": 2,
    "occupancy_ratio": 0.25, "acuity_ratio": 3.2, "edwin": 0.5 * (0.25 / 0.75) * 3.2, "edwin_band": "Active",
    "gedwin": _GEDWIN, "gedwin_band": "Active",
    "gedwin_delta_1h": _GEDWIN - (math.log(0.15 / 0.85) + math.log(10 / 3)),
    "nedocs": -20 + 85.8 * 0.25 + 600 / 300 + 13.4 + 0.93 * 1.0 + 5.64 * (20 / 60), "nedocs_level": 1,
}
_FAC002 = {
    "census": 0, "edwin": None, "gedwin": None, "gedwin_band": "Active", "nedocs": 0.0, "nedocs_level": 1,
    "dq_flags": ["no_attending_on_duty"], "longest_boarder_h": 0.0, "last_bedded_wait_h": 0.0,
}
EXPECTED: dict[tuple[str, str], object] = {
    **{("FAC001", k): v for k, v in _FAC001.items()},
    **{("FAC002", k): v for k, v in _FAC002.items()},
}


def visits_frame(spark: SparkSession, rows) -> DataFrame:
    return spark.createDataFrame(rows, VISIT_DDL)


def facilities_frame(spark: SparkSession, rows) -> DataFrame:
    """rows: (facility_id, ed_beds, inpatient_beds)."""
    full = [(fid, f"{fid} Hospital", "community", "Central", beds, inpatient) for fid, beds, inpatient in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, facility_name STRING, facility_type STRING, region STRING, ed_beds INT, inpatient_beds INT"
    )


def roster_frame(spark: SparkSession, rows) -> DataFrame:
    """rows: (facility_id, shift_start, shift_end, attendings_on_duty)."""
    full = [(fid, s, e, "day", n) for fid, s, e, n in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, shift_start TIMESTAMP, shift_end TIMESTAMP, shift_name STRING, attendings_on_duty INT"
    )


def as_of_frame(spark: SparkSession, *times) -> DataFrame:
    return spark.createDataFrame([(t,) for t in times], "snapshot_ts TIMESTAMP")


def _close(actual, expected) -> bool:
    if isinstance(expected, float) and actual is not None:
        return math.isclose(float(actual), expected, rel_tol=1e-9, abs_tol=1e-9)
    return actual == expected


def check_golden(spark: SparkSession, expected: dict[tuple[str, str], object] = EXPECTED) -> list[str]:
    snaps = compute_snapshots(
        visits_frame(spark, SCENARIO_VISITS), roster_frame(spark, GOLDEN_ROSTER),
        facilities_frame(spark, GOLDEN_FACILITIES), as_of_frame(spark, T),
    )
    rows = {r["facility_id"]: r.asDict() for r in snaps.collect()}
    return [f"{fid}.{col}: expected {want}, got {rows[fid][col]}"
            for (fid, col), want in expected.items() if not _close(rows[fid][col], want)]
```

`src/ed_ops/validation/pipeline.py`:
```python
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
```

Replace the whole content of `tests/factories.py` with:
```python
"""Test-data builders. All datetimes are naive UTC. The scenario lives in ed_ops.validation.golden, so unit
tests and the ed_validate job use exactly the same hand-calculated dataset."""

from ed_ops.validation.golden import (  # noqa: F401 (re-exported for tests)
    SCENARIO_VISITS, T, VISIT_DDL, as_of_frame, facilities_frame, m, roster_frame, visit, visits_frame,
)

visits_df = visits_frame
facilities_df = facilities_frame
roster_df = roster_frame
as_of_df = as_of_frame
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `scripts/pytest.sh -v`
Expected: the whole suite passes. That includes `test_validation.py` (4 tests) and every Plan 1 test that imports `tests.factories`.

- [ ] **Step 5: Commit**

```bash
git add src/ed_ops/validation tests/factories.py tests/unit/test_validation.py
git commit -m "feat: golden scenario shared by tests and ed_validate, plus pipeline assertions" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Wiring: setup guards, idempotent quarantine, entry points and jobs

**Files:**
- Modify: `src/ed_ops/setup.py`, `src/ed_ops/streaming.py`, `src/ed_ops/cli.py`, `src/ed_ops/delta.py`, `src/ed_ops/pipeline/silver_visits.py`, `pyproject.toml`, `databricks.yml`, `resources/jobs.yml`
- Create: `src/ed_ops/validation/tasks.py`, `resources/validate.yml`
- Test: `tests/unit/test_setup_guards.py`, `tests/unit/test_cli.py`, `tests/unit/test_delta.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - Guards: `setup.generator_live(live_started_ts) -> bool` and `setup.streaming_active(live_started_ts, updated_at, now) -> bool`, the latter true when live and the state was updated within 5 minutes.
  - New setup tasks: `backfill_notes_ai`, `refresh_drivers`, `backfill_narratives`, `governance_checks`.
  - `delta.txn_options(cfg, app, version) -> dict[str, str]` and `delta.append_into(df, cfg, logical, name, txn=None)`.
  - `silver_visits.process_batch(batch_df, batch_id, cfg)`.
  - Streaming tasks: `streaming.run_notes_ai`, `run_ai_retry`, `run_narratives`.
  - Validation tasks: `validation.tasks.validate_golden`, `validate_pipeline`, `teardown_schemas`.
  - 10 new console scripts (19 in total).
  - Jobs: `ed_setup` (9 tasks), `ed_streaming` (7 tasks), `ed_validate` (11 tasks).

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_setup_guards.py`:
```python
from datetime import datetime, timedelta

from ed_ops.setup import generator_live, streaming_active

NOW = datetime(2026, 11, 10, 12)


def test_generator_live_once_started():
    assert generator_live(None) is False
    assert generator_live(NOW - timedelta(days=1)) is True


def test_streaming_active_requires_recent_state_updates():
    assert streaming_active(NOW - timedelta(hours=3), NOW - timedelta(minutes=1), NOW) is True
    assert streaming_active(NOW - timedelta(hours=3), NOW - timedelta(hours=2), NOW) is False  # paused
    assert streaming_active(None, NOW, NOW) is False  # fresh setup: backfill just wrote state but never went live
```

Append to `tests/unit/test_delta.py`:
```python


def test_idempotent_append_options_include_checkpoint_version():
    from ed_ops.config import Config
    from ed_ops.delta import txn_options

    assert txn_options(Config(schema_prefix="ed_", checkpoint_version=2), "quarantine", 17) == {
        "txnAppId": "ed_quarantine_v2", "txnVersion": "17",
    }
```

In `tests/unit/test_cli.py`:
- change `assert len(scripts) == 9` to `assert len(scripts) == 19`;
- replace the whole function `test_bundle_tasks_reference_existing_entry_points` with:
```python
def _jobs():
    jobs = {}
    for name in ("jobs.yml", "validate.yml"):
        jobs.update(yaml.safe_load((ROOT / "resources" / name).read_text())["resources"]["jobs"])
    return jobs


def test_bundle_tasks_reference_existing_entry_points():
    from dataclasses import fields

    from ed_ops.config import Config

    scripts = set(tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"])
    jobs = _jobs()
    used = {t["python_wheel_task"]["entry_point"] for job in jobs.values() for t in job["tasks"]}
    assert used == scripts
    assert [t["task_key"] for t in jobs["ed_setup"]["tasks"]] == [
        "create_objects", "seed_reference", "backfill_generate", "backfill_ingest", "backfill_gold",
        "backfill_notes_ai", "refresh_drivers", "backfill_narratives", "governance_checks",
    ]
    assert {t["task_key"] for t in jobs["ed_streaming"]["tasks"]} == {
        "generator", "bronze_ingest", "silver_visits", "gold_metrics", "notes_ai", "ai_retry", "narratives",
    }
    cluster = jobs["ed_streaming"]["job_clusters"][0]["new_cluster"]
    assert cluster["spark_version"] == "18.x-scala2.13"  # Databricks picks the ARM build for Graviton nodes itself
    assert cluster["runtime_engine"] == "PHOTON"
    assert cluster["data_security_mode"] == "USER_ISOLATION"
    assert cluster["aws_attributes"]["availability"] == "ON_DEMAND"
    assert cluster["node_type_id"] == cluster["driver_node_type_id"] == "${var.streaming_node_type}"
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["variables"]["streaming_node_type"]["default"] == "r7gd.2xlarge"
    names = {f.name for f in fields(Config)}
    for job in ("ed_setup", "ed_validate"):
        assert set(jobs[job]["tasks"][0]["python_wheel_task"]["named_parameters"]) <= names
    validate = jobs["ed_validate"]
    params = validate["tasks"][0]["python_wheel_task"]["named_parameters"]
    assert params["schema_prefix"] == "ed_validate_" and params["facility_count"] == "5"
    assert next(t for t in validate["tasks"] if t["task_key"] == "teardown")["run_if"] == "ALL_DONE"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `scripts/pytest.sh tests/unit/test_setup_guards.py tests/unit/test_cli.py tests/unit/test_delta.py -v`
Expected:
- `ImportError: cannot import name 'generator_live'`;
- `test_cli` fails (9 ≠ 19 scripts; `resources/validate.yml` not found);
- `ImportError: cannot import name 'txn_options'`.

- [ ] **Step 3: Idempotent quarantine append**

In `src/ed_ops/delta.py`, replace `append_into` with:
```python
def txn_options(cfg: Config, app: str, version: int) -> dict[str, str]:
    """Delta idempotent-write options. The checkpoint version is part of the app id because a checkpoint reset
    restarts micro-batch ids at 0."""
    return {"txnAppId": f"{cfg.schema_prefix}{app}_v{cfg.checkpoint_version}", "txnVersion": str(version)}


def append_into(df: DataFrame, cfg: Config, logical: str, name: str, txn: tuple[str, int] | None = None) -> None:
    writer = conform(df, table_def(logical, name)).write.mode("append")
    if txn is not None:
        writer = writer.options(**txn_options(cfg, *txn))
    writer.saveAsTable(cfg.table(logical, name))
```
In `src/ed_ops/pipeline/silver_visits.py`:
- change `def process_batch(batch_df: DataFrame, cfg: Config) -> None:` to `def process_batch(batch_df: DataFrame, batch_id: int, cfg: Config) -> None:`;
- change `append_into(quarantine_rows(rejected), cfg, "silver", "ed_events_quarantine")` to `append_into(quarantine_rows(rejected), cfg, "silver", "ed_events_quarantine", txn=("quarantine", batch_id))`;
- change `.writeStream.foreachBatch(lambda df, _batch_id: process_batch(df, cfg))` to `.writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, cfg))`.

- [ ] **Step 4: Setup guards and new setup tasks**

In `src/ed_ops/setup.py`, replace the module docstring with:
```python
"""ed_setup job tasks (serverless). Idempotent. Once the generator has gone live, ed_streaming owns the generator,
bronze, silver and gold checkpoints, so generate/ingest/gold skip. The notes, driver and narrative tasks refuse to
run while ed_streaming is active: pause it first (docs/runbook-plan2.md)."""
```
Change `from datetime import timedelta` to `from datetime import datetime, timedelta`, then add these imports:
```python
from pyspark.sql import functions as F

from ed_ops.ai import DatabricksAi
from ed_ops.governance.checks import run_checks
from ed_ops.pipeline.gold_narratives import narrative_tick
from ed_ops.pipeline.silver_notes_ai import start_silver_notes_ai
```
Add after the imports:
```python
ACTIVE_WINDOW = timedelta(minutes=5)


def generator_live(live_started_ts: datetime | None) -> bool:
    return live_started_ts is not None


def streaming_active(live_started_ts: datetime | None, updated_at: datetime | None, now: datetime) -> bool:
    return generator_live(live_started_ts) and updated_at is not None and updated_at >= now - ACTIVE_WINDOW


def _state_row(spark: SparkSession, cfg: Config):
    rows = spark.table(cfg.table("sim", "generator_state")).filter(F.col("generator_id") == "main").collect()
    return rows[0] if rows else None


def _skip_if_live(spark: SparkSession, cfg: Config, task: str) -> bool:
    row = _state_row(spark, cfg)
    if row is not None and generator_live(row.live_started_ts):
        print(f"{task}: generator is live since {row.live_started_ts}; ed_streaming owns this step - skipping")
        return True
    return False


def _refuse_if_streaming(spark: SparkSession, cfg: Config, task: str) -> None:
    row = _state_row(spark, cfg)
    if row is not None and streaming_active(row.live_started_ts, row.updated_at, utcnow()):
        raise RuntimeError(f"{task}: ed_streaming is running (generator state updated {row.updated_at}). "
                           "Pause it and cancel its run first (docs/runbook-plan2.md).")
```
Make these two lines the first lines of `backfill_generate`, `backfill_ingest` and `backfill_gold`, with each function's own name as the string:
```python
    if _skip_if_live(spark, cfg, "backfill_generate"):
        return
```
Append the new tasks:
```python


def backfill_notes_ai(spark: SparkSession, cfg: Config) -> None:
    _refuse_if_streaming(spark, cfg, "backfill_notes_ai")
    start_silver_notes_ai(spark, cfg, available_now=True).awaitTermination()


def refresh_drivers(spark: SparkSession, cfg: Config) -> None:
    _refuse_if_streaming(spark, cfg, "refresh_drivers")
    gold_metrics.refresh_drivers(spark, cfg)


def backfill_narratives(spark: SparkSession, cfg: Config) -> None:
    _refuse_if_streaming(spark, cfg, "backfill_narratives")
    narrative_tick(spark, cfg, DatabricksAi(cfg.llm_endpoint))


def governance_checks(spark: SparkSession, cfg: Config) -> None:
    violations = run_checks(spark, cfg)
    for v in violations:
        print(f"VIOLATION {v.check}: {v.object} - {v.detail}")
    if violations:
        raise RuntimeError(f"{len(violations)} governance violation(s); see output above")
```

- [ ] **Step 5: Streaming tasks, validation tasks and CLI**

In `src/ed_ops/streaming.py`, add the imports `from ed_ops.pipeline.gold_narratives import start_gold_narratives` and `from ed_ops.pipeline.silver_notes_ai import start_ai_retry, start_silver_notes_ai`, then append:
```python


def run_notes_ai(spark: SparkSession, cfg: Config) -> None:
    start_silver_notes_ai(spark, cfg).awaitTermination()


def run_ai_retry(spark: SparkSession, cfg: Config) -> None:
    start_ai_retry(spark, cfg).awaitTermination()


def run_narratives(spark: SparkSession, cfg: Config) -> None:
    start_gold_narratives(spark, cfg).awaitTermination()
```

`src/ed_ops/validation/tasks.py`:
```python
"""ed_validate job tasks."""

from __future__ import annotations

from pyspark.sql import SparkSession

from ed_ops.config import LOGICAL_SCHEMAS, Config
from ed_ops.governance.checks import run_checks
from ed_ops.validation.golden import check_golden
from ed_ops.validation.pipeline import evaluate_pipeline, pipeline_metrics


def validate_golden(spark: SparkSession, cfg: Config) -> None:
    mismatches = check_golden(spark)
    for line in mismatches:
        print(f"GOLDEN MISMATCH {line}")
    if mismatches:
        raise RuntimeError(f"golden scenario: {len(mismatches)} mismatch(es)")
    print("golden scenario matches hand-calculated EDWIN/gEDWIN/NEDOCS on this runtime")


def validate_pipeline(spark: SparkSession, cfg: Config) -> None:
    metrics = pipeline_metrics(spark, cfg)
    print(f"pipeline metrics: {metrics}")
    failures = evaluate_pipeline(metrics, cfg) + [f"{v.check}: {v.object} - {v.detail}" for v in run_checks(spark, cfg)]
    for f in failures:
        print(f"FAIL {f}")
    if failures:
        raise RuntimeError(f"ed_validate: {len(failures)} failure(s)")
    print("ed_validate: all pipeline and governance assertions passed")


def teardown_schemas(spark: SparkSession, cfg: Config) -> None:
    if "validate" not in cfg.schema_prefix:
        raise RuntimeError(f"refusing to drop schemas for prefix {cfg.schema_prefix!r}: only validate prefixes")
    for logical in LOGICAL_SCHEMAS:
        spark.sql(f"DROP SCHEMA IF EXISTS {cfg.schema(logical)} CASCADE")
    print(f"dropped {cfg.schema_prefix}* schemas")
```

In `src/ed_ops/cli.py`, add the import `from ed_ops.validation import tasks as validation` below `from ed_ops import setup, streaming`, then append:
```python


def backfill_notes_ai() -> None:
    _run(setup.backfill_notes_ai)


def refresh_drivers() -> None:
    _run(setup.refresh_drivers)


def backfill_narratives() -> None:
    _run(setup.backfill_narratives)


def governance_checks() -> None:
    _run(setup.governance_checks)


def stream_notes_ai() -> None:
    _run(streaming.run_notes_ai)


def stream_ai_retry() -> None:
    _run(streaming.run_ai_retry)


def stream_narratives() -> None:
    _run(streaming.run_narratives)


def validate_golden() -> None:
    _run(validation.validate_golden)


def validate_pipeline() -> None:
    _run(validation.validate_pipeline)


def teardown_schemas() -> None:
    _run(validation.teardown_schemas)
```

In `pyproject.toml`, add to `[project.scripts]`:
```toml
ed-ops-backfill-notes-ai = "ed_ops.cli:backfill_notes_ai"
ed-ops-refresh-drivers = "ed_ops.cli:refresh_drivers"
ed-ops-backfill-narratives = "ed_ops.cli:backfill_narratives"
ed-ops-governance-checks = "ed_ops.cli:governance_checks"
ed-ops-stream-notes-ai = "ed_ops.cli:stream_notes_ai"
ed-ops-stream-ai-retry = "ed_ops.cli:stream_ai_retry"
ed-ops-stream-narratives = "ed_ops.cli:stream_narratives"
ed-ops-validate-golden = "ed_ops.cli:validate_golden"
ed-ops-validate-pipeline = "ed_ops.cli:validate_pipeline"
ed-ops-teardown-schemas = "ed_ops.cli:teardown_schemas"
```

- [ ] **Step 6: Bundle variables and jobs**

In `databricks.yml`, add under `variables:`, before `streaming_node_type:`:
```yaml
  ai_backfill_days:
    description: Triage notes newer than this many days get the full AI chain; older backfill notes are stored without text.
    default: "2"
  llm_endpoint:
    description: Model serving endpoint for ai_query (ESI suggestions and narratives).
    default: databricks-claude-sonnet-5-5
  narrative_minutes:
    description: Minutes between narrative refreshes.
    default: "15"
  ai_retry_minutes:
    description: Minutes between AI retry sweeps.
    default: "15"
```

In `resources/jobs.yml`, add these lines to the `&common` named parameters, after `tag_prefix: ${var.tag_prefix}`:
```yaml
              ai_backfill_days: ${var.ai_backfill_days}
              llm_endpoint: ${var.llm_endpoint}
              narrative_minutes: ${var.narrative_minutes}
              ai_retry_minutes: ${var.ai_retry_minutes}
```
Append these tasks to `ed_setup`, after `backfill_gold`:
```yaml
        - task_key: backfill_notes_ai
          depends_on: [{task_key: backfill_ingest}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-notes-ai, named_parameters: *common}
        - task_key: refresh_drivers
          depends_on: [{task_key: backfill_gold}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-refresh-drivers, named_parameters: *common}
        - task_key: backfill_narratives
          depends_on: [{task_key: refresh_drivers}, {task_key: backfill_notes_ai}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-narratives, named_parameters: *common}
        - task_key: governance_checks
          depends_on: [{task_key: backfill_narratives}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-governance-checks, named_parameters: *common}
```
Append these tasks to `ed_streaming`:
```yaml
        - task_key: notes_ai
          job_cluster_key: streaming
          libraries: [{whl: ../dist/*.whl}]
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-stream-notes-ai, named_parameters: *common}
        - task_key: ai_retry
          job_cluster_key: streaming
          libraries: [{whl: ../dist/*.whl}]
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-stream-ai-retry, named_parameters: *common}
        - task_key: narratives
          job_cluster_key: streaming
          libraries: [{whl: ../dist/*.whl}]
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-stream-narratives, named_parameters: *common}
```

`resources/validate.yml`:
```yaml
resources:
  jobs:
    ed_validate:
      name: ed-ops validate
      description: >-
        End-to-end validation at small scale in ed_validate_* schemas: golden scenario on this runtime, the full batch
        pipeline (generate, ingest, AI notes, gold, drivers, narratives), pipeline and governance assertions, teardown.
      max_concurrent_runs: 1
      environments:
        - environment_key: default
          spec:
            environment_version: "4"
            dependencies:
              - ../dist/*.whl
      tasks:
        - task_key: validate_golden
          environment_key: default
          python_wheel_task:
            package_name: ed_ops
            entry_point: ed-ops-validate-golden
            named_parameters: &validate
              catalog: ${var.catalog}
              schema_prefix: ed_validate_
              facility_count: "5"
              backfill_days: "1"
              ai_backfill_days: "1"
              tick_seconds: ${var.tick_seconds}
              timezone: ${var.timezone}
              seed: ${var.seed}
              checkpoint_version: "1"
              secret_scope: ${var.secret_scope}
              tag_prefix: ${var.tag_prefix}
              llm_endpoint: ${var.llm_endpoint}
              narrative_minutes: ${var.narrative_minutes}
              ai_retry_minutes: ${var.ai_retry_minutes}
        - task_key: create_objects
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-create-objects, named_parameters: *validate}
        - task_key: seed_reference
          depends_on: [{task_key: create_objects}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-seed-reference, named_parameters: *validate}
        - task_key: backfill_generate
          depends_on: [{task_key: seed_reference}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-generate, named_parameters: *validate}
        - task_key: backfill_ingest
          depends_on: [{task_key: backfill_generate}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-ingest, named_parameters: *validate}
        - task_key: backfill_gold
          depends_on: [{task_key: backfill_ingest}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-gold, named_parameters: *validate}
        - task_key: backfill_notes_ai
          depends_on: [{task_key: backfill_ingest}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-notes-ai, named_parameters: *validate}
        - task_key: refresh_drivers
          depends_on: [{task_key: backfill_gold}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-refresh-drivers, named_parameters: *validate}
        - task_key: backfill_narratives
          depends_on: [{task_key: refresh_drivers}, {task_key: backfill_notes_ai}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-backfill-narratives, named_parameters: *validate}
        - task_key: validate_pipeline
          depends_on: [{task_key: backfill_narratives}, {task_key: validate_golden}]
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-validate-pipeline, named_parameters: *validate}
        - task_key: teardown
          depends_on: [{task_key: validate_pipeline}]
          run_if: ALL_DONE
          environment_key: default
          python_wheel_task: {package_name: ed_ops, entry_point: ed-ops-teardown-schemas, named_parameters: *validate}
```

- [ ] **Step 7: Run the tests and validate the bundle**

Run: `scripts/pytest.sh -v`
Expected: the whole suite passes.

Run: `databricks bundle validate -t dev -p <profile>`
Expected: `Validation OK!`.

- [ ] **Step 8: Commit**

```bash
git add src/ed_ops/setup.py src/ed_ops/streaming.py src/ed_ops/cli.py src/ed_ops/delta.py src/ed_ops/pipeline/silver_visits.py src/ed_ops/validation/tasks.py pyproject.toml databricks.yml resources/jobs.yml resources/validate.yml tests/unit/test_setup_guards.py tests/unit/test_cli.py tests/unit/test_delta.py
git commit -m "feat: wire AI notes, retry, narratives, governance and ed_validate into the bundle; live-safe ed_setup" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 10: Workspace rollout, live verification and `ed_validate`

**Files:**
- Create: `docs/runbook-plan2.md`

**Interfaces:**
- Consumes: the bundle from Task 9.
- Produces:
  - `silver.triage_notes` and the narrative tables populated on the dev deployment;
  - `ed_streaming` running 7 tasks;
  - a green `ed_validate` run.

Each check gives a command and its expected result. If a check fails, reproduce the logic in a unit test first wherever that's possible, then fix and redeploy.

- [ ] **Step 1: Pause live streaming and stop its run** (the notes, driver and narrative backfills refuse to run while it's active)

```bash
databricks bundle deploy -t dev -p <profile> --var streaming_pause_status=PAUSED
JOB=$(databricks bundle summary -t dev -p <profile> -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['id'])")
databricks jobs cancel-all-runs --job-id $JOB -p <profile>
```
Expected: `databricks jobs list-runs --job-id $JOB --active-only -p <profile> -o json` shows no active runs within a minute. Then wait 5 minutes so `streaming_active` turns false.

- [ ] **Step 2: Run the upgraded setup against the existing deployment**

```bash
databricks bundle run ed_setup -t dev -p <profile>
```
Expected:
- **All 9 tasks succeed.**
- `backfill_generate`, `backfill_ingest` and `backfill_gold` print `generator is live ... skipping`.
- `backfill_notes_ai` processes every bronze TRIAGE event: about 7k notes from the last 2 days get the full AI chain, and the rest are `skipped`.
- `refresh_drivers`, `backfill_narratives` and `governance_checks` succeed; the last prints `governance: 0 violation(s)`.

- [ ] **Step 3: Check the enrichment and narratives**

```bash
C=<catalog>
scripts/run_sql.py \
  "SELECT ai_status, count(*) FROM $C.ed_silver.triage_notes GROUP BY 1 ORDER BY 1" \
  "SELECT count_if(residual_phi_flag), count_if(undertriage_flag), round(avg(CASE WHEN ai_status='ok' THEN 1 ELSE 0 END),3) FROM $C.ed_silver.triage_notes WHERE ai_enriched" \
  "SELECT complaint_category, count(*) FROM $C.ed_silver.triage_notes WHERE ai_status='ok' GROUP BY 1 ORDER BY 2 DESC" \
  "SELECT left(note_masked, 160), heart_rate, spo2, temperature_c, suggested_esi, esi_level FROM $C.ed_silver.triage_notes WHERE ai_status='ok' LIMIT 3" \
  "SELECT facility_id, gedwin_band, headline, left(explanation, 140) FROM $C.ed_gold.facility_narrative ORDER BY facility_id LIMIT 5" \
  "SELECT headline, crowded_n, crowded_facilities, doing_well_facilities FROM $C.ed_gold.network_narrative" \
  "SELECT d.driver, count(*) FROM $C.ed_gold.facility_snapshot_5min LATERAL VIEW explode(top_drivers) t AS d GROUP BY 1 ORDER BY 2 DESC"
```
Expected:
1. `ok` covers the large majority of enriched notes, with `skipped` for the older ones, and `error` plus `parse_error` together at 5% or less.
2. Residual-PHI flags are few. Under-triage flags are present: the generator under-triages about 3% of notes.
3. The categories roughly follow the archetype weights.
4. Masked notes show `[MASKED]` and no names, MRNs or phone numbers; vitals are typed.
5. 50 facility narratives cite the input numbers.
6. One network narrative lists only Crowded facilities in `crowded_facilities`.
7. Driver names include `arrival_surge_ratio`, and none are `arrivals_1h`.

- [ ] **Step 4: Resume live streaming and verify the new streams**

```bash
databricks bundle deploy -t dev -p <profile> --var streaming_pause_status=UNPAUSED
```
Wait 20 minutes, then:
```bash
scripts/run_sql.py \
  "SELECT max(processed_at), current_timestamp(), count_if(processed_at > current_timestamp() - INTERVAL 15 MINUTES) FROM $C.ed_silver.triage_notes" \
  "SELECT max(generated_at) FROM $C.ed_gold.facility_narrative" \
  "SELECT max(computed_at) FROM $C.ed_gold.facility_current" \
  "SELECT ai_status, count(*) FROM $C.ed_silver.triage_notes WHERE processed_at > current_timestamp() - INTERVAL 20 MINUTES GROUP BY 1"
```
Expected:
- New notes processed in the last 15 minutes (about 30–40 at about 2.4 a minute).
- Narratives regenerated after the restart.
- `facility_current` less than 2 minutes old.
- New notes mostly `ok`.

Confirm the 7 tasks are RUNNING with `databricks jobs get-run <active run id> -p <profile> -o json`.

- [ ] **Step 5: Run `ed_validate`**

```bash
databricks bundle run ed_validate -t dev -p <profile>
```
Expected:
- All 11 tasks succeed.
- `validate_golden` prints `golden scenario matches ... on this runtime`.
- `validate_pipeline` prints `all pipeline and governance assertions passed`.
- `teardown` drops the `ed_validate_*` schemas; `scripts/run_sql.py "SHOW SCHEMAS IN $C LIKE 'ed_validate_*'"` prints no rows.

- [ ] **Step 6: Write the runbook and commit**

`docs/runbook-plan2.md`:
```markdown
# ED Ops: AI enrichment, narratives, governance and validation runbook (Plan 2)

Builds on `docs/runbook-plan1.md`. LLM endpoint: `databricks-claude-sonnet-5-5` (bundle var `llm_endpoint`).

## What runs where
- ed_streaming (classic, on-demand Photon r7gd.2xlarge): generator, bronze_ingest, silver_visits, gold_metrics,
  notes_ai (every 60 s), ai_retry (every ai_retry_minutes), narratives (every narrative_minutes).
- ed_setup (serverless) is safe to re-run. Once the generator is live, generate/ingest/gold skip; the notes backfill,
  refresh_drivers and narratives refuse while ed_streaming is active, so pause it first:

      databricks bundle deploy -t dev -p <profile> --var streaming_pause_status=PAUSED
      JOB=$(databricks bundle summary -t dev -p <profile> -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['id'])")
      databricks jobs cancel-all-runs --job-id $JOB -p <profile>      # wait 5 min, then:
      databricks bundle run ed_setup -t dev -p <profile>
      databricks bundle deploy -t dev -p <profile> --var streaming_pause_status=UNPAUSED

- ed_validate (serverless): end-to-end proof in ed_validate_* schemas, torn down afterwards:

      databricks bundle run ed_validate -t dev -p <profile>

## AI behaviour
- ai_mask runs first, then a regex post-check (phone, MRN, full dates, email); everything else sees masked text only.
- Notes older than ai_backfill_days are stored without text (ai_status = 'skipped').
- ai_query uses failOnError => false and a JSON-only prompt (no responseFormat: Claude 5.x rejects it).
- Failed notes (error / parse_error) are retried up to 3 times; a successful row is never overwritten.
- Narratives see only computed metrics (input_json is stored); a reply that fails to parse keeps the previous one.

## Governance
- Regular UC tags (no tag policies), keys prefixed ed_ (bundle var tag_prefix).
- governance_checks fails ed_setup on a missing comment or tag, a raw PHI tag outside bronze/sim, a PHI-pattern match
  in sampled silver/gold/ref data, or an ai_mask name miss rate above 2%.

## Useful queries
    SELECT ai_status, count(*) FROM <catalog>.ed_silver.triage_notes GROUP BY 1
    SELECT * FROM <catalog>.ed_gold.network_narrative
```

```bash
git add docs/runbook-plan2.md
git commit -m "docs: Plan 2 operator runbook" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Self-Review Notes (completed by the plan author)

**Spec coverage:**

| Spec section | Where this plan covers it |
|---|---|
| §7.4 driver naming | Task 1 |
| §8 `silver_notes_ai` | Tasks 4–5 |
| §8 `ai_retry_sweep` | Task 5 |
| §8 `gold_narratives` | Task 6 |
| §9.1 chain order, regex post-check, `ai_extract` parsing, JSON-only `ai_query`, skip-old notes | Task 4 |
| §9.2 narratives with `input_json` | Task 6 |
| §10 narrative tables | Task 2 |
| §11.3 regular prefixed tags | Applied by Plan 1 DDL; verified by Task 7 |
| §11.5 checks 1–5 | Task 7 |
| §15 AI error handling | Tasks 4–6 |
| §16.3 `ed_validate` | Tasks 8–10 |
| §17 `ai_backfill_days`, `llm_endpoint` | Task 9 |

`warehouse_id` and the Lakebase variables belong to Plan 3.

**Deferred to Plan 3:** `gold.visit_facts`, Lakebase, the app, Genie and its benchmarks, the customer target and `run_as` service principal, a cloud-portable node type, and the remaining Plan 1 deferred minors that Plan 2 doesn't address (backfill warm-up day, column migration step, pediatric patient ages).
