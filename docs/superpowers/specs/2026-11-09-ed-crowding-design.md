# ED Crowding Command Center — Design Spec

- **Date:** 2026-11-09
- **Status:** Revised after workspace verification, awaiting review
- **Author:** Jagjit Natt (with Claude)
- **Audience:** Databricks FE handover to customer (healthcare provider network)

---

## 1. Purpose and success criteria

A Databricks reference solution that simulates real-time patient flow across **50 emergency departments (EDs)**. It measures crowding with peer-reviewed indices and explains which facilities are crowded and which are doing well. It covers the whole platform: data generation, streaming medallion pipeline, AI functions on clinical text, Unity Catalog governance, Lakebase serving, a Databricks App and a Genie space.

**Success criteria**

1. Within one run of `ed_setup`, the system holds 14 days of history and then streams live data, with no manual steps.
2. The app shows, per facility and network-wide, refreshed about every minute:
   - door-to-provider (D2P) mean, median and P90 over the last 60 minutes, with sample size;
   - EDWIN, gEDWIN and NEDOCS with bands;
   - flow metrics (LWBS, length of stay (LOS), boarding, staffing);
   - an AI-generated explanation grounded in computed drivers.
3. Crowding emerges from simulated patient journeys, so waits rise with occupancy. It is not painted on.
4. Raw PHI exists only in `bronze` and `sim`, protected by grants. Silver and everything downstream hold **physically de-identified** data. Structured PHI fields are redacted deterministically. Free text (triage notes, chief complaints) is masked with `ai_mask` plus a deterministic regex pass. **No Unity Catalog column masks or ABAC are used.**
5. Every table and column has a comment and the required governed tags, verified by an automated check.
6. Genie answers the benchmark question set about current and historical crowding.
7. A customer can deploy with a handful of bundle commands and tear everything down cleanly.

**Non-goals (for now):** user write-back from the app (reserved schema only), geographic map, row-level security by region, real EHR/HL7/FHIR integration, clinical validity of AI-suggested ESI.

---

## 2. Architecture overview

```
 ┌──────────────────────── ed_streaming (continuous job, classic standard-access cluster) ─────────────────────────┐
 │                                                                                                               │
 │ generator ──JSON──▶ /Volumes/<cat>/bronze/landing/events ──Auto Loader──▶ bronze.ed_events                    │
 │ (rate src, 1-min tick,                                                         │                              │
 │  sim.visit_plan state)                                         ┌───────────────┴───────────────┐              │
 │                                                                ▼                               ▼              │
 │                                       silver_visits (foreachBatch MERGE)        silver_notes_ai (AI funcs)    │
 │                                       silver.ed_visits + quarantine             silver.triage_notes           │
 │                                                                └───────────────┬───────────────┘              │
 │                                                                                ▼                              │
 │                                gold_metrics (1-min ticker) ──▶ gold.facility_current, gold.facility_snapshot_5min,
 │                                                                gold.ed_board_current                          │
 │                                gold_narratives (15-min)    ──▶ gold.facility_narrative, gold.network_narrative│
 └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
          │ synced tables (CDF)                                     │ UC tables / views
          ▼                                                         ▼
   Lakebase (Postgres) ◀── FastAPI + React app ──▶ Genie Conversation API ──▶ Genie space (gold + ref + masked notes)
```

Choices made during design:

- **Plain Delta tables + Structured Streaming** instead of Spark Declarative Pipelines, to keep things simple. The cost is that data-quality checks, upserts and checkpoints are written by hand. The design handles all three explicitly (§8, §15).
- **All metrics are computed in the gold layer** (Spark), with a single implementation. Lakebase only serves pre-computed results. This keeps the app and Genie consistent.
- **Gold uses a snapshot "ticker"**: a 1-minute streaming trigger whose `foreachBatch` recomputes point-in-time metrics from the current silver state. We rejected stateful `transformWithState` because it is harder to get right with late events and harder to backfill.

---

## 3. Unity Catalog layout

- **Catalog:** bundle variable `catalog`.
  - **dev default is `hls_jagjit_lb_playground_catalog`** (existing). Verified on the dev workspace: the deployer has **no `CREATE CATALOG`** on the shared metastore.
  - The `customer` target defaults to `ed_ops`. When `create_catalog = true`, `ed_setup` creates it; otherwise it must already exist.
- **Schemas:** physical name is `{schema_prefix}{logical}`, with bundle variable `schema_prefix` (default `ed_`), so the solution can sit inside a shared catalog.
  - This spec uses **logical** names throughout: `silver.ed_visits` means `<catalog>.ed_silver.ed_visits`.
  - The validation run uses prefix `ed_validate_` in the same catalog.

| Schema | Contents |
|---|---|
| `bronze` | `ed_events`; volume `landing` (raw JSON events) |
| `silver` | `ed_visits`, `triage_notes`, `ed_events_quarantine` |
| `gold` | `facility_current`, `facility_snapshot_5min`, `ed_board_current`, `facility_narrative`, `network_narrative`; view `visit_facts` |
| `ref` | `facility`, `physician_roster`; volume `checkpoints` |
| `sim` | `visit_plan`, `generator_state` (generator internals, not for consumers) |
| `serving` | Lakebase **synced tables** (§12). Each one is a UC object named after its gold/ref source, for example `serving.facility_current`. The sync pipelines' storage is in this schema too |

All timestamps are stored as UTC `TIMESTAMP`. Local-time logic (diurnal curves, display) uses bundle variable `timezone` (default `America/Chicago`). Every facility is in that one timezone.

---

## 4. Reference data

### `ref.facility` (50 rows; dbldatagen, seeded)

`facility_id` (PK, `FAC001`…`FAC050`), `facility_name` (synthetic), `region`, `city`, `state`, `latitude`, `longitude`, `facility_type` (`level_1_trauma`, `community`, `rural_critical_access`, `pediatric`), `ed_beds` (B_T, 8–80), `inpatient_beds` (25–600), `baseline_daily_visits`, `scenario_profile`.

The size attributes are correlated with type. For example, rural critical access sites have 8–15 ED beds and 25–60 inpatient beds. Level I trauma sites have 50–80 ED beds and 300–600 inpatient beds.

`scenario_profile` controls how each facility behaves. The mix is fixed so the demo always tells a story:

| Profile | Count (of 50) | Behaviour |
|---|---|---|
| `normal` | 30 | Baseline |
| `chronic_boarding` | 5 | Long inpatient-bed waits for admits, so NEDOCS runs high |
| `understaffed` | 5 | Thin night shifts, so EDWIN runs high and D2P is long |
| `surge_prone` | 5 | Join regional and live surge events |
| `high_performer` | 5 | Faster flow, more physicians, fast-track |

### `ref.physician_roster`

PK `(facility_id, shift_start)`. Columns: `shift_start`, `shift_end`, `attendings_on_duty` (Nₐ). There are 3 shifts per day (07–15, 15–23, 23–07 local). Coverage scales with `baseline_daily_visits`, and the profile adjusts it. Setup generates the backfill window plus 7 days ahead. The generator extends it daily.

---

## 5. Event model

Events are JSON lines written to `/Volumes/<cat>/bronze/landing/events/yyyy/mm/dd/hh/tick_<epoch>.json`.

**Envelope (all events):** `event_id` (UUID), `event_type`, `event_ts` (when the event happened), `emitted_ts` (when it was written), `facility_id`, `visit_id`, `payload` (struct).

`visit_id` is built to sort well: `{facility_id}-{arrival yyyyMMddHHmm}-{seq:03d}`, for example `FAC017-202611091432-003`. Liquid clustering then keeps each MERGE inside a small set of recent files.

| `event_type` | Payload | Notes |
|---|---|---|
| `ARRIVAL` | `arrival_mode` (`walk_in`/`ambulance`), `patient_name`, `mrn`, `dob`, `sex`, `phone`, `address` (struct: `street`, `city`, `state`, `zip`), `chief_complaint_text` | Contains raw PHI; de-identified in `silver_visits` (§11.2) |
| `TRIAGE` | `esi_level` (1–5), `triage_note` (free text with embedded PHI and vitals), `chief_complaint_text` | |
| `BED_ASSIGNED` | `bed_id` | |
| `PROVIDER_GREET` | `provider_id` | D2P = greet − arrival |
| `VENT_START` / `VENT_END` | — | A subset of ESI 1–2 patients |
| `DISPOSITION` | `disposition` (`DISCHARGE`/`ADMIT`/`TRANSFER`) | For admits, `event_ts` is the admit decision time |
| `LWBS` | — | Ends the journey; no greet or disposition follows |
| `DEPARTURE` | — | For admits, departure − disposition = boarding time |

**Ordering rules:**
- `ARRIVAL < TRIAGE ≤ {BED_ASSIGNED, PROVIDER_GREET}` (either order; fast-track patients are greeted in triage) `< DISPOSITION < DEPARTURE`.
- `LWBS` can occur any time after `TRIAGE` and before `PROVIDER_GREET`.
- `VENT_START` and `VENT_END` fall between `BED_ASSIGNED` and `DEPARTURE`.

---

## 6. Patient-journey generator

### Mechanics

The generator is a Structured Streaming query on a `rate` source with a `processingTime` trigger of `tick_seconds` (default 60). Each micro-batch's `foreachBatch` runs one **tick** at wall-clock time `now`:

1. **Arrivals.** For each facility, draw a Poisson count with mean
   `baseline_daily_visits × hourly_curve[local_hour] × dow_factor[local_dow] × scenario_multiplier(facility, now) × tick_seconds / 86400 × 24 / Σhourly_curve`
   (the curve is normalised so the daily total matches the baseline). **dbldatagen** generates each arrival's attributes:
   - demographics and PHI;
   - ESI, using a mix that depends on facility type (for example, community: ESI1 1%, ESI2 12%, ESI3 45%, ESI4 32%, ESI5 10%);
   - complaint archetype, arrival mode (ambulance share is higher for ESI 1–2), and a patience threshold for LWBS.
2. **Plan the journey with load feedback.** Interval durations are drawn from lognormal distributions and scaled by facility load. Load is computed from `sim.visit_plan` at the arrival time:
   - **occupancy** = census / `ed_beds`;
   - **staffing pressure** = census / Nₐ.

   The base intervals are door-to-triage, triage-to-bed, door-to-provider, provider-to-disposition, and disposition-to-departure (discharge) or the boarding time (admit, scaled up for `chronic_boarding`). Provider waits grow sharply as occupancy approaches and exceeds 1. If the planned wait exceeds the patient's patience, the journey becomes LWBS. Disposition probabilities depend on ESI. Ventilation probability is non-zero only for ESI 1–2. The planned visit is upserted into `sim.visit_plan`.
3. **Emit** every planned event with `event_ts ∈ (last_emitted_ts, now]`, then advance `sim.generator_state.last_emitted_ts`.
4. **Chaos** (configurable rates):
   - `dup_rate` 1%: re-emit an event emitted in the last 30 minutes, with the same `event_id`. This stays inside the silver dedupe watermark.
   - `late_rate` 2%: hold back an event for 5–20 minutes past its `event_ts`.
   - `malformed_rate` 0.5%: null `visit_id`, ESI outside 1–5, or an unknown `facility_id`.

### Backfill

`ed_setup` runs the **same tick function** with a simulated clock, in hourly steps from `now − backfill_days` (default 14) up to `now`. The output goes to the same landing path, so backfill data flows through the normal ingestion path. Hourly steps keep the load feedback while keeping the step count small (336).

### Scenarios

- **Regional flu surge** on backfill days 9–11: arrivals ×1.4 and higher ESI 2–3 share for `surge_prone` facilities and their region.
- **Live surge:** `live_surge_offset_min` (default 30) after streaming starts, two `surge_prone` facilities get arrivals ×2.0 for `live_surge_duration_min` (default 90). Demo viewers watch them tip from busy to crowded.

### Triage-note templates

About 40 hand-written templates in `generator/notes_templates.py` have slots for name, DOB, MRN, phone, vitals, onset and symptoms. Templates are chosen by complaint archetype and ESI.

About 3% of notes are deliberately **under-triaged**: high-acuity cues in the text (for example, crushing chest pain with diaphoresis, or SpO₂ 86%) paired with a nurse ESI of 4–5.

There are no LLM calls in generation, so runs are deterministic given `seed`.

### Division of labour

dbldatagen produces reference data and per-arrival attribute rows. The journey and timing engine (`generator/journey.py`) is custom Python and pandas, because dbldatagen has no notion of sequential state.

---

## 7. Metric definitions

All metrics are computed for a facility *f* at time *T* by **one** function: `metrics.snapshots.compute_snapshots(visits, roster, facilities, as_of_times)`. Scalar formulas are in `metrics/formulas.py` as pure functions with Spark equivalents.

**In-ED set:** `arrival_ts ≤ T AND (departure_ts IS NULL OR departure_ts > T)`, where `departure_ts` includes LWBS.

### 7.1 Door-to-provider (headline wait)

| Column | Definition |
|---|---|
| `d2p_mean_min`, `d2p_median_min`, `d2p_p90_min` | `greet_ts − arrival_ts` in minutes, for visits with `greet_ts ∈ (T−60m, T]`. Uses the exact `percentile` |
| `d2p_sample_n` | Count of those visits. The UI marks values as **low sample** when n < 5. All three statistics are null when n = 0 |
| `waiting_now_n`, `longest_current_wait_min` | In ED with no greet yet and not LWBS: count, and max(T − arrival) |

### 7.2 Crowding indices

Source: *gEDWIN: a simple and practical index for real-time monitoring of emergency department crowding*, BMC Emergency Medicine, 2025 (PMC12648796).

| Symbol / column | Definition |
|---|---|
| `census` (paper's B_A) | Count of the in-ED set, including the waiting room |
| `n_esi1`…`n_esi5`, `n_untriaged` | Census by triage level |
| `occupancy_ratio` (OR) | census / `ed_beds`. The raw value may exceed 1 and is shown as % |
| `OR_c` | clamp(OR, 0.01, 0.99). The 0.99 cap comes from the paper. **The 0.01 floor is our addition** (logit(0) = −∞) |
| tᵢ | 6 − ESI (ESI-1 → 5). **Untriaged patients use t = 3** (our assumption) |
| `acuity_ratio` (AR) | Σnᵢtᵢ / census |
| `attendings_on_duty` (Nₐ) | From `ref.physician_roster` at T |
| `edwin` | (1/Nₐ) · OR_c/(1−OR_c) · AR (paper's eq. 6; equals eq. 5 whenever OR < 1) |
| `edwin_band` | `Active` < 1.5 ≤ `Busy` ≤ 2 < `Crowded` |
| `gedwin` | ln(OR_c/(1−OR_c)) + ln(AR). **Natural log is assumed** (the paper doesn't state the base; its analysis used R, where `log()` is natural) |
| `gedwin_band` | `Active` < 2.06 ≤ `Busy` ≤ 2.34 < `Crowded`. **This is the headline status** |
| `gedwin_delta_1h` | gEDWIN(T) − gEDWIN(T − 1h); drives the trend arrow |

NEDOCS (Weiss et al. 2004):

| Column | Definition |
|---|---|
| `boarders_n` | In ED with `disposition = 'ADMIT'` |
| `vents_n` | In ED with `vent_start_ts ≤ T` and (`vent_end_ts` null or > T) |
| `longest_boarder_h` | max(T − disposition_ts) over boarders; 0 if there are none |
| `last_bedded_wait_h` | (bed_assigned_ts − arrival_ts) of the most recent `BED_ASSIGNED` ≤ T within 24 h; 0 if there is none |
| `nedocs` | clamp(−20 + 85.8·(census/ed_beds) + 600·(boarders_n/inpatient_beds) + 13.4·vents_n + 0.93·longest_boarder_h + 5.64·last_bedded_wait_h, 0, 200) |
| `nedocs_level` | 1 Not busy (0–20), 2 Busy (21–60), 3 Extremely busy (61–100), 4 Overcrowded (101–140), 5 Severely overcrowded (141–180), 6 Dangerously overcrowded (181–200) |

**Edge cases**

- census = 0: `edwin = 0`, `acuity_ratio = null`, `gedwin = null`, `gedwin_band = 'Active'`.
- Nₐ = 0: `edwin = null`, `edwin_band = null`, and `dq_flags` contains `no_attending_on_duty`. gEDWIN and NEDOCS are unaffected.

### 7.3 Flow and supporting metrics

1-hour samples are too sparse for rates and distributions, so flow metrics use **24 h windows**:

| Column | Definition | Window |
|---|---|---|
| `lwbs_rate_24h` | LWBS ÷ all departures | 24 h |
| `los_median_h_24h`, `los_p90_h_24h` | departure − arrival, discharged patients | 24 h |
| `boarding_median_h_24h` | departure − disposition, admitted patients who departed | 24 h |
| `admit_rate_24h` | admits ÷ dispositions | 24 h |
| `arrivals_1h`, `ambulance_arrivals_1h` | Arrivals in (T−60m, T] | 1 h |
| `arrival_surge_ratio` | `arrivals_1h` ÷ the facility's 14-day mean arrivals for the same local hour | 1 h vs baseline |
| `patients_per_attending` | census ÷ Nₐ | point-in-time |
| `undertriage_flags_n` | In-ED patients with `undertriage_flag = true` (from §9) | point-in-time |

### 7.4 Drivers (input to narratives)

The driver candidates are `occupancy_ratio`, `boarder_share` (boarders_n / census), `patients_per_attending`, `arrival_surge_ratio` and `high_acuity_share` (ESI 1–2 / census). Each is compared with the facility's own 14-day baseline at the same local hour, as a z-score using the baseline mean and standard deviation over 5-minute snapshots.

`top_drivers` is an array of up to 3 `struct<driver, value, baseline, z>`, ordered by |z| and keeping only |z| ≥ 1.

### 7.5 Late-event correction

On each tick, `gold_metrics` writes `facility_current` for T = now. It also recomputes and MERGEs `facility_snapshot_5min` for every 5-minute T in the trailing 30 minutes. The generator's maximum lateness is 20 minutes, so history corrects itself.

---

## 8. Pipeline tasks

All run as tasks of the continuous `ed_streaming` job, each with its own checkpoint at `/Volumes/<cat>/ref/checkpoints/<stream>/v<checkpoint_version>`.

| Task | Implementation |
|---|---|
| `generator` | §6 |
| `bronze_ingest` | Auto Loader (`cloudFiles`, JSON) with an explicit schema and `_rescued_data`. Appends to `bronze.ed_events` with `_ingest_ts`, `_source_file`, `ingest_date` |
| `silver_visits` | Stream from `bronze.ed_events` with `withWatermark('event_ts', '1 hour').dropDuplicatesWithinWatermark(['event_id'])`; `foreachBatch`: (1) validate and send failures to `silver.ed_events_quarantine` with a `reason_code`; (2) **de-identify** structured PHI on `ARRIVAL` events (§11.2) and drop the raw PHI fields and `chief_complaint_text`, so no raw PHI is written to silver; (3) pivot events to a per-visit partial row; (4) `MERGE` on `(facility_id, visit_id)`, setting each stage timestamp with `COALESCE(target, source)`. Out-of-order events fill in without overwriting, and replays or duplicates outside the watermark are harmless (idempotent) |
| `silver_notes_ai` | Stream from `bronze.ed_events` filtered to `TRIAGE`; `foreachBatch` runs the AI chain (§9) and MERGEs into `silver.triage_notes` on `(facility_id, visit_id)`. It **never writes to `silver.ed_visits`**: each silver table has exactly one writer, which avoids concurrent-MERGE conflicts. Gold joins the two |
| `ai_retry_sweep` | Separate task with a `rate` source and 15-minute trigger: re-runs the AI chain for `silver.triage_notes` rows with `ai_status = 'error'` and `ai_attempts < 3`. It writes only to `silver.triage_notes`; because it updates only error rows and `silver_notes_ai` only inserts new visits, the two don't conflict in practice under row-level concurrency |
| `gold_metrics` | `rate` source, 1-minute trigger. `foreachBatch` reads the current state of `silver.ed_visits` left-joined to `silver.triage_notes` (in-ED visits plus departures within 24 h; 14-day hourly baselines cached and refreshed hourly), runs `compute_snapshots`, and MERGEs `facility_current`, `facility_snapshot_5min` (trailing 30 min) and `ed_board_current`. **No deletes anywhere** (decided): the board is insert/update only. Visits that left the ED in this tick are updated to `in_ed = false` with `departed_ts` |
| `gold_narratives` | `rate` source, 15-minute trigger. `ai_query` per facility over `facility_current` plus `top_drivers`, then one network-wide call. MERGEs both narrative tables |

Every table has exactly one streaming writer. `ed_setup` writes before streaming starts.

---

## 9. AI functions

### 9.1 Triage-note enrichment (`silver_notes_ai`)

Run in this order. Everything after step 1 runs on masked text only.

1. `ai_mask(triage_note, array('person','phone number','date of birth','address','medical record number','email'))` produces `note_masked`. The same call is applied to `chief_complaint_text`, producing `chief_complaint_masked`. The `TRIAGE` event carries the chief complaint, so `silver_visits` never needs to call an LLM.
2. **Deterministic post-check**: regex for phone, `MRN-\d+`, dates and emails over `note_masked` and `chief_complaint_masked`. Any match is replaced with `[REDACTED]` and sets `residual_phi_flag = true`. This gives a measured `ai_mask` miss rate.
3. `ai_classify(note_masked, array('cardiac','respiratory','neurological','trauma','gastrointestinal','infectious','psychiatric','obstetric','musculoskeletal','other'))` produces `complaint_category`.
4. `ai_extract(note_masked, array('heart_rate','blood_pressure','respiratory_rate','spo2','temperature','pain_score','symptom_onset'))` returns **strings that include units** (verified: `"89% RA"`, `"9/10"`, `"37.1C"`, `"92/58"`, `"40 min"`). Typed columns are produced with `regexp_extract` and then `try_cast`:
   - `spo2` takes the leading number;
   - `pain_score` takes the number before `/10`;
   - `temperature_c` takes the number and converts °F when it is over 50;
   - `bp_systolic` and `bp_diastolic` split on `/`;
   - `symptom_onset` stays as text.
5. `ai_query(llm_endpoint, <ESI prompt>, failOnError => false)` with **no `responseFormat`**.
   - Verified: `responseFormat` is **rejected by Claude 5.x endpoints** (*"structured output … requires forced tool use, which newer Anthropic models reject"*). Claude 4.x accepts it but ignored the length limits.
   - The prompt asks for **only** a JSON object `{"suggested_esi": <1-5>, "rationale": <≤30 words>}`. The result is parsed with `from_json(regexp_extract(result, '(\\{.*\\})', 1), 'suggested_esi INT, rationale STRING')`. This was verified clean on `databricks-claude-sonnet-5-5`.
   - A null parse, or `suggested_esi` outside 1–5, sets `ai_status = 'parse_error'`, which the retry sweep handles. An `errorMessage` sets `ai_status = 'error'`.
   - `undertriage_flag = (esi_level − suggested_esi) ≥ 2`. The UI labels this as demo decision-support, not clinical advice.

`silver.triage_notes` stores **only** `note_masked`, `chief_complaint_masked` and the derived columns; the raw text exists only in bronze.

**Backfill cost control:**
- Notes with `event_ts < now − ai_backfill_days` (default 2) get only the deterministic regex masking, with `ai_enriched = false`.
- Live notes always get the full chain.

### 9.2 Narratives (`gold_narratives`)

- **`facility_narrative`:**
  - The prompt includes the facility's current metrics and `top_drivers` JSON, with the instruction "use only these numbers; do not invent values".
  - Output contract (prompt-enforced JSON, parsed with `from_json`, same approach as §9.1 step 5): `{headline STRING, explanation STRING (≤60 words), recommended_actions ARRAY<STRING> (≤3)}`. If parsing fails, the previous narrative is kept.
  - The table stores `input_json`, `model`, `generated_at`.
- **`network_narrative`:** one row. The input is all facilities' bands, gEDWIN, D2P median and top driver. The output covers which facilities are crowded and why, which are doing well, and network-level actions.

`llm_endpoint` is a bundle variable, default `databricks-claude-sonnet-5-5` (confirmed READY on the dev workspace).

---

## 10. Table catalog

All tables are **managed Delta**, liquid-clustered on their **MERGE keys** (append-only tables on their main query keys), with **CDF enabled** on every table that is synced to Lakebase. `metadata/tables.yml` is the single source for columns, types, comments, PKs, clustering and tags. The DDL is generated from it.

| Table | Write | PK / MERGE key | `CLUSTER BY` | CDF | Synced |
|---|---|---|---|---|---|
| `bronze.ed_events` | append | — (`event_id` unique after dedupe downstream) | `(facility_id, event_ts)` | no | no |
| `silver.ed_visits` | MERGE | `(facility_id, visit_id)` | `(facility_id, visit_id)` | yes | no |
| `silver.triage_notes` | MERGE | `(facility_id, visit_id)` | `(facility_id, visit_id)` | no | no |
| `silver.ed_events_quarantine` | append | — | `(ingest_date, facility_id)` | no | no |
| `gold.facility_current` | MERGE | `(facility_id)` | `(facility_id)` | yes | Continuous |
| `gold.facility_snapshot_5min` | MERGE | `(facility_id, snapshot_ts)` | `(facility_id, snapshot_ts)` | yes | Continuous |
| `gold.ed_board_current` | MERGE (insert/update only) | `(facility_id, visit_id)` | `(facility_id, visit_id)` | yes | Continuous |
| `gold.facility_narrative` | MERGE | `(facility_id)` | `(facility_id)` | yes | Continuous |
| `gold.network_narrative` | MERGE | `(network_id)` (one row, `'NETWORK'`) | `(network_id)` | yes | Continuous |
| `gold.visit_facts` | **view** (see below) | — | — | — | no |
| `ref.facility` | MERGE | `(facility_id)` | `(facility_id)` | yes | Snapshot |
| `ref.physician_roster` | MERGE | `(facility_id, shift_start)` | `(facility_id, shift_start)` | no | no |
| `sim.visit_plan` | MERGE | `(facility_id, visit_id)` | `(facility_id, visit_id)` | no | no |
| `sim.generator_state` | MERGE | `(generator_id)` | `(generator_id)` | no | no |

### Key column sets

- **`silver.ed_visits`:**
  - `facility_id`, `visit_id`, `arrival_ts`, `arrival_mode`, `esi_level`, `triage_ts`, `bed_assigned_ts`, `greet_ts`, `vent_start_ts`, `vent_end_ts`, `disposition`, `disposition_ts`, `lwbs_ts`, `departure_ts`;
  - de-identified demographics (§11.2): `patient_name_redacted`, `mrn_hash`, `birth_year`, `age_at_arrival`, `sex`, `phone_last4`, `city`, `state`;
  - `last_event_ts`, `updated_at`.
- **`silver.triage_notes`:** `facility_id`, `visit_id`, `triage_ts`, `esi_level`, `note_masked`, `chief_complaint_masked`, `residual_phi_flag`, `complaint_category`, `heart_rate`, `bp_systolic`, `bp_diastolic`, `respiratory_rate`, `spo2`, `temperature_c`, `pain_score`, `symptom_onset`, `suggested_esi`, `esi_rationale`, `undertriage_flag`, `ai_enriched`, `ai_status`, `ai_error`, `ai_attempts`, `processed_at`.
- **`gold.visit_facts`** (view): the operational columns of `silver.ed_visits` (plus `age_at_arrival`, `sex`; without `patient_name_redacted`, `phone_last4` or `mrn_hash`) left-joined to the non-text AI columns of `silver.triage_notes`, plus derived `d2p_min`, `los_h`, `boarding_h`.
- **`gold.facility_current` / `facility_snapshot_5min`:**
  - keys and identity: `facility_id`, `snapshot_ts`, `computed_at`;
  - every metric column in §7;
  - `top_drivers`, `dq_flags`.

  `facility_current` also has `facility_name`, `facility_type`, `region` (denormalised for the app).
- **`gold.ed_board_current`:** `facility_id`, `visit_id`, `in_ed` (boolean; the app and Genie filter on `in_ed = true`), `departed_ts`, `status` (`waiting`, `in_bed`, `boarding`, `departed`), `esi_level`, `suggested_esi`, `undertriage_flag`, `complaint_category`, `minutes_in_ed`, `minutes_waiting`, `arrival_mode`, `on_vent`. No PHI.

---

## 11. Governance

### 11.1 Access by layer

| Layer | PHI | Grants |
|---|---|---|
| `bronze` | Raw (structured + free text) | Job service principal, `ed_admins` |
| `sim` | Raw (generator internals) | Job service principal, `ed_admins` |
| `silver.ed_visits` | None (structured PHI de-identified before write) | Analysts |
| `silver.triage_notes` | None (free text masked by `ai_mask` + regex before write) | Analysts |
| `gold`, `ref` | None | Analysts, app service principal, Genie users |

### 11.2 PHI de-identification (static, in the pipeline)

**No Unity Catalog column masks or ABAC** (decided). PHI is removed **physically** before anything is written to silver. Raw PHI exists only in `bronze` and `sim`, protected by grants (§11.1). There are no dynamic masks, so no identity ever needs a "PHI reader" privilege beyond bronze.

**Structured fields** are redacted deterministically in `silver_visits`:

| Raw field (bronze) | Silver column | Rule | `deid_method` tag |
|---|---|---|---|
| `patient_name` | `patient_name_redacted` | constant `'REDACTED'` | `redacted` |
| `mrn` | `mrn_hash` | `sha2(concat(mrn, <salt>), 256)`; salt in secret scope `ed_ops`, key `mrn_salt` | `hashed` |
| `dob` | `birth_year`, `age_at_arrival` | year only; whole years at `arrival_ts` | `generalized` |
| `phone` | `phone_last4` | last 4 digits | `generalized` |
| `address` | `city`, `state` | street and ZIP dropped | `generalized` |

`patient_name_redacted` and `phone_last4` are kept on purpose, to show "this is what redaction looks like" in the demo. `mrn_hash` lets analysts join and count distinct patients without seeing the MRN.

**Free text** (`triage_note`, `chief_complaint_text`) is masked with `ai_mask` plus the regex post-check in `silver_notes_ai` (§9.1). Its `deid_method` tag is `ai_mask`.

### 11.3 Governed tags (created by `ed_setup`)

| Tag | Level | Allowed values |
|---|---|---|
| `layer` | table | `bronze`, `silver`, `gold`, `ref`, `sim` |
| `domain` | table | `emergency_department` |
| `data_classification` | table, column | `phi`, `confidential`, `internal` |
| `contains_phi` | table | `true`, `false` |
| `data_origin` | table | `synthetic` |
| `phi_type` | column | `name`, `mrn`, `dob`, `contact`, `address`, `free_text`. Applied to raw columns in `bronze` and `sim`, and to the de-identified silver columns derived from them |
| `deid_method` | column | `redacted`, `hashed`, `generalized`, `ai_mask`. Required on every silver column carrying a `phi_type` tag |

### 11.4 Comments

Every table and column comment comes from `metadata/tables.yml`. Column comments include units and window definitions, for example: *"Door-to-provider median in minutes, over patients greeted in the 60 minutes ending snapshot_ts."*

### 11.5 Automated checks (`governance/checks.py`, run in `ed_setup` and `ed_validate`)

1. Every table and column in the solution's schemas has a non-empty comment.
2. Every table has `layer`, `domain`, `data_classification`, `contains_phi`, `data_origin`.
3. No column tagged `phi_type` exists in `gold`, `ref`, any synced table, or any table in the Genie space.
4. Every `phi_type`-tagged column outside `bronze` and `sim` also has a `deid_method` tag.
5. **Content scan:** the PHI regex set (phone, `MRN-\d+`, full dates of birth, emails) finds no matches in any `silver`, `gold` or `ref` string column (sampled). The generator's synthetic name list is also checked against `silver.triage_notes.note_masked` to estimate the `ai_mask` miss rate.

---

## 12. Lakebase

- **Lakebase Autoscaling** (decided).
  - The bundle declares a `postgres_projects` resource `ed-ops`, a `postgres_branches` resource per target (`dev`, or `production` for `customer`) and a `postgres_endpoints` read-write compute endpoint, with min/max capacity from `lakebase_min_cu` / `lakebase_max_cu` and scale-to-zero enabled.
  - Postgres database `ed_ops`: created by `ed_setup` through the SDK (`postgres.create_database`, owned by the deployer's branch role), because bundles don't declare Postgres databases.
- **Synced tables** per §10 are created by `serving/lakebase_sync.py` in `ed_setup` through the SDK (`w.postgres.create_synced_table`), because bundles don't declare autoscaling synced tables yet. The step is idempotent: it creates a table if missing and leaves matching specs alone.
  - Verified: the synced table can live in a **standard UC catalog**; no registered Lakebase catalog (and so no `CREATE CATALOG`) is needed.
  - UC name: `<catalog>.serving.<table>`. Postgres name: schema = the physical `serving` schema name, table = `<table>`. For example, Postgres `ed_serving.facility_current`.
  - Spec: `branch`, `postgres_database = 'ed_ops'`, `source_table_full_name`, `primary_key_columns`, `scheduling_policy`, `create_database_objects_if_missing = true`, `new_pipeline_spec.storage_catalog/storage_schema = <catalog>/serving`.
  - **Measured latency:** a MERGE (updates plus insert) on a liquid-clustered CDF source reached Postgres in **about 13 seconds** in Continuous mode.
  - Each synced table's primary key is the PK in §10.
  - Sync modes are bundle variables: `sync_mode_live` (default `CONTINUOUS`) and `sync_mode_ref` (default `SNAPSHOT`).
  - Continuous sync and the app's 30-second polling keep the compute awake during demos; scale-to-zero only saves cost when the solution is idle.
- The app connects through a `postgres` app resource (project branch plus database) with `CAN_CONNECT_AND_CREATE`.
- Postgres schema `app`, owned by the app service principal, is created empty and reserved for future write-back. Synced tables can't be written to, so write-back will need its own tables.
- The app's service principal gets `SELECT` on the synced tables only.

---

## 13. Databricks App

### 13.1 Backend: FastAPI (Python)

- Lakebase connection: a `psycopg` (v3) connection pool. Host and user come from the app's database resource. A background task refreshes the OAuth database credential before it expires.
- A 15-second in-process TTL cache per endpoint.
- Data access goes through a `Repository` interface (a Lakebase implementation and a fake for tests).
- Endpoints:

| Method | Path | Returns |
|---|---|---|
| GET | `/api/health` | status, `data_age_seconds` (from `max(computed_at)`) |
| GET | `/api/network/summary` | KPI tiles + `network_narrative` |
| GET | `/api/facilities` | all `facility_current` rows (+ 6 h gEDWIN sparkline series) |
| GET | `/api/facilities/{id}` | current row + `facility_narrative` |
| GET | `/api/facilities/{id}/history?hours=24\|168` | `facility_snapshot_5min` series |
| GET | `/api/facilities/{id}/board` | `ed_board_current` rows |
| GET | `/api/definitions` | formulas, bands, assumptions, citations |
| POST | `/api/genie/messages` | start or continue a Genie conversation (on-behalf-of-user token) |
| GET | `/api/genie/conversations/{conv}/messages/{msg}` | poll result: text, SQL, result table |

- **The app must be able to call Genie** (requirement).
  - The Genie space is declared as an app resource (`genie_space`, permission `CAN_RUN`), so the app service principal can always call it.
  - When on-behalf-of-user authorization is enabled, the backend calls Genie with the user's forwarded token, so the user's UC permissions apply. Otherwise it uses the app service principal, which has `SELECT` only on the Genie space's PHI-free tables. Both paths are implemented and tested.

### 13.2 Frontend: React + Vite + TypeScript + `react-apexcharts`

The frontend is built at deploy time and served by FastAPI as static files (one app, one port). It polls every 30 seconds. A **"data delayed"** banner appears when `data_age_seconds > 300`.

1. **Network overview**
   - Network narrative banner.
   - KPI tiles: number of facilities in Crowded, Busy and Active; network median D2P; total census; total boarders.
   - Ranked horizontal bar chart of all 50 facilities by gEDWIN, coloured by band, with annotation lines at 2.06 and 2.34.
   - Bubble chart: x = occupancy %, y = D2P median, size = census, colour = band.
   - Heatmap: facility × local hour for the last 24 h, coloured by gEDWIN band.
   - Sortable facility table with a 6 h gEDWIN sparkline. Clicking a row opens the facility detail.
2. **Facility detail**
   - Radial gauges for gEDWIN, EDWIN and NEDOCS, each with its own bands.
   - D2P mean, median and P90 with n and the low-sample badge.
   - 24 h / 7-day line charts of D2P and gEDWIN with band annotations.
   - Stacked area chart of census by ESI.
   - Boarding and flow stats.
   - Narrative card with `top_drivers`.
   - De-identified ED board, with under-triage rows highlighted.
3. **Ask Genie:** a slide-over chat panel showing the answer, a collapsible SQL view and a result table.
4. **Definitions:** content from `/api/definitions`, including all assumptions in §7 and the paper citations.

There is no geographic map (ApexCharts can't draw one). The heatmap and bubble chart cover the cross-facility view. Leaflet is a possible later addition.

---

## 14. Genie space

Defined in `genie/space.json` and deployed as a bundle `genie_spaces` resource, running on serverless SQL warehouse `warehouse_id`.

- **Tables:**
  - `gold.facility_current`, `gold.facility_snapshot_5min`, `gold.visit_facts`
  - `gold.facility_narrative`, `gold.network_narrative`
  - `silver.triage_notes`
  - `ref.facility`, `ref.physician_roster`
- **Instructions:**
  - glossary (D2P, census, boarding, LWBS, ESI, EDWIN, gEDWIN, NEDOCS);
  - band thresholds;
  - "crowded" means `gedwin_band = 'Crowded'` unless the user names EDWIN or NEDOCS;
  - use `facility_current` for "now" questions and `facility_snapshot_5min` for history;
  - timestamps are UTC, so convert to `America/Chicago` for local-time questions;
  - `visit_facts` holds per-visit questions.
- **About 10 example SQL queries:**
  - top crowded now;
  - % of the last 24 h each facility spent Crowded;
  - P90 D2P trend for a facility;
  - worst boarding now;
  - under-triage counts by facility (7 days);
  - EDWIN versus gEDWIN rank disagreement;
  - LWBS rate by facility type;
  - busiest local hour of day;
  - facilities improving versus deteriorating (`gedwin_delta_1h`);
  - complaint-category mix for a facility today.
- **About 15 benchmarks** in `genie/benchmarks.yml` (question plus expected SQL), runnable from `ed_validate`.

---

## 15. Error handling

| Component | Failure | Handling |
|---|---|---|
| All streams | Crash or restart | Continuous job restarts with backoff. Each stream has its own checkpoint. `scripts/reset_checkpoints.sh` bumps `checkpoint_version` |
| Generator | Tick replayed | Deterministic file names; Auto Loader skips files it has seen; downstream `event_id` dedupe; `generator_state` is updated after the file is written |
| Bronze | Bad JSON or schema drift | Explicit schema, `_rescued_data`; never fails on a single record |
| Silver | Invalid event | Quarantine with `reason_code` (`missing_visit_id`, `invalid_esi`, `unknown_facility`, `impossible_order`, `unparseable`) |
| Silver | Late or out-of-order event | `COALESCE` MERGE; gold recomputes the trailing 30 minutes |
| AI | Endpoint error or timeout | `failOnError => false`, `ai_status='error'`; retry sweep up to 3 attempts; the row always keeps deterministic masking |
| Gold | Computation error | Task fails and restarts; idempotent MERGEs |
| Narratives | LLM failure | Keep the previous narrative; `generated_at` shows its age in the UI |
| App | Lakebase unavailable or stale | Retry with backoff; `/api/health` exposes data age; UI banner |

---

## 16. Testing

### 16.1 Local unit tests (pytest)

These run on the local Homebrew Spark 4.1.2 without Delta. All logic is written as pure functions or DataFrame transforms, so it can be tested without MERGE.

- `metrics/formulas.py` against hand-computed fixtures:
  - normal case;
  - OR ≥ 1 (cap);
  - census = 0;
  - Nₐ = 0;
  - untriaged patients;
  - NEDOCS worked example and clamp.
- `compute_snapshots`: small hand-built visit sets (D2P window boundaries, LWBS excluded from D2P, boarders, vents, 24 h windows, `top_drivers`).
- Event-to-visit folding: out-of-order, duplicates, LWBS.
- Generator: per-visit timestamp ordering, no greet after LWBS, boarding for every admit, arrival rate within tolerance of the configured mean (seeded), mean D2P higher under higher load.
- PHI regex post-check: catches each pattern; no false positives on vitals (for example `BP 120/80`).
- De-identification (`governance/deid.py`): each structured rule (redact, salted hash is stable and salt-dependent, birth year and age at arrival, last 4 of phone, city/state); the de-identified `ARRIVAL` row carries no raw PHI field.
- DDL generator: `tables.yml` produces the expected `CREATE TABLE … CLUSTER BY … COMMENT … TBLPROPERTIES`.

### 16.2 App tests

pytest + FastAPI `TestClient` with the fake `Repository`; vitest for band colouring, number formatting and low-sample logic.

### 16.3 `ed_validate` job (on the workspace)

Runs at small scale (5 facilities, 1-day backfill, about 10 live ticks) in the same catalog with `schema_prefix = ed_validate_`. Synced-table checks use a separate `validate` branch of the same Lakebase project, so the dev branch is never touched. It asserts:

1. Rows flow bronze → silver → gold.
2. Quarantine count is greater than 0 and matches the configured chaos.
3. Governance checks in §11.5 pass, including the PHI content scan.
4. A seeded **golden scenario** produces exactly the hand-calculated EDWIN, gEDWIN and NEDOCS. The scenario is a test-only facility `FAC000` in the validate schemas' `ref.facility` and `ref.physician_roster`, plus a fixed visit set loaded directly into `silver.ed_visits` and `silver.triage_notes`. It is never created in the main schemas.
5. Synced tables exist and have the same row counts as their sources.
6. Optionally, the Genie benchmark pass rate.

---

## 17. Deployment and configuration

### Databricks Asset Bundle (`databricks.yml`)

- **Targets:** `dev` (FE VM workspace `https://fevm-hls-jagjit-lb-playground.cloud.databricks.com`, CLI profile `fevm-playground`) and `customer` (template).
- **Artifacts:** Python wheel `ed_ops` and the frontend build.
- **Resources:**
  - Jobs:
    - `ed_setup` (serverless; idempotent): DDL, tags, grants, salt secret, ref data, backfill, historical snapshots, Lakebase synced tables, governance checks.
    - `ed_streaming` (continuous; **classic job cluster**, decided): **DBR 18 LTS** (`18.x-scala2.13`, Spark 4.1, which matches the local Spark 4.1.2 used for unit tests), **standard access mode** (supports Structured Streaming with `foreachBatch`), single driver plus 1–2 workers, `streaming_node_type`. Runs as a dedicated service principal with read/write on `bronze`, `sim`, `silver`, `gold` and `ref`, and read on secret scope `ed_ops`.
    - `ed_validate` (serverless).
  - The app.
  - The Genie space (bundle `genie_spaces` resource, built from `genie/space.json`).
  - Lakebase resources and synced tables (§12).

### Variables

| Variable | Default |
|---|---|
| `catalog` | dev: `hls_jagjit_lb_playground_catalog`; customer: `ed_ops` |
| `create_catalog` | dev: `false`; customer: `true` |
| `schema_prefix` | `ed_` |
| `facility_count` | 50 |
| `backfill_days` | 14 |
| `ai_backfill_days` | 2 |
| `tick_seconds` | 60 |
| `dup_rate` / `late_rate` / `malformed_rate` | 0.01 / 0.02 / 0.005 |
| `live_surge_offset_min` / `live_surge_duration_min` | 30 / 90 |
| `llm_endpoint` | `databricks-claude-sonnet-5-5` |
| `warehouse_id` | dev: `abef38163a09f120` (Serverless Starter Warehouse); required for other targets |
| `lakebase_min_cu` / `lakebase_max_cu` | 0.5 / 2 |
| `sync_mode_live` / `sync_mode_ref` | `CONTINUOUS` / `SNAPSHOT` |
| `timezone` | `America/Chicago` |
| `seed` | 42 |
| `checkpoint_version` | 1 |
| `streaming_node_type` | smallest general-purpose node |

### Teardown

`databricks bundle destroy` (removes jobs, app, Genie space and the Lakebase project) plus `scripts/teardown.sh`. It deletes the synced tables, then drops the `{schema_prefix}*` schemas, and drops the catalog only when `create_catalog = true`.

---

## 18. Repository layout

```
databricks.yml
resources/            jobs.yml, app.yml, lakebase.yml (postgres project/branch/endpoint), genie.yml
metadata/             tables.yml, tags.yml
src/ed_ops/
  config.py
  ddl/                generate.py, apply.py
  generator/          facilities.py, roster.py, journey.py, notes_templates.py, chaos.py, tick.py
  metrics/            formulas.py, snapshots.py
  pipeline/           bronze.py, silver_visits.py, silver_notes_ai.py, gold_metrics.py, gold_narratives.py
  governance/         tags.py, deid.py, grants.py, checks.py
  serving/            lakebase_sync.py, genie_space.py
jobs/                 thin entry points per task
genie/                space.json, benchmarks.yml
app/
  app.yaml
  backend/            FastAPI (main.py, repository.py, lakebase.py, genie.py)
  frontend/           React + Vite + TS
tests/                unit/, app/, integration/
docs/                 superpowers/specs/, architecture.md, runbook.md
scripts/              reset_checkpoints.sh, teardown.sh
README.md
```

---

## 19. Assumptions and verifications at implementation

**Design assumptions (also shown in the app's Definitions page)**

1. gEDWIN uses the natural log.
2. Untriaged patients are given t = 3.
3. OR is floored at 0.01 (the paper only caps at 0.99).
4. Flow metrics use 24 h windows; D2P uses 1 h.
5. All facilities are in one timezone.

**Verified on the dev workspace (2026-11-09), with scratch resources created and deleted**

| # | Check | Result |
|---|---|---|
| 1 | Streaming compute | Classic cluster (decided). DBR 18 LTS (Spark 4.1.0) in standard access mode runs a `rate` source with a `processingTime` trigger and a `foreachBatch` MERGE ✅ |
| 2 | AI functions on classic DBR 18 | `ai_mask`, `ai_classify` and `ai_extract` work ✅. `ai_extract` returns strings with units, so regex parsing is needed (§9.1) |
| 3 | `ai_query` structured output | `responseFormat` is **rejected** by Claude 5.x. Use a JSON-only prompt and `from_json` (§9.1); verified on `databricks-claude-sonnet-5-5` ✅ |
| 4 | Lakebase Autoscaling synced tables | Continuous sync from a liquid-clustered CDF Delta table, inserts and updates via MERGE, **about 13 s** to Postgres ✅. The synced table can sit in a standard UC catalog ✅. There are no deletes in the design (decided) |
| 5 | App on-behalf-of-user | An app requesting `dashboards.genie` and `sql` got both as **effective** user scopes ✅ |
| 6 | UC permissions | **No `CREATE CATALOG`** on the shared metastore. Use the existing catalog with `schema_prefix` (§3) |
| 7 | PHI masking | No ABAC and no UC column masks (decided); PHI is de-identified physically (§11.2) |
| 8 | Bundle resources | `genie_spaces`, `postgres_projects/branches/endpoints`; app resources `genie_space` and `postgres` ✅. Postgres databases and autoscaling synced tables are created by SDK in `ed_setup` |
| 9 | Endpoint | `databricks-claude-sonnet-5-5` is ready ✅ |

**Minor, checked during implementation:** installing `dbldatagen` (PyPI) as a job library on the standard-access-mode cluster.

---

## 20. References

- gEDWIN / EDWIN: *gEDWIN: a simple and practical index for real-time monitoring of emergency department crowding.* BMC Emergency Medicine, 2025. https://pmc.ncbi.nlm.nih.gov/articles/PMC12648796/
- NEDOCS: Weiss SJ et al. *Estimating the degree of emergency department overcrowding in academic medical centers: results of the National ED Overcrowding Study (NEDOCS).* Acad Emerg Med, 2004.
- Databricks AI functions: https://docs.databricks.com/aws/en/large-language-models/ai-functions
- Lakebase Autoscaling synced tables: https://docs.databricks.com/aws/en/oltp/projects/sync-tables
- ApexCharts: https://github.com/apexcharts/apexcharts.js
- dbldatagen: https://github.com/databrickslabs/dbldatagen
