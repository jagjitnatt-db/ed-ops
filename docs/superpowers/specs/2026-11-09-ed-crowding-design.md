# ED Crowding Command Center — Design Spec

- **Date:** 2026-11-09
- **Status:** Draft, awaiting review
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
4. No PHI reaches gold, Lakebase, the app or Genie. Structured PHI in silver is masked by tag-driven policies. Free-text PHI is masked with `ai_mask` plus a deterministic regex pass.
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

Catalog: bundle variable `catalog` (default `ed_ops`).

| Schema | Contents |
|---|---|
| `bronze` | `ed_events`; volume `landing` (raw JSON events) |
| `silver` | `ed_visits`, `triage_notes`, `ed_events_quarantine` |
| `gold` | `facility_current`, `facility_snapshot_5min`, `ed_board_current`, `facility_narrative`, `network_narrative`; view `visit_facts` |
| `ref` | `facility`, `physician_roster`; volume `checkpoints` |
| `sim` | `visit_plan`, `generator_state` (generator internals, not for consumers) |

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
| `ARRIVAL` | `arrival_mode` (`walk_in`/`ambulance`), `patient_name`, `mrn`, `dob`, `sex`, `phone`, `address`, `chief_complaint_text` | Contains structured PHI |
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
| `silver_visits` | Stream from `bronze.ed_events` with `withWatermark('event_ts', '1 hour').dropDuplicatesWithinWatermark(['event_id'])`; `foreachBatch`: (1) validate and send failures to `silver.ed_events_quarantine` with a `reason_code`; (2) pivot events to a per-visit partial row; (3) `MERGE` on `(facility_id, visit_id)`, setting each stage timestamp with `COALESCE(target, source)`. Out-of-order events fill in without overwriting, and replays or duplicates outside the watermark are harmless (idempotent) |
| `silver_notes_ai` | Stream from `bronze.ed_events` filtered to `TRIAGE`; `foreachBatch` runs the AI chain (§9) and MERGEs into `silver.triage_notes` on `(facility_id, visit_id)`. It **never writes to `silver.ed_visits`**: each silver table has exactly one writer, which avoids concurrent-MERGE conflicts. Gold joins the two |
| `ai_retry_sweep` | Separate task with a `rate` source and 15-minute trigger: re-runs the AI chain for `silver.triage_notes` rows with `ai_status = 'error'` and `ai_attempts < 3`. It writes only to `silver.triage_notes`; because it updates only error rows and `silver_notes_ai` only inserts new visits, the two don't conflict in practice under row-level concurrency |
| `gold_metrics` | `rate` source, 1-minute trigger. `foreachBatch` reads the current state of `silver.ed_visits` left-joined to `silver.triage_notes` (in-ED visits plus departures within 24 h; 14-day hourly baselines cached and refreshed hourly), runs `compute_snapshots`, and MERGEs `facility_current`, `facility_snapshot_5min` (trailing 30 min) and `ed_board_current` (with `WHEN NOT MATCHED BY SOURCE THEN DELETE`) |
| `gold_narratives` | `rate` source, 15-minute trigger. `ai_query` per facility over `facility_current` plus `top_drivers`, then one network-wide call. MERGEs both narrative tables |

Every table has exactly one streaming writer. `ed_setup` writes before streaming starts.

---

## 9. AI functions

### 9.1 Triage-note enrichment (`silver_notes_ai`)

Run in this order. Everything after step 1 runs on masked text only.

1. `ai_mask(triage_note, array('person','phone number','date of birth','address','medical record number','email'))` produces `note_masked`.
2. **Deterministic post-check**: regex for phone, `MRN-\d+`, dates and emails over `note_masked`. Any match is replaced with `[REDACTED]` and sets `residual_phi_flag = true`. This gives a measured `ai_mask` miss rate.
3. `ai_classify(note_masked, array('cardiac','respiratory','neurological','trauma','gastrointestinal','infectious','psychiatric','obstetric','musculoskeletal','other'))` produces `complaint_category`.
4. `ai_extract(note_masked, array('heart_rate','blood_pressure','respiratory_rate','spo2','temperature','pain_score','symptom_onset'))` produces typed columns via `try_cast` (`bp_systolic`/`bp_diastolic` are parsed from `blood_pressure`).
5. `ai_query(llm_endpoint, <ESI prompt>, responseFormat => {suggested_esi INT, rationale STRING}, failOnError => false)`. `undertriage_flag = (esi_level − suggested_esi) ≥ 2`. The UI labels this as demo decision-support, not clinical advice.

`silver.triage_notes` stores **only** `note_masked` and the derived columns; the raw note exists only in bronze.

**Backfill cost control:**
- Notes with `event_ts < now − ai_backfill_days` (default 2) get only the deterministic regex masking, with `ai_enriched = false`.
- Live notes always get the full chain.

### 9.2 Narratives (`gold_narratives`)

- **`facility_narrative`:**
  - The prompt includes the facility's current metrics and `top_drivers` JSON, with the instruction "use only these numbers; do not invent values".
  - `responseFormat` is `{headline STRING, explanation STRING (≤60 words), recommended_actions ARRAY<STRING> (≤3)}`.
  - The table stores `input_json`, `model`, `generated_at`.
- **`network_narrative`:** one row. The input is all facilities' bands, gEDWIN, D2P median and top driver. The output covers which facilities are crowded and why, which are doing well, and network-level actions.

`llm_endpoint` is a bundle variable, defaulting to the latest Claude Sonnet Foundation Model API endpoint available in the workspace (confirmed at implementation).

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
| `gold.ed_board_current` | MERGE + delete | `(facility_id, visit_id)` | `(facility_id, visit_id)` | yes | Continuous |
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
  - PHI: `patient_name`, `mrn`, `dob`, `sex`, `phone`, `address`;
  - `mrn_hash`, `chief_complaint_text`, `last_event_ts`, `updated_at`.
- **`silver.triage_notes`:** `facility_id`, `visit_id`, `triage_ts`, `esi_level`, `note_masked`, `residual_phi_flag`, `complaint_category`, `heart_rate`, `bp_systolic`, `bp_diastolic`, `respiratory_rate`, `spo2`, `temperature_c`, `pain_score`, `symptom_onset`, `suggested_esi`, `esi_rationale`, `undertriage_flag`, `ai_enriched`, `ai_status`, `ai_error`, `ai_attempts`, `processed_at`.
- **`gold.visit_facts`** (view): non-PHI columns of `silver.ed_visits` left-joined to the non-text AI columns of `silver.triage_notes`, plus derived `d2p_min`, `los_h`, `boarding_h`.
- **`gold.facility_current` / `facility_snapshot_5min`:**
  - keys and identity: `facility_id`, `snapshot_ts`, `computed_at`;
  - every metric column in §7;
  - `top_drivers`, `dq_flags`.

  `facility_current` also has `facility_name`, `facility_type`, `region` (denormalised for the app).
- **`gold.ed_board_current`:** `facility_id`, `visit_id`, `status` (`waiting`, `in_bed`, `boarding`), `esi_level`, `suggested_esi`, `undertriage_flag`, `complaint_category`, `minutes_in_ed`, `minutes_waiting`, `arrival_mode`, `on_vent`. No PHI.

---

## 11. Governance

### 11.1 Access by layer

| Layer | PHI | Grants |
|---|---|---|
| `bronze` | Raw (structured + free text) | Job service principal, `ed_admins` |
| `sim` | Raw (generator internals) | Job service principal, `ed_admins` |
| `silver.ed_visits` | Structured PHI columns | Analysts; PHI masked by ABAC unless the user is in `ed_phi_readers` |
| `silver.triage_notes` | Masked text only | Analysts |
| `gold`, `ref` | None | Analysts, app service principal, Genie users |

### 11.2 Structured PHI masking

An **ABAC** policy on the catalog matches columns with governed tag `phi_type` and applies a mask function per value, unless the user is a member of `ed_phi_readers`:

| `phi_type` | Mask |
|---|---|
| `name` | `'REDACTED'` |
| `mrn` | `'MRN-****' \|\| right(mrn, 2)` |
| `dob` | year only (`date_trunc('year', dob)`) |
| `contact` | `'***-***-' \|\| right(phone, 4)` |
| `address` | city only |

**Fallback:** if ABAC isn't available on the workspace, apply the same functions with per-column `ALTER TABLE … ALTER COLUMN … SET MASK`.

`mrn_hash = sha2(concat(mrn, <salt>), 256)`, with the salt in secret scope `ed_ops` (key `mrn_salt`). It lets analysts join and count distinct patients without seeing the MRN.

### 11.3 Governed tags (created by `ed_setup`)

| Tag | Level | Allowed values |
|---|---|---|
| `layer` | table | `bronze`, `silver`, `gold`, `ref`, `sim` |
| `domain` | table | `emergency_department` |
| `data_classification` | table, column | `phi`, `confidential`, `internal` |
| `contains_phi` | table | `true`, `false` |
| `data_origin` | table | `synthetic` |
| `phi_type` | column | `name`, `mrn`, `dob`, `contact`, `address`, `free_text` |

### 11.4 Comments

Every table and column comment comes from `metadata/tables.yml`. Column comments include units and window definitions, for example: *"Door-to-provider median in minutes, over patients greeted in the 60 minutes ending snapshot_ts."*

### 11.5 Automated checks (`governance/checks.py`, run in `ed_setup` and `ed_validate`)

1. Every table and column in the catalog has a non-empty comment.
2. Every table has `layer`, `domain`, `data_classification`, `contains_phi`, `data_origin`.
3. No column tagged `phi_type` exists in `gold`, `ref`, any synced table, or any table in the Genie space.
4. Every `phi_type` column in `silver` is covered by a mask.

---

## 12. Lakebase

- One Lakebase instance (capacity is bundle variable `lakebase_capacity`, smallest by default) and database `ed_ops`. Provisioned or Autoscaling: whichever supports synced tables on the target workspace (confirmed at implementation).
- Synced tables per §10. Sync modes are bundle variables (`sync_mode_live` default `CONTINUOUS`, `sync_mode_ref` default `SNAPSHOT`).
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

- Genie calls use **on-behalf-of-user** authorization (the user's UC permissions apply). If OBO isn't enabled, it falls back to the app service principal, and the UI notes this.

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

Defined in `genie/space.json`, created and updated by the SDK in `ed_setup`, running on serverless SQL warehouse `warehouse_id`.

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
- DDL generator: `tables.yml` produces the expected `CREATE TABLE … CLUSTER BY … COMMENT … TBLPROPERTIES`.

### 16.2 App tests

pytest + FastAPI `TestClient` with the fake `Repository`; vitest for band colouring, number formatting and low-sample logic.

### 16.3 `ed_validate` job (on the workspace)

Runs at small scale (5 facilities, 1-day backfill, about 10 live ticks) in catalog `<catalog>_validate`. It asserts:

1. Rows flow bronze → silver → gold.
2. Quarantine count is greater than 0 and matches the configured chaos.
3. Governance checks in §11.5 pass.
4. A seeded **golden scenario** produces exactly the hand-calculated EDWIN, gEDWIN and NEDOCS. The scenario is a test-only facility `FAC000` in the validate catalog's `ref.facility` and `ref.physician_roster`, plus a fixed visit set loaded directly into `silver.ed_visits` and `silver.triage_notes`. It is never created in the main catalog.
5. Synced tables exist and have the same row counts as their sources.
6. Optionally, the Genie benchmark pass rate.

---

## 17. Deployment and configuration

### Databricks Asset Bundle (`databricks.yml`)

- **Targets:** `dev` (FE VM workspace) and `customer` (template).
- **Artifacts:** Python wheel `ed_ops` and the frontend build.
- **Resources:**
  - Jobs:
    - `ed_setup` (serverless; idempotent): DDL, tags, ref data, backfill, historical snapshots, masks, Lakebase and synced tables, Genie space, governance checks.
    - `ed_streaming` (continuous; classic job cluster, **standard access mode**, latest LTS DBR): the classic cluster is needed because serverless streaming supports only `availableNow` triggers (to be re-verified at implementation). Standard access mode enforces ABAC masks on reads.
    - `ed_validate` (serverless).
  - The app.
  - The Lakebase instance and synced tables, using bundle resources where supported and SDK calls in `ed_setup` otherwise.

### Variables

| Variable | Default |
|---|---|
| `catalog` | `ed_ops` |
| `facility_count` | 50 |
| `backfill_days` | 14 |
| `ai_backfill_days` | 2 |
| `tick_seconds` | 60 |
| `dup_rate` / `late_rate` / `malformed_rate` | 0.01 / 0.02 / 0.005 |
| `live_surge_offset_min` / `live_surge_duration_min` | 30 / 90 |
| `llm_endpoint` | latest Claude Sonnet FMAPI endpoint (confirmed at implementation) |
| `warehouse_id` | — (required) |
| `lakebase_capacity` | smallest |
| `sync_mode_live` / `sync_mode_ref` | `CONTINUOUS` / `SNAPSHOT` |
| `timezone` | `America/Chicago` |
| `seed` | 42 |
| `checkpoint_version` | 1 |
| `streaming_node_type` | smallest general-purpose node |

### Teardown

`databricks bundle destroy` plus `scripts/teardown.sh`: drop the catalog(s), delete the Lakebase instance and the Genie space.

---

## 18. Repository layout

```
databricks.yml
resources/            jobs.yml, app.yml, lakebase.yml
metadata/             tables.yml, tags.yml
src/ed_ops/
  config.py
  ddl/                generate.py, apply.py
  generator/          facilities.py, roster.py, journey.py, notes_templates.py, chaos.py, tick.py
  metrics/            formulas.py, snapshots.py
  pipeline/           bronze.py, silver_visits.py, silver_notes_ai.py, gold_metrics.py, gold_narratives.py
  governance/         tags.py, masks.py, checks.py
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

**Verify on the target workspace before building the dependent part**

1. Lakebase flavour (Provisioned or Autoscaling) and synced-table support, including CDF-based Continuous sync from managed Delta tables.
2. ABAC availability (otherwise use the per-column mask fallback).
3. Serverless streaming trigger support (could remove the need for a classic cluster).
4. Available Claude FMAPI endpoint name for `llm_endpoint`; `ai_mask`, `ai_classify` and `ai_extract` availability.
5. On-behalf-of-user authorization for Databricks Apps (Genie scope).
6. Bundle resource support for Lakebase instances, synced tables and Genie spaces (otherwise use SDK calls in `ed_setup`).

---

## 20. References

- gEDWIN / EDWIN: *gEDWIN: a simple and practical index for real-time monitoring of emergency department crowding.* BMC Emergency Medicine, 2025. https://pmc.ncbi.nlm.nih.gov/articles/PMC12648796/
- NEDOCS: Weiss SJ et al. *Estimating the degree of emergency department overcrowding in academic medical centers: results of the National ED Overcrowding Study (NEDOCS).* Acad Emerg Med, 2004.
- Databricks AI functions: https://docs.databricks.com/aws/en/large-language-models/ai-functions
- Lakebase synced tables: https://docs.databricks.com/aws/en/oltp/instances/sync-data/sync-table
- ApexCharts: https://github.com/apexcharts/apexcharts.js
- dbldatagen: https://github.com/databrickslabs/dbldatagen
