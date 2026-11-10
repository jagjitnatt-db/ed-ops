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
