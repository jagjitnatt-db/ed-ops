# ED Ops: AI enrichment, narratives, governance and validation runbook (Plan 2)

Builds on `docs/runbook-plan1.md`. LLM endpoint: `databricks-claude-sonnet-5-5` (bundle var `llm_endpoint`).

## What runs where
- ed_streaming (classic, on-demand Photon r7gd.2xlarge): generator, bronze_ingest, silver_visits, gold_metrics,
  notes_ai (every 60 s), ai_retry (every ai_retry_minutes), narratives (every narrative_minutes).
- ed_setup (serverless) is safe to re-run. Once the generator is live, generate/ingest/gold skip; the notes backfill,
  refresh_drivers and narratives refuse while ed_streaming is active, so pause it first:

      databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=PAUSED
      JOB=$(databricks bundle summary -t dev -p fevm-playground -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['id'])")
      databricks jobs cancel-all-runs --job-id $JOB -p fevm-playground      # wait 5 min, then:
      databricks bundle run ed_setup -t dev -p fevm-playground
      databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=UNPAUSED

- ed_validate (serverless): end-to-end proof in ed_validate_* schemas, torn down afterwards:

      databricks bundle run ed_validate -t dev -p fevm-playground

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
    SELECT ai_status, count(*) FROM hls_jagjit_lb_playground_catalog.ed_silver.triage_notes GROUP BY 1
    SELECT * FROM hls_jagjit_lb_playground_catalog.ed_gold.network_narrative
