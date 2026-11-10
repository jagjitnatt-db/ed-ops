# Runbook

Placeholders: `<profile>` is the Databricks CLI profile for the workspace, `<catalog>` the existing Unity Catalog
catalog (never created or dropped). Schemas: ed_bronze, ed_silver, ed_gold, ed_ref, ed_sim, ed_serving. Tag keys are
prefixed ed_.

## Configure an environment (once per workspace and target)
Nothing environment-specific is committed. Copy the example and fill in your values; the file is git-ignored and is
read by both the Databricks CLI and the scripts in `scripts/`:

    mkdir -p .databricks/bundle/dev
    cp variable-overrides.example.json .databricks/bundle/dev/variable-overrides.json   # catalog, warehouse_id
    export DATABRICKS_CONFIG_PROFILE=<profile>                                         # used by scripts/*.py and *.sh

For the production template use `.databricks/bundle/customer/variable-overrides.json` and also set
`run_as_service_principal`. Any value can instead be passed with `--var name=value` or `BUNDLE_VAR_<name>`.

## Volumes and timings (defaults)
~3,440 visits/day across 50 EDs: ~48k visits and ~285k events for the 14-day backfill; live about 2.4 arrivals/min and
13 events/min. Gold history grows ~14,400 rows/day. A fresh `ed_setup` takes about 45-60 min (create_objects ~14 min of
sequential DDL/tag statements). The MRN hashing salt is created automatically in the secret scope on first run.

## What runs where
- ed_streaming (classic, on-demand Photon r7gd.2xlarge): generator, bronze_ingest, silver_visits, gold_metrics,
  notes_ai (every 60 s), ai_retry (every ai_retry_minutes), narratives (every narrative_minutes).
- ed_setup (serverless, safe to re-run): once the generator is live, generate/ingest/gold skip; the notes backfill,
  refresh_drivers and narratives refuse while ed_streaming is active. serving_setup and genie_space can run any time.
- ed_validate and ed_genie_benchmarks (serverless): on demand.

## Start / stop live streaming (the classic cluster bills while running)
    scripts/deploy.sh -t dev -p <profile> --var streaming_pause_status=UNPAUSED
    scripts/deploy.sh -t dev -p <profile> --var streaming_pause_status=PAUSED
    JOB=$(databricks bundle summary -t dev -p <profile> -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['id'])")
    databricks jobs cancel-all-runs --job-id $JOB -p <profile>      # pausing alone leaves the active run up

## Re-run setup (safe; pause streaming first - AI/driver/narrative tasks refuse while it runs)
    databricks bundle run ed_setup -t dev -p <profile>
    databricks bundle run ed_setup --only genie_space -t dev -p <profile>    # refresh Genie instructions only

## App
    databricks bundle run ed_ops -t dev -p <profile>                 # (re)deploy and start the app
    databricks apps logs <app name> -p <profile> --tail-lines 100

## Health checks
    scripts/run_sql.py "SELECT max(computed_at), current_timestamp() FROM <catalog>.ed_gold.facility_current"
    scripts/run_sql.py "SELECT ai_status, count(*) FROM <catalog>.ed_silver.triage_notes GROUP BY 1"
    databricks bundle run ed_validate -t dev -p <profile>            # end-to-end, cleans up after itself
    databricks bundle run ed_genie_benchmarks -t dev -p <profile>    # Genie accuracy (>= 60% to pass)

## Costs (all knobs are bundle variables)
- ed_streaming: one on-demand Photon r7gd.2xlarge driver + 1 worker, always on while UNPAUSED.
- AI: ~2.4 triage notes/min x 5 AI calls; narratives 51 ai_query calls every narrative_minutes (15).
- Lakebase: 0.5-2 CU autoscaling; Continuous sync keeps it awake while streaming.
- Volume: tune facility_count, backfill_days, ai_backfill_days, tick_seconds.

## Reset streams (fresh checkpoints, keep data; note bronze re-ingests landing files)
    scripts/deploy.sh -t dev -p <profile> --var checkpoint_version=2 --var streaming_pause_status=UNPAUSED

## Tear down
    scripts/deploy.sh -t dev -p <profile> --var streaming_pause_status=PAUSED   # then cancel-all-runs as above
    scripts/teardown.sh ed_ --yes                      # synced tables, Genie space, then the ed_* schemas in var.catalog
    databricks bundle destroy -t dev -p <profile>   # jobs, app, Lakebase project

## AI behaviour
- ai_mask runs first, then a regex post-check (phone, MRN, full dates, email); everything else sees masked text only.
- Notes older than ai_backfill_days are stored without text (ai_status = 'skipped').
- ai_query uses failOnError => false and a JSON-only prompt (no responseFormat: Claude 5.x rejects it).
- Failed notes (error / parse_error) are retried up to 3 times; a successful row is never overwritten.
- Narratives see only computed metrics (input_json is stored); a reply that fails to parse keeps the previous one.

## Governance
- Regular UC tags (no tag policies), keys prefixed ed_ (bundle var tag_prefix), because the metastore already has
  governed domain/data_classification policies.
- governance_checks fails ed_setup on a missing comment or tag, a disallowed tag value, a raw PHI tag outside
  bronze/sim, a PHI-pattern match in sampled silver/gold/ref data, or an ai_mask name miss rate above 2%.
