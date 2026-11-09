# ED Ops: core data platform runbook (Plan 1)

Workspace: https://fevm-hls-jagjit-lb-playground.cloud.databricks.com (CLI profile `fevm-playground`).
Catalog: `hls_jagjit_lb_playground_catalog` (existing; never created or dropped). Schemas: `ed_bronze`, `ed_silver`, `ed_gold`, `ed_ref`, `ed_sim`, `ed_serving`.
UC tag keys are prefixed with `tag_prefix` (default `ed_`, e.g. `ed_layer`, `ed_data_classification`) because the metastore already has governed `domain`/`data_classification` policies.

## Volumes (defaults)
~3,440 visits/day across 50 EDs → ~48k visits and ~285k events for the 14-day backfill; live ≈ 2.4 arrivals/min, ~13 events/min. Gold history grows ~14,400 rows/day. `ed_setup` takes ~45 min (create_objects ~14 min of sequential DDL/tag statements).

## Deploy and set up
    databricks bundle deploy -t dev -p fevm-playground
    databricks bundle run ed_setup -t dev -p fevm-playground        # ~30-60 min: DDL, reference data, 14-day backfill

## Start / stop live streaming (classic DBR 18 LTS cluster, costs money while running)
    databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=UNPAUSED
    databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=PAUSED
    # Pausing does not stop the active run (or its cluster); cancel it explicitly:
    JOB=$(databricks bundle summary -t dev -p fevm-playground -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['resources']['jobs']['ed_streaming']['id'])")
    databricks jobs cancel-all-runs --job-id $JOB -p fevm-playground

## Check freshness
    scripts/run_sql.py "SELECT max(computed_at), current_timestamp() FROM hls_jagjit_lb_playground_catalog.ed_gold.facility_current"

## Reset streams (fresh checkpoints, keep data)
    databricks bundle deploy -t dev -p fevm-playground --var checkpoint_version=2 --var streaming_pause_status=UNPAUSED

## Tear down (drops the ed_* schemas only)
    databricks bundle deploy -t dev -p fevm-playground --var streaming_pause_status=PAUSED   # then cancel-all-runs as above
    scripts/teardown.sh ed_ --yes
    databricks bundle destroy -t dev -p fevm-playground
