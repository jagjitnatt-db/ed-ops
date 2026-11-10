# ED Crowding Command Center (Databricks reference solution)

Real-time emergency-department crowding for a 50-facility health system, built entirely on Databricks:
a synthetic patient-journey simulator, a Structured Streaming medallion pipeline, EDWIN / gEDWIN / NEDOCS crowding
scores, AI Functions on triage notes, AI-written narratives, Lakebase serving, a React + ApexCharts Databricks App and a
Genie space. All data is synthetic and de-identified.

## Deploy (about 1 hour end to end)

    # prerequisites: Databricks CLI authenticated to the workspace, Node 20+, uv, an existing UC catalog and SQL warehouse
    uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e ".[dev]"
    mkdir -p .databricks/bundle/dev && cp variable-overrides.example.json .databricks/bundle/dev/variable-overrides.json
    #   ...then set your existing catalog and SQL warehouse id in that (git-ignored) file
    scripts/pytest.sh                                                   # local tests (Spark 4.1 + Java 17)
    scripts/deploy.sh -t dev -p <profile>                               # builds the frontend, deploys jobs, app, Lakebase
    databricks bundle run ed_setup -t dev -p <profile>                  # DDL, reference data, 14-day backfill, AI, serving, Genie
    databricks bundle run ed_ops -t dev -p <profile>                    # start the app
    scripts/deploy.sh -t dev -p <profile> --var streaming_pause_status=UNPAUSED   # go live
    databricks bundle run ed_validate -t dev -p <profile>               # end-to-end proof in ed_validate_* schemas

For a production deployment use `-t customer` with `.databricks/bundle/customer/variable-overrides.json` holding `catalog`,
`warehouse_id` and `run_as_service_principal` (or pass them with `--var`). Nothing environment-specific is committed.

See `docs/architecture.md` for the design and `docs/runbook.md` for operations, costs and teardown.
The design spec is `docs/superpowers/specs/2026-11-09-ed-crowding-design.md`.
