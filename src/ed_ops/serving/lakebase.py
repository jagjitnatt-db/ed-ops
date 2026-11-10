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
