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
