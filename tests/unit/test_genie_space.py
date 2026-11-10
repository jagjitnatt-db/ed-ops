import json
import re
from types import SimpleNamespace

from ed_ops.config import Config
from ed_ops.ddl.metadata import load_catalog_def
from ed_ops.serving.genie_space import build_serialized_space, grant_can_run, load_space_doc, upsert_genie_space

CFG = Config(warehouse_id="wh1", genie_space_title="ED Ops Crowding Assistant (dev)")


def test_serialized_space_shape():
    s = build_serialized_space(CFG, load_space_doc())
    assert s["version"] == 2
    ids = [t["identifier"] for t in s["data_sources"]["tables"]]
    assert ids == sorted(ids) and "hls_jagjit_lb_playground_catalog.ed_gold.visit_facts" in ids
    cat = load_catalog_def()
    known = {CFG.table(t.schema, t.name) for t in cat.tables} | {CFG.table(v.schema, v.name) for v in cat.views}
    assert set(ids) <= known
    assert not any("_bronze." in i or "_sim." in i or "ed_visits" in i for i in ids)  # no raw-PHI tables
    text = s["instructions"]["text_instructions"]
    assert len(text) == 1 and "America/Chicago" in "".join(text[0]["content"])
    examples = s["instructions"]["example_question_sqls"]
    assert len(examples) == 10
    for e in examples + text:
        assert re.fullmatch(r"[0-9a-f]{32}", e["id"])
    for e in examples:
        sql = "".join(e["sql"])
        assert "{" not in sql and "hls_jagjit_lb_playground_catalog.ed_" in sql
    assert len({e["id"] for e in examples}) == 10
    assert build_serialized_space(CFG, load_space_doc()) == s  # deterministic ids


class FakeApi:
    def __init__(self, spaces):
        self.spaces = spaces
        self.calls = []

    def do(self, method, path, query=None, body=None):
        self.calls.append((method, path, body))
        if method == "GET":
            return {"spaces": self.spaces}
        if method == "POST":
            self.spaces.append({"space_id": "new123", "title": body["title"]})
            return {"space_id": "new123"}
        return {}


def test_upsert_creates_then_updates_by_title():
    api = FakeApi([{"space_id": "other", "title": "Something else"}])
    w = SimpleNamespace(api_client=api)
    serialized = build_serialized_space(CFG, load_space_doc())
    assert upsert_genie_space(w, CFG, serialized) == "new123"
    post = [c for c in api.calls if c[0] == "POST"][0]
    assert post[1] == "/api/2.0/genie/spaces" and post[2]["warehouse_id"] == "wh1"
    assert json.loads(post[2]["serialized_space"]) == serialized
    assert upsert_genie_space(w, CFG, serialized) == "new123"  # second run updates, no new space
    assert [c[0] for c in api.calls if c[0] in ("POST", "PATCH")] == ["POST", "PATCH"]
    assert [c for c in api.calls if c[0] == "PATCH"][0][1] == "/api/2.0/genie/spaces/new123"


def test_grant_can_run():
    api = FakeApi([])
    grant_can_run(SimpleNamespace(api_client=api), "sp1", "app-sp-uuid")
    method, path, body = api.calls[0]
    assert (method, path) == ("PATCH", "/api/2.0/permissions/genie/sp1")
    assert body == {"access_control_list": [{"service_principal_name": "app-sp-uuid", "permission_level": "CAN_RUN"}]}
