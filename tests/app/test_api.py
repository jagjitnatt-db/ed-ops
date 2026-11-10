from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from backend.main import create_app
from backend.settings import Settings

NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)


def fac(fid, band, gedwin, census, d2p=None, boarders=0):
    return {"facility_id": fid, "facility_name": f"{fid} Hospital", "facility_type": "community", "region": "Central",
            "snapshot_ts": NOW, "census": census, "gedwin": gedwin, "gedwin_band": band, "gedwin_delta_1h": 0.1,
            "edwin": 1.0, "edwin_band": "Active", "nedocs": 40.0, "nedocs_level": 2, "occupancy_ratio": 0.5,
            "d2p_mean_min": d2p, "d2p_median_min": d2p, "d2p_p90_min": d2p, "d2p_sample_n": 6, "waiting_now_n": 2,
            "longest_current_wait_min": 30.0, "boarders_n": boarders, "longest_boarder_h": 1.0, "lwbs_rate_24h": 0.02,
            "los_median_h_24h": 3.5, "attendings_on_duty": 2, "patients_per_attending": 4.0, "arrival_surge_ratio": 1.1,
            "undertriage_flags_n": 0, "top_drivers": [], "computed_at": NOW}


class FakeRepository:
    def __init__(self, age=30.0, fail=False):
        self.age, self.fail, self.calls = age, fail, 0
        self.rows = [fac("FAC001", "Crowded", 2.9, 30, 50.0, 4), fac("FAC002", "Active", None, 0, None, 0),
                     fac("FAC003", "Busy", 2.2, 12, 20.0, 1)]

    def _call(self):
        self.calls += 1
        if self.fail:
            raise ConnectionError("lakebase down")

    def facilities(self):
        self._call()
        return [dict(r) for r in self.rows]

    def facility(self, facility_id):
        self._call()
        return next((dict(r) for r in self.rows if r["facility_id"] == facility_id), None)

    def narrative(self, facility_id):
        self._call()
        return {"facility_id": facility_id, "headline": "h", "explanation": "e", "recommended_actions": ["a"]}

    def network(self):
        self._call()
        return {"network_id": "NETWORK", "headline": "17 crowded"}

    def history(self, facility_id, hours):
        self._call()
        return [] if facility_id == "FAC002" else [{"snapshot_ts": NOW - timedelta(minutes=5), "gedwin": 2.8}]

    def board(self, facility_id):
        self._call()
        return [{"visit_id": "v1", "status": "waiting", "esi_level": 3}]

    def sparklines(self, hours):
        self._call()
        return {"FAC001": [[NOW, 2.9]]}

    def heatmap(self, hours):
        self._call()
        return [{"facility_id": "FAC001", "facility_name": "FAC001 Hospital", "hour": NOW, "gedwin": 2.9},
                {"facility_id": "FAC002", "facility_name": "FAC002 Hospital", "hour": NOW, "gedwin": None}]

    def network_history(self, hours, bucket_minutes):
        self._call()
        self.last_bucket = (hours, bucket_minutes)
        return [{"bucket": NOW, "census": 512, "d2p_median_min": 31.5, "waiting_now_n": 40, "longest_current_wait_min": 95.0,
                 "boarders_n": 61, "lwbs_rate_24h": 0.03, "crowded_n": 9}]

    def data_age_seconds(self):
        self._call()
        return self.age


class FakeGenie:
    def __init__(self):
        self.tokens = []

    def ask(self, token, question, conversation_id=None):
        self.tokens.append(token)
        return {"conversation_id": conversation_id or "c1", "message_id": "m1"}

    def poll(self, token, conversation_id, message_id):
        self.tokens.append(token)
        return {"done": True, "status": "COMPLETED", "text": "answer", "sql": "SELECT 1", "columns": ["x"], "rows": [["1"]], "error": None}


def client(repo=None, genie=None, cache_seconds=0.0):
    settings = Settings(schema="ed_serving", lakebase_endpoint="e", genie_space_title="t", local_tz="America/Chicago",
                        cache_seconds=cache_seconds, stale_after_seconds=300)
    return TestClient(create_app(repository=repo or FakeRepository(), genie=genie or FakeGenie(), settings=settings, static_dir=None))


def test_summary_counts_and_kpis():
    body = client().get("/api/network/summary").json()
    assert body["counts"] == {"Crowded": 1, "Busy": 1, "Active": 1}
    assert body["census"] == 42 and body["boarders"] == 5 and body["facilities"] == 3
    assert body["network_d2p_median_min"] == 35.0  # median of the non-null facility medians (50, 20)
    assert body["narrative"]["headline"] == "17 crowded"


def test_facilities_include_sparklines_and_nulls():
    rows = {r["facility_id"]: r for r in client().get("/api/facilities").json()["facilities"]}
    assert len(rows["FAC001"]["sparkline"]) == 1 and rows["FAC001"]["sparkline"][0][1] == 2.9
    assert rows["FAC002"]["sparkline"] == [] and rows["FAC002"]["gedwin"] is None


def test_facility_detail_404_and_history_validation():
    c = client()
    assert c.get("/api/facilities/FAC999").status_code == 404
    assert c.get("/api/facilities/FAC001").json()["narrative"]["headline"] == "h"
    assert c.get("/api/facilities/FAC002/history?hours=24").json() == {"points": []}
    assert c.get("/api/facilities/FAC001/history?hours=0").status_code == 422
    assert c.get("/api/facilities/FAC001/history?hours=169").status_code == 422
    assert c.get("/api/facilities/FAC001/board").json()["patients"][0]["visit_id"] == "v1"


def test_heatmap_bands_default_to_active_for_null():
    cells = client().get("/api/heatmap").json()["cells"]
    assert [c["band"] for c in cells] == ["Crowded", "Active"]


@pytest.mark.parametrize("age,status,code", [(30.0, "ok", 200), (900.0, "delayed", 200), (None, "delayed", 200)])
def test_health_reports_freshness(age, status, code):
    r = client(FakeRepository(age=age)).get("/api/health")
    assert (r.status_code, r.json()["status"]) == (code, status)


def test_health_unavailable_when_lakebase_down():
    r = client(FakeRepository(fail=True)).get("/api/health")
    assert r.status_code == 503 and r.json()["status"] == "unavailable"


def test_data_endpoints_return_503_json_when_lakebase_down():
    r = client(FakeRepository(fail=True)).get("/api/facilities")
    assert r.status_code == 503 and "unavailable" in r.json()["detail"]


def test_cache_avoids_repeat_queries():
    repo = FakeRepository()
    c = client(repo, cache_seconds=60)
    c.get("/api/facilities")
    first = repo.calls
    c.get("/api/facilities")
    assert repo.calls == first


def test_genie_endpoints_forward_user_token():
    genie = FakeGenie()
    c = client(genie=genie)
    r = c.post("/api/genie/messages", json={"question": "Which EDs are crowded?"}, headers={"x-forwarded-access-token": "user-tok"})
    assert r.json() == {"conversation_id": "c1", "message_id": "m1"}
    assert c.get("/api/genie/conversations/c1/messages/m1").json()["rows"] == [["1"]]
    assert genie.tokens == ["user-tok", None]


def test_genie_errors_are_json():
    class Broken(FakeGenie):
        def ask(self, token, question, conversation_id=None):
            raise LookupError("Genie space 't' not found or not shared with you")

    r = client(genie=Broken()).post("/api/genie/messages", json={"question": "q"})
    assert r.status_code == 503 and "not found" in r.json()["detail"]
    assert client().post("/api/genie/messages", json={"question": " "}).status_code == 422


def test_definitions():
    d = client().get("/api/definitions").json()
    assert d["bands"]["gedwin"] == {"busy": 2.06, "crowded": 2.34}
    assert any("natural log" in a for a in d["assumptions"])


@pytest.mark.parametrize("hours,bucket", [(6, 15), (24, 60), (168, 360)])
def test_network_history_buckets_by_range(hours, bucket):
    repo = FakeRepository()
    body = client(repo).get(f"/api/network/history?hours={hours}").json()
    assert body["bucket_minutes"] == bucket and repo.last_bucket == (hours, bucket)
    assert body["buckets"][0]["census"] == 512 and body["buckets"][0]["crowded_n"] == 9


def test_network_history_rejects_other_ranges():
    assert client().get("/api/network/history?hours=12").status_code == 422

