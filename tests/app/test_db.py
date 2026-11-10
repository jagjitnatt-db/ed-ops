from types import SimpleNamespace

from backend.db import TokenProvider


def test_token_provider_caches_and_refreshes():
    issued = []

    def factory():
        def generate(endpoint):
            issued.append(endpoint)
            return SimpleNamespace(token=f"t{len(issued)}")

        return SimpleNamespace(postgres=SimpleNamespace(generate_database_credential=generate))

    clock = [1000.0]
    tp = TokenProvider("projects/p/branches/production/endpoints/primary", factory, refresh_seconds=2400, clock=lambda: clock[0])
    assert tp.token() == "t1" and tp.token() == "t1"
    clock[0] += 2401  # past the refresh window (tokens live one hour): a new connection gets a fresh token
    assert tp.token() == "t2"
    assert issued == ["projects/p/branches/production/endpoints/primary"] * 2
