"""Lakebase connection pool. Host, port, database, user and SSL mode come from the PG* variables the app's postgres
resource injects; the password is an OAuth token refreshed well before its one-hour expiry, so every new pool
connection authenticates with a valid token."""

from __future__ import annotations

import threading
import time
from typing import Callable

from backend.settings import Settings


class TokenProvider:
    def __init__(self, endpoint: str, client_factory: Callable[[], object], refresh_seconds: int = 2400,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.endpoint, self.client_factory, self.refresh_seconds, self.clock = endpoint, client_factory, refresh_seconds, clock
        self._token: str | None = None
        self._issued = 0.0
        self._lock = threading.Lock()

    def token(self) -> str:
        with self._lock:
            if self._token is None or self.clock() - self._issued >= self.refresh_seconds:
                cred = self.client_factory().postgres.generate_database_credential(endpoint=self.endpoint)
                self._token, self._issued = cred.token, self.clock()
            return self._token


def make_pool(settings: Settings):
    import psycopg
    from databricks.sdk import WorkspaceClient
    from psycopg_pool import ConnectionPool

    provider = TokenProvider(settings.lakebase_endpoint, WorkspaceClient)

    class OAuthConnection(psycopg.Connection):
        @classmethod
        def connect(cls, conninfo: str = "", **kwargs):
            kwargs["password"] = provider.token()
            return super().connect(conninfo, **kwargs)

    return ConnectionPool(conninfo="", connection_class=OAuthConnection, min_size=1, max_size=8,
                          max_lifetime=2400, timeout=10, kwargs={"connect_timeout": 10}, open=True)
