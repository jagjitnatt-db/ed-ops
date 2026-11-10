"""FastAPI backend: reads only Lakebase, proxies Genie, serves the built React app (spec 13)."""

from __future__ import annotations

import logging
import statistics
from pathlib import Path
from typing import Callable

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator

from backend.cache import TtlCache
from backend.definitions import DEFINITIONS, band
from backend.settings import Settings, load_settings

# Cockpit range toggle -> bar width: 6 h in 15-min bars, 24 h hourly, 7 days in 6-hour bars (24-28 bars each).
RANGE_BUCKET_MINUTES = {6: 15, 24: 60, 168: 360}
log = logging.getLogger("ed_ops.app")
DEFAULT_STATIC = Path(__file__).resolve().parents[1] / "frontend" / "dist"


class AskBody(BaseModel):
    question: str
    conversation_id: str | None = None

    @field_validator("question")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("question must not be blank")
        return v.strip()


def _lazy(factory: Callable[[], object]) -> Callable[[], object]:
    box: list = []

    def get():
        if not box:
            box.append(factory())
        return box[0]

    return get


def create_app(repository=None, genie=None, settings: Settings | None = None, static_dir: Path | None = DEFAULT_STATIC) -> FastAPI:
    settings = settings or load_settings()
    cache = TtlCache(settings.cache_seconds)

    def build_repo():
        from backend.db import make_pool
        from backend.repository import LakebaseRepository

        return LakebaseRepository(make_pool(settings), settings.schema)

    def build_genie():
        from backend.genie import GenieService, workspace_client

        return GenieService(settings.genie_space_title, workspace_client)

    repo = (lambda: repository) if repository is not None else _lazy(build_repo)
    genie_service = (lambda: genie) if genie is not None else _lazy(build_genie)
    app = FastAPI(title="ED Ops Crowding Command Center")

    def data(key: str, fn: Callable[[], object]):
        try:
            return cache.get(key, fn)
        except Exception as exc:  # Lakebase unreachable, suspended or misconfigured
            log.exception("data query %s failed", key)
            raise HTTPException(status_code=503, detail=f"data unavailable: {type(exc).__name__}") from exc

    @app.get("/api/health")
    def health():
        try:
            age = repo().data_age_seconds()
        except Exception:
            return JSONResponse({"status": "unavailable", "data_age_seconds": None}, status_code=503)
        status = "ok" if age is not None and age <= settings.stale_after_seconds else "delayed"
        return {"status": status, "data_age_seconds": age}

    @app.get("/api/network/summary")
    def summary():
        def build():
            rows = repo().facilities()
            counts = {"Crowded": 0, "Busy": 0, "Active": 0}
            for r in rows:
                counts[r.get("gedwin_band") or band(r.get("gedwin"))] += 1
            medians = [r["d2p_median_min"] for r in rows if r.get("d2p_median_min") is not None]
            return {
                "counts": counts,
                "facilities": len(rows),
                "census": sum(r.get("census") or 0 for r in rows),
                "boarders": sum(r.get("boarders_n") or 0 for r in rows),
                "network_d2p_median_min": statistics.median(medians) if medians else None,
                "as_of": max((r["snapshot_ts"] for r in rows), default=None),
                "narrative": repo().network(),
            }

        return data("summary", build)

    @app.get("/api/network/history")
    def network_history(hours: int = Query(24)):
        if hours not in RANGE_BUCKET_MINUTES:
            raise HTTPException(status_code=422, detail=f"hours must be one of {sorted(RANGE_BUCKET_MINUTES)}")
        minutes = RANGE_BUCKET_MINUTES[hours]
        rows = data(f"network_history:{hours}", lambda: repo().network_history(hours, minutes))
        return {"bucket_minutes": minutes, "buckets": rows}

    @app.get("/api/facilities")
    def facilities():
        def build():
            rows, lines = repo().facilities(), repo().sparklines(6)
            for r in rows:
                r["sparkline"] = lines.get(r["facility_id"], [])
            return {"facilities": rows}

        return data("facilities", build)

    @app.get("/api/facilities/{facility_id}")
    def facility(facility_id: str):
        row = data(f"facility:{facility_id}", lambda: repo().facility(facility_id))
        if row is None:
            raise HTTPException(status_code=404, detail=f"unknown facility {facility_id}")
        return {"facility": row, "narrative": data(f"narrative:{facility_id}", lambda: repo().narrative(facility_id))}

    @app.get("/api/facilities/{facility_id}/history")
    def history(facility_id: str, hours: int = Query(24, ge=1, le=168)):
        return {"points": data(f"history:{facility_id}:{hours}", lambda: repo().history(facility_id, hours))}

    @app.get("/api/facilities/{facility_id}/board")
    def board(facility_id: str):
        return {"patients": data(f"board:{facility_id}", lambda: repo().board(facility_id))}

    @app.get("/api/heatmap")
    def heatmap():
        def build():
            return {"cells": [{**c, "band": band(c.get("gedwin"))} for c in repo().heatmap(24)]}

        return data("heatmap", build)

    @app.get("/api/definitions")
    def definitions():
        return DEFINITIONS

    @app.post("/api/genie/messages")
    def ask(body: AskBody, request: Request):
        try:
            return genie_service().ask(request.headers.get("x-forwarded-access-token"), body.question, body.conversation_id)
        except LookupError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Genie request failed: {exc}") from exc

    @app.get("/api/genie/conversations/{conversation_id}/messages/{message_id}")
    def poll(conversation_id: str, message_id: str, request: Request):
        try:
            return genie_service().poll(request.headers.get("x-forwarded-access-token"), conversation_id, message_id)
        except LookupError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Genie request failed: {exc}") from exc

    if static_dir is not None and (static_dir / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str):
            if path.startswith("api/"):
                raise HTTPException(status_code=404)
            return FileResponse(static_dir / "index.html")

    return app


app = create_app()
