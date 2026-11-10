"""Genie space (spec 14): rendered from src/ed_ops/genie/space.yml and created or updated by title from ed_setup
(Genie rejects spaces whose tables do not exist yet, so bundle deploy cannot own it)."""

from __future__ import annotations

import json
import uuid
from importlib import resources

import yaml

from ed_ops.config import LOGICAL_SCHEMAS, Config

NAMESPACE = uuid.UUID("2b7f9a4e-91c3-4d6a-8e15-0c4f7d3a6b21")


def load_space_doc() -> dict:
    return yaml.safe_load(resources.files("ed_ops.genie").joinpath("space.yml").read_text())


def _id(*parts: str) -> str:
    return uuid.uuid5(NAMESPACE, "|".join(parts)).hex


def _render(text: str, cfg: Config) -> str:
    return text.format(**{logical: cfg.schema(logical) for logical in LOGICAL_SCHEMAS}, timezone=cfg.timezone)


def build_serialized_space(cfg: Config, doc: dict) -> dict:
    tables = sorted(cfg.table(*entry.split(".", 1)) for entry in doc["tables"])
    return {
        "version": 2,
        "data_sources": {"tables": [{"identifier": t} for t in tables]},
        "instructions": {
            "text_instructions": [{"id": _id("text", "0"), "content": [_render(doc["instructions"], cfg)]}],
            "example_question_sqls": [
                {"id": _id("example", e["question"]), "question": [e["question"]], "sql": [_render(e["sql"], cfg).strip()]}
                for e in doc["examples"]
            ],
        },
    }


def list_spaces(w) -> list[dict]:
    spaces, token = [], None
    while True:
        query = {"page_size": 100, **({"page_token": token} if token else {})}
        page = w.api_client.do("GET", "/api/2.0/genie/spaces", query=query) or {}
        spaces += page.get("spaces", [])
        token = page.get("next_page_token")
        if not token:
            return spaces


def find_space_id(w, title: str) -> str | None:
    return next((s["space_id"] for s in list_spaces(w) if s.get("title") == title), None)


def upsert_genie_space(w, cfg: Config, serialized: dict) -> str:
    body = {
        "title": cfg.genie_space_title,
        "description": load_space_doc()["description"],
        "warehouse_id": cfg.warehouse_id,
        "serialized_space": json.dumps(serialized),
    }
    space_id = find_space_id(w, cfg.genie_space_title)
    if space_id:
        w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
        print(f"genie: updated space {space_id} ({cfg.genie_space_title})")
        return space_id
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
    print(f"genie: created space {space_id} ({cfg.genie_space_title})")
    return space_id


def grant_can_run(w, space_id: str, application_id: str) -> None:
    w.api_client.do(
        "PATCH",
        f"/api/2.0/permissions/genie/{space_id}",
        body={"access_control_list": [{"service_principal_name": application_id, "permission_level": "CAN_RUN"}]},
    )
    print(f"genie: granted CAN_RUN on {space_id} to {application_id}")
