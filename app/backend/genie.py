"""Genie Conversation API proxy. Calls run with the user's forwarded token (on-behalf-of-user) when present, so the
user's Unity Catalog permissions apply; otherwise with the app service principal. The space is found by title."""

from __future__ import annotations

from typing import Callable

MAX_ROWS = 200
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}


class GenieService:
    def __init__(self, space_title: str, client_factory: Callable[[str | None], object]) -> None:
        self.space_title, self.client_factory = space_title, client_factory
        self._space_id: str | None = None

    def _space(self, w) -> str:
        if self._space_id:
            return self._space_id
        token, spaces = None, []
        while True:
            page = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 100, **({"page_token": token} if token else {})}) or {}
            spaces += page.get("spaces", [])
            token = page.get("next_page_token")
            if not token:
                break
        match = next((s["space_id"] for s in spaces if s.get("title") == self.space_title), None)
        if not match:
            raise LookupError(f"Genie space {self.space_title!r} not found or not shared with you")
        self._space_id = match
        return match

    def ask(self, token: str | None, question: str, conversation_id: str | None = None) -> dict:
        w = self.client_factory(token)
        space = self._space(w)
        if conversation_id:
            r = w.genie.create_message(space_id=space, conversation_id=conversation_id, content=question).response
            return {"conversation_id": conversation_id, "message_id": getattr(r, "message_id", None) or r.id}
        r = w.genie.start_conversation(space_id=space, content=question).response
        return {"conversation_id": r.conversation_id, "message_id": r.message_id}

    def poll(self, token: str | None, conversation_id: str, message_id: str) -> dict:
        w = self.client_factory(token)
        space = self._space(w)
        msg = w.genie.get_message(space_id=space, conversation_id=conversation_id, message_id=message_id)
        status = str(getattr(msg.status, "value", msg.status))
        out = {"done": status in TERMINAL, "status": status, "text": None, "sql": None, "columns": [], "rows": [],
               "error": str(getattr(msg.error, "error", msg.error)) if msg.error else None}
        for att in msg.attachments or []:
            if getattr(att, "text", None) is not None and out["text"] is None:
                out["text"] = att.text.content
            if getattr(att, "query", None) is not None and status == "COMPLETED":
                out["sql"] = att.query.query
                out["text"] = out["text"] or att.query.description
                res = w.genie.get_message_attachment_query_result(
                    space_id=space, conversation_id=conversation_id, message_id=message_id, attachment_id=att.attachment_id
                ).statement_response
                out["columns"] = [c.name for c in res.manifest.schema.columns]
                out["rows"] = list(res.result.data_array or [])[:MAX_ROWS]
        return out


def workspace_client(token: str | None):
    from databricks.sdk import WorkspaceClient

    if token:
        import os

        return WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=token, auth_type="pat")
    return WorkspaceClient()
