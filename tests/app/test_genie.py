from types import SimpleNamespace

import pytest

from backend.genie import GenieService


class FakeGenieApi:
    def start_conversation(self, space_id, content):
        return SimpleNamespace(response=SimpleNamespace(conversation_id="c1", message_id="m1"))

    def create_message(self, space_id, conversation_id, content):
        return SimpleNamespace(response=SimpleNamespace(conversation_id=conversation_id, message_id="m2", id="m2"))

    def get_message(self, space_id, conversation_id, message_id):
        if message_id == "m1":
            att = [SimpleNamespace(attachment_id="a1", text=SimpleNamespace(content="Two EDs."),
                                   query=SimpleNamespace(query="SELECT 1", description="d"))]
            return SimpleNamespace(status=SimpleNamespace(value="COMPLETED"), attachments=att, error=None)
        return SimpleNamespace(status=SimpleNamespace(value="EXECUTING_QUERY"), attachments=[], error=None)

    def get_message_attachment_query_result(self, space_id, conversation_id, message_id, attachment_id):
        cols = [SimpleNamespace(name="facility"), SimpleNamespace(name="gedwin")]
        sr = SimpleNamespace(manifest=SimpleNamespace(schema=SimpleNamespace(columns=cols)),
                             result=SimpleNamespace(data_array=[["A", "2.9"]] * 300))
        return SimpleNamespace(statement_response=sr)


def make(spaces):
    calls = []

    def factory(token):
        calls.append(token)
        api = SimpleNamespace(do=lambda method, path, query=None, body=None: {"spaces": spaces})
        return SimpleNamespace(genie=FakeGenieApi(), api_client=api)

    return GenieService("ED Ops Crowding Assistant (dev)", factory), calls


def test_ask_and_poll_with_user_token():
    svc, calls = make([{"space_id": "s1", "title": "ED Ops Crowding Assistant (dev)"}])
    assert svc.ask("tok", "q") == {"conversation_id": "c1", "message_id": "m1"}
    assert svc.ask("tok", "follow-up", conversation_id="c1") == {"conversation_id": "c1", "message_id": "m2"}
    done = svc.poll("tok", "c1", "m1")
    assert done["done"] and done["text"] == "Two EDs." and done["sql"] == "SELECT 1"
    assert done["columns"] == ["facility", "gedwin"] and len(done["rows"]) == 200  # capped
    pending = svc.poll("tok", "c1", "m2")
    assert pending["done"] is False and pending["status"] == "EXECUTING_QUERY"
    assert set(calls) == {"tok"}


def test_missing_space_raises_lookup_error():
    svc, _ = make([{"space_id": "s1", "title": "Something else"}])
    with pytest.raises(LookupError, match="not found"):
        svc.ask(None, "q")
