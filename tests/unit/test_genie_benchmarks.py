from types import SimpleNamespace

import pytest

from ed_ops.serving.genie_benchmarks import ask_genie, norm, rows_match


@pytest.mark.parametrize("value,expected", [(3, "3"), (3.0, "3"), ("3.04", "3"), (2.26, "2.3"), ("Peoria", "peoria"), (None, ""), (True, "true")])
def test_norm(value, expected):
    assert norm(value) == expected


def test_rows_match_allows_extra_columns_and_any_order():
    expected = [("community", 28), ("rural_critical_access", 12)]
    actual = [["12", "rural_critical_access", "x"], ["28.0", "community", "y"]]
    assert rows_match(expected, actual)
    assert not rows_match(expected, actual[:1])  # row count differs
    assert not rows_match([("community", 29)], [["28", "community"]])


def test_ask_genie_reads_query_attachment():
    att = SimpleNamespace(attachment_id="a1", query=SimpleNamespace(query="SELECT 1"), text=None)
    msg = SimpleNamespace(status=SimpleNamespace(value="COMPLETED"), conversation_id="c1", message_id="m1", id="m1",
                          attachments=[att], error=None)
    result = SimpleNamespace(statement_response=SimpleNamespace(result=SimpleNamespace(data_array=[["28", "community"]])))
    genie = SimpleNamespace(start_conversation_and_wait=lambda space_id, content: msg,
                            get_message_attachment_query_result=lambda **kw: result)
    rows, err = ask_genie(SimpleNamespace(genie=genie), "s1", "q")
    assert rows == [["28", "community"]] and err is None


def test_ask_genie_reports_failures():
    msg = SimpleNamespace(status=SimpleNamespace(value="FAILED"), conversation_id="c", message_id="m", id="m",
                          attachments=[], error=SimpleNamespace(error="boom"))
    genie = SimpleNamespace(start_conversation_and_wait=lambda space_id, content: msg)
    rows, err = ask_genie(SimpleNamespace(genie=genie), "s1", "q")
    assert rows == [] and "FAILED" in err
