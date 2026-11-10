"""Genie benchmarks (spec 14): ask each benchmark question through the Conversation API and compare Genie's result
with the expected SQL's result (values normalised, extra columns and row order tolerated)."""

from __future__ import annotations

from collections import Counter
from importlib import resources

import yaml
from pyspark.sql import SparkSession

from ed_ops.config import LOGICAL_SCHEMAS, Config
from ed_ops.serving.genie_space import find_space_id

MIN_PASS_RATE = 0.6


def norm(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    try:
        number = round(float(value), 1)
        return str(int(number)) if number == int(number) else str(number)
    except (TypeError, ValueError):
        return str(value).strip().lower()


def rows_match(expected: list[tuple], actual: list[list]) -> bool:
    if len(expected) != len(actual):
        return False
    remaining = [Counter(norm(v) for v in row) for row in actual]
    for row in expected:
        want = Counter(norm(v) for v in row)
        hit = next((i for i, have in enumerate(remaining) if not want - have), None)
        if hit is None:
            return False
        remaining.pop(hit)
    return True


def ask_genie(w, space_id: str, question: str) -> tuple[list[list], str | None]:
    msg = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
    status = str(getattr(msg.status, "value", msg.status))
    if status != "COMPLETED":
        return [], f"{status}: {getattr(msg.error, 'error', msg.error)}"
    for att in msg.attachments or []:
        if getattr(att, "query", None) is not None:
            res = w.genie.get_message_attachment_query_result(
                space_id=space_id, conversation_id=msg.conversation_id, message_id=msg.message_id or msg.id,
                attachment_id=att.attachment_id,
            )
            return list(res.statement_response.result.data_array or []), None
    return [], "no query attachment in Genie's answer"


def run_benchmarks(spark: SparkSession, cfg: Config) -> dict:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    space_id = find_space_id(w, cfg.genie_space_title)
    if not space_id:
        raise RuntimeError(f"Genie space {cfg.genie_space_title!r} not found; run ed_setup first")
    items = yaml.safe_load(resources.files("ed_ops.genie").joinpath("benchmarks.yml").read_text())
    schemas = {logical: cfg.schema(logical) for logical in LOGICAL_SCHEMAS}
    results = []
    for item in items:
        expected = [tuple(r) for r in spark.sql(item["sql"].format(**schemas)).collect()]
        actual, error = ask_genie(w, space_id, item["question"])
        passed = error is None and rows_match(expected, actual)
        results.append({"question": item["question"], "passed": passed, "error": error})
        print(f"{'PASS' if passed else 'FAIL'}  {item['question']}" + (f"  ({error})" if error else ""))
    rate = sum(r["passed"] for r in results) / len(results)
    print(f"genie benchmarks: {rate:.0%} passed ({sum(r['passed'] for r in results)}/{len(results)})")
    if rate < MIN_PASS_RATE:
        raise RuntimeError(f"Genie benchmark pass rate {rate:.0%} < {MIN_PASS_RATE:.0%}")
    return {"pass_rate": rate, "results": results}
