"""Databricks AI Functions behind a small interface so every transform around them is testable locally.

Methods take a column *name* (AI functions are SQL functions; ai_query needs the named argument
failOnError => false, which the DataFrame API cannot express). Never pass responseFormat: Claude 5.x
endpoints reject it; callers ask for JSON in the prompt and parse with from_json.
"""

from __future__ import annotations

from typing import Protocol

from pyspark.sql import Column
from pyspark.sql import functions as F

from ed_ops.ddl.generate import sql_string

MASK_LABELS = ("person", "phone number", "date of birth", "address", "medical record number", "email")
CATEGORIES = (
    "cardiac", "respiratory", "neurological", "trauma", "gastrointestinal", "infectious", "psychiatric",
    "obstetric", "musculoskeletal", "other",
)
VITAL_LABELS = ("heart_rate", "blood_pressure", "respiratory_rate", "spo2", "temperature", "pain_score", "symptom_onset")


def _array(values: tuple[str, ...]) -> str:
    return "array(" + ", ".join(sql_string(v) for v in values) + ")"


class AiFunctions(Protocol):
    endpoint: str

    def mask(self, column: str) -> Column: ...

    def classify(self, column: str) -> Column: ...

    def extract(self, column: str) -> Column: ...

    def query(self, column: str) -> Column: ...


class DatabricksAi:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    def mask(self, column: str) -> Column:
        return F.expr(f"ai_mask(`{column}`, {_array(MASK_LABELS)})")

    def classify(self, column: str) -> Column:
        return F.expr(f"ai_classify(`{column}`, {_array(CATEGORIES)})")

    def extract(self, column: str) -> Column:
        return F.expr(f"ai_extract(`{column}`, {_array(VITAL_LABELS)})")

    def query(self, column: str) -> Column:
        return F.expr(f"ai_query({sql_string(self.endpoint)}, `{column}`, failOnError => false)")
