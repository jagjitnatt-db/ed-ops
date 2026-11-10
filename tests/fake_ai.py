"""Local stand-in for Databricks AI Functions: deterministic Spark expressions with the same output shapes."""

from pyspark.sql import Column
from pyspark.sql import functions as F

from ed_ops.ai import VITAL_LABELS

NAME = r"Pat\d+ Tester"
MRN = r"MRN-\d+"
PHONE = r"\d{3}-555-\d{4}"
DOB = r"\d{2}/\d{2}/\d{4}"

ESI_MARKER = "Emergency Severity Index"
FACILITY_MARKER = "single emergency department"
NETWORK_MARKER = "health system"


class FakeAi:
    endpoint = "fake-endpoint"

    def __init__(self, miss_phone: bool = False, fail_marker: str | None = "FAILME", garbage_marker: str | None = "GARBAGE"):
        self.miss_phone = miss_phone
        self.fail_marker = fail_marker
        self.garbage_marker = garbage_marker

    def mask(self, column: str) -> Column:
        c = F.col(column)
        for pattern in (NAME, MRN, DOB) + (() if self.miss_phone else (PHONE,)):
            c = F.regexp_replace(c, pattern, "[MASKED]")
        return c

    def classify(self, column: str) -> Column:
        c = F.col(column)
        return F.when(c.rlike("(?i)chest"), F.lit("cardiac")).when(c.rlike("(?i)SOB|breath"), F.lit("respiratory")).otherwise(F.lit("other"))

    def extract(self, column: str) -> Column:
        c = F.col(column)
        patterns = {
            "heart_rate": r"HR (\d+)",
            "blood_pressure": r"BP (\d+/\d+)",
            "respiratory_rate": r"RR (\d+)",
            "spo2": r"SpO2 (\d+% ?[A-Za-z]*)",
            "temperature": r"T ([\d.]+[CF])",
            "pain_score": r"(\d+/10)",
            "symptom_onset": r"x (\d+ \w+)",
        }
        return F.struct(*[F.nullif(F.regexp_extract(c, patterns[k], 1), F.lit("")).alias(k) for k in VITAL_LABELS])

    def _reply(self, prompt: Column) -> Column:
        esi = F.concat(
            F.lit('Sure! ```json\n{"suggested_esi": '),
            F.when(prompt.rlike("(?i)crushing|stroke|sepsis"), F.lit("1")).otherwise(F.lit("3")),
            F.lit(',\n "rationale": "fake rationale"}\n```'),
        )
        facility = F.lit('{"headline": "Fake facility status", "explanation": "Census and waits from the input.", '
                         '"recommended_actions": ["a", "b", "c", "d"]}')
        network = F.lit('{"headline": "Fake network status", "explanation": "Two EDs crowded.", '
                        '"crowded_facilities": ["X"], "doing_well_facilities": ["Y"], "recommended_actions": ["z"]}')
        return (
            F.when(prompt.contains(ESI_MARKER), esi)
            .when(prompt.contains(FACILITY_MARKER), facility)
            .when(prompt.contains(NETWORK_MARKER), network)
            .otherwise(F.lit("{}"))
        )

    def query(self, column: str) -> Column:
        prompt = F.col(column)
        failing = prompt.contains(self.fail_marker) if self.fail_marker else F.lit(False)
        garbage = prompt.contains(self.garbage_marker) if self.garbage_marker else F.lit(False)
        result = F.when(failing, F.lit(None).cast("string")).when(garbage, F.lit("I cannot help with that.")).otherwise(self._reply(prompt))
        error = F.when(failing, F.lit("fake endpoint error")).otherwise(F.lit(None).cast("string"))
        return F.struct(result.alias("result"), error.alias("errorMessage"))
