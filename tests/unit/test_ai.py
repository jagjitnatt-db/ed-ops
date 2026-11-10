from pyspark.sql import functions as F

from ed_ops.ai import CATEGORIES, MASK_LABELS, VITAL_LABELS, DatabricksAi
from tests.fake_ai import FakeAi


def test_databricks_expressions(spark):
    ai = DatabricksAi("databricks-claude-sonnet-5-5")
    assert "ai_mask(`note`, array('person'" in str(ai.mask("note"))
    assert "'medical record number'" in str(ai.mask("note"))
    assert "ai_classify(`note_masked`, array('cardiac'" in str(ai.classify("note_masked"))
    assert "ai_extract(`note_masked`, array('heart_rate'" in str(ai.extract("note_masked"))
    q = str(ai.query("_prompt"))
    assert "ai_query('databricks-claude-sonnet-5-5', `_prompt`" in q and "failOnError" in q
    assert "responseFormat" not in q
    assert len(CATEGORIES) == 10 and "person" in MASK_LABELS and VITAL_LABELS[0] == "heart_rate"


def test_fake_ai_output_shapes(spark):
    df = spark.createDataFrame([("Pt Pat1 Tester MRN-7000001 ph 312-555-0001 crushing chest pain HR 118, BP 92/58, RR 24, SpO2 89% RA, T 37.1C. Pain 9/10.",)], "note STRING")
    ai = FakeAi()
    row = df.select(
        ai.mask("note").alias("m"), ai.classify("note").alias("c"), ai.extract("note").alias("x"),
        ai.query("note").alias("q"),
    ).collect()[0]
    assert "Pat1" not in row.m and "MRN-7000001" not in row.m and "[MASKED]" in row.m
    assert row.c == "cardiac"
    assert row.x.heart_rate == "118" and row.x.spo2 == "89% RA" and row.x.pain_score == "9/10"
    assert set(row.q.asDict()) == {"result", "errorMessage"}
