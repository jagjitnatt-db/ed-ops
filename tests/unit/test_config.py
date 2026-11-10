import pytest

from ed_ops.config import Config, parse_config


def test_schema_and_table_names_use_prefix():
    cfg = Config()
    assert cfg.schema("silver") == "hls_jagjit_lb_playground_catalog.ed_silver"
    assert cfg.table("gold", "facility_current") == "hls_jagjit_lb_playground_catalog.ed_gold.facility_current"


def test_unknown_logical_schema_rejected():
    with pytest.raises(ValueError, match="unknown logical schema"):
        Config().schema("platinum")


def test_paths():
    cfg = Config(schema_prefix="ed_smoke_", checkpoint_version=3)
    assert cfg.landing_path == "/Volumes/hls_jagjit_lb_playground_catalog/ed_smoke_bronze/landing/events"
    assert cfg.checkpoint("gold_metrics") == (
        "/Volumes/hls_jagjit_lb_playground_catalog/ed_smoke_ref/checkpoints/gold_metrics/v3"
    )


def test_parse_config_accepts_dash_underscore_and_equals_forms():
    cfg, rest = parse_config(["--facility-count", "5", "--schema_prefix=ed_smoke_", "--seed=7", "--extra", "x"])
    assert cfg.facility_count == 5
    assert cfg.schema_prefix == "ed_smoke_"
    assert cfg.seed == 7
    assert rest == ["--extra", "x"]


def test_parse_config_floats():
    cfg, _ = parse_config(["--late-rate", "0.5"])
    assert cfg.late_rate == 0.5


def test_tick_seconds_must_be_multiple_of_60():
    with pytest.raises(ValueError, match="multiple of 60"):
        Config(tick_seconds=30)


def test_ai_and_narrative_defaults():
    cfg = Config()
    assert (cfg.ai_backfill_days, cfg.llm_endpoint, cfg.narrative_minutes, cfg.ai_retry_minutes) == (
        2, "databricks-claude-sonnet-5-5", 15, 15
    )
    parsed, _ = parse_config(["--llm-endpoint=databricks-claude-haiku-5-5", "--ai_backfill_days=1"])
    assert parsed.llm_endpoint == "databricks-claude-haiku-5-5" and parsed.ai_backfill_days == 1
