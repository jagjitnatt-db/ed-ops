"""Deterministic de-identification of structured PHI before anything is written to silver (spec 11.2).
No Unity Catalog column masks or ABAC are used; free text is masked by ai_mask in Plan 2."""

from __future__ import annotations

import secrets

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ed_ops.config import Config

SALT_KEY = "mrn_salt"
DEID_COLUMNS = ("patient_name_redacted", "mrn_hash", "birth_year", "age_at_arrival", "sex", "phone_last4", "city", "state")


def deidentify(events: DataFrame, salt: str) -> DataFrame:
    p = F.col("payload")
    arrival = F.col("event_type") == "ARRIVAL"
    return (
        events.withColumn("patient_name_redacted", F.when(arrival & p["patient_name"].isNotNull(), F.lit("REDACTED")))
        .withColumn("mrn_hash", F.when(arrival & p["mrn"].isNotNull(), F.sha2(F.concat(p["mrn"], F.lit(salt)), 256)))
        .withColumn("birth_year", F.when(arrival, F.year(p["dob"])))
        .withColumn("age_at_arrival", F.when(arrival, F.floor(F.months_between(F.col("event_ts"), p["dob"]) / 12).cast("int")))
        .withColumn("sex", F.when(arrival, p["sex"]))
        .withColumn("phone_last4", F.when(arrival, F.substring(F.regexp_replace(p["phone"], "[^0-9]", ""), -4, 4)))
        .withColumn("city", F.when(arrival, p["address"]["city"]))
        .withColumn("state", F.when(arrival, p["address"]["state"]))
        .withColumn(
            "payload",
            F.struct(
                p["arrival_mode"].alias("arrival_mode"),
                p["esi_level"].alias("esi_level"),
                p["bed_id"].alias("bed_id"),
                p["provider_id"].alias("provider_id"),
                p["disposition"].alias("disposition"),
            ),
        )
    )


def ensure_salt(cfg: Config) -> None:
    """Create the secret scope and a random MRN salt once (idempotent)."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    if cfg.secret_scope not in {s.name for s in w.secrets.list_scopes()}:
        w.secrets.create_scope(scope=cfg.secret_scope)
    if SALT_KEY not in {k.key for k in w.secrets.list_secrets(scope=cfg.secret_scope)}:
        w.secrets.put_secret(scope=cfg.secret_scope, key=SALT_KEY, string_value=secrets.token_hex(32))


def get_salt(cfg: Config) -> str:
    from databricks.sdk.runtime import dbutils

    return dbutils.secrets.get(scope=cfg.secret_scope, key=SALT_KEY)
