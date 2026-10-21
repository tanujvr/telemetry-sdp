"""SILVER: parsed, validated, de-duplicated, enriched.

bronze_telemetry
   -> bronze_parsed (view: safe typing)
        |-- valid   -> silver_telemetry            (dedupe + anomaly flags)
        |-- invalid -> silver_telemetry_quarantine (with the list of failed rules)
bronze_device_registry -> dim_device (SCD Type 2 via AUTO CDC)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TEMP_MAX_C = float(spark.conf.get("alert_temp_max_c", "85"))  # noqa: F821
VIB_MAX = float(spark.conf.get("alert_vibration_max_mm_s", "7.1"))  # noqa: F821
BATTERY_MIN = float(spark.conf.get("alert_battery_min_pct", "15"))  # noqa: F821
SIGNAL_MIN = float(spark.conf.get("alert_signal_min_dbm", "-90"))  # noqa: F821
DEDUP_WATERMARK = spark.conf.get("dedup_watermark", "2 hours")  # noqa: F821

# One source of truth for "what is a valid reading". Used for routing (valid vs quarantine)
# and as an expectation safety-net on the silver table.
RULES = {
    "event_id_present": "event_id IS NOT NULL",
    "device_id_present": "device_id IS NOT NULL",
    "event_ts_parsed": "event_ts IS NOT NULL",
    "event_ts_not_in_future": "event_ts <= current_timestamp() + INTERVAL 5 MINUTES",
    "temperature_in_range": "temperature_c BETWEEN -50 AND 150",
    "humidity_in_range": "humidity_pct IS NULL OR humidity_pct BETWEEN 0 AND 100",
    "battery_in_range": "battery_pct IS NULL OR battery_pct BETWEEN 0 AND 100",
    "no_rescued_data": "_rescued_data IS NULL",
}


def _passes(rule_sql: str):
    # NULL result (e.g. comparing a NULL column) counts as a failure
    return F.coalesce(F.expr(rule_sql), F.lit(False))


def _is_valid():
    cond = F.lit(True)
    for rule in RULES.values():
        cond = cond & _passes(rule)
    return cond


def _failed_rules():
    return F.array_compact(F.array(*[F.when(~_passes(r), F.lit(n)) for n, r in RULES.items()]))


@dp.temporary_view(name="bronze_parsed")
def bronze_parsed():
    """Typed view. try_to_timestamp never throws (important under ANSI mode)."""
    return (
        spark.readStream.table("bronze_telemetry")  # noqa: F821
        .withColumn("event_ts", F.expr("try_to_timestamp(event_ts)"))
        .withColumn("device_id", F.expr("nullif(trim(device_id), '')"))
    )


@dp.table(
    name="silver_telemetry",
    comment="Validated, de-duplicated telemetry with anomaly flags.",
    table_properties={"quality": "silver"},
    cluster_by=["event_date", "device_id"],
)
@dp.expect_all_or_drop(RULES)  # safety net + event-log metrics; rows are already filtered below
def silver_telemetry():
    # Invalid rows are filtered BEFORE the watermark: a single bogus future timestamp would
    # otherwise push the watermark forward and make good events look "late".
    return (
        spark.readStream.table("bronze_parsed")  # noqa: F821
        .filter(_is_valid())
        .withWatermark("event_ts", DEDUP_WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn(
            "anomaly_reasons",
            F.array_compact(
                F.array(
                    F.when(F.col("temperature_c") > TEMP_MAX_C, F.lit("HIGH_TEMPERATURE")),
                    F.when(F.col("vibration_mm_s") > VIB_MAX, F.lit("HIGH_VIBRATION")),
                    F.when(F.col("battery_pct") < BATTERY_MIN, F.lit("LOW_BATTERY")),
                    F.when(F.col("signal_dbm") < SIGNAL_MIN, F.lit("WEAK_SIGNAL")),
                )
            ),
        )
        .withColumn("is_anomaly", F.size("anomaly_reasons") > 0)
    )


@dp.table(
    name="silver_telemetry_quarantine",
    comment="Rejected telemetry rows with the rules they violated, for triage/replay.",
    table_properties={"quality": "silver"},
)
def silver_telemetry_quarantine():
    return (
        spark.readStream.table("bronze_parsed")  # noqa: F821
        .filter(~_is_valid())
        .withColumn("failed_rules", _failed_rules())
    )


# ---------------------------------------------------------------- device dimension (SCD2)
@dp.temporary_view(name="device_registry_clean")
def device_registry_clean():
    return (
        spark.readStream.table("bronze_device_registry")  # noqa: F821
        .withColumn("updated_at", F.expr("try_to_timestamp(updated_at)"))
        .withColumn("op", F.upper(F.coalesce("op", F.lit("UPSERT"))))
        .filter(F.col("device_id").isNotNull() & F.col("updated_at").isNotNull())
    )


dp.create_streaming_table(
    name="dim_device",
    comment="Device master data, SCD Type 2 (__START_AT / __END_AT; open row has __END_AT IS NULL).",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="dim_device",
    source="device_registry_clean",
    keys=["device_id"],
    sequence_by="updated_at",
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "_rescued_data", "_source_file", "_file_modified_at", "_ingested_at"],
    stored_as_scd_type=2,
    track_history_column_list=["site", "device_type", "firmware_version"],
)
