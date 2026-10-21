"""GOLD: business-ready aggregates (materialized views = incremental where possible)."""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

_FAR_FUTURE = F.expr("timestamp'9999-12-31 00:00:00'")


@dp.materialized_view(
    name="gold_device_health_hourly",
    comment="Per-device, per-hour health metrics and a simple health score.",
    table_properties={"quality": "gold"},
    cluster_by=["hour_ts", "device_id"],
)
def gold_device_health_hourly():
    return (
        spark.read.table("silver_telemetry")  # noqa: F821
        .groupBy("device_id", F.date_trunc("hour", "event_ts").alias("hour_ts"))
        .agg(
            F.count("*").alias("readings"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.round(F.avg("vibration_mm_s"), 3).alias("avg_vibration_mm_s"),
            F.max("vibration_mm_s").alias("max_vibration_mm_s"),
            F.min("battery_pct").alias("min_battery_pct"),
            F.round(F.avg("signal_dbm"), 1).alias("avg_signal_dbm"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_readings"),
        )
        .withColumn("anomaly_rate", F.round(F.col("anomaly_readings") / F.col("readings"), 4))
        .withColumn("health_score", F.round(100 * (1 - F.col("anomaly_rate")), 1))
    )


@dp.materialized_view(
    name="gold_fleet_daily_kpis",
    comment="Daily fleet KPIs by site/device type. Uses a point-in-time join to the SCD2 device dimension.",
    table_properties={"quality": "gold"},
    cluster_by=["event_date", "site"],
)
def gold_fleet_daily_kpis():
    t = spark.read.table("silver_telemetry").alias("t")  # noqa: F821
    d = spark.read.table("dim_device").alias("d")  # noqa: F821
    joined = t.join(
        d,
        (F.col("t.device_id") == F.col("d.device_id"))
        & (F.col("t.event_ts") >= F.col("d.__START_AT"))
        & (F.col("t.event_ts") < F.coalesce(F.col("d.__END_AT"), _FAR_FUTURE)),
        "left",
    )
    return (
        joined.groupBy(
            F.col("t.event_date").alias("event_date"),
            F.coalesce(F.col("d.site"), F.lit("UNKNOWN")).alias("site"),
            F.coalesce(F.col("d.device_type"), F.lit("UNKNOWN")).alias("device_type"),
        )
        .agg(
            F.countDistinct("t.device_id").alias("active_devices"),
            F.count("*").alias("total_readings"),
            F.round(F.avg("t.temperature_c"), 2).alias("avg_temperature_c"),
            F.max("t.temperature_c").alias("max_temperature_c"),
            F.round(F.avg("t.battery_pct"), 1).alias("avg_battery_pct"),
            F.countDistinct(F.when(F.col("t.battery_pct") < 15, F.col("t.device_id"))).alias("devices_low_battery"),
            F.sum(F.when(F.col("t.is_anomaly"), 1).otherwise(0)).alias("anomaly_readings"),
        )
        .withColumn("anomaly_rate", F.round(F.col("anomaly_readings") / F.col("total_readings"), 4))
    )


@dp.materialized_view(
    name="gold_device_latest_status",
    comment="Latest reading + connectivity status per currently-registered device. "
    "Uses current_timestamp(), so this view is fully recomputed on refresh.",
    table_properties={"quality": "gold"},
)
def gold_device_latest_status():
    w = Window.partitionBy("device_id").orderBy(F.col("event_ts").desc())
    latest = (
        spark.read.table("silver_telemetry")  # noqa: F821
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )
    current_dim = (
        spark.read.table("dim_device")  # noqa: F821
        .filter(F.col("__END_AT").isNull())  # decommissioned devices have a closed row -> excluded
        .select("device_id", "site", "device_type", F.col("firmware_version").alias("registered_firmware"))
    )
    minutes = (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("event_ts")) / 60
    return (
        latest.join(current_dim, "device_id", "inner")
        .withColumn("minutes_since_last_seen", F.round(minutes, 1))
        .withColumn(
            "connectivity_status",
            F.when(F.col("minutes_since_last_seen") <= 15, "ONLINE")
            .when(F.col("minutes_since_last_seen") <= 60, "STALE")
            .otherwise("OFFLINE"),
        )
        .select(
            "device_id", "site", "device_type", "registered_firmware",
            F.col("event_ts").alias("last_seen_at"), "minutes_since_last_seen", "connectivity_status",
            "temperature_c", "humidity_pct", "vibration_mm_s", "battery_pct", "signal_dbm",
            "status", "is_anomaly", "anomaly_reasons",
        )
    )


@dp.table(
    name="gold_alerts",
    comment="One row per alert (anomaly reading x reason). Append-only stream for alerting/BI.",
    table_properties={"quality": "gold"},
    cluster_by=["event_ts"],
)
def gold_alerts():
    return (
        spark.readStream.table("silver_telemetry")  # noqa: F821
        .filter("is_anomaly")
        .select(
            "event_id", "device_id", "event_ts",
            F.explode("anomaly_reasons").alias("alert_type"),
            "temperature_c", "vibration_mm_s", "battery_pct", "signal_dbm",
        )
        .withColumn(
            "severity",
            F.when(F.col("alert_type").isin("HIGH_TEMPERATURE", "HIGH_VIBRATION"), "HIGH").otherwise("MEDIUM"),
        )
    )


@dp.materialized_view(
    name="gold_data_quality_daily",
    comment="Daily data-quality scorecard: rule violations vs. total ingested records.",
    table_properties={"quality": "gold"},
)
def gold_data_quality_daily():
    total = (
        spark.read.table("bronze_telemetry")  # noqa: F821
        .groupBy(F.to_date("_ingested_at").alias("ingest_date"))
        .agg(F.count("*").alias("total_records"))
    )
    bad = (
        spark.read.table("silver_telemetry_quarantine")  # noqa: F821
        .select(F.to_date("_ingested_at").alias("ingest_date"), F.explode("failed_rules").alias("rule"))
        .groupBy("ingest_date", "rule")
        .agg(F.count("*").alias("violations"))
    )
    return (
        bad.join(total, "ingest_date")
        .withColumn("violation_rate", F.round(F.col("violations") / F.col("total_records"), 4))
        .select("ingest_date", "rule", "violations", "total_records", "violation_rate")
    )
