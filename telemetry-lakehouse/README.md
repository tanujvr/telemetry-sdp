# Databricks IoT Telemetry Lakehouse

End-to-end telemetry project on **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables), deployed with a **Databricks Asset Bundle**.

```
 generate_telemetry.py ──► UC Volume /landing ──► Auto Loader
                              ├─ telemetry/*.json          │
                              └─ device_registry/*.json    ▼
 BRONZE  bronze_telemetry · bronze_device_registry   (raw, rescued data, lineage cols)
 SILVER  silver_telemetry            valid + de-duplicated + anomaly flags
         silver_telemetry_quarantine rejected rows + failed_rules[]
         dim_device                  SCD Type 2 via AUTO CDC (UPSERT/DELETE)
 GOLD    gold_device_health_hourly   gold_fleet_daily_kpis (point-in-time join to SCD2)
         gold_device_latest_status   gold_alerts   gold_data_quality_daily
```

## Layout
| Path | What |
|---|---|
| `databricks.yml` | Bundle, variables (thresholds, catalog), dev/prod targets |
| `resources/` | Schema + landing Volume, pipeline, job (generate → refresh pipeline) |
| `src/ingestion/generate_telemetry.py` | Simulator: dupes, bad records, anomalies, registry CDC |
| `src/pipeline/transformations/` | `01_bronze.py`, `02_silver.py`, `03_gold.py` |
| `sql/dashboard_queries.sql` | Ready queries for AI/BI dashboards |
| `tests/` | pytest for the simulator |

## Prerequisites
Unity Catalog, serverless compute enabled, an **existing catalog** you can create schemas in, and a recent Databricks CLI (`databricks -v`, v0.250+).

## Deploy & run
```bash
# 1. set workspace host in databricks.yml, then:
databricks bundle validate -t dev --var catalog=<your_catalog>
databricks bundle deploy   -t dev --var catalog=<your_catalog>
databricks bundle run telemetry_job -t dev --var catalog=<your_catalog>
```
Run the job again any time: each run lands new files and the pipeline picks up only those (incremental).
First run tip: pass a longer history, e.g. change `--window-minutes` to `360` in `resources/job.yml`.

## Local test (no Databricks needed)
```bash
pip install pytest && pytest
python src/ingestion/generate_telemetry.py --base-path ./landing_demo
```

## Design notes
- **Quarantine, not silent drop**: invalid rows are routed to `silver_telemetry_quarantine` with `failed_rules`; `gold_data_quality_daily` turns that into a scorecard.
- **Filter before watermark**: invalid rows are removed before `dropDuplicatesWithinWatermark`; one bad future timestamp would otherwise advance the watermark and make valid events "late".
- **Dedup window**: duplicates are only detected within `dedup_watermark` (default 2 hours). Events later than that are dropped by the stateful operator. Tune the variable to your device behaviour.
- **SCD2 join**: `gold_fleet_daily_kpis` attributes each reading to the site/type valid *at event time*.
- **Timestamps are UTC.** `event_date` is the UTC date.
- `gold_device_latest_status` uses `current_timestamp()`, so it is fully recomputed each refresh (cheap at fleet scale, fine for dashboards).
- Thresholds (temperature, vibration, battery, signal) are bundle variables, no code change needed.

## Swap in a real stream (Kafka / Event Hubs)
Replace the body of `bronze_telemetry()` in `01_bronze.py`:
```python
return (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", spark.conf.get("kafka_bootstrap"))
    .option("subscribe", "device-telemetry")
    .option("startingOffsets", "earliest")
    .load()
    .select(F.from_json(F.col("value").cast("string"), TELEMETRY_SCHEMA).alias("j"),
            F.col("value").cast("string").alias("_raw"))
    .select("j.*", F.current_timestamp().alias("_ingested_at"))
)
```
(then add `_rescued_data` as a null column or adjust the `no_rescued_data` rule), and set `continuous: true` in `resources/pipeline.yml`.

## Troubleshooting
- Older API: if your workspace rejects `from pyspark import pipelines as dp`, use `import dlt` with the same decorator names (`dlt.table`, `dlt.view`, `dlt.apply_changes`).
- If the serverless job task asks for an environment, add an `environments:` block to `resources/job.yml` (the simulator only needs the standard library).
