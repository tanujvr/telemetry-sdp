-- Replace <catalog>.<schema> with your deployment target (dev mode prefixes the schema name).

-- 1) Fleet connectivity snapshot
SELECT connectivity_status, COUNT(*) AS devices
FROM <catalog>.<schema>.gold_device_latest_status
GROUP BY connectivity_status;

-- 2) Daily anomaly rate by site
SELECT event_date, site, anomaly_rate, active_devices, total_readings
FROM <catalog>.<schema>.gold_fleet_daily_kpis
ORDER BY event_date DESC, anomaly_rate DESC;

-- 3) Worst devices in the last 24h
SELECT device_id, ROUND(AVG(health_score), 1) AS avg_health_score, SUM(anomaly_readings) AS anomalies
FROM <catalog>.<schema>.gold_device_health_hourly
WHERE hour_ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY device_id
ORDER BY avg_health_score ASC
LIMIT 10; :param_1

-- 4) Latest HIGH-severity alerts
SELECT event_ts, device_id, alert_type, temperature_c, vibration_mm_s, battery_pct
FROM <catalog>.<schema>.gold_alerts
WHERE severity = 'HIGH'
ORDER BY event_ts DESC
LIMIT 50;

-- 5) Data-quality scorecard
SELECT ingest_date, rule, violations, violation_rate
FROM <catalog>.<schema>.gold_data_quality_daily
ORDER BY ingest_date DESC, violations DESC;

-- 6) Device history (SCD2): firmware changes over time
SELECT device_id, firmware_version, __START_AT, __END_AT
FROM <catalog>.<schema>.dim_device
WHERE device_id = 'dev-0001'
ORDER BY __START_AT;
