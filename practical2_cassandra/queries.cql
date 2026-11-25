USE energy_monitoring;
-- 1. ALL STATIONS and exact PRIMARY KEY lookup
SELECT station_id,station_name,station_type,region,status FROM power_stations;
SELECT * FROM power_stations WHERE station_id=123e4567-e89b-12d3-a456-426614174000;
-- 2. Index-supported lookups
SELECT station_name, station_type, max_capacity_mw, location FROM power_stations WHERE status='active';
SELECT station_name,location,max_capacity_mw FROM power_stations WHERE region='Південний';
SELECT station_name FROM power_stations WHERE station_type='hydro';
-- Accurate top-5 by clustering descending; unlike LIMIT 5 on unsorted base table.
SELECT station_name, max_capacity_mw FROM stations_by_capacity WHERE station_status='active' LIMIT 5;
-- 3. Composite partition key + clustering order
SELECT recorded_at,power_output_mw,voltage_kv,efficiency_percent FROM station_telemetry
 WHERE station_id=123e4567-e89b-12d3-a456-426614174000
 AND time_bucket='2025-09-17 14:00:00+0000' ORDER BY recorded_at DESC LIMIT 10;
-- 4. Aggregate over one bounded partition; no GROUP BY needed
SELECT AVG(power_output_mw),AVG(efficiency_percent),COUNT(*) FROM station_telemetry
 WHERE station_id=123e4567-e89b-12d3-a456-426614174000
 AND time_bucket='2025-09-17 14:00:00+0000';
-- 5. Alerts by exact partition and optional secondary index
SELECT alert_id,severity,message,acknowledged,resolved FROM station_alerts
 WHERE station_id=123e4567-e89b-12d3-a456-426614174000 AND alert_date='2025-09-17';
SELECT alert_id,severity,message FROM station_alerts
 WHERE station_id=123e4567-e89b-12d3-a456-426614174000 AND alert_date='2025-09-17' AND severity='warning';
-- 6. Hourly stats and time-to-live
SELECT * FROM hourly_statistics WHERE station_id=123e4567-e89b-12d3-a456-426614174000 AND date='2025-09-17';
SELECT station_id,recorded_at,TTL(power_output_mw) FROM station_telemetry LIMIT 5;
-- 7. Token distribution is only meaningful with multiple nodes
SELECT token(station_id),station_id,station_name FROM power_stations;
SELECT COUNT(*) FROM power_stations;
SELECT COUNT(*) FROM station_telemetry;
SELECT COUNT(*) FROM station_alerts;
DESCRIBE TABLE station_telemetry;
