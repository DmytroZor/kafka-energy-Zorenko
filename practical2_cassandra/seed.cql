-- P2 seed: example plant data taken from the scenario; educational/illustrative.
-- Re-runnable UPSERTs; note COUNTER operation should be run only once.
USE energy_monitoring;

INSERT INTO power_stations (station_id, station_name, station_type, location, region,
 max_capacity_mw, commissioned_date, status, coordinates, created_at, updated_at)
VALUES (123e4567-e89b-12d3-a456-426614174000, 'Київська ТЕС', 'thermal', 'Київ',
 'Центральний', 1400.0, '1960-01-01', 'active', {'lat':50.4501,'lon':30.5234},
 toTimestamp(now()), toTimestamp(now()));
INSERT INTO power_stations (station_id, station_name, station_type, location, region,
 max_capacity_mw, commissioned_date, status, coordinates, created_at, updated_at)
VALUES (223e4567-e89b-12d3-a456-426614174001, 'Дніпровська ГЕС', 'hydro', 'Запоріжжя',
 'Південний', 1548.0, '1932-10-10', 'active', {'lat':47.8388,'lon':35.1396},
 toTimestamp(now()), toTimestamp(now()));
INSERT INTO power_stations (station_id, station_name, station_type, location, region,
 max_capacity_mw, commissioned_date, status, coordinates, created_at, updated_at)
VALUES (323e4567-e89b-12d3-a456-426614174002, 'Сонячна станція Нікополь', 'solar', 'Нікополь',
 'Південний', 246.0, '2019-12-01', 'active', {'lat':47.5659,'lon':34.3975},
 toTimestamp(now()), toTimestamp(now()));

INSERT INTO station_telemetry (station_id,time_bucket,recorded_at,power_output_mw,
 voltage_kv,frequency_hz,temperature_c,efficiency_percent,fuel_consumption_rate,status,alert_level)
VALUES (123e4567-e89b-12d3-a456-426614174000,'2025-09-17 14:00:00+0000','2025-09-17 14:15:30+0000',1150.5,220.1,50.01,75.2,87.5,2.8,'online',0);
INSERT INTO station_telemetry (station_id,time_bucket,recorded_at,power_output_mw,
 voltage_kv,frequency_hz,temperature_c,efficiency_percent,fuel_consumption_rate,status,alert_level)
VALUES (123e4567-e89b-12d3-a456-426614174000,'2025-09-17 14:00:00+0000','2025-09-17 14:16:30+0000',1145.8,219.8,49.99,76.1,87.2,2.9,'online',0);
INSERT INTO station_telemetry (station_id,time_bucket,recorded_at,power_output_mw,
 voltage_kv,frequency_hz,temperature_c,efficiency_percent,status,alert_level)
VALUES (223e4567-e89b-12d3-a456-426614174001,'2025-09-17 14:00:00+0000','2025-09-17 14:15:45+0000',800.2,400.5,50.0,22.3,92.1,'online',0);
INSERT INTO station_telemetry (station_id,time_bucket,recorded_at,power_output_mw,
 voltage_kv,frequency_hz,temperature_c,efficiency_percent,status,alert_level)
VALUES (323e4567-e89b-12d3-a456-426614174002,'2025-09-17 14:00:00+0000','2025-09-17 14:15:00+0000',180.0,35.2,50.0,32.4,91.2,'online',1);

-- A fixed ID makes this INSERT repeatable; resolving it is demonstrated in maintenance.py.
INSERT INTO station_alerts (station_id, alert_date, alert_id, alert_timestamp,
 severity, alert_type, message, current_value, threshold_value, acknowledged, resolved)
VALUES (123e4567-e89b-12d3-a456-426614174000, '2025-09-17',
 97e49ae0-93ba-11f0-8a1a-0242ac120002, '2025-09-17 14:18:00+0000',
 'warning', 'temperature', 'Температура перевищує норму', 78.5, 75.0, false, false);

INSERT INTO hourly_statistics (station_id,date,hour,avg_power_mw,max_power_mw,min_power_mw,
 total_energy_mwh,avg_efficiency_percent,uptime_minutes)
VALUES (123e4567-e89b-12d3-a456-426614174000,'2025-09-17','2025-09-17 14:00:00+0000',
 1148.15,1150.5,1145.8,1148.15,87.35,60);

-- Materialized by the application (not a Cassandra Materialized View): top stations.
INSERT INTO stations_by_capacity (station_status,max_capacity_mw,station_id,station_name,station_type)
VALUES ('active',1400.0,123e4567-e89b-12d3-a456-426614174000,'Київська ТЕС','thermal');
INSERT INTO stations_by_capacity (station_status,max_capacity_mw,station_id,station_name,station_type)
VALUES ('active',1548.0,223e4567-e89b-12d3-a456-426614174001,'Дніпровська ГЕС','hydro');
INSERT INTO stations_by_capacity (station_status,max_capacity_mw,station_id,station_name,station_type)
VALUES ('active',246.0,323e4567-e89b-12d3-a456-426614174002,'Сонячна станція Нікополь','solar');
