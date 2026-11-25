-- P2: query-driven educational Cassandra schema. Single-node replication=1.
CREATE KEYSPACE IF NOT EXISTS energy_monitoring
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

USE energy_monitoring;

CREATE TABLE IF NOT EXISTS power_stations (
    station_id UUID PRIMARY KEY,
    station_name TEXT,
    station_type TEXT,
    location TEXT,
    region TEXT,
    max_capacity_mw DOUBLE,
    commissioned_date DATE,
    status TEXT,
    coordinates MAP<TEXT, DOUBLE>,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Query-oriented projection for a genuine top-capacity lookup by status.
CREATE TABLE IF NOT EXISTS stations_by_capacity (
    station_status TEXT,
    max_capacity_mw DOUBLE,
    station_id UUID,
    station_name TEXT,
    station_type TEXT,
    PRIMARY KEY (station_status, max_capacity_mw, station_id)
) WITH CLUSTERING ORDER BY (max_capacity_mw DESC, station_id ASC);

CREATE TABLE IF NOT EXISTS station_telemetry (
    station_id UUID,
    time_bucket TIMESTAMP,
    recorded_at TIMESTAMP,
    power_output_mw DOUBLE,
    voltage_kv DOUBLE,
    frequency_hz DOUBLE,
    temperature_c DOUBLE,
    efficiency_percent DOUBLE,
    fuel_consumption_rate DOUBLE,
    status TEXT,
    alert_level INT,
    PRIMARY KEY ((station_id, time_bucket), recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC)
AND default_time_to_live = 604800;

-- Added: referenced repeatedly in original task, but the supplied schema omits it.
CREATE TABLE IF NOT EXISTS station_alerts (
    station_id UUID,
    alert_date DATE,
    alert_id TIMEUUID,
    alert_timestamp TIMESTAMP,
    severity TEXT,
    alert_type TEXT,
    message TEXT,
    current_value DOUBLE,
    threshold_value DOUBLE,
    acknowledged BOOLEAN,
    resolved BOOLEAN,
    PRIMARY KEY ((station_id, alert_date), alert_id)
) WITH CLUSTERING ORDER BY (alert_id DESC);

CREATE TABLE IF NOT EXISTS hourly_statistics (
    station_id UUID,
    date DATE,
    hour TIMESTAMP,
    avg_power_mw DOUBLE,
    max_power_mw DOUBLE,
    min_power_mw DOUBLE,
    total_energy_mwh DOUBLE,
    avg_efficiency_percent DOUBLE,
    uptime_minutes INT,
    PRIMARY KEY ((station_id, date), hour)
) WITH CLUSTERING ORDER BY (hour DESC);

CREATE TABLE IF NOT EXISTS hourly_alert_counts (
    station_id UUID,
    date DATE,
    hour TIMESTAMP,
    alert_count COUNTER,
    PRIMARY KEY ((station_id, date), hour)
);

-- Secondary indexes: small demo only; production commonly uses query-specific tables.
CREATE INDEX IF NOT EXISTS station_type_idx ON power_stations (station_type);
CREATE INDEX IF NOT EXISTS station_region_idx ON power_stations (region);
CREATE INDEX IF NOT EXISTS station_status_idx ON power_stations (status);
CREATE INDEX IF NOT EXISTS alert_level_idx ON station_telemetry (alert_level);
CREATE INDEX IF NOT EXISTS station_alerts_severity_idx ON station_alerts (severity);
