[CmdletBinding()]
param(
    [ValidateRange(1, 300)]
    [int]$HistoryDays = 30,

    [ValidatePattern('^\d{8}-\d{6}$')]
    [string]$ResumeRun
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)

$runId = Get-Date -Format 'yyyyMMdd-HHmmss'
if ($ResumeRun) { $runId = $ResumeRun }
$project = "energy-zorenko-$runId"
$evidence = Join-Path (Get-Location) "results\run-$runId"
if ($ResumeRun) {
    if (-not (Test-Path -LiteralPath (Join-Path $evidence '04-live-baseline.txt'))) {
        throw "No completed streaming simulation in $evidence"
    }
}
else {
    New-Item -ItemType Directory -Path $evidence | Out-Null
}
$broker = 'kafka:29092'
$topic = 'power-station-data-zorenko'
$group = 'energy-monitor-zorenko'
$workers = [Collections.Generic.List[string]]::new()
$startupAttempted = $false

function Compose { & docker compose -p $project @args }
function Kafka { Compose exec -T kafka @args }
function Cassandra { Compose exec -T cassandra @args }
function Lab { Compose run --rm -T lab @args }

function Write-RunLine([string]$Line) {
    try { [Console]::WriteLine($Line) }
    catch [System.IO.IOException] {}
}

function Save-Stage([string]$StageName, [string]$Command, [scriptblock]$Action) {
    $path = Join-Path $evidence "$StageName.txt"
    Write-RunLine "`n=== $StageName ==="
    Write-RunLine "> $Command"
    $ErrorActionPreference = 'Continue'
    $global:LASTEXITCODE = 0
    $lines = [Collections.Generic.List[string]]::new()
    & $Action 2>&1 | ForEach-Object {
        $line = $_.ToString()
        $lines.Add($line)
        Write-RunLine $line
    }
    $code = $LASTEXITCODE
    @("PS> $Command", '', $lines.ToArray()) | Set-Content -LiteralPath $path -Encoding UTF8
    if ($code -ne 0 -or ($lines -match '^.*:\d+:(InvalidRequest|SyntaxException|Unauthorized|ConfigurationException|AlreadyExists)')) {
        throw "$StageName failed (exit $code). See $path"
    }
}

function Start-Worker([string]$Suffix, [string[]]$Command) {
    $name = "$project-$Suffix"
    $workers.Add($name)
    Compose run -d -T --name $name lab @Command
    if ($LASTEXITCODE -ne 0) { throw "Could not start $name" }
}

function Wait-Worker([string]$Suffix) {
    $name = "$project-$Suffix"
    $code = docker wait $name
    if ($LASTEXITCODE -ne 0 -or $code -ne '0') {
        docker logs $name
        throw "$name exited with status $code"
    }
}

function Wait-Services {
    $deadline = (Get-Date).AddMinutes(8)
    do {
        $ErrorActionPreference = 'Continue'
        Kafka kafka-topics --bootstrap-server $broker --list 2>$null | Out-Null
        $kafkaReady = $LASTEXITCODE -eq 0
        Cassandra cqlsh -e 'DESCRIBE KEYSPACES' 2>$null | Out-Null
        $cassandraReady = $LASTEXITCODE -eq 0
        if ($kafkaReady -and $cassandraReady) { return }
        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)
    throw 'Kafka or Cassandra did not become ready.'
}

function Complete-StreamingChecks {
    foreach ($topicName in @('energy.raw','energy.aggregated','energy.anomalies','energy.hourly')) {
        $capture = "04-$topicName"
        $capturePath = Join-Path $evidence "$capture.txt"
        if ($ResumeRun -and (Test-Path -LiteralPath $capturePath) -and
            (Select-String -LiteralPath $capturePath -Pattern 'Processed a total of 3 messages' -Quiet)) {
            continue
        }
        Save-Stage $capture "kafka-console-consumer --topic $topicName --from-beginning --max-messages 3" {
            Kafka kafka-console-consumer --bootstrap-server $broker --topic $topicName --from-beginning --max-messages 3 --timeout-ms 20000
        }
        if (-not (Select-String -LiteralPath $capturePath -Pattern 'Processed a total of 3 messages' -Quiet)) {
            throw "The topic $topicName did not yield three messages. See $capture.txt."
        }
    }
    Save-Stage '04-offsets' 'kafka-consumer-groups --describe --all-groups; kafka-get-offsets --topic energy.raw' {
        Kafka kafka-consumer-groups --bootstrap-server $broker --describe --all-groups
        Kafka kafka-get-offsets --bootstrap-server $broker --topic energy.raw --time -1
    }
    foreach ($workerMode in @('bytewax','raw','agg','anomaly','hourly')) {
        Save-Stage "04-log-$workerMode" "docker logs --tail 18 $project-$workerMode" {
            docker logs --tail 18 "$project-$workerMode"
        }
    }
    Save-Stage '04-queries' 'cqlsh -f practical4_streaming/queries.cql' {
        Cassandra cqlsh -f /workspace/practical4_streaming/queries.cql
    }
    Save-Stage '04-storage' 'nodetool flush energy_monitoring4; tablestats raw_readings; compactionstats' {
        Cassandra nodetool flush energy_monitoring4
        Cassandra nodetool tablestats energy_monitoring4.raw_readings
        Cassandra nodetool compactionstats
    }
    Save-Stage '04-chart' 'python -m practical4_streaming.visualize_results --region Kyiv' {
        Lab python -m practical4_streaming.visualize_results --region Kyiv --output "results/run-$runId/energy_metrics.png"
    }
    Save-Stage '04-tests' 'python -m pytest -q' { Lab python -m pytest -q }
}

try {
    if ($ResumeRun) {
        $startupAttempted = $true
        Save-Stage '04-resume' "docker compose -p $project start zookeeper cassandra kafka" {
            Compose start zookeeper cassandra kafka
        }
        Wait-Services
        Complete-StreamingChecks
        'All stages completed' | Set-Content -LiteralPath (Join-Path $evidence 'status.txt') -Encoding UTF8
        return
    }
    Save-Stage '00-docker' 'docker version; docker compose version' {
        docker version
        if ($LASTEXITCODE -eq 0) { docker compose version }
    }
    $startupAttempted = $true
    Save-Stage '00-start' "docker compose -p $project up -d zookeeper kafka cassandra" {
        Compose up -d zookeeper kafka cassandra
    }
    Save-Stage '00-build' "docker compose -p $project build lab" { Compose build lab }
    Wait-Services
    Save-Stage '01-environment' 'docker compose ps; kafka-topics --version; java -version; python --version' {
        Compose ps
        Kafka kafka-topics --version
        Kafka java -version
        Lab python --version
    }
    Save-Stage '01-topics' 'python create_topics.py; kafka-topics --list and --describe; kafka-configs --describe' {
        Lab python create_topics.py
        if ($LASTEXITCODE -ne 0) { return }
        Kafka kafka-topics --bootstrap-server $broker --list
        Kafka kafka-topics --bootstrap-server $broker --describe --topic $topic
        Kafka kafka-configs --bootstrap-server $broker --entity-type topics --entity-name $topic --describe
    }

    # Keep plain-text commands and perf records away from the JSON application topic.
    $consoleTopic = 'power-station-console-zorenko'
    $perfTopic = 'power-station-perf-zorenko'
    foreach ($name in @($consoleTopic, $perfTopic)) {
        Kafka kafka-topics --bootstrap-server $broker --create --topic $name --partitions 3 --replication-factor 1
        if ($LASTEXITCODE -ne 0) { throw "Failed to create $name" }
    }
    Save-Stage '01-console-producer' "3 plain-text readings | docker compose exec -T kafka kafka-console-producer --bootstrap-server $broker --topic $consoleTopic --producer-property client.id=console-producer-zorenko" {
        $readings = 'TPP_KYIV_01: 1200 MW', 'HPP_DNIPRO_01: 800 MW', 'SOLAR_NIKOPOL_01: 200 MW'
        $readings | Write-Output
        $readings | docker compose -p $project exec -T kafka kafka-console-producer --bootstrap-server $broker --topic $consoleTopic --producer-property client.id=console-producer-zorenko
    }
    Save-Stage '01-console-consumer' "kafka-console-consumer --topic $consoleTopic --from-beginning --max-messages 3" {
        Kafka kafka-console-consumer --bootstrap-server $broker --topic $consoleTopic --from-beginning --max-messages 3 --timeout-ms 20000 --consumer-property client.id=console-consumer-zorenko
    }
    Save-Stage '01-python-producer' 'docker compose run --rm lab python -m practical1_kafka.producer --count 150 --interval 0.03' {
        Lab python -m practical1_kafka.producer --count 150 --interval 0.03
    }
    Save-Stage '01-python-consumer' 'practical1_kafka.consumer with the same group twice, then a new group' {
        foreach ($consumerGroup in @($group, $group, "$group-replay")) {
            Write-Output "Group: $consumerGroup"
            Lab python -m practical1_kafka.consumer --max-messages 6 --group $consumerGroup
            if ($LASTEXITCODE -ne 0) { return }
        }
    }
    Save-Stage '01-group' "kafka-consumer-groups --describe --group $group" {
        Kafka kafka-consumer-groups --bootstrap-server $broker --describe --group $group
    }
    Save-Stage '01-perf-producer' 'kafka-producer-perf-test (10000 records, 1024 bytes, 1000 records/s cap, gzip)' {
        Kafka kafka-producer-perf-test --topic $perfTopic --num-records 10000 --record-size 1024 --throughput 1000 --producer-props "bootstrap.servers=$broker" compression.type=gzip
    }
    Save-Stage '01-perf-consumer' 'kafka-consumer-perf-test (10000 records)' {
        Kafka kafka-consumer-perf-test --bootstrap-server $broker --topic $perfTopic --messages 10000
    }
    Save-Stage '01-monitoring' 'docker compose logs --tail 20 kafka; kafka-log-dirs --describe' {
        Compose logs --tail 20 kafka
        Kafka kafka-log-dirs --bootstrap-server $broker --describe --topic-list $topic
    }

    Save-Stage '02-environment' 'nodetool status; cqlsh --version' { Cassandra nodetool status; Cassandra cqlsh --version }
    Save-Stage '02-schema' 'cqlsh -f practical2_cassandra/schema.cql; DESCRIBE TABLES; system_schema.indexes' {
        Cassandra cqlsh -f /workspace/practical2_cassandra/schema.cql
        if ($LASTEXITCODE -ne 0) { return }
        Cassandra cqlsh -k energy_monitoring -e "DESCRIBE TABLES; SELECT index_name,table_name FROM system_schema.indexes WHERE keyspace_name='energy_monitoring';"
    }
    Save-Stage '02-queries' 'cqlsh -f practical2_cassandra/seed.cql and queries.cql' {
        Cassandra cqlsh -f /workspace/practical2_cassandra/seed.cql
        if ($LASTEXITCODE -ne 0) { return }
        Cassandra cqlsh -f /workspace/practical2_cassandra/queries.cql
    }
    Save-Stage '02-crud' 'CQL UPDATE, alert resolution by TIMEUUID, COUNTER +1 and DELETE of a demo row' {
        Lab python -m practical2_cassandra.maintenance --resolve
        if ($LASTEXITCODE -ne 0) { return }
        $crud = @'
SELECT alert_id,acknowledged,resolved FROM station_alerts WHERE station_id=123e4567-e89b-12d3-a456-426614174000 AND alert_date='2025-09-17';
UPDATE power_stations SET status='maintenance',updated_at=toTimestamp(now()) WHERE station_id=323e4567-e89b-12d3-a456-426614174002;
INSERT INTO stations_by_capacity (station_status,max_capacity_mw,station_id,station_name,station_type) VALUES ('maintenance',246.0,323e4567-e89b-12d3-a456-426614174002,'Solar Nikopol','solar');
DELETE FROM stations_by_capacity WHERE station_status='active' AND max_capacity_mw=246.0 AND station_id=323e4567-e89b-12d3-a456-426614174002;
SELECT station_name,status FROM power_stations WHERE station_id=323e4567-e89b-12d3-a456-426614174002;
SELECT station_name,max_capacity_mw FROM stations_by_capacity WHERE station_status='active';
UPDATE hourly_alert_counts SET alert_count=alert_count+1 WHERE station_id=123e4567-e89b-12d3-a456-426614174000 AND date='2025-09-17' AND hour='2025-09-17 14:00:00+0000';
SELECT alert_count FROM hourly_alert_counts WHERE station_id=123e4567-e89b-12d3-a456-426614174000 AND date='2025-09-17';
INSERT INTO power_stations (station_id,station_name,status) VALUES (923e4567-e89b-12d3-a456-426614174009,'Temporary lab row','test');
SELECT station_name FROM power_stations WHERE station_id=923e4567-e89b-12d3-a456-426614174009;
DELETE FROM power_stations WHERE station_id=923e4567-e89b-12d3-a456-426614174009;
SELECT station_name FROM power_stations WHERE station_id=923e4567-e89b-12d3-a456-426614174009;
'@
        Cassandra cqlsh -k energy_monitoring -e $crud
    }
    Save-Stage '02-storage' 'nodetool flush energy_monitoring; nodetool tablestats energy_monitoring.station_telemetry; nodetool compactionstats' {
        Cassandra nodetool flush energy_monitoring
        Cassandra nodetool tablestats energy_monitoring.station_telemetry
        Cassandra nodetool compactionstats
    }

    Save-Stage '03-schema' 'cqlsh -f practical3_optimization/schema.cql; DESCRIBE TABLE telemetry_bad and telemetry_hourly' {
        Cassandra cqlsh -f /workspace/practical3_optimization/schema.cql
        if ($LASTEXITCODE -ne 0) { return }
        Cassandra cqlsh -k demo_practice -e 'DESCRIBE TABLE telemetry_bad; DESCRIBE TABLE telemetry_hourly;'
    }
    Save-Stage '03-generate' "python -m practical3_optimization.generate --days $HistoryDays --step-minutes 1" {
        Lab python -m practical3_optimization.generate --days $HistoryDays --step-minutes 1
    }
    Copy-Item results/p3_generation_metadata.json -Destination $evidence
    $metadata = Get-Content results/p3_generation_metadata.json -Raw | ConvertFrom-Json
    $end = [DateTimeOffset]::Parse($metadata.end_hour_utc)
    $hour = $end.AddHours(-1).ToString('yyyy-MM-dd HH:mm:ss+0000')
    $upper = $end.ToString('yyyy-MM-dd HH:mm:ss+0000')
    $device = $metadata.device
    foreach ($table in @('telemetry_bad', 'telemetry_hourly')) {
        Save-Stage "03-before-$table" "nodetool tablestats demo_practice.$table before flush" {
            Cassandra nodetool tablestats "demo_practice.$table"
        }
    }
    Save-Stage '03-flush' 'nodetool flush demo_practice' { Cassandra nodetool flush demo_practice }
    foreach ($table in @('telemetry_bad', 'telemetry_hourly')) {
        Save-Stage "03-after-$table" "nodetool tablestats demo_practice.$table after flush" {
            Cassandra nodetool tablestats "demo_practice.$table"
        }
        $filter = "device_id='$device' AND recorded_at>='$hour' AND recorded_at<'$upper'"
        if ($table -eq 'telemetry_hourly') { $filter = "$filter AND bucket_hour='$hour'" }
        $trace = "TRACING ON; SELECT COUNT(*) FROM $table WHERE $filter; TRACING OFF;"
        Save-Stage "03-trace-$table" "cqlsh -k demo_practice -e $trace" { Cassandra cqlsh -k demo_practice -e $trace }
    }
    Save-Stage '03-partitions' 'python -m practical3_optimization.analyze_partitions (using the two post-flush tablestats logs)' {
        $mount = "results/run-$runId"
        Lab python -m practical3_optimization.analyze_partitions --bad "$mount/03-after-telemetry_bad.txt" --good "$mount/03-after-telemetry_hourly.txt" --outfile "$mount/p3_partition_comparison.csv"
    }
    Save-Stage '03-benchmark' 'python -m practical3_optimization.benchmark --iterations 50' {
        Lab python -m practical3_optimization.benchmark --iterations 50 --outfile "results/run-$runId/p3_benchmark.csv"
    }

    Save-Stage '04-environment' 'Python, Bytewax and Kafka topic configuration' {
        Lab python -m pip show bytewax kafka-python cassandra-driver confluent-kafka
        foreach ($name in @('energy.raw','energy.aggregated','energy.anomalies','energy.hourly')) {
            Kafka kafka-topics --bootstrap-server $broker --describe --topic $name
            if ($LASTEXITCODE -ne 0) { return }
        }
    }
    Save-Stage '04-schema' 'cqlsh -f practical4_streaming/schema.cql and DESCRIBE TABLES' {
        Cassandra cqlsh -f /workspace/practical4_streaming/schema.cql
        if ($LASTEXITCODE -ne 0) { return }
        Cassandra cqlsh -k energy_monitoring4 -e 'DESCRIBE TABLES;'
    }
    Save-Stage '04-workers' 'Start Bytewax (2 workers) and four Cassandra writer containers' {
        Start-Worker 'bytewax' @('python','-m','bytewax.run','practical4_streaming.energy_stream_processor:flow','-w','2')
        foreach ($mode in @('raw','agg','anomaly','hourly')) {
            Start-Worker $mode @('python','-m','practical4_streaming.cassandra_writer',$mode)
        }
        Compose ps
    }
    Save-Stage '04-hour-fixture' 'Simulator, 20 meters for 3 s, event times shifted to the previous UTC hour' {
        Lab python -m practical4_streaming.smart_meter_simulator --meters 20 --duration 3 --event-time-offset-seconds -3600
    }
    Start-Worker 'probe' @('python','-m','practical4_streaming.performance_probe','--count','1000','--timeout-ms','25000','--output',"results/run-$runId/p4_probe.csv")
    Start-Sleep -Seconds 8
    Save-Stage '04-live-load' 'Simulator, 200 meters, 0.1 s interval, 15 s, 10% injected anomalies' {
        Lab python -m practical4_streaming.smart_meter_simulator --meters 200 --interval 0.1 --duration 15 --anomaly-rate 0.10
    }
    Wait-Worker 'probe'
    Save-Stage '04-probe' 'Kafka consumer measurement for live events' { docker logs "$project-probe" }
    Save-Stage '04-live-baseline' 'Simulator, 20 meters, 1 s interval, 135 s, 10% injected anomalies' {
        Lab python -m practical4_streaming.smart_meter_simulator --meters 20 --interval 1 --duration 135 --anomaly-rate 0.10
    }
    Complete-StreamingChecks
    'All stages completed' | Set-Content -LiteralPath (Join-Path $evidence 'status.txt') -Encoding UTF8
}
catch {
    $_.ToString() | Set-Content -LiteralPath (Join-Path $evidence 'status.txt') -Encoding UTF8
    throw
}
finally {
    if ($startupAttempted) {
        foreach ($name in $workers) {
            docker stop --time 5 $name | Out-Null
        }
        Save-Stage '00-stop' "docker compose -p $project stop" { Compose stop -t 25 }
    }
    Write-RunLine "`nEvidence: $evidence"
}
