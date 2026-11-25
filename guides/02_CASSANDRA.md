# Практична №2 — Apache Cassandra: енергетична система

**Мета:** побудувати NoSQL-модель електростанцій, часового ряду показників, попереджень та агрегованих даних; попрактикуватися з keyspace, CQL, secondary indexes, TTL, CRUD, counter, аналізом фізичних характеристик таблиць.

## Етап 0. Перевірка Cassandra

Продовжуйте після практичної №1; відкрийте PowerShell у корені проєкту:

```powershell
docker compose up -d cassandra
docker compose ps
docker compose logs --tail 60 cassandra
docker compose exec -T cassandra nodetool status
docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES"
```

Початкове завантаження Cassandra триває довше за Kafka, тому спочатку дочекайтеся статусу `healthy`. У `nodetool status` для одного вузла має бути **UN** (Up/Normal). Якщо немає підключення до CQL, подивіться `docker compose logs -f cassandra` і повторіть після готовності.

У документах наведено ручне встановлення Java та Cassandra для Windows, Linux і macOS. У цьому варіанті Java вже входить в Docker-образ Cassandra, а утиліти `cqlsh`/`nodetool` запускаються всередині контейнера. Це функціонально виконує завдання без залежностей системного Java на Windows.

## Етап 1. Створення keyspace та таблиць

Схема знаходиться у `practical2_cassandra/schema.cql`; вона вже змонтована в Cassandra-контейнер у `/workspace/`.

```powershell
docker compose exec -T cassandra cqlsh -f /workspace/practical2_cassandra/schema.cql
docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACE energy_monitoring"
docker compose exec cassandra cqlsh
```

У відкритому `cqlsh` виконайте:

```sql
USE energy_monitoring;
DESCRIBE TABLES;
DESCRIBE TABLE power_stations;
DESCRIBE TABLE station_telemetry;
DESCRIBE TABLE station_alerts;
DESCRIBE TABLE hourly_statistics;
DESCRIBE TABLE hourly_alert_counts;
DESCRIBE TABLE stations_by_capacity;
```

Структура:

| Таблиця | Ключ | Призначення |
|---|---|---|
| `power_stations` | `station_id` | Довідник електростанцій |
| `station_telemetry` | `((station_id, time_bucket), recorded_at)` | Вимірювання по станції та годині, 7-денний TTL |
| `station_alerts` | `((station_id, alert_date), alert_id)` | Попередження, унікальний TIMEUUID |
| `hourly_statistics` | `((station_id, date), hour)` | Погодинні підсумки |
| `hourly_alert_counts` | `((station_id, date), hour)` | Лічильник подій з `COUNTER` |
| `stations_by_capacity` | `(station_status, max_capacity_mw, station_id)` | Реальне сортування станцій за потужністю |

**Виправлення методички:** таблиця `station_alerts` використовується в INSERT/SELECT/UPDATE, але немає повного CREATE TABLE; у цій реалізації створена явно. Для «топ-5 за потужністю» `LIMIT 5` на `power_stations` не гарантує вибір найбільших станцій; застосовано спеціальну таблицю з clustering DESC. Для одного вузла достатньо `SimpleStrategy` + RF1.

## Етап 2. Створення індексів

Вони вже включені у `schema.cql`:

- `station_type_idx` по типу станції;
- `station_region_idx` по регіону;
- `station_status_idx` по статусу;
- `alert_level_idx` по рівню телеметричних тривог;
- `station_alerts_severity_idx` по severity попереджень.

Перевірте:

```sql
DESCRIBE TABLE power_stations;
DESCRIBE TABLE station_telemetry;
SELECT index_name, table_name FROM system_schema.indexes WHERE keyspace_name='energy_monitoring';
```

Це навчальний демонстраційний випадок: secondary indexes можуть бути дорогими при широкому розподілі даних. Для основних high-load запитів бажано використовувати спеціальні таблиці під конкретний access pattern.

## Етап 3. Seed-дані та CRUD

Завершіть інтерактивний `cqlsh` командою `EXIT;`. Запустіть заповнення таблиць:

```powershell
docker compose exec -T cassandra cqlsh -f /workspace/practical2_cassandra/seed.cql
```

Цей файл створює три **умовні навчальні** електростанції, чотири вимірювання телеметрії, одне попередження та збережену погодинну статистику. Дати відтворюють наданий приклад, тому результати запитів за `2025-09-17` залишаться передбачуваними. Cassandra застосовує TTL від часу запису до БД, а не від дати в колонці `recorded_at`.

Запитайте:

```powershell
docker compose exec -T cassandra cqlsh -f /workspace/practical2_cassandra/queries.cql
```

Файл містить усі головні SELECT, зокрема пошук за PK, вторинним індексом, найновіші вимірювання за годину, `AVG` над обмеженою партицією, попередження, часові ряди, токени та TTL. Для швидкої перевірки кількості записів:

```powershell
docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM energy_monitoring.power_stations;"
docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM energy_monitoring.station_telemetry;"
docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM energy_monitoring.station_alerts;"
```

За одноразового застосування seed до порожнього keyspace очікується 3, 4 та 1 логічні записи відповідно. Якщо таблиці існували раніше, результат може відрізнятися.

### Додаткові CRUD-операції вручну

```sql
USE energy_monitoring;
-- READ: останні вимірювання
SELECT * FROM station_telemetry
WHERE station_id=123e4567-e89b-12d3-a456-426614174000
AND time_bucket='2025-09-17 14:00:00+0000' LIMIT 5;

-- UPDATE: перевести сонячну станцію в обслуговування
UPDATE power_stations SET status='maintenance', updated_at=toTimestamp(now())
WHERE station_id=323e4567-e89b-12d3-a456-426614174002;

-- Повернути початковий статус (для повторної перевірки індексу)
UPDATE power_stations SET status='active', updated_at=toTimestamp(now())
WHERE station_id=323e4567-e89b-12d3-a456-426614174002;

-- COUNTER: цей крок запускати ОДИН раз (не ідемпотентний)
UPDATE hourly_alert_counts SET alert_count=alert_count+1
WHERE station_id=123e4567-e89b-12d3-a456-426614174000
AND date='2025-09-17' AND hour='2025-09-17 14:00:00+0000';
SELECT * FROM hourly_alert_counts
WHERE station_id=123e4567-e89b-12d3-a456-426614174000 AND date='2025-09-17';
```

Для демонстрації **DELETE** краще створити окремий тестовий об'єкт і видалити його:

```sql
INSERT INTO power_stations (station_id,station_name,status)
VALUES (423e4567-e89b-12d3-a456-426614174003,'Тестова станція','test');
SELECT station_name FROM power_stations WHERE station_id=423e4567-e89b-12d3-a456-426614174003;
DELETE FROM power_stations WHERE station_id=423e4567-e89b-12d3-a456-426614174003;
SELECT station_name FROM power_stations WHERE station_id=423e4567-e89b-12d3-a456-426614174003;
```

**Важлива деталь `TIMEUUID`:** приклад методички пропонує UPDATE з `alert_id = now()`, але це створює **новий** UUID і не оновлює потрібний рядок. Для правильної операції використовуйте `practical2_cassandra/maintenance.py`:

```powershell
docker compose run --rm lab python -m practical2_cassandra.maintenance
docker compose run --rm lab python -m practical2_cassandra.maintenance --resolve
```

Скрипт спочатку читає точний `alert_id`, потім змінює лише наявний запис. Зафіксуйте різницю на скриншотах до/після. За наявності денормалізованих проєкцій зміна статусу станції також потребує оновлення відповідних проєкцій у реальному застосунку.

## Етап 4. Аналітика

Виконайте SELECT з `queries.cql`. Поясніть у звіті:

1. `SELECT ... WHERE station_id=... AND time_bucket=... LIMIT 10` читає конкретну партицію та користується clustering DESC.
2. `AVG` та `COUNT` над **однією** партицією з малим обсягом придатні для демонстрації, але для масштабної аналітики краще використовувати `hourly_statistics`.
3. `stations_by_capacity WHERE station_status='active' LIMIT 5` повертає найбільші потужності завдяки `CLUSTERING ORDER BY (max_capacity_mw DESC)`.
4. Запит без повного partition key з `ALLOW FILTERING` на великій таблиці може вимагати сканування великого обсягу даних. Не вимірюйте таким запитом продуктивність production-системи.

## Етап 5. Обслуговування та фізичні метрики

```powershell
docker compose exec -T cassandra nodetool status
docker compose exec -T cassandra nodetool info
docker compose exec -T cassandra nodetool flush energy_monitoring
docker compose exec -T cassandra nodetool tablestats energy_monitoring.station_telemetry
docker compose exec -T cassandra nodetool compactionstats
docker compose exec -T cassandra cqlsh -e "SELECT TTL(power_output_mw) FROM energy_monitoring.station_telemetry LIMIT 1;"
```

`flush` записує memtable у SSTable, тож після нього можна побачити `SSTable count`, `Space used (live)`, статистику партицій. `TTL` — кількість секунд, що залишилися до логічного завершення терміну життя значення. Число токенів від `token(station_id)` можна дослідити навіть в одно-вузловому середовищі, але **рівномірність реального розподілення по вузлах** за одного вузла не перевіряється.

## Етап 6. Завершення й докази

Зберіть скриншоти: `nodetool status`, `DESCRIBE KEYSPACE`, `DESCRIBE TABLES`, SELECT трьох станцій, результат телеметрії, popередження, вторинний індекс, UPDATE до/після, COUNTER, TTL, `nodetool tablestats`. Для звіту використайте `results/REPORT_02.md`. **Не зупиняйте Cassandra перед практичною №3.**
