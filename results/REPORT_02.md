# Практична робота №2

**Тема:** Apache Cassandra — зберігання енергетичних даних.  
**Виконав:** [ПІБ, група]  
**Дата:** [дата]

## Мета

Засвоїти основи CQL і моделювання даних для Cassandra: keyspace, partition/clustering key, таблиці, індекси, INSERT/SELECT/UPDATE/DELETE, часові ряди, лічильники та адміністрування.

## Програмне середовище та підготовка

У Docker розгортається одно-вузлова Cassandra 4.1; всередині контейнера застосовуються `cqlsh` і `nodetool`. Реальні версії та конфігурація після запуску: [вказати]. Точні інструкції: `guides/02_CASSANDRA.md`.

## Теоретичні відомості

Cassandra — розподілена NoSQL система, де keyspace задає параметри реплікації. Partition key визначає місце розташування й групування даних, clustering key упорядковує рядки всередині партиції. Запити за повним partition key ефективні, а запити без нього можуть вимагати додаткових індексів або окремих денормалізованих таблиць. TTL задає термін зберігання атрибутів від моменту запису. Дані спочатку надходять до commit log і memtable, а після flush зберігаються у SSTables.

## Хід роботи

### 0. Установлення та перевірка сервісу

Підготовлено `docker-compose.yml`; команди: `docker compose up -d cassandra`, `docker compose exec -T cassandra nodetool status`, `docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES"`. **Рисунок 1 — Cassandra UN, доступний cqlsh**: [вставити].

### 1. Створення keyspace і таблиць

Файл `practical2_cassandra/schema.cql` створює keyspace `energy_monitoring` та таблиці `power_stations`, `station_telemetry`, `hourly_statistics`, `hourly_alert_counts`, `station_alerts`, `stations_by_capacity`. Для одновузлового середовища використовується replication factor 1. Для збереження телеметрії застосовано складений ключ `((station_id,time_bucket),recorded_at)` і TTL 7 днів. Додано відсутній у прикладі методички `CREATE TABLE station_alerts`. **Рисунок 2 — `DESCRIBE KEYSPACE`**, **Рисунок 3 — `DESCRIBE TABLES`**: [вставити].

### 2. Індекси

Створено індекси `station_type_idx`, `station_region_idx`, `station_status_idx`, `alert_level_idx`, `station_alerts_severity_idx`. Перевірка: `SELECT index_name,table_name FROM system_schema.indexes WHERE keyspace_name='energy_monitoring';`. **Рисунок 4 — список індексів**: [вставити].

### 3. Наповнення та CRUD

`seed.cql` містить три навчальні станції, чотири заміри та одне попередження. Це умовні дані для відтворення структури, а не вимірювання реальних станцій. `queries.cql` містить пошук за ключами, індексами та запити за часовими інтервалами. `maintenance.py` демонструє зміну статусу попередження **за наявним TIMEUUID**. Команди: `docker compose exec -T cassandra cqlsh -f /workspace/practical2_cassandra/seed.cql`, `docker compose exec -T cassandra cqlsh -f /workspace/practical2_cassandra/queries.cql`. **Рисунок 5 — три станції**, **Рисунок 6 — телеметрія**, **Рисунок 7 — повідомлення про тривогу**, **Рисунок 8 — UPDATE/DELETE і лічильник**: [вставити].

### 4. Аналітичні запити

Показано пошук за регіоном/типом, топ електростанцій за встановленою потужністю, середні показники всередині обмеженої часової партиції та готові годинні агрегати. Для топу використано `stations_by_capacity` із clustering order DESC; `LIMIT 5` у довільній таблиці сам по собі не означає п'ять найбільших значень. **Рисунок 9 — запити/результати**: [вставити].

### 5. TTL, counters, token

Перевіряються `TTL(power_output_mw)`, `UPDATE hourly_alert_counts SET alert_count=alert_count+1...`, `token(station_id)`. Термін TTL у секундах залежить від часу, що минув від запису. Повторення інкременту COUNTER змінить результат, тому цю команду потрібно запускати свідомо. **Рисунок 10 — TTL, counter, token**: [вставити].

### 6. Обслуговування

`nodetool flush energy_monitoring`, `nodetool tablestats energy_monitoring.station_telemetry`, `nodetool compactionstats`, `nodetool info`. **Рисунок 11 — таблиця статистики SSTable/партицій**: [вставити].

## Результати після фактичного запуску

| Вимір / перевірка | Результат |
|---|---|
| Стан вузла (UN?) | [фактично] |
| Створені таблиці | [вивід `DESCRIBE TABLES`] |
| Кількість станцій | [фактично] |
| Кількість телеметричних записів | [фактично] |
| Кількість alerts | [фактично] |
| TTL, секунд | [фактично] |
| SSTable count | [з `nodetool tablestats`] |
| Space used (live), байт | [з `nodetool tablestats`] |

## Висновок

Підготовлено CQL-схему енергетичної системи з денормалізованими таблицями під різні доступи та скрипти для створення тестових даних. Порівняння індексованих запитів і запитів за повним ключем, а також фізичні характеристики SSTable необхідно задокументувати на основі **власного фактичного запуску**. Застосована схема погодинного партиціонування використовується в наступній практичній.
