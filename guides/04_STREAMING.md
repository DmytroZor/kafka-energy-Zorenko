# Практична №4 — Real-time моніторинг енергетичних даних: Kafka, Bytewax, Cassandra

**Мета:** розробити наскрізну потокову систему розумних лічильників з детекцією аномалій, 1-хвилинною агрегацією за регіонами, часовими рядами в Cassandra, пакетним читанням Kafka, TTL та перевіркою навантаження. Джерело: `Практика 4 Kafka Streams Concepts.pdf` (шість завдань у «Хід роботи»).

## Архітектура (що саме потрібно запустити)

```text
20+ симульованих лічильників (Python)
          |
          |  JSON, key=meter_id, UTC timestamp (мс)
          v
      Kafka topic energy.raw  (4 partitions, TTL 7d)
          |                           \
          |                            \-> Cassandra Writer raw -> raw_readings (hourly bucket)
          v
   Bytewax streaming dataflow (key_on(region), stateful folds)
          |
          +--> detect_anomaly ---> Kafka energy.anomalies ---> Writer anomaly
          |                                               |-> anomalies
          |                                               |-> anomalies_by_severity
          |
          +--> 60s tumbling window ---> Kafka energy.aggregated ---> Writer agg
          |                                                |-> regional_metrics
          |
          +--> 3600s tumbling window --> Kafka energy.hourly ----> Writer hourly
                                                           |-> hourly_stats
```

**Розподіл ролей:** Kafka отримує/зберігає події; Bytewax обробляє їх зі станом і перерозподіляє за ключем `region`; Cassandra Writer читає різні топіки окремими групами; Cassandra зберігає дані під конкретні типи запитів. Наявність чотирьох партицій у топіку дозволяє масштабувати споживання, але кількість партицій сама по собі не гарантує чотири одночасні фізичні процеси.

## Завдання 1. Підготовка середовища та топіків

У PowerShell у корені проєкту:

```powershell
docker compose up -d zookeeper kafka cassandra
docker compose ps
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
docker compose exec -T cassandra nodetool status
docker compose run --rm lab python create_topics.py
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic energy.raw
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic energy.aggregated
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic energy.anomalies
```

Очікуються `energy.raw` — 4 partitions, retention 7 днів; `energy.aggregated` — 4 partitions, 30 днів; `energy.anomalies` — 2 partitions, 90 днів. Додано `energy.hourly` для зберігання годинних підсумків. `create_topics.py` можна повторювати: наявні топіки воно пропускає. Для підтвердження бібліотек:

```powershell
docker compose run --rm lab python -c "import bytewax, kafka, cassandra, confluent_kafka; print('Python dependencies OK')"
```

На Windows **не потрібно** окремо запускати ZooKeeper чи налаштовувати `JAVA_HOME`: ці дії вже виконані Docker-образами Kafka/Cassandra, а Python/Bytewax працює у `lab`-контейнері.

## Завдання 2. Cassandra schema

```powershell
docker compose exec -T cassandra cqlsh -f /workspace/practical4_streaming/schema.cql
docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACE energy_monitoring4"
docker compose exec -T cassandra cqlsh -e "DESCRIBE TABLE energy_monitoring4.raw_readings;"
```

У keyspace `energy_monitoring4` створюються:

- `raw_readings`: `PRIMARY KEY ((meter_id,hour_bucket),reading_time)` і `region STATIC`, TWCS, LZ4, TTL 7 днів;
- `regional_metrics`: хвилинні агреговані метрики в межах `region`, TTL 30 днів;
- `anomalies`: події за `(region,hour_bucket)`, TTL 90 днів;
- `anomalies_by_severity`: денормалізована таблиця за `(severity,hour_bucket)`, TTL 90 днів;
- `hourly_stats`: історична погодинна статистика по `(region,date)`.

**Відмінність від PDF:** документ вказує `NetworkTopologyStrategy, datacenter1:3`, що на одному вузлі неможливо повноцінно реалізувати. Тут вказано RF=1 у `datacenter1`; для 3 реальних вузлів відновіть RF=3. У PDF показано Materialized View для аномалій, але Materialized Views у Cassandra мають конфігураційні та експлуатаційні обмеження. Основне рішення застосовує дві звичайні таблиці, які Writer оновлює послідовно; окремий файл `materialized_view_optional.cql` пояснює альтернативний MV-випадок. **Ніколи не виконуйте `DROP TABLE` над даними без резервної копії.**

## Завдання 3. IoT-симулятор

Файл: `practical4_streaming/smart_meter_simulator.py`. За замовчуванням створює 20 пристроїв `SM-0000`... `SM-0019` у п'яти регіонах, кожну секунду формує `timestamp`, `region`, `voltage`, `current`, `power`, `frequency`, `temperature`, `power_factor`, `sample_interval_sec` та відправляє в `energy.raw`. Приблизно 10% повідомлень мають контрольовані відхилення (перенапруга або сплеск струму). Це **синтетичні дані**, а не показники реальних електростанцій.

Спочатку перевірте симулятор окремо коротким запуском:

```powershell
docker compose run --rm lab python -m practical4_streaming.smart_meter_simulator --meters 5 --duration 10 --interval 1
```

Потім подивіться перші п'ять JSON-повідомлень:

```powershell
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic energy.raw --from-beginning --max-messages 5
```

Очікування: у JSON присутній `meter_id`, часовий штамп у мілісекундах UTC та числові виміри. Зробіть скриншот симулятора і JSON у Kafka. Основний довгий запуск потрібно виконати **лише після запуску процесора та writer-ів** (наступні завдання).

## Завдання 4. Bytewax Stream Processing

Основний файл `practical4_streaming/energy_stream_processor.py` використовує **офіційні конектори** `bytewax.connectors.kafka` версії Bytewax 0.21.1. Потік виконує `input → decode → validate`, після чого поділяється на три гілки: миттєва детекція аномалій, keyed 60-секундні вікна та keyed 3600-секундні вікна. `fold_window` тримає окремий стан `(region, window)`, `EventClock` бере час із поля `timestamp`; 5 секунд — допуск на перестановку/затримку подій.

У **новому PowerShell (Термінал A)** запустіть:

```powershell
docker compose run --rm lab python -m bytewax.run practical4_streaming.energy_stream_processor:flow
```

Залиште працювати. Альтернатива для досліду паралелізму (замість, а не разом із попереднім процесором):

```powershell
docker compose run --rm lab python -m bytewax.run practical4_streaming.energy_stream_processor:flow -w 2
```

**Критично:** не запускайте `python practical4_streaming/energy_stream_processor.py`, оскільки цей файл лише **оголошує Dataflow**. Bytewax запускається через `python -m bytewax.run ...:flow`. Звичайний Python-словник/спільний singleton стану з різних процесів не гарантує коректної агрегації; `fold_window` розподіляє керований стан за ключами всередині Bytewax.

**Аномалії:** напруга поза 200–240 V → warning або critical при нижче 190 чи вище 250; струм більше 60 A → warning (більше 90 A → critical); частота поза 49.5–50.5 Hz → warning. `deviation_score` — визначена в програмі проста міра відхилення, а **не** статистичний Z-score. Правила послідовні: при одночасному порушенні кількох параметрів формується одна подія за пріоритетним правилом.

**Агрегація:** 60-секундне tumbling window за регіоном; `avg_voltage`, `max_voltage`, `min_voltage`, `total_power`, `avg_power`, `active_meters` (кількість унікальних `meter_id`), `readings_count`. `total_power` тут — **сума значень W за всі вимірювання у вікні**, її не слід плутати з енергією в кВт·год. У годинному потоці `total_energy` вимірюється в кВт·год на основі інтервалу дискретизації.

## Завдання 5. Cassandra Writer: чотири режими

Код: `practical4_streaming/cassandra_writer.py`. Він читає записи з Kafka пачками по 100, записує їх у відповідні таблиці через prepared statements і лише потім фіксує Kafka offsets. Запити до різних партицій Cassandra не загортаються в один великий `BEGIN BATCH` — це небезпечний для продуктивності antipattern. Повторне доставлення може відбутися, але первинні ключі зроблено стабільними, тому звичайні повторні INSERT перезаписують відповідні рядки.

Відкрийте **окремі чотири PowerShell-термінали** й запустіть по одній команді:

**Термінал B — raw writer**

```powershell
docker compose run --rm lab python -m practical4_streaming.cassandra_writer raw
```

**Термінал C — aggregates writer**

```powershell
docker compose run --rm lab python -m practical4_streaming.cassandra_writer agg
```

**Термінал D — anomalies writer**

```powershell
docker compose run --rm lab python -m practical4_streaming.cassandra_writer anomaly
```

**Термінал E — hourly writer**

```powershell
docker compose run --rm lab python -m practical4_streaming.cassandra_writer hourly
```

Тільки після запуску всіх writer-ів відкрийте **Термінал F — основний симулятор**:

```powershell
docker compose run --rm lab python -m practical4_streaming.smart_meter_simulator --meters 20 --duration 135 --interval 1 --anomaly-rate 0.10
```

Дочекайтесь закінчення симулятора. Він має згенерувати приблизно `20 × 135 ≈ 2700` повідомлень, але точна кількість залежить від реального виконання циклу. Bytewax закриває 60-секундні вікна після просування event-time watermark: повідомлення `energy.aggregated` з'являються **не одразу після старту**, а після межі часових вікон. Аналогічно, `hourly_stats` може лишитися порожньою після 135 секунд, якщо жодне повне годинне вікно ще не завершилося. Це не є помилкою програми.

У новому терміналі перевірте дані Kafka:

```powershell
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic energy.aggregated --from-beginning --max-messages 3
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic energy.anomalies --from-beginning --max-messages 3
```

Якщо нових агрегатів не видно, огляньте журнали термінала з Bytewax; за потреби запустіть симулятор ще на 75 секунд, щоб події перетнули наступну часову межу. Після запису даних вимкніть процесор і writer-ів через Ctrl+C у відповідних терміналах.

## Завдання 6. Перевірка в Cassandra та аналіз

```powershell
docker compose exec -T cassandra cqlsh -f /workspace/practical4_streaming/queries.cql
docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM energy_monitoring4.raw_readings;"
docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM energy_monitoring4.regional_metrics;"
docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM energy_monitoring4.anomalies;"
docker compose exec -T cassandra cqlsh -e "SELECT * FROM energy_monitoring4.regional_metrics WHERE region='Kyiv' LIMIT 5;"
```

Для читання одного пристрою за годину й критичних аномалій за годину потрібно підставити **дійсні значення `hour_bucket` у UTC**, а не застарілі жовтневі дати з PDF. Знайти годинні бакети можна через `SELECT meter_id,hour_bucket FROM energy_monitoring4.raw_readings LIMIT 10;`. Потім:

```sql
SELECT reading_time,voltage,current,power FROM energy_monitoring4.raw_readings
WHERE meter_id='SM-0001' AND hour_bucket='2026-11-22 10:00:00+0000' LIMIT 10;

SELECT detected_at,region,meter_id,voltage FROM energy_monitoring4.anomalies_by_severity
WHERE severity='critical' AND hour_bucket='2026-11-22 10:00:00+0000' LIMIT 20;
```

**Замініть** дату прикладу на той бакет, який справді було отримано з вашого Cassandra. На відміну від наведеної в матеріалах спроби `SELECT ... WHERE severity='critical' AND detected_at>=...`, схема вимагає повного partition key `(severity,hour_bucket)`. Для всього дня повторіть запит для кожної години дня або додайте спеціальну таблицю під денний пошук.

Перевірте TTL і стан дискових структур:

```powershell
docker compose exec -T cassandra nodetool status
docker compose exec -T cassandra nodetool flush energy_monitoring4
docker compose exec -T cassandra nodetool tablestats energy_monitoring4
docker compose exec -T cassandra nodetool compactionstats
docker compose exec -T cassandra cqlsh -e "SELECT TTL(voltage) FROM energy_monitoring4.raw_readings LIMIT 1;"
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --all-groups
```

Під час самостійного виведення діаграми із **реальних** агрегованих даних:

```powershell
docker compose run --rm lab python -m practical4_streaming.visualize_results --region Kyiv
```

З'явиться `results/energy_metrics.png` із двома графіками. Якщо в `regional_metrics` ще немає записів, скрипт завершиться з поясненням, що даних недостатньо; він не підставляє штучну таблицю результатів.

### Замір Kafka throughput та event age

В окремому терміналі запустіть **перед симулятором**:

```powershell
docker compose run --rm lab python -m practical4_streaming.performance_probe --topic energy.raw --count 400 --timeout-ms 30000
```

Після цього запускайте симулятор з попереднього етапу. Скрипт надрукує events/sec та p50/p95/p99 **віку події на момент читання** з Kafka і збереже справжні виміри у `results/p4_probe.csv`. Це вимірювання Kafka consumer, **не** гарантована затримка «IoT → Cassandra». Для наближеного навантаження 1000+ подій/с спробуйте окремий експеримент:

```powershell
docker compose run --rm lab python -m practical4_streaming.smart_meter_simulator --meters 200 --duration 45 --interval 0.1
```

Теоретична швидкість генератора 2000 повідомлень/с; досягнення фактичної швидкості залежить від ресурсів. Зберіть спостереження, не видавайте параметри симулятора за виміряну пропускну здатність. Ціль `p95 < 500 мс`, вказана у методичці, є **ціллю**, не доведеним результатом цього середовища.

### Перевірка самостійної логіки без брокера

```powershell
docker compose run --rm lab python -m pytest -q
```

Тести в `tests/test_energy_logic.py` перевіряють детекцію аномалій, коректність `active_meters`, мінімум/максимум/середнє у вікні, енергію та час UTC. Це unit-тести; вони не замінюють інтеграційні тести з Kafka й Cassandra.

## Типові помилки і діагностика

| Симптом | Що перевірити |
|---|---|
| `NoBrokersAvailable` | `docker compose ps`; адреса Kafka з контейнера `kafka:29092`, з Windows `localhost:9092` |
| `NoHostAvailable` Cassandra | `cassandra` готовий/healthy? створений `energy_monitoring4`? `nodetool status` показує UN? |
| `ModuleNotFoundError: bytewax` | Запускайте через `docker compose run --rm lab python -m ...`, перед тим `docker compose build lab` |
| Немає агрегатів | Минуло ще недостатньо 60 секунд event-time або Bytewax не працює; перевірте `energy.aggregated` |
| `hourly_stats` порожня | Не закрилося повне годинне вікно; для 135-секундної симуляції це допустимо |
| Є Kafka аномалії, але нема Cassandra аномалій | Writer `anomaly` запущений? `consumer group` lag? Перевірте журнал writer-а |
| Повторення повідомлень після рестарту | Bytewax без recovery починає з ранніх offset; для цієї навчальної схеми CQL upsert стабілізує рядки. Для production потрібні Bytewax recovery/offset semantics і гарантії обробки |
| Запит `WHERE severity='critical'` не працює | Таблиця `anomalies_by_severity` вимагає також `hour_bucket` |
| Порт 9092 чи 9042 уже зайнятий | Зупиніть інший процес або змініть порти, налаштувавши клієнтів відповідно |

## Що включити в роботу та звіт

Потрібні реальні скриншоти: список/партиції топіків, keyspace та схеми Cassandra, JSON IoT-симулятора, лог Bytewax, події з `energy.anomalies`, події `energy.aggregated`, логи всіх Writer-ів, SELECT із усіх заповнених таблиць, `nodetool`, TTL, Kafka consumer lag, графік, unit-тести, вимірювання throughput/latency та аналіз відмінності від цільових значень. Для оформлення використайте `results/REPORT_04.md`.
