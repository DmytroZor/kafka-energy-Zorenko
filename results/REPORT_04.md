# Практична робота №4

**Тема:** Real-time система моніторингу енергетичних даних на базі Apache Kafka, Bytewax та Apache Cassandra.  
**Виконав:** [ПІБ, група]  
**Дата:** [дата]

## Мета

Реалізувати наскрізний потік даних від симульованих лічильників електроенергії через Kafka й Bytewax до оптимізованого сховища Cassandra; обчислювати погодинні та хвилинні агрегати, виявляти відхилення й перевіряти продуктивність системи.

## Архітектура системи

```text
IoT Meter Simulator (20+ smart meters)
           |
           v
    Kafka energy.raw
      |            \
      |             -> Cassandra Writer (raw) -> raw_readings
      v
 Bytewax flow
   |           |              \
   v           v               v
anomalies   minute windows   hourly windows
   |           |               |
Kafka       Kafka           Kafka
energy.     energy.         energy.
anomalies   aggregated      hourly
   |           |               |
   v           v               v
Cassandra   Cassandra       Cassandra
anomalies   regional_       hourly_stats
+by_severity metrics
```

**Kafka** буферизує й маршрутизує повідомлення; **Bytewax** виконує операції над потоками; **Cassandra Writer** читає результати з Kafka та записує CQL; **Cassandra** зберігає часові ряди за оптимізованими partition keys. Додатковий `energy.hourly` забезпечує наповнення таблиці погодинних підсумків.

## Теоретичні відомості

Віконування (windowing) визначає відрізки часу, для яких обчислюються статистики. У реалізації використовуються tumbling windows по 60 секунд і 3600 секунд з event-time та допуском на запізнення 5 секунд. Stateful aggregation підтримує окремі накопичувачі для кожного регіону й часу вікна. Показники потужності зберігаються у ватах, енергія оцінюється у кВт·год на основі тривалості інтервалу між вимірами: `energy_kwh += power_W * sample_interval_seconds / 3_600_000`. `total_power` — сума вибіркових потужностей, а не одиниця енергії.

## Хід роботи

### 1. Підготовка середовища

Підготовлено Docker-контейнери Kafka, ZooKeeper та Cassandra, Python-образ з `bytewax==0.21.1`, `kafka-python`, `confluent-kafka`, `cassandra-driver`. `create_topics.py` створює `energy.raw` (4 partitions), `energy.aggregated` (4), `energy.anomalies` (2), `energy.hourly` (2). Retention відповідає 7/30/90 дням відповідно. **Рисунок 1 — контейнери**, **Рисунок 2 — топіки і partitions**: [вставити після запуску].

### 2. Схема Cassandra

`practical4_streaming/schema.cql` створює keyspace `energy_monitoring4`, таблиці `raw_readings`, `regional_metrics`, `anomalies`, `anomalies_by_severity`, `hourly_stats`. `raw_readings` використовує `(meter_id,hour_bucket)` та TWCS. На одному вузлі застосовано RF1 замість RF3 із методичного сценарію на трьох вузлах. Замість Materialized View у базовому сценарії застосовано явну денормалізацію аномалій за severity; необов'язковий сценарій з MV описано в `materialized_view_optional.cql`. **Рисунок 3 — keyspace**, **Рисунок 4 — CQL-схеми**: [вставити].

### 3. IoT Simulator

`smart_meter_simulator.py` створює набір розумних лічильників у кількох регіонах. Подія JSON містить час у мс UTC, meter ID, регіон, напругу, струм, потужність, частоту, температуру, power factor та інтервал дискретизації. Опція `--anomaly-rate 0.10` **ймовірнісно** інжектує відхилення в синтетичні дані. Основний сценарій: 20 лічильників, 135 секунд, одне вимірювання за секунду; теоретично приблизно 2700 повідомлень. **Рисунок 5 — фактичне повідомлення в Kafka**, **Рисунок 6 — вивід симулятора**: [вставити].

### 4. Bytewax Stream Processing

`energy_stream_processor.py` перетворює Kafka JSON у валідовані словники, застосовує keyed aggregation за регіонами і `fold_window` для 60-секундних та годинних вікон. Виявлення аномалій здійснюється за правилами: напруга поза 200–240 V, струм понад 60 A, частота поза 49.5–50.5 Hz; ступінь critical при напрузі поза 190–250 V або струмі понад 90 A. `avg_voltage`, `min_voltage`, `max_voltage`, `total_power`, `active_meters`, `readings_count` записуються до `energy.aggregated`; окремі повідомлення про відхилення до `energy.anomalies`. **Рисунок 7 — Bytewax**, **Рисунки 8–9 — приклади агрегатів і аномалій у Kafka**: [вставити].

### 5. Cassandra Writers

`cassandra_writer.py` має режими `raw`, `agg`, `anomaly`, `hourly`. Кожен consumer використовує власну групу; після успішного CQL-запису всієї прочитаної порції фіксує offset. Для `anomaly` виконується запис у `anomalies` та `anomalies_by_severity`, щоб забезпечити ефективні запити за регіоном і за критичністю. Не використовується cross-partition giant batch. **Рисунки 10–13 — виводи чотирьох writer-ів**: [вставити].

### 6. Тестування й аналітика

`practical4_streaming/queries.cql` перевіряє вміст Cassandra. `performance_probe.py` заміряє читання Kafka та вік повідомлень відносно часу події, `visualize_results.py` формує `results/energy_metrics.png` з фактично записаних агрегатів; pytest перевіряє незалежну бізнес-логіку. Команди й порядок запуску наведено в `guides/04_STREAMING.md`. **Рисунки 14–18 — CQL, TTL, tablestats, графік, latency й pytest**: [вставити].

## Результати після фактичного запуску

| Показник | Виміряне значення / підтвердження |
|---|---|
| Працездатність Kafka та Cassandra | [результати `docker compose ps` / nodetool] |
| Записів у `raw_readings` | [SELECT COUNT(*)] |
| Регіональних агрегатів | [SELECT COUNT(*)] |
| Виявлених аномалій | [SELECT COUNT(*)] |
| `anomalies_by_severity` доступна за повним PK | [так/ні + SQL] |
| Годинних агрегатів (за завершеним вікном) | [SELECT COUNT(*) або пояснення] |
| Відібрано повідомлень для benchmark | [із p4_probe.csv] |
| Фактичний throughput в Kafka consumer, events/sec | [число з p4_probe.csv] |
| p50 віку події, ms | [із p4_probe.csv] |
| p95 віку події, ms | [із p4_probe.csv] |
| p99 віку події, ms | [із p4_probe.csv] |
| Unit-тести | [фактичний вивід pytest] |

Цільові значення з методички — **1000+ events/sec, p95<500 ms** — не є автоматично досягнутими. Заміри `performance_probe.py` стосуються віку події при читанні з Kafka; вони **не вимірюють end-to-end latency IoT → Cassandra**. Окремий дослід з 200 лічильниками та інтервалом 0.1 с лише формує потенційне навантаження до 2000 messages/sec — підтвердити треба фактичними даними.

## Обговорення та обмеження

Модель даних Cassandra зорієнтована на прогнозований доступ до невеликих погодинних партицій. TTL 7/30/90 днів автоматизує обмеження періоду зберігання raw/aggregates/anomalies. Наявність лише одного брокера й одного вузла Cassandra не доводить відмовостійкості розподіленого кластера. Принаймні одноразова семантика offset commit потребує ідемпотентного downstream-запису: стабільні первинні ключі зменшують кількість логічних дублікатів. Годинні вікна закриваються лише при просуванні event-time; після запуску на 135 секунд вони можуть ще не з'явитися. Для промислового сценарію потрібні recovery/checkpoint, централізований моніторинг, безпечні схеми обробки помилок та багатовузлова реплікація.

## Висновок

Підготовлено компоненти системи реального часу: IoT-симулятор, Kafka topics, Bytewax stream-processing із часовими вікнами, детектор аномалій, споживачі з записом до Cassandra та інструменти тестування. Результати **реального** проходження даних від симулятора до бази даних і вимірювання продуктивності належить підтвердити скриншотами, CQL-вибірками й CSV після власного запуску.
