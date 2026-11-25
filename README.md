# Чотири практичні роботи: Apache Kafka, Apache Cassandra, Bytewax

Проєкт містить реалізацію практичних робіт №1–4 **в порядку з наданих методичних матеріалів**. Мова інструкцій, пояснень і шаблонів звітів — українська. Платформа для відтворення — **Windows 10/11, Docker Desktop (WSL2), PowerShell, VS Code**. Для Linux/macOS достатньо адаптувати синтаксис оболонки.

## Початок роботи

Спершу відкрийте **[`guides/00_START.md`](guides/00_START.md)**, потім без пропусків:

1. [`guides/01_KAFKA.md`](guides/01_KAFKA.md): сервер, topics, консольні та Python producer/consumer, Kafka benchmark, контрольні питання.
2. [`guides/02_CASSANDRA.md`](guides/02_CASSANDRA.md): keyspace, модель електростанцій, CQL, індекси, seed, CRUD, counter, TTL, nodetool.
3. [`guides/03_OPTIMIZATION.md`](guides/03_OPTIMIZATION.md): A/B схеми Simple Wide Row і Hourly Bucketing, однакові часові ряди, генератор, nodetool і p50/p95/p99 benchmark.
4. [`guides/04_STREAMING.md`](guides/04_STREAMING.md): IoT-сенсори → Kafka → Bytewax → Kafka → Cassandra, аномалії, регіональні й годинні вікна, реальні виміри та графік.

Після кожної роботи заповніть відповідний `results/REPORT_0N.docx` (або вихідний Markdown `results/REPORT_0N.md`). Звіти містять науково-технічні пояснення, вимоги, виконані етапи й місця для **фактичних скриншотів та результатів**. Не переносіть очікувані результати до розділу реальних вимірювань, не заповнивши його після роботи власного стенда.

## Файли проєкту

```text
energy_labs_zorenko/
  docker-compose.yml             локальні Kafka, ZooKeeper, Cassandra + Python/Bytewax
  Dockerfile, requirements.txt    відтворюване Python-середовище
  common.py, create_topics.py    конфігурація та ініціалізація Kafka
  practical1_kafka/              producer.py, consumer.py
  practical2_cassandra/          schema.cql, seed.cql, queries.cql, maintenance.py
  practical3_optimization/       schema.cql, generate.py, benchmark.py, analyze_partitions.py
  practical4_streaming/          schema.cql, smart_meter_simulator.py,
                                energy_stream_processor.py, cassandra_writer.py,
                                performance_probe.py, visualize_results.py
  guides/                        загальні і чотири індивідуальні інструкції
  tests/                         перевірки чистої логіки Python
  results/                       шаблони звітів, справжні виміри й графіки після запуску
```

## Перевірки

Код Python пройшов статичне компілювання (`compileall`); незалежні unit-тести перевіряються командою `python -m pytest -q`. Живий запуск Docker/Kafka/Cassandra/Bytewax у середовищі підготовки не був доступний, тому **інтеграційна робота в усіх чотирьох практичних має бути перевірена на вашому комп'ютері**. Інструкції містять команди запуску, верифікації, варіанти діагностики й збирання доказів. Це навчальний кластер з одним брокером і одним вузлом БД, без відмовостійкості.

### Відмінності від методичних прикладів

- Ім'я Kafka-topic включає прізвище згідно з вимогою №1 (`power-station-data-zorenko`).
- В №2 створено пропущену в прикладі таблицю `station_alerts`; уточнено коректність `TIMEUUID` під час UPDATE і реалізовано реальний top-by-capacity-запит.
- В №3 узгоджено ім'я `telemetry_hourly`, організовано детерміновану генерацію **однакових** даних для двох таблиць і коректне порівняння того самого часового інтервалу.
- В №4 замість RF3 на одно-вузловій Cassandra використано RF1; замість небезпечного запиту за `severity` без повного ключа створено окрему денормалізовану таблицю; Bytewax використовує версію 0.21.1 з сучасним API.

Методичні джерела: `Практична робота Kafka.pdf`; `Практика Apache Cassandra 2.pdf`; `Практика 3 Оптимізація Cassandra.docx`; `Практика 4 Kafka Streams Concepts.pdf`. Актуальні API-довідки: https://docs.bytewax.io/stable/ та https://docs.confluent.io/platform/current/ .
