# Практична №4 — Streaming pipeline Kafka → Bytewax → Cassandra

Основні компоненти: `smart_meter_simulator.py`, `energy_stream_processor.py` (Bytewax 0.21.1), `cassandra_writer.py` (режими raw, agg, anomaly, hourly), `schema.cql`, `energy_logic.py`, `visualize_results.py`, перевірки та документація в `../guides/04_STREAMING.md`.

Використано **event-time** tumbling windows 60 секунд і 3600 секунд, ключ `(region)`; операторами Bytewax є `key_on`, `fold_window`, `map`, `filter`, `input`, `output`. Кожен регіон обробляється зі своїм станом. Дані аномалій виявляються на кожному вимірі незалежно від закриття часового вікна.

Зміни до методички: для одного вузла `NetworkTopologyStrategy: datacenter1=1` замість 3; індекс аномалій — явна денормалізована таблиця замість експериментального MV; Bytewax Kafka connector використано зі встановленого `bytewax.connectors.kafka`, замість власного неперевіреного файлу конектора. Додано окремий `energy.hourly` та writer hourly. Не заявляємо, що гарантоване exactly-once: at-least-once + idempotent CQL keys.
