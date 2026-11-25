# Практична №1 — Apache Kafka

Мета: брокер Kafka 3.7.x із ZooKeeper, 3-партиційний топік `power-station-data-zorenko`, чотири допоміжні топіки, консольний і програмний producer/consumer, consumer groups, вимірювання швидкості та коректна зупинка.

Повний порядок дій і команди для PowerShell описані в `../guides/01_KAFKA.md`. Для швидкого відтворення запустіть із кореня `docker compose up -d zookeeper kafka`, `docker compose run --rm lab python create_topics.py`, producer та consumer у двох терміналах.

Примітка: назви основного топіка та проєкту містять прізвище відповідно до вимоги документа. Kafka 3.7.x у цьому стенді працює з ZooKeeper; застосування KRaft змінило б архітектуру завдання.
