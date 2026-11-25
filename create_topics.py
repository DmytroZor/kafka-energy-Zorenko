"""Create every topic requested in P1/P4; safely rerunnable."""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from common import KAFKA, P1_TOPIC

TOPICS = [
    (P1_TOPIC, 3, 604800000),
    ("power-telemetry", 3, 604800000),
    ("grid-alerts", 2, 604800000),
    ("energy-consumption", 4, 604800000),
    ("weather-data", 2, 604800000),
    ("energy.raw", 4, 604800000),
    ("energy.aggregated", 4, 2592000000),
    ("energy.anomalies", 2, 7776000000),
    ("energy.hourly", 4, 7776000000),
]

def main():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA, client_id="energy-labs-topic-admin")
    try:
        existing = set(admin.list_topics())
        for name, parts, retention in TOPICS:
            if name in existing:
                print(f"EXISTS: {name}")
                continue
            topic = NewTopic(name, num_partitions=parts, replication_factor=1,
                             topic_configs={"retention.ms": str(retention)})
            try:
                admin.create_topics([topic], validate_only=False)
                print(f"CREATED: {name} partitions={parts} replication=1")
            except TopicAlreadyExistsError:
                print(f"EXISTS: {name}")
    finally:
        admin.close()

if __name__ == "__main__":
    main()
