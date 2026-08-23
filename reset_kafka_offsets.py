import os

from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()

    topic = os.environ["KAFKA_TOPIC"]
    group = os.environ["KAFKA_CONSUMER_GROUP"]
    partition = 0

    consumer = Consumer(
        {
            "bootstrap.servers": (
                f'{os.environ["KAFKA_HOST"]}:'
                f'{os.environ.get("KAFKA_PORT", "9091")}'
            ),
            "security.protocol": "SASL_SSL",
            "ssl.ca.location": os.environ["YC_CA_PATH"],
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": os.environ["KAFKA_USER"],
            "sasl.password": os.environ["KAFKA_PASSWORD"],
            "group.id": group,
            "enable.auto.commit": False,
        }
    )

    try:
        tp = TopicPartition(topic, partition)

        committed = consumer.committed(
            [tp],
            timeout=10,
        )[0]

        earliest, latest = consumer.get_watermark_offsets(
            tp,
            timeout=10,
        )

        print(
            f"group={group}, "
            f"partition={partition}, "
            f"committed={committed.offset}, "
            f"earliest={earliest}, "
            f"latest={latest}"
        )

        consumer.commit(
            offsets=[
                TopicPartition(
                    topic,
                    partition,
                    earliest,
                )
            ],
            asynchronous=False,
        )

        committed_after = consumer.committed(
            [tp],
            timeout=10,
        )[0]

        print(
            f"Offset reset: "
            f"{committed.offset} -> {committed_after.offset}"
        )

    finally:
        consumer.close()


if __name__ == "__main__":
    main()