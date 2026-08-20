import json
from typing import Dict, Optional

from confluent_kafka import Consumer


def error_callback(err) -> None:
    print(f"Kafka error: {err}")


class KafkaConsumer:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        topic: str,
        group: str,
        cert_path: str,
    ) -> None:
        params = {
            "bootstrap.servers": f"{host}:{port}",
            "security.protocol": "SASL_SSL",
            "ssl.ca.location": cert_path,
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": user,
            "sasl.password": password,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "error_cb": error_callback,
            "client.id": "final-project-consumer",
        }

        self._topic = topic
        self._consumer = Consumer(params)
        self._consumer.subscribe([topic])

    def consume(self, timeout: float = 1.0) -> Optional[Dict]:
        msg = self._consumer.poll(timeout=timeout)

        if msg is None:
            return None

        if msg.error():
            raise RuntimeError(msg.error())

        value = msg.value().decode("utf-8")

        return json.loads(value)

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()