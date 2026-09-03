from confluent_kafka import Consumer


def error_callback(err) -> None:
    print(f"Kafka error: {err}")


class KafkaConsumer:
    def __init__(
        self,
        host: str,
        port: int,
        topic: str,
        group: str,
        user: str,
        password: str,
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
            "client.id": "raw-loader",
        }

        self._consumer = Consumer(params)
        self._consumer.subscribe([topic])

    def consume(
        self,
        timeout: float = 1.0,
    ) -> tuple[str, int, int, str] | None:
        msg = self._consumer.poll(timeout=timeout)

        if msg is None:
            return None

        if msg.error():
            raise RuntimeError(msg.error())

        return (
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.value().decode("utf-8"),
        )

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()