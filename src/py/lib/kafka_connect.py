from confluent_kafka import Consumer


def error_callback(err) -> None:
    print(f"Kafka error: {err}")


class KafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group: str,
        security_protocol: str = "PLAINTEXT",
        user: str | None = None,
        password: str | None = None,
        cert_path: str | None = None,
    ) -> None:
        params = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": security_protocol,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "error_cb": error_callback,
            "client.id": "raw-loader",
        }

        if security_protocol == "SASL_SSL":
            params.update({
                "ssl.ca.location": cert_path,
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": user,
                "sasl.password": password,
            })

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