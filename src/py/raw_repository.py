from typing import Sequence

import clickhouse_connect
from clickhouse_connect.driver.client import Client


class RawRepository:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        cert_path: str,
    ) -> None:
        self._client: Client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=True,
            ca_cert=cert_path,
        )

    def save_events(self, events: Sequence[tuple]) -> None:
        if not events:
            return

        self._client.insert(
            "raw.transaction_service_events",
            events,
            column_names=[
                "object_id",
                "object_type",
                "sent_dttm",
                "payload",
            ],
        )

    def close(self) -> None:
        self._client.close()