import clickhouse_connect
from clickhouse_connect.driver.client import Client


class ClickHouseClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        secure: bool = False,
        cert_path: str | None = None,
    ) -> None:
        self._client: Client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=secure,
            ca_cert=cert_path,
        )

    @property
    def client(self) -> Client:
        return self._client

    def close(self) -> None:
        self._client.close()