import argparse
import logging
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from dds_loader.dds_processor import DdsProcessor
from dds_loader.dds_repository import DdsRepository
from lib.clickhouse_client import ClickHouseClient


def main(
    process_date: date,
) -> None:
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    clickhouse = ClickHouseClient(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8443")),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get(
            "CLICKHOUSE_DATABASE",
            "dds",
        ),
        cert_path=os.environ["CERT_PATH"],
    )

    sql_dir = Path("src/sql/dml/dds")

    repository = DdsRepository(
        clickhouse=clickhouse,
        sql_dir=sql_dir,
    )

    processor = DdsProcessor(
        drop_shadow_partition=repository.drop_transactions_shadow_partition,
        load_shadow_partition=repository.load_transactions_shadow_partition,
        replace_partition=repository.replace_transactions_partition,
    )

    try:
        processor.run(process_date)
    finally:
        repository.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--process-date",
        required=True,
        type=date.fromisoformat,
    )

    args = parser.parse_args()

    main(args.process_date)