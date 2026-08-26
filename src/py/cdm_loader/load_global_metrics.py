import argparse
import logging
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from cdm_loader.cdm_processor import CdmProcessor
from cdm_loader.cdm_repository import CdmRepository
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
            "cdm",
        ),
        cert_path=os.environ["CERT_PATH"],
    )

    sql_dir = Path("src/sql/dml/cdm")

    repository = CdmRepository(
        clickhouse=clickhouse,
        sql_dir=sql_dir,
    )

    processor = CdmProcessor(
        drop_shadow_partition=repository.drop_global_metrics_shadow_partition,
        load_shadow_partition=repository.load_global_metrics_shadow_partition,
        replace_partition=repository.replace_global_metrics_partition,
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