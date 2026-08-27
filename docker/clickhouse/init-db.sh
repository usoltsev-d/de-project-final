#!/bin/bash
set -e

echo "Creating databases..."

clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery <<SQL

CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS dds;
CREATE DATABASE IF NOT EXISTS cdm;

SQL

apply_ddl() {
    directory="$1"

    echo "Applying DDL from $directory"

    for file in "$directory"/*.sql; do
        echo "Applying $file"

        clickhouse-client \
          --user "$CLICKHOUSE_USER" \
          --password "$CLICKHOUSE_PASSWORD" \
          --multiquery \
          < "$file"
    done
}

apply_ddl "/app/sql/ddl/raw"
apply_ddl "/app/sql/ddl/stg"
apply_ddl "/app/sql/ddl/dds"
apply_ddl "/app/sql/ddl/cdm"

echo "ClickHouse initialization completed."
