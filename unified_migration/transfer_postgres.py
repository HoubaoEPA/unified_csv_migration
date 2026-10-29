from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from unified_migration.pipeline_lock import pipeline_lock
from unified_migration.postgres_writer import staging_table_name, validate_schema_name
from unified_migration.settings import settings
from unified_migration.table_config import (
    DEFAULT_CONFIG,
    TableConfig,
    load_table_config,
    select_table_configs,
)


def _psycopg_url(database_url: str) -> str:
    return database_url.replace("postgresql+psycopg://", "postgresql://", 1)


def _qualified(schema: str, table: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))


def _table_columns(
    connection: psycopg.Connection[Any], schema: str, table: str
) -> list[tuple[str, str, bool]] | None:
    rows = connection.execute(
        """SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                  a.attnotnull
           FROM pg_catalog.pg_attribute AS a
           JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
           JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
           WHERE n.nspname = %s AND c.relname = %s
             AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
           ORDER BY a.attnum""",
        (schema, table),
    ).fetchall()
    return [(row[0], row[1], row[2]) for row in rows] if rows else None


def _row_count(connection: psycopg.Connection[Any], schema: str, table: str) -> int:
    return connection.execute(
        sql.SQL("SELECT count(*) FROM {}").format(_qualified(schema, table))
    ).fetchone()[0]


def _source_batch_id(
    connection: psycopg.Connection[Any], schema: str, table: str
) -> str | None:
    columns = _table_columns(connection, schema, table) or []
    if "_migration_batch_id" not in {column[0] for column in columns}:
        return None
    values = connection.execute(
        sql.SQL("SELECT DISTINCT {} FROM {} LIMIT 2").format(
            sql.Identifier("_migration_batch_id"), _qualified(schema, table)
        )
    ).fetchall()
    if len(values) == 1:
        return values[0][0]
    return None


def _ensure_transfer_control(
    connection: psycopg.Connection[Any],
    schema: str,
    run_table: str,
    batch_table: str,
) -> None:
    connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    connection.execute(
        sql.SQL(
            """CREATE TABLE IF NOT EXISTS {} (
                   run_id text PRIMARY KEY,
                   selected_tables text NOT NULL,
                   started_at timestamptz NOT NULL,
                   completed_at timestamptz,
                   status text NOT NULL,
                   error_message text
               )"""
        ).format(_qualified(schema, run_table))
    )
    connection.execute(
        sql.SQL(
            """CREATE TABLE IF NOT EXISTS {} (
                   transfer_batch_id text PRIMARY KEY,
                   transfer_run_id text NOT NULL,
                   source_schema text NOT NULL,
                   source_table text NOT NULL,
                   destination_schema text NOT NULL,
                   destination_table text NOT NULL,
                   source_migration_batch_id text,
                   source_row_count bigint,
                   destination_row_count bigint,
                   started_at timestamptz NOT NULL,
                   completed_at timestamptz,
                   status text NOT NULL,
                   error_message text
               )"""
        ).format(_qualified(schema, batch_table))
    )


def _create_staging(
    destination: psycopg.Connection[Any],
    schema: str,
    staging: str,
    columns: list[tuple[str, str, bool]],
) -> None:
    definitions = []
    for name, postgres_type, not_null in columns:
        definition = sql.SQL("{} {}").format(
            sql.Identifier(name), sql.SQL(postgres_type)
        )
        if not_null:
            definition += sql.SQL(" NOT NULL")
        definitions.append(definition)
    destination.execute(
        sql.SQL("CREATE TABLE {} ({})").format(
            _qualified(schema, staging), sql.SQL(", ").join(definitions)
        )
    )


def _stream_table(
    source: psycopg.Connection[Any],
    destination: psycopg.Connection[Any],
    source_schema: str,
    source_table: str,
    destination_schema: str,
    staging: str,
    columns: list[tuple[str, str, bool]],
) -> None:
    column_list = sql.SQL(", ").join(sql.Identifier(column[0]) for column in columns)
    copy_out = sql.SQL(
        "COPY (SELECT {} FROM {}) TO STDOUT WITH (FORMAT CSV, NULL '\\N')"
    ).format(column_list, _qualified(source_schema, source_table))
    copy_in = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    ).format(_qualified(destination_schema, staging), column_list)
    with source.cursor().copy(copy_out) as reader:
        with destination.cursor().copy(copy_in) as writer:
            for chunk in reader:
                writer.write(chunk)


def inspect_transfer(
    *,
    source: psycopg.Connection[Any],
    destination: psycopg.Connection[Any],
    source_schema: str,
    destination_schema: str,
    table_configs: list[TableConfig],
    force: bool = False,
) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    for config in table_configs:
        table = config.destination_table
        source_columns = _table_columns(source, source_schema, table)
        if source_columns is None:
            raise RuntimeError(f"Source table does not exist: {source_schema}.{table}")
        destination_columns = _table_columns(destination, destination_schema, table)
        if destination_columns is not None and destination_columns != source_columns:
            raise RuntimeError(
                f"Destination schema differs from the source for "
                f"{destination_schema}.{table}. Apply a controlled schema migration first."
            )
        source_count = _row_count(source, source_schema, table)
        destination_count = (
            _row_count(destination, destination_schema, table)
            if destination_columns is not None
            else None
        )
        source_batch = _source_batch_id(source, source_schema, table)
        destination_batch = (
            _source_batch_id(destination, destination_schema, table)
            if destination_columns is not None
            else None
        )
        already_current = (
            not force
            and destination_columns is not None
            and source_batch is not None
            and source_batch == destination_batch
            and source_count == destination_count
        )
        plans.append(
            {
                "source_table": config.source_table,
                "table": table,
                "columns": source_columns,
                "source_rows": source_count,
                "destination_rows_before": destination_count,
                "source_migration_batch_id": source_batch,
                "destination_migration_batch_id": destination_batch,
                "action": "skip_already_current" if already_current else (
                    "create" if destination_columns is None else "refresh"
                ),
            }
        )
    return plans


def transfer_postgres(
    *,
    source_database_url: str,
    destination_database_url: str,
    source_schema: str,
    destination_schema: str,
    control_schema: str,
    transfer_run_table: str,
    transfer_batch_table: str,
    table_configs: list[TableConfig],
    dry_run: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    if not destination_database_url:
        raise ValueError("A destination PostgreSQL URL is required")
    source_schema = validate_schema_name(source_schema)
    destination_schema = validate_schema_name(destination_schema)
    if (
        _psycopg_url(source_database_url) == _psycopg_url(destination_database_url)
        and source_schema == destination_schema
    ):
        raise ValueError("Source and destination PostgreSQL targets must be different")
    control_schema = validate_schema_name(control_schema)
    transfer_run_table = validate_schema_name(transfer_run_table)
    transfer_batch_table = validate_schema_name(transfer_batch_table)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    manifest = [config.destination_table for config in table_configs]

    with psycopg.connect(_psycopg_url(source_database_url)) as source:
        source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with psycopg.connect(_psycopg_url(destination_database_url)) as destination:
            plans = inspect_transfer(
                source=source,
                destination=destination,
                source_schema=source_schema,
                destination_schema=destination_schema,
                table_configs=table_configs,
                force=force,
            )
            if dry_run:
                destination.rollback()
                return {"mode": "dry-run", "run_id": None, "tables": plans}

            destination.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(destination_schema)
                )
            )
            _ensure_transfer_control(
                destination, control_schema, transfer_run_table, transfer_batch_table
            )
            destination.execute(
                sql.SQL(
                    "INSERT INTO {} (run_id, selected_tables, started_at, status) "
                    "VALUES (%s, %s, %s, 'running')"
                ).format(_qualified(control_schema, transfer_run_table)),
                (run_id, json.dumps(manifest), started_at),
            )
            destination.commit()

            staged: list[tuple[dict[str, Any], str, str]] = []
            try:
                destination.execute("BEGIN")
                for plan in plans:
                    transfer_batch_id = str(uuid.uuid4())
                    if plan["action"] == "skip_already_current":
                        destination.execute(
                            sql.SQL(
                                """INSERT INTO {} (
                                    transfer_batch_id, transfer_run_id, source_schema,
                                    source_table, destination_schema, destination_table,
                                    source_migration_batch_id, source_row_count,
                                    destination_row_count, started_at, completed_at, status)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'skipped')"""
                            ).format(_qualified(control_schema, transfer_batch_table)),
                            (
                                transfer_batch_id, run_id, source_schema, plan["table"],
                                destination_schema, plan["table"],
                                plan["source_migration_batch_id"], plan["source_rows"],
                                plan["destination_rows_before"], started_at,
                                datetime.now(timezone.utc),
                            ),
                        )
                        continue
                    staging = staging_table_name(plan["table"], transfer_batch_id)
                    _create_staging(
                        destination, destination_schema, staging, plan["columns"]
                    )
                    _stream_table(
                        source, destination, source_schema, plan["table"],
                        destination_schema, staging, plan["columns"]
                    )
                    staged_count = _row_count(destination, destination_schema, staging)
                    if staged_count != plan["source_rows"]:
                        raise RuntimeError(
                            f"Row count mismatch for {plan['table']}: "
                            f"source={plan['source_rows']}, staging={staged_count}"
                        )
                    staged.append((plan, staging, transfer_batch_id))

                for plan, staging, transfer_batch_id in staged:
                    table = plan["table"]
                    column_list = sql.SQL(", ").join(
                        sql.Identifier(column[0]) for column in plan["columns"]
                    )
                    if plan["action"] == "create":
                        destination.execute(
                            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                                _qualified(destination_schema, staging), sql.Identifier(table)
                            )
                        )
                    else:
                        destination.execute(
                            sql.SQL("TRUNCATE TABLE {}").format(
                                _qualified(destination_schema, table)
                            )
                        )
                        destination.execute(
                            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                                _qualified(destination_schema, table), column_list,
                                column_list, _qualified(destination_schema, staging),
                            )
                        )
                        destination.execute(
                            sql.SQL("DROP TABLE {}").format(
                                _qualified(destination_schema, staging)
                            )
                        )
                    final_count = _row_count(destination, destination_schema, table)
                    if final_count != plan["source_rows"]:
                        raise RuntimeError(
                            f"Published row count mismatch for {table}: "
                            f"source={plan['source_rows']}, destination={final_count}"
                        )
                    plan["destination_rows_after"] = final_count
                    destination.execute(
                        sql.SQL(
                            """INSERT INTO {} (
                                transfer_batch_id, transfer_run_id, source_schema,
                                source_table, destination_schema, destination_table,
                                source_migration_batch_id, source_row_count,
                                destination_row_count, started_at, completed_at, status)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'completed')"""
                        ).format(_qualified(control_schema, transfer_batch_table)),
                        (
                            transfer_batch_id, run_id, source_schema, table,
                            destination_schema, table, plan["source_migration_batch_id"],
                            plan["source_rows"], final_count, started_at,
                            datetime.now(timezone.utc),
                        ),
                    )
                destination.execute(
                    sql.SQL(
                        "UPDATE {} SET status='completed', completed_at=%s WHERE run_id=%s"
                    ).format(_qualified(control_schema, transfer_run_table)),
                    (datetime.now(timezone.utc), run_id),
                )
                destination.commit()
            except Exception as exc:
                destination.rollback()
                destination.execute(
                    sql.SQL(
                        """UPDATE {} SET status='failed', completed_at=%s,
                           error_message=%s WHERE run_id=%s"""
                    ).format(_qualified(control_schema, transfer_run_table)),
                    (datetime.now(timezone.utc), str(exc)[:4000], run_id),
                )
                destination.commit()
                raise
    return {"mode": "transfer", "run_id": run_id, "status": "completed", "tables": plans}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream configured Unified landing tables between PostgreSQL databases."
    )
    parser.add_argument(
        "--source-database-url", default=settings.transfer_source_database_url
    )
    parser.add_argument(
        "--destination-database-url",
        default=settings.transfer_destination_database_url,
    )
    parser.add_argument("--source-schema", default=settings.source_schema)
    parser.add_argument("--destination-schema", default=settings.source_schema)
    parser.add_argument("--control-schema", default=settings.control_schema)
    parser.add_argument("--table-config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--table", action="append")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.destination_database_url:
        raise SystemExit(
            "Set MIGRATION_TRANSFER_DESTINATION_DATABASE_URL or pass "
            "--destination-database-url"
        )
    try:
        configs = select_table_configs(
            load_table_config(args.table_config), args.table
        )
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Invalid migration table configuration: {exc}") from exc
    if not configs:
        raise SystemExit("No enabled migration tables were found in the configuration")
    with pipeline_lock(args.destination_database_url):
        result = transfer_postgres(
            source_database_url=args.source_database_url,
            destination_database_url=args.destination_database_url,
            source_schema=args.source_schema,
            destination_schema=args.destination_schema,
            control_schema=args.control_schema,
            transfer_run_table=settings.transfer_run_table,
            transfer_batch_table=settings.transfer_batch_table,
            table_configs=configs,
            dry_run=args.dry_run,
            force=args.force,
        )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
