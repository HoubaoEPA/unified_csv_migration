from __future__ import annotations

import hashlib
import csv
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import Column, MetaData, Table, Text, create_engine, func, insert, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.elements import quoted_name


METADATA_COLUMNS = ("_migration_batch_id", "_migrated_at", "_source_row_number")
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
POSTGRES_IDENTIFIER_LIMIT = 63


def _quote_identifier(identifier: str) -> str:
    """Quote a PostgreSQL identifier that may originate in a source CSV."""
    return '"' + identifier.replace('"', '""') + '"'


def _table_columns(
    connection: Connection,
    schema: str,
    table: str,
) -> list[tuple[str, str, str]] | None:
    """Return ordered column metadata for a base table, or None when absent."""
    table_type = connection.scalar(
        text(
            """SELECT table_type
               FROM information_schema.tables
               WHERE table_schema = :schema AND table_name = :table"""
        ),
        {"schema": schema, "table": table},
    )
    if table_type is None:
        return None
    if table_type != "BASE TABLE":
        raise RuntimeError(f"Destination {schema}.{table} is not a base table")

    rows = connection.execute(
        text(
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_schema = :schema AND table_name = :table
               ORDER BY ordinal_position"""
        ),
        {"schema": schema, "table": table},
    )
    return [(row.column_name, row.data_type, row.is_nullable) for row in rows]


def _require_compatible_columns(
    source_schema: str,
    destination: str,
    destination_columns: list[tuple[str, str, str]],
    staging_columns: list[tuple[str, str, str]],
) -> None:
    if destination_columns != staging_columns:
        raise RuntimeError(
            f"Destination schema differs from the source for "
            f"{source_schema}.{destination}. Apply a controlled schema migration "
            "before loading data."
        )


def validate_schema_name(schema: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(schema):
        raise ValueError(f"Unsafe PostgreSQL schema name: {schema!r}")
    if len(schema.encode("utf-8")) > POSTGRES_IDENTIFIER_LIMIT:
        raise ValueError(
            f"PostgreSQL identifier {schema!r} exceeds the "
            f"{POSTGRES_IDENTIFIER_LIMIT}-byte limit"
        )
    return schema


def postgres_table_name(source_table: str) -> str:
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", source_table)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()
    return re.sub(r"[^a-z0-9_]+", "_", name).strip("_")


def staging_table_name(destination: str, batch_id: str) -> str:
    """Return a unique PostgreSQL staging identifier within the 63-byte limit."""
    suffix = batch_id.replace("-", "")[:12]
    prefix = "_staging_"
    available = POSTGRES_IDENTIFIER_LIMIT - len(prefix) - len(suffix) - 1
    if available < 1:
        raise ValueError("Staging table identifier has no room for a destination name")
    return f"{prefix}{destination[:available]}_{suffix}"


def source_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _string_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def _ensure_schemas_and_control(
    engine: Engine,
    source_schema: str,
    control_schema: str,
    batch_table: str,
    run_table: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{source_schema}"'))
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{control_schema}"'))
        connection.execute(
            text(
                f'''CREATE TABLE IF NOT EXISTS "{control_schema}"."{run_table}" (
                    run_id text PRIMARY KEY,
                    source_kind text NOT NULL,
                    source_path text NOT NULL,
                    source_sha256 text,
                    selected_tables text NOT NULL,
                    expected_table_count integer NOT NULL,
                    completed_table_count integer NOT NULL DEFAULT 0,
                    failed_table_count integer NOT NULL DEFAULT 0,
                    started_at timestamptz NOT NULL,
                    completed_at timestamptz,
                    status text NOT NULL,
                    error_message text
                )'''
            )
        )
        connection.execute(
            text(
                f'''CREATE TABLE IF NOT EXISTS "{control_schema}"."{batch_table}" (
                    batch_id text PRIMARY KEY,
                    migration_run_id text,
                    source_kind text NOT NULL,
                    source_path text NOT NULL,
                    source_sha256 text,
                    source_table text NOT NULL,
                    destination_schema text NOT NULL,
                    destination_table text NOT NULL,
                    started_at timestamptz NOT NULL,
                    completed_at timestamptz,
                    source_row_count bigint,
                    destination_row_count bigint,
                    status text NOT NULL,
                    error_message text
                )'''
            )
        )
        connection.execute(
            text(
                f'''ALTER TABLE "{control_schema}"."{batch_table}"
                    ADD COLUMN IF NOT EXISTS migration_run_id text'''
            )
        )


def start_migration_run(
    *,
    database_url: str,
    source_schema: str,
    control_schema: str,
    batch_table: str,
    run_table: str,
    source_kind: str,
    source_path: Path,
    source_sha256: str | None,
    selected_tables: list[dict[str, str]],
) -> str:
    """Create one parent run for the exact table manifest selected by a command."""
    if not selected_tables:
        raise ValueError("A migration run requires at least one selected table")
    source_schema = validate_schema_name(source_schema)
    control_schema = validate_schema_name(control_schema)
    batch_table = validate_schema_name(batch_table)
    run_table = validate_schema_name(run_table)
    engine = create_engine(database_url, future=True)
    try:
        _ensure_schemas_and_control(
            engine,
            source_schema,
            control_schema,
            batch_table,
            run_table,
        )
        run_id = str(uuid.uuid4())
        with engine.begin() as connection:
            connection.execute(
                text(
                    f'''INSERT INTO "{control_schema}"."{run_table}"
                    (run_id, source_kind, source_path, source_sha256,
                     selected_tables, expected_table_count, started_at, status)
                    VALUES (:run_id, :source_kind, :source_path, :source_sha256,
                            :selected_tables, :expected_count, :started_at, 'running')'''
                ),
                {
                    "run_id": run_id,
                    "source_kind": source_kind,
                    "source_path": str(source_path),
                    "source_sha256": source_sha256,
                    "selected_tables": json.dumps(selected_tables, ensure_ascii=False),
                    "expected_count": len(selected_tables),
                    "started_at": datetime.now(timezone.utc),
                },
            )
        return run_id
    finally:
        engine.dispose()


def finish_migration_run(
    *,
    database_url: str,
    control_schema: str,
    batch_table: str,
    run_table: str,
    run_id: str,
    succeeded: bool,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Finalize a run from its child batch statuses."""
    control_schema = validate_schema_name(control_schema)
    batch_table = validate_schema_name(batch_table)
    run_table = validate_schema_name(run_table)
    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            counts = connection.execute(
                text(
                    f'''SELECT
                        count(*) FILTER (WHERE status = 'completed'),
                        count(*) FILTER (WHERE status = 'failed')
                        FROM "{control_schema}"."{batch_table}"
                        WHERE migration_run_id = :run_id'''
                ),
                {"run_id": run_id},
            ).one()
            completed_count, failed_count = counts
            expected_count = connection.scalar(
                text(
                    f'''SELECT expected_table_count
                        FROM "{control_schema}"."{run_table}"
                        WHERE run_id = :run_id'''
                ),
                {"run_id": run_id},
            )
            if expected_count is None:
                raise RuntimeError(f"Migration run not found: {run_id}")
            completed = succeeded and failed_count == 0 and completed_count == expected_count
            status = "completed" if completed else "failed"
            if succeeded and not completed and not error_message:
                error_message = (
                    f"Expected {expected_count} completed table batches; "
                    f"found {completed_count} completed and {failed_count} failed"
                )
            connection.execute(
                text(
                    f'''UPDATE "{control_schema}"."{run_table}"
                        SET completed_at=:completed_at,
                            completed_table_count=:completed_count,
                            failed_table_count=:failed_count,
                            status=:status,
                            error_message=:error_message
                        WHERE run_id=:run_id'''
                ),
                {
                    "completed_at": datetime.now(timezone.utc),
                    "completed_count": completed_count,
                    "failed_count": failed_count,
                    "status": status,
                    "error_message": error_message[:4000] if error_message else None,
                    "run_id": run_id,
                },
            )
        return {
            "run_id": run_id,
            "status": status,
            "expected_tables": expected_count,
            "completed_tables": completed_count,
            "failed_tables": failed_count,
        }
    finally:
        engine.dispose()


def _validate_source_columns(source_table: str, columns: list[str]) -> None:
    if not columns:
        raise ValueError(f"Source table {source_table!r} has no columns")
    duplicates = sorted({column for column in columns if columns.count(column) > 1})
    if duplicates:
        raise ValueError(f"Duplicate source columns: {duplicates}")
    collisions = sorted(set(columns).intersection(METADATA_COLUMNS))
    if collisions:
        raise ValueError(f"Source columns conflict with migration metadata: {collisions}")


def _create_staging_table(
    engine: Engine,
    source_schema: str,
    staging: str,
    columns: list[str],
) -> Table:
    metadata = MetaData()
    staging_table = Table(
        quoted_name(staging, True),
        metadata,
        *(Column(quoted_name(column, True), Text) for column in columns),
        Column("_migration_batch_id", Text),
        Column("_migrated_at", Text),
        Column("_source_row_number", Text),
        schema=quoted_name(source_schema, True),
    )
    metadata.create_all(engine, tables=[staging_table])
    return staging_table


def _populate_staging_metadata(
    connection: Connection,
    source_schema: str,
    staging: str,
    batch_id: str,
    migrated_at: str,
) -> None:
    qualified = f"{_quote_identifier(source_schema)}.{_quote_identifier(staging)}"
    connection.execute(
        text(
            f'''WITH numbered AS (
                    SELECT ctid, row_number() OVER (ORDER BY ctid) AS source_row_number
                    FROM {qualified}
                )
                UPDATE {qualified} AS staging
                SET _migration_batch_id = :batch_id,
                    _migrated_at = :migrated_at,
                    _source_row_number = numbered.source_row_number::text
                FROM numbered
                WHERE staging.ctid = numbered.ctid'''
        ),
        {"batch_id": batch_id, "migrated_at": migrated_at},
    )
    for column in METADATA_COLUMNS:
        connection.execute(
            text(
                f"ALTER TABLE {qualified} ALTER COLUMN "
                f"{_quote_identifier(column)} SET NOT NULL"
            )
        )


def _publish_staging(
    connection: Connection,
    source_schema: str,
    staging: str,
    destination: str,
    loaded: int,
) -> None:
    destination_columns = _table_columns(connection, source_schema, destination)
    if destination_columns is None:
        connection.execute(
            text(
                f"ALTER TABLE {_quote_identifier(source_schema)}."
                f"{_quote_identifier(staging)} RENAME TO "
                f"{_quote_identifier(destination)}"
            )
        )
        return

    staging_columns = _table_columns(connection, source_schema, staging)
    if staging_columns is None:
        raise RuntimeError(f"Staging table {source_schema}.{staging} was not created")
    _require_compatible_columns(
        source_schema,
        destination,
        destination_columns,
        staging_columns,
    )
    quoted_columns = ", ".join(
        _quote_identifier(column) for column, _, _ in destination_columns
    )
    connection.execute(
        text(
            f"TRUNCATE TABLE {_quote_identifier(source_schema)}."
            f"{_quote_identifier(destination)}"
        )
    )
    connection.execute(
        text(
            f"INSERT INTO {_quote_identifier(source_schema)}."
            f"{_quote_identifier(destination)} ({quoted_columns}) "
            f"SELECT {quoted_columns} FROM {_quote_identifier(source_schema)}."
            f"{_quote_identifier(staging)}"
        )
    )
    destination_count = connection.scalar(
        text(
            f"SELECT count(*) FROM {_quote_identifier(source_schema)}."
            f"{_quote_identifier(destination)}"
        )
    ) or 0
    if destination_count != loaded:
        raise RuntimeError(
            f"Published row count mismatch: staging={loaded}, "
            f"destination={destination_count}"
        )
    connection.execute(
        text(
            f"DROP TABLE {_quote_identifier(source_schema)}."
            f"{_quote_identifier(staging)}"
        )
    )


StagingLoader = Callable[[Connection, Table, str, str], int]


def _migrate_with_loader(
    *,
    database_url: str,
    source_schema: str,
    control_schema: str,
    batch_table: str,
    source_kind: str,
    source_path: Path,
    source_table: str,
    destination_table: str,
    columns: list[str],
    load_staging: StagingLoader,
    source_sha256: str | None = None,
    run_table: str = "unified_migration_runs",
    migration_run_id: str | None = None,
) -> dict[str, Any]:
    _validate_source_columns(source_table, columns)
    source_schema = validate_schema_name(source_schema)
    control_schema = validate_schema_name(control_schema)
    batch_table = validate_schema_name(batch_table)
    run_table = validate_schema_name(run_table)
    destination = validate_schema_name(destination_table)
    engine = create_engine(database_url, future=True)
    _ensure_schemas_and_control(
        engine,
        source_schema,
        control_schema,
        batch_table,
        run_table,
    )
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    staging = staging_table_name(destination, batch_id)

    with engine.begin() as connection:
        connection.execute(
            text(
                f'''INSERT INTO "{control_schema}"."{batch_table}"
                (batch_id, migration_run_id, source_kind, source_path, source_sha256, source_table,
                 destination_schema, destination_table, started_at, status)
                VALUES (:batch_id, :migration_run_id, :source_kind, :source_path, :sha256, :source_table,
                        :destination_schema, :destination_table, :started_at, 'running')'''
            ),
            {
                "batch_id": batch_id,
                "migration_run_id": migration_run_id,
                "source_kind": source_kind,
                "source_path": str(source_path),
                "sha256": source_sha256,
                "source_table": source_table,
                "destination_schema": source_schema,
                "destination_table": destination,
                "started_at": started_at,
            },
        )

    source_count: int | None = None
    try:
        staging_table = _create_staging_table(engine, source_schema, staging, columns)
        migrated_at = datetime.now(timezone.utc).isoformat()
        with engine.begin() as connection:
            source_count = load_staging(connection, staging_table, batch_id, migrated_at)
            _populate_staging_metadata(
                connection,
                source_schema,
                staging,
                batch_id,
                migrated_at,
            )
            loaded = connection.scalar(select(func.count()).select_from(staging_table)) or 0
            if loaded != source_count:
                raise RuntimeError(
                    f"Row count mismatch: source={source_count}, staging={loaded}"
                )
            _publish_staging(connection, source_schema, staging, destination, loaded)
            connection.execute(
                text(
                    f'''UPDATE "{control_schema}"."{batch_table}"
                    SET completed_at=:completed_at, source_row_count=:count,
                        destination_row_count=:count, status='completed'
                    WHERE batch_id=:batch_id'''
                ),
                {"completed_at": datetime.now(timezone.utc), "count": loaded, "batch_id": batch_id},
            )
        return {
            "batch_id": batch_id,
            "migration_run_id": migration_run_id,
            "source_table": source_table,
            "destination": f"{source_schema}.{destination}",
            "source_rows": source_count,
            "destination_rows": loaded,
            "status": "completed",
        }
    except Exception as exc:
        with engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{source_schema}"."{staging}"'))
            connection.execute(
                text(
                    f'''UPDATE "{control_schema}"."{batch_table}"
                    SET completed_at=:completed_at, source_row_count=:count,
                        status='failed', error_message=:error
                    WHERE batch_id=:batch_id'''
                ),
                {
                    "completed_at": datetime.now(timezone.utc),
                    "count": source_count,
                    "error": str(exc)[:4000],
                    "batch_id": batch_id,
                },
            )
        raise
    finally:
        engine.dispose()


def migrate_rows(
    *,
    database_url: str,
    source_schema: str,
    control_schema: str,
    batch_table: str,
    source_kind: str,
    source_path: Path,
    source_table: str,
    destination_table: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    source_sha256: str | None = None,
    run_table: str = "unified_migration_runs",
    migration_run_id: str | None = None,
) -> dict[str, Any]:
    """Load a small in-memory row collection through the shared publisher."""

    def load_staging(
        connection: Connection,
        staging_table: Table,
        batch_id: str,
        migrated_at: str,
    ) -> int:
        for offset in range(0, len(rows), 500):
            payload = []
            for index, row in enumerate(rows[offset : offset + 500], start=offset + 1):
                item = {column: _string_value(row.get(column)) for column in columns}
                item.update(
                    {
                        "_migration_batch_id": batch_id,
                        "_migrated_at": migrated_at,
                        "_source_row_number": str(index),
                    }
                )
                payload.append(item)
            if payload:
                connection.execute(insert(staging_table), payload)
        return len(rows)

    return _migrate_with_loader(
        database_url=database_url,
        source_schema=source_schema,
        control_schema=control_schema,
        batch_table=batch_table,
        run_table=run_table,
        migration_run_id=migration_run_id,
        source_kind=source_kind,
        source_path=source_path,
        source_sha256=source_sha256,
        source_table=source_table,
        destination_table=destination_table,
        columns=columns,
        load_staging=load_staging,
    )


def migrate_csv_file(
    *,
    database_url: str,
    source_schema: str,
    control_schema: str,
    batch_table: str,
    source_kind: str,
    source_path: Path,
    source_table: str,
    destination_table: str,
    csv_path: Path,
    source_sha256: str | None = None,
    run_table: str = "unified_migration_runs",
    migration_run_id: str | None = None,
) -> dict[str, Any]:
    """Stream a UTF-8 CSV file into PostgreSQL with psycopg COPY."""
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        columns = next(reader, [])
    _validate_source_columns(source_table, columns)

    def load_staging(
        connection: Connection,
        staging_table: Table,
        batch_id: str,
        migrated_at: str,
    ) -> int:
        copy_sql = (
            f"COPY {_quote_identifier(source_schema)}."
            f"{_quote_identifier(str(staging_table.name))} "
            f"({', '.join(_quote_identifier(column) for column in columns)}) "
            "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '\\N', ENCODING 'UTF8')"
        )
        driver_connection = connection.connection.driver_connection
        with csv_path.open("rb") as handle:
            with driver_connection.cursor() as cursor, cursor.copy(copy_sql) as copy:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    copy.write(chunk)
        return connection.scalar(select(func.count()).select_from(staging_table)) or 0

    return _migrate_with_loader(
        database_url=database_url,
        source_schema=source_schema,
        control_schema=control_schema,
        batch_table=batch_table,
        run_table=run_table,
        migration_run_id=migration_run_id,
        source_kind=source_kind,
        source_path=source_path,
        source_sha256=source_sha256,
        source_table=source_table,
        destination_table=destination_table,
        columns=columns,
        load_staging=load_staging,
    )
