from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import create_engine, text

from unified_migration.pipeline_lock import pipeline_lock
from unified_migration.postgres_writer import _quote_identifier, validate_schema_name
from unified_migration.settings import settings


def _batch_suffix(batch_id: str) -> str:
    return batch_id.replace("-", "")[:12]


def recover_migration_state(
    *,
    database_url: str,
    source_schema: str,
    control_schema: str,
    batch_table: str,
    run_table: str,
    stale_hours: float = 24,
    apply: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Report or recover stale migrations and their orphan staging tables."""
    if stale_hours <= 0:
        raise ValueError("stale_hours must be greater than zero")
    source_schema = validate_schema_name(source_schema)
    control_schema = validate_schema_name(control_schema)
    batch_table = validate_schema_name(batch_table)
    run_table = validate_schema_name(run_table)
    checked_at = now or datetime.now(timezone.utc)
    cutoff = checked_at - timedelta(hours=stale_hours)
    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            batches = connection.execute(
                text(
                    f'''SELECT batch_id, status, started_at
                        FROM {_quote_identifier(control_schema)}.
                             {_quote_identifier(batch_table)}'''
                )
            ).mappings().all()
            stale_batches = [
                row for row in batches
                if row["status"] == "running" and row["started_at"] < cutoff
            ]
            stale_batch_ids = {row["batch_id"] for row in stale_batches}
            batch_by_suffix = {_batch_suffix(row["batch_id"]): row for row in batches}

            stale_runs = connection.execute(
                text(
                    f'''SELECT run_id, started_at
                        FROM {_quote_identifier(control_schema)}.
                             {_quote_identifier(run_table)}
                        WHERE status = 'running' AND started_at < :cutoff
                        ORDER BY started_at'''
                ),
                {"cutoff": cutoff},
            ).mappings().all()
            staging_tables = connection.execute(
                text(
                    """SELECT table_name
                       FROM information_schema.tables
                       WHERE table_schema = :schema
                         AND table_type = 'BASE TABLE'
                         AND table_name LIKE '\\_staging\\_%' ESCAPE '\\'
                       ORDER BY table_name"""
                ),
                {"schema": source_schema},
            ).scalars().all()

            cleanup_tables: list[str] = []
            protected_tables: list[str] = []
            for table_name in staging_tables:
                suffix = table_name.rsplit("_", 1)[-1]
                batch = batch_by_suffix.get(suffix)
                if batch is None or batch["status"] != "running":
                    cleanup_tables.append(table_name)
                elif batch["batch_id"] in stale_batch_ids:
                    cleanup_tables.append(table_name)
                else:
                    protected_tables.append(table_name)

            if apply:
                failure = "Recovery marked this migration stale after the configured timeout"
                for batch_id in stale_batch_ids:
                    connection.execute(
                        text(
                            f'''UPDATE {_quote_identifier(control_schema)}.
                                      {_quote_identifier(batch_table)}
                                SET status = 'failed', completed_at = :completed_at,
                                    error_message = :error
                                WHERE batch_id = :batch_id AND status = 'running' '''
                        ),
                        {
                            "completed_at": checked_at,
                            "error": failure,
                            "batch_id": batch_id,
                        },
                    )
                for run in stale_runs:
                    counts = connection.execute(
                        text(
                            f'''SELECT
                                count(*) FILTER (WHERE status = 'completed'),
                                count(*) FILTER (WHERE status = 'failed')
                                FROM {_quote_identifier(control_schema)}.
                                     {_quote_identifier(batch_table)}
                                WHERE migration_run_id = :run_id'''
                        ),
                        {"run_id": run["run_id"]},
                    ).one()
                    connection.execute(
                        text(
                            f'''UPDATE {_quote_identifier(control_schema)}.
                                      {_quote_identifier(run_table)}
                                SET status = 'failed', completed_at = :completed_at,
                                    completed_table_count = :completed_count,
                                    failed_table_count = :failed_count,
                                    error_message = :error
                                WHERE run_id = :run_id AND status = 'running' '''
                        ),
                        {
                            "completed_at": checked_at,
                            "completed_count": counts[0],
                            "failed_count": counts[1],
                            "error": failure,
                            "run_id": run["run_id"],
                        },
                    )
                for table_name in cleanup_tables:
                    connection.execute(
                        text(
                            f"DROP TABLE {_quote_identifier(source_schema)}."
                            f"{_quote_identifier(table_name)}"
                        )
                    )

        return {
            "mode": "apply" if apply else "dry-run",
            "checked_at": checked_at.isoformat(),
            "stale_before": cutoff.isoformat(),
            "stale_run_ids": [row["run_id"] for row in stale_runs],
            "stale_batch_ids": sorted(stale_batch_ids),
            "staging_tables_to_remove": cleanup_tables,
            "protected_staging_tables": protected_tables,
        }
    finally:
        engine.dispose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report or recover stale unified migration state."
    )
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--stale-hours", type=float, default=24)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply recovery. Without this flag the command is read-only.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with pipeline_lock(args.database_url):
        summary = recover_migration_state(
            database_url=args.database_url,
            source_schema=settings.source_schema,
            control_schema=settings.control_schema,
            batch_table=settings.batch_table,
            run_table=settings.run_table,
            stale_hours=args.stale_hours,
            apply=args.apply,
        )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
