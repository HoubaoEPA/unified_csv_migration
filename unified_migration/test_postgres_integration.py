from __future__ import annotations

import json
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

from unified_migration.postgres_writer import (
    finish_migration_run,
    migrate_csv_file,
    migrate_rows,
    start_migration_run,
    staging_table_name,
)
from unified_migration.recover import recover_migration_state
from unified_migration.table_config import TableConfig
from unified_migration.transfer_postgres import transfer_postgres


DATABASE_URL = os.getenv("MIGRATION_TEST_DATABASE_URL")


@unittest.skipUnless(
    DATABASE_URL,
    "Set MIGRATION_TEST_DATABASE_URL to run PostgreSQL integration tests",
)
class PostgreSQLMigrationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = f"migration_it_{uuid.uuid4().hex[:12]}"
        cls.engine = create_engine(DATABASE_URL, future=True)
        with cls.engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{cls.schema}"'))

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            with cls.engine.begin() as connection:
                connection.execute(text(f'DROP SCHEMA "{cls.schema}" CASCADE'))
        finally:
            cls.engine.dispose()

    def migrate(
        self,
        destination: str,
        columns: list[str],
        rows: list[dict[str, str | None]],
        migration_run_id: str | None = None,
    ) -> dict[str, object]:
        return migrate_rows(
            database_url=DATABASE_URL,
            source_schema=self.schema,
            control_schema=self.schema,
            batch_table="migration_batches",
            run_table="migration_runs",
            migration_run_id=migration_run_id,
            source_kind="integration_test",
            source_path=Path("integration-test.csv"),
            source_sha256="integration-test",
            source_table=destination,
            destination_table=destination,
            columns=columns,
            rows=rows,
        )

    def test_run_tracks_the_selected_table_manifest(self) -> None:
        manifest = [
            {
                "source_table": "FirstSource",
                "destination_table": "unified_src_run_first",
            },
            {
                "source_table": "DifferentSource",
                "destination_table": "unified_src_run_different",
            },
        ]
        run_id = start_migration_run(
            database_url=DATABASE_URL,
            source_schema=self.schema,
            control_schema=self.schema,
            batch_table="migration_batches",
            run_table="migration_runs",
            source_kind="integration_test",
            source_path=Path("integration-test.csv"),
            source_sha256="integration-test",
            selected_tables=manifest,
        )
        self.migrate(
            "unified_src_run_first",
            ["RecordID", "Value"],
            [{"RecordID": "1", "Value": "first"}],
            migration_run_id=run_id,
        )
        self.migrate(
            "unified_src_run_different",
            ["Code", "Description", "OptionalField"],
            [{"Code": "A", "Description": "different", "OptionalField": None}],
            migration_run_id=run_id,
        )
        summary = finish_migration_run(
            database_url=DATABASE_URL,
            control_schema=self.schema,
            batch_table="migration_batches",
            run_table="migration_runs",
            run_id=run_id,
            succeeded=True,
        )
        self.assertEqual(
            summary,
            {
                "run_id": run_id,
                "status": "completed",
                "expected_tables": 2,
                "completed_tables": 2,
                "failed_tables": 0,
            },
        )
        with self.engine.connect() as connection:
            stored = connection.execute(
                text(
                    f'''SELECT selected_tables, status, expected_table_count,
                               completed_table_count, failed_table_count
                        FROM "{self.schema}"."migration_runs"
                        WHERE run_id = :run_id'''
                ),
                {"run_id": run_id},
            ).one()
            linked_batches = connection.scalar(
                text(
                    f'''SELECT count(*) FROM "{self.schema}"."migration_batches"
                        WHERE migration_run_id = :run_id'''
                ),
                {"run_id": run_id},
            )
        self.assertEqual(stored[0], json.dumps(manifest))
        self.assertEqual(stored[1:], ("completed", 2, 2, 0))
        self.assertEqual(linked_batches, 2)

    def test_recovery_is_dry_run_first_and_protects_recent_work(self) -> None:
        now = datetime.now(timezone.utc)
        stale_run_id = str(uuid.uuid4())
        recent_run_id = str(uuid.uuid4())
        stale_batch_id = str(uuid.uuid4())
        recent_batch_id = str(uuid.uuid4())
        stale_staging = staging_table_name("unified_src_stale", stale_batch_id)
        recent_staging = staging_table_name("unified_src_recent", recent_batch_id)
        orphan_staging = "_staging_unmatched_123456789abc"
        with self.engine.begin() as connection:
            for run_id, started_at in (
                (stale_run_id, now - timedelta(hours=25)),
                (recent_run_id, now - timedelta(hours=1)),
            ):
                connection.execute(
                    text(
                        f'''INSERT INTO "{self.schema}"."migration_runs"
                            (run_id, source_kind, source_path, selected_tables,
                             expected_table_count, started_at, status)
                            VALUES (:run_id, 'recovery_test', 'test', '[]', 1,
                                    :started_at, 'running')'''
                    ),
                    {"run_id": run_id, "started_at": started_at},
                )
            for batch_id, run_id, destination, started_at in (
                (
                    stale_batch_id,
                    stale_run_id,
                    "unified_src_stale",
                    now - timedelta(hours=25),
                ),
                (
                    recent_batch_id,
                    recent_run_id,
                    "unified_src_recent",
                    now - timedelta(hours=1),
                ),
            ):
                connection.execute(
                    text(
                        f'''INSERT INTO "{self.schema}"."migration_batches"
                            (batch_id, migration_run_id, source_kind, source_path,
                             source_table, destination_schema, destination_table,
                             started_at, status)
                            VALUES (:batch_id, :run_id, 'recovery_test', 'test',
                                    'source', :schema, :destination,
                                    :started_at, 'running')'''
                    ),
                    {
                        "batch_id": batch_id,
                        "run_id": run_id,
                        "schema": self.schema,
                        "destination": destination,
                        "started_at": started_at,
                    },
                )
            for table_name in (stale_staging, recent_staging, orphan_staging):
                connection.execute(
                    text(
                        f'CREATE TABLE "{self.schema}"."{table_name}" '
                        "(value text)"
                    )
                )

        arguments = {
            "database_url": DATABASE_URL,
            "source_schema": self.schema,
            "control_schema": self.schema,
            "batch_table": "migration_batches",
            "run_table": "migration_runs",
            "stale_hours": 24,
            "now": now,
        }
        dry_run = recover_migration_state(**arguments)
        self.assertEqual(dry_run["mode"], "dry-run")
        self.assertEqual(dry_run["stale_run_ids"], [stale_run_id])
        self.assertEqual(dry_run["stale_batch_ids"], [stale_batch_id])
        self.assertEqual(
            set(dry_run["staging_tables_to_remove"]),
            {stale_staging, orphan_staging},
        )
        self.assertEqual(dry_run["protected_staging_tables"], [recent_staging])
        with self.engine.connect() as connection:
            self.assertEqual(
                connection.scalar(
                    text(
                        f'''SELECT status FROM "{self.schema}"."migration_runs"
                            WHERE run_id = :run_id'''
                    ),
                    {"run_id": stale_run_id},
                ),
                "running",
            )
            self.assertIsNotNone(
                connection.scalar(
                    text("SELECT to_regclass(:name)"),
                    {"name": f'"{self.schema}"."{stale_staging}"'},
                )
            )

        applied = recover_migration_state(**arguments, apply=True)
        self.assertEqual(applied["mode"], "apply")
        with self.engine.connect() as connection:
            statuses = connection.execute(
                text(
                    f'''SELECT run_id, status FROM "{self.schema}"."migration_runs"
                        WHERE run_id IN (:stale, :recent) ORDER BY run_id'''
                ),
                {"stale": stale_run_id, "recent": recent_run_id},
            ).all()
            batch_statuses = connection.execute(
                text(
                    f'''SELECT batch_id, status FROM "{self.schema}"."migration_batches"
                        WHERE batch_id IN (:stale, :recent) ORDER BY batch_id'''
                ),
                {"stale": stale_batch_id, "recent": recent_batch_id},
            ).all()
            remaining = {
                name: connection.scalar(
                    text("SELECT to_regclass(:name)"),
                    {"name": f'"{self.schema}"."{name}"'},
                )
                for name in (stale_staging, recent_staging, orphan_staging)
            }
        self.assertEqual(dict(statuses)[stale_run_id], "failed")
        self.assertEqual(dict(statuses)[recent_run_id], "running")
        self.assertEqual(dict(batch_statuses)[stale_batch_id], "failed")
        self.assertEqual(dict(batch_statuses)[recent_batch_id], "running")
        self.assertIsNone(remaining[stale_staging])
        self.assertIsNone(remaining[orphan_staging])
        self.assertIsNotNone(remaining[recent_staging])

    def test_refresh_preserves_views_and_rejects_schema_changes(self) -> None:
        first = self.migrate(
            "unified_src_first",
            ["RecordID", "Value"],
            [{"RecordID": "1", "Value": "old"}],
        )
        self.assertEqual(first["status"], "completed")

        with self.engine.begin() as connection:
            original_oid = connection.scalar(
                text("SELECT CAST(to_regclass(:name) AS oid)"),
                {"name": f'"{self.schema}"."unified_src_first"'},
            )
            connection.execute(
                text(
                    f'CREATE VIEW "{self.schema}"."dependent_view" AS '
                    f'SELECT "RecordID", "Value" '
                    f'FROM "{self.schema}"."unified_src_first"'
                )
            )

        second = self.migrate(
            "unified_src_first",
            ["RecordID", "Value"],
            [
                {"RecordID": "2", "Value": "new"},
                {"RecordID": "3", "Value": "newer"},
            ],
        )
        self.assertEqual(second["destination_rows"], 2)

        with self.engine.connect() as connection:
            refreshed_oid = connection.scalar(
                text("SELECT CAST(to_regclass(:name) AS oid)"),
                {"name": f'"{self.schema}"."unified_src_first"'},
            )
            view_rows = connection.execute(
                text(
                    f'SELECT "RecordID", "Value" '
                    f'FROM "{self.schema}"."dependent_view" ORDER BY "RecordID"'
                )
            ).all()
        self.assertEqual(refreshed_oid, original_oid)
        self.assertEqual(view_rows, [("2", "new"), ("3", "newer")])

        with self.assertRaisesRegex(RuntimeError, "controlled schema migration"):
            self.migrate(
                "unified_src_first",
                ["RecordID", "Value", "NewColumn"],
                [{"RecordID": "4", "Value": "unsafe", "NewColumn": "x"}],
            )

        with self.engine.connect() as connection:
            preserved_rows = connection.execute(
                text(
                    f'SELECT "RecordID", "Value" '
                    f'FROM "{self.schema}"."dependent_view" ORDER BY "RecordID"'
                )
            ).all()
        self.assertEqual(preserved_rows, [("2", "new"), ("3", "newer")])

        different = self.migrate(
            "unified_src_different",
            ["Code", "Description", "OptionalField"],
            [{"Code": "A", "Description": "Different schema", "OptionalField": None}],
        )
        self.assertEqual(different["destination_rows"], 1)

    def test_postgres_transfer_creates_refreshes_and_skips_current_table(self) -> None:
        destination_schema = f"transfer_it_{uuid.uuid4().hex[:12]}"
        table = "unified_src_transfer_values"
        config = TableConfig(
            source_table="TransferValues",
            destination_table=table,
        )
        batch_one = str(uuid.uuid4())
        with self.engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{destination_schema}"'))
            connection.execute(
                text(
                    f'''CREATE TABLE "{self.schema}"."{table}" (
                            "RecordID" text NOT NULL,
                            "Value" text,
                            _migration_batch_id text NOT NULL,
                            _migrated_at text NOT NULL,
                            _source_row_number text NOT NULL
                        )'''
                )
            )
            connection.execute(
                text(
                    f'''INSERT INTO "{self.schema}"."{table}"
                        VALUES ('1', NULL, :batch, '2026-01-01T00:00:00Z', '1'),
                               ('2', '', :batch, '2026-01-01T00:00:00Z', '2'),
                               ('3', :value, :batch, '2026-01-01T00:00:00Z', '3')'''
                ),
                {"batch": batch_one, "value": "multiline\nUnicode Ω"},
            )

        arguments = {
            "source_database_url": DATABASE_URL,
            "destination_database_url": DATABASE_URL,
            "source_schema": self.schema,
            "destination_schema": destination_schema,
            "control_schema": destination_schema,
            "transfer_run_table": "transfer_runs",
            "transfer_batch_table": "transfer_batches",
            "table_configs": [config],
        }
        try:
            dry_run = transfer_postgres(**arguments, dry_run=True)
            self.assertEqual(dry_run["tables"][0]["action"], "create")

            created = transfer_postgres(**arguments)
            self.assertEqual(created["status"], "completed")
            self.assertEqual(created["tables"][0]["destination_rows_after"], 3)
            with self.engine.begin() as connection:
                original_oid = connection.scalar(
                    text("SELECT CAST(to_regclass(:name) AS oid)"),
                    {"name": f'"{destination_schema}"."{table}"'},
                )
                rows = connection.execute(
                    text(
                        f'''SELECT "RecordID", "Value"
                            FROM "{destination_schema}"."{table}"
                            ORDER BY "RecordID"'''
                    )
                ).all()
                connection.execute(
                    text(
                        f'''CREATE VIEW "{destination_schema}"."transfer_view" AS
                            SELECT "RecordID", "Value"
                            FROM "{destination_schema}"."{table}"'''
                    )
                )
            self.assertEqual(
                rows,
                [("1", None), ("2", ""), ("3", "multiline\nUnicode Ω")],
            )

            skipped = transfer_postgres(**arguments)
            self.assertEqual(skipped["tables"][0]["action"], "skip_already_current")

            batch_two = str(uuid.uuid4())
            with self.engine.begin() as connection:
                connection.execute(text(f'TRUNCATE "{self.schema}"."{table}"'))
                connection.execute(
                    text(
                        f'''INSERT INTO "{self.schema}"."{table}"
                            VALUES ('4', 'replacement', :batch,
                                    '2026-01-02T00:00:00Z', '1')'''
                    ),
                    {"batch": batch_two},
                )
            refreshed = transfer_postgres(**arguments)
            self.assertEqual(refreshed["tables"][0]["action"], "refresh")
            with self.engine.connect() as connection:
                refreshed_oid = connection.scalar(
                    text("SELECT CAST(to_regclass(:name) AS oid)"),
                    {"name": f'"{destination_schema}"."{table}"'},
                )
                view_rows = connection.execute(
                    text(
                        f'''SELECT "RecordID", "Value"
                            FROM "{destination_schema}"."transfer_view"'''
                    )
                ).all()
            self.assertEqual(refreshed_oid, original_oid)
            self.assertEqual(view_rows, [("4", "replacement")])
        finally:
            with self.engine.begin() as connection:
                connection.execute(
                    text(f'DROP SCHEMA IF EXISTS "{destination_schema}" CASCADE')
                )

    def test_csv_copy_preserves_null_empty_unicode_and_multiline_text(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "source.csv"
            csv_path.write_text(
                '"RecordID","Value"\n'
                '"1",\\N\n'
                '"2",""\n'
                '"3","\\N"\n'
                '"4","comma, quote ""value"" and\nmultiline Ω"\n',
                encoding="utf-8",
                newline="",
            )
            summary = migrate_csv_file(
                database_url=DATABASE_URL,
                source_schema=self.schema,
                control_schema=self.schema,
                batch_table="migration_batches",
                run_table="migration_runs",
                source_kind="integration_test_csv",
                source_path=csv_path,
                source_sha256="integration-test",
                source_table="CsvSource",
                destination_table="unified_src_csv_values",
                csv_path=csv_path,
            )
        self.assertEqual(summary["destination_rows"], 4)

        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f'''SELECT "RecordID", "Value", _source_row_number
                        FROM "{self.schema}"."unified_src_csv_values"
                        ORDER BY "RecordID"'''
                )
            ).all()
        self.assertEqual(
            rows,
            [
                ("1", None, "1"),
                ("2", "", "2"),
                ("3", r"\N", "3"),
                ("4", 'comma, quote "value" and\nmultiline Ω', "4"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
