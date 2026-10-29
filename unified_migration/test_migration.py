from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from unified_migration.postgres_writer import (
    POSTGRES_IDENTIFIER_LIMIT,
    _quote_identifier,
    _require_compatible_columns,
    migrate_csv_file,
    postgres_table_name,
    staging_table_name,
    validate_schema_name,
)
from unified_migration.import_csv import resolve_csv_inputs
from unified_migration.table_config import load_table_config, select_table_configs


class MigrationUnitTests(unittest.TestCase):
    def test_csv_directory_resolves_configured_tables(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cancer = root / "Cancer_UnifiedDB_IUCLID_Table_Combo.csv"
            oht = root / "suggestedOHT_Lookup__20261028.csv"
            cancer.write_text("RecordID\n1\n", encoding="utf-8")
            oht.write_text("OHT\nOHT 72\n", encoding="utf-8")
            resolved = resolve_csv_inputs([root], load_table_config())
        self.assertEqual(
            [config.source_table for config, _ in resolved],
            ["Cancer_UnifiedDB_IUCLID_Table_Combo", "suggestedOHT_Lookup"],
        )

    def test_csv_directory_rejects_unconfigured_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unknown.csv"
            path.write_text("RecordID\n1\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not identify"):
                resolve_csv_inputs([Path(directory)], load_table_config())

    def test_csv_loader_rejects_missing_header_before_connecting(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "empty.csv"
            csv_path.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "has no columns"):
                migrate_csv_file(
                    database_url="postgresql+psycopg://invalid/unused",
                    source_schema="chemdash",
                    control_schema="chemdash",
                    batch_table="migration_batches",
                    source_kind="test",
                    source_path=csv_path,
                    source_table="Example",
                    destination_table="unified_src_example",
                    csv_path=csv_path,
                )

    def test_postgres_identifier_quoting(self) -> None:
        self.assertEqual(_quote_identifier('Dose "value"'), '"Dose ""value"""')

    def test_compatible_destination_columns_are_accepted(self) -> None:
        columns = [
            ("RecordID", "text", "YES"),
            ("_migration_batch_id", "text", "NO"),
        ]
        _require_compatible_columns("chemdash", "unified_src_cancer", columns, columns)

    def test_destination_column_mismatch_is_rejected(self) -> None:
        destination = [("RecordID", "text", "YES")]
        staging = [("RecordID", "text", "YES"), ("NewField", "text", "YES")]
        with self.assertRaisesRegex(RuntimeError, "controlled schema migration"):
            _require_compatible_columns(
                "chemdash",
                "unified_src_cancer",
                destination,
                staging,
            )

    def test_postgres_table_name(self) -> None:
        self.assertEqual(
            postgres_table_name("Cancer_UnifiedDB_IUCLID_Table_Combo"),
            "cancer_unified_db_iuclid_table_combo",
        )

    def test_staging_names_fit_postgres_identifier_limit(self) -> None:
        batch_id = "35630896-d7fe-4d71-91c8-ec924cd5109a"
        for source_table in (
            "Cancer_UnifiedDB_IUCLID_Table_Combo",
            "Developmental_UnifiedDB_IUCLID_Table_Combo",
            "suggestedOHT_Lookup",
        ):
            destination = postgres_table_name(source_table)
            staging = staging_table_name(destination, batch_id)
            self.assertLessEqual(len(staging.encode("utf-8")), POSTGRES_IDENTIFIER_LIMIT)
            self.assertTrue(staging.endswith("_35630896d7fe"))

    def test_table_config_derives_optional_destination(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "tables.json"
            config_path.write_text(
                '{"tables":[{"source_table":"AAAA_unifiedDB_IUCLID_Table_combo",'
                '"enabled":true}]}',
                encoding="utf-8",
            )
            configs = load_table_config(config_path)
        self.assertEqual(
            configs[0].destination_table,
            "unified_src_aaaa_unified_db_iuclid_table_combo",
        )

    def test_table_config_accepts_destination_override(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "tables.json"
            config_path.write_text(
                '{"tables":[{"source_table":"AAAA_unifiedDB_IUCLID_Table_combo",'
                '"destination_table":"unified_src_aaaa_records"}]}',
                encoding="utf-8",
            )
            configs = load_table_config(config_path)
        self.assertEqual(configs[0].destination_table, "unified_src_aaaa_records")

    def test_table_config_rejects_non_landing_destination(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "tables.json"
            config_path.write_text(
                '{"tables":[{"source_table":"Unsafe",'
                '"destination_table":"chemical_substance"}]}',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "unified_src_"):
                load_table_config(config_path)

    def test_unconfigured_table_selection_is_rejected(self) -> None:
        configs = load_table_config()
        with self.assertRaises(ValueError):
            select_table_configs(configs, ["Unconfigured_Table"])

    def test_duplicate_table_selection_is_rejected(self) -> None:
        configs = load_table_config()
        selected = configs[0].source_table
        with self.assertRaisesRegex(ValueError, "only once"):
            select_table_configs(configs, [selected, selected])

    def test_default_config_uses_unified_source_prefix(self) -> None:
        configs = load_table_config()
        self.assertEqual(
            {config.destination_table for config in configs},
            {
                "unified_src_cancer",
                "unified_src_developmental",
                "unified_src_oht_lookup",
            },
        )

    def test_schema_validation(self) -> None:
        self.assertEqual(validate_schema_name("unified"), "unified")
        with self.assertRaises(ValueError):
            validate_schema_name("invalid-schema")
        with self.assertRaises(ValueError):
            validate_schema_name("a" * 64)

if __name__ == "__main__":
    unittest.main()
