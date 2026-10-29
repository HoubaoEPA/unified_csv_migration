# Unified DB CSV Migration

This package loads configured CSV table exports into the existing PostgreSQL
landing tables. CSV is the only source format.

The loader does not perform ChemDash endpoint normalization. That remains the
responsibility of the separate `etl/` package.

## Table configuration

`unified_migration/tables.json` maps each CSV source name to its established
PostgreSQL destination:

```json
{
  "tables": [
    {
      "source_table": "Cancer_UnifiedDB_IUCLID_Table_Combo",
      "destination_table": "unified_src_cancer",
      "enabled": true
    }
  ]
}
```

Keep the existing `destination_table` values unchanged. They identify the
already-established PostgreSQL landing tables. Adding a new mapping does not
change PostgreSQL by itself, but its first import creates a landing table, so
coordinate any new table with the database owner.

CSV columns are loaded as PostgreSQL `text`. Original column names and null
values are preserved. The loader also maintains `_migration_batch_id`,
`_migrated_at`, and `_source_row_number` metadata. If an existing destination's
columns differ from its CSV header, the load stops and leaves the published
table unchanged; it never alters that schema automatically.

## Setup

From the repository root, create an isolated environment and install the tool:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
Copy-Item .env.example .env.migration
```

Set `MIGRATION_DATABASE_URL` in `.env.migration`. The default source and control
schema is `chemdash`.

## Import CSV files

Name each file after its `source_table` in `tables.json`:

```text
Cancer_UnifiedDB_IUCLID_Table_Combo.csv
Developmental_UnifiedDB_IUCLID_Table_Combo.csv
suggestedOHT_Lookup.csv
```

A suffix after two underscores is also accepted, which is useful for dated
exports such as `suggestedOHT_Lookup__20261028.csv`.

Load every CSV in a directory in one tracked migration run:

```powershell
python -m unified_migration.import_csv C:\data\unified-db
```

Or pass selected files explicitly:

```powershell
python -m unified_migration.import_csv `
  C:\data\unified-db\Cancer_UnifiedDB_IUCLID_Table_Combo.csv `
  C:\data\unified-db\suggestedOHT_Lookup.csv
```

For compatibility, one arbitrarily named file can be mapped explicitly:

```powershell
python -m unified_migration.import_csv C:\data\latest.csv `
  --table suggestedOHT_Lookup
```

The command rejects unknown files, duplicate files for one source table, and
schema mismatches before publishing. Data is streamed through PostgreSQL
`COPY`. Existing tables are refilled transactionally rather than dropped, so
their identity, dependent views, indexes, constraints, and grants are retained.

Migration holds the shared ChemDash PostgreSQL advisory lock while replacing
tables. Each invocation records one `unified_migration_runs` row and a
`unified_migration_batches` row for each CSV.

## PostgreSQL-to-PostgreSQL transfer

The existing transfer workflow remains available and does not alter schemas:

```powershell
python -m unified_migration.transfer_postgres --dry-run
python -m unified_migration.transfer_postgres
```

Configure `MIGRATION_TRANSFER_SOURCE_DATABASE_URL` and
`MIGRATION_TRANSFER_DESTINATION_DATABASE_URL` in `.env.migration`. Use
`--table <source_table>` to transfer one configured table.

## Verification and recovery

Run unit checks:

```powershell
python -m unittest unified_migration.test_migration
```

PostgreSQL integration tests require an explicitly configured non-production
test database:

```powershell
$env:MIGRATION_TEST_DATABASE_URL = "postgresql+psycopg://user:password@host:5432/test_database"
python -m unittest unified_migration.test_postgres_integration
Remove-Item Env:MIGRATION_TEST_DATABASE_URL
```

Inspect interrupted runs without modifying anything:

```powershell
python -m unified_migration.recover
```

After reviewing the output, stale state can be recovered explicitly with
`python -m unified_migration.recover --stale-hours 24 --apply`.
