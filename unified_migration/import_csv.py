from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from unified_migration.pipeline_lock import pipeline_lock
from unified_migration.postgres_writer import (
    finish_migration_run, migrate_csv_file, source_checksum, start_migration_run,
)
from unified_migration.settings import settings
from unified_migration.table_config import DEFAULT_CONFIG, TableConfig, load_table_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load one or more configured Unified DB CSV files into PostgreSQL."
    )
    parser.add_argument(
        "csv", type=Path, nargs="+",
        help=("CSV file(s), or a directory containing CSV files. Names must be "
              "<source_table>.csv or <source_table>__<label>.csv."),
    )
    parser.add_argument(
        "--table", help="Source table override for one CSV file (backward compatible)."
    )
    parser.add_argument("--table-config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--source-schema", default=settings.source_schema)
    parser.add_argument("--control-schema", default=settings.control_schema)
    parser.add_argument("--batch-table", default=settings.batch_table)
    parser.add_argument("--run-table", default=settings.run_table)
    return parser.parse_args()


def _collect_csv_paths(inputs: list[Path]) -> list[Path]:
    paths: list[Path] = []
    for input_path in inputs:
        resolved = input_path.resolve()
        if resolved.is_dir():
            paths.extend(sorted(resolved.glob("*.csv"), key=lambda path: path.name.casefold()))
        elif resolved.is_file() and resolved.suffix.casefold() == ".csv":
            paths.append(resolved)
        else:
            raise ValueError(f"CSV input not found or not a .csv file: {resolved}")
    if not paths:
        raise ValueError("No CSV files were found in the supplied input")
    duplicate_paths = {path for path in paths if paths.count(path) > 1}
    if duplicate_paths:
        raise ValueError(
            "CSV input was supplied more than once: "
            + ", ".join(str(path) for path in sorted(duplicate_paths))
        )
    return paths


def _config_for_csv(path: Path, configs: list[TableConfig]) -> TableConfig:
    matches = [
        config for config in configs
        if path.stem == config.source_table
        or path.stem.startswith(f"{config.source_table}__")
    ]
    if len(matches) != 1:
        raise ValueError(
            f"CSV file {path.name!r} does not identify exactly one source_table in tables.json"
        )
    return matches[0]


def resolve_csv_inputs(
    inputs: list[Path], configs: list[TableConfig], table_override: str | None = None,
) -> list[tuple[TableConfig, Path]]:
    paths = _collect_csv_paths(inputs)
    if table_override:
        if len(paths) != 1:
            raise ValueError("--table can only be used with one CSV file")
        matching = [config for config in configs if config.source_table == table_override]
        if not matching:
            raise ValueError(f"Table is not configured in tables.json: {table_override}")
        return [(matching[0], paths[0])]

    resolved = [(_config_for_csv(path, configs), path) for path in paths]
    names = [config.source_table for config, _ in resolved]
    duplicate_tables = {name for name in names if names.count(name) > 1}
    if duplicate_tables:
        raise ValueError(
            "Only one CSV may be supplied per source table: "
            + ", ".join(sorted(duplicate_tables))
        )
    return resolved


def _input_set_checksum(csv_inputs: list[tuple[TableConfig, Path]]) -> str:
    digest = hashlib.sha256()
    for config, path in csv_inputs:
        digest.update(config.source_table.encode("utf-8"))
        digest.update(b"\0")
        digest.update(source_checksum(path).encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def main() -> None:
    args = parse_args()
    try:
        csv_inputs = resolve_csv_inputs(
            args.csv, load_table_config(args.table_config), args.table
        )
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Invalid CSV migration input: {exc}") from exc

    manifest = [
        {"source_table": config.source_table,
         "destination_table": config.destination_table,
         "csv_file": str(path)}
        for config, path in csv_inputs
    ]
    run_source = csv_inputs[0][1] if len(csv_inputs) == 1 else Path("multiple-csv-files")
    summaries = []
    with pipeline_lock(args.database_url):
        run_id = start_migration_run(
            database_url=args.database_url, source_schema=args.source_schema,
            control_schema=args.control_schema, batch_table=args.batch_table,
            run_table=args.run_table, source_kind="csv", source_path=run_source,
            source_sha256=_input_set_checksum(csv_inputs), selected_tables=manifest,
        )
        try:
            for table_config, csv_path in csv_inputs:
                summaries.append(migrate_csv_file(
                    database_url=args.database_url, source_schema=args.source_schema,
                    control_schema=args.control_schema, batch_table=args.batch_table,
                    run_table=args.run_table, migration_run_id=run_id,
                    source_kind="csv", source_path=csv_path,
                    source_sha256=source_checksum(csv_path),
                    source_table=table_config.source_table,
                    destination_table=table_config.destination_table, csv_path=csv_path,
                ))
        except Exception as exc:
            finish_migration_run(
                database_url=args.database_url, control_schema=args.control_schema,
                batch_table=args.batch_table, run_table=args.run_table,
                run_id=run_id, succeeded=False, error_message=str(exc),
            )
            raise
        run_summary = finish_migration_run(
            database_url=args.database_url, control_schema=args.control_schema,
            batch_table=args.batch_table, run_table=args.run_table,
            run_id=run_id, succeeded=True,
        )
    print(json.dumps({"run": run_summary, "tables": summaries}, indent=2))


if __name__ == "__main__":
    main()
