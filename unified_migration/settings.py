from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _dotenv_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key.strip()] = value
    return values


def _environment() -> dict[str, str]:
    root = Path(__file__).resolve().parents[1]
    explicit = os.getenv("MIGRATION_DOTENV")
    candidates = [
        Path(explicit) if explicit else None,
        root / ".env",
        root / ".env.migration",
        root / "unified_migration" / ".env",
    ]
    values: dict[str, str] = {}
    for candidate in candidates:
        if candidate is not None:
            values.update(_dotenv_values(candidate))
    values.update(os.environ)
    return values


ENV = _environment()


@dataclass(frozen=True)
class MigrationSettings:
    database_url: str = ENV.get(
        "MIGRATION_DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/oppttest",
    )
    source_schema: str = ENV.get("MIGRATION_SOURCE_SCHEMA", "chemdash")
    control_schema: str = ENV.get("MIGRATION_CONTROL_SCHEMA", "chemdash")
    batch_table: str = ENV.get("MIGRATION_BATCH_TABLE", "unified_migration_batches")
    run_table: str = ENV.get("MIGRATION_RUN_TABLE", "unified_migration_runs")
    transfer_source_database_url: str = ENV.get(
        "MIGRATION_TRANSFER_SOURCE_DATABASE_URL",
        ENV.get(
            "MIGRATION_DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/oppttest",
        ),
    )
    transfer_destination_database_url: str = ENV.get(
        "MIGRATION_TRANSFER_DESTINATION_DATABASE_URL", ""
    )
    transfer_run_table: str = ENV.get(
        "MIGRATION_TRANSFER_RUN_TABLE", "unified_transfer_runs"
    )
    transfer_batch_table: str = ENV.get(
        "MIGRATION_TRANSFER_BATCH_TABLE", "unified_transfer_batches"
    )


settings = MigrationSettings()
