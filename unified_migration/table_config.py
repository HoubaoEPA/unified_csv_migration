from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from unified_migration.postgres_writer import POSTGRES_IDENTIFIER_LIMIT, postgres_table_name


DEFAULT_CONFIG = Path(__file__).with_name("tables.json")
VALID_DESTINATION = re.compile(r"^[a-z_][a-z0-9_]*$")
RESERVED_DESTINATIONS = {
    "migration_batches",
    "migration_runs",
    "unified_migration_batches",
    "unified_migration_runs",
}


@dataclass(frozen=True)
class TableConfig:
    source_table: str
    destination_table: str
    enabled: bool = True


def _boolean(value: object, *, field: str, source_table: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} for {source_table!r} must be true or false")
    return value


def load_table_config(path: Path = DEFAULT_CONFIG) -> list[TableConfig]:
    if not path.is_file():
        raise FileNotFoundError(f"Migration table configuration not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("tables") if isinstance(payload, dict) else None
    if not isinstance(entries, list) or not entries:
        raise ValueError("Migration table configuration requires a non-empty 'tables' list")

    configs: list[TableConfig] = []
    source_names: set[str] = set()
    destination_names: set[str] = set()
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"Table configuration entry {index} must be an object")
        source = entry.get("source_table")
        if not isinstance(source, str) or not source.strip():
            raise ValueError(f"Table configuration entry {index} requires source_table")
        source = source.strip()
        destination = entry.get("destination_table")
        if destination is None:
            destination = f"unified_src_{postgres_table_name(source)}"
        if not isinstance(destination, str) or not destination.strip():
            raise ValueError(f"destination_table for {source!r} must be a non-empty string")
        destination = destination.strip()
        enabled = _boolean(entry.get("enabled", True), field="enabled", source_table=source)

        if not VALID_DESTINATION.fullmatch(destination):
            raise ValueError(
                f"Destination {destination!r} must use lowercase PostgreSQL snake_case"
            )
        if len(destination.encode("utf-8")) > POSTGRES_IDENTIFIER_LIMIT:
            raise ValueError(
                f"Destination {destination!r} exceeds PostgreSQL's "
                f"{POSTGRES_IDENTIFIER_LIMIT}-byte identifier limit"
            )
        if (
            destination in RESERVED_DESTINATIONS
            or destination.startswith("_staging_")
            or not destination.startswith("unified_src_")
        ):
            raise ValueError(
                f"Destination {destination!r} is unsafe; Unified landing tables "
                "must use the 'unified_src_' prefix"
            )
        if source in source_names:
            raise ValueError(f"Duplicate source_table in migration configuration: {source}")
        if destination in destination_names:
            raise ValueError(f"Duplicate destination_table in migration configuration: {destination}")

        source_names.add(source)
        destination_names.add(destination)
        configs.append(
            TableConfig(
                source_table=source,
                destination_table=destination,
                enabled=enabled,
            )
        )
    return configs


def select_table_configs(configs: list[TableConfig], selected: list[str] | None) -> list[TableConfig]:
    by_source = {config.source_table: config for config in configs}
    if selected:
        unknown = [source for source in selected if source not in by_source]
        if unknown:
            raise ValueError(
                "Tables are not configured in tables.json: " + ", ".join(unknown)
            )
        if len(selected) != len(set(selected)):
            raise ValueError("A source table may be selected only once per migration run")
        return [by_source[source] for source in selected]
    return [config for config in configs if config.enabled]
