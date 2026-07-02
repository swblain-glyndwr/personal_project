"""Next Ads feature-store registry helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REGISTRY_PATH = (
    PROJECT_ROOT / "configs" / "features" / "nextads_feature_store.yaml"
)
REQUIRED_TABLE_FIELDS = {
    "name",
    "entity",
    "grain",
    "primary_keys",
    "source_job",
    "owner",
    "freshness",
    "training_safe",
    "consumers",
}


def normalize_schema_name(schema: str) -> str:
    """Normalise Databricks user/schema identifiers for feature-store paths."""
    local_part = schema.split("@", maxsplit=1)[0]
    normalized = re.sub(r"[^a-z0-9]+", "_", local_part.lower()).strip("_")
    if not normalized:
        raise ValueError(f"Invalid empty schema after normalisation: {schema}")
    return normalized


@dataclass(frozen=True)
class FeatureTableSpec:
    """Validated feature table registry entry."""

    name: str
    entity: str
    grain: str
    primary_keys: tuple[str, ...]
    source_job: str
    owner: str
    freshness: str
    training_safe: bool
    consumers: tuple[str, ...]
    timestamp_key: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "FeatureTableSpec":
        missing_fields = sorted(REQUIRED_TABLE_FIELDS - set(raw))
        if missing_fields:
            raise ValueError(
                f"Feature table {raw.get('name', '<unknown>')} is missing "
                f"required fields: {', '.join(missing_fields)}"
            )

        primary_keys = tuple(raw["primary_keys"])
        if not primary_keys:
            raise ValueError(f"Feature table {raw['name']} has no primary keys")

        consumers = tuple(raw["consumers"])
        if not consumers:
            raise ValueError(f"Feature table {raw['name']} has no consumers")

        return cls(
            name=str(raw["name"]),
            entity=str(raw["entity"]),
            grain=str(raw["grain"]),
            primary_keys=primary_keys,
            timestamp_key=raw.get("timestamp_key"),
            source_job=str(raw["source_job"]),
            owner=str(raw["owner"]),
            freshness=str(raw["freshness"]),
            training_safe=bool(raw["training_safe"]),
            consumers=consumers,
        )


@dataclass(frozen=True)
class FeatureStoreRegistry:
    """Parsed Next Ads feature-store registry."""

    name: str
    description: str
    default_catalog: str
    default_schema: str
    table_root: Path
    physical_tables: tuple[FeatureTableSpec, ...]
    compatibility_views: tuple[dict[str, Any], ...]

    def table_names(self) -> list[str]:
        return [table.name for table in self.physical_tables]

    def table_spec(self, table_name: str) -> FeatureTableSpec:
        for table in self.physical_tables:
            if table.name == table_name:
                return table
        raise KeyError(f"Unknown feature-store table: {table_name}")

    def sql_contract_path(self, table_name: str) -> Path:
        self.table_spec(table_name)
        return self.table_root / f"create_table_{table_name}.sql"

    def resolved_table_path(
        self,
        table_name: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> str:
        self.table_spec(table_name)
        target_schema = schema or self.default_schema
        return ".".join(
            [
                catalog or self.default_catalog,
                normalize_schema_name(target_schema),
                table_name,
            ]
        )


def load_feature_store_registry(
    path: str | Path = DEFAULT_REGISTRY_PATH,
) -> FeatureStoreRegistry:
    """Load and validate the Next Ads feature-store registry."""
    registry_path = Path(path)
    raw_registry = yaml.safe_load(registry_path.read_text())
    feature_store = raw_registry["feature_store"]

    table_root = PROJECT_ROOT / feature_store["table_root"]
    physical_tables = tuple(
        FeatureTableSpec.from_dict(raw)
        for raw in feature_store.get("physical_tables", [])
    )
    compatibility_views = tuple(
        feature_store.get("compatibility_views", [])
    )

    table_names = [table.name for table in physical_tables]
    duplicates = sorted(
        {
            table_name
            for table_name in table_names
            if table_names.count(table_name) > 1
        }
    )
    if duplicates:
        raise ValueError(
            "Duplicate feature-store table names: " + ", ".join(duplicates)
        )

    return FeatureStoreRegistry(
        name=str(feature_store["name"]),
        description=str(feature_store["description"]),
        default_catalog=str(feature_store["default_catalog"]),
        default_schema=str(feature_store["default_schema"]),
        table_root=table_root,
        physical_tables=physical_tables,
        compatibility_views=compatibility_views,
    )
