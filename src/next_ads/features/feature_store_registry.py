"""Next Ads feature-store registry helpers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import hashlib
from pathlib import Path
import re
from string import Formatter
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
    "state",
}
REQUIRED_BINDING_FIELDS = {
    "environment",
    "catalog",
    "schema",
    "bundle_target",
    "repository_declared",
    "table_name_template",
}
REQUIRED_VIEW_FIELDS = {"name", "source_job", "source_feature", "consumers"}
SUPPORTED_ENVIRONMENTS = {"DEV", "PREPROD", "PROD"}


class OfflineFeatureState(StrEnum):
    """Delivery state for one logical offline feature contract."""

    ACTIVE = "ACTIVE"
    COMPATIBILITY = "COMPATIBILITY"
    SCAFFOLD = "SCAFFOLD"


def normalize_schema_name(schema: str) -> str:
    """Normalise Databricks user/schema identifiers for feature-store paths."""
    local_part = schema.split("@", maxsplit=1)[0]
    normalized = re.sub(r"[^a-z0-9]+", "_", local_part.lower()).strip("_")
    if not normalized:
        raise ValueError(f"Invalid empty schema after normalisation: {schema}")
    return normalized


def normalize_release_id(release_id: str) -> str:
    """Build a readable, collision-resistant table-name release token."""
    if not isinstance(release_id, str) or not release_id.strip():
        raise ValueError("release_id must be non-blank text")
    raw_release_id = release_id.strip()
    readable_stem = normalize_schema_name(raw_release_id)[:48]
    stable_suffix = hashlib.sha256(raw_release_id.encode("utf-8")).hexdigest()[
        :12
    ]
    return f"{readable_stem}_{stable_suffix}"


def _required_text(raw: Any, field_name: str, context: str) -> str:
    if not isinstance(raw, str):
        raise ValueError(f"{context} {field_name} must be text")
    value = raw.strip()
    if not value:
        raise ValueError(f"{context} has an empty {field_name}")
    return value


def _validated_string_tuple(
    raw: Any,
    field_name: str,
    context: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if isinstance(raw, str) or not isinstance(raw, (list, tuple)):
        raise ValueError(f"{context} {field_name} must be a list")

    if any(not isinstance(value, str) for value in raw):
        raise ValueError(f"{context} {field_name} values must be text")
    values = tuple(value.strip() for value in raw)
    if not allow_empty and not values:
        raise ValueError(f"{context} has no {field_name}")
    if any(not value for value in values):
        raise ValueError(f"{context} {field_name} contains a blank value")
    duplicates = sorted({value for value in values if values.count(value) > 1})
    if duplicates:
        raise ValueError(
            f"{context} {field_name} contains duplicates: "
            + ", ".join(duplicates)
        )
    return values


@dataclass(frozen=True)
class OfflineFeatureDefinition:
    """Environment-neutral definition of one offline feature table."""

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
    state: OfflineFeatureState = OfflineFeatureState.ACTIVE
    missing_contracts: tuple[str, ...] = ()
    builder: str | None = None
    write_mode: str = "merge"

    def __post_init__(self) -> None:
        """Default the logical builder to the legacy source-job metadata."""
        if self.builder is None:
            object.__setattr__(self, "builder", self.source_job)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "OfflineFeatureDefinition":
        missing_fields = sorted(REQUIRED_TABLE_FIELDS - set(raw))
        if missing_fields:
            raise ValueError(
                f"Feature table {raw.get('name', '<unknown>')} is missing "
                f"required fields: {', '.join(missing_fields)}"
            )

        context = f"Feature table {raw['name']}"
        primary_keys = _validated_string_tuple(
            raw["primary_keys"], "primary_keys", context
        )
        consumers = _validated_string_tuple(
            raw["consumers"], "consumers", context
        )
        if not isinstance(raw["training_safe"], bool):
            raise ValueError(f"{context} training_safe must be a boolean")

        try:
            state = OfflineFeatureState(str(raw["state"]).upper())
        except ValueError as exc:
            valid_states = ", ".join(
                state.value for state in OfflineFeatureState
            )
            raise ValueError(
                f"Feature table {raw['name']} has unsupported state "
                f"{raw['state']!r}; expected one of: {valid_states}"
            ) from exc

        missing_contracts = _validated_string_tuple(
            raw.get("missing_contracts", ()),
            "missing_contracts",
            context,
            allow_empty=True,
        )
        if state is OfflineFeatureState.SCAFFOLD and not missing_contracts:
            raise ValueError(
                f"Scaffold feature table {raw['name']} must declare "
                "missing_contracts"
            )
        if state is not OfflineFeatureState.SCAFFOLD and missing_contracts:
            raise ValueError(
                f"Implemented feature table {raw['name']} cannot declare "
                "missing_contracts"
            )

        timestamp_key = raw.get("timestamp_key")
        if timestamp_key is not None:
            timestamp_key = _required_text(
                timestamp_key, "timestamp_key", context
            )
        write_mode = str(raw.get("write_mode", "merge")).lower()
        if write_mode not in {"merge", "overwrite"}:
            raise ValueError(
                f"{context} has unsupported write_mode {write_mode!r}; "
                "expected merge or overwrite"
            )

        return cls(
            name=_required_text(raw["name"], "name", context),
            entity=_required_text(raw["entity"], "entity", context),
            grain=_required_text(raw["grain"], "grain", context),
            primary_keys=primary_keys,
            source_job=_required_text(
                raw["source_job"], "source_job", context
            ),
            owner=_required_text(raw["owner"], "owner", context),
            freshness=_required_text(raw["freshness"], "freshness", context),
            training_safe=raw["training_safe"],
            consumers=consumers,
            timestamp_key=timestamp_key,
            state=state,
            missing_contracts=missing_contracts,
            builder=(
                _required_text(raw["builder"], "builder", context)
                if "builder" in raw
                else None
            ),
            write_mode=write_mode,
        )

    @property
    def feature_id(self) -> str:
        """Return the stable logical identifier used by plans and consumers."""
        return self.name

    @property
    def implemented(self) -> bool:
        """Return whether the contract represents implemented behaviour."""
        return self.state is not OfflineFeatureState.SCAFFOLD


# Compatibility name retained for current materialisation and setup callers.
FeatureTableSpec = OfflineFeatureDefinition


@dataclass(frozen=True)
class OfflineStoreBinding:
    """Physical namespace declared for one environment."""

    environment: str
    catalog: str
    schema: str
    bundle_target: str
    repository_declared: bool
    table_name_template: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "OfflineStoreBinding":
        missing_fields = sorted(REQUIRED_BINDING_FIELDS - set(raw))
        if missing_fields:
            raise ValueError(
                "Offline store binding is missing required fields: "
                + ", ".join(missing_fields)
            )

        environment = _required_text(
            raw["environment"], "environment", "Offline store binding"
        ).upper()
        if environment not in SUPPORTED_ENVIRONMENTS:
            raise ValueError(
                f"Unsupported offline store environment: {environment}"
            )
        if not isinstance(raw["repository_declared"], bool):
            raise ValueError(
                f"Offline store binding {environment} must use a boolean "
                "repository_declared value"
            )

        text_fields = {
            field_name: _required_text(
                raw[field_name],
                field_name,
                f"Offline store binding {environment}",
            )
            for field_name in (
                "catalog",
                "schema",
                "bundle_target",
                "table_name_template",
            )
        }

        template_fields = []
        for _, field_name, format_spec, conversion in Formatter().parse(
            text_fields["table_name_template"]
        ):
            if field_name is None:
                continue
            if format_spec or conversion:
                raise ValueError(
                    f"Offline store binding {environment} table_name_template "
                    "cannot use formatting options"
                )
            template_fields.append(field_name)
        if template_fields.count("feature_id") != 1:
            raise ValueError(
                f"Offline store binding {environment} table_name_template "
                "must contain {feature_id} exactly once"
            )
        unsupported_fields = sorted(
            set(template_fields) - {"feature_id", "release_id"}
        )
        if unsupported_fields:
            raise ValueError(
                f"Offline store binding {environment} table_name_template "
                "has unsupported fields: " + ", ".join(unsupported_fields)
            )
        release_field_count = template_fields.count("release_id")
        if environment == "PREPROD" and release_field_count != 1:
            raise ValueError(
                "PREPROD offline store binding table_name_template must "
                "contain {release_id} exactly once"
            )
        if environment != "PREPROD" and release_field_count:
            raise ValueError(
                f"Offline store binding {environment} table_name_template "
                "cannot contain {release_id}"
            )

        return cls(
            environment=environment,
            catalog=text_fields["catalog"],
            schema=text_fields["schema"],
            bundle_target=text_fields["bundle_target"],
            repository_declared=raw["repository_declared"],
            table_name_template=text_fields["table_name_template"],
        )

    @property
    def repository_state(self) -> str:
        """Describe whether this job target exists in repository config."""
        return "REPO_DECLARED" if self.repository_declared else "PLANNED"

    @property
    def requires_release_id(self) -> bool:
        return "{release_id}" in self.table_name_template

    def _resolved_table_name(
        self,
        feature_id: str,
        release_id: str | None,
        *,
        allow_template: bool,
    ) -> str:
        if self.requires_release_id and release_id is None:
            if not allow_template:
                raise ValueError(
                    "PREPROD table resolution requires a release_id"
                )
            normalized_release_id = "{release_id}"
        else:
            normalized_release_id = (
                normalize_release_id(release_id) if release_id else ""
            )
        return self.table_name_template.format(
            feature_id=feature_id,
            release_id=normalized_release_id,
        )

    def table_path_template(self, feature_id: str) -> str:
        """Return the location, retaining any required release placeholder."""
        table_name = self._resolved_table_name(
            feature_id, None, allow_template=True
        )
        return ".".join(
            [self.catalog, normalize_schema_name(self.schema), table_name]
        )

    def resolved_table_path(
        self,
        feature_id: str,
        release_id: str | None = None,
    ) -> str:
        """Resolve a logical table name in this environment."""
        table_name = self._resolved_table_name(
            feature_id, release_id, allow_template=False
        )
        return ".".join(
            [self.catalog, normalize_schema_name(self.schema), table_name]
        )


@dataclass(frozen=True)
class FeatureStoreRegistry:
    """Parsed Next Ads feature-store registry."""

    name: str
    description: str
    default_catalog: str
    default_schema: str
    table_root: Path
    physical_tables: tuple[OfflineFeatureDefinition, ...]
    compatibility_views: tuple[dict[str, Any], ...]
    store_bindings: tuple[OfflineStoreBinding, ...]

    @property
    def offline_features(self) -> tuple[OfflineFeatureDefinition, ...]:
        """Return logical feature definitions in their declared order."""
        return self.physical_tables

    @property
    def implemented_features(self) -> tuple[OfflineFeatureDefinition, ...]:
        """Return implemented definitions without scaffold-only contracts."""
        return tuple(
            table for table in self.physical_tables if table.implemented
        )

    def features_for_builder(
        self,
        builder: str,
        *,
        include_scaffolds: bool = False,
    ) -> tuple[OfflineFeatureDefinition, ...]:
        """Return definitions owned by one logical builder in registry order."""
        selected = tuple(
            table
            for table in self.physical_tables
            if table.builder == builder
            and (include_scaffolds or table.implemented)
        )
        return selected

    def table_names(self) -> list[str]:
        return [table.name for table in self.physical_tables]

    def table_spec(self, table_name: str) -> OfflineFeatureDefinition:
        for table in self.physical_tables:
            if table.name == table_name:
                return table
        raise KeyError(f"Unknown feature-store table: {table_name}")

    def store_binding(self, environment: str) -> OfflineStoreBinding:
        """Return the physical binding for a named environment."""
        normalized_environment = environment.upper()
        for binding in self.store_bindings:
            if binding.environment == normalized_environment:
                return binding
        raise KeyError(
            f"Unknown offline feature-store environment: {environment}"
        )

    def sql_contract_path(self, table_name: str) -> Path:
        self.table_spec(table_name)
        return self.table_root / f"create_table_{table_name}.sql"

    def view_contract_path(self, view_name: str) -> Path:
        if view_name not in {
            str(view["name"]) for view in self.compatibility_views
        }:
            raise KeyError(
                f"Unknown feature-store compatibility view: {view_name}"
            )
        return self.table_root / f"create_view_{view_name}.sql"

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

    def resolved_binding_table_path(
        self,
        table_name: str,
        environment: str,
        release_id: str | None = None,
    ) -> str:
        """Resolve a logical definition through an environment binding."""
        self.table_spec(table_name)
        return self.store_binding(environment).resolved_table_path(
            table_name, release_id=release_id
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
        OfflineFeatureDefinition.from_dict(raw)
        for raw in feature_store.get("physical_tables", [])
    )
    compatibility_views = tuple(feature_store.get("compatibility_views", []))
    store_bindings = tuple(
        OfflineStoreBinding.from_dict(raw)
        for raw in feature_store.get("store_bindings", [])
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

    binding_environments = [binding.environment for binding in store_bindings]
    duplicate_bindings = sorted(
        {
            environment
            for environment in binding_environments
            if binding_environments.count(environment) > 1
        }
    )
    if duplicate_bindings:
        raise ValueError(
            "Duplicate offline store bindings: "
            + ", ".join(duplicate_bindings)
        )

    required_environments = {"DEV", "PREPROD", "PROD"}
    missing_environments = sorted(
        required_environments - set(binding_environments)
    )
    if missing_environments:
        raise ValueError(
            "Missing offline store bindings: "
            + ", ".join(missing_environments)
        )

    dev_binding = next(
        binding for binding in store_bindings if binding.environment == "DEV"
    )
    if dev_binding.catalog != str(
        feature_store["default_catalog"]
    ) or dev_binding.schema != str(feature_store["default_schema"]):
        raise ValueError(
            "DEV offline store binding must match the legacy default catalog "
            "and schema while existing callers use those defaults"
        )

    known_table_names = set(table_names)
    view_names = []
    view_sources = []
    for view in compatibility_views:
        if not isinstance(view, dict):
            raise ValueError("Each compatibility view must be a mapping")
        missing_fields = sorted(REQUIRED_VIEW_FIELDS - set(view))
        if missing_fields:
            raise ValueError(
                f"Compatibility view {view.get('name', '<unknown>')} is "
                "missing required fields: " + ", ".join(missing_fields)
            )

        context = f"Compatibility view {view['name']}"
        view_name = _required_text(view["name"], "name", context)
        source_feature = _required_text(
            view["source_feature"], "source_feature", context
        )
        _required_text(view["source_job"], "source_job", context)
        _validated_string_tuple(view["consumers"], "consumers", context)
        view_names.append(view_name)
        view_sources.append((view_name, source_feature))

    duplicate_views = sorted(
        {
            view_name
            for view_name in view_names
            if view_names.count(view_name) > 1
        }
    )
    if duplicate_views:
        raise ValueError(
            "Duplicate feature-store compatibility view names: "
            + ", ".join(duplicate_views)
        )
    collisions = sorted(set(view_names) & known_table_names)
    if collisions:
        raise ValueError(
            "Feature-store views collide with physical tables: "
            + ", ".join(collisions)
        )

    for view_name, source_feature in view_sources:
        if source_feature not in known_table_names:
            raise ValueError(
                f"Compatibility view {view_name} references unknown "
                f"source feature: {source_feature}"
            )
        view_contract_path = table_root / f"create_view_{view_name}.sql"
        if not view_contract_path.is_file():
            raise ValueError(
                f"Compatibility view {view_name} is missing SQL contract: "
                f"{view_contract_path}"
            )

    return FeatureStoreRegistry(
        name=str(feature_store["name"]),
        description=str(feature_store["description"]),
        default_catalog=str(feature_store["default_catalog"]),
        default_schema=str(feature_store["default_schema"]),
        table_root=table_root,
        physical_tables=physical_tables,
        compatibility_views=compatibility_views,
        store_bindings=store_bindings,
    )
