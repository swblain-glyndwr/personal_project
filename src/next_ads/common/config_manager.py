import os
from pathlib import Path

from dotenv import load_dotenv
from dsutils.logtools import get_logger
from dynaconf import Dynaconf, Validator

from next_ads.common.paths import PROJECT_ROOT

logger = get_logger(__name__)


def _existing_path(primary: str, fallback: str) -> str:
    """Prefer target paths while allowing legacy flat config paths."""
    if (PROJECT_ROOT / primary).exists():
        return primary
    return fallback


def _settings_files(client: str = "next_uk") -> list[str]:
    files = [
        _existing_path(
            "configs/runtime/settings.yaml", "config/settings.yaml"
        ),
        _existing_path(
            "configs/delivery/global_solution_settings.yaml",
            "config/global_solution_settings.yaml",
        ),
        _existing_path(
            "configs/control/load_control_sheet_settings.yaml",
            "config/load_control_sheet_settings.yaml",
        ),
        _existing_path(
            "configs/adsv2/load_control_sheet_v2_settings.yaml",
            "config/load_control_sheet_v2_settings.yaml",
        ),
        _existing_path(
            "configs/runtime/tables_settings.yaml",
            "config/tables_settings.yaml",
        ),
        _existing_path(
            "configs/model/model_settings.yaml",
            "config/model_settings.yaml",
        ),
        _existing_path("configs/runtime/users.yaml", "config/users.yaml"),
    ]
    client_file = _existing_path(
        f"configs/clients/{client}.yaml",
        f"config/{client}.yaml",
    )
    files.append(client_file)
    return files


def _env_local_files():
    return [
        PROJECT_ROOT
        / _existing_path("configs/runtime/.env.local", "config/.env.local")
    ]


def _is_mapping(value) -> bool:
    return isinstance(value, dict) or hasattr(value, "to_dict")


def _required_validators() -> list[Validator]:
    mapping_keys = [
        "tables_read",
        "tables_write",
        "gcp",
        "control_sheet",
        "control_sheet_v2",
        "exclusions_sheet",
        "theme_mapping",
        "theme_mapping_v2",
        "locations",
    ]
    string_keys = [
        "client",
        "catalog_read",
        "catalog_write",
        "schema_read",
        "schema_write",
    ]
    validators = [
        Validator(*string_keys, must_exist=True, is_type_of=str),
    ]
    validators.extend(
        Validator(
            key,
            must_exist=True,
            condition=_is_mapping,
            messages={
                "condition": f"{key} must be a mapping loaded from Dynaconf",
            },
        )
        for key in mapping_keys
    )
    return validators


def _iter_leaf_values(value):
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if isinstance(value, dict):
        for child in value.values():
            yield from _iter_leaf_values(child)
        return
    yield value


def _validate_table_paths(config: Dynaconf) -> None:
    for section_name in ("tables_read", "tables_write"):
        section = getattr(config, section_name)
        bad_values = [
            value
            for value in _iter_leaf_values(section)
            if not isinstance(value, str) or not value.strip()
        ]
        if bad_values:
            raise ValueError(
                f"{section_name} must contain only non-empty table path strings"
            )


def _resolve_job_env(job_env: str | None) -> str:
    resolved_env = job_env or os.environ.get("JOB_ENV")
    if resolved_env:
        return resolved_env
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        raise ValueError(
            "JOB_ENV must be set when loading Dynaconf inside Databricks"
        )
    return "dev"


def load_config(
    job_env: str | None = None, client: str = "next_uk"
) -> Dynaconf:
    """Load configuration.

    Explicitly loads .env files into os.environ before Dynaconf initialization.
    This ensures @format {env[USER_SCHEMA]} works correctly.

    Environment variable precedence:
    1. CI/CD pipeline exports (highest)
    2. Databricks cluster spark_env_vars
    3. .env.local file (local development)
    4. Default fallback: 'ds_sandbox'

    Set DYNACONF_SKIP_ENV=true to skip loading .env.local (useful for testing).
    """
    job_env = _resolve_job_env(job_env)

    # Skip .env.local loading if DYNACONF_SKIP_ENV is set (for unit tests)
    skip_env = os.environ.get("DYNACONF_SKIP_ENV", "false").lower() == "true"

    if not skip_env:
        # Explicitly load .env files into os.environ before Dynaconf.
        for env_file in _env_local_files():
            env_path = Path(env_file)
            if env_path.exists():
                logger.info(f"Loading environment variables from {env_path}")
                # override=False means CI/CD vars take precedence
                load_dotenv(env_path, override=False)

    # Set default for USER_SCHEMA if not already set
    # When running in Databricks, this will be overridden by cluster spark_env_vars
    if "USER_SCHEMA" not in os.environ:
        os.environ["USER_SCHEMA"] = "ds_sandbox"
        logger.info("USER_SCHEMA not set, using default: ds_sandbox")
    elif "databricks_spn" in os.environ["USER_SCHEMA"]:
        # If USER_SCHEMA is set to a Databricks SPN, override it to default to ds_sandbox
        logger.info(f"USER_SCHEMA is set to {os.environ['USER_SCHEMA']}")
        os.environ["USER_SCHEMA"] = "ds_sandbox"

    config = Dynaconf(
        settings_files=_settings_files(client),
        environments=True,
        env_switcher="JOB_ENV",
    )
    config.setenv(job_env)
    config.validators.register(*_required_validators())
    config.validators.validate()
    _validate_table_paths(config)
    return config
