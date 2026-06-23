import os
from pathlib import Path

from dotenv import load_dotenv
from dsutils.logtools import get_logger
from dynaconf import Dynaconf


logger = get_logger(__name__)


def _find_project_root() -> Path:
    """Find the repo root from either legacy or src package locations."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path(__file__).resolve().parents[3]


PROJECT_ROOT = _find_project_root()


def _existing_path(primary: str, fallback: str) -> str:
    """Prefer current paths while allowing the future target path."""
    if (PROJECT_ROOT / primary).exists():
        return primary
    return fallback


def _settings_files() -> list[str]:
    return [
        _existing_path("config/settings.yaml", "configs/settings.yaml"),
        _existing_path(
            "config/global_solution_settings.yaml",
            "configs/global_solution_settings.yaml",
        ),
        _existing_path(
            "config/load_control_sheet_settings.yaml",
            "configs/load_control_sheet_settings.yaml",
        ),
        _existing_path(
            "config/load_control_sheet_v2_settings.yaml",
            "configs/adsv2/load_control_sheet_v2_settings.yaml",
        ),
        _existing_path(
            "config/tables_settings.yaml",
            "configs/tables_settings.yaml",
        ),
        _existing_path(
            "config/model_settings.yaml",
            "configs/model_settings.yaml",
        ),
        _existing_path("config/users.yaml", "configs/users.yaml"),
    ]


def _env_local_files() -> list[Path]:
    return [
        PROJECT_ROOT
        / _existing_path("config/.env.local", "configs/.env.local")
    ]


def load_config(job_env: str) -> Dynaconf:
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
        settings_files=_settings_files(),
        environments=True,
        env_switcher="JOB_ENV",
    )
    config.setenv(job_env)
    return config
