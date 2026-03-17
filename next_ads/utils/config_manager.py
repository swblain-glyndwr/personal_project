import os
from dynaconf import Dynaconf
from dsutils.logtools import get_logger
from pathlib import Path
from dotenv import load_dotenv


logger = get_logger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


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
        # Explicitly load .env files into os.environ
        # This must happen BEFORE Dynaconf initialization
        for env_file in [PROJECT_ROOT / "config/.env.local"]:
            env_path = Path(env_file)
            if env_path.exists():
                logger.info(f"Loading environment variables from {env_path}")
                load_dotenv(env_path, override=False)  # override=False means CI/CD vars take precedence

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
        settings_files=[
            "config/settings.yaml",
            "config/global_solution_settings.yaml",
            "config/load_control_sheet_settings.yaml",
            "config/tables_settings.yaml",
            "config/users.yaml",
        ],
        environments=True,
        env_switcher="JOB_ENV",
    )
    config.setenv(job_env)
    return config
