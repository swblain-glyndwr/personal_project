from dynaconf import Dynaconf


def load_config(job_env: str) -> Dynaconf:
    """Load configuration."""
    config = Dynaconf(
        settings_files=["config/settings.yaml",
                        "config/global_solution_settings.yaml"],
        environments=True,
        env_switcher="JOB_ENV",
    )
    config.setenv(job_env)
    return config
