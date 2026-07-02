from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_yaml(path: str) -> dict:
    return yaml.safe_load((PROJECT_ROOT / path).read_text())


def load_job(path: str, key: str, target: str = "DEV") -> dict:
    config = load_yaml(path)

    target_jobs = (
        config.get("targets", {})
        .get(target, {})
        .get("resources", {})
        .get("jobs", {})
    )
    if key in target_jobs:
        return target_jobs[key]

    global_jobs = config.get("resources", {}).get("jobs", {})
    if key in global_jobs:
        return global_jobs[key]

    available_targets = {
        target_name: sorted(
            target_config.get("resources", {}).get("jobs", {}).keys()
        )
        for target_name, target_config in config.get("targets", {}).items()
    }
    raise KeyError(
        f"Job {key!r} not found in {path!r} for target {target!r}; "
        f"target jobs: {available_targets}"
    )
