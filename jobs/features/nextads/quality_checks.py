"""Run feature-store quality checks."""

from _registry_job import metadata_only_main


if __name__ == "__main__":
    metadata_only_main("quality_checks")
