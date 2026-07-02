"""Compatibility wrapper for the moved Databricks trigger entrypoint."""

from jobs.nextads_main.trigger_databricks_job import main, parse_args, trigger_job

__all__ = ["main", "parse_args", "trigger_job"]


if __name__ == "__main__":
    main()
