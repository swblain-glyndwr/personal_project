from __future__ import annotations

import argparse
import sys
from pathlib import Path


DEFAULT_CLIENT = "next_uk"
DEFAULT_JOB_ENV = "dev"
DEFAULT_LOG_LEVEL = "INFO"


def resolve_project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        from dsutils.dbc import get_dbutils

        dbutils = get_dbutils()
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        if not notebook_path.startswith("/Workspace"):
            notebook_path = "/Workspace" + notebook_path
        return Path(notebook_path).parents[2]


def bootstrap_project_imports() -> None:
    project_root = resolve_project_root()
    src_root = project_root / "src"
    if src_root.exists() and str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
    if str(project_root) not in sys.path:
        sys.path.insert(1, str(project_root))


def seed_latest_tables(*, client: str, log_level: str) -> None:
    bootstrap_project_imports()
    from jobs.table_operations import init_starting_tables

    init_starting_tables.main(CLIENT=client, LOG_LEVEL=log_level)


def run_dev_setup(
    *,
    mode: str,
    job_env: str = DEFAULT_JOB_ENV,
    client: str = DEFAULT_CLIENT,
    log_level: str = DEFAULT_LOG_LEVEL,
) -> None:
    if job_env.lower() != "dev":
        raise ValueError("Personal DEV setup only supports job_env=dev")

    bootstrap_project_imports()
    from jobs.table_operations.table_operations import create_missing_tables

    create_missing_tables(
        job_env=job_env,
        client=client,
        log_level=log_level,
        confirm_mutating=True,
        dry_run=False,
    )

    if mode == "create_only":
        return
    if mode == "seed_latest":
        seed_latest_tables(client=client, log_level=log_level)
        return

    raise ValueError(f"Unsupported DEV setup mode: {mode!r}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare personal DEV tables for Next Ads development."
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--mode",
        choices=("create_only", "seed_latest"),
        help="DEV setup mode. Used by Databricks job parameters.",
    )
    mode_group.add_argument(
        "--create-only",
        dest="mode",
        action="store_const",
        const="create_only",
        help="Create missing personal DEV tables without seeding data.",
    )
    mode_group.add_argument(
        "--seed-latest",
        dest="mode",
        action="store_const",
        const="seed_latest",
        help="Create missing tables, then seed required latest/reference tables.",
    )
    mode_group.add_argument(
        "--sample",
        dest="mode",
        action="store_const",
        const="seed_latest",
        help="Deprecated alias for --seed-latest.",
    )
    mode_group.add_argument(
        "--standard",
        dest="mode",
        action="store_const",
        const="create_only",
        help="Deprecated alias for --create-only.",
    )
    parser.add_argument("--job_env", default=DEFAULT_JOB_ENV)
    parser.add_argument("--client", default=DEFAULT_CLIENT)
    parser.add_argument("--log_level", default=DEFAULT_LOG_LEVEL)
    parser.set_defaults(mode="create_only")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def main() -> None:
    args = parse_args()
    run_dev_setup(
        mode=args.mode,
        job_env=args.job_env,
        client=args.client,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
