"""Compatibility wrapper for the moved personal DEV setup entrypoint."""

from jobs.table_operations import setup_dev_tables as _setup_dev_tables


parse_args = _setup_dev_tables.parse_args
run_dev_setup = _setup_dev_tables.run_dev_setup


def main(sample=None):
    if sample is None:
        return _setup_dev_tables.main()

    mode = "seed_latest" if sample else "create_only"
    return run_dev_setup(mode=mode)


if __name__ == "__main__":
    main()
