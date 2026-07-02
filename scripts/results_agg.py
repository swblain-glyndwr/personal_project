"""Compatibility wrapper for the moved results aggregation entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.results.results_agg", run_name="__main__")


if __name__ == "__main__":
    main()
