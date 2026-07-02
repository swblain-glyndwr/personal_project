"""Compatibility wrapper for the moved results performance checks entrypoint."""

import runpy


def main():
    runpy.run_module(
        "jobs.results.results_performance_checks",
        run_name="__main__",
    )


if __name__ == "__main__":
    main()
