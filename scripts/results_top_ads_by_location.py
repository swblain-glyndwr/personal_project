"""Compatibility wrapper for the moved top ads reporting entrypoint."""

import runpy


def main():
    runpy.run_module(
        "jobs.results.results_top_ads_by_location",
        run_name="__main__",
    )


if __name__ == "__main__":
    main()
