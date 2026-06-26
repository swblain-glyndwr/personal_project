"""Compatibility wrapper for the moved results stage 1 entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.results.results_1", run_name="__main__")


if __name__ == "__main__":
    main()
