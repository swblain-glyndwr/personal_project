"""Compatibility wrapper for the moved results stage 2 entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.results.results_2", run_name="__main__")


if __name__ == "__main__":
    main()
