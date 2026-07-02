"""Compatibility wrapper for the moved results stage 3 entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.results.results_3", run_name="__main__")


if __name__ == "__main__":
    main()
