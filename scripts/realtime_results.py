"""Compatibility wrapper for the moved realtime results entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.realtime.realtime_results", run_name="__main__")


if __name__ == "__main__":
    main()
