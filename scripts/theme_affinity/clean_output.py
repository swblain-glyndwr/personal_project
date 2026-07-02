"""Compatibility wrapper for the moved Theme Affinity clean-output entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.model.theme_affinity.clean_output", run_name="__main__")


if __name__ == "__main__":
    main()
