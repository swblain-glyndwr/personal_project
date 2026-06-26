"""Compatibility wrapper for the moved Theme Affinity sense-check entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.model.theme_affinity.sense_check", run_name="__main__")


if __name__ == "__main__":
    main()
