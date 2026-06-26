"""Compatibility wrapper for the moved theme-score mapping entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.nextads_main.map_theme_scores_to_ads", run_name="__main__")


if __name__ == "__main__":
    main()
