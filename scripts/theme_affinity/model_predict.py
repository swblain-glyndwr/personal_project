"""Compatibility wrapper for the moved Theme Affinity prediction entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.model.theme_affinity.model_predict", run_name="__main__")


if __name__ == "__main__":
    main()
