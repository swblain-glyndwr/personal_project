"""Compatibility wrapper for the moved page-build entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.nextads_main.build_page", run_name="__main__")


if __name__ == "__main__":
    main()
