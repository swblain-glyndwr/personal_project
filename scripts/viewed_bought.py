"""Compatibility wrapper for the moved viewed-bought realtime input entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.realtime.viewed_bought", run_name="__main__")


if __name__ == "__main__":
    main()
