"""Compatibility wrapper for the moved assign-customer-cells entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.nextads_main.assign_customer_cells", run_name="__main__")


if __name__ == "__main__":
    main()
