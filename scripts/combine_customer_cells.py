"""Compatibility wrapper for the moved combine-customer-cells entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.nextads_main.combine_customer_cells", run_name="__main__")


if __name__ == "__main__":
    main()
