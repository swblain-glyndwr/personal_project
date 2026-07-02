"""Compatibility wrapper for the moved build-Markov-chain entrypoint."""

import runpy


def main():
    runpy.run_module("jobs.nextads_main.build_markov_chain", run_name="__main__")


if __name__ == "__main__":
    main()
