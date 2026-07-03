"""Compatibility wrapper for the moved build-Markov-chain entrypoint."""

import runpy


def main(*args, **kwargs):
    if args or kwargs:
        from jobs.nextads_main.build_markov_chain import main as moved_main

        return moved_main(*args, **kwargs)

    runpy.run_module("jobs.nextads_main.build_markov_chain", run_name="__main__")


if __name__ == "__main__":
    main()
