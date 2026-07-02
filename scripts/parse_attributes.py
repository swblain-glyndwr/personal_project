"""Compatibility wrapper for the moved item-attribute parsing entrypoint."""

from jobs.nextads_main.parse_attributes import main, parse_args


if __name__ == "__main__":
    main(**parse_args())
