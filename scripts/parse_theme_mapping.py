"""Compatibility wrapper for the moved theme-mapping entrypoint."""

from jobs.nextads_main.parse_theme_mapping import main, parse_args


if __name__ == "__main__":
    main(**parse_args())
