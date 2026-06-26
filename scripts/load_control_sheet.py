"""Compatibility wrapper for the moved load-control-sheet entrypoint."""

from jobs.nextads_main.load_control_sheet import main, parse_args


if __name__ == "__main__":
    main(**parse_args())
