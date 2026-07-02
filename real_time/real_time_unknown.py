"""Compatibility wrapper for moved realtime unknown helpers."""

from next_ads.realtime.unknown import (
    create_backfill_udf,
    format_stream_archive,
    main,
    run_realtime_unknown,
    set_ads,
)


__all__ = [
    "create_backfill_udf",
    "format_stream_archive",
    "main",
    "run_realtime_unknown",
    "set_ads",
]


if __name__ == "__main__":
    main()
