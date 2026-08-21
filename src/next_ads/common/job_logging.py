"""Shared logging configuration for repository-owned job entrypoints."""

from __future__ import annotations

import logging


_NOISY_DEPENDENCY_LOGGERS = ("py4j", "py4j.clientserver")


def _resolve_log_level(log_level: str | int) -> int:
    if isinstance(log_level, int):
        return log_level
    resolved = getattr(logging, str(log_level).strip().upper(), None)
    if not isinstance(resolved, int):
        raise ValueError(f"Unsupported log level: {log_level!r}")
    return resolved


def configure_job_logging(
    log_level: str | int = logging.INFO,
    *,
    log_format: str | None = None,
    force: bool = False,
) -> None:
    """Configure application logging without verbose Py4J callback traffic."""
    options: dict[str, object] = {"force": force}
    if log_format is not None:
        options["format"] = log_format
    logging.basicConfig(level=_resolve_log_level(log_level), **options)
    for logger_name in _NOISY_DEPENDENCY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
