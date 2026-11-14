"""JSON-lines logging setup for MergeMind AI command-line and pipeline runs."""

from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import time
from typing import Any


LOGGER_NAME = "mergemind"
_CONTEXT_FIELDS = (
    "event", "run_id", "stage", "entity_type", "duration_seconds",
    "input_counts", "output_counts", "rejected_count", "error_code",
    "error_type", "status", "metrics",
)


class StructuredFormatter(logging.Formatter):
    """Serialize stable log fields as one UTF-8 JSON object per line."""

    converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S")
        payload: dict[str, Any] = {
            "timestamp": f"{timestamp}.{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception_type"] = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


def configure_logging(
    *,
    log_level: str = "INFO",
    log_file: str | Path | None = None,
    quiet: bool = False,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3,
) -> logging.Logger:
    """Configure console and optional rotating JSONL file handlers.

    Existing handlers owned by the MergeMind logger are replaced, making this
    safe to call repeatedly from tests or embedded command-line entry points.
    """

    if not isinstance(log_level, str):
        raise ValueError("log_level must be one of DEBUG, INFO, WARNING, ERROR, or CRITICAL.")
    level = logging.getLevelName(log_level.upper())
    if not isinstance(level, int):
        raise ValueError("log_level must be one of DEBUG, INFO, WARNING, ERROR, or CRITICAL.")
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer.")
    if isinstance(backup_count, bool) or not isinstance(backup_count, int) or backup_count < 0:
        raise ValueError("backup_count must be a non-negative integer.")

    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(level)
    logger.propagate = False
    formatter = StructuredFormatter()

    if not quiet:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)
    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        rotating = RotatingFileHandler(
            path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
        )
        rotating.setFormatter(formatter)
        logger.addHandler(rotating)
    return logger


__all__ = ("LOGGER_NAME", "StructuredFormatter", "configure_logging")
