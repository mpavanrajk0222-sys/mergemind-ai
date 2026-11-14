from __future__ import annotations

import os
from pathlib import Path

from src.database import DEFAULT_DATABASE_PATH, initialize_database
from src.pipeline import DEFAULT_REPORT_DIRECTORY


def database_path() -> Path:
    configured = os.environ.get("MERGEMIND_DATABASE_PATH")
    return Path(configured).expanduser().resolve() if configured else DEFAULT_DATABASE_PATH


def report_directory() -> Path:
    configured = os.environ.get("MERGEMIND_REPORT_DIRECTORY")
    return Path(configured).expanduser().resolve() if configured else DEFAULT_REPORT_DIRECTORY


def migrate_existing_database() -> bool:
    """Apply the backend's additive schema setup only to an existing database."""

    path = database_path()
    if not path.is_file():
        return False
    initialize_database(path)
    return True
