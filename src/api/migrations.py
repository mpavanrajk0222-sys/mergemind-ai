from __future__ import annotations

from src.api.config import migrate_existing_database


def prepare_api_database() -> None:
    """Apply only the existing backend's additive schema setup to an existing DB."""

    migrate_existing_database()
