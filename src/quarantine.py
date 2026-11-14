"""Persistent quarantine management for validation-rejected source rows."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Literal

from src.database import (
    get_quarantine_history,
    get_quarantine_record,
    get_quarantine_records,
    get_quarantine_statistics,
    save_quarantine_entries,
    update_quarantine_record,
)
from src.validation import RejectedRecord, ValidationResult


EntityType = Literal["customer", "product"]
QuarantineStatus = Literal["QUARANTINED", "RESOLVED", "DISCARDED"]


@dataclass(frozen=True)
class QuarantineRecord:
    """Immutable snapshot of one validation-rejected source row."""

    quarantine_id: str
    run_id: str
    entity_type: EntityType
    source_record_id: str | None
    source_file: str
    source_record_number: int
    original_record: dict[str, Any]
    validation_issues: list[dict[str, Any]]
    quarantine_status: QuarantineStatus
    quarantined_at: datetime
    resolved_at: datetime | None = None
    resolution_action: str | None = None
    resolution_notes: str | None = None


def _record_id(rejected: RejectedRecord) -> str | None:
    key = "customer_id" if rejected.entity_type == "customer" else "product_id"
    value = rejected.source_fields.get(key)
    if value is None or not str(value).strip():
        return None
    return str(value)


def _quarantine_id(run_id: str, rejected: RejectedRecord) -> str:
    identity = [run_id, rejected.entity_type, rejected.source_file,
                rejected.source_record_number, _record_id(rejected)]
    encoded = json.dumps(identity, ensure_ascii=False, separators=(",", ":"), default=str)
    return "quarantine-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


def _rejected_records(results: Iterable[ValidationResult | RejectedRecord]) -> Iterable[RejectedRecord]:
    for item in results:
        if isinstance(item, ValidationResult):
            yield from item.rejected_records
        elif isinstance(item, RejectedRecord):
            yield item
        else:
            raise TypeError("Quarantine creation expects ValidationResult or RejectedRecord values.")


def create_quarantine_entries(
    run_id: str,
    validation_results: Iterable[ValidationResult | RejectedRecord],
    *,
    quarantined_at: datetime | None = None,
) -> list[QuarantineRecord]:
    """Build entries only from records already rejected by validation.

    Validation codes and messages are copied as supplied by the validator;
    this module does not decide whether a source record is valid.
    """

    timestamp = quarantined_at or datetime.now(timezone.utc)
    entries = []
    for rejected in _rejected_records(validation_results):
        if not rejected.errors:
            raise ValueError("A rejected record must contain at least one ValidationIssue.")
        entries.append(QuarantineRecord(
            quarantine_id=_quarantine_id(run_id, rejected),
            run_id=run_id,
            entity_type=rejected.entity_type,
            source_record_id=_record_id(rejected),
            source_file=rejected.source_file,
            source_record_number=rejected.source_record_number,
            original_record=copy.deepcopy(rejected.source_fields),
            validation_issues=[
                {
                    "record_id": issue.record_id,
                    "field_name": issue.field_name,
                    "error_code": issue.error_code,
                    "message": issue.message,
                }
                for issue in rejected.errors
            ],
            quarantine_status="QUARANTINED",
            quarantined_at=timestamp,
        ))
    return sorted(entries, key=lambda item: (item.entity_type, item.source_file,
                                              item.source_record_number, item.quarantine_id))


def persist_quarantine_entries(run_id: str, entries: Iterable[QuarantineRecord], *, db_path=None) -> int:
    """Persist new quarantine entries without replacing resolution state."""

    return save_quarantine_entries(run_id, entries, db_path=db_path)


def retrieve_quarantine_records(
    run_id: str,
    *,
    entity_type: str | None = None,
    error_code: str | None = None,
    status: str | None = None,
    db_path=None,
) -> list[dict[str, Any]]:
    return get_quarantine_records(run_id, entity_type=entity_type, error_code=error_code,
                                  quarantine_status=status, db_path=db_path)


def retrieve_quarantine_record(run_id: str, quarantine_id: str, *, db_path=None) -> dict[str, Any] | None:
    return get_quarantine_record(run_id, quarantine_id, db_path=db_path)


def retrieve_quarantine_history(run_id: str, quarantine_id: str, *, db_path=None) -> list[dict[str, Any]]:
    return get_quarantine_history(run_id, quarantine_id, db_path=db_path)


def resolve_quarantine_record(
    run_id: str,
    quarantine_id: str,
    *,
    resolution_action: str,
    notes: str | None = None,
    db_path=None,
) -> dict[str, Any]:
    """Mark an item resolved for external follow-up; does not reprocess it."""

    return update_quarantine_record(run_id, quarantine_id, "RESOLVED",
                                    resolution_action=resolution_action,
                                    resolution_notes=notes, db_path=db_path)


def discard_quarantine_record(
    run_id: str,
    quarantine_id: str,
    *,
    notes: str | None = None,
    db_path=None,
) -> dict[str, Any]:
    """Mark an item discarded while retaining its original payload and history."""

    return update_quarantine_record(run_id, quarantine_id, "DISCARDED",
                                    resolution_action="DISCARD",
                                    resolution_notes=notes, db_path=db_path)


def retrieve_quarantine_statistics(run_id: str, *, db_path=None) -> dict[str, Any]:
    return get_quarantine_statistics(run_id, db_path=db_path)


__all__ = (
    "QuarantineRecord", "create_quarantine_entries", "persist_quarantine_entries",
    "retrieve_quarantine_records", "retrieve_quarantine_record", "retrieve_quarantine_history",
    "resolve_quarantine_record", "discard_quarantine_record", "retrieve_quarantine_statistics",
)
