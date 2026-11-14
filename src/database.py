"""SQLite persistence for MergeMind AI pipeline artifacts.

The module stores run-scoped source, decision, master, lineage, and error data.
Each public write operation is transactional and idempotent for its natural
keys. No pipeline orchestration or matching logic is implemented here.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from src.duplicate_detection import DetectionResult
from src.ingestion import IngestedRecord
from src.normalization import NormalizedRecord
from src.probable_matching import ProbableMatchingResult, ProbablePair
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA
from src.survivorship import MasterRecord, RejectedFieldValue
from src.validation import RejectedRecord


DEFAULT_DATABASE_PATH = Path(__file__).resolve().parent.parent / "data" / "output" / "mergemind.db"
_SCHEMAS = {"customer": CUSTOMER_SCHEMA, "product": PRODUCT_SCHEMA}
_SECRET_KEY_PARTS = {
    "password", "passwd", "secret", "token", "credential", "credentials",
    "apikey", "accesskey", "privatekey", "authorization", "authheader",
}
_INPUT_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")


class RunInputMismatchError(ValueError):
    """Raised when a run ID is resumed with a different input fingerprint."""


@dataclass(frozen=True)
class DuplicateDecision:
    """Persistence-friendly pair decision with JSON-ready evidence mappings."""

    entity_type: str
    record_id_1: str
    record_id_2: str
    decision: str
    confidence_score: float | None = None
    matching_evidence: Mapping[str, Any] | None = None
    conflicting_evidence: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class PipelineError:
    """One pipeline error to persist."""

    stage: str
    error_code: str
    error_message: str
    source_record_id: str | None = None
    created_at: datetime | None = None
    error_id: str | None = None


def _database_path(value: str | Path | None) -> Path:
    return DEFAULT_DATABASE_PATH if value is None else Path(value)


def _iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    raise TypeError(f"Value of type {type(value).__name__} cannot be represented as JSON.")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def _reject_secret_keys(value: Any, location: str = "record") -> None:
    """Refuse persistence when a payload contains an obvious secret field."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            compact_key = re.sub(r"[^a-z0-9]", "", str(key).casefold())
            if any(part in compact_key for part in _SECRET_KEY_PARTS):
                raise ValueError(f"Refusing to persist secret-like field {location}.{key}.")
            _reject_secret_keys(child, f"{location}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_secret_keys(child, f"{location}[{index}]")


@contextmanager
def _connect(path: str | Path | None) -> Iterator[sqlite3.Connection]:
    resolved_path = _database_path(path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(resolved_path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        yield connection
    finally:
        connection.close()


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    customer_input_count INTEGER NOT NULL DEFAULT 0 CHECK (customer_input_count >= 0),
    product_input_count INTEGER NOT NULL DEFAULT 0 CHECK (product_input_count >= 0),
    accepted_count INTEGER NOT NULL DEFAULT 0 CHECK (accepted_count >= 0),
    rejected_count INTEGER NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    master_record_count INTEGER NOT NULL DEFAULT 0 CHECK (master_record_count >= 0),
    error_count INTEGER NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    input_fingerprint TEXT
);

CREATE TABLE IF NOT EXISTS source_records (
    source_record_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'product')),
    source_system TEXT,
    source_file TEXT NOT NULL,
    source_record_number INTEGER NOT NULL CHECK (source_record_number > 0),
    original_record_json TEXT NOT NULL,
    normalized_record_json TEXT,
    validation_status TEXT NOT NULL,
    PRIMARY KEY (run_id, entity_type, source_record_id),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS duplicate_pairs (
    pair_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'product')),
    record_id_1 TEXT NOT NULL,
    record_id_2 TEXT NOT NULL,
    decision TEXT NOT NULL,
    confidence_score REAL,
    matching_evidence_json TEXT NOT NULL,
    conflicting_evidence_json TEXT NOT NULL,
    CHECK (record_id_1 < record_id_2),
    UNIQUE (run_id, entity_type, record_id_1, record_id_2),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id, entity_type, record_id_1)
        REFERENCES source_records(run_id, entity_type, source_record_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id, entity_type, record_id_2)
        REFERENCES source_records(run_id, entity_type, source_record_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS master_records (
    run_id TEXT NOT NULL,
    master_id TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'product')),
    merge_method TEXT NOT NULL CHECK (merge_method IN ('EXACT', 'STRONG_PROBABLE', 'SINGLETON')),
    merged_record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, master_id),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS field_lineage (
    lineage_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    master_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    selected_value TEXT,
    selected_source_record_id TEXT,
    selected_source_system TEXT,
    selection_rule TEXT NOT NULL,
    rejected_values_json TEXT NOT NULL,
    UNIQUE (run_id, master_id, field_name),
    FOREIGN KEY (run_id, master_id)
        REFERENCES master_records(run_id, master_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pipeline_errors (
    error_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    source_record_id TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS review_items (
    run_id TEXT NOT NULL,
    review_id TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'product')),
    record_id_1 TEXT NOT NULL,
    record_id_2 TEXT NOT NULL,
    confidence_score REAL,
    matching_evidence_json TEXT NOT NULL,
    conflicting_evidence_json TEXT NOT NULL,
    decision_explanation TEXT NOT NULL,
    review_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (review_status IN ('PENDING', 'ACCEPTED_MATCH', 'REJECTED_MATCH', 'DEFERRED')),
    created_at TEXT NOT NULL,
    reviewed_at TEXT,
    reviewer_id TEXT,
    reviewer_notes TEXT,
    PRIMARY KEY (run_id, review_id),
    CHECK (record_id_1 < record_id_2),
    UNIQUE (run_id, entity_type, record_id_1, record_id_2),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id, entity_type, record_id_1)
        REFERENCES source_records(run_id, entity_type, source_record_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id, entity_type, record_id_2)
        REFERENCES source_records(run_id, entity_type, source_record_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS review_history (
    history_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    review_id TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    reviewer_id TEXT,
    reviewer_notes TEXT,
    changed_at TEXT NOT NULL,
    FOREIGN KEY (run_id, review_id)
        REFERENCES review_items(run_id, review_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS quarantine_records (
    run_id TEXT NOT NULL,
    quarantine_id TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'product')),
    source_record_id TEXT,
    source_file TEXT NOT NULL,
    source_record_number INTEGER NOT NULL CHECK (source_record_number > 0),
    original_record_json TEXT NOT NULL,
    validation_issues_json TEXT NOT NULL,
    quarantine_status TEXT NOT NULL DEFAULT 'QUARANTINED'
        CHECK (quarantine_status IN ('QUARANTINED', 'RESOLVED', 'DISCARDED')),
    quarantined_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution_action TEXT,
    resolution_notes TEXT,
    PRIMARY KEY (run_id, quarantine_id),
    UNIQUE (run_id, entity_type, source_file, source_record_number),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS quarantine_history (
    history_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    quarantine_id TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    resolution_action TEXT,
    resolution_notes TEXT,
    changed_at TEXT NOT NULL,
    FOREIGN KEY (run_id, quarantine_id)
        REFERENCES quarantine_records(run_id, quarantine_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_source_records_run_entity
    ON source_records(run_id, entity_type);
CREATE INDEX IF NOT EXISTS idx_duplicate_pairs_run_decision
    ON duplicate_pairs(run_id, decision);
CREATE INDEX IF NOT EXISTS idx_master_records_run_entity
    ON master_records(run_id, entity_type);
CREATE INDEX IF NOT EXISTS idx_field_lineage_master
    ON field_lineage(run_id, master_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_errors_run_stage
    ON pipeline_errors(run_id, stage);
CREATE INDEX IF NOT EXISTS idx_review_items_run_status
    ON review_items(run_id, review_status);
CREATE INDEX IF NOT EXISTS idx_review_items_run_entity_status
    ON review_items(run_id, entity_type, review_status);
CREATE INDEX IF NOT EXISTS idx_review_history_item_time
    ON review_history(run_id, review_id, changed_at);
CREATE INDEX IF NOT EXISTS idx_quarantine_run_status
    ON quarantine_records(run_id, quarantine_status);
CREATE INDEX IF NOT EXISTS idx_quarantine_run_entity_status
    ON quarantine_records(run_id, entity_type, quarantine_status);
CREATE INDEX IF NOT EXISTS idx_quarantine_history_item_time
    ON quarantine_history(run_id, quarantine_id, changed_at);
"""


def initialize_database(db_path: str | Path | None = None) -> Path:
    """Create the database directory, tables, indexes, and FK enforcement."""

    path = _database_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(path) as connection:
        with connection:
            connection.executescript(_SCHEMA_SQL)
            _ensure_compatibility_columns(connection)
    return path


def _ensure_initialized(connection: sqlite3.Connection) -> None:
    # Public persistence functions remain convenient and safe when callers
    # have not explicitly initialized the database first.
    connection.executescript(_SCHEMA_SQL)
    _ensure_compatibility_columns(connection)


def _ensure_compatibility_columns(connection: sqlite3.Connection) -> None:
    """Apply additive, data-preserving column migrations for older databases."""

    columns = {row["name"] for row in connection.execute("PRAGMA table_info(pipeline_runs)")}
    if "input_fingerprint" not in columns:
        connection.execute("ALTER TABLE pipeline_runs ADD COLUMN input_fingerprint TEXT")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_pipeline_run(
    run_id: str,
    *,
    db_path: str | Path | None = None,
    started_at: datetime | str | None = None,
    status: str = "RUNNING",
    customer_input_count: int = 0,
    product_input_count: int = 0,
    accepted_count: int = 0,
    rejected_count: int = 0,
    master_record_count: int = 0,
    error_count: int = 0,
    input_fingerprint: str | None = None,
    replace_existing: bool = False,
) -> None:
    """Create or resume a run; optionally replace only that run's artifacts.

    Default behavior resumes/upserts run metadata while preserving child rows.
    When both are available, a stored input fingerprint must match the new
    value; use ``replace_existing=True`` to deliberately start with changed
    inputs. Older runs with no fingerprint adopt the first supplied value.
    ``replace_existing=True`` deletes this run and its cascaded children inside
    one transaction, then creates a fresh run. Other runs are untouched.
    """

    if not run_id or not isinstance(run_id, str):
        raise ValueError("run_id must be a non-empty string.")
    timestamp = _iso(started_at or _now())
    counts = (customer_input_count, product_input_count, accepted_count, rejected_count, master_record_count, error_count)
    if any(not isinstance(count, int) or count < 0 for count in counts):
        raise ValueError("Pipeline run counts must be non-negative integers.")
    if input_fingerprint is not None and (
        not isinstance(input_fingerprint, str) or not _INPUT_FINGERPRINT_RE.fullmatch(input_fingerprint)
    ):
        raise ValueError("input_fingerprint must be a lowercase SHA-256 hex digest or None.")
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            existing = connection.execute(
                "SELECT input_fingerprint FROM pipeline_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if (
                existing is not None
                and not replace_existing
                and existing["input_fingerprint"] is not None
                and existing["input_fingerprint"] != input_fingerprint
            ):
                raise RunInputMismatchError(
                    f"Run {run_id!r} already belongs to different inputs. "
                    "Use replace_existing=True (or --replace-existing) to replace that run."
                )
            if replace_existing:
                connection.execute("DELETE FROM pipeline_runs WHERE run_id = ?", (run_id,))
            connection.execute(
                """INSERT INTO pipeline_runs (
                    run_id, started_at, completed_at, status, customer_input_count,
                    product_input_count, accepted_count, rejected_count, master_record_count, error_count,
                    input_fingerprint
                ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    started_at = COALESCE(pipeline_runs.started_at, excluded.started_at),
                    completed_at = NULL,
                    status = excluded.status,
                    customer_input_count = excluded.customer_input_count,
                    product_input_count = excluded.product_input_count,
                    accepted_count = excluded.accepted_count,
                    rejected_count = excluded.rejected_count,
                    master_record_count = excluded.master_record_count,
                    error_count = excluded.error_count,
                    input_fingerprint = COALESCE(pipeline_runs.input_fingerprint, excluded.input_fingerprint)""",
                (run_id, timestamp, status, *counts, input_fingerprint),
            )


def update_pipeline_run_status(
    run_id: str,
    status: str,
    *,
    db_path: str | Path | None = None,
    completed_at: datetime | str | None = None,
    customer_input_count: int | None = None,
    product_input_count: int | None = None,
    accepted_count: int | None = None,
    rejected_count: int | None = None,
    master_record_count: int | None = None,
    error_count: int | None = None,
) -> None:
    """Update run status and any supplied aggregate counts."""

    fields = {
        "customer_input_count": customer_input_count,
        "product_input_count": product_input_count,
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "master_record_count": master_record_count,
        "error_count": error_count,
    }
    for key, value in fields.items():
        if value is not None and (not isinstance(value, int) or value < 0):
            raise ValueError(f"{key} must be a non-negative integer.")
    completed_value = _iso(completed_at)
    if completed_value is None and status.upper() != "RUNNING":
        completed_value = _iso(_now())
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            assignments = ["status = ?", "completed_at = ?"]
            values: list[Any] = [status, completed_value]
            for name, value in fields.items():
                if value is not None:
                    assignments.append(f"{name} = ?")
                    values.append(value)
            values.append(run_id)
            cursor = connection.execute(
                f"UPDATE pipeline_runs SET {', '.join(assignments)} WHERE run_id = ?", values
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Pipeline run {run_id!r} does not exist.")


def _source_key(entity_type: str, source_fields: Mapping[str, Any]) -> str:
    if entity_type not in _SCHEMAS:
        raise ValueError(f"Unsupported entity type {entity_type!r}.")
    value = source_fields.get(_SCHEMAS[entity_type].primary_key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Source record has no usable {_SCHEMAS[entity_type].primary_key}.")
    return value


def _record_parts(record: Any) -> tuple[Mapping[str, Any], str, str, int, str, str, datetime, str | None]:
    if isinstance(record, NormalizedRecord):
        return (
            record.source_fields, record.entity_type, record.source_file,
            record.source_record_number, record.source_format, "ACCEPTED",
            record.ingested_at, _json({"source_fields": record.source_fields, "normalized_fields": record.normalized_fields,
                                       "entity_type": record.entity_type, "source_file": record.source_file,
                                       "source_record_number": record.source_record_number,
                                       "source_format": record.source_format, "ingested_at": record.ingested_at}),
        )
    if isinstance(record, RejectedRecord):
        return (record.source_fields, record.entity_type, record.source_file, record.source_record_number,
                record.source_format, "REJECTED", record.ingested_at, None)
    if isinstance(record, IngestedRecord):
        return (record.source_fields, record.entity_type, record.source_file, record.source_record_number,
                record.source_format, "UNVALIDATED", record.ingested_at, None)
    raise TypeError("Source records must be IngestedRecord, NormalizedRecord, or RejectedRecord values.")


def save_source_records(
    run_id: str,
    records: Iterable[IngestedRecord | NormalizedRecord | RejectedRecord],
    *,
    db_path: str | Path | None = None,
    normalized_records: Iterable[NormalizedRecord] = (),
    validation_statuses: Mapping[tuple[str, str], str] | None = None,
    source_record_id_overrides: Mapping[tuple[str, str, int], str] | None = None,
) -> int:
    """Upsert source rows and return the number of submitted records.

    Existing records for the same run/entity/source ID are updated in place.
    Normalized records can be supplied separately to associate them with their
    original ingested records; rejected validation objects are marked rejected.
    ``source_record_id_overrides`` supplies storage-only keys for rejected
    records whose source ID is missing or collides, without changing source JSON.
    """

    batch = list(records)
    normalized_by_key: dict[tuple[str, str], NormalizedRecord] = {}
    for normalized in normalized_records:
        if not isinstance(normalized, NormalizedRecord):
            raise TypeError("normalized_records must contain NormalizedRecord values.")
        normalized_by_key[(normalized.entity_type, _source_key(normalized.entity_type, normalized.source_fields))] = normalized
    rows = []
    for record in batch:
        source_fields, entity_type, source_file, number, source_format, default_status, ingested_at, embedded_normalized = _record_parts(record)
        override_key = (entity_type, source_file, number)
        source_id = (source_record_id_overrides or {}).get(override_key)
        if source_id is None:
            try:
                source_id = _source_key(entity_type, source_fields)
            except ValueError:
                raise ValueError(
                    f"Source record from {source_file!r} at record {number} has no source ID; "
                    "provide a deterministic storage ID override to persist it."
                ) from None
        elif not isinstance(source_id, str) or not source_id.strip():
            raise ValueError(f"Invalid source record ID override for {override_key!r}.")
        _reject_secret_keys(source_fields)
        normalized = record if isinstance(record, NormalizedRecord) else normalized_by_key.get((entity_type, source_id))
        normalized_json = embedded_normalized
        if normalized is not None and not isinstance(record, NormalizedRecord):
            normalized_json = _json({"source_fields": normalized.source_fields, "normalized_fields": normalized.normalized_fields,
                                     "entity_type": normalized.entity_type, "source_file": normalized.source_file,
                                     "source_record_number": normalized.source_record_number,
                                     "source_format": normalized.source_format, "ingested_at": normalized.ingested_at})
        status = (validation_statuses or {}).get((entity_type, source_id), default_status)
        rows.append((source_id, run_id, entity_type, source_fields.get("source_system"), source_file,
                     number, _json(source_fields), normalized_json, status))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            connection.executemany(
                """INSERT INTO source_records (
                    source_record_id, run_id, entity_type, source_system, source_file,
                    source_record_number, original_record_json, normalized_record_json, validation_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, entity_type, source_record_id) DO UPDATE SET
                    source_system = excluded.source_system,
                    source_file = excluded.source_file,
                    source_record_number = excluded.source_record_number,
                    original_record_json = excluded.original_record_json,
                    normalized_record_json = excluded.normalized_record_json,
                    validation_status = excluded.validation_status""",
                rows,
            )
    return len(rows)


def _pair_id(run_id: str, entity_type: str, first: str, second: str) -> str:
    encoded = _json([run_id, entity_type, first, second])
    return "pair-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


def _decision_rows(run_id: str, decisions: Iterable[DuplicateDecision]) -> list[tuple[Any, ...]]:
    rows = []
    for decision in decisions:
        if decision.entity_type not in _SCHEMAS:
            raise ValueError(f"Unsupported entity type {decision.entity_type!r} in duplicate decision.")
        if not decision.record_id_1 or not decision.record_id_2 or decision.record_id_1 == decision.record_id_2:
            raise ValueError("Duplicate decisions must reference two distinct non-empty record IDs.")
        first, second = sorted((decision.record_id_1, decision.record_id_2))
        matching = dict(decision.matching_evidence or {})
        conflicting = dict(decision.conflicting_evidence or {})
        _reject_secret_keys(matching, "matching_evidence")
        _reject_secret_keys(conflicting, "conflicting_evidence")
        rows.append((_pair_id(run_id, decision.entity_type, first, second), run_id, decision.entity_type,
                     first, second, decision.decision, decision.confidence_score, _json(matching), _json(conflicting)))
    return rows


def _decisions_from_results(
    exact_result: DetectionResult | None,
    probable_result: ProbableMatchingResult | None,
) -> list[DuplicateDecision]:
    decisions: list[DuplicateDecision] = []
    if exact_result is not None:
        groups = {group.group_id: group for group in exact_result.duplicate_groups}
        for pair in exact_result.duplicate_pairs:
            group = groups[pair.group_id]
            first, second = pair.source_record_ids
            decisions.append(DuplicateDecision(pair.entity_type, first, second, "EXACT", None,
                                               {"matching_attributes": group.matching_attributes,
                                                "matching_evidence": group.matching_evidence}, {}))
        for conflict in exact_result.conflict_candidates:
            first, second = conflict.source_record_ids
            decisions.append(DuplicateDecision(conflict.entity_type, first, second, "IDENTITY_CONFLICT", None,
                                               {}, {"conflicting_fields": conflict.conflicting_fields,
                                                    "explanation": conflict.explanation}))
    if probable_result is not None:
        for pair in probable_result.probable_pairs:
            decisions.append(DuplicateDecision(
                entity_type=pair.entity_type,
                record_id_1=pair.source_record_id_1,
                record_id_2=pair.source_record_id_2,
                decision=pair.decision,
                confidence_score=pair.overall_confidence_score,
                matching_evidence={"matching_attributes": pair.matching_attributes,
                                   "similarity_scores": pair.similarity_scores,
                                   "explanation": pair.explanation},
                conflicting_evidence={"conflicting_attributes": pair.conflicting_attributes},
            ))
    # Exact groups and probable decisions normally do not overlap, but ensure
    # a malformed caller cannot create two rows for the same natural pair.
    by_key: dict[tuple[str, str, str], DuplicateDecision] = {}
    for item in decisions:
        first, second = sorted((item.record_id_1, item.record_id_2))
        key = (item.entity_type, first, second)
        current = by_key.get(key)
        # Preserve the more specific exact-identifier conflict if probable
        # matching also emits a decision for the same source pair. The review
        # queue independently retains the probable decision and its evidence.
        if current is not None and current.decision == "IDENTITY_CONFLICT" and item.decision != "IDENTITY_CONFLICT":
            continue
        by_key[key] = item
    return [by_key[key] for key in sorted(by_key)]


def save_duplicate_decisions(
    run_id: str,
    decisions: Iterable[DuplicateDecision] | None = None,
    *,
    db_path: str | Path | None = None,
    exact_result: DetectionResult | None = None,
    probable_result: ProbableMatchingResult | None = None,
) -> int:
    """Upsert pair decisions from explicit rows and/or detection results."""

    combined = list(decisions or ()) + _decisions_from_results(exact_result, probable_result)
    rows = _decision_rows(run_id, combined)
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            connection.executemany(
                """INSERT INTO duplicate_pairs (
                    pair_id, run_id, entity_type, record_id_1, record_id_2, decision,
                    confidence_score, matching_evidence_json, conflicting_evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, entity_type, record_id_1, record_id_2) DO UPDATE SET
                    decision = excluded.decision,
                    confidence_score = excluded.confidence_score,
                    matching_evidence_json = excluded.matching_evidence_json,
                    conflicting_evidence_json = excluded.conflicting_evidence_json""",
                rows,
            )
    return len(rows)


def _master_json(master: MasterRecord) -> str:
    payload = {
        "master_id": master.master_id,
        "entity_type": master.entity_type,
        "merged_fields": master.merged_fields,
        "contributing_source_record_ids": master.contributing_source_record_ids,
        "contributing_source_systems": master.contributing_source_systems,
        "field_level_decisions": master.field_level_decisions,
        "merge_method": master.merge_method,
        "created_at": master.created_at,
    }
    _reject_secret_keys(payload, "master_record")
    return _json(payload)


def _lineage_id(run_id: str, master_id: str, field_name: str) -> str:
    encoded = _json([run_id, master_id, field_name])
    return "lineage-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


def _field_value(value: Any) -> str | None:
    return None if value is None else _json(value)


def _lineage_rows(run_id: str, masters: Iterable[MasterRecord]) -> list[tuple[Any, ...]]:
    rows = []
    for master in masters:
        for field_name, decision in master.field_level_decisions.items():
            rejected = [asdict(item) if isinstance(item, RejectedFieldValue) else item
                        for item in decision.rejected_conflicting_values]
            _reject_secret_keys({"selected_value": decision.selected_value, "rejected_values": rejected}, "field_lineage")
            rows.append((_lineage_id(run_id, master.master_id, field_name), run_id, master.master_id,
                         field_name, _field_value(decision.selected_value), decision.selected_source_record_id,
                         decision.selected_source_system, decision.selection_rule, _json(rejected)))
    return rows


def _save_lineage_rows(connection: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
    connection.executemany(
        """INSERT INTO field_lineage (
            lineage_id, run_id, master_id, field_name, selected_value,
            selected_source_record_id, selected_source_system, selection_rule, rejected_values_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, master_id, field_name) DO UPDATE SET
            selected_value = excluded.selected_value,
            selected_source_record_id = excluded.selected_source_record_id,
            selected_source_system = excluded.selected_source_system,
            selection_rule = excluded.selection_rule,
            rejected_values_json = excluded.rejected_values_json""",
        rows,
    )


def save_field_lineage(
    run_id: str,
    master_records: Iterable[MasterRecord],
    *,
    db_path: str | Path | None = None,
) -> int:
    """Persist field-level lineage for already stored master records."""

    rows = _lineage_rows(run_id, list(master_records))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            _save_lineage_rows(connection, rows)
    return len(rows)


def save_master_records(
    run_id: str,
    master_records: Iterable[MasterRecord],
    *,
    db_path: str | Path | None = None,
    include_lineage: bool = True,
) -> int:
    """Persist masters and, by default, their lineage in one transaction."""

    masters = list(master_records)
    rows = []
    for master in masters:
        if master.entity_type not in _SCHEMAS:
            raise ValueError(f"Unsupported entity type {master.entity_type!r} in master record.")
        rows.append((run_id, master.master_id, master.entity_type, master.merge_method,
                     _master_json(master), _iso(master.created_at)))
    lineage = _lineage_rows(run_id, masters) if include_lineage else []
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            connection.executemany(
                """INSERT INTO master_records (run_id, master_id, entity_type, merge_method, merged_record_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, master_id) DO UPDATE SET
                    entity_type = excluded.entity_type,
                    merge_method = excluded.merge_method,
                    merged_record_json = excluded.merged_record_json,
                    created_at = excluded.created_at""",
                rows,
            )
            if include_lineage:
                _save_lineage_rows(connection, lineage)
    return len(rows)


def _error_id(run_id: str, error: PipelineError) -> str:
    identity = ([run_id, "caller_id", error.error_id] if error.error_id else
                [run_id, error.stage, error.error_code, error.error_message, error.source_record_id])
    return "error-" + hashlib.sha256(_json(identity).encode("utf-8")).hexdigest()[:24]


def save_pipeline_errors(
    run_id: str,
    errors: Iterable[PipelineError | Mapping[str, Any]],
    *,
    db_path: str | Path | None = None,
) -> int:
    """Upsert errors by caller ID or deterministic run/error content hash."""

    batch = [error if isinstance(error, PipelineError) else PipelineError(**error) for error in errors]
    rows = []
    for error in batch:
        _reject_secret_keys({"error_message": error.error_message}, "pipeline_error")
        rows.append((_error_id(run_id, error), run_id, error.stage, error.error_code,
                     error.error_message, error.source_record_id, _iso(error.created_at or _now())))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            connection.executemany(
                """INSERT INTO pipeline_errors (error_id, run_id, stage, error_code, error_message, source_record_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(error_id) DO UPDATE SET
                    stage = excluded.stage,
                    error_code = excluded.error_code,
                    error_message = excluded.error_message,
                    source_record_id = excluded.source_record_id,
                    created_at = excluded.created_at""",
                rows,
            )
    return len(rows)


_REVIEW_STATUSES = {"PENDING", "ACCEPTED_MATCH", "REJECTED_MATCH", "DEFERRED"}
_REVIEW_TRANSITIONS = {
    "PENDING": {"ACCEPTED_MATCH", "REJECTED_MATCH", "DEFERRED"},
    "DEFERRED": {"PENDING", "ACCEPTED_MATCH", "REJECTED_MATCH"},
    "ACCEPTED_MATCH": set(),
    "REJECTED_MATCH": set(),
}


def _review_payload(item: Any) -> dict[str, Any]:
    if is_dataclass(item):
        return asdict(item)
    if isinstance(item, Mapping):
        return dict(item)
    raise TypeError("Review items must be dataclass or mapping values.")


def save_review_items(
    run_id: str,
    review_items: Iterable[Any],
    *,
    db_path: str | Path | None = None,
) -> int:
    """Insert new pending review items idempotently without changing decisions.

    A CREATED event is recorded in ``review_history`` only when an item is
    first inserted. Repeated pipeline runs cannot reset its reviewed state.
    Returns the number of newly inserted review items.
    """

    payloads = [_review_payload(item) for item in review_items]
    rows = []
    for item in payloads:
        entity = item.get("entity_type")
        first, second = item.get("record_id_1"), item.get("record_id_2")
        if entity not in _SCHEMAS or not isinstance(first, str) or not isinstance(second, str) or first >= second:
            raise ValueError("Review items require a valid entity and sorted, distinct source record IDs.")
        status = item.get("review_status", "PENDING")
        if status != "PENDING":
            raise ValueError("New review items must start in PENDING status.")
        review_id = _validate_review_id(item.get("review_id"))
        matching = item.get("matching_evidence") or {}
        conflicting = item.get("conflicting_evidence") or {}
        _reject_secret_keys(matching, "review_matching_evidence")
        _reject_secret_keys(conflicting, "review_conflicting_evidence")
        rows.append((
            run_id, review_id, entity, first, second, item.get("confidence_score"),
            _json(matching), _json(conflicting), item.get("decision_explanation", ""), status,
            _iso(item.get("created_at")), _iso(item.get("reviewed_at")), item.get("reviewer_id"),
            item.get("reviewer_notes"),
        ))
    inserted = 0
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            for row in rows:
                cursor = connection.execute(
                    """INSERT INTO review_items (
                        run_id, review_id, entity_type, record_id_1, record_id_2, confidence_score,
                        matching_evidence_json, conflicting_evidence_json, decision_explanation,
                        review_status, created_at, reviewed_at, reviewer_id, reviewer_notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, entity_type, record_id_1, record_id_2) DO NOTHING""",
                    row,
                )
                if cursor.rowcount:
                    inserted += 1
                    connection.execute(
                        """INSERT INTO review_history (
                            history_id, run_id, review_id, previous_status, new_status,
                            reviewer_id, reviewer_notes, changed_at
                        ) VALUES (?, ?, ?, NULL, 'PENDING', NULL, NULL, ?)""",
                        (uuid.uuid4().hex, run_id, row[1], row[10]),
                    )
    return inserted


def _validate_review_status(status: str) -> str:
    if not isinstance(status, str) or status not in _REVIEW_STATUSES:
        raise ValueError(f"Invalid review status {status!r}.")
    return status


def _validate_review_id(review_id: str) -> str:
    if not isinstance(review_id, str) or not re.fullmatch(r"review-[0-9a-f]{24}", review_id):
        raise ValueError("Invalid review_id format.")
    return review_id


def _review_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "review_id": row["review_id"],
        "run_id": row["run_id"],
        "entity_type": row["entity_type"],
        "record_id_1": row["record_id_1"],
        "record_id_2": row["record_id_2"],
        "confidence_score": row["confidence_score"],
        "matching_evidence": json.loads(row["matching_evidence_json"]),
        "conflicting_evidence": json.loads(row["conflicting_evidence_json"]),
        "decision_explanation": row["decision_explanation"],
        "review_status": row["review_status"],
        "created_at": row["created_at"],
        "reviewed_at": row["reviewed_at"],
        "reviewer_id": row["reviewer_id"],
        "reviewer_notes": row["reviewer_notes"],
    }


def get_review_item(
    run_id: str,
    review_id: str,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    review_id = _validate_review_id(review_id)
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        row = connection.execute(
            "SELECT * FROM review_items WHERE run_id = ? AND review_id = ?", (run_id, review_id)
        ).fetchone()
        return None if row is None else _review_row(row)


def get_review_history(
    run_id: str,
    review_id: str,
    *,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    review_id = _validate_review_id(review_id)
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            """SELECT previous_status, new_status, reviewer_id, reviewer_notes, changed_at
            FROM review_history WHERE run_id = ? AND review_id = ? ORDER BY changed_at, history_id""",
            (run_id, review_id),
        ).fetchall()
        return [dict(row) for row in rows]


def get_review_items(
    run_id: str,
    *,
    entity_type: str | None = None,
    review_status: str | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type is not None:
        if entity_type not in _SCHEMAS:
            raise ValueError(f"Unsupported entity type {entity_type!r}.")
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if review_status is not None:
        clauses.append("review_status = ?")
        values.append(_validate_review_status(review_status))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            f"SELECT * FROM review_items WHERE {' AND '.join(clauses)} ORDER BY entity_type, record_id_1, record_id_2",
            values,
        ).fetchall()
        return [_review_row(row) for row in rows]


def get_pending_review_items(run_id: str, *, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    return get_review_items(run_id, review_status="PENDING", db_path=db_path)


def get_review_items_by_entity(
    run_id: str, entity_type: str, *, db_path: str | Path | None = None
) -> list[dict[str, Any]]:
    return get_review_items(run_id, entity_type=entity_type, db_path=db_path)


def update_review_item(
    run_id: str,
    review_id: str,
    new_status: str,
    *,
    reviewer_id: str | None = None,
    reviewer_notes: str | None = None,
    reviewed_at: datetime | str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Apply one valid status transition and append history atomically."""

    review_id = _validate_review_id(review_id)
    new_status = _validate_review_status(new_status)
    timestamp = _iso(reviewed_at or datetime.now(timezone.utc))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            row = connection.execute(
                "SELECT review_status FROM review_items WHERE run_id = ? AND review_id = ?",
                (run_id, review_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"Review item {review_id!r} was not found in run {run_id!r}.")
            previous = row["review_status"]
            if new_status not in _REVIEW_TRANSITIONS[previous]:
                raise ValueError(f"Invalid review transition {previous} -> {new_status}.")
            cursor = connection.execute(
                """UPDATE review_items SET review_status = ?, reviewed_at = ?, reviewer_id = ?, reviewer_notes = ?
                WHERE run_id = ? AND review_id = ? AND review_status = ?""",
                (new_status, timestamp, reviewer_id, reviewer_notes, run_id, review_id, previous),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Review item changed concurrently; no decision was applied.")
            connection.execute(
                """INSERT INTO review_history (
                    history_id, run_id, review_id, previous_status, new_status,
                    reviewer_id, reviewer_notes, changed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (uuid.uuid4().hex, run_id, review_id, previous, new_status,
                 reviewer_id, reviewer_notes, timestamp),
            )
            updated = connection.execute(
                "SELECT * FROM review_items WHERE run_id = ? AND review_id = ?", (run_id, review_id)
            ).fetchone()
            return _review_row(updated)


def get_review_statistics(run_id: str, *, db_path: str | Path | None = None) -> dict[str, Any]:
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            "SELECT entity_type, review_status, COUNT(*) AS total FROM review_items WHERE run_id = ? GROUP BY entity_type, review_status",
            (run_id,),
        ).fetchall()
    stats: dict[str, Any] = {
        "total_review_items": 0,
        "pending_reviews": 0,
        "accepted_reviews": 0,
        "rejected_reviews": 0,
        "deferred_reviews": 0,
        "customer_review_count": 0,
        "product_review_count": 0,
        "by_entity": {"customer": {}, "product": {}},
    }
    status_keys = {
        "PENDING": "pending_reviews",
        "ACCEPTED_MATCH": "accepted_reviews",
        "REJECTED_MATCH": "rejected_reviews",
        "DEFERRED": "deferred_reviews",
    }
    for row in rows:
        entity, status, count = row["entity_type"], row["review_status"], int(row["total"])
        stats["total_review_items"] += count
        stats[status_keys[status]] += count
        stats[f"{entity}_review_count"] += count
        stats["by_entity"][entity][status] = count
    for entity in ("customer", "product"):
        for status in _REVIEW_STATUSES:
            stats["by_entity"][entity].setdefault(status, 0)
    return stats


_QUARANTINE_STATUSES = {"QUARANTINED", "RESOLVED", "DISCARDED"}


def _validate_quarantine_id(quarantine_id: str) -> str:
    if not isinstance(quarantine_id, str) or not re.fullmatch(r"quarantine-[0-9a-f]{24}", quarantine_id):
        raise ValueError("Invalid quarantine_id format.")
    return quarantine_id


def _quarantine_payload(item: Any) -> dict[str, Any]:
    if is_dataclass(item):
        return asdict(item)
    if isinstance(item, Mapping):
        return dict(item)
    raise TypeError("Quarantine entries must be dataclass or mapping values.")


def save_quarantine_entries(
    run_id: str,
    entries: Iterable[Any],
    *,
    db_path: str | Path | None = None,
) -> int:
    """Insert quarantined records idempotently and audit their creation.

    Existing entries are left unchanged so ordinary reruns cannot reset a
    resolved/discarded state or overwrite its history. Returns newly inserted
    quarantine row count.
    """

    payloads = [_quarantine_payload(entry) for entry in entries]
    rows = []
    for item in payloads:
        quarantine_id = _validate_quarantine_id(item.get("quarantine_id"))
        if item.get("run_id") != run_id:
            raise ValueError("Quarantine entry run_id does not match the persistence run.")
        if item.get("entity_type") not in _SCHEMAS:
            raise ValueError(f"Invalid quarantine entity_type {item.get('entity_type')!r}.")
        status = item.get("quarantine_status", "QUARANTINED")
        if status != "QUARANTINED":
            raise ValueError("New quarantine entries must start in QUARANTINED status.")
        issues = item.get("validation_issues") or []
        if not issues:
            raise ValueError("Quarantine entries must retain at least one validation issue.")
        _reject_secret_keys(item.get("original_record") or {}, "quarantine_original_record")
        rows.append((
            run_id, quarantine_id, item["entity_type"], item.get("source_record_id"),
            item["source_file"], item["source_record_number"], _json(item.get("original_record") or {}),
            _json(issues), status, _iso(item.get("quarantined_at")), item.get("resolved_at"),
            item.get("resolution_action"), item.get("resolution_notes"),
        ))
    inserted = 0
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            for row in rows:
                cursor = connection.execute(
                    """INSERT INTO quarantine_records (
                        run_id, quarantine_id, entity_type, source_record_id, source_file,
                        source_record_number, original_record_json, validation_issues_json,
                        quarantine_status, quarantined_at, resolved_at, resolution_action, resolution_notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, entity_type, source_file, source_record_number) DO NOTHING""",
                    row,
                )
                if cursor.rowcount:
                    inserted += 1
                    connection.execute(
                        """INSERT INTO quarantine_history (
                            history_id, run_id, quarantine_id, previous_status, new_status,
                            resolution_action, resolution_notes, changed_at
                        ) VALUES (?, ?, ?, NULL, 'QUARANTINED', NULL, NULL, ?)""",
                        (uuid.uuid4().hex, run_id, row[1], row[9]),
                    )
    return inserted


def _quarantine_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "quarantine_id": row["quarantine_id"],
        "run_id": row["run_id"],
        "entity_type": row["entity_type"],
        "source_record_id": row["source_record_id"],
        "source_file": row["source_file"],
        "source_record_number": row["source_record_number"],
        "original_record": json.loads(row["original_record_json"]),
        "validation_issues": json.loads(row["validation_issues_json"]),
        "quarantine_status": row["quarantine_status"],
        "quarantined_at": row["quarantined_at"],
        "resolved_at": row["resolved_at"],
        "resolution_action": row["resolution_action"],
        "resolution_notes": row["resolution_notes"],
    }


def get_quarantine_record(
    run_id: str,
    quarantine_id: str,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    quarantine_id = _validate_quarantine_id(quarantine_id)
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        row = connection.execute(
            "SELECT * FROM quarantine_records WHERE run_id = ? AND quarantine_id = ?",
            (run_id, quarantine_id),
        ).fetchone()
        return None if row is None else _quarantine_row(row)


def get_quarantine_history(
    run_id: str,
    quarantine_id: str,
    *,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    quarantine_id = _validate_quarantine_id(quarantine_id)
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            """SELECT previous_status, new_status, resolution_action, resolution_notes, changed_at
            FROM quarantine_history WHERE run_id = ? AND quarantine_id = ? ORDER BY changed_at, history_id""",
            (run_id, quarantine_id),
        ).fetchall()
        return [dict(row) for row in rows]


def get_quarantine_records(
    run_id: str,
    *,
    entity_type: str | None = None,
    error_code: str | None = None,
    quarantine_status: str | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type is not None:
        if entity_type not in _SCHEMAS:
            raise ValueError(f"Unsupported entity type {entity_type!r}.")
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if quarantine_status is not None:
        if quarantine_status not in _QUARANTINE_STATUSES:
            raise ValueError(f"Invalid quarantine status {quarantine_status!r}.")
        clauses.append("quarantine_status = ?")
        values.append(quarantine_status)
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            f"SELECT * FROM quarantine_records WHERE {' AND '.join(clauses)} ORDER BY entity_type, source_file, source_record_number",
            values,
        ).fetchall()
        records = [_quarantine_row(row) for row in rows]
    if error_code is not None:
        records = [record for record in records
                   if any(issue.get("error_code") == error_code for issue in record["validation_issues"])]
    return records


def update_quarantine_record(
    run_id: str,
    quarantine_id: str,
    new_status: str,
    *,
    resolution_action: str,
    resolution_notes: str | None = None,
    resolved_at: datetime | str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Resolve or discard one item with an atomic audit-history append."""

    quarantine_id = _validate_quarantine_id(quarantine_id)
    if new_status not in {"RESOLVED", "DISCARDED"}:
        raise ValueError(f"Invalid quarantine status transition target {new_status!r}.")
    if not isinstance(resolution_action, str) or not resolution_action.strip():
        raise ValueError("resolution_action must be a non-empty string.")
    timestamp = _iso(resolved_at or datetime.now(timezone.utc))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        with connection:
            row = connection.execute(
                "SELECT quarantine_status FROM quarantine_records WHERE run_id = ? AND quarantine_id = ?",
                (run_id, quarantine_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"Quarantine record {quarantine_id!r} was not found in run {run_id!r}.")
            previous = row["quarantine_status"]
            if previous != "QUARANTINED":
                raise ValueError(f"Invalid quarantine transition {previous} -> {new_status}.")
            cursor = connection.execute(
                """UPDATE quarantine_records SET quarantine_status = ?, resolved_at = ?,
                    resolution_action = ?, resolution_notes = ?
                WHERE run_id = ? AND quarantine_id = ? AND quarantine_status = 'QUARANTINED'""",
                (new_status, timestamp, resolution_action, resolution_notes, run_id, quarantine_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Quarantine record changed concurrently; no resolution was applied.")
            connection.execute(
                """INSERT INTO quarantine_history (
                    history_id, run_id, quarantine_id, previous_status, new_status,
                    resolution_action, resolution_notes, changed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (uuid.uuid4().hex, run_id, quarantine_id, previous, new_status,
                 resolution_action, resolution_notes, timestamp),
            )
            updated = connection.execute(
                "SELECT * FROM quarantine_records WHERE run_id = ? AND quarantine_id = ?",
                (run_id, quarantine_id),
            ).fetchone()
            return _quarantine_row(updated)


def get_quarantine_statistics(run_id: str, *, db_path: str | Path | None = None) -> dict[str, Any]:
    records = get_quarantine_records(run_id, db_path=db_path)
    stats: dict[str, Any] = {
        "total_quarantined_records": len(records),
        "quarantined_count": 0,
        "resolved_count": 0,
        "discarded_count": 0,
        "customer_quarantine_count": 0,
        "product_quarantine_count": 0,
        "counts_by_validation_error_code": {},
        "by_entity": {"customer": {"QUARANTINED": 0, "RESOLVED": 0, "DISCARDED": 0},
                      "product": {"QUARANTINED": 0, "RESOLVED": 0, "DISCARDED": 0}},
    }
    status_keys = {"QUARANTINED": "quarantined_count", "RESOLVED": "resolved_count", "DISCARDED": "discarded_count"}
    for record in records:
        status, entity = record["quarantine_status"], record["entity_type"]
        stats[status_keys[status]] += 1
        stats[f"{entity}_quarantine_count"] += 1
        stats["by_entity"][entity][status] += 1
        for issue in record["validation_issues"]:
            code = issue["error_code"]
            stats["counts_by_validation_error_code"][code] = stats["counts_by_validation_error_code"].get(code, 0) + 1
    return stats


def get_pipeline_run_summary(run_id: str, *, db_path: str | Path | None = None) -> dict[str, Any] | None:
    """Return run metadata and persisted artifact counts, or ``None``."""

    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        row = connection.execute("SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        summary = dict(row)
        for name, query in (
            ("source_record_rows", "SELECT COUNT(*) FROM source_records WHERE run_id = ?"),
            ("duplicate_pair_rows", "SELECT COUNT(*) FROM duplicate_pairs WHERE run_id = ?"),
            ("master_record_rows", "SELECT COUNT(*) FROM master_records WHERE run_id = ?"),
            ("field_lineage_rows", "SELECT COUNT(*) FROM field_lineage WHERE run_id = ?"),
            ("pipeline_error_rows", "SELECT COUNT(*) FROM pipeline_errors WHERE run_id = ?"),
            ("review_item_rows", "SELECT COUNT(*) FROM review_items WHERE run_id = ?"),
            ("review_history_rows", "SELECT COUNT(*) FROM review_history WHERE run_id = ?"),
            ("quarantine_record_rows", "SELECT COUNT(*) FROM quarantine_records WHERE run_id = ?"),
            ("quarantine_history_rows", "SELECT COUNT(*) FROM quarantine_history WHERE run_id = ?"),
        ):
            summary[name] = connection.execute(query, (run_id,)).fetchone()[0]
        return summary


def _validate_pagination(limit: int, offset: int) -> tuple[int, int]:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer.")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError("offset must be a non-negative integer.")
    return limit, offset


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("run_id must be a non-empty string.")
    return run_id


def get_source_records(
    run_id: str,
    *,
    entity_type: str | None = None,
    validation_status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Retrieve source rows for one run, decoding their original and normalized JSON."""

    run_id = _validate_run_id(run_id)
    limit, offset = _validate_pagination(limit, offset)
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type is not None:
        if entity_type not in _SCHEMAS:
            raise ValueError(f"Unsupported entity type {entity_type!r}.")
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if validation_status is not None:
        if not isinstance(validation_status, str) or not validation_status:
            raise ValueError("validation_status must be a non-empty string when supplied.")
        clauses.append("validation_status = ?")
        values.append(validation_status)
    values.extend((limit, offset))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            f"""SELECT * FROM source_records WHERE {' AND '.join(clauses)}
            ORDER BY entity_type, source_file, source_record_number, source_record_id
            LIMIT ? OFFSET ?""",
            values,
        ).fetchall()
        return [
            {
                **dict(row),
                "original_record": json.loads(row["original_record_json"]),
                "normalized_record": (
                    json.loads(row["normalized_record_json"])
                    if row["normalized_record_json"] is not None else None
                ),
            }
            for row in rows
        ]


def get_duplicate_decisions(
    run_id: str,
    *,
    entity_type: str | None = None,
    decision: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Retrieve run-scoped pair decisions with decoded evidence payloads."""

    run_id = _validate_run_id(run_id)
    limit, offset = _validate_pagination(limit, offset)
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type is not None:
        if entity_type not in _SCHEMAS:
            raise ValueError(f"Unsupported entity type {entity_type!r}.")
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if decision is not None:
        if not isinstance(decision, str) or not decision:
            raise ValueError("decision must be a non-empty string when supplied.")
        clauses.append("decision = ?")
        values.append(decision)
    values.extend((limit, offset))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            f"""SELECT * FROM duplicate_pairs WHERE {' AND '.join(clauses)}
            ORDER BY entity_type, record_id_1, record_id_2, pair_id
            LIMIT ? OFFSET ?""",
            values,
        ).fetchall()
        return [
            {
                **dict(row),
                "matching_evidence": json.loads(row["matching_evidence_json"]),
                "conflicting_evidence": json.loads(row["conflicting_evidence_json"]),
            }
            for row in rows
        ]


def get_pipeline_errors(
    run_id: str,
    *,
    stage: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Retrieve operational errors for one run, optionally scoped to a stage."""

    run_id = _validate_run_id(run_id)
    limit, offset = _validate_pagination(limit, offset)
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if stage is not None:
        if not isinstance(stage, str) or not stage:
            raise ValueError("stage must be a non-empty string when supplied.")
        clauses.append("stage = ?")
        values.append(stage)
    values.extend((limit, offset))
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            f"""SELECT * FROM pipeline_errors WHERE {' AND '.join(clauses)}
            ORDER BY created_at, error_id LIMIT ? OFFSET ?""",
            values,
        ).fetchall()
        return [dict(row) for row in rows]


def get_field_lineage(
    run_id: str,
    master_id: str,
    *,
    field_name: str | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Retrieve one master's field lineage, including rejected-value detail."""

    run_id = _validate_run_id(run_id)
    if not isinstance(master_id, str) or not master_id:
        raise ValueError("master_id must be a non-empty string.")
    clauses = ["run_id = ?", "master_id = ?"]
    values: list[Any] = [run_id, master_id]
    if field_name is not None:
        if not isinstance(field_name, str) or not field_name:
            raise ValueError("field_name must be a non-empty string when supplied.")
        clauses.append("field_name = ?")
        values.append(field_name)
    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            f"""SELECT * FROM field_lineage WHERE {' AND '.join(clauses)}
            ORDER BY field_name, lineage_id""",
            values,
        ).fetchall()
        return [
            {
                **dict(row),
                "selected_value": (
                    json.loads(row["selected_value"])
                    if row["selected_value"] is not None else None
                ),
                "rejected_values": json.loads(row["rejected_values_json"]),
            }
            for row in rows
        ]


def get_master_records(run_id: str, *, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    """Retrieve masters with decoded merged data and field-lineage rows."""

    with _connect(db_path) as connection:
        _ensure_initialized(connection)
        rows = connection.execute(
            "SELECT * FROM master_records WHERE run_id = ? ORDER BY entity_type, master_id", (run_id,)
        ).fetchall()
        masters = []
        for row in rows:
            payload = json.loads(row["merged_record_json"])
            lineage_rows = connection.execute(
                "SELECT * FROM field_lineage WHERE run_id = ? AND master_id = ? ORDER BY field_name",
                (run_id, row["master_id"]),
            ).fetchall()
            payload["field_lineage"] = [
                {
                    "field_name": item["field_name"],
                    "selected_value": json.loads(item["selected_value"]) if item["selected_value"] is not None else None,
                    "selected_source_record_id": item["selected_source_record_id"],
                    "selected_source_system": item["selected_source_system"],
                    "selection_rule": item["selection_rule"],
                    "rejected_values": json.loads(item["rejected_values_json"]),
                }
                for item in lineage_rows
            ]
            masters.append(payload)
        return masters


__all__ = (
    "DEFAULT_DATABASE_PATH", "DuplicateDecision", "PipelineError", "RunInputMismatchError", "initialize_database",
    "create_pipeline_run", "update_pipeline_run_status", "save_source_records",
    "save_duplicate_decisions", "save_master_records", "save_field_lineage",
    "save_pipeline_errors", "get_pipeline_run_summary", "get_master_records",
    "get_source_records", "get_duplicate_decisions", "get_pipeline_errors", "get_field_lineage",
    "save_review_items", "get_review_item", "get_review_history", "get_review_items",
    "get_pending_review_items", "get_review_items_by_entity", "update_review_item",
    "get_review_statistics",
    "save_quarantine_entries", "get_quarantine_record", "get_quarantine_history",
    "get_quarantine_records", "update_quarantine_record", "get_quarantine_statistics",
)
