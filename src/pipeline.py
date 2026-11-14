"""End-to-end MergeMind AI pipeline orchestration.

This module wires existing ingestion, validation, normalization, matching,
survivorship, persistence, and reporting APIs together. It contains no matching
or survivorship implementation of its own.
"""

from __future__ import annotations

import json
import hashlib
import logging
import re
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from src.database import (
    PipelineError,
    RunInputMismatchError,
    create_pipeline_run,
    get_pipeline_run_summary,
    initialize_database,
    save_duplicate_decisions,
    save_master_records,
    save_pipeline_errors,
    save_source_records,
    update_pipeline_run_status,
)
from src.duplicate_detection import DetectionResult, detect_exact_duplicates
from src.ingestion import IngestedRecord, load_records
from src.normalization import NormalizedRecord, normalize_records
from src.probable_matching import ProbableMatchingResult, detect_probable_duplicates
from src.quarantine import (
    QuarantineRecord,
    create_quarantine_entries,
    persist_quarantine_entries,
    retrieve_quarantine_statistics,
)
from src.review_queue import ReviewItem, generate_review_items, retrieve_review_statistics, save_review_queue
from src.survivorship import MasterRecord, UnresolvedSurvivorshipConflict, create_master_records
from src.validation import RejectedRecord, ValidationResult, validate_records
from src.structured_logging import LOGGER_NAME


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CUSTOMER_INPUT = PROJECT_ROOT / "data" / "input" / "customers.csv"
DEFAULT_PRODUCT_INPUT = PROJECT_ROOT / "data" / "input" / "products.csv"
DEFAULT_REPORT_DIRECTORY = PROJECT_ROOT / "reports"
LOGGER = logging.getLogger(LOGGER_NAME)
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

STAGE_ORDER = (
    "initialize_database",
    "fingerprint_inputs",
    "create_pipeline_run",
    "ingest",
    "validate",
    "create_quarantine_entries",
    "normalize",
    "detect_exact_duplicates",
    "detect_probable_duplicates",
    "create_master_records",
    "generate_review_items",
    "persist_source_records",
    "persist_quarantine_entries",
    "persist_review_items",
    "persist_duplicate_decisions",
    "persist_master_records_and_lineage",
    "persist_pipeline_errors",
    "update_run_status_and_summary",
    "generate_report",
)


@dataclass
class PipelineResult:
    """Final observable result from one pipeline invocation."""

    run_id: str
    input_fingerprint: str | None
    status: str
    started_at: str
    completed_at: str
    stage_order: list[str]
    stage_durations_seconds: dict[str, float]
    stage_metrics: dict[str, dict[str, Any]]
    throughput_metrics: dict[str, Any]
    execution_summary: dict[str, Any]
    customer_metrics: dict[str, Any]
    product_metrics: dict[str, Any]
    rejected_record_count: int
    data_quality_issue_count: int
    data_quality_issues: list[dict[str, Any]]
    operational_error_count: int
    operational_errors: list[dict[str, Any]]
    unresolved_conflicts: list[dict[str, Any]]
    review_statistics: dict[str, Any]
    quarantine_statistics: dict[str, Any]
    database_persistence_counts: dict[str, int]
    report_path: str
    execution_duration_seconds: float

    @property
    def errors(self) -> list[dict[str, Any]]:
        """Backward-compatible alias for operational errors."""

        return self.operational_errors

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["rejected_record_count"] = (
            self.customer_metrics["records_rejected"] + self.product_metrics["records_rejected"]
        )
        payload["data_quality_issue_count"] = sum(
            len(issue.get("validation_errors", ())) for issue in self.data_quality_issues
        )
        payload["operational_error_count"] = len(self.operational_errors)
        payload["errors"] = list(self.operational_errors)
        return payload


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_run_id() -> str:
    return f"run-{_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _valid_run_id(value: str) -> str:
    if not isinstance(value, str) or not _SAFE_RUN_ID.fullmatch(value) or value in {".", ".."}:
        raise ValueError("run_id must be 1-128 safe letters, digits, dots, underscores, or hyphens; it must start with a letter or digit.")
    return value


def _empty_metrics() -> dict[str, Any]:
    return {
        "records_ingested": 0,
        "records_accepted": 0,
        "records_rejected": 0,
        "exact_duplicate_groups": 0,
        "exact_duplicate_pairs": 0,
        "strong_probable_matches": 0,
        "review_required_pairs": 0,
        "no_match_candidate_pairs": 0,
        "probable_candidate_pairs_evaluated": 0,
        "probable_total_possible_pairs": 0,
        "probable_candidate_reduction_percent": 0.0,
        "master_records_created": 0,
        "unresolved_conflicts": 0,
        "data_quality_issue_count": 0,
        "operational_error_count": 0,
        "errors": 0,
    }


def _rate(numerator: int, seconds: float) -> float:
    return round(numerator / seconds, 6) if seconds > 0 else 0.0


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator > 0 else 0.0


def _conflict_dict(conflict: UnresolvedSurvivorshipConflict) -> dict[str, Any]:
    return {
        "entity_type": conflict.entity_type,
        "source_record_ids": list(conflict.source_record_ids),
        "conflict_code": conflict.conflict_code,
        "explanation": conflict.explanation,
        "recommended_action": conflict.recommended_action,
    }


def _append_error(
    errors: list[dict[str, Any]],
    stage: str,
    error_code: str,
    message: str,
    source_record_id: Any | None = None,
    entity_type: str | None = None,
) -> None:
    errors.append({
        "stage": stage,
        "error_code": error_code,
        "error_message": str(message),
        "source_record_id": None if source_record_id is None else str(source_record_id),
        "entity_type": entity_type,
    })


def _pipeline_error_rows(errors: list[dict[str, Any]]) -> list[PipelineError]:
    return [
        PipelineError(
            stage=item["stage"],
            error_code=item["error_code"],
            error_message=(f"[{item['entity_type']}] " if item.get("entity_type") else "") + item["error_message"],
            source_record_id=item.get("source_record_id"),
        )
        for item in errors
    ]


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    temporary.write_text(serialized + "\n", encoding="utf-8")
    temporary.replace(path)


def _input_fingerprint(
    customers_path: str | Path | None,
    products_path: str | Path | None,
) -> str | None:
    """Hash entity roles, input basenames, and file bytes without loading files."""

    digest = hashlib.sha256(b"mergemind-input-fingerprint-v1\0")
    for entity, path in (("customer", customers_path), ("product", products_path)):
        digest.update(entity.encode("ascii") + b"\0")
        if path is None:
            digest.update(b"NO_INPUT\0")
            continue
        input_path = Path(path)
        filename = input_path.name.encode("utf-8")
        file_digest = hashlib.sha256()
        size = 0
        try:
            with input_path.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    file_digest.update(chunk)
                    size += len(chunk)
        except OSError:
            # Let ingestion produce the normal missing/unreadable-file failure.
            # A previous fingerprinted run will reject this unknown input safely.
            return None
        digest.update(len(filename).to_bytes(8, "big"))
        digest.update(filename)
        digest.update(size.to_bytes(8, "big"))
        digest.update(file_digest.digest())
    return digest.hexdigest()


def _storage_key_override(record: NormalizedRecord | RejectedRecord, source_id: Any) -> str:
    """Make a run-local key for rejected rows with absent/repeated source IDs."""

    identity = json.dumps(
        [record.entity_type, record.source_file, record.source_record_number, source_id],
        ensure_ascii=False, separators=(",", ":"), default=str,
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return f"__rejected__{record.entity_type}__{digest}"


def _persistable_source_records(
    accepted: list[NormalizedRecord], rejected: list[RejectedRecord]
) -> tuple[list[NormalizedRecord | RejectedRecord], dict[tuple[str, str, int], str]]:
    """Retain all rows, surrogating only colliding/missing database keys.

    Original fields are not modified. The original JSON therefore keeps a
    missing or repeated source ID while the table primary key remains unique.
    """

    schema_keys = {"customer": "customer_id", "product": "product_id"}
    rows: list[NormalizedRecord | RejectedRecord] = []
    overrides: dict[tuple[str, str, int], str] = {}
    seen: set[tuple[str, str]] = set()
    source_rows: list[NormalizedRecord | RejectedRecord] = [*accepted, *rejected]
    source_rows.sort(key=lambda record: (
        record.entity_type,
        record.source_record_number,
        0 if isinstance(record, NormalizedRecord) else 1,
    ))
    for record in source_rows:
        source_id = record.source_fields.get(schema_keys[record.entity_type])
        usable_id = source_id if isinstance(source_id, str) and source_id.strip() else None
        key = (record.entity_type, usable_id) if usable_id is not None else None
        if key is None or key in seen:
            overrides[(record.entity_type, record.source_file, record.source_record_number)] = _storage_key_override(record, source_id)
        else:
            seen.add(key)
        rows.append(record)
    return rows, overrides


def run_pipeline(
    *,
    customers_path: str | Path | None = DEFAULT_CUSTOMER_INPUT,
    products_path: str | Path | None = DEFAULT_PRODUCT_INPUT,
    run_id: str | None = None,
    database_path: str | Path | None = None,
    report_directory: str | Path = DEFAULT_REPORT_DIRECTORY,
    replace_existing: bool = False,
) -> PipelineResult:
    """Execute the complete pipeline and always return a success/failure result.

    An omitted entity path disables that entity; the default invocation runs
    both bundled CSV files. Stage failures stop further processing, are
    reported, and result in a FAILED run whenever the database is reachable.
    """

    active_run_id = _valid_run_id(run_id) if run_id is not None else _new_run_id()
    started_at = _now()
    overall_started = time.perf_counter()
    completed_at = started_at
    status = "RUNNING"
    durations: dict[str, float] = {}
    metrics = {"customer": _empty_metrics(), "product": _empty_metrics()}
    data_quality_issues: list[dict[str, Any]] = []
    operational_errors: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    persistence_counts: dict[str, int] = {}
    stage_metrics: dict[str, dict[str, Any]] = {}
    normalized_by_entity: dict[str, list[NormalizedRecord]] = {"customer": [], "product": []}
    validation_by_entity: dict[str, ValidationResult | None] = {"customer": None, "product": None}
    exact_by_entity: dict[str, DetectionResult] = {}
    probable_by_entity: dict[str, ProbableMatchingResult] = {}
    masters_by_entity: dict[str, list[MasterRecord]] = {"customer": [], "product": []}
    survivorship_by_entity = {}
    review_items: list[ReviewItem] = []
    quarantine_entries: list[QuarantineRecord] = []
    review_statistics = {
        "total_review_items": 0, "pending_reviews": 0, "accepted_reviews": 0,
        "rejected_reviews": 0, "deferred_reviews": 0, "customer_review_count": 0,
        "product_review_count": 0,
        "by_entity": {"customer": {"PENDING": 0, "ACCEPTED_MATCH": 0, "REJECTED_MATCH": 0, "DEFERRED": 0},
                      "product": {"PENDING": 0, "ACCEPTED_MATCH": 0, "REJECTED_MATCH": 0, "DEFERRED": 0}},
    }
    quarantine_statistics = {
        "total_quarantined_records": 0, "quarantined_count": 0, "resolved_count": 0,
        "discarded_count": 0, "customer_quarantine_count": 0, "product_quarantine_count": 0,
        "counts_by_validation_error_code": {},
        "by_entity": {"customer": {"QUARANTINED": 0, "RESOLVED": 0, "DISCARDED": 0},
                      "product": {"QUARANTINED": 0, "RESOLVED": 0, "DISCARDED": 0}},
    }
    ingested_by_entity: dict[str, list[IngestedRecord]] = {"customer": [], "product": []}
    current_stage = "initialize_database"
    report_path = Path(report_directory) / f"{active_run_id}_report.json"
    run_created = False
    input_fingerprint: str | None = None

    def stage(
        name: str,
        operation: Callable[[], Any],
        *,
        input_counts: Callable[[], dict[str, int]] | None = None,
        output_counts: Callable[[], dict[str, int]] | None = None,
        rejected_count: Callable[[], int] | None = None,
    ) -> Any:
        nonlocal current_stage
        current_stage = name
        started = time.perf_counter()
        inputs = input_counts() if input_counts is not None else {}
        stage_metrics[name] = {
            "input_counts": inputs, "output_counts": {}, "rejected_count": 0, "status": "RUNNING",
        }
        LOGGER.info("pipeline stage started", extra={
            "event": "stage_started", "run_id": active_run_id, "stage": name,
            "input_counts": inputs, "output_counts": {},
        })
        try:
            output = operation()
        except Exception as exc:
            elapsed = round(time.perf_counter() - started, 6)
            durations[name] = elapsed
            stage_metrics[name].update({"duration_seconds": elapsed, "status": "FAILED"})
            LOGGER.error("pipeline stage failed", extra={
                "event": "stage_failed", "run_id": active_run_id, "stage": name,
                "duration_seconds": elapsed, "error_code": type(exc).__name__.upper(),
                "error_type": type(exc).__name__, "input_counts": inputs,
                "output_counts": {}, "rejected_count": 0,
            })
            raise
        elapsed = round(time.perf_counter() - started, 6)
        outputs = output_counts() if output_counts is not None else {}
        rejects = rejected_count() if rejected_count is not None else 0
        durations[name] = elapsed
        stage_metrics[name].update({
            "output_counts": outputs, "rejected_count": rejects,
            "duration_seconds": elapsed, "status": "COMPLETED",
        })
        LOGGER.info("pipeline stage completed", extra={
            "event": "stage_completed", "run_id": active_run_id, "stage": name,
            "duration_seconds": elapsed, "input_counts": inputs, "output_counts": outputs,
            "rejected_count": rejects,
        })
        return output

    LOGGER.info("pipeline execution started", extra={
        "event": "pipeline_started", "run_id": active_run_id,
    })

    try:
        stage("initialize_database", lambda: initialize_database(database_path),
              output_counts=lambda: {"database_initialized": 1})

        def fingerprint_inputs() -> str | None:
            nonlocal input_fingerprint
            input_fingerprint = _input_fingerprint(customers_path, products_path)
            return input_fingerprint

        input_fingerprint = stage(
            "fingerprint_inputs", fingerprint_inputs,
            input_counts=lambda: {"input_files": int(customers_path is not None) + int(products_path is not None)},
            output_counts=lambda: {"fingerprint_created": int(input_fingerprint is not None)},
        )
        stage("create_pipeline_run", lambda: create_pipeline_run(
            active_run_id, db_path=database_path, started_at=started_at, status="RUNNING",
            input_fingerprint=input_fingerprint,
            replace_existing=replace_existing,
        ), input_counts=lambda: {"run_id": 1}, output_counts=lambda: {"run_created_or_resumed": 1})
        run_created = True

        def ingest_all() -> None:
            for entity, path in (("customer", customers_path), ("product", products_path)):
                if path is None:
                    continue
                records = load_records(Path(path), entity)
                ingested_by_entity[entity] = records
                metrics[entity]["records_ingested"] = len(records)

        stage("ingest", ingest_all,
              input_counts=lambda: {"customer_files": int(customers_path is not None),
                                    "product_files": int(products_path is not None)},
              output_counts=lambda: {"customer_records": metrics["customer"]["records_ingested"],
                                     "product_records": metrics["product"]["records_ingested"]})

        def validate_all() -> None:
            for entity in ("customer", "product"):
                if (entity == "customer" and customers_path is None) or (entity == "product" and products_path is None):
                    continue
                result = validate_records(ingested_by_entity[entity], entity)
                validation_by_entity[entity] = result
                metrics[entity]["records_accepted"] = result.validation_summary.valid_records
                metrics[entity]["records_rejected"] = result.validation_summary.rejected_records
                for rejection in result.rejected_records:
                    issue_details = []
                    for issue in rejection.errors:
                        issue_details.append({
                            "field_name": issue.field_name,
                            "error_code": issue.error_code,
                            "message": issue.message,
                        })
                    data_quality_issues.append({
                        "source_record_id": rejection.source_fields.get(
                            "customer_id" if entity == "customer" else "product_id"
                        ),
                        "entity_type": entity,
                        "source_file": rejection.source_file,
                        "source_record_number": rejection.source_record_number,
                        "validation_errors": issue_details,
                    })
                    metrics[entity]["data_quality_issue_count"] += len(issue_details)

        stage("validate", validate_all,
              input_counts=lambda: {"customer_records": metrics["customer"]["records_ingested"],
                                    "product_records": metrics["product"]["records_ingested"]},
              output_counts=lambda: {"customer_accepted": metrics["customer"]["records_accepted"],
                                     "product_accepted": metrics["product"]["records_accepted"]},
              rejected_count=lambda: sum(item["records_rejected"] for item in metrics.values()))

        def create_quarantines() -> None:
            results = [result for result in validation_by_entity.values() if result is not None]
            quarantine_entries.extend(create_quarantine_entries(active_run_id, results))

        stage("create_quarantine_entries", create_quarantines,
              input_counts=lambda: {"customer_rejected": metrics["customer"]["records_rejected"],
                                    "product_rejected": metrics["product"]["records_rejected"]},
              output_counts=lambda: {"customer_quarantines": sum(item.entity_type == "customer" for item in quarantine_entries),
                                     "product_quarantines": sum(item.entity_type == "product" for item in quarantine_entries)})

        def normalize_all() -> None:
            for entity in ("customer", "product"):
                validation = validation_by_entity[entity]
                if validation is not None:
                    normalized_by_entity[entity] = normalize_records(validation.valid_records, entity)

        stage("normalize", normalize_all,
              input_counts=lambda: {"customer_accepted": metrics["customer"]["records_accepted"],
                                    "product_accepted": metrics["product"]["records_accepted"]},
              output_counts=lambda: {"customer_normalized": len(normalized_by_entity["customer"]),
                                     "product_normalized": len(normalized_by_entity["product"])})

        def exact_all() -> None:
            for entity in ("customer", "product"):
                if validation_by_entity[entity] is None:
                    continue
                result = detect_exact_duplicates(normalized_by_entity[entity], entity)
                exact_by_entity[entity] = result
                metrics[entity]["exact_duplicate_groups"] = len(result.duplicate_groups)
                metrics[entity]["exact_duplicate_pairs"] = len(result.duplicate_pairs)

        stage("detect_exact_duplicates", exact_all,
              input_counts=lambda: {"customer_records": len(normalized_by_entity["customer"]),
                                    "product_records": len(normalized_by_entity["product"])},
              output_counts=lambda: {"customer_groups": metrics["customer"]["exact_duplicate_groups"],
                                     "customer_pairs": metrics["customer"]["exact_duplicate_pairs"],
                                     "product_groups": metrics["product"]["exact_duplicate_groups"],
                                     "product_pairs": metrics["product"]["exact_duplicate_pairs"]})

        def probable_all() -> None:
            for entity in ("customer", "product"):
                if validation_by_entity[entity] is None:
                    continue
                result = detect_probable_duplicates(
                    normalized_by_entity[entity], entity, exact_result=exact_by_entity[entity]
                )
                probable_by_entity[entity] = result
                metrics[entity]["strong_probable_matches"] = len(result.strong_probable_matches)
                metrics[entity]["review_required_pairs"] = len(result.review_candidates)
                metrics[entity]["no_match_candidate_pairs"] = len(result.non_matches)
                metrics[entity]["probable_candidate_pairs_evaluated"] = result.matching_summary.candidate_pairs_evaluated
                metrics[entity]["probable_total_possible_pairs"] = result.matching_summary.total_possible_pairs
                metrics[entity]["probable_candidate_reduction_percent"] = result.matching_summary.candidate_reduction_percent

        stage("detect_probable_duplicates", probable_all,
              input_counts=lambda: {"customer_records": len(normalized_by_entity["customer"]),
                                    "product_records": len(normalized_by_entity["product"])},
              output_counts=lambda: {
                  "customer_strong": metrics["customer"]["strong_probable_matches"],
                  "customer_review_required": metrics["customer"]["review_required_pairs"],
                  "customer_no_match": metrics["customer"]["no_match_candidate_pairs"],
                  "product_strong": metrics["product"]["strong_probable_matches"],
                  "product_review_required": metrics["product"]["review_required_pairs"],
                  "product_no_match": metrics["product"]["no_match_candidate_pairs"],
                  "customer_candidate_pairs_evaluated": metrics["customer"]["probable_candidate_pairs_evaluated"],
                  "product_candidate_pairs_evaluated": metrics["product"]["probable_candidate_pairs_evaluated"],
              })

        def create_masters() -> None:
            for entity in ("customer", "product"):
                if validation_by_entity[entity] is None:
                    continue
                result = create_master_records(
                    normalized_by_entity[entity], entity,
                    exact_result=exact_by_entity[entity], probable_result=probable_by_entity[entity],
                )
                survivorship_by_entity[entity] = result
                masters_by_entity[entity] = result.master_records
                metrics[entity]["master_records_created"] = len(result.master_records)
                metrics[entity]["unresolved_conflicts"] = len(result.unresolved_conflicts)
                unresolved.extend(_conflict_dict(item) for item in result.unresolved_conflicts)

        stage("create_master_records", create_masters,
              input_counts=lambda: {"customer_records": len(normalized_by_entity["customer"]),
                                    "product_records": len(normalized_by_entity["product"])},
              output_counts=lambda: {"customer_masters": metrics["customer"]["master_records_created"],
                                     "product_masters": metrics["product"]["master_records_created"]})

        def generate_reviews() -> None:
            for entity in ("customer", "product"):
                if entity not in exact_by_entity:
                    continue
                review_items.extend(generate_review_items(
                    active_run_id,
                    exact_result=exact_by_entity[entity],
                    probable_result=probable_by_entity[entity],
                    survivorship_result=survivorship_by_entity[entity],
                    normalized_records=normalized_by_entity[entity],
                ))

        stage("generate_review_items", generate_reviews,
              input_counts=lambda: {"candidate_pairs": sum(
                  len(result.probable_pairs) for result in probable_by_entity.values()
              ) + sum(len(result.conflict_candidates) for result in exact_by_entity.values())},
              output_counts=lambda: {"customer_reviews": sum(item.entity_type == "customer" for item in review_items),
                                     "product_reviews": sum(item.entity_type == "product" for item in review_items)})

        def persist_sources() -> None:
            rows: list[NormalizedRecord | RejectedRecord] = []
            source_id_overrides: dict[tuple[str, str, int], str] = {}
            for entity in ("customer", "product"):
                validation = validation_by_entity[entity]
                if validation is not None:
                    entity_rows, entity_overrides = _persistable_source_records(
                        normalized_by_entity[entity], validation.rejected_records
                    )
                    rows.extend(entity_rows)
                    source_id_overrides.update(entity_overrides)
            persistence_counts["source_records_submitted"] = save_source_records(
                active_run_id, rows, db_path=database_path,
                source_record_id_overrides=source_id_overrides,
            )

        stage("persist_source_records", persist_sources,
              input_counts=lambda: {"records": sum(
                  len(validation.valid_records) + len(validation.rejected_records)
                  for validation in validation_by_entity.values() if validation is not None
              )},
              output_counts=lambda: {"records_submitted": persistence_counts.get("source_records_submitted", 0)})

        def persist_quarantines() -> None:
            persistence_counts["quarantine_records_submitted"] = persist_quarantine_entries(
                active_run_id, quarantine_entries, db_path=database_path
            )
            quarantine_statistics.update(retrieve_quarantine_statistics(active_run_id, db_path=database_path))

        stage("persist_quarantine_entries", persist_quarantines,
              input_counts=lambda: {"quarantine_records": len(quarantine_entries)},
              output_counts=lambda: {"records_submitted": persistence_counts.get("quarantine_records_submitted", 0)})

        def persist_reviews() -> None:
            persistence_counts["review_items_submitted"] = save_review_queue(
                active_run_id, review_items, db_path=database_path
            )
            review_statistics.update(retrieve_review_statistics(active_run_id, db_path=database_path))

        stage("persist_review_items", persist_reviews,
              input_counts=lambda: {"review_items": len(review_items)},
              output_counts=lambda: {"records_submitted": persistence_counts.get("review_items_submitted", 0)})

        def persist_decisions() -> None:
            persistence_counts["duplicate_decisions_submitted"] = 0
            for entity in ("customer", "product"):
                if entity in exact_by_entity:
                    persistence_counts["duplicate_decisions_submitted"] += save_duplicate_decisions(
                        active_run_id, db_path=database_path,
                        exact_result=exact_by_entity[entity], probable_result=probable_by_entity[entity],
                    )

        stage("persist_duplicate_decisions", persist_decisions,
              input_counts=lambda: {"decisions": sum(
                  len(result.duplicate_pairs) + len(result.conflict_candidates)
                  for result in exact_by_entity.values()
              ) + sum(len(result.probable_pairs) for result in probable_by_entity.values())},
              output_counts=lambda: {"records_submitted": persistence_counts.get("duplicate_decisions_submitted", 0)})

        def persist_masters() -> None:
            persistence_counts["master_records_submitted"] = save_master_records(
                active_run_id, masters_by_entity["customer"] + masters_by_entity["product"],
                db_path=database_path,
            )

        stage("persist_master_records_and_lineage", persist_masters,
              input_counts=lambda: {"master_records": sum(len(items) for items in masters_by_entity.values())},
              output_counts=lambda: {"records_submitted": persistence_counts.get("master_records_submitted", 0)})

        # Record validation findings and keyless/duplicate source persistence
        # issues after artifact writes. The database upserts make this safe on
        # a repeated run with the same ID.
        def persist_errors() -> None:
            persistence_counts["pipeline_errors_submitted"] = save_pipeline_errors(
                active_run_id, _pipeline_error_rows(operational_errors), db_path=database_path
            )

        stage("persist_pipeline_errors", persist_errors,
              input_counts=lambda: {"operational_errors": len(operational_errors)},
              output_counts=lambda: {"records_submitted": persistence_counts.get("pipeline_errors_submitted", 0)})

        status = "COMPLETED"
    except RunInputMismatchError:
        # This is a rejected resume request, not a failed attempt to execute
        # the existing run. Preserve its status, artifacts, and report.
        LOGGER.warning("pipeline resume rejected", extra={
            "event": "run_input_mismatch", "run_id": active_run_id,
            "stage": current_stage, "error_code": "RUN_INPUT_MISMATCH",
        })
        raise
    except Exception as exc:
        status = "FAILED"
        _append_error(operational_errors, current_stage, type(exc).__name__.upper(), str(exc))
        LOGGER.error("pipeline execution failed", extra={
            "event": "pipeline_failed", "run_id": active_run_id,
            "stage": current_stage, "error_code": type(exc).__name__.upper(),
            "error_type": type(exc).__name__,
        })

    completed_at = _now()
    execution_duration = round(time.perf_counter() - overall_started, 6)

    # Update run summary and status on both success and failure. Errors here
    # are captured without falsely converting a failed persistence attempt to
    # success; a second best-effort FAILED update follows when needed.
    if run_created:
        try:
            stage("update_run_status_and_summary", lambda: update_pipeline_run_status(
                    active_run_id, status, db_path=database_path, completed_at=completed_at,
                    customer_input_count=metrics["customer"]["records_ingested"],
                    product_input_count=metrics["product"]["records_ingested"],
                    accepted_count=sum(value["records_accepted"] for value in metrics.values()),
                    rejected_count=sum(value["records_rejected"] for value in metrics.values()),
                    master_record_count=sum(value["master_records_created"] for value in metrics.values()),
                    error_count=len(operational_errors),
                ), input_counts=lambda: {"operational_errors": len(operational_errors)},
                output_counts=lambda: {"run_summary_updated": 1})
        except Exception as exc:
            status = "FAILED"
            _append_error(operational_errors, "update_run_status_and_summary", type(exc).__name__.upper(), str(exc))
            try:
                update_pipeline_run_status(active_run_id, "FAILED", db_path=database_path, completed_at=_now(),
                                           error_count=len(operational_errors))
            except Exception as status_exc:
                _append_error(operational_errors, "mark_run_failed", type(status_exc).__name__.upper(), str(status_exc))

    try:
        persistence_counts.update(_persisted_counts(active_run_id, database_path))
    except Exception as exc:
        status = "FAILED"
        _append_error(operational_errors, "read_persistence_summary", type(exc).__name__.upper(), str(exc))
        if run_created:
            try:
                update_pipeline_run_status(active_run_id, "FAILED", db_path=database_path,
                                           completed_at=_now(), error_count=len(operational_errors))
            except Exception as status_exc:
                _append_error(operational_errors, "mark_run_failed", type(status_exc).__name__.upper(), str(status_exc))

    for entity in ("customer", "product"):
        scoped_operational = sum(item.get("entity_type") == entity for item in operational_errors)
        metrics[entity]["operational_error_count"] = scoped_operational
        metrics[entity]["errors"] = scoped_operational
    if run_created:
        try:
            review_statistics.update(retrieve_review_statistics(active_run_id, db_path=database_path))
            quarantine_statistics.update(retrieve_quarantine_statistics(active_run_id, db_path=database_path))
        except Exception as exc:
            status = "FAILED"
            _append_error(operational_errors, "read_review_statistics", type(exc).__name__.upper(), str(exc))
            LOGGER.error("pipeline statistics retrieval failed", extra={
                "event": "stage_failed", "run_id": active_run_id,
                "stage": "read_review_statistics", "error_code": type(exc).__name__.upper(),
                "error_type": type(exc).__name__,
            })
    # Recompute after failure recovery/report preparation so persisted totals
    # reflect the final diagnostics where database access remains available.
    if run_created:
        try:
            save_pipeline_errors(active_run_id, _pipeline_error_rows(operational_errors), db_path=database_path)
            update_pipeline_run_status(active_run_id, status, db_path=database_path,
                                       completed_at=completed_at, error_count=len(operational_errors))
            persistence_counts.update(_persisted_counts(active_run_id, database_path))
        except Exception as exc:
            status = "FAILED"
            _append_error(operational_errors, "persist_final_status_or_errors", type(exc).__name__.upper(), str(exc))
            try:
                update_pipeline_run_status(active_run_id, "FAILED", db_path=database_path,
                                           completed_at=_now(), error_count=len(operational_errors))
            except Exception as status_exc:
                _append_error(operational_errors, "mark_run_failed", type(status_exc).__name__.upper(), str(status_exc))

    initial_duration = round(time.perf_counter() - overall_started, 6)
    total_ingested = sum(value["records_ingested"] for value in metrics.values())
    throughput_metrics = {
        "records_processed": total_ingested,
        "records_per_second": _rate(total_ingested, initial_duration),
        "by_entity": {
            entity: {
                "records_processed": values["records_ingested"],
                "records_per_second": _rate(values["records_ingested"], initial_duration),
                "validation_acceptance_rate": _ratio(values["records_accepted"], values["records_ingested"]),
                "validation_rejection_rate": _ratio(values["records_rejected"], values["records_ingested"]),
            }
            for entity, values in metrics.items()
        },
    }
    execution_summary = {
        "status": status,
        "total_duration_seconds": initial_duration,
        "records_processed": total_ingested,
        "rejected_records": sum(values["records_rejected"] for values in metrics.values()),
        "data_quality_issue_count": sum(len(issue["validation_errors"]) for issue in data_quality_issues),
        "operational_error_count": len(operational_errors),
        "master_record_count": sum(values["master_records_created"] for values in metrics.values()),
    }
    result = PipelineResult(
        run_id=active_run_id,
        input_fingerprint=input_fingerprint,
        status=status,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        stage_order=list(durations),
        stage_durations_seconds=durations,
        stage_metrics=stage_metrics,
        throughput_metrics=throughput_metrics,
        execution_summary=execution_summary,
        customer_metrics=metrics["customer"],
        product_metrics=metrics["product"],
        rejected_record_count=(metrics["customer"]["records_rejected"] + metrics["product"]["records_rejected"]),
        data_quality_issue_count=sum(len(issue["validation_errors"]) for issue in data_quality_issues),
        data_quality_issues=data_quality_issues,
        operational_error_count=len(operational_errors),
        operational_errors=operational_errors,
        unresolved_conflicts=unresolved,
        review_statistics=review_statistics,
        quarantine_statistics=quarantine_statistics,
        database_persistence_counts=persistence_counts,
        report_path=str(report_path),
        execution_duration_seconds=round(time.perf_counter() - overall_started, 6),
    )
    try:
        report_inputs = {"records": total_ingested}
        stage_metrics["generate_report"] = {
            "input_counts": report_inputs, "output_counts": {},
            "rejected_count": 0, "status": "RUNNING",
        }
        LOGGER.info("pipeline stage started", extra={
            "event": "stage_started", "run_id": active_run_id,
            "stage": "generate_report", "input_counts": report_inputs,
            "output_counts": {},
        })
        report_started = time.perf_counter()
        result.stage_durations_seconds.setdefault("generate_report", 0.0)
        _write_report(report_path, result.to_dict())
        result.stage_durations_seconds["generate_report"] = round(time.perf_counter() - report_started, 6)
        stage_metrics["generate_report"].update({
            "duration_seconds": result.stage_durations_seconds["generate_report"],
            "output_counts": {"reports_written": 1}, "status": "COMPLETED",
        })
        result.stage_order = list(result.stage_durations_seconds)
        result.execution_duration_seconds = round(time.perf_counter() - overall_started, 6)
        result.throughput_metrics["records_per_second"] = _rate(
            total_ingested, result.execution_duration_seconds
        )
        for entity, values in result.throughput_metrics["by_entity"].items():
            values["records_per_second"] = _rate(
                values["records_processed"], result.execution_duration_seconds
            )
        result.execution_summary.update({
            "status": result.status,
            "total_duration_seconds": result.execution_duration_seconds,
            "operational_error_count": len(result.operational_errors),
        })
        _write_report(report_path, result.to_dict())
        LOGGER.info("pipeline stage completed", extra={
            "event": "stage_completed", "run_id": active_run_id,
            "stage": "generate_report", "duration_seconds": result.stage_durations_seconds["generate_report"],
            "input_counts": report_inputs, "output_counts": {"reports_written": 1},
            "rejected_count": result.rejected_record_count,
        })
        completion_level = LOGGER.info if result.status == "COMPLETED" else LOGGER.error
        completion_level("pipeline execution finished", extra={
            "event": "pipeline_completed" if result.status == "COMPLETED" else "pipeline_failed",
            "run_id": active_run_id,
            "status": result.status, "duration_seconds": result.execution_duration_seconds,
            "metrics": {
                "records_processed": total_ingested,
                "masters": execution_summary["master_record_count"],
                "operational_errors": len(result.operational_errors),
            },
        })
    except Exception as exc:
        result.status = "FAILED"
        _append_error(result.errors, "generate_report", type(exc).__name__.upper(), str(exc))
        stage_elapsed = round(time.perf_counter() - report_started, 6)
        result.stage_durations_seconds["generate_report"] = stage_elapsed
        result.stage_order = list(result.stage_durations_seconds)
        result.execution_duration_seconds = round(time.perf_counter() - overall_started, 6)
        result.execution_summary.update({
            "status": "FAILED",
            "total_duration_seconds": result.execution_duration_seconds,
            "operational_error_count": len(result.operational_errors),
        })
        result.throughput_metrics["records_per_second"] = _rate(
            total_ingested, result.execution_duration_seconds
        )
        for entity, values in result.throughput_metrics["by_entity"].items():
            values["records_per_second"] = _rate(
                values["records_processed"], result.execution_duration_seconds
            )
        result.stage_metrics["generate_report"].update({
            "duration_seconds": stage_elapsed, "status": "FAILED",
        })
        LOGGER.error("pipeline stage failed", extra={
            "event": "stage_failed", "run_id": active_run_id,
            "stage": "generate_report", "duration_seconds": stage_elapsed,
            "error_code": type(exc).__name__.upper(), "error_type": type(exc).__name__,
            "input_counts": report_inputs,
        })
        if run_created:
            try:
                save_pipeline_errors(active_run_id, _pipeline_error_rows(result.errors), db_path=database_path)
                update_pipeline_run_status(active_run_id, "FAILED", db_path=database_path,
                                           completed_at=_now(), error_count=len(result.errors))
                result.database_persistence_counts.update(_persisted_counts(active_run_id, database_path))
            except Exception as status_exc:
                _append_error(result.errors, "mark_run_failed", type(status_exc).__name__.upper(), str(status_exc))
        try:
            _write_report(report_path, result.to_dict())
        except Exception:
            pass
        LOGGER.error("pipeline execution failed", extra={
            "event": "pipeline_failed", "run_id": active_run_id,
            "stage": "generate_report", "status": "FAILED",
            "error_code": type(exc).__name__.upper(), "error_type": type(exc).__name__,
        })
    result.operational_error_count = len(result.operational_errors)
    result.rejected_record_count = result.customer_metrics["records_rejected"] + result.product_metrics["records_rejected"]
    result.data_quality_issue_count = sum(len(issue["validation_errors"]) for issue in result.data_quality_issues)
    return result


def _persisted_counts(run_id: str, database_path: str | Path | None) -> dict[str, int]:
    summary = get_pipeline_run_summary(run_id, db_path=database_path)
    if summary is None:
        raise RuntimeError(f"Pipeline run {run_id!r} was not found after persistence.")
    keys = (
        "source_record_rows", "duplicate_pair_rows", "master_record_rows",
        "field_lineage_rows", "pipeline_error_rows", "review_item_rows", "review_history_rows",
        "quarantine_record_rows", "quarantine_history_rows",
    )
    return {"pipeline_run_rows": 1, **{key: int(summary[key]) for key in keys}}


__all__ = ("PipelineResult", "STAGE_ORDER", "run_pipeline")
