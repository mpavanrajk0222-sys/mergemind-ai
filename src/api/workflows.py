from __future__ import annotations

import json
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from src.api.config import database_path, report_directory
from src.database import (
    get_field_lineage,
    get_pipeline_errors,
    get_pipeline_run_summary,
    get_quarantine_history,
    get_quarantine_record,
    get_review_history,
    get_review_item,
    get_source_records,
)


_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class WorkflowDataUnavailable(RuntimeError):
    pass


def _connect_readonly() -> sqlite3.Connection:
    path = database_path()
    if not path.is_file():
        raise WorkflowDataUnavailable("The configured MergeMind database is unavailable.")
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    return connection


def _validate_run_id(run_id: str) -> str:
    if not _SAFE_RUN_ID.fullmatch(run_id) or run_id in {".", ".."}:
        raise ValueError("Invalid run ID.")
    return run_id


def _normalize_confidence(item: dict[str, Any]) -> dict[str, Any]:
    score = item.get("confidence_score")
    if isinstance(score, (int, float)) and score > 1:
        item["confidence_score"] = score / 100
    return item


def _report_path(run_id: str) -> Path:
    safe_id = _validate_run_id(run_id)
    base = report_directory().resolve()
    candidate = (base / f"{safe_id}_report.json").resolve()
    if candidate.parent != base:
        raise ValueError("Invalid report identifier.")
    return candidate


def list_runs(status: str | None, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
    if status is not None and status not in {"RUNNING", "COMPLETED", "FAILED"}:
        raise ValueError("Status must be RUNNING, COMPLETED, or FAILED.")
    clauses: list[str] = []
    values: list[Any] = []
    if status:
        clauses.append("status = ?")
        values.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect_readonly() as connection:
        total = int(connection.execute(f"SELECT COUNT(*) FROM pipeline_runs {where}", values).fetchone()[0])
        rows = connection.execute(
            f"SELECT run_id, started_at, completed_at, status, customer_input_count, product_input_count, "
            f"accepted_count, rejected_count, master_record_count, error_count FROM pipeline_runs {where} "
            "ORDER BY started_at DESC, run_id DESC LIMIT ? OFFSET ?",
            [*values, limit, offset],
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        try:
            item["report_available"] = _report_path(item["run_id"]).is_file()
        except (ValueError, KeyError):
            item["report_available"] = False
        items.append(item)
    return items, total


def get_run(run_id: str) -> dict[str, Any] | None:
    _validate_run_id(run_id)
    summary = get_pipeline_run_summary(run_id, db_path=database_path())
    if summary is None:
        return None
    summary.pop("input_fingerprint", None)
    report = read_report(run_id)
    summary["report_available"] = report is not None
    if report is not None:
        for key in (
            "stage_order", "stage_durations_seconds", "stage_metrics", "throughput_metrics",
            "execution_summary", "customer_metrics", "product_metrics", "data_quality_issues",
            "operational_errors", "unresolved_conflicts", "review_statistics", "quarantine_statistics",
            "database_persistence_counts", "execution_duration_seconds",
        ):
            if key in report:
                summary[key] = report[key]
    return summary


def get_run_errors(run_id: str, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
    _validate_run_id(run_id)
    with _connect_readonly() as connection:
        total = int(connection.execute("SELECT COUNT(*) FROM pipeline_errors WHERE run_id = ?", (run_id,)).fetchone()[0])
    items = get_pipeline_errors(run_id, limit=limit, offset=offset, db_path=database_path())
    for item in items:
        item["error_message"] = _redact_paths(item["error_message"])
    return items, total


def get_duplicate_page(
    run_id: str, entity_type: str | None, decision: str | None, search: str | None, limit: int, offset: int,
) -> tuple[list[dict[str, Any]], int]:
    _validate_run_id(run_id)
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type:
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if decision:
        clauses.append("decision = ?")
        values.append(decision)
    if search:
        clauses.append("(record_id_1 LIKE ? OR record_id_2 LIKE ?)")
        values.extend((f"%{search}%", f"%{search}%"))
    with _connect_readonly() as connection:
        total = int(connection.execute(
            f"SELECT COUNT(*) FROM duplicate_pairs WHERE {' AND '.join(clauses)}", values
        ).fetchone()[0])
        rows = connection.execute(
            f"SELECT * FROM duplicate_pairs WHERE {' AND '.join(clauses)} "
            "ORDER BY entity_type, record_id_1, record_id_2, pair_id LIMIT ? OFFSET ?",
            [*values, limit, offset],
        ).fetchall()
    return [_normalize_confidence({
        **dict(row),
        "matching_evidence": json.loads(row["matching_evidence_json"]),
        "conflicting_evidence": json.loads(row["conflicting_evidence_json"]),
    }) for row in rows], total


def get_pair_sources(run_id: str, entity_type: str, record_ids: list[str]) -> list[dict[str, Any]]:
    _validate_run_id(run_id)
    if not record_ids:
        return []
    placeholders = ",".join("?" for _ in record_ids)
    with _connect_readonly() as connection:
        rows = connection.execute(
            f"SELECT * FROM source_records WHERE run_id = ? AND entity_type = ? "
            f"AND source_record_id IN ({placeholders}) ORDER BY source_record_id",
            [run_id, entity_type, *record_ids],
        ).fetchall()
    return [
        {
            **dict(row),
            "original_record": json.loads(row["original_record_json"]),
            "normalized_record": json.loads(row["normalized_record_json"]) if row["normalized_record_json"] else None,
        }
        for row in rows
    ]


def list_reviews(
    run_id: str, entity_type: str | None, status: str | None, limit: int, offset: int,
) -> tuple[list[dict[str, Any]], int]:
    _validate_run_id(run_id)
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type:
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if status:
        clauses.append("review_status = ?")
        values.append(status)
    where = " AND ".join(clauses)
    with _connect_readonly() as connection:
        total = int(connection.execute(f"SELECT COUNT(*) FROM review_items WHERE {where}", values).fetchone()[0])
        rows = connection.execute(
            f"SELECT * FROM review_items WHERE {where} ORDER BY created_at DESC, review_id LIMIT ? OFFSET ?",
            [*values, limit, offset],
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        item["matching_evidence"] = json.loads(item.pop("matching_evidence_json"))
        item["conflicting_evidence"] = json.loads(item.pop("conflicting_evidence_json"))
        items.append(_normalize_confidence(item))
    return items, total


def get_review_detail(run_id: str, review_id: str) -> dict[str, Any] | None:
    item = get_review_item(run_id, review_id, db_path=database_path())
    if item is not None:
        _normalize_confidence(item)
        item["history"] = get_review_history(run_id, review_id, db_path=database_path())
        item["source_records"] = get_pair_sources(run_id, item["entity_type"], [item["record_id_1"], item["record_id_2"]])
    return item


def list_quarantine(
    run_id: str, entity_type: str | None, error_code: str | None, status: str | None,
    limit: int, offset: int,
) -> tuple[list[dict[str, Any]], int]:
    _validate_run_id(run_id)
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type:
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if status:
        clauses.append("quarantine_status = ?")
        values.append(status)
    if error_code:
        clauses.append("EXISTS (SELECT 1 FROM json_each(quarantine_records.validation_issues_json) AS issue "
                       "WHERE json_extract(issue.value, '$.error_code') = ?)")
        values.append(error_code)
    where = " AND ".join(clauses)
    with _connect_readonly() as connection:
        total = int(connection.execute(
            f"SELECT COUNT(*) FROM quarantine_records WHERE {where}", values
        ).fetchone()[0])
        rows = connection.execute(
            f"SELECT * FROM quarantine_records WHERE {where} "
            "ORDER BY quarantined_at DESC, quarantine_id LIMIT ? OFFSET ?",
            [*values, limit, offset],
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        item["original_record"] = json.loads(item.pop("original_record_json"))
        item["validation_issues"] = json.loads(item.pop("validation_issues_json"))
        items.append(item)
    return items, total


def get_quarantine_detail(run_id: str, quarantine_id: str) -> dict[str, Any] | None:
    item = get_quarantine_record(run_id, quarantine_id, db_path=database_path())
    if item is not None:
        item["history"] = get_quarantine_history(run_id, quarantine_id, db_path=database_path())
    return item


def list_masters(
    run_id: str, entity_type: str | None, search: str | None, limit: int, offset: int,
) -> tuple[list[dict[str, Any]], int]:
    _validate_run_id(run_id)
    clauses = ["run_id = ?"]
    values: list[Any] = [run_id]
    if entity_type:
        clauses.append("entity_type = ?")
        values.append(entity_type)
    if search:
        clauses.append("(master_id LIKE ? OR merged_record_json LIKE ?)")
        escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"%{escaped}%"
        values.extend((pattern, pattern))
    where = " AND ".join(clauses)
    with _connect_readonly() as connection:
        total = int(connection.execute(f"SELECT COUNT(*) FROM master_records WHERE {where}", values).fetchone()[0])
        rows = connection.execute(
            f"SELECT * FROM master_records WHERE {where} ORDER BY entity_type, master_id LIMIT ? OFFSET ?",
            [*values, limit, offset],
        ).fetchall()
        masters = []
        for row in rows:
            payload = json.loads(row["merged_record_json"])
            payload["run_id"] = row["run_id"]
            payload["master_id"] = row["master_id"]
            payload["entity_type"] = row["entity_type"]
            payload["merge_method"] = row["merge_method"]
            payload["created_at"] = row["created_at"]
            masters.append(payload)
    for master in masters:
        master["field_lineage"] = get_field_lineage(
            run_id, master["master_id"], db_path=database_path()
        )
    return masters, total


def get_master(run_id: str, master_id: str) -> dict[str, Any] | None:
    _validate_run_id(run_id)
    with _connect_readonly() as connection:
        row = connection.execute(
            "SELECT * FROM master_records WHERE run_id = ? AND master_id = ?",
            (run_id, master_id),
        ).fetchone()
    if row is None:
        return None
    payload = json.loads(row["merged_record_json"])
    payload.update({
        "run_id": row["run_id"], "master_id": row["master_id"],
        "entity_type": row["entity_type"], "merge_method": row["merge_method"],
        "created_at": row["created_at"],
    })
    payload["field_lineage"] = get_field_lineage(run_id, master_id, db_path=database_path())
    return payload


def get_lineage(run_id: str, master_id: str) -> list[dict[str, Any]]:
    _validate_run_id(run_id)
    if get_master(run_id, master_id) is None:
        raise KeyError("Master record was not found.")
    return get_field_lineage(run_id, master_id, db_path=database_path())


def read_report(run_id: str) -> dict[str, Any] | None:
    path = _report_path(run_id)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkflowDataUnavailable("The pipeline report is unavailable or invalid.") from exc
    if not isinstance(payload, dict):
        raise WorkflowDataUnavailable("The pipeline report is unavailable or invalid.")
    payload.pop("report_path", None)
    return _redact_payload(payload)


def list_reports(limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
    with _connect_readonly() as connection:
        run_ids = [row[0] for row in connection.execute(
            "SELECT run_id FROM pipeline_runs ORDER BY started_at DESC, run_id DESC"
        )]
    available = [
        {"run_id": run_id, "report_url": f"/api/v1/reports/{run_id}/download"}
        for run_id in run_ids
        if _report_path(run_id).is_file()
    ]
    return available[offset:offset + limit], len(available)


def _redact_paths(value: str) -> str:
    roots = {
        str(Path(__file__).resolve().parents[2]),
        str(report_directory().resolve()),
        str(database_path().resolve()),
        tempfile.gettempdir(),
    }
    redacted = value
    for root in roots:
        if root:
            redacted = redacted.replace(root, "[local path]").replace(root.replace("\\", "/"), "[local path]")
    return redacted


def _redact_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _redact_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_payload(item) for item in value]
    return _redact_paths(value) if isinstance(value, str) else value


def report_file(run_id: str) -> Path | None:
    path = _report_path(run_id)
    return path if path.is_file() else None
