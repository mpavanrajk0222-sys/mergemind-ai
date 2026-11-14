from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.database import (
    DuplicateDecision,
    PipelineError,
    RunInputMismatchError,
    create_pipeline_run,
    get_duplicate_decisions,
    get_field_lineage,
    get_master_records,
    get_pipeline_errors,
    get_pipeline_run_summary,
    get_source_records,
    initialize_database,
    save_duplicate_decisions,
    save_field_lineage,
    save_master_records,
    save_pipeline_errors,
    save_source_records,
    update_pipeline_run_status,
)
from src.ingestion import IngestedRecord
from src.normalization import normalize_records
from src.survivorship import create_master_records


def ingested(entity: str, source_id: str, *, name: str | None = None, source_system: str = "system-a"):
    if entity == "customer":
        fields = {
            "customer_id": source_id, "name": name or "Test Customer", "email": None, "phone": None,
            "city": None, "address": None, "updated_at": "2025-01-02T00:00:00+00:00",
            "source_system": source_system,
        }
    else:
        fields = {
            "product_id": source_id, "product_name": name or "Test Product", "sku": "SKU-1",
            "brand": "Brand", "category": "Test", "price": "10.25",
            "updated_at": "2025-01-02T00:00:00+00:00", "source_system": source_system,
        }
    return IngestedRecord(fields, entity, f"{entity}s.csv", 1, "csv", datetime(2025, 1, 3, tzinfo=timezone.utc))


def setup_run(path: Path, run_id: str = "run-1") -> Path:
    initialize_database(path)
    create_pipeline_run(run_id, db_path=path)
    return path


def test_initialization_creates_directory_tables_indexes_and_foreign_keys(tmp_path):
    path = tmp_path / "new" / "nested" / "merge.db"
    initialize_database(path)
    with sqlite3.connect(path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"pipeline_runs", "source_records", "duplicate_pairs", "master_records", "field_lineage", "pipeline_errors"} <= tables
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0  # connection-local; enabled by persistence connections
        indexes = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        assert "idx_duplicate_pairs_run_decision" in indexes


def test_foreign_keys_are_enforced_by_database_connections(tmp_path):
    path = setup_run(tmp_path / "foreign.db")
    with pytest.raises(sqlite3.IntegrityError):
        save_source_records("missing-run", [ingested("customer", "c1")], db_path=path)


def test_successful_customer_and_product_source_inserts_with_normalized_json(tmp_path):
    path = setup_run(tmp_path / "records.db")
    customers = [ingested("customer", "c1")]
    products = [ingested("product", "p1")]
    customer_normalized = normalize_records(customers, "customer")
    product_normalized = normalize_records(products, "product")
    assert save_source_records("run-1", customers, db_path=path, normalized_records=customer_normalized) == 1
    assert save_source_records("run-1", products, db_path=path, normalized_records=product_normalized) == 1
    summary = get_pipeline_run_summary("run-1", db_path=path)
    assert summary["source_record_rows"] == 2
    with sqlite3.connect(path) as connection:
        rows = connection.execute("SELECT entity_type, original_record_json, normalized_record_json FROM source_records ORDER BY entity_type").fetchall()
    assert json.loads(rows[0][1])["customer_id"] == "c1"
    assert json.loads(rows[0][2])["normalized_fields"]["name"] == "test customer"
    assert json.loads(rows[1][1])["product_id"] == "p1"


def test_parameterized_storage_keeps_sql_like_input_as_data(tmp_path):
    path = setup_run(tmp_path / "injection.db")
    payload = "Robert'); DROP TABLE pipeline_runs;--"
    save_source_records("run-1", [ingested("customer", "c1", name=payload)], db_path=path)
    summary = get_pipeline_run_summary("run-1", db_path=path)
    assert summary is not None
    with sqlite3.connect(path) as connection:
        stored = json.loads(connection.execute("SELECT original_record_json FROM source_records WHERE source_record_id = ?", ("c1",)).fetchone()[0])
    assert stored["name"] == payload


def test_secret_like_field_is_refused_and_write_is_atomic(tmp_path):
    path = setup_run(tmp_path / "secrets.db")
    good = ingested("customer", "c1")
    bad = ingested("customer", "c2")
    bad.source_fields["api_key"] = "do-not-store"
    with pytest.raises(ValueError, match="secret-like field"):
        save_source_records("run-1", [good, bad], db_path=path)
    assert get_pipeline_run_summary("run-1", db_path=path)["source_record_rows"] == 0


def test_duplicate_decisions_upsert_idempotently_and_rollback_batch_on_fk_failure(tmp_path):
    path = setup_run(tmp_path / "pairs.db")
    records = [ingested("customer", f"c{i}") for i in range(1, 4)]
    save_source_records("run-1", records, db_path=path)
    pair = DuplicateDecision("customer", "c1", "c2", "STRONG_PROBABLE_MATCH", 91.5,
                             {"name": 98.0}, {"phone": "different"})
    assert save_duplicate_decisions("run-1", [pair], db_path=path) == 1
    assert save_duplicate_decisions("run-1", [pair], db_path=path) == 1
    assert get_pipeline_run_summary("run-1", db_path=path)["duplicate_pair_rows"] == 1
    invalid_pair = DuplicateDecision("customer", "c1", "missing", "REVIEW_REQUIRED")
    with pytest.raises(sqlite3.IntegrityError):
        save_duplicate_decisions("run-1", [pair, invalid_pair], db_path=path)
    assert get_pipeline_run_summary("run-1", db_path=path)["duplicate_pair_rows"] == 1


def test_master_and_lineage_round_trip_for_customer_and_product(tmp_path):
    path = setup_run(tmp_path / "masters.db")
    customer_records = normalize_records([ingested("customer", "c1")], "customer")
    product_records = normalize_records([ingested("product", "p1")], "product")
    save_source_records("run-1", [*customer_records, *product_records], db_path=path)
    customer_master = create_master_records(customer_records, "customer").master_records
    product_master = create_master_records(product_records, "product").master_records
    assert save_master_records("run-1", customer_master + product_master, db_path=path) == 2
    retrieved = get_master_records("run-1", db_path=path)
    assert {row["entity_type"] for row in retrieved} == {"customer", "product"}
    assert all(row["field_lineage"] for row in retrieved)
    assert next(row for row in retrieved if row["entity_type"] == "product")["merged_fields"]["price"] == "10.25"


def test_lineage_without_master_reference_fails(tmp_path):
    path = setup_run(tmp_path / "orphan-lineage.db")
    orphan = create_master_records(normalize_records([ingested("customer", "c1")], "customer"), "customer").master_records[0]
    with pytest.raises(sqlite3.IntegrityError):
        save_field_lineage("run-1", [orphan], db_path=path)


def test_master_and_lineage_write_rolls_back_as_one_transaction(tmp_path):
    path = setup_run(tmp_path / "master-lineage-rollback.db")
    records = normalize_records([ingested("customer", "c1")], "customer")
    save_source_records("run-1", records, db_path=path)
    master = create_master_records(records, "customer").master_records
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TRIGGER reject_lineage BEFORE INSERT ON field_lineage
            BEGIN SELECT RAISE(ABORT, 'forced lineage failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="forced lineage failure"):
        save_master_records("run-1", master, db_path=path)
    summary = get_pipeline_run_summary("run-1", db_path=path)
    assert summary["master_record_rows"] == 0
    assert summary["field_lineage_rows"] == 0


def test_run_isolation_and_replacing_only_one_run(tmp_path):
    path = setup_run(tmp_path / "isolation.db", "run-a")
    create_pipeline_run("run-b", db_path=path)
    save_source_records("run-a", [ingested("customer", "same-id")], db_path=path)
    save_source_records("run-b", [ingested("customer", "same-id")], db_path=path)
    create_pipeline_run("run-a", db_path=path, replace_existing=True)
    assert get_pipeline_run_summary("run-a", db_path=path)["source_record_rows"] == 0
    assert get_pipeline_run_summary("run-b", db_path=path)["source_record_rows"] == 1


def test_caller_error_ids_are_scoped_to_each_run(tmp_path):
    path = setup_run(tmp_path / "error-isolation.db", "run-a")
    create_pipeline_run("run-b", db_path=path)
    save_pipeline_errors("run-a", [PipelineError("stage", "E1", "failure", error_id="shared")], db_path=path)
    save_pipeline_errors("run-b", [PipelineError("stage", "E2", "failure", error_id="shared")], db_path=path)
    assert get_pipeline_run_summary("run-a", db_path=path)["pipeline_error_rows"] == 1
    assert get_pipeline_run_summary("run-b", db_path=path)["pipeline_error_rows"] == 1


def test_same_run_execution_resumes_and_upserts_without_duplicate_rows(tmp_path):
    path = setup_run(tmp_path / "idempotency.db")
    records = normalize_records([ingested("customer", "c1")], "customer")
    save_source_records("run-1", records, db_path=path)
    masters = create_master_records(records, "customer").master_records
    save_master_records("run-1", masters, db_path=path)
    save_master_records("run-1", masters, db_path=path)
    error = PipelineError("validation", "BAD_ROW", "Rejected row", "c1")
    save_pipeline_errors("run-1", [error], db_path=path)
    save_pipeline_errors("run-1", [error], db_path=path)
    create_pipeline_run("run-1", db_path=path, accepted_count=1)
    summary = get_pipeline_run_summary("run-1", db_path=path)
    assert summary["source_record_rows"] == 1
    assert summary["master_record_rows"] == 1
    assert summary["field_lineage_rows"] == len(masters[0].field_level_decisions)
    assert summary["pipeline_error_rows"] == 1
    assert summary["accepted_count"] == 1


def test_pipeline_status_update_and_summary_for_empty_run(tmp_path):
    path = setup_run(tmp_path / "status.db")
    assert get_master_records("run-1", db_path=path) == []
    update_pipeline_run_status("run-1", "COMPLETED", db_path=path, accepted_count=0, rejected_count=0)
    summary = get_pipeline_run_summary("run-1", db_path=path)
    assert summary["status"] == "COMPLETED"
    assert summary["completed_at"]
    assert summary["master_record_rows"] == 0


def test_invalid_run_status_update_raises(tmp_path):
    path = setup_run(tmp_path / "bad-run.db")
    with pytest.raises(KeyError):
        update_pipeline_run_status("absent", "FAILED", db_path=path)


def test_run_input_fingerprint_mismatch_requires_explicit_replacement(tmp_path):
    path = setup_run(tmp_path / "fingerprint.db")
    first = "a" * 64
    changed = "b" * 64
    create_pipeline_run("run-1", db_path=path, input_fingerprint=first)
    with pytest.raises(RunInputMismatchError, match="different inputs"):
        create_pipeline_run("run-1", db_path=path, input_fingerprint=changed)
    assert get_pipeline_run_summary("run-1", db_path=path)["input_fingerprint"] == first
    create_pipeline_run("run-1", db_path=path, input_fingerprint=changed, replace_existing=True)
    assert get_pipeline_run_summary("run-1", db_path=path)["input_fingerprint"] == changed


def test_old_pipeline_runs_schema_adds_fingerprint_without_losing_rows(tmp_path):
    path = tmp_path / "older-schema.db"
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TABLE pipeline_runs (
            run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, completed_at TEXT, status TEXT NOT NULL,
            customer_input_count INTEGER NOT NULL DEFAULT 0, product_input_count INTEGER NOT NULL DEFAULT 0,
            accepted_count INTEGER NOT NULL DEFAULT 0, rejected_count INTEGER NOT NULL DEFAULT 0,
            master_record_count INTEGER NOT NULL DEFAULT 0, error_count INTEGER NOT NULL DEFAULT 0
        )""")
        connection.execute(
            "INSERT INTO pipeline_runs(run_id,started_at,status) VALUES(?,?,?)",
            ("legacy", "2025-01-01T00:00:00+00:00", "COMPLETED"),
        )
    initialize_database(path)
    initialize_database(path)
    summary = get_pipeline_run_summary("legacy", db_path=path)
    assert summary["status"] == "COMPLETED"
    assert summary["started_at"] == "2025-01-01T00:00:00+00:00"
    assert summary["input_fingerprint"] is None


def test_retrieval_apis_filter_paginate_and_isolate_runs(tmp_path):
    path = setup_run(tmp_path / "read-apis.db", "run-a")
    create_pipeline_run("run-b", db_path=path)
    customer_inputs = [ingested("customer", "c1"), ingested("customer", "c2")]
    product_inputs = [ingested("product", "p1"), ingested("product", "p2")]
    normalized = normalize_records(customer_inputs, "customer") + normalize_records(product_inputs, "product")
    save_source_records(
        "run-a", normalized, db_path=path,
        validation_statuses={("customer", "c2"): "REJECTED"},
    )
    save_source_records("run-b", [ingested("customer", "c1", name="other run")], db_path=path)

    save_duplicate_decisions("run-a", [
        DuplicateDecision("customer", "c1", "c2", "EXACT", 100.0,
                          {"email": "same"}, {"phone": "different"}),
        DuplicateDecision("product", "p1", "p2", "REVIEW_REQUIRED", 72.0,
                          {"name": 90.0}, {"sku": "different"}),
    ], db_path=path)
    save_pipeline_errors("run-a", [
        PipelineError("ingestion", "BAD_CSV", "bad csv", error_id="error-a"),
        PipelineError("validation", "INTERNAL", "validation failure", error_id="error-b"),
        PipelineError("ingestion", "MISSING", "missing input", error_id="error-c"),
    ], db_path=path)

    masters = create_master_records(normalize_records([customer_inputs[0]], "customer"), "customer").master_records
    save_master_records("run-a", masters, db_path=path)

    source_page = get_source_records("run-a", limit=2, offset=1, db_path=path)
    assert len(source_page) == 2
    assert [row["source_record_id"] for row in source_page] == ["c2", "p1"]
    assert source_page[0]["validation_status"] == "REJECTED"
    assert source_page[0]["original_record"]["customer_id"] == "c2"
    assert get_source_records("run-a", entity_type="product", db_path=path)[0]["source_record_id"] == "p1"
    assert [row["source_record_id"] for row in get_source_records(
        "run-a", validation_status="REJECTED", db_path=path
    )] == ["c2"]
    assert [row["source_record_id"] for row in get_source_records("run-b", db_path=path)] == ["c1"]

    exact = get_duplicate_decisions("run-a", entity_type="customer", decision="EXACT", db_path=path)
    assert len(exact) == 1
    assert exact[0]["matching_evidence"] == {"email": "same"}
    assert exact[0]["conflicting_evidence"] == {"phone": "different"}
    assert get_duplicate_decisions("run-a", limit=1, offset=1, db_path=path)[0]["entity_type"] == "product"
    assert get_duplicate_decisions("run-b", db_path=path) == []

    errors = get_pipeline_errors("run-a", stage="ingestion", limit=1, offset=1, db_path=path)
    assert len(errors) == 1 and errors[0]["error_code"] == "MISSING"
    assert get_pipeline_errors("run-b", db_path=path) == []

    master_id = masters[0].master_id
    lineage = get_field_lineage("run-a", master_id, db_path=path)
    assert lineage
    assert [row["field_name"] for row in get_field_lineage(
        "run-a", master_id, field_name="name", db_path=path
    )] == ["name"]
    assert get_field_lineage("run-b", master_id, db_path=path) == []
    assert all(isinstance(row["rejected_values"], list) for row in lineage)


def test_retrieval_apis_empty_results_and_argument_validation(tmp_path):
    path = setup_run(tmp_path / "empty-read-apis.db")
    assert get_source_records("run-1", db_path=path) == []
    assert get_duplicate_decisions("run-1", db_path=path) == []
    assert get_pipeline_errors("run-1", db_path=path) == []
    assert get_field_lineage("run-1", "master-absent", db_path=path) == []
    for getter in (get_source_records, get_duplicate_decisions, get_pipeline_errors):
        with pytest.raises(ValueError, match="limit"):
            getter("run-1", limit=0, db_path=path)
        with pytest.raises(ValueError, match="offset"):
            getter("run-1", offset=-1, db_path=path)
        with pytest.raises(ValueError, match="limit"):
            getter("run-1", limit=True, db_path=path)
    with pytest.raises(ValueError, match="entity type"):
        get_source_records("run-1", entity_type="supplier", db_path=path)
    with pytest.raises(ValueError, match="entity type"):
        get_duplicate_decisions("run-1", entity_type="supplier", db_path=path)
    with pytest.raises(ValueError, match="master_id"):
        get_field_lineage("run-1", "", db_path=path)


def test_retrieval_filters_are_parameterized_and_foreign_key_consistent(tmp_path):
    path = setup_run(tmp_path / "safe-read-apis.db")
    save_source_records("run-1", [ingested("customer", "c1"), ingested("customer", "c2")], db_path=path)
    save_duplicate_decisions("run-1", [DuplicateDecision("customer", "c1", "c2", "EXACT")], db_path=path)
    save_pipeline_errors("run-1", [PipelineError("stage-a", "E1", "message")], db_path=path)
    attack = "x' OR 1=1 --"
    assert get_source_records("run-1", validation_status=attack, db_path=path) == []
    assert get_duplicate_decisions("run-1", decision=attack, db_path=path) == []
    assert get_pipeline_errors("run-1", stage=attack, db_path=path) == []
    assert get_source_records(attack, db_path=path) == []
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 1
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

