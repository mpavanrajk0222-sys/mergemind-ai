from __future__ import annotations

import copy
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from src.database import create_pipeline_run, get_pipeline_run_summary, initialize_database
from src.ingestion import IngestedRecord
from src.quarantine import (
    create_quarantine_entries,
    discard_quarantine_record,
    persist_quarantine_entries,
    resolve_quarantine_record,
    retrieve_quarantine_history,
    retrieve_quarantine_record,
    retrieve_quarantine_records,
    retrieve_quarantine_statistics,
)
from src.validation import validate_records


INGESTED_AT = datetime(2025, 2, 1, tzinfo=timezone.utc)


def customer(record_id="C1", **updates):
    fields = {"customer_id": record_id, "name": "Alex Doe", "email": "alex@example.test",
              "phone": "+14155550101", "city": "Boston", "address": "1 Main St",
              "updated_at": "2025-01-01T00:00:00Z", "source_system": "crm"}
    fields.update(updates)
    return IngestedRecord(fields, "customer", "customers.jsonl", 1, "jsonl", INGESTED_AT)


def product(record_id="P1", **updates):
    fields = {"product_id": record_id, "product_name": "Widget", "sku": "W-1", "brand": "Acme",
              "category": "Tools", "price": "10.25", "updated_at": "2025-01-01T00:00:00Z",
              "source_system": "catalog"}
    fields.update(updates)
    return IngestedRecord(fields, "product", "products.csv", 1, "csv", INGESTED_AT)


def rejected(entity_record):
    entity = entity_record.entity_type
    return validate_records([entity_record], entity)


def setup_db(tmp_path, run_ids=("run-a",)):
    path = tmp_path / "nested" / "quarantine.db"
    initialize_database(path)
    for run_id in run_ids:
        create_pipeline_run(run_id, db_path=path)
    return path


@pytest.mark.parametrize(("record", "code"), [
    (customer(name=None), "MISSING_REQUIRED_FIELD"),
    (customer(name=""), "EMPTY_REQUIRED_FIELD"),
    (customer(name="   "), "WHITESPACE_REQUIRED_FIELD"),
    (customer(name=42), "INVALID_TYPE"),
    (customer(updated_at="not-a-date"), "INVALID_TIMESTAMP"),
    (customer(email="broken-email"), "INVALID_EMAIL"),
    (customer(phone="12"), "INVALID_PHONE"),
    (product(price="not-numeric"), "INVALID_DECIMAL"),
    (product(price="-1.00"), "NEGATIVE_PRICE"),
])
def test_existing_validation_error_categories_create_quarantine_entries(record, code):
    result = rejected(record)
    entries = create_quarantine_entries("run-a", [result], quarantined_at=INGESTED_AT)
    assert len(entries) == 1
    assert entries[0].quarantine_status == "QUARANTINED"
    assert code in {issue["error_code"] for issue in entries[0].validation_issues}


def test_product_invalid_type_and_customer_product_entries_supported():
    customer_result = rejected(customer(name=17))
    product_result = rejected(product(product_name=17))
    items = create_quarantine_entries("both", [customer_result, product_result])
    assert {item.entity_type for item in items} == {"customer", "product"}
    assert len(items) == 2


def test_unexpected_field_rejection_is_quarantined_when_strict_validation_is_requested():
    record = customer()
    record.source_fields["unexpected_note"] = "kept verbatim"
    result = validate_records([record], "customer", strict_unexpected_fields=True)
    items = create_quarantine_entries("run-a", [result])
    assert len(items) == 1
    assert items[0].original_record["unexpected_note"] == "kept verbatim"
    assert any(issue["error_code"] == "UNEXPECTED_FIELD" for issue in items[0].validation_issues)


def test_multiple_validation_issues_are_kept_in_one_quarantine_record():
    result = rejected(customer(name=None, email="bad", phone="3", updated_at="bad-time"))
    entries = create_quarantine_entries("run-a", [result])
    codes = {issue["error_code"] for issue in entries[0].validation_issues}
    assert {"MISSING_REQUIRED_FIELD", "INVALID_EMAIL", "INVALID_PHONE", "INVALID_TIMESTAMP"} <= codes
    assert len(entries[0].validation_issues) == len(result.rejected_records[0].errors)


def test_original_source_fields_are_preserved_and_quarantine_id_is_deterministic():
    source = customer(name=None, email=" raw@example.test ")
    before = copy.deepcopy(source.source_fields)
    result = rejected(source)
    first = create_quarantine_entries("run-a", [result], quarantined_at=INGESTED_AT)[0]
    second = create_quarantine_entries("run-a", [result], quarantined_at=INGESTED_AT)[0]
    assert first.original_record == before
    assert source.source_fields == before
    assert first.source_record_id == "C1"
    assert first.quarantine_id == second.quarantine_id


def test_missing_and_duplicate_source_ids_are_distinct_quarantine_rows():
    missing = replace(customer(None, name=None), source_record_number=1)
    repeated_first = replace(customer("DUP"), source_record_number=2)
    repeated_second = replace(customer("DUP", name=None), source_record_number=3)
    result = validate_records([missing, repeated_first, repeated_second], "customer")
    entries = create_quarantine_entries("run-a", [result])
    assert len(entries) == 2
    missing_item = next(item for item in entries if item.source_record_id is None)
    duplicate_item = next(item for item in entries if item.source_record_id == "DUP")
    assert any(issue["error_code"] == "DUPLICATE_SOURCE_ID" for issue in duplicate_item.validation_issues)
    assert missing_item.quarantine_id != duplicate_item.quarantine_id


def test_create_from_validation_results_ignores_valid_records():
    result = validate_records([customer("C1")], "customer")
    assert result.valid_records
    assert create_quarantine_entries("run-a", [result]) == []


def test_persistence_is_idempotent_filters_and_retrieves_details(tmp_path):
    path = setup_db(tmp_path)
    customer_result = rejected(customer(name=None))
    product_result = rejected(product(price="-4"))
    entries = create_quarantine_entries("run-a", [customer_result, product_result])
    assert persist_quarantine_entries("run-a", entries, db_path=path) == 2
    assert persist_quarantine_entries("run-a", entries, db_path=path) == 0
    assert len(retrieve_quarantine_records("run-a", db_path=path)) == 2
    assert len(retrieve_quarantine_records("run-a", entity_type="product", db_path=path)) == 1
    assert len(retrieve_quarantine_records("run-a", error_code="NEGATIVE_PRICE", db_path=path)) == 1
    item = retrieve_quarantine_record("run-a", entries[0].quarantine_id, db_path=path)
    assert item["original_record"] == entries[0].original_record
    assert item["validation_issues"] == entries[0].validation_issues
    assert len(retrieve_quarantine_history("run-a", entries[0].quarantine_id, db_path=path)) == 1


def test_resolution_and_discard_are_audited_and_terminal(tmp_path):
    path = setup_db(tmp_path)
    results = [rejected(customer(name=None)), rejected(product(price="-1"))]
    items = create_quarantine_entries("run-a", results)
    persist_quarantine_entries("run-a", items, db_path=path)
    resolved = resolve_quarantine_record("run-a", items[0].quarantine_id,
                                         resolution_action="corrected_in_source_system",
                                         notes="source owner confirmed correction", db_path=path)
    discarded = discard_quarantine_record("run-a", items[1].quarantine_id,
                                           notes="obsolete product row", db_path=path)
    assert resolved["quarantine_status"] == "RESOLVED"
    assert resolved["resolved_at"] and resolved["resolution_action"] == "corrected_in_source_system"
    assert discarded["quarantine_status"] == "DISCARDED"
    assert discarded["resolution_action"] == "DISCARD"
    assert [entry["new_status"] for entry in retrieve_quarantine_history("run-a", items[0].quarantine_id, db_path=path)] == ["QUARANTINED", "RESOLVED"]
    with pytest.raises(ValueError, match="Invalid quarantine transition"):
        discard_quarantine_record("run-a", items[0].quarantine_id, db_path=path)


def test_missing_record_and_invalid_quarantine_id_errors(tmp_path):
    path = setup_db(tmp_path)
    with pytest.raises(ValueError, match="quarantine_id"):
        retrieve_quarantine_record("run-a", "bad-id", db_path=path)
    with pytest.raises(KeyError, match="not found"):
        resolve_quarantine_record("run-a", "quarantine-000000000000000000000000",
                                  resolution_action="fixed", db_path=path)


def test_run_isolation_and_quarantine_state_survives_rerun_insert(tmp_path):
    path = setup_db(tmp_path, ("run-a", "run-b"))
    result = rejected(customer(name=None))
    item_a = create_quarantine_entries("run-a", [result])[0]
    item_b = create_quarantine_entries("run-b", [result])[0]
    assert item_a.quarantine_id != item_b.quarantine_id
    persist_quarantine_entries("run-a", [item_a], db_path=path)
    persist_quarantine_entries("run-b", [item_b], db_path=path)
    resolve_quarantine_record("run-a", item_a.quarantine_id,
                              resolution_action="fixed_elsewhere", db_path=path)
    assert persist_quarantine_entries("run-a", [item_a], db_path=path) == 0
    assert retrieve_quarantine_record("run-a", item_a.quarantine_id, db_path=path)["quarantine_status"] == "RESOLVED"
    assert retrieve_quarantine_record("run-b", item_b.quarantine_id, db_path=path)["quarantine_status"] == "QUARANTINED"
    assert get_pipeline_run_summary("run-a", db_path=path)["quarantine_record_rows"] == 1
    assert get_pipeline_run_summary("run-b", db_path=path)["quarantine_record_rows"] == 1


def test_resolution_update_rolls_back_when_audit_insert_fails(tmp_path):
    path = setup_db(tmp_path)
    item = create_quarantine_entries("run-a", [rejected(customer(name=None))])[0]
    persist_quarantine_entries("run-a", [item], db_path=path)
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TRIGGER fail_quarantine_history BEFORE INSERT ON quarantine_history
            WHEN NEW.new_status = 'RESOLVED' BEGIN SELECT RAISE(ABORT, 'forced quarantine history error'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="forced quarantine history error"):
        resolve_quarantine_record("run-a", item.quarantine_id,
                                  resolution_action="fix", db_path=path)
    assert retrieve_quarantine_record("run-a", item.quarantine_id, db_path=path)["quarantine_status"] == "QUARANTINED"
    assert [event["new_status"] for event in retrieve_quarantine_history("run-a", item.quarantine_id, db_path=path)] == ["QUARANTINED"]


def test_empty_quarantine_statistics_are_zero(tmp_path):
    path = setup_db(tmp_path)
    assert retrieve_quarantine_records("run-a", db_path=path) == []
    stats = retrieve_quarantine_statistics("run-a", db_path=path)
    assert stats["total_quarantined_records"] == 0
    assert stats["quarantined_count"] == stats["resolved_count"] == stats["discarded_count"] == 0
    assert stats["counts_by_validation_error_code"] == {}
