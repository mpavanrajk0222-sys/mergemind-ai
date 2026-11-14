from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

import src.pipeline as pipeline
from src.database import RunInputMismatchError, get_master_records, get_pipeline_errors, get_pipeline_run_summary


CUSTOMER_FIELDS = ["customer_id", "name", "email", "phone", "city", "address", "updated_at", "source_system"]
PRODUCT_FIELDS = ["product_id", "product_name", "sku", "brand", "category", "price", "updated_at", "source_system"]


def write_csv(path: Path, headers: list[str], rows: list[dict[str, str | None]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    return path


def customer_rows() -> list[dict[str, str | None]]:
    return [
        {"customer_id": "c1", "name": "Ada Lovelace", "email": "ada@example.com", "phone": "+1 202 555 0101",
         "city": "London", "address": "1 Main Street", "updated_at": "2025-01-01T00:00:00+00:00", "source_system": "crm"},
        {"customer_id": "c2", "name": "Ada Lovelace", "email": "ada@example.com", "phone": "+1 202 555 0101",
         "city": "London", "address": "1 Main Street", "updated_at": "2025-01-02T00:00:00+00:00", "source_system": "billing"},
    ]


def product_rows() -> list[dict[str, str | None]]:
    return [
        {"product_id": "p1", "product_name": "Widget Pro", "sku": "W-100", "brand": "Acme",
         "category": "Hardware", "price": "10.00", "updated_at": "2025-01-01T00:00:00+00:00", "source_system": "catalog"},
        {"product_id": "p2", "product_name": "Widget Pro", "sku": "W-100", "brand": "Acme",
         "category": "Hardware", "price": "11.00", "updated_at": "2025-01-02T00:00:00+00:00", "source_system": "erp"},
    ]


def fixture_files(tmp_path: Path) -> tuple[Path, Path]:
    return (
        write_csv(tmp_path / "input" / "customers.csv", CUSTOMER_FIELDS, customer_rows()),
        write_csv(tmp_path / "input" / "products.csv", PRODUCT_FIELDS, product_rows()),
    )


def run(tmp_path: Path, *, customers=None, products=None, run_id="test-run", **kwargs):
    customer_file, product_file = fixture_files(tmp_path)
    return pipeline.run_pipeline(
        customers_path=customer_file if customers is None else customers,
        products_path=product_file if products is None else products,
        run_id=run_id,
        database_path=tmp_path / "db" / "merge.db",
        report_directory=tmp_path / "reports",
        **kwargs,
    )


def test_successful_complete_pipeline_and_persisted_report_counts(tmp_path):
    result = run(tmp_path)
    assert result.status == "COMPLETED"
    assert result.customer_metrics["records_ingested"] == 2
    assert result.customer_metrics["exact_duplicate_groups"] == 1
    assert result.product_metrics["exact_duplicate_groups"] == 1
    assert result.customer_metrics["master_records_created"] == 1
    assert result.product_metrics["master_records_created"] == 1
    assert result.errors == []
    report = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
    summary = get_pipeline_run_summary("test-run", db_path=tmp_path / "db" / "merge.db")
    assert report["database_persistence_counts"]["source_record_rows"] == summary["source_record_rows"] == 4
    assert report["database_persistence_counts"]["duplicate_pair_rows"] == summary["duplicate_pair_rows"]
    assert report["database_persistence_counts"]["master_record_rows"] == summary["master_record_rows"] == 2
    assert report["database_persistence_counts"]["field_lineage_rows"] == summary["field_lineage_rows"]
    assert report["database_persistence_counts"]["pipeline_run_rows"] == 1
    assert report["status"] == summary["status"] == "COMPLETED"
    assert report["stage_order"][-1] == "generate_report"
    assert set(report["stage_order"]) <= set(report["stage_durations_seconds"])
    assert report["review_statistics"]["total_review_items"] == summary["review_item_rows"] == 0
    assert report["review_statistics"]["pending_reviews"] == 0
    table_counts = {
        "pipeline_run_rows": ("pipeline_runs", "run_id"),
        "source_record_rows": ("source_records", "run_id"),
        "duplicate_pair_rows": ("duplicate_pairs", "run_id"),
        "master_record_rows": ("master_records", "run_id"),
        "field_lineage_rows": ("field_lineage", "run_id"),
        "pipeline_error_rows": ("pipeline_errors", "run_id"),
        "review_item_rows": ("review_items", "run_id"),
        "review_history_rows": ("review_history", "run_id"),
        "quarantine_record_rows": ("quarantine_records", "run_id"),
        "quarantine_history_rows": ("quarantine_history", "run_id"),
    }
    with sqlite3.connect(tmp_path / "db" / "merge.db") as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        for report_key, (table, run_column) in table_counts.items():
            actual = connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {run_column} = ?", ("test-run",)
            ).fetchone()[0]
            assert report["database_persistence_counts"][report_key] == actual


def test_customer_only_and_product_only_execution(tmp_path):
    customer_file, product_file = fixture_files(tmp_path)
    customer_result = pipeline.run_pipeline(
        customers_path=customer_file, products_path=None, run_id="customers-only",
        database_path=tmp_path / "only.db", report_directory=tmp_path / "reports",
    )
    product_result = pipeline.run_pipeline(
        customers_path=None, products_path=product_file, run_id="products-only",
        database_path=tmp_path / "only.db", report_directory=tmp_path / "reports",
    )
    assert customer_result.status == product_result.status == "COMPLETED"
    assert customer_result.customer_metrics["records_ingested"] == 2
    assert customer_result.product_metrics["records_ingested"] == 0
    assert product_result.product_metrics["records_ingested"] == 2
    assert product_result.customer_metrics["records_ingested"] == 0


def test_empty_inputs_complete_with_zero_metrics(tmp_path):
    empty_customer = write_csv(tmp_path / "empty-c.csv", CUSTOMER_FIELDS, [])
    empty_product = write_csv(tmp_path / "empty-p.csv", PRODUCT_FIELDS, [])
    result = pipeline.run_pipeline(customers_path=empty_customer, products_path=empty_product,
                                   run_id="empty", database_path=tmp_path / "empty.db",
                                   report_directory=tmp_path / "reports")
    assert result.status == "COMPLETED"
    assert result.customer_metrics["records_ingested"] == 0
    assert result.product_metrics["master_records_created"] == 0


def test_invalid_business_record_is_rejected_but_pipeline_completes(tmp_path):
    rows = customer_rows()
    rows[0]["name"] = None
    customer_file = write_csv(tmp_path / "invalid-c.csv", CUSTOMER_FIELDS, rows)
    result = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="invalid-row", database_path=tmp_path / "invalid.db",
                                   report_directory=tmp_path / "reports")
    assert result.status == "COMPLETED"
    assert result.customer_metrics["records_rejected"] == 1
    assert result.rejected_record_count == 1
    assert result.data_quality_issue_count == 1
    assert result.operational_error_count == 0
    assert result.operational_errors == result.errors == []
    quality_issue = result.data_quality_issues[0]
    assert quality_issue["source_record_id"] == "c1"
    assert quality_issue["entity_type"] == "customer"
    assert quality_issue["source_file"] == customer_file.name
    assert quality_issue["source_record_number"] == 1
    assert quality_issue["validation_errors"][0]["error_code"] in {"MISSING_REQUIRED_FIELD", "EMPTY_REQUIRED_FIELD"}
    summary = get_pipeline_run_summary("invalid-row", db_path=tmp_path / "invalid.db")
    assert summary["source_record_rows"] == 2
    assert summary["pipeline_error_rows"] == 0
    assert summary["error_count"] == 0
    with sqlite3.connect(tmp_path / "invalid.db") as connection:
        statuses = connection.execute(
            "SELECT validation_status, COUNT(*) FROM source_records WHERE run_id=? GROUP BY validation_status",
            ("invalid-row",),
        ).fetchall()
    assert dict(statuses) == {"ACCEPTED": 1, "REJECTED": 1}
    report = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
    assert report["rejected_record_count"] == 1
    assert report["data_quality_issue_count"] == 1
    assert len(report["data_quality_issues"]) == 1
    assert report["operational_error_count"] == 0
    assert report["operational_errors"] == report["errors"] == []
    assert report["quarantine_statistics"]["total_quarantined_records"] == 1
    assert report["quarantine_statistics"]["quarantined_count"] == 1
    assert report["quarantine_statistics"]["counts_by_validation_error_code"] == {"EMPTY_REQUIRED_FIELD": 1}


def test_pipeline_quarantine_status_survives_rerun_and_valid_rows_are_not_quarantined(tmp_path):
    rows = customer_rows()
    rows[0]["name"] = None
    customer_file = write_csv(tmp_path / "quarantine-input.csv", CUSTOMER_FIELDS, rows)
    database = tmp_path / "quarantine-pipeline.db"
    reports = tmp_path / "reports"
    first = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                  run_id="quarantine-rerun", database_path=database,
                                  report_directory=reports)
    with sqlite3.connect(database) as connection:
        quarantine_id = connection.execute(
            "SELECT quarantine_id FROM quarantine_records WHERE run_id=?", ("quarantine-rerun",)
        ).fetchone()[0]
        valid_quarantine_count = connection.execute(
            "SELECT COUNT(*) FROM quarantine_records WHERE run_id=? AND source_record_id='c2'",
            ("quarantine-rerun",),
        ).fetchone()[0]
    assert valid_quarantine_count == 0
    from src.quarantine import resolve_quarantine_record
    resolve_quarantine_record("quarantine-rerun", quarantine_id,
                              resolution_action="fixed_outside_pipeline", db_path=database)
    second = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="quarantine-rerun", database_path=database,
                                   report_directory=reports)
    assert first.status == second.status == "COMPLETED"
    assert second.quarantine_statistics["total_quarantined_records"] == 1
    assert second.quarantine_statistics["resolved_count"] == 1
    report = json.loads(Path(second.report_path).read_text(encoding="utf-8"))
    assert report["quarantine_statistics"] == second.quarantine_statistics
    with sqlite3.connect(database) as connection:
        status, history_count = connection.execute(
            "SELECT quarantine_status,(SELECT COUNT(*) FROM quarantine_history h "
            "WHERE h.run_id=q.run_id AND h.quarantine_id=q.quarantine_id) "
            "FROM quarantine_records q WHERE run_id=? AND quarantine_id=?",
            ("quarantine-rerun", quarantine_id),
        ).fetchone()
        rejected_status = connection.execute(
            "SELECT validation_status FROM source_records WHERE run_id=? AND source_record_id='c1'",
            ("quarantine-rerun",),
        ).fetchone()[0]
    assert status == "RESOLVED"
    assert history_count == 2
    assert rejected_status == "REJECTED"


def test_missing_or_duplicate_source_ids_are_still_persisted_as_rejected(tmp_path):
    rows = customer_rows()
    rows[0]["customer_id"] = None
    rows[1]["customer_id"] = "c1"
    rows.append(dict(rows[1]))
    customer_file = write_csv(tmp_path / "invalid-ids.csv", CUSTOMER_FIELDS, rows)
    result = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="invalid-ids", database_path=tmp_path / "invalid-ids.db",
                                   report_directory=tmp_path / "reports")
    assert result.status == "COMPLETED"
    with sqlite3.connect(tmp_path / "invalid-ids.db") as connection:
        rows = connection.execute(
            "SELECT source_record_id, validation_status, original_record_json FROM source_records WHERE run_id=? ORDER BY source_record_number",
            ("invalid-ids",),
        ).fetchall()
    assert len(rows) == 3
    assert rows[0][0].startswith("__rejected__customer__")
    assert rows[0][1] == "REJECTED"
    assert json.loads(rows[0][2])["customer_id"] == ""
    assert rows[1][0] == "c1"
    assert rows[1][1] == "ACCEPTED"
    assert rows[2][0].startswith("__rejected__customer__")
    assert rows[2][1] == "REJECTED"
    assert json.loads(rows[2][2])["customer_id"] == "c1"


def test_malformed_and_missing_input_files_fail_without_marking_completed(tmp_path):
    malformed = tmp_path / "malformed.csv"
    malformed.write_text('customer_id,name\nc1,"unterminated\n', encoding="utf-8")
    malformed_result = pipeline.run_pipeline(customers_path=malformed, products_path=None,
                                              run_id="malformed", database_path=tmp_path / "bad.db",
                                              report_directory=tmp_path / "reports")
    missing_result = pipeline.run_pipeline(customers_path=tmp_path / "missing.csv", products_path=None,
                                            run_id="missing", database_path=tmp_path / "bad.db",
                                            report_directory=tmp_path / "reports")
    assert malformed_result.status == missing_result.status == "FAILED"
    assert "MALFORMEDCSVSTRUCTUREERROR" in malformed_result.errors[0]["error_code"]
    assert "FILENOTFOUNDINGESTIONERROR" in missing_result.errors[0]["error_code"]
    assert get_pipeline_run_summary("malformed", db_path=tmp_path / "bad.db")["status"] == "FAILED"
    assert get_pipeline_run_summary("missing", db_path=tmp_path / "bad.db")["status"] == "FAILED"


def test_database_initialization_failure_creates_failed_report(tmp_path):
    invalid_db_path = tmp_path / "is-a-directory.db"
    invalid_db_path.mkdir()
    customer_file, _ = fixture_files(tmp_path)
    result = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="db-failure", database_path=invalid_db_path,
                                   report_directory=tmp_path / "reports")
    assert result.status == "FAILED"
    assert result.errors[0]["stage"] == "initialize_database"
    assert Path(result.report_path).is_file()


def test_database_persistence_failure_marks_run_failed(tmp_path, monkeypatch):
    customer_file, _ = fixture_files(tmp_path)

    def fail_master_write(*args, **kwargs):
        raise sqlite3.OperationalError("simulated disk write failure")

    monkeypatch.setattr(pipeline, "save_master_records", fail_master_write)
    result = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="persist-failure", database_path=tmp_path / "persist.db",
                                   report_directory=tmp_path / "reports")
    summary = get_pipeline_run_summary("persist-failure", db_path=tmp_path / "persist.db")
    assert result.status == "FAILED"
    assert summary["status"] == "FAILED"
    assert summary["source_record_rows"] == 2
    assert summary["master_record_rows"] == 0
    assert summary["pipeline_error_rows"] == summary["error_count"] == 1
    assert result.operational_error_count == 1
    assert result.data_quality_issue_count == 0
    assert any(error["stage"] == "persist_master_records_and_lineage" for error in result.errors)
    report = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
    assert report["operational_error_count"] == 1
    assert report["data_quality_issue_count"] == 0
    assert report["operational_errors"][0]["stage"] == "persist_master_records_and_lineage"


def test_repeat_run_is_idempotent_and_replace_only_affects_selected_run(tmp_path):
    customer_file, product_file = fixture_files(tmp_path)
    database = tmp_path / "reruns.db"
    reports = tmp_path / "reports"
    first = pipeline.run_pipeline(customers_path=customer_file, products_path=product_file,
                                  run_id="rerun-a", database_path=database, report_directory=reports)
    second = pipeline.run_pipeline(customers_path=customer_file, products_path=product_file,
                                   run_id="rerun-a", database_path=database, report_directory=reports)
    other = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                  run_id="rerun-b", database_path=database, report_directory=reports)
    before = get_pipeline_run_summary("rerun-a", db_path=database)
    replaced = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                     run_id="rerun-a", database_path=database,
                                     report_directory=reports, replace_existing=True)
    after = get_pipeline_run_summary("rerun-a", db_path=database)
    other_after = get_pipeline_run_summary("rerun-b", db_path=database)
    assert first.status == second.status == other.status == replaced.status == "COMPLETED"
    assert before["source_record_rows"] == second.database_persistence_counts["source_record_rows"] == 4
    assert after["source_record_rows"] == 2
    assert other_after["source_record_rows"] == 2
    assert after["master_record_rows"] == 1
    assert get_master_records("rerun-a", db_path=database)


def test_input_fingerprint_blocks_changed_content_and_filename_until_replaced(tmp_path):
    customer_file, _ = fixture_files(tmp_path)
    database = tmp_path / "fingerprinted.db"
    reports = tmp_path / "reports"
    first = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                  run_id="fingerprinted", database_path=database,
                                  report_directory=reports)
    report_before = Path(first.report_path).read_bytes()
    fingerprint = first.input_fingerprint
    assert fingerprint and len(fingerprint) == 64

    independent = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                        run_id="fingerprinted-independent", database_path=database,
                                        report_directory=reports)
    assert independent.input_fingerprint == fingerprint

    renamed = tmp_path / "input" / "renamed-customers.csv"
    renamed.write_bytes(customer_file.read_bytes())
    with pytest.raises(RunInputMismatchError, match="different inputs"):
        pipeline.run_pipeline(customers_path=renamed, products_path=None, run_id="fingerprinted",
                              database_path=database, report_directory=reports)
    assert Path(first.report_path).read_bytes() == report_before

    rows = customer_rows()
    rows[0]["name"] = "Ada Changed"
    write_csv(customer_file, CUSTOMER_FIELDS, rows)
    with pytest.raises(RunInputMismatchError, match="different inputs"):
        pipeline.run_pipeline(customers_path=customer_file, products_path=None, run_id="fingerprinted",
                              database_path=database, report_directory=reports)
    assert get_pipeline_run_summary("fingerprinted", db_path=database)["status"] == "COMPLETED"

    replaced = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                     run_id="fingerprinted", database_path=database,
                                     report_directory=reports, replace_existing=True)
    assert replaced.status == "COMPLETED"
    assert replaced.input_fingerprint != fingerprint
    assert get_pipeline_run_summary("fingerprinted-independent", db_path=database)["status"] == "COMPLETED"


def test_reversed_input_order_has_deterministic_master_ids_and_distinct_fingerprint(tmp_path):
    customer_file, _ = fixture_files(tmp_path)
    baseline = pipeline.run_pipeline(customers_path=customer_file, products_path=None, run_id="order-a",
                                     database_path=tmp_path / "order.db", report_directory=tmp_path / "reports")
    baseline_ids = [row["master_id"] for row in get_master_records("order-a", db_path=tmp_path / "order.db")]
    reversed_file = write_csv(tmp_path / "input" / "reversed.csv", CUSTOMER_FIELDS, list(reversed(customer_rows())))
    reversed_run = pipeline.run_pipeline(customers_path=reversed_file, products_path=None, run_id="order-b",
                                         database_path=tmp_path / "order.db", report_directory=tmp_path / "reports")
    reversed_ids = [row["master_id"] for row in get_master_records("order-b", db_path=tmp_path / "order.db")]
    assert baseline.status == reversed_run.status == "COMPLETED"
    assert baseline_ids == reversed_ids
    assert baseline.input_fingerprint != reversed_run.input_fingerprint


def test_validation_rejections_remain_data_quality_issues_when_later_stage_fails(tmp_path, monkeypatch):
    rows = customer_rows()
    rows[0]["name"] = ""
    customer_file = write_csv(tmp_path / "invalid-input.csv", CUSTOMER_FIELDS, rows)

    def fail_normalization(*args, **kwargs):
        raise RuntimeError("injected normalization failure")

    monkeypatch.setattr(pipeline, "normalize_records", fail_normalization)
    result = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="dq-and-failure", database_path=tmp_path / "dq.db",
                                   report_directory=tmp_path / "reports")
    assert result.status == "FAILED"
    assert result.rejected_record_count == 1
    assert result.data_quality_issue_count == 1
    assert result.operational_error_count == 1
    assert result.data_quality_issues[0]["validation_errors"][0]["error_code"] == "EMPTY_REQUIRED_FIELD"


@pytest.mark.parametrize(("stage_name", "function_name"), [
    ("ingest", "load_records"),
    ("validate", "validate_records"),
    ("normalize", "normalize_records"),
    ("detect_exact_duplicates", "detect_exact_duplicates"),
    ("detect_probable_duplicates", "detect_probable_duplicates"),
    ("create_master_records", "create_master_records"),
    ("persist_source_records", "save_source_records"),
    ("persist_quarantine_entries", "persist_quarantine_entries"),
    ("persist_review_items", "save_review_queue"),
    ("persist_master_records_and_lineage", "save_master_records"),
    ("generate_report", "_write_report"),
])
def test_stage_failure_is_recorded_and_same_input_run_recovers(tmp_path, monkeypatch, stage_name, function_name):
    customer_file, product_file = fixture_files(tmp_path)
    database = tmp_path / f"failure-{stage_name}.db"
    reports = tmp_path / "reports"
    original = getattr(pipeline, function_name)
    failed_once = False

    def fail_once(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            monkeypatch.setattr(pipeline, function_name, original)
            raise RuntimeError(f"injected {stage_name} failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(pipeline, function_name, fail_once)
    first = pipeline.run_pipeline(customers_path=customer_file, products_path=product_file,
                                  run_id=f"failed-{stage_name}", database_path=database,
                                  report_directory=reports)
    assert first.status == "FAILED"
    assert any(error["stage"] == stage_name for error in first.operational_errors)
    summary = get_pipeline_run_summary(first.run_id, db_path=database)
    assert summary["status"] == "FAILED"
    assert summary["error_count"] >= 1
    assert any(error["stage"] == stage_name for error in get_pipeline_errors(first.run_id, db_path=database))
    if stage_name == "generate_report":
        failed_report = json.loads(Path(first.report_path).read_text(encoding="utf-8"))
        assert failed_report["status"] == "FAILED"
        assert failed_report["database_persistence_counts"]["pipeline_error_rows"] == summary["pipeline_error_rows"]

    recovered = pipeline.run_pipeline(customers_path=customer_file, products_path=product_file,
                                      run_id=first.run_id, database_path=database,
                                      report_directory=reports)
    assert recovered.status == "COMPLETED"
    assert get_pipeline_run_summary(first.run_id, db_path=database)["status"] == "COMPLETED"
    report = json.loads(Path(recovered.report_path).read_text(encoding="utf-8"))
    assert report["status"] == "COMPLETED"
    assert report["database_persistence_counts"]["master_record_rows"] == get_pipeline_run_summary(
        first.run_id, db_path=database
    )["master_record_rows"]


def test_stage_execution_order_is_reported(tmp_path):
    customer_file, _ = fixture_files(tmp_path)
    result = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="stage-order", database_path=tmp_path / "order.db",
                                   report_directory=tmp_path / "reports")
    assert result.stage_order == list(pipeline.STAGE_ORDER)
    assert result.stage_order.index("ingest") < result.stage_order.index("validate")
    assert result.stage_order.index("validate") < result.stage_order.index("normalize")
    assert result.stage_order.index("normalize") < result.stage_order.index("detect_exact_duplicates")
    assert result.stage_order.index("detect_exact_duplicates") < result.stage_order.index("detect_probable_duplicates")
    assert result.stage_order.index("create_master_records") < result.stage_order.index("persist_source_records")
    assert result.stage_order.index("persist_master_records_and_lineage") < result.stage_order.index("update_run_status_and_summary")


def test_matching_and_master_ids_are_deterministic_across_repeated_runs(tmp_path):
    customer_file, product_file = fixture_files(tmp_path)
    first = pipeline.run_pipeline(customers_path=customer_file, products_path=product_file,
                                  run_id="deterministic", database_path=tmp_path / "det.db",
                                  report_directory=tmp_path / "reports", replace_existing=True)
    first_masters = get_master_records("deterministic", db_path=tmp_path / "det.db")
    second = pipeline.run_pipeline(customers_path=customer_file, products_path=product_file,
                                   run_id="deterministic", database_path=tmp_path / "det.db",
                                   report_directory=tmp_path / "reports", replace_existing=True)
    second_masters = get_master_records("deterministic", db_path=tmp_path / "det.db")
    assert first.customer_metrics == second.customer_metrics
    assert first.product_metrics == second.product_metrics
    assert [master["master_id"] for master in first_masters] == [master["master_id"] for master in second_masters]


def test_review_queue_is_persisted_reported_and_preserved_on_pipeline_rerun(tmp_path):
    rows = customer_rows()
    rows[1]["phone"] = "+1 202 555 0199"
    customer_file = write_csv(tmp_path / "review-candidates.csv", CUSTOMER_FIELDS, rows)
    database = tmp_path / "review-pipeline.db"
    reports = tmp_path / "reports"
    first = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                  run_id="review-rerun", database_path=database,
                                  report_directory=reports)
    assert first.status == "COMPLETED"
    assert first.review_statistics["total_review_items"] >= 1
    assert first.review_statistics["pending_reviews"] >= 1
    assert first.review_statistics["customer_review_count"] == first.review_statistics["total_review_items"]
    report = json.loads(Path(first.report_path).read_text(encoding="utf-8"))
    assert report["review_statistics"] == first.review_statistics
    with sqlite3.connect(database) as connection:
        review_id = connection.execute(
            "SELECT review_id FROM review_items WHERE run_id=? AND review_status='PENDING' LIMIT 1",
            ("review-rerun",),
        ).fetchone()[0]
    from src.review_queue import accept_review
    accept_review("review-rerun", review_id, reviewer_id="tester", notes="confirmed", db_path=database)

    second = pipeline.run_pipeline(customers_path=customer_file, products_path=None,
                                   run_id="review-rerun", database_path=database,
                                   report_directory=reports)
    assert second.status == "COMPLETED"
    assert second.review_statistics["accepted_reviews"] == 1
    assert second.review_statistics["total_review_items"] == first.review_statistics["total_review_items"]
    with sqlite3.connect(database) as connection:
        status, history_count = connection.execute(
            "SELECT review_status,(SELECT COUNT(*) FROM review_history WHERE review_id=r.review_id AND run_id=r.run_id) "
            "FROM review_items r WHERE run_id=? AND review_id=?", ("review-rerun", review_id),
        ).fetchone()
    assert status == "ACCEPTED_MATCH"
    assert history_count == 2


def test_report_path_rejects_traversal_run_ids(tmp_path):
    with pytest.raises(ValueError, match="run_id"):
        pipeline.run_pipeline(customers_path=None, products_path=None, run_id="../escape",
                              database_path=tmp_path / "bad-id.db", report_directory=tmp_path / "reports")


def test_cli_help_is_available(capsys):
    from scripts.run_pipeline import main

    with pytest.raises(SystemExit) as caught:
        main(["--help"])
    assert caught.value.code == 0
    assert "--replace-existing" in capsys.readouterr().out

