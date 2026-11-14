from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

import pytest

from scripts.run_pipeline import main
from src.pipeline import run_pipeline
from src.structured_logging import LOGGER_NAME, configure_logging


def _customer_file(path: Path, rows: list[dict[str, str]]) -> Path:
    fields = ["customer_id", "name", "email", "phone", "city", "address", "updated_at", "source_system"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _row(customer_id: str) -> dict[str, str]:
    return {
        "customer_id": customer_id, "name": f"Customer {customer_id}", "email": "",
        "phone": "", "city": "", "address": "", "updated_at": "2025-01-01T00:00:00+00:00",
        "source_system": "test",
    }


@pytest.fixture(autouse=True)
def reset_mergemind_logging():
    yield
    configure_logging(log_level="INFO", quiet=True)


def test_logging_level_json_context_console_and_rotating_file(tmp_path, capsys):
    path = tmp_path / "logs" / "pipeline.jsonl"
    logger = configure_logging(log_level="INFO", log_file=path, max_bytes=300, backup_count=2)
    logger.debug("filtered debug")
    for index in range(12):
        logger.info("test event", extra={
            "event": "stage_completed", "run_id": "log-run-1", "stage": "validate",
            "input_counts": {"customer_records": index}, "output_counts": {"accepted": index},
        })
    assert path.exists()
    rotated = sorted(path.parent.glob("pipeline.jsonl*"))
    assert len(rotated) >= 2
    records = [json.loads(line) for file in rotated for line in file.read_text(encoding="utf-8").splitlines()]
    assert all(item["message"] != "filtered debug" for item in records)
    assert any(item.get("run_id") == "log-run-1" and item.get("stage") == "validate" for item in records)
    assert all(item["timestamp"].endswith("Z") for item in records)
    assert "test event" in capsys.readouterr().err


def test_pipeline_logs_stage_boundaries_and_report_metrics(tmp_path):
    log_path = tmp_path / "run.jsonl"
    configure_logging(log_level="INFO", log_file=log_path, quiet=True)
    source = _customer_file(tmp_path / "input" / "customers.csv", [_row("c1"), _row("c2")])
    result = run_pipeline(
        customers_path=source, products_path=None, run_id="structured-run",
        database_path=tmp_path / "db.sqlite", report_directory=tmp_path / "reports",
    )
    report = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    events = {(item.get("event"), item.get("stage")) for item in entries}
    assert ("stage_started", "validate") in events
    assert ("stage_completed", "validate") in events
    assert all(item.get("run_id") == result.run_id for item in entries)
    assert report["stage_metrics"]["validate"]["input_counts"]["customer_records"] == 2
    assert report["stage_metrics"]["validate"]["output_counts"]["customer_accepted"] == 2
    assert report["throughput_metrics"]["by_entity"]["customer"]["validation_acceptance_rate"] == 1.0
    assert report["throughput_metrics"]["by_entity"]["customer"]["validation_rejection_rate"] == 0.0
    assert report["execution_summary"]["records_processed"] == 2
    assert report["execution_summary"]["total_duration_seconds"] == report["execution_duration_seconds"]


def test_stage_failure_log_has_structured_diagnostics_without_source_values(tmp_path, monkeypatch):
    import src.pipeline as pipeline_module

    log_path = tmp_path / "failed.jsonl"
    configure_logging(log_level="INFO", log_file=log_path, quiet=True)
    source = _customer_file(tmp_path / "input" / "customers.csv", [_row("private-customer-id")])

    def fail_validation(*args, **kwargs):
        raise RuntimeError("failure text does not belong in structured logs")

    monkeypatch.setattr(pipeline_module, "validate_records", fail_validation)
    result = run_pipeline(customers_path=source, products_path=None, run_id="failed-log-run",
                          database_path=tmp_path / "failed.db", report_directory=tmp_path / "reports")
    records = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    failure = next(item for item in records if item.get("event") == "stage_failed")
    assert result.status == "FAILED"
    assert failure["run_id"] == "failed-log-run"
    assert failure["stage"] == "validate"
    assert failure["error_type"] == "RuntimeError"
    assert "output_counts" in failure and "input_counts" in failure
    assert "private-customer-id" not in log_path.read_text(encoding="utf-8")
    assert "failure text does not belong" not in log_path.read_text(encoding="utf-8")


def test_zero_record_metrics_are_safe_and_cli_log_options_work(tmp_path, capsys):
    quiet_log = tmp_path / "quiet.jsonl"
    code = main([
        "--customers-only", "--customers", str(_customer_file(tmp_path / "empty.csv", [])),
        "--run-id", "quiet-run", "--database", str(tmp_path / "quiet.db"),
        "--report-directory", str(tmp_path / "reports"), "--log-level", "WARNING",
        "--log-file", str(quiet_log), "--quiet",
    ])
    assert code == 0
    captured = capsys.readouterr()
    assert "MergeMind AI run quiet-run: COMPLETED" in captured.out
    assert "pipeline stage" not in captured.err
    report = json.loads((tmp_path / "reports" / "quiet-run_report.json").read_text(encoding="utf-8"))
    assert report["throughput_metrics"]["records_processed"] == 0
    assert report["throughput_metrics"]["records_per_second"] == 0.0
    assert report["throughput_metrics"]["by_entity"]["customer"]["validation_acceptance_rate"] == 0.0
    assert quiet_log.exists()
    assert logging.getLogger(LOGGER_NAME).level == logging.WARNING
