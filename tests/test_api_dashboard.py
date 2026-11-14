from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from src.api.app import app
from src.database import initialize_database


def _create_dashboard_database(path: Path) -> None:
    now = datetime.now(timezone.utc)
    recent = now.isoformat()
    previous = (now - timedelta(days=1)).isoformat()
    initialize_database(path)
    with sqlite3.connect(path) as connection:
        connection.executescript(
            "DROP TABLE review_history; DROP TABLE review_items; "
            "DROP TABLE quarantine_history; DROP TABLE quarantine_records; "
            "ALTER TABLE pipeline_runs DROP COLUMN input_fingerprint;"
        )
        connection.executemany(
            "INSERT INTO pipeline_runs (run_id, started_at, completed_at, status, customer_input_count, "
            "product_input_count, accepted_count, rejected_count, master_record_count, error_count) "
            "VALUES (?, ?, ?, ?, 3, 0, 2, 1, 2, 0)",
            [
                ("test-run", recent, recent, "COMPLETED"),
                ("older-run", previous, previous, "COMPLETED"),
            ],
        )
        connection.executemany(
            "INSERT INTO source_records (source_record_id, run_id, entity_type, source_system, source_file, "
            "source_record_number, original_record_json, normalized_record_json, validation_status) "
            "VALUES (?, 'test-run', 'customer', 'test', 'customers.csv', ?, ?, NULL, ?)",
            [
                ("c1", 1, '{"customer_id":"c1"}', "ACCEPTED"),
                ("c2", 2, '{"customer_id":"c2"}', "ACCEPTED"),
                ("c3", 3, '{"customer_id":"c3"}', "REJECTED"),
            ],
        )
        connection.executemany(
            "INSERT INTO duplicate_pairs (pair_id, run_id, entity_type, record_id_1, record_id_2, decision, "
            "confidence_score, matching_evidence_json, conflicting_evidence_json) VALUES (?, 'test-run', 'customer', ?, ?, ?, NULL, '{}', '{}')",
            [
                ("pair-exact", "c1", "c2", "EXACT"),
                ("pair-review", "c1", "c3", "REVIEW_REQUIRED"),
                ("pair-none", "c2", "c3", "NO_MATCH"),
            ],
        )
        connection.executemany(
            "INSERT INTO master_records (run_id, master_id, entity_type, merge_method, merged_record_json, created_at) "
            "VALUES ('test-run', ?, 'customer', 'SINGLETON', '{}', ?)",
            [("m1", recent), ("m2", recent)],
        )


def test_dashboard_aggregates_real_persisted_counts_after_additive_migration(
    tmp_path: Path, monkeypatch
) -> None:
    database_path = tmp_path / "dashboard.db"
    _create_dashboard_database(database_path)
    monkeypatch.setenv("MERGEMIND_DATABASE_PATH", str(database_path))

    with TestClient(app) as client:
        response = client.get("/api/v1/dashboard")

    assert response.status_code == 200
    payload = response.json()
    assert payload["kpis"] == {
        "source_records": 3,
        "accepted_records": 2,
        "rejected_records": 1,
        "master_records": 2,
        "exact_duplicate_groups": 1,
        "exact_duplicate_pairs": 1,
        "probable_match_decisions": {
            "STRONG_PROBABLE_MATCH": 0,
            "REVIEW_REQUIRED": 1,
            "NO_MATCH": 1,
        },
        "pending_reviews": 0,
        "quarantine_records": 0,
        "accepted_rate_percent": 66.67,
    }
    assert payload["review_data_available"] is True
    assert payload["quarantine_data_available"] is True
    assert payload["runs"][0]["run_id"] == "test-run"


def test_dashboard_validates_parameters_and_paginates_runs(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "dashboard.db"
    _create_dashboard_database(database_path)
    monkeypatch.setenv("MERGEMIND_DATABASE_PATH", str(database_path))
    with TestClient(app) as client:
        invalid = client.get("/api/v1/dashboard?days=0")
        page = client.get("/api/v1/dashboard?limit=1&offset=1")

    assert invalid.status_code == 422
    assert page.status_code == 200
    assert page.json()["runs"][0]["run_id"] == "older-run"
    assert page.json()["total_runs"] == 2


def test_dashboard_returns_service_unavailable_without_database(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MERGEMIND_DATABASE_PATH", str(tmp_path / "missing.db"))

    with TestClient(app) as client:
        response = client.get("/api/v1/dashboard")

    assert response.status_code == 503
    assert response.json()["detail"] == "The configured MergeMind database is unavailable."


def test_api_startup_migrates_legacy_schema_additively_and_preserves_rows(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "legacy.db"
    _create_dashboard_database(database_path)
    monkeypatch.setenv("MERGEMIND_DATABASE_PATH", str(database_path))

    with TestClient(app) as client:
        response = client.get("/api/v1/dashboard")

    assert response.status_code == 200
    with sqlite3.connect(database_path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        run_columns = {row[1] for row in connection.execute("PRAGMA table_info(pipeline_runs)")}
        source_count = connection.execute("SELECT COUNT(*) FROM source_records").fetchone()[0]
        run_count = connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
    assert {"review_items", "review_history", "quarantine_records", "quarantine_history"}.issubset(tables)
    assert "input_fingerprint" in run_columns
    assert source_count == 3
    assert run_count == 2
