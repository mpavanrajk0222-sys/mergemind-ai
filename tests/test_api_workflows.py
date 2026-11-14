from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from src.api.app import app
from src.database import save_review_items
from src.review_queue import ReviewItem


CUSTOMER_CSV = """customer_id,name,email,phone,city,address,updated_at,source_system
c1,Acme Person,person@example.com,+15551234567,Springfield,1 Main St,2026-11-14T10:00:00Z,crm
c2,Acme Person,person@example.com,+15551234567,Springfield,1 Main St,2026-11-14T10:00:00Z,crm
c3,Other Person,other@example.com,+15557654321,Denver,3 Main St,2026-11-14T10:00:00Z,crm
c4,,invalid@example.com,+15557654322,Denver,4 Main St,2026-11-14T10:00:00Z,crm
c5,,invalid2@example.com,+15557654323,Denver,5 Main St,2026-11-14T10:00:00Z,crm
"""


def test_upload_pipeline_explore_review_quarantine_masters_lineage_and_report(
    tmp_path: Path, monkeypatch
) -> None:
    database_path = tmp_path / "integration.db"
    reports_path = tmp_path / "reports"
    monkeypatch.setenv("MERGEMIND_DATABASE_PATH", str(database_path))
    monkeypatch.setenv("MERGEMIND_REPORT_DIRECTORY", str(reports_path))

    with TestClient(app) as client:
        upload = client.post(
            "/api/v1/ingestion/uploads",
            data={"entity_type": "customer"},
            files={"file": ("customers.csv", CUSTOMER_CSV.encode(), "text/csv")},
        )
        assert upload.status_code == 200
        upload_data = upload.json()
        assert upload_data["total_records"] == 5
        assert upload_data["accepted_records"] == 3
        assert upload_data["rejected_records"] == 2

        execute_body = {"customer_upload_id": upload_data["upload_id"]}
        headers = {"Idempotency-Key": "workflow-integration-001"}
        execution = client.post("/api/v1/pipeline-runs", json=execute_body, headers=headers)
        assert execution.status_code == 200, execution.text
        result = execution.json()
        run_id = result["run_id"]
        assert result["status"] == "COMPLETED"
        assert "report_path" not in result["result"]

        duplicate_page = client.get(
            f"/api/v1/pipeline-runs/{run_id}/duplicate-decisions?decision=EXACT"
        )
        assert duplicate_page.status_code == 200
        duplicate_items = duplicate_page.json()["items"]
        assert duplicate_items
        assert duplicate_items[0]["confidence_score"] is None
        assert len(duplicate_items[0]["source_records"]) == 2

        with __import__("sqlite3").connect(database_path) as connection:
            source_ids = {row[0] for row in connection.execute(
                "SELECT source_record_id FROM source_records WHERE run_id = ? AND validation_status = 'ACCEPTED'",
                (run_id,),
            )}
        review_id = "review-aaaaaaaaaaaaaaaaaaaaaaaa"
        assert {"c1", "c3"}.issubset(source_ids)
        save_review_items(run_id, [ReviewItem(
            review_id=review_id,
            run_id=run_id,
            entity_type="customer",
            record_id_1="c1",
            record_id_2="c3",
            confidence_score=0.73,
            matching_evidence={"test": "persisted evidence"},
            conflicting_evidence={"city": {"c1": "Springfield", "c3": "Denver"}},
            decision_explanation="Integration test review item.",
            review_status="PENDING",
            created_at=datetime.now(timezone.utc),
        )], db_path=database_path)
        review_page = client.get(f"/api/v1/pipeline-runs/{run_id}/reviews?status=PENDING&entity_type=customer")
        assert review_page.status_code == 200
        assert review_page.json()["total"] == 1
        review_detail = client.get(f"/api/v1/pipeline-runs/{run_id}/reviews/{review_id}")
        assert len(review_detail.json()["source_records"]) == 2
        decision = client.post(
            f"/api/v1/pipeline-runs/{run_id}/reviews/{review_id}/decision",
            json={"decision": "DEFERRED", "reviewer_id": "test-reviewer", "notes": "Check later"},
        )
        assert decision.status_code == 200
        assert decision.json()["review_status"] == "DEFERRED"
        assert decision.json()["history"][-1]["new_status"] == "DEFERRED"
        assert decision.json()["history"][-1]["reviewer_id"] == "test-reviewer"
        assert decision.json()["history"][-1]["reviewer_notes"] == "Check later"

        quarantine_page = client.get(f"/api/v1/pipeline-runs/{run_id}/quarantine?error_code=EMPTY_REQUIRED_FIELD")
        assert quarantine_page.status_code == 200
        assert quarantine_page.json()["total"] == 2
        quarantine_id = quarantine_page.json()["items"][0]["quarantine_id"]
        quarantine_detail = client.get(f"/api/v1/pipeline-runs/{run_id}/quarantine/{quarantine_id}")
        assert quarantine_detail.json()["original_record"]
        action = client.post(
            f"/api/v1/pipeline-runs/{run_id}/quarantine/{quarantine_id}/resolution",
            json={"action": "RESOLVE", "resolution_action": "CONTACT_SOURCE", "notes": "Source follow-up"},
        )
        assert action.status_code == 200
        assert action.json()["quarantine_status"] == "RESOLVED"
        assert action.json()["history"][-1]["new_status"] == "RESOLVED"
        assert action.json()["history"][-1]["resolution_notes"] == "Source follow-up"

        master_page = client.get(f"/api/v1/pipeline-runs/{run_id}/masters?entity_type=customer&limit=2")
        assert master_page.status_code == 200
        assert master_page.json()["total"] >= 2
        master_id = master_page.json()["items"][0]["master_id"]
        master_detail = client.get(f"/api/v1/pipeline-runs/{run_id}/masters/{master_id}")
        assert master_detail.status_code == 200
        assert "merged_fields" in master_detail.json()
        lineage = client.get(f"/api/v1/pipeline-runs/{run_id}/masters/{master_id}/lineage")
        assert lineage.status_code == 200
        assert lineage.json()["fields"]

        runs = client.get("/api/v1/pipeline-runs?status=COMPLETED")
        assert runs.status_code == 200
        assert runs.json()["total"] == 1
        run_detail = client.get(f"/api/v1/pipeline-runs/{run_id}")
        assert run_detail.status_code == 200
        assert run_detail.json()["stage_metrics"]
        assert "data_quality_issues" in run_detail.json()
        assert "operational_errors" in run_detail.json()

        report_list = client.get("/api/v1/reports")
        assert report_list.status_code == 200
        assert report_list.json()["total"] == 1
        report_response = client.get(f"/api/v1/reports/{run_id}/download")
        assert report_response.status_code == 200
        report = report_response.json()
        assert "report_path" not in report
        assert report["run_id"] == run_id

        repeated = client.post("/api/v1/pipeline-runs", json=execute_body, headers=headers)
        assert repeated.status_code == 200
        assert repeated.json()["run_id"] == run_id
        assert client.get("/api/v1/pipeline-runs").json()["total"] == 1


def test_duplicate_explorer_filters_identity_conflicts(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "identity-conflict.db"
    reports_path = tmp_path / "reports"
    monkeypatch.setenv("MERGEMIND_DATABASE_PATH", str(database_path))
    monkeypatch.setenv("MERGEMIND_REPORT_DIRECTORY", str(reports_path))
    conflict_csv = """customer_id,name,email,phone,city,address,updated_at,source_system
c1,Jordan Lee,jordan@example.test,+14155550101,Portland,1 Main St,2026-11-14T10:00:00Z,crm
c2,Jordan Lee,jordan@example.test,+14155550102,Portland,2 Main St,2026-11-14T10:00:00Z,crm
"""

    with TestClient(app) as client:
        upload = client.post(
            "/api/v1/ingestion/uploads",
            data={"entity_type": "customer"},
            files={"file": ("conflicts.csv", conflict_csv.encode(), "text/csv")},
        )
        assert upload.status_code == 200
        execution = client.post(
            "/api/v1/pipeline-runs",
            json={"customer_upload_id": upload.json()["upload_id"]},
            headers={"Idempotency-Key": "identity-conflict-flow-001"},
        )
        assert execution.status_code == 200, execution.text
        run_id = execution.json()["run_id"]

        conflict_page = client.get(
            f"/api/v1/pipeline-runs/{run_id}/duplicate-decisions?decision=IDENTITY_CONFLICT"
        )
        assert conflict_page.status_code == 200, conflict_page.text
        assert conflict_page.json()["total"] == 1
        assert conflict_page.json()["items"][0]["decision"] == "IDENTITY_CONFLICT"
