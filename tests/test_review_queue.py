from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from src.database import create_pipeline_run, initialize_database, save_source_records
from src.duplicate_detection import DetectionResult, detect_exact_duplicates
from src.ingestion import IngestedRecord
from src.normalization import NormalizedRecord
from src.probable_matching import MatchingSummary, ProbableMatchingResult, ProbablePair
from src.review_queue import (
    ReviewItem,
    accept_review,
    defer_review,
    generate_review_items,
    reject_review,
    retrieve_pending_reviews,
    retrieve_review,
    retrieve_review_history,
    retrieve_review_statistics,
    retrieve_reviews_by_entity,
    save_review_queue,
)
from src.survivorship import (
    SurvivorshipResult,
    SurvivorshipSummary,
    UnresolvedSurvivorshipConflict,
    create_master_records,
)


INGESTED_AT = datetime(2025, 1, 1, tzinfo=timezone.utc)


def customer(record_id: str, *, name="Alex Doe", email="shared@example.test", phone="+14155550101", source="crm"):
    fields = {"customer_id": record_id, "name": name, "email": email, "phone": phone,
              "city": "Boston", "address": "1 Main St", "updated_at": "2025-01-01T00:00:00+00:00",
              "source_system": source}
    normalized = {"name": name.casefold(), "email": email.casefold() if email else None,
                  "phone": phone, "city": "boston", "address": "1 main st"}
    return NormalizedRecord(fields, normalized, "customer", "customers.csv", int(record_id[1:]), "csv", INGESTED_AT)


def product(record_id: str, *, name="Widget X", sku="SKU-1", brand="Brand A"):
    fields = {"product_id": record_id, "product_name": name, "sku": sku, "brand": brand,
              "category": "Tools", "price": "10.00", "updated_at": "2025-01-01T00:00:00+00:00",
              "source_system": "catalog"}
    normalized = {"product_name": name.casefold(), "sku": sku.casefold() if sku else None,
                  "brand": brand.casefold() if brand else None, "category": "tools"}
    return NormalizedRecord(fields, normalized, "product", "products.csv", int(record_id[1:]), "csv", INGESTED_AT)


def probable_pair(entity, first, second, decision="REVIEW_REQUIRED", confidence=62.5):
    return ProbablePair(entity, first, second, {"name": 90.0, "email": 0.0}, confidence,
                        ("name",), ("email",), decision, f"review {first}/{second}")


def probable_result(entity, pairs):
    return ProbableMatchingResult(
        probable_pairs=list(pairs),
        strong_probable_matches=[pair for pair in pairs if pair.decision == "STRONG_PROBABLE_MATCH"],
        review_candidates=[pair for pair in pairs if pair.decision == "REVIEW_REQUIRED"],
        non_matches=[pair for pair in pairs if pair.decision == "NO_MATCH"],
        matching_summary=MatchingSummary(entity, 3, 0, 3, len(pairs), 0.0,
                                         sum(pair.decision == "STRONG_PROBABLE_MATCH" for pair in pairs),
                                         sum(pair.decision == "REVIEW_REQUIRED" for pair in pairs),
                                         sum(pair.decision == "NO_MATCH" for pair in pairs)),
    )


def empty_survivorship(entity="customer", conflicts=()):
    return SurvivorshipResult([], list(conflicts), SurvivorshipSummary(entity, 0, 0, 0, 0, len(conflicts)))


def setup_db(tmp_path, run_id="run-a"):
    path = tmp_path / "nested" / "review.db"
    initialize_database(path)
    create_pipeline_run(run_id, db_path=path)
    return path


def save_sources(path, run_id, records):
    ingested = [IngestedRecord(record.source_fields, record.entity_type, record.source_file,
                               record.source_record_number, record.source_format, record.ingested_at)
                for record in records]
    save_source_records(run_id, ingested, normalized_records=records, db_path=path)


def test_customer_probable_review_candidate_contains_evidence_and_deterministic_id():
    records = [customer("C1"), customer("C2", name="Alex D.", email="other@example.test", phone="+14155550102")]
    pair = probable_pair("customer", "C1", "C2")
    result = generate_review_items("run-a", probable_result=probable_result("customer", [pair]),
                                   normalized_records=records)
    assert len(result) == 1
    item = result[0]
    assert item.entity_type == "customer"
    assert (item.record_id_1, item.record_id_2) == ("C1", "C2")
    assert item.confidence_score == 62.5
    assert item.review_status == "PENDING"
    assert "PROBABLE_REVIEW_REQUIRED" in item.matching_evidence["candidate_sources"]
    assert item.conflicting_evidence["evidence_by_source"]["PROBABLE_REVIEW_REQUIRED"]["conflicting_attributes"] == ("email",)
    assert item.review_id == generate_review_items("other-run", probable_result=probable_result("customer", [pair]))[0].review_id


def test_product_probable_review_candidate_is_supported():
    records = [product("P1"), product("P2", name="Widget X Plus", sku="SKU-2", brand="Brand B")]
    items = generate_review_items("run-p", probable_result=probable_result(
        "product", [probable_pair("product", "P2", "P1")]), normalized_records=records)
    assert len(items) == 1
    assert items[0].entity_type == "product"
    assert (items[0].record_id_1, items[0].record_id_2) == ("P1", "P2")


def test_exact_conflict_candidates_are_included_with_source_values():
    records = [customer("C1"), customer("C2", name="Grace Hopper")]
    exact = detect_exact_duplicates(records, "customer")
    items = generate_review_items("run-a", exact_result=exact, normalized_records=records)
    assert len(items) == 1
    assert "EXACT_DETECTION_CONFLICT" in items[0].conflicting_evidence["candidate_sources"]
    values = items[0].conflicting_evidence["evidence_by_source"]["EXACT_DETECTION_CONFLICT"]["field_values_by_record"]
    assert values["name"] == {"C1": "Alex Doe", "C2": "Grace Hopper"}


def test_ambiguous_probable_connections_are_queued_but_merged_pairs_are_not():
    rows = [customer("C1"), customer("C2", name="Alex D."), customer("C3", name="Alex Doe Jr")]
    pair1, pair2 = probable_pair("customer", "C1", "C2", "STRONG_PROBABLE_MATCH"), probable_pair("customer", "C2", "C3", "STRONG_PROBABLE_MATCH")
    decisions = probable_result("customer", [pair1, pair2])
    unsafe = empty_survivorship("customer", [
        UnresolvedSurvivorshipConflict("customer", ("C1", "C2"), "AMBIGUOUS_PROBABLE_COMPONENT", "shared endpoint"),
        UnresolvedSurvivorshipConflict("customer", ("C2", "C3"), "AMBIGUOUS_PROBABLE_COMPONENT", "shared endpoint"),
    ])
    items = generate_review_items("run-a", probable_result=decisions, survivorship_result=unsafe,
                                  normalized_records=rows)
    assert {(item.record_id_1, item.record_id_2) for item in items} == {("C1", "C2"), ("C2", "C3")}
    assert all("AMBIGUOUS_PROBABLE_CONNECTION" in item.matching_evidence["candidate_sources"] for item in items)

    merged = create_master_records(rows[:2], "customer", probable_result=[pair1])
    assert generate_review_items("run-a", probable_result=probable_result("customer", [pair1]),
                                 survivorship_result=merged,
                                 normalized_records=rows[:2]) == []


def test_no_match_and_strong_match_without_unsafe_survivorship_conflict_are_excluded():
    pairs = [probable_pair("customer", "C1", "C2", "NO_MATCH"),
             probable_pair("customer", "C1", "C3", "STRONG_PROBABLE_MATCH")]
    items = generate_review_items("run-a", probable_result=probable_result("customer", pairs))
    assert items == []


def test_duplicate_candidate_sources_are_combined_into_one_review_item():
    records = [customer("C1"), customer("C2", name="Grace Hopper")]
    exact = detect_exact_duplicates(records, "customer")
    probable = probable_result("customer", [probable_pair("customer", "C1", "C2")])
    items = generate_review_items("run-a", exact_result=exact, probable_result=probable,
                                  normalized_records=records)
    assert len(items) == 1
    assert items[0].matching_evidence["candidate_sources"] == ["EXACT_DETECTION_CONFLICT", "PROBABLE_REVIEW_REQUIRED"]
    assert items[0].conflicting_evidence["candidate_sources"] == ["EXACT_DETECTION_CONFLICT", "PROBABLE_REVIEW_REQUIRED"]


def test_no_match_decision_excludes_same_pair_even_if_another_source_flags_it():
    records = [customer("C1"), customer("C2", name="Grace Hopper")]
    exact = detect_exact_duplicates(records, "customer")
    probable = probable_result("customer", [probable_pair("customer", "C1", "C2", "NO_MATCH")])
    assert generate_review_items("run-a", exact_result=exact, probable_result=probable,
                                 normalized_records=records) == []


def test_database_insert_pending_retrieval_and_duplicate_prevention(tmp_path):
    path = setup_db(tmp_path)
    records = [customer("C1"), customer("C2", name="Alex D.")]
    save_sources(path, "run-a", records)
    item = generate_review_items("run-a", probable_result=probable_result(
        "customer", [probable_pair("customer", "C1", "C2")]))[0]
    assert save_review_queue("run-a", [item], db_path=path) == 1
    assert save_review_queue("run-a", [item], db_path=path) == 0
    assert len(retrieve_pending_reviews("run-a", db_path=path)) == 1
    assert retrieve_reviews_by_entity("run-a", "customer", db_path=path)[0]["review_id"] == item.review_id
    assert retrieve_review("run-a", item.review_id, db_path=path)["review_status"] == "PENDING"
    assert len(retrieve_review_history("run-a", item.review_id, db_path=path)) == 1


@pytest.mark.parametrize(("action", "status"), [
    (accept_review, "ACCEPTED_MATCH"), (reject_review, "REJECTED_MATCH"), (defer_review, "DEFERRED"),
])
def test_review_actions_update_state_and_append_audit_history(tmp_path, action, status):
    path = setup_db(tmp_path)
    records = [product("P1"), product("P2", sku="SKU-2")]
    save_sources(path, "run-a", records)
    item = generate_review_items("run-a", probable_result=probable_result(
        "product", [probable_pair("product", "P1", "P2")]))[0]
    save_review_queue("run-a", [item], db_path=path)
    updated = action("run-a", item.review_id, reviewer_id="reviewer-7", notes="manual decision", db_path=path)
    assert updated["review_status"] == status
    assert updated["reviewer_id"] == "reviewer-7"
    assert updated["reviewer_notes"] == "manual decision"
    history = retrieve_review_history("run-a", item.review_id, db_path=path)
    assert [event["new_status"] for event in history] == ["PENDING", status]
    assert history[-1]["previous_status"] == "PENDING"
    stats = retrieve_review_statistics("run-a", db_path=path)
    assert stats["total_review_items"] == 1
    assert stats[{"ACCEPTED_MATCH": "accepted_reviews", "REJECTED_MATCH": "rejected_reviews", "DEFERRED": "deferred_reviews"}[status]] == 1


def test_deferred_item_can_be_resolved_and_terminal_decisions_cannot_be_overwritten(tmp_path):
    path = setup_db(tmp_path)
    records = [customer("C1"), customer("C2", name="Alex D.")]
    save_sources(path, "run-a", records)
    item = generate_review_items("run-a", probable_result=probable_result(
        "customer", [probable_pair("customer", "C1", "C2")]))[0]
    save_review_queue("run-a", [item], db_path=path)
    defer_review("run-a", item.review_id, db_path=path)
    accepted = accept_review("run-a", item.review_id, reviewer_id="r2", db_path=path)
    assert accepted["review_status"] == "ACCEPTED_MATCH"
    with pytest.raises(ValueError, match="Invalid review transition"):
        reject_review("run-a", item.review_id, db_path=path)
    assert [event["new_status"] for event in retrieve_review_history("run-a", item.review_id, db_path=path)] == [
        "PENDING", "DEFERRED", "ACCEPTED_MATCH"
    ]


def test_invalid_review_ids_and_missing_items_are_rejected(tmp_path):
    path = setup_db(tmp_path)
    with pytest.raises(ValueError, match="review_id"):
        retrieve_review("run-a", "not-a-review-id", db_path=path)
    with pytest.raises(KeyError, match="not found"):
        accept_review("run-a", "review-000000000000000000000000", db_path=path)


def test_review_decision_transaction_rolls_back_when_history_insert_fails(tmp_path):
    path = setup_db(tmp_path)
    records = [customer("C1"), customer("C2", name="Alex D.")]
    save_sources(path, "run-a", records)
    item = generate_review_items("run-a", probable_result=probable_result(
        "customer", [probable_pair("customer", "C1", "C2")]))[0]
    save_review_queue("run-a", [item], db_path=path)
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TRIGGER reject_review_history BEFORE INSERT ON review_history
            WHEN NEW.new_status = 'ACCEPTED_MATCH' BEGIN SELECT RAISE(ABORT, 'forced history failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="forced history failure"):
        accept_review("run-a", item.review_id, db_path=path)
    assert retrieve_review("run-a", item.review_id, db_path=path)["review_status"] == "PENDING"
    assert [event["new_status"] for event in retrieve_review_history("run-a", item.review_id, db_path=path)] == ["PENDING"]


def test_run_isolation_and_rerun_preserve_review_decisions(tmp_path):
    path = setup_db(tmp_path, "run-a")
    create_pipeline_run("run-b", db_path=path)
    records = [customer("C1"), customer("C2", name="Alex D.")]
    save_sources(path, "run-a", records)
    save_sources(path, "run-b", records)
    item_a = generate_review_items("run-a", probable_result=probable_result(
        "customer", [probable_pair("customer", "C1", "C2")]))[0]
    item_b = generate_review_items("run-b", probable_result=probable_result(
        "customer", [probable_pair("customer", "C1", "C2")]))[0]
    assert item_a.review_id == item_b.review_id
    save_review_queue("run-a", [item_a], db_path=path)
    save_review_queue("run-b", [item_b], db_path=path)
    accept_review("run-a", item_a.review_id, db_path=path)
    assert save_review_queue("run-a", [item_a], db_path=path) == 0
    assert retrieve_review("run-a", item_a.review_id, db_path=path)["review_status"] == "ACCEPTED_MATCH"
    assert retrieve_review("run-b", item_b.review_id, db_path=path)["review_status"] == "PENDING"
    assert retrieve_review_statistics("run-a", db_path=path)["accepted_reviews"] == 1
    assert retrieve_review_statistics("run-b", db_path=path)["pending_reviews"] == 1


def test_empty_queue_returns_zero_statistics(tmp_path):
    path = setup_db(tmp_path)
    assert retrieve_pending_reviews("run-a", db_path=path) == []
    stats = retrieve_review_statistics("run-a", db_path=path)
    assert stats["total_review_items"] == 0
    assert stats["customer_review_count"] == stats["product_review_count"] == 0
