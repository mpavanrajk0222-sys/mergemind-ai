from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

import pytest

from src.duplicate_detection import detect_exact_duplicates
from src.normalization import NormalizedRecord, normalize_records
from src.probable_matching import ProbablePair
from src.survivorship import create_master_records


def customer(record_id: str, *, updated: str = "2025-01-01T00:00:00+00:00", **fields) -> NormalizedRecord:
    values = {
        "customer_id": record_id,
        "name": "Alex Doe",
        "email": None,
        "phone": None,
        "city": None,
        "address": None,
        "updated_at": updated,
        "source_system": "crm",
    }
    values.update(fields)
    valid = [item for item in normalize_records([
        _ingested(values, "customer", record_id)
    ], "customer")]
    return valid[0]


def product(record_id: str, *, updated: str = "2025-01-01T00:00:00+00:00", **fields) -> NormalizedRecord:
    values = {
        "product_id": record_id,
        "product_name": "Widget",
        "sku": None,
        "brand": None,
        "category": None,
        "price": None,
        "updated_at": updated,
        "source_system": "catalog",
    }
    values.update(fields)
    return normalize_records([_ingested(values, "product", record_id)], "product")[0]


def _ingested(values: dict, entity: str, record_id: str):
    from src.ingestion import IngestedRecord

    return IngestedRecord(
        source_fields=values,
        entity_type=entity,
        source_file=f"{entity}s.csv",
        source_record_number=int(record_id[1:]) if record_id[1:].isdigit() else 1,
        source_format="csv",
        ingested_at=datetime(2025, 1, 5, tzinfo=timezone.utc),
    )


def strong(first: str, second: str, entity: str = "customer") -> ProbablePair:
    return ProbablePair(entity, first, second, {}, 90.0, (), (), "STRONG_PROBABLE_MATCH", "strong evidence")


def decision(first: str, second: str, value: str, entity: str = "customer") -> ProbablePair:
    return ProbablePair(entity, first, second, {}, 40.0, (), (), value, f"{value.lower()} explanation")


def test_exact_group_selects_fields_and_preserves_conflicting_lineage():
    records = [
        customer("c1", email="old@example.com", phone="111", name="Alex", source_system="crm"),
        customer("c2", email="new@example.com", phone="222", name="Alexandra Doe", city="Pune",
                 updated="2025-02-01T00:00:00+00:00", source_system="billing"),
        customer("c3", email="new@example.com", phone="222", name="Alexandra Doe", city="Pune",
                 updated="2025-02-01T00:00:00+00:00", source_system="support"),
    ]
    exact = detect_exact_duplicates(records, "customer")
    result = create_master_records(records, "customer", exact_result=exact)
    master = next(row for row in result.master_records if row.merge_method == "EXACT")
    assert master.contributing_source_record_ids == ("c2", "c3")
    assert master.contributing_source_systems == ("billing", "support")
    assert master.merged_fields["email"] == "new@example.com"
    assert master.field_level_decisions["email"].selected_source_record_id == "c2"
    assert result.summary.singleton_count == 1


def test_strong_probable_pair_merges_and_singletons_remain_individual():
    rows = [customer("c1", name="A Doe"), customer("c2", name="Alex Doe"), customer("c3")]
    result = create_master_records(rows, "customer", probable_result=[strong("c1", "c2")])
    assert sorted(master.merge_method for master in result.master_records) == ["SINGLETON", "STRONG_PROBABLE"]
    assert result.summary.merged_group_count == 1


def test_review_required_and_no_match_pairs_are_not_merged():
    rows = [customer("c1"), customer("c2"), customer("c3")]
    result = create_master_records(rows, "customer", probable_result=[
        decision("c1", "c2", "REVIEW_REQUIRED"), decision("c2", "c3", "NO_MATCH")
    ])
    assert len(result.master_records) == 3
    assert all(row.merge_method == "SINGLETON" for row in result.master_records)
    assert [item.conflict_code for item in result.unresolved_conflicts] == ["REVIEW_REQUIRED_PAIR"]


def test_probable_transitive_component_is_reviewed_and_not_merged():
    rows = [customer("c1"), customer("c2"), customer("c3")]
    result = create_master_records(rows, "customer", probable_result=[strong("c1", "c2"), strong("c2", "c3")])
    assert len(result.master_records) == 3
    assert all(row.merge_method == "SINGLETON" for row in result.master_records)
    assert len([item for item in result.unresolved_conflicts if item.conflict_code == "AMBIGUOUS_PROBABLE_COMPONENT"]) == 2


def test_exact_group_cannot_be_extended_by_probable_edge():
    rows = [customer("c1", email="same@example.com", phone="1"), customer("c2", email="same@example.com", phone="1"), customer("c3")]
    exact = detect_exact_duplicates(rows, "customer")
    result = create_master_records(rows, "customer", exact_result=exact, probable_result=[strong("c1", "c3")])
    assert sorted(len(row.contributing_source_record_ids) for row in result.master_records) == [1, 2]
    assert any(item.conflict_code == "PROBABLE_MATCH_CONNECTS_EXISTING_GROUPS" for item in result.unresolved_conflicts)


@pytest.mark.parametrize("field", ["email", "phone"])
def test_conflicting_customer_identifiers_are_retained(field):
    first = customer("c1", **{field: "one@example.com" if field == "email" else "111"})
    second = customer("c2", **{field: "two@example.com" if field == "email" else "222"})
    result = create_master_records([first, second], "customer", probable_result=[strong("c1", "c2")])
    master = result.master_records[0]
    lineage = master.field_level_decisions[field]
    assert len(lineage.rejected_conflicting_values) == 1
    assert lineage.selected_source_record_id in {"c1", "c2"}


def test_missing_optional_values_stay_null_and_incomplete_name_loses():
    rows = [customer("c1", name="Alex", updated="2025-02-01T00:00:00+00:00"),
            customer("c2", name="Alexandra Marie Doe", updated="2025-01-01T00:00:00+00:00")]
    result = create_master_records(rows, "customer", probable_result=[strong("c1", "c2")])
    master = result.master_records[0]
    assert master.merged_fields["name"] == "Alexandra Marie Doe"
    assert master.merged_fields["email"] is None
    assert master.field_level_decisions["email"].selection_rule == "no_non_empty_value"


def test_latest_valid_timestamp_is_retained():
    rows = [customer("c1", updated="2025-01-01T00:00:00+00:00"), customer("c2", updated="2025-03-01T00:00:00+00:00")]
    master = create_master_records(rows, "customer", probable_result=[strong("c1", "c2")]).master_records[0]
    assert master.merged_fields["updated_at"] == "2025-03-01T00:00:00+00:00"
    assert master.field_level_decisions["updated_at"].selected_source_record_id == "c2"


def test_product_conflicting_skus_are_not_silently_selected():
    rows = [product("p1", sku="SKU-1"), product("p2", sku="SKU-2")]
    result = create_master_records(rows, "product", probable_result=[strong("p1", "p2", "product")])
    master = result.master_records[0]
    assert master.merged_fields["sku"] is None
    assert len(master.field_level_decisions["sku"].rejected_conflicting_values) == 2
    assert any(item.conflict_code == "CONFLICTING_SKUS" for item in result.unresolved_conflicts)


def test_product_price_uses_latest_timestamp_and_records_alternatives():
    rows = [product("p1", price="10.00", updated="2025-01-01T00:00:00+00:00"),
            product("p2", price="12.50", updated="2025-02-01T00:00:00+00:00")]
    master = create_master_records(rows, "product", probable_result=[strong("p1", "p2", "product")]).master_records[0]
    assert master.merged_fields["price"] == "12.50"
    assert master.field_level_decisions["price"].selection_rule.startswith("latest_source_updated_at")
    assert master.field_level_decisions["price"].rejected_conflicting_values[0].value == "10.00"


def test_empty_input_and_determinism_under_reversal_and_input_immutability():
    assert create_master_records([], "customer").master_records == []
    rows = [customer("c1", email="same@example.com", phone="1"), customer("c2", email="same@example.com", phone="1"), customer("c3")]
    before = deepcopy([row.source_fields for row in rows])
    exact = detect_exact_duplicates(rows, "customer")
    left = create_master_records(rows, "customer", exact_result=exact)
    right = create_master_records(list(reversed(rows)), "customer", exact_result=detect_exact_duplicates(list(reversed(rows)), "customer"))
    assert left == right
    assert [row.source_fields for row in rows] == before
    memberships = [record_id for row in left.master_records for record_id in row.contributing_source_record_ids]
    assert sorted(memberships) == ["c1", "c2", "c3"]
    assert len(memberships) == len(set(memberships))


def test_master_created_at_is_deterministic_source_ingestion_time():
    rows = [customer("c1"), customer("c2")]
    master = create_master_records(rows, "customer", probable_result=[strong("c1", "c2")]).master_records[0]
    assert master.created_at == datetime(2025, 1, 5, tzinfo=timezone.utc)
