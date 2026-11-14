from datetime import datetime, timezone

import pytest

from src.ingestion import IngestedRecord
from src.normalization import NormalizedRecord, normalize_records
from src.duplicate_detection import detect_exact_duplicates
from src.validation import validate_records


INGESTED_AT = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _customer(
    record_id: str,
    *,
    name: str = "ada lovelace",
    email: str | None = "ada@example.test",
    phone: str | None = "+14155550100",
) -> NormalizedRecord:
    source_fields = {
        "customer_id": record_id,
        "name": name,
        "email": email,
        "phone": phone,
        "city": "london",
        "address": "1 main street",
        "updated_at": "2025-01-01T00:00:00Z",
        "source_system": f"source-{record_id}",
    }
    return NormalizedRecord(
        source_fields,
        {"name": name, "email": email, "phone": phone, "city": "london", "address": "1 main street"},
        "customer",
        "customers.csv",
        int(record_id[1:]),
        "csv",
        INGESTED_AT,
    )


def _product(
    record_id: str,
    *,
    name: str = "cedar mug",
    sku: str | None = "sku-1",
    brand: str | None = "northstar",
) -> NormalizedRecord:
    source_fields = {
        "product_id": record_id,
        "product_name": name,
        "sku": sku,
        "brand": brand,
        "category": "home",
        "price": "12.50",
        "updated_at": "2025-01-01T00:00:00Z",
        "source_system": f"source-{record_id}",
    }
    return NormalizedRecord(
        source_fields,
        {"product_name": name, "sku": sku, "brand": brand, "category": "home"},
        "product",
        "products.csv",
        int(record_id[1:]),
        "csv",
        INGESTED_AT,
    )


def test_customer_same_email_phone_and_name_forms_exact_group() -> None:
    result = detect_exact_duplicates([_customer("C1"), _customer("C2")], "customer")

    assert len(result.duplicate_groups) == 1
    group = result.duplicate_groups[0]
    assert group.source_record_ids == ("C1", "C2")
    assert group.matching_attributes == ("email", "phone", "name")
    assert group.matching_evidence == {
        "email": "ada@example.test", "phone": "+14155550100", "name": "ada lovelace"
    }
    assert group.match_type == "EXACT"
    assert len(result.duplicate_pairs) == 1


def test_three_customer_records_form_one_group_and_all_unique_pairs() -> None:
    result = detect_exact_duplicates([_customer("C3"), _customer("C1"), _customer("C2")], "customer")

    assert len(result.duplicate_groups) == 1
    assert result.duplicate_groups[0].source_record_ids == ("C1", "C2", "C3")
    assert {pair.source_record_ids for pair in result.duplicate_pairs} == {
        ("C1", "C2"), ("C1", "C3"), ("C2", "C3")
    }
    assert result.detection_summary.duplicate_pair_count == 3


def test_customer_same_email_and_phone_but_conflicting_names_is_reviewed() -> None:
    result = detect_exact_duplicates(
        [_customer("C1"), _customer("C2", name="grace hopper")], "customer"
    )

    assert result.duplicate_groups == []
    assert result.conflict_candidates[0].source_record_ids == ("C1", "C2")
    assert result.conflict_candidates[0].conflicting_fields == ("name",)
    assert result.conflict_candidates[0].recommended_action == "REVIEW"
    assert result.unmatched_records == []


def test_customer_same_email_and_name_with_different_phones_is_reviewed() -> None:
    result = detect_exact_duplicates(
        [_customer("C1"), _customer("C2", phone="+442012345678")], "customer"
    )

    assert result.duplicate_groups == []
    assert result.conflict_candidates[0].conflicting_fields == ("phone",)


def test_customer_same_phone_and_name_with_different_emails_is_reviewed() -> None:
    result = detect_exact_duplicates(
        [_customer("C1"), _customer("C2", email="ada@other.test")], "customer"
    )

    assert result.duplicate_groups == []
    assert result.conflict_candidates[0].conflicting_fields == ("email",)


def test_customer_name_only_or_shared_name_does_not_match() -> None:
    records = [
        _customer("C1", email="one@example.test", phone="+14155550101"),
        _customer("C2", email="two@example.test", phone="+14155550102"),
    ]
    result = detect_exact_duplicates(records, "customer")

    assert result.duplicate_groups == []
    assert result.duplicate_pairs == []
    assert result.conflict_candidates == []
    assert len(result.unmatched_records) == 2


@pytest.mark.parametrize("missing_field", ["email", "phone"])
def test_missing_customer_identifier_does_not_create_match(missing_field: str) -> None:
    first = _customer("C1", **{missing_field: None})
    second = _customer("C2", **{missing_field: None})

    result = detect_exact_duplicates([first, second], "customer")

    assert result.duplicate_groups == []
    assert result.unmatched_records == [first, second]


def test_customer_grouping_is_deterministic_across_input_order() -> None:
    records = [_customer("C3"), _customer("C1"), _customer("C2")]

    first = detect_exact_duplicates(records, "customer")
    second = detect_exact_duplicates(list(reversed(records)), "customer")

    assert first == second
    assert first.duplicate_groups[0].group_id == second.duplicate_groups[0].group_id


def test_same_sku_and_brand_forms_exact_product_group() -> None:
    result = detect_exact_duplicates([_product("P1"), _product("P2")], "product")

    assert len(result.duplicate_groups) == 1
    group = result.duplicate_groups[0]
    assert group.source_record_ids == ("P1", "P2")
    assert group.matching_attributes == ("sku", "product_name", "brand")
    assert group.matching_evidence == {"sku": "sku-1", "product_name": "cedar mug", "brand": "northstar"}


def test_multiple_products_sharing_sku_brand_and_name_form_group() -> None:
    result = detect_exact_duplicates([_product("P3"), _product("P1"), _product("P2")], "product")

    assert len(result.duplicate_groups) == 1
    assert result.duplicate_groups[0].source_record_ids == ("P1", "P2", "P3")
    assert len(result.duplicate_pairs) == 3


def test_same_sku_with_conflicting_brands_is_reviewed_not_merged() -> None:
    result = detect_exact_duplicates(
        [_product("P1"), _product("P2", brand="other brand")], "product"
    )

    assert result.duplicate_groups == []
    assert result.conflict_candidates[0].conflicting_fields == ("brand",)
    assert result.conflict_candidates[0].recommended_action == "REVIEW"


def test_same_sku_with_conflicting_product_names_is_reviewed() -> None:
    result = detect_exact_duplicates(
        [_product("P1"), _product("P2", name="cedar cup")], "product"
    )

    assert result.duplicate_groups == []
    assert result.conflict_candidates[0].conflicting_fields == ("product_name",)


def test_similar_product_names_with_different_skus_do_not_match() -> None:
    result = detect_exact_duplicates(
        [_product("P1", sku="sku-1"), _product("P2", sku="sku-2")], "product"
    )

    assert result.duplicate_groups == []
    assert result.conflict_candidates == []
    assert len(result.unmatched_records) == 2


def test_missing_sku_does_not_create_product_match() -> None:
    records = [_product("P1", sku=None), _product("P2", sku=None)]

    result = detect_exact_duplicates(records, "product")

    assert result.duplicate_groups == []
    assert result.unmatched_records == records


def test_missing_brand_is_compatible_when_sku_and_name_agree() -> None:
    records = [_product("P1", brand=None), _product("P2", brand="northstar")]

    result = detect_exact_duplicates(records, "product")

    assert len(result.duplicate_groups) == 1
    assert result.duplicate_groups[0].matching_attributes == ("sku", "product_name")
    assert result.conflict_candidates == []


def test_product_grouping_is_deterministic_across_input_order() -> None:
    records = [_product("P3"), _product("P1"), _product("P2")]

    assert detect_exact_duplicates(records, "product") == detect_exact_duplicates(list(reversed(records)), "product")


@pytest.mark.parametrize("entity_type", ["customer", "product"])
def test_empty_and_single_record_batches_have_no_groups(entity_type: str) -> None:
    record = _customer("C1") if entity_type == "customer" else _product("P1")
    empty = detect_exact_duplicates([], entity_type)
    single = detect_exact_duplicates([record], entity_type)

    assert empty.detection_summary.records_processed == 0
    assert empty.duplicate_groups == empty.duplicate_pairs == empty.conflict_candidates == []
    assert single.detection_summary.records_processed == 1
    assert single.unmatched_records == [record]


def test_records_do_not_appear_in_multiple_exact_groups_and_pairs_are_unique() -> None:
    first = _customer("C1")
    same = _customer("C2")
    conflict = _customer("C3", phone="+442012345678")
    result = detect_exact_duplicates([first, same, conflict], "customer")

    grouped_ids = [record_id for group in result.duplicate_groups for record_id in group.source_record_ids]
    assert len(grouped_ids) == len(set(grouped_ids))
    pair_ids = [pair.source_record_ids for pair in result.duplicate_pairs]
    assert len(pair_ids) == len(set(pair_ids))
    assert {candidate.source_record_ids for candidate in result.conflict_candidates} == {("C1", "C3"), ("C2", "C3")}


def test_detection_does_not_mutate_input_records() -> None:
    for entity_type, records in (
        ("customer", [_customer("C1"), _customer("C2")]),
        ("product", [_product("P1"), _product("P2")]),
    ):
        original_fields = [record.source_fields.copy() for record in records]
        original_normalized = [record.normalized_fields.copy() for record in records]

        detect_exact_duplicates(records, entity_type)

        assert [record.source_fields for record in records] == original_fields
        assert [record.normalized_fields for record in records] == original_normalized


def test_detection_integrates_with_ingestion_validation_and_normalization() -> None:
    ingested = [
        IngestedRecord(
            {"customer_id": "C1", "name": " Ada   Lovelace ", "email": "ADA@example.test", "phone": "+1 (415) 555-0100", "updated_at": "2025-01-01T00:00:00Z", "source_system": "a"},
            "customer", "customers.json", 1, "json", INGESTED_AT,
        ),
        IngestedRecord(
            {"customer_id": "C2", "name": "ada lovelace", "email": "ada@example.test", "phone": "+14155550100", "updated_at": "2025-01-01T00:00:00Z", "source_system": "b"},
            "customer", "customers.json", 2, "json", INGESTED_AT,
        ),
    ]
    validation = validate_records(ingested, "customer")
    normalized = normalize_records(validation.valid_records, "customer")

    result = detect_exact_duplicates(normalized, "customer")

    assert result.detection_summary.duplicate_group_count == 1
    assert result.duplicate_groups[0].source_record_ids == ("C1", "C2")


def test_detection_entities_are_separate_and_checked() -> None:
    with pytest.raises(ValueError, match="entity type"):
        detect_exact_duplicates([_product("P1")], "customer")
