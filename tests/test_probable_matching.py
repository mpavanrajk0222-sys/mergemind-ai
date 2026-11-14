from datetime import datetime, timezone

import pytest

from src.duplicate_detection import detect_exact_duplicates
from src.normalization import NormalizedRecord, normalize_records
from src.probable_matching import MatchingConfig, detect_probable_duplicates
from src.validation import validate_records


INGESTED_AT = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _customer(
    record_id: str,
    *,
    name: str = "ada lovelace",
    email: str | None = "ada@example.test",
    phone: str | None = "+14155550100",
    city: str | None = "london",
) -> NormalizedRecord:
    fields = {
        "customer_id": record_id,
        "name": name,
        "email": email,
        "phone": phone,
        "city": city,
        "address": None,
        "updated_at": "2025-01-01T00:00:00Z",
        "source_system": "crm",
    }
    return NormalizedRecord(
        fields,
        {"name": name, "email": email, "phone": phone, "city": city, "address": None},
        "customer", "customers.csv", int(record_id[1:]), "csv", INGESTED_AT,
    )


def _product(
    record_id: str,
    *,
    name: str = "wireless mouse",
    sku: str | None = "sku-1",
    brand: str | None = "logitech",
    category: str | None = "electronics",
    price: str | None = "25.00",
) -> NormalizedRecord:
    fields = {
        "product_id": record_id,
        "product_name": name,
        "sku": sku,
        "brand": brand,
        "category": category,
        "price": price,
        "updated_at": "2025-01-01T00:00:00Z",
        "source_system": "catalog",
    }
    return NormalizedRecord(
        fields,
        {"product_name": name, "sku": sku, "brand": brand, "category": category},
        "product", "products.csv", int(record_id[1:]), "csv", INGESTED_AT,
    )


def test_customer_identical_names_with_different_contacts_are_not_probable() -> None:
    records = [
        _customer("C1", email="one@example.test", phone="+14155550101"),
        _customer("C2", email="two@example.test", phone="+14155550102"),
    ]

    result = detect_probable_duplicates(records, "customer")

    assert len(result.probable_pairs) == 1
    assert result.probable_pairs[0].decision == "NO_MATCH"
    assert result.strong_probable_matches == []


@pytest.mark.parametrize("identifier", ["email", "phone"])
def test_similar_customer_name_and_shared_identifier_are_supported(identifier: str) -> None:
    first_values = {"email": None, "phone": None}
    second_values = {"email": None, "phone": None}
    first_values[identifier] = "same@example.test" if identifier == "email" else "+14155550100"
    second_values[identifier] = first_values[identifier]
    records = [
        _customer("C1", name="pavan kumar", **first_values),
        _customer("C2", name="pavan k.", **second_values),
    ]

    result = detect_probable_duplicates(records, "customer")

    assert len(result.probable_pairs) == 1
    assert result.probable_pairs[0].decision in {"STRONG_PROBABLE_MATCH", "REVIEW_REQUIRED"}
    assert identifier in result.probable_pairs[0].matching_attributes
    assert result.probable_pairs[0].similarity_scores["name"] < 100


def test_customer_shared_email_with_conflicting_phone_requires_review() -> None:
    records = [
        _customer("C1", name="rahul sharma", email="r@example.test", phone="+14155550101"),
        _customer("C2", name="rahul s.", email="r@example.test", phone="+14155550102"),
    ]

    result = detect_probable_duplicates(records, "customer")

    pair = result.probable_pairs[0]
    assert pair.decision == "REVIEW_REQUIRED"
    assert "phone" in pair.conflicting_attributes
    assert pair.overall_confidence_score > 0


def test_customer_different_names_and_identifiers_are_not_matched() -> None:
    records = [
        _customer("C1", name="jordan lee", email="one@example.test", phone="+14155550101", city="london"),
        _customer("C2", name="quinn taylor", email="two@example.test", phone="+14155550102", city="london"),
    ]

    result = detect_probable_duplicates(records, "customer")

    assert result.strong_probable_matches == []
    assert all(pair.decision == "NO_MATCH" for pair in result.probable_pairs)


@pytest.mark.parametrize("missing", ["email", "phone"])
def test_customer_missing_contact_does_not_raise_confidence(missing: str) -> None:
    first_values = {"email": "maria@example.test", "phone": "+14155550100"}
    second_values = dict(first_values)
    first_values[missing] = None
    second_values[missing] = None
    first = _customer("C1", name="maria garcia", **first_values)
    second = _customer("C2", name="maria garcia", **second_values)

    result = detect_probable_duplicates([first, second], "customer")
    assert result.probable_pairs[0].decision == "STRONG_PROBABLE_MATCH"
    assert result.probable_pairs[0].overall_confidence_score == 75.0
    assert result.probable_pairs[0].overall_confidence_score < 100.0
    assert result.probable_pairs[0].similarity_scores[missing] is None


def test_similar_customer_names_with_conflicting_cities_require_review() -> None:
    records = [
        _customer("C1", name="samantha rogers", email="same@example.test", phone=None, city="boston"),
        _customer("C2", name="samantha rodgers", email="same@example.test", phone=None, city="seattle"),
    ]

    result = detect_probable_duplicates(records, "customer")

    assert result.review_candidates
    assert "city" in result.review_candidates[0].conflicting_attributes
    assert result.strong_probable_matches == []


def test_unicode_customer_names_are_compared_deterministically() -> None:
    records = [
        _customer("C1", name="josé alvarez", email="j@example.test", phone=None),
        _customer("C2", name="jose alvarez", email="j@example.test", phone=None),
    ]

    result = detect_probable_duplicates(records, "customer")

    assert result.probable_pairs[0].similarity_scores["name"] > 80
    assert result.probable_pairs[0].decision != "NO_MATCH"


def test_customer_matching_is_deterministic_for_reordered_input() -> None:
    records = [
        _customer("C1", name="pavan kumar", email="same@example.test", phone=None),
        _customer("C2", name="pavan k.", email="same@example.test", phone=None),
        _customer("C3", name="other person", email="other@example.test", phone=None),
    ]

    assert detect_probable_duplicates(records, "customer") == detect_probable_duplicates(list(reversed(records)), "customer")


def test_product_similar_names_same_brand_use_multiple_signals() -> None:
    records = [
        _product("P1", name="wireless mouse", sku="sku-a", price="25.00"),
        _product("P2", name="wireless mousse", sku="sku-b", price="25.25"),
    ]

    result = detect_probable_duplicates(records, "product")

    assert len(result.probable_pairs) == 1
    pair = result.probable_pairs[0]
    assert pair.similarity_scores["product_name"] > 80
    assert pair.similarity_scores["brand"] == 100
    assert pair.decision == "REVIEW_REQUIRED"  # distinct non-empty SKUs block automatic matching
    assert "sku" in pair.conflicting_attributes


def test_similar_product_names_with_conflicting_brands_require_review() -> None:
    result = detect_probable_duplicates(
        [_product("P1", sku="sku-a"), _product("P2", sku="sku-b", brand="microsoft")],
        "product",
    )

    assert result.probable_pairs
    assert result.probable_pairs[0].decision != "STRONG_PROBABLE_MATCH"
    assert "brand" in result.probable_pairs[0].conflicting_attributes


def test_same_product_sku_with_conflicting_brand_requires_review() -> None:
    result = detect_probable_duplicates(
        [_product("P1"), _product("P2", brand="microsoft")], "product"
    )

    assert result.probable_pairs[0].decision == "REVIEW_REQUIRED"
    assert "brand" in result.probable_pairs[0].conflicting_attributes


def test_product_name_similarity_with_different_skus_does_not_auto_match() -> None:
    result = detect_probable_duplicates(
        [
            _product("P1", name="wireless mouse", sku="sku-a", category="accessories", price="20"),
            _product("P2", name="wireless mousse", sku="sku-b", category="accessories", brand="other", price="80"),
        ],
        "product",
    )

    assert result.strong_probable_matches == []
    assert result.probable_pairs[0].decision == "NO_MATCH"
    assert "sku" in result.probable_pairs[0].conflicting_attributes


def test_missing_product_sku_does_not_match_on_name_alone() -> None:
    result = detect_probable_duplicates(
        [
            _product("P1", sku=None, brand=None, category=None, price=None),
            _product("P2", sku=None, brand=None, category=None, price=None),
        ],
        "product",
    )

    assert result.probable_pairs == []
    assert result.matching_summary.candidate_pairs_evaluated == 0


def test_product_price_difference_is_scored_and_reduces_confidence() -> None:
    first = _product("P1", name="logitech wireless mouse", sku=None, price="20")
    similar_price = _product("P2", name="logitech wireless mouse", sku=None, price="21")
    distant_price = _product("P3", name="logitech wireless mouse", sku=None, price="80")

    near = detect_probable_duplicates([first, similar_price], "product").probable_pairs[0]
    far = detect_probable_duplicates([first, distant_price], "product").probable_pairs[0]

    assert near.similarity_scores["price"] > far.similarity_scores["price"]
    assert near.overall_confidence_score > far.overall_confidence_score


def test_product_different_names_are_kept_as_separate_pair_decisions() -> None:
    records = [
        _product("P1", name="logitech wireless mouse", sku="sku-a"),
        _product("P2", name="logitech gaming mouse", sku="sku-b"),
    ]

    result = detect_probable_duplicates(records, "product")

    assert all(len((pair.source_record_id_1, pair.source_record_id_2)) == 2 for pair in result.probable_pairs)
    assert not hasattr(result, "duplicate_groups")


def test_empty_and_single_inputs_have_no_candidate_pairs() -> None:
    assert detect_probable_duplicates([], "customer").probable_pairs == []
    assert detect_probable_duplicates([_product("P1")], "product").probable_pairs == []


def test_exact_group_members_are_excluded_from_probable_pairs() -> None:
    first, second = _customer("C1"), _customer("C2")
    exact_result = detect_exact_duplicates([first, second], "customer")

    result = detect_probable_duplicates([first, second], "customer", exact_result=exact_result)

    assert result.probable_pairs == []
    assert result.matching_summary.exact_group_records_excluded == 2


def test_no_duplicate_pair_output_and_no_transitive_clusters() -> None:
    records = [
        _customer("C1", name="jonathan smith", email="shared@example.test", phone=None, city="city one"),
        _customer("C2", name="jonathan smyth", email="shared@example.test", phone="+14155550100", city="city two"),
        _customer("C3", name="jonathan smyth", email="other@example.test", phone="+14155550100", city="city three"),
    ]

    result = detect_probable_duplicates(records, "customer")
    pair_ids = [(pair.source_record_id_1, pair.source_record_id_2) for pair in result.probable_pairs]

    assert len(pair_ids) == len(set(pair_ids))
    assert set(pair_ids) == {("C1", "C2"), ("C2", "C3")}
    assert all(len(pair) == 2 for pair in pair_ids)


def test_original_normalized_input_is_not_modified() -> None:
    records = [_customer("C1"), _customer("C2", name="ada love lace")]
    original_source = [record.source_fields.copy() for record in records]
    original_normalized = [record.normalized_fields.copy() for record in records]

    detect_probable_duplicates(records, "customer")

    assert [record.source_fields for record in records] == original_source
    assert [record.normalized_fields for record in records] == original_normalized


def test_scores_stay_within_consistent_numeric_boundaries() -> None:
    results = [
        detect_probable_duplicates([_customer("C1"), _customer("C2", name="ada lovelace jr")], "customer"),
        detect_probable_duplicates([_product("P1"), _product("P2", sku="sku-2")], "product"),
    ]
    for result in results:
        for pair in result.probable_pairs:
            assert 0.0 <= pair.overall_confidence_score <= 100.0
            assert all(score is None or 0.0 <= score <= 100.0 for score in pair.similarity_scores.values())


def test_configurable_thresholds_and_blocking_rules() -> None:
    records = [
        _customer("C1", name="pavan kumar", email="same@example.test", phone=None),
        _customer("C2", name="pavan k.", email="same@example.test", phone=None),
    ]
    default = detect_probable_duplicates(records, "customer")
    tuned = detect_probable_duplicates(
        records,
        "customer",
        config=MatchingConfig(name_similarity_threshold=70, overall_confidence_threshold=55, review_threshold=50),
    )
    blocked = detect_probable_duplicates(
        records,
        "customer",
        config=MatchingConfig(customer_blocking_rules=("phone",)),
    )

    assert default.probable_pairs[0].decision == "REVIEW_REQUIRED"
    assert tuned.probable_pairs[0].decision == "STRONG_PROBABLE_MATCH"
    assert blocked.probable_pairs == []
    with pytest.raises(ValueError, match="threshold"):
        MatchingConfig(name_similarity_threshold=101)


def test_validation_normalization_exact_and_probable_modules_integrate() -> None:
    raw = [
        {
            "customer_id": "C1", "name": "Pavan Kumar", "email": "pavan@example.test", "phone": "",
            "city": "Delhi", "address": "1 Main St", "updated_at": "2025-01-01T00:00:00Z", "source_system": "a",
        },
        {
            "customer_id": "C2", "name": "Pavan K.", "email": "pavan@example.test", "phone": "",
            "city": "Delhi", "address": "1 Main Street", "updated_at": "2025-01-01T00:00:00Z", "source_system": "b",
        },
    ]
    from src.ingestion import IngestedRecord

    records = [IngestedRecord(row, "customer", "input.json", n, "json", INGESTED_AT) for n, row in enumerate(raw, 1)]
    validated = validate_records(records, "customer")
    normalized = normalize_records(validated.valid_records, "customer")
    exact = detect_exact_duplicates(normalized, "customer")
    probable = detect_probable_duplicates(normalized, "customer", exact_result=exact)

    assert len(probable.probable_pairs) == 1
    assert probable.probable_pairs[0].decision in {"STRONG_PROBABLE_MATCH", "REVIEW_REQUIRED"}


def test_blocking_reduces_candidate_comparisons_against_all_pairs() -> None:
    records = []
    for number in range(300):
        prefix = f"{number:04d}"
        records.append(_customer(
            f"C{number + 1}",
            name=f"person{prefix} surname{prefix}",
            email=f"person{number}@example.test",
            phone=f"+1415{number:07d}",
            city=f"city-{number}",
        ))
    # Add four likely pair candidates through a shared email block.
    for number in range(4):
        index = number * 2
        records[index] = _customer(
            f"C{index + 1}", name=f"person{index:04d} surname{index:04d}",
            email=f"shared-{number}@example.test", phone=None, city=f"city-{index}",
        )
        records[index + 1] = _customer(
            f"C{index + 2}", name=f"person{index:04d} surnam{index:04d}",
            email=f"shared-{number}@example.test", phone=None, city=f"city-{index + 1}",
        )

    summary = detect_probable_duplicates(records, "customer").matching_summary

    assert summary.total_possible_pairs == 300 * 299 // 2
    assert summary.candidate_pairs_evaluated <= 4
    assert summary.candidate_reduction_percent > 99.9
