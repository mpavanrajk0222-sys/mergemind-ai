from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.ingestion import IngestedRecord
from src.validation import validate_records


INGESTED_AT = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def _record(fields: dict, entity_type: str = "customer", number: int = 1) -> IngestedRecord:
    return IngestedRecord(
        source_fields=fields,
        entity_type=entity_type,
        source_file=f"{entity_type}s.csv",
        source_record_number=number,
        source_format="csv",
        ingested_at=INGESTED_AT,
    )


def _customer(**overrides) -> IngestedRecord:
    fields = {
        "customer_id": "C-1",
        "name": "Avery Adams",
        "email": "avery@example.test",
        "phone": "+1 (415) 555-0100",
        "city": "Portland",
        "address": "10 Maple Street",
        "updated_at": "2025-01-02T03:04:05Z",
        "source_system": "crm_a",
    }
    fields.update(overrides)
    return _record(fields)


def _product(**overrides) -> IngestedRecord:
    fields = {
        "product_id": "P-1",
        "product_name": "Cedar Mug",
        "sku": "SKU-1",
        "brand": "Northstar",
        "category": "Home",
        "price": "19.95",
        "updated_at": "2025-01-02T03:04:05+00:00",
        "source_system": "catalog_a",
    }
    fields.update(overrides)
    return _record(fields, entity_type="product")


def _codes(result) -> set[str]:
    return {issue.error_code for rejected in result.rejected_records for issue in rejected.errors}


def test_valid_customer_record_is_accepted_unchanged() -> None:
    source = _customer()
    original_fields = source.source_fields.copy()

    result = validate_records([source], "customer")

    assert result.valid_records == [source]
    assert result.rejected_records == []
    assert result.validation_summary.total_records == 1
    assert result.validation_summary.valid_records == 1
    assert result.validation_summary.rejected_records == 0
    assert source.source_fields == original_fields


def test_valid_product_price_is_checked_as_decimal_without_rewriting() -> None:
    source = _product(price="19.95")

    result = validate_records([source], "product")

    assert result.valid_records == [source]
    assert source.source_fields["price"] == "19.95"
    assert Decimal(source.source_fields["price"]) == Decimal("19.95")


@pytest.mark.parametrize(
    "record,field,code",
    [
        (_customer(customer_id=None), "customer_id", "MISSING_REQUIRED_FIELD"),
        (_customer(name=None), "name", "MISSING_REQUIRED_FIELD"),
        (_product(product_id=None), "product_id", "MISSING_REQUIRED_FIELD"),
        (_product(product_name=None), "product_name", "MISSING_REQUIRED_FIELD"),
        (_customer(email="not-an-email"), "email", "INVALID_EMAIL"),
        (_customer(phone="123"), "phone", "INVALID_PHONE"),
        (_product(price="not-a-price"), "price", "INVALID_DECIMAL"),
        (_product(price="-0.01"), "price", "NEGATIVE_PRICE"),
        (_customer(updated_at="yesterday"), "updated_at", "INVALID_TIMESTAMP"),
        (_product(source_system=""), "source_system", "EMPTY_REQUIRED_FIELD"),
    ],
)
def test_invalid_field_values_are_rejected(record: IngestedRecord, field: str, code: str) -> None:
    result = validate_records([record], record.entity_type)

    assert result.valid_records == []
    assert len(result.rejected_records) == 1
    assert code in _codes(result)
    assert any(issue.field_name == field for issue in result.rejected_records[0].errors)


def test_missing_optional_fields_and_empty_optional_values_are_accepted() -> None:
    customer = _customer(email=None, phone=" ", city="", address=None)
    product = _product(sku=None, brand="", category="  ", price=None)

    customer_result = validate_records([customer], "customer")
    product_result = validate_records([product], "product")

    assert customer_result.valid_records == [customer]
    assert product_result.valid_records == [product]
    assert customer.source_fields["phone"] == " "
    assert product.source_fields["category"] == "  "


@pytest.mark.parametrize(
    "record,field,code",
    [
        (_customer(customer_id=""), "customer_id", "EMPTY_REQUIRED_FIELD"),
        (_customer(name="  \t"), "name", "WHITESPACE_REQUIRED_FIELD"),
        (_product(product_name="  "), "product_name", "WHITESPACE_REQUIRED_FIELD"),
        (_customer(source_system=None), "source_system", "MISSING_REQUIRED_FIELD"),
    ],
)
def test_empty_and_whitespace_required_values_are_rejected(record: IngestedRecord, field: str, code: str) -> None:
    result = validate_records([record], record.entity_type)
    issue = next(issue for issue in result.rejected_records[0].errors if issue.field_name == field)
    assert issue.error_code == code


def test_duplicate_customer_source_ids_reject_second_occurrence_only() -> None:
    first = _customer()
    second = _customer(name="Different name")

    result = validate_records([first, second], "customer")

    assert result.valid_records == [first]
    assert len(result.rejected_records) == 1
    assert result.rejected_records[0].errors[0].error_code == "DUPLICATE_SOURCE_ID"
    assert result.validation_summary.error_counts_by_code == {"DUPLICATE_SOURCE_ID": 1}


def test_duplicate_product_source_ids_are_rejected() -> None:
    result = validate_records([_product(), _product(product_name="Another Mug")], "product")

    assert result.validation_summary.valid_records == 1
    assert result.validation_summary.rejected_records == 1
    assert "DUPLICATE_SOURCE_ID" in _codes(result)


def test_unexpected_fields_are_reported_but_accepted_by_default() -> None:
    source = _customer(external_note="source extension")

    result = validate_records([source], "customer")

    assert result.valid_records == [source]
    assert result.rejected_records == []
    assert result.schema_drift[0].unexpected_fields == ("external_note",)
    assert result.validation_summary.schema_drift_records == 1
    assert result.validation_summary.unexpected_field_counts == {"external_note": 1}


def test_unexpected_fields_are_rejected_in_strict_mode() -> None:
    result = validate_records([_product(import_batch="B1")], "product", strict_unexpected_fields=True)

    assert result.valid_records == []
    assert result.rejected_records[0].errors[0].error_code == "UNEXPECTED_FIELD"


def test_rejected_record_preserves_original_fields_and_all_ingestion_metadata() -> None:
    source = _customer(name=" ", external_tag="kept")
    original_fields = source.source_fields.copy()

    result = validate_records([source], "customer", strict_unexpected_fields=True)
    rejected = result.rejected_records[0]

    assert rejected.source_fields == original_fields
    assert rejected.source_file == source.source_file
    assert rejected.source_record_number == source.source_record_number
    assert rejected.entity_type == source.entity_type
    assert rejected.source_format == source.source_format
    assert rejected.ingested_at is source.ingested_at
    assert {issue.field_name for issue in rejected.errors} == {"name", "external_tag"}
    assert all(issue.record_id == "C-1" and issue.error_code and issue.message for issue in rejected.errors)
    assert source.source_fields == original_fields


def test_multiple_validation_errors_are_returned_for_one_record() -> None:
    source = _customer(customer_id="", name="", email="invalid", updated_at="bad", source_system=" ")

    result = validate_records([source], "customer")
    errors = result.rejected_records[0].errors

    assert len(errors) == 5
    assert {error.field_name for error in errors} == {"customer_id", "name", "email", "updated_at", "source_system"}


def test_empty_batch_has_entity_summary() -> None:
    result = validate_records([], "product")

    assert result.valid_records == []
    assert result.rejected_records == []
    assert result.validation_summary.entity_type == "product"
    assert result.validation_summary.total_records == 0


def test_customer_and_product_batches_are_validated_independently() -> None:
    customer_result = validate_records([_customer()], "customer")
    product_result = validate_records([_product()], "product")

    assert customer_result.validation_summary.entity_type == "customer"
    assert product_result.validation_summary.entity_type == "product"
    with pytest.raises(ValueError, match="entity type"):
        validate_records([_product()], "customer")
