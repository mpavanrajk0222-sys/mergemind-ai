from datetime import datetime, timezone
import unicodedata

import pytest

from src.ingestion import IngestedRecord
from src.normalization import (
    normalize_address,
    normalize_brand,
    normalize_category,
    normalize_city,
    normalize_customer,
    normalize_email,
    normalize_name,
    normalize_phone,
    normalize_product,
    normalize_product_name,
    normalize_records,
    normalize_sku,
)
from src.validation import validate_records


INGESTED_AT = datetime(2025, 5, 6, 7, 8, 9, tzinfo=timezone.utc)


def _record(fields: dict, entity_type: str = "customer") -> IngestedRecord:
    return IngestedRecord(fields, entity_type, "sample.csv", 7, "csv", INGESTED_AT)


def _customer(**overrides) -> IngestedRecord:
    fields = {
        "customer_id": "CUS-7",
        "name": "  Ada   LOVELACE, Jr. ",
        "email": "Ada.Lovelace@Example.TEST",
        "phone": "+1 (415) 555-0100",
        "city": "  San   José ",
        "address": "  12,  Main   St. ",
        "updated_at": "2025-05-06T07:08:09Z",
        "source_system": "crm",
    }
    fields.update(overrides)
    return _record(fields)


def _product(**overrides) -> IngestedRecord:
    fields = {
        "product_id": "PRD-7",
        "product_name": "  Premium   Café Mug ",
        "sku": " AB- 001 / X ",
        "brand": " NorthStar  Co. ",
        "category": " Home &   Kitchen ",
        "price": "12.50",
        "updated_at": "2025-05-06T07:08:09Z",
        "source_system": "catalog",
    }
    fields.update(overrides)
    return _record(fields, "product")


def test_customer_name_email_phone_city_and_address_normalization() -> None:
    source = _customer()
    normalized = normalize_customer(source)

    assert normalized.normalized_fields == {
        "name": "ada lovelace, jr.",
        "email": "ada.lovelace@example.test",
        "phone": "+14155550100",
        "city": "san josé",
        "address": "12, main st.",
    }
    assert normalize_name("  Ana   María O'Neil-Smith! ") == "ana maría o'neil-smith!"
    assert normalize_email("  User+Tag@EXAMPLE.COM  ") == "user+tag@example.com"
    assert normalize_phone("+44 (0)20-1234.5678") == "+4402012345678"
    assert normalize_city("  New   York  ") == "new york"
    assert normalize_address("  9-A, King’s   Road ") == "9-a, king’s road"


def test_product_name_sku_brand_and_category_normalization() -> None:
    normalized = normalize_product(_product())

    assert normalized.normalized_fields == {
        "product_name": "premium café mug",
        "sku": "ab- 001 / x",
        "brand": "northstar co.",
        "category": "home & kitchen",
    }
    assert normalize_product_name("  Steel   Bottle - 20oz ") == "steel bottle - 20oz"
    assert normalize_sku(" SKU--A/01  ") == "sku--a/01"
    assert normalize_brand("  O'Neil & Sons  ") == "o'neil & sons"
    assert normalize_category("  Home / Garden ") == "home / garden"


def test_unicode_is_normalized_conservatively_and_whitespace_collapsed() -> None:
    decomposed = unicodedata.normalize("NFD", "José")

    assert normalize_name(decomposed) == "josé"
    # NFC is conservative: compatibility characters are retained, not folded.
    assert normalize_city("ＡＢＣ") == "ａｂｃ"
    assert normalize_address("A\t\n  Street") == "a street"


@pytest.mark.parametrize("value", [None, "", "   ", "\t\n"])
def test_optional_null_and_empty_values_normalize_to_none(value: str | None) -> None:
    customer = normalize_customer(_customer(email=value, phone=value, city=value, address=value))
    product = normalize_product(_product(sku=value, brand=value, category=value))

    assert customer.normalized_fields["email"] is None
    assert customer.normalized_fields["phone"] is None
    assert customer.normalized_fields["city"] is None
    assert customer.normalized_fields["address"] is None
    assert product.normalized_fields["sku"] is None
    assert product.normalized_fields["brand"] is None
    assert product.normalized_fields["category"] is None


def test_phone_keeps_ambiguous_values_identifiable_and_preserves_digits() -> None:
    assert normalize_phone("001 234 567 890") == "001234567890"
    assert normalize_phone("Call ext 123") == "call ext 123"
    assert normalize_phone("+1 800 FLOWERS") == "+1 800 flowers"


def test_normalization_keeps_original_fields_and_ingestion_metadata() -> None:
    source = _customer()
    original_fields = source.source_fields.copy()

    normalized = normalize_customer(source)

    assert normalized.source_fields == original_fields
    assert normalized.source_fields is not source.source_fields
    assert source.source_fields == original_fields
    assert normalized.entity_type == source.entity_type
    assert normalized.source_file == source.source_file
    assert normalized.source_record_number == source.source_record_number
    assert normalized.source_format == source.source_format
    assert normalized.ingested_at is source.ingested_at
    assert normalized.source_fields["name"] != normalized.normalized_fields["name"]


def test_same_input_produces_deterministic_output() -> None:
    record = _customer()

    assert normalize_customer(record) == normalize_customer(record)
    assert normalize_product(_product()) == normalize_product(_product())


def test_customer_and_product_records_are_routed_independently() -> None:
    customer = _customer()
    product = _product()

    normalized_customers = normalize_records([customer], "customer")
    normalized_products = normalize_records([product], "product")

    assert normalized_customers[0].entity_type == "customer"
    assert normalized_customers[0].normalized_fields["name"] == "ada lovelace, jr."
    assert normalized_products[0].entity_type == "product"
    assert normalized_products[0].normalized_fields["product_name"] == "premium café mug"
    with pytest.raises(ValueError, match="Cannot normalize"):
        normalize_customer(product)


def test_normalize_records_integrates_with_validation_valid_records() -> None:
    source_customer = _customer()
    source_product = _product()
    validated_customer = validate_records([source_customer], "customer")
    validated_product = validate_records([source_product], "product")

    customers = normalize_records(validated_customer.valid_records, "customer")
    products = normalize_records(validated_product.valid_records, "product")

    assert len(customers) == len(products) == 1
    assert customers[0].normalized_fields["email"] == "ada.lovelace@example.test"
    assert products[0].normalized_fields["sku"] == "ab- 001 / x"


def test_missing_optional_fields_and_unknown_comparison_values_are_safe() -> None:
    customer = _customer(email=None, phone=None, city=None, address=None)
    product = _product(sku=None, brand=None, category=None)

    normalized_customer = normalize_customer(customer)
    assert normalized_customer.normalized_fields["name"] == "ada lovelace, jr."
    assert all(normalized_customer.normalized_fields[field] is None for field in ("email", "phone", "city", "address"))
    assert normalize_product(product).normalized_fields["sku"] is None
