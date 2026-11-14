"""Create comparison-friendly values without changing ingested source fields."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Literal

from src.ingestion import IngestedRecord


EntityType = Literal["customer", "product"]


@dataclass(frozen=True)
class NormalizedRecord:
    """A source record paired with separate normalized comparison fields."""

    source_fields: dict[str, Any]
    normalized_fields: dict[str, str | None]
    entity_type: EntityType
    source_file: str
    source_record_number: int
    source_format: str
    ingested_at: datetime


def _normalize_text(value: str | None) -> str | None:
    """NFC-normalize, trim, collapse whitespace, and casefold text."""

    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Text normalizers expect str or None, received {type(value).__name__}.")
    value = unicodedata.normalize("NFC", value)
    normalized = " ".join(value.split())
    return unicodedata.normalize("NFC", normalized.casefold()) if normalized else None


def normalize_name(value: str | None) -> str | None:
    """Normalize a customer name while preserving punctuation and symbols."""

    return _normalize_text(value)


def normalize_email(value: str | None) -> str | None:
    """Normalize email casing and surrounding/repeated whitespace for comparison."""

    return _normalize_text(value)


def normalize_phone(value: str | None) -> str | None:
    """Remove common phone formatting while retaining meaningful characters.

    A leading plus and every digit (including leading zeroes) are preserved.
    Values containing letters or unfamiliar punctuation are returned as
    normalized text instead of having potentially meaningful content stripped.
    """

    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Phone normalizer expects str or None, received {type(value).__name__}.")
    value = unicodedata.normalize("NFC", value).strip()
    if not value:
        return None

    formatting_characters = frozenset(" \t\r\n().-/")
    normalized_characters: list[str] = []
    for index, character in enumerate(value):
        if character.isdigit():
            normalized_characters.append(character)
        elif character == "+" and index == 0:
            normalized_characters.append(character)
        elif character in formatting_characters:
            continue
        else:
            # Preserve extensions and ambiguous values as recognizable text.
            return _normalize_text(value)

    normalized = "".join(normalized_characters)
    return normalized if any(character.isdigit() for character in normalized) else _normalize_text(value)


def normalize_city(value: str | None) -> str | None:
    """Normalize city casing and whitespace without dropping location text."""

    return _normalize_text(value)


def normalize_address(value: str | None) -> str | None:
    """Normalize address casing and whitespace while retaining punctuation."""

    return _normalize_text(value)


def normalize_product_name(value: str | None) -> str | None:
    """Normalize product-name casing and whitespace without fuzzy changes."""

    return _normalize_text(value)


def normalize_sku(value: str | None) -> str | None:
    """Normalize SKU casing and whitespace while retaining all SKU symbols."""

    return _normalize_text(value)


def normalize_brand(value: str | None) -> str | None:
    """Normalize brand casing and whitespace."""

    return _normalize_text(value)


def normalize_category(value: str | None) -> str | None:
    """Normalize category casing and whitespace."""

    return _normalize_text(value)


def _require_entity(record: IngestedRecord, expected: EntityType) -> None:
    if not isinstance(record, IngestedRecord):
        raise TypeError("Normalization expects IngestedRecord values from the validation result's valid_records.")
    if record.entity_type != expected:
        raise ValueError(f"Cannot normalize {record.entity_type!r} record as {expected!r}.")


def normalize_customer(record: IngestedRecord) -> NormalizedRecord:
    """Return normalized customer comparison values and original provenance."""

    _require_entity(record, "customer")
    source_fields = dict(record.source_fields)
    normalized_fields = {
        "name": normalize_name(source_fields.get("name")),
        "email": normalize_email(source_fields.get("email")),
        "phone": normalize_phone(source_fields.get("phone")),
        "city": normalize_city(source_fields.get("city")),
        "address": normalize_address(source_fields.get("address")),
    }
    return NormalizedRecord(
        source_fields=source_fields,
        normalized_fields=normalized_fields,
        entity_type=record.entity_type,
        source_file=record.source_file,
        source_record_number=record.source_record_number,
        source_format=record.source_format,
        ingested_at=record.ingested_at,
    )


def normalize_product(record: IngestedRecord) -> NormalizedRecord:
    """Return normalized product comparison values and original provenance."""

    _require_entity(record, "product")
    source_fields = dict(record.source_fields)
    normalized_fields = {
        "product_name": normalize_product_name(source_fields.get("product_name")),
        "sku": normalize_sku(source_fields.get("sku")),
        "brand": normalize_brand(source_fields.get("brand")),
        "category": normalize_category(source_fields.get("category")),
    }
    return NormalizedRecord(
        source_fields=source_fields,
        normalized_fields=normalized_fields,
        entity_type=record.entity_type,
        source_file=record.source_file,
        source_record_number=record.source_record_number,
        source_format=record.source_format,
        ingested_at=record.ingested_at,
    )


def normalize_records(records: Iterable[IngestedRecord], entity_type: str) -> list[NormalizedRecord]:
    """Normalize a validated-record collection using the requested entity schema.

    Pass ``ValidationResult.valid_records`` from ``src.validation``. Rejected
    records are intentionally not normalized by this module.
    """

    normalized_entity = entity_type.lower() if isinstance(entity_type, str) else ""
    if normalized_entity not in ("customer", "product"):
        raise ValueError(f"Unsupported entity type {entity_type!r}; expected 'customer' or 'product'.")
    normalizer = normalize_customer if normalized_entity == "customer" else normalize_product
    return [normalizer(record) for record in records]


__all__ = (
    "NormalizedRecord",
    "normalize_name",
    "normalize_email",
    "normalize_phone",
    "normalize_city",
    "normalize_address",
    "normalize_product_name",
    "normalize_sku",
    "normalize_brand",
    "normalize_category",
    "normalize_customer",
    "normalize_product",
    "normalize_records",
)
