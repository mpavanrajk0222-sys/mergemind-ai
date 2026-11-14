"""Validate ingested records against the declarative source schemas.

Validation leaves source fields and ingestion metadata unchanged. By default,
unexpected fields are accepted and reported as schema drift; pass
``strict_unexpected_fields=True`` to reject records containing them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Literal

from src.ingestion import IngestedRecord
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA, EntitySchema, FieldSpec


EntityType = Literal["customer", "product"]

_SCHEMAS: dict[str, EntitySchema] = {
    "customer": CUSTOMER_SCHEMA,
    "product": PRODUCT_SCHEMA,
}
_EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_PHONE_CHARACTERS_PATTERN = re.compile(r"^\+?[0-9\s().-]+$")


@dataclass(frozen=True)
class ValidationIssue:
    """One validation error associated with a source record and field."""

    record_id: Any | None
    field_name: str
    error_code: str
    message: str


@dataclass(frozen=True)
class SchemaDrift:
    """Unexpected source fields observed on one record."""

    record_id: Any | None
    source_file: str
    source_record_number: int
    unexpected_fields: tuple[str, ...]


@dataclass(frozen=True)
class RejectedRecord:
    """Original ingested record plus all validation errors found for it."""

    source_fields: dict[str, Any]
    entity_type: EntityType
    source_file: str
    source_record_number: int
    source_format: str
    ingested_at: datetime
    errors: tuple[ValidationIssue, ...]


@dataclass(frozen=True)
class ValidationSummary:
    """Counts for a single validation batch."""

    entity_type: EntityType
    total_records: int
    valid_records: int
    rejected_records: int
    error_counts_by_code: dict[str, int]
    schema_drift_records: int
    unexpected_field_counts: dict[str, int]


@dataclass(frozen=True)
class ValidationResult:
    """Separated accepted and rejected records with summary and drift details."""

    valid_records: list[IngestedRecord]
    rejected_records: list[RejectedRecord]
    validation_summary: ValidationSummary
    schema_drift: list[SchemaDrift]


def _normalize_entity_type(entity_type: str) -> EntityType:
    normalized = entity_type.lower() if isinstance(entity_type, str) else ""
    if normalized not in _SCHEMAS:
        raise ValueError(f"Unsupported entity type {entity_type!r}; expected 'customer' or 'product'.")
    return normalized  # type: ignore[return-value]


def _issue(record_id: Any | None, field_name: str, code: str, message: str) -> ValidationIssue:
    return ValidationIssue(record_id, field_name, code, message)


def _timestamp_is_valid(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if not isinstance(value, str) or ("T" not in value and "t" not in value):
        return False
    candidate = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return True


def _decimal_value(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _validate_field(
    record_id: Any | None,
    field: FieldSpec,
    fields: dict[str, Any],
    entity_type: EntityType,
) -> list[ValidationIssue]:
    name = field.name
    if name not in fields or fields[name] is None:
        if field.required:
            return [_issue(record_id, name, "MISSING_REQUIRED_FIELD", f"Required field '{name}' is missing or null.")]
        return []

    value = fields[name]
    if isinstance(value, str) and value.strip() == "":
        if field.required:
            code = "EMPTY_REQUIRED_FIELD" if value == "" else "WHITESPACE_REQUIRED_FIELD"
            return [_issue(record_id, name, code, f"Required field '{name}' must not be empty or whitespace-only.")]
        # Optional blank strings are treated as null for validation only.
        return []

    if field.data_type is str:
        if not isinstance(value, str):
            return [_issue(record_id, name, "INVALID_TYPE", f"Field '{name}' must be a string.")]
        if entity_type == "customer" and name == "email" and not _EMAIL_PATTERN.fullmatch(value):
            return [_issue(record_id, name, "INVALID_EMAIL", "Email must have a basic local@domain.tld structure.")]
        if entity_type == "customer" and name == "phone":
            digits = sum(character.isdigit() for character in value)
            if not _PHONE_CHARACTERS_PATTERN.fullmatch(value) or not 7 <= digits <= 15:
                return [_issue(record_id, name, "INVALID_PHONE", "Phone must contain 7 to 15 digits and only common international separators.")]
        return []

    if field.data_type is datetime:
        if not _timestamp_is_valid(value):
            return [_issue(record_id, name, "INVALID_TIMESTAMP", f"Field '{name}' must be a valid ISO-8601 timestamp.")]
        return []

    if field.data_type is Decimal:
        decimal_value = _decimal_value(value)
        if decimal_value is None or not decimal_value.is_finite():
            return [_issue(record_id, name, "INVALID_DECIMAL", f"Field '{name}' must be a finite numeric decimal.")]
        if decimal_value < 0:
            return [_issue(record_id, name, "NEGATIVE_PRICE", "Product price must be non-negative.")]
        return []

    if not isinstance(value, field.data_type):
        return [_issue(record_id, name, "INVALID_TYPE", f"Field '{name}' has an unexpected data type.")]
    return []


def _record_id(record: IngestedRecord, schema: EntitySchema) -> Any | None:
    return record.source_fields.get(schema.primary_key)


def validate_records(
    records: Iterable[IngestedRecord],
    entity_type: str,
    *,
    strict_unexpected_fields: bool = False,
) -> ValidationResult:
    """Validate a batch of ingested records without changing source values.

    In permissive mode (the default), unexpected fields are retained and
    accepted, while each affected record is listed in ``schema_drift`` and
    counted in the summary. Strict mode reports each unexpected field as an
    error, causing the affected record to be rejected.

    For duplicate source primary keys, the first occurrence claims that key;
    subsequent occurrences receive ``DUPLICATE_SOURCE_ID`` and are rejected.
    This is a source-key quality rule, not business-level entity matching.
    """

    normalized_entity = _normalize_entity_type(entity_type)
    schema = _SCHEMAS[normalized_entity]
    known_fields = {field.name for field in schema.fields}
    source_records = list(records)
    valid_records: list[IngestedRecord] = []
    rejected_records: list[RejectedRecord] = []
    drift_records: list[SchemaDrift] = []
    errors_by_code: dict[str, int] = {}
    unexpected_field_counts: dict[str, int] = {}
    seen_source_ids: dict[str, tuple[str, int]] = {}

    for batch_number, record in enumerate(source_records, start=1):
        if not isinstance(record, IngestedRecord):
            raise TypeError(f"Input record {batch_number} must be an IngestedRecord instance.")
        if record.entity_type != normalized_entity:
            raise ValueError(
                f"Input record {batch_number} has entity type {record.entity_type!r}; "
                f"validation batch is for {normalized_entity!r}."
            )

        record_id = _record_id(record, schema)
        errors: list[ValidationIssue] = []
        for field in schema.fields:
            errors.extend(_validate_field(record_id, field, record.source_fields, normalized_entity))

        primary_key = schema.primary_key
        primary_value = record.source_fields.get(primary_key)
        if isinstance(primary_value, str) and primary_value.strip():
            if primary_value in seen_source_ids:
                first_source_file, first_record_number = seen_source_ids[primary_value]
                errors.append(_issue(
                    record_id,
                    primary_key,
                    "DUPLICATE_SOURCE_ID",
                    f"Source identifier {primary_value!r} duplicates record {first_record_number} in "
                    f"{first_source_file!r}; only its first occurrence can be accepted.",
                ))
            else:
                seen_source_ids[primary_value] = (record.source_file, record.source_record_number)

        unexpected_fields = tuple(name for name in record.source_fields if name not in known_fields)
        if unexpected_fields:
            drift_records.append(SchemaDrift(
                record_id=record_id,
                source_file=record.source_file,
                source_record_number=record.source_record_number,
                unexpected_fields=unexpected_fields,
            ))
            for name in unexpected_fields:
                unexpected_field_counts[name] = unexpected_field_counts.get(name, 0) + 1
                if strict_unexpected_fields:
                    errors.append(_issue(
                        record_id,
                        name,
                        "UNEXPECTED_FIELD",
                        f"Field '{name}' is not defined by the {normalized_entity} schema.",
                    ))

        if errors:
            for validation_error in errors:
                errors_by_code[validation_error.error_code] = errors_by_code.get(validation_error.error_code, 0) + 1
            rejected_records.append(RejectedRecord(
                source_fields=record.source_fields,
                entity_type=record.entity_type,
                source_file=record.source_file,
                source_record_number=record.source_record_number,
                source_format=record.source_format,
                ingested_at=record.ingested_at,
                errors=tuple(errors),
            ))
        else:
            valid_records.append(record)

    summary = ValidationSummary(
        entity_type=normalized_entity,
        total_records=len(source_records),
        valid_records=len(valid_records),
        rejected_records=len(rejected_records),
        error_counts_by_code=errors_by_code,
        schema_drift_records=len(drift_records),
        unexpected_field_counts=unexpected_field_counts,
    )
    return ValidationResult(valid_records, rejected_records, summary, drift_records)


__all__ = (
    "ValidationIssue",
    "SchemaDrift",
    "RejectedRecord",
    "ValidationSummary",
    "ValidationResult",
    "validate_records",
)
