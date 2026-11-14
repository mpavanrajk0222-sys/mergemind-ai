"""Declarative source-schema specifications for MergeMind AI.

This module describes expected fields and constraints only. It does not parse,
validate, normalize, or ingest records.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class FieldSpec:
    """Metadata for one source field."""

    name: str
    data_type: type
    required: bool
    nullable: bool
    description: str


@dataclass(frozen=True)
class EntitySchema:
    """Declarative schema metadata for one entity type."""

    entity_name: str
    fields: tuple[FieldSpec, ...]
    primary_key: str
    unique_fields: tuple[str, ...]
    constraints: tuple[str, ...]


CUSTOMER_SCHEMA = EntitySchema(
    entity_name="customer",
    fields=(
        FieldSpec("customer_id", str, True, False, "Unique source identifier."),
        FieldSpec("name", str, True, False, "Customer name as supplied by the source."),
        FieldSpec("email", str, False, True, "Optional customer email address."),
        FieldSpec("phone", str, False, True, "Optional customer phone number."),
        FieldSpec("city", str, False, True, "Optional customer city."),
        FieldSpec("address", str, False, True, "Optional customer address."),
        FieldSpec("updated_at", datetime, True, False, "Source update timestamp in ISO-8601 form."),
        FieldSpec("source_system", str, True, False, "Name or identifier of the originating system."),
    ),
    primary_key="customer_id",
    unique_fields=("customer_id",),
    constraints=(
        "customer_id is the required unique source identifier.",
        "name, updated_at, and source_system are required and non-null.",
        "email, phone, city, and address are optional and may be null.",
    ),
)


PRODUCT_SCHEMA = EntitySchema(
    entity_name="product",
    fields=(
        FieldSpec("product_id", str, True, False, "Unique source identifier."),
        FieldSpec("product_name", str, True, False, "Product name as supplied by the source."),
        FieldSpec("sku", str, False, True, "Optional stock keeping unit."),
        FieldSpec("brand", str, False, True, "Optional product brand."),
        FieldSpec("category", str, False, True, "Optional product category."),
        FieldSpec("price", Decimal, False, True, "Optional decimal price; currency and scale are unspecified."),
        FieldSpec("updated_at", datetime, True, False, "Source update timestamp in ISO-8601 form."),
        FieldSpec("source_system", str, True, False, "Name or identifier of the originating system."),
    ),
    primary_key="product_id",
    unique_fields=("product_id",),
    constraints=(
        "product_id is the required unique source identifier.",
        "product_name, updated_at, and source_system are required and non-null.",
        "sku, brand, category, and price are optional and may be null.",
    ),
)


__all__ = ("FieldSpec", "EntitySchema", "CUSTOMER_SCHEMA", "PRODUCT_SCHEMA")
