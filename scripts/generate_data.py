"""Generate reproducible synthetic MergeMind AI source data and labels.

The generator is scenario-driven rather than statistically representative:
duplicate and invalid rates are deliberately injected for test coverage.
Names, source systems, addresses, prices, and timestamps are synthetic, and
contact details use reserved/example values. These files are useful for
functional tests, not for estimating real-world duplicate prevalence or
production matching accuracy.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA, EntitySchema


DEFAULT_SEED = 42
DEFAULT_RECORD_COUNT = 1_000
MIN_RECORD_COUNT = 6

CUSTOMER_FIRST_NAMES = (
    "Avery", "Blake", "Casey", "Devon", "Elliot", "Finley", "Gray", "Harper",
    "Jordan", "Kai", "Logan", "Morgan", "Parker", "Quinn", "Riley", "Sage",
    "Taylor", "Uma", "Val", "Willow",
)
CUSTOMER_LAST_NAMES = (
    "Adams", "Bennett", "Carter", "Diaz", "Ellis", "Foster", "Garcia", "Hayes",
    "Ibrahim", "Jones", "Kim", "Lopez", "Miller", "Nguyen", "Owens", "Patel",
    "Reed", "Singh", "Turner", "Walker",
)
PRODUCT_BASE_NAMES = (
    "Cedar Blend Coffee", "Summit Trail Bottle", "Harbor Canvas Tote",
    "Meadow Ceramic Mug", "Atlas Desk Lamp", "Willow Knit Throw",
    "Ridge Running Jacket", "Orchard Storage Basket", "Cove Travel Pouch",
    "Juniper Kitchen Scale", "Field Notes Journal", "Lumen Reading Light",
)
STREETS = ("Maple", "Oak", "Cedar", "Pine", "Willow", "Lake", "Hill", "Park")
CITIES = (
    "Portland", "Madison", "Raleigh", "Denver", "Austin", "Albany", "Salem",
    "Boise", "Richmond", "Tucson", "Boulder", "Columbus",
)
SOURCE_SYSTEMS = ("crm_north", "crm_south", "retail_portal", "partner_feed")
PRODUCT_CATEGORIES = ("Beverage", "Home", "Outdoor", "Accessories", "Kitchen", "Office")
PRODUCT_BRANDS = ("Northstar", "Fieldhouse", "Cedar & Co.", "Summit Works", "Harborline")

CUSTOMER_LABEL_COLUMNS = ("record_id", "expected_match_type", "expected_group_id")
PRODUCT_LABEL_COLUMNS = CUSTOMER_LABEL_COLUMNS


def _timestamp(rng: random.Random) -> str:
    """Return a stable, realistic UTC ISO-8601 timestamp."""

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    value = start + timedelta(seconds=rng.randrange(0, 365 * 24 * 60 * 60 * 2))
    return value.isoformat(timespec="seconds")


def _counts(record_count: int) -> tuple[int, int, int, int]:
    """Return unique, exact-row, probable-row, and invalid-row counts."""

    invalid_count = max(1, record_count // 100)
    exact_group_count = max(1, record_count // 100)
    probable_group_count = max(1, record_count // 100)
    # The minimum is chosen so even the smallest accepted dataset has one
    # unique row, one exact pair, one probable pair, and one invalid row.
    while invalid_count + 2 * exact_group_count + 2 * probable_group_count > record_count - 1:
        if probable_group_count > 1:
            probable_group_count -= 1
        elif exact_group_count > 1:
            exact_group_count -= 1
        else:
            invalid_count -= 1
    unique_count = record_count - invalid_count - 2 * exact_group_count - 2 * probable_group_count
    return unique_count, exact_group_count, probable_group_count, invalid_count


def _customer_record(record_number: int, rng: random.Random) -> dict[str, Any]:
    first = rng.choice(CUSTOMER_FIRST_NAMES)
    last = rng.choice(CUSTOMER_LAST_NAMES)
    record_id = f"CUS-{record_number:08d}"
    return {
        "customer_id": record_id,
        "name": f"{first} {last} {record_number:06d}",
        "email": f"{first.lower()}.{last.lower()}.{record_number:06d}@example.test",
        "phone": f"+1-202-{record_number:07d}",
        "city": rng.choice(CITIES),
        "address": f"{100 + record_number} {rng.choice(STREETS)} Street",
        "updated_at": _timestamp(rng),
        "source_system": rng.choice(SOURCE_SYSTEMS),
    }


def _product_record(record_number: int, rng: random.Random) -> dict[str, Any]:
    record_id = f"PRD-{record_number:08d}"
    sku = None if record_number % 13 == 0 else f"SKU-{record_number:07d}"
    brand = None if record_number % 17 == 0 else rng.choice(PRODUCT_BRANDS)
    return {
        "product_id": record_id,
        "product_name": f"{rng.choice(PRODUCT_BASE_NAMES)} {record_number:06d}",
        "sku": sku,
        "brand": brand,
        "category": rng.choice(PRODUCT_CATEGORIES),
        "price": f"{Decimal(rng.randrange(499, 50_000)) / Decimal(100):.2f}",
        "updated_at": _timestamp(rng),
        "source_system": rng.choice(SOURCE_SYSTEMS),
    }


def _with_record_id(record: dict[str, Any], record_number: int, prefix: str) -> dict[str, Any]:
    duplicate = record.copy()
    key = "customer_id" if prefix == "CUS" else "product_id"
    duplicate[key] = f"{prefix}-{record_number:08d}"
    return duplicate


def _make_customers(record_count: int, rng: random.Random) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    unique_count, exact_groups, probable_groups, invalid_count = _counts(record_count)
    records: list[dict[str, Any]] = []
    labels: list[dict[str, str]] = []
    next_number = 1

    def add(record: dict[str, Any], match_type: str, group_id: str) -> None:
        records.append(record)
        labels.append({
            "record_id": record["customer_id"],
            "expected_match_type": match_type,
            "expected_group_id": group_id,
        })

    for index in range(unique_count):
        record = _customer_record(next_number, rng)
        # Guarantee optional-field null examples without making them invalid.
        if index == 0:
            record["email"] = None
        if index == 1:
            record["phone"] = None
        record_id = record["customer_id"]
        add(record, "unique", f"customer-unique-{record_id}")
        next_number += 1

    for group_number in range(1, exact_groups + 1):
        group_id = f"customer-exact-{group_number:05d}"
        base = _customer_record(next_number, rng)
        # Include a valid exact case with a missing optional email.
        if group_number == 1:
            base["email"] = None
        add(base, "exact", group_id)
        next_number += 1
        copy = _with_record_id(base, next_number, "CUS")
        copy["source_system"] = rng.choice([s for s in SOURCE_SYSTEMS if s != base["source_system"]])
        add(copy, "exact", group_id)
        next_number += 1

    for group_number in range(1, probable_groups + 1):
        group_id = f"customer-probable-{group_number:05d}"
        base = _customer_record(next_number, rng)
        add(base, "probable", group_id)
        next_number += 1
        variant = _with_record_id(base, next_number, "CUS")
        first_name, separator, remainder = base["name"].partition(" ")
        variant["name"] = f"{first_name} Q. {remainder}" if separator else f"{base['name']} Q"
        # One controlled conflicting-location case; the shared email and phone
        # preserve a strong relationship signal for future evaluation.
        if group_number == 1:
            variant["city"] = next(city for city in CITIES if city != base["city"])
            variant["address"] = f"{100 + next_number} {next(street for street in STREETS if f'{street} Street' not in base['address'])} Street"
        add(variant, "probable", group_id)
        next_number += 1

    for invalid_number in range(invalid_count):
        record = _customer_record(next_number, rng)
        missing_field = ("name", "updated_at", "source_system")[invalid_number % 3]
        record[missing_field] = None
        record_id = record["customer_id"]
        add(record, "invalid", f"customer-invalid-{record_id}")
        next_number += 1

    return records, labels


def _make_products(record_count: int, rng: random.Random) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    unique_count, exact_groups, probable_groups, invalid_count = _counts(record_count)
    records: list[dict[str, Any]] = []
    labels: list[dict[str, str]] = []
    next_number = 1

    def add(record: dict[str, Any], match_type: str, group_id: str) -> None:
        records.append(record)
        labels.append({
            "record_id": record["product_id"],
            "expected_match_type": match_type,
            "expected_group_id": group_id,
        })

    for index in range(unique_count):
        record = _product_record(next_number, rng)
        record_id = record["product_id"]
        add(record, "unique", f"product-unique-{record_id}")
        next_number += 1

    for group_number in range(1, exact_groups + 1):
        group_id = f"product-exact-{group_number:05d}"
        base = _product_record(next_number, rng)
        # Ensure the optional SKU and brand null cases appear in every output.
        if group_number == 1:
            base["sku"] = None
            base["brand"] = None
        add(base, "exact", group_id)
        next_number += 1
        copy = _with_record_id(base, next_number, "PRD")
        copy["source_system"] = rng.choice([s for s in SOURCE_SYSTEMS if s != base["source_system"]])
        add(copy, "exact", group_id)
        next_number += 1

    for group_number in range(1, probable_groups + 1):
        group_id = f"product-probable-{group_number:05d}"
        base = _product_record(next_number, rng)
        add(base, "probable", group_id)
        next_number += 1
        variant = _with_record_id(base, next_number, "PRD")
        variant["product_name"] = f"{base['product_name']} Edition"
        if group_number == 1:
            variant["price"] = f"{Decimal(base['price']) + Decimal('0.75'):.2f}"
        add(variant, "probable", group_id)
        next_number += 1

    for invalid_number in range(invalid_count):
        record = _product_record(next_number, rng)
        missing_field = ("product_name", "updated_at", "source_system")[invalid_number % 3]
        record[missing_field] = None
        record_id = record["product_id"]
        add(record, "invalid", f"product-invalid-{record_id}")
        next_number += 1

    return records, labels


def _write_csv(path: Path, fieldnames: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def _schema_columns(schema: EntitySchema) -> tuple[str, ...]:
    return tuple(field.name for field in schema.fields)


def generate_datasets(project_root: Path, record_count: int = DEFAULT_RECORD_COUNT, seed: int = DEFAULT_SEED) -> dict[str, Counter[str]]:
    """Generate customer and product inputs plus separate ground-truth labels."""

    if record_count < MIN_RECORD_COUNT:
        raise ValueError(f"record_count must be at least {MIN_RECORD_COUNT} to include every scenario category")

    root = Path(project_root)
    customer_rng = random.Random(seed)
    product_rng = random.Random(seed + 1)
    customer_records, customer_labels = _make_customers(record_count, customer_rng)
    product_records, product_labels = _make_products(record_count, product_rng)

    paths_and_rows = (
        (root / "data" / "input" / "customers.csv", _schema_columns(CUSTOMER_SCHEMA), customer_records),
        (root / "data" / "input" / "products.csv", _schema_columns(PRODUCT_SCHEMA), product_records),
        (root / "data" / "ground_truth" / "customer_labels.csv", CUSTOMER_LABEL_COLUMNS, customer_labels),
        (root / "data" / "ground_truth" / "product_labels.csv", PRODUCT_LABEL_COLUMNS, product_labels),
    )
    for path, columns, rows in paths_and_rows:
        _write_csv(path, columns, rows)

    return {
        "customers": Counter(label["expected_match_type"] for label in customer_labels),
        "products": Counter(label["expected_match_type"] for label in product_labels),
    }


def _format_counts(counts: Counter[str]) -> str:
    return ", ".join(f"{category}={counts.get(category, 0)}" for category in ("unique", "exact", "probable", "invalid"))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic MergeMind AI customer and product CSV datasets.")
    parser.add_argument(
        "--records-per-entity",
        type=int,
        default=DEFAULT_RECORD_COUNT,
        help=f"number of rows per entity (minimum {MIN_RECORD_COUNT}; default: {DEFAULT_RECORD_COUNT})",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help=f"deterministic random seed (default: {DEFAULT_SEED})")
    args = parser.parse_args(argv)
    if args.records_per_entity < MIN_RECORD_COUNT:
        parser.error(f"--records-per-entity must be at least {MIN_RECORD_COUNT}")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    distributions = generate_datasets(PROJECT_ROOT, args.records_per_entity, args.seed)
    print(f"Generated {args.records_per_entity} customer rows: {_format_counts(distributions['customers'])}")
    print(f"Generated {args.records_per_entity} product rows: {_format_counts(distributions['products'])}")
    print(f"Seed: {args.seed}")
    print("Inputs: data/input/customers.csv, data/input/products.csv")
    print("Ground truth: data/ground_truth/customer_labels.csv, data/ground_truth/product_labels.csv")


if __name__ == "__main__":
    main()
