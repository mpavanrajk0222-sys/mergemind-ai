import csv
from pathlib import Path

from scripts.generate_data import generate_datasets
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as file_handle:
        reader = csv.DictReader(file_handle)
        return list(reader.fieldnames or []), list(reader)


def test_expected_files_columns_ids_and_separate_labels(tmp_path: Path) -> None:
    generate_datasets(tmp_path, record_count=30, seed=42)
    customer_path = tmp_path / "data" / "input" / "customers.csv"
    product_path = tmp_path / "data" / "input" / "products.csv"
    customer_labels_path = tmp_path / "data" / "ground_truth" / "customer_labels.csv"
    product_labels_path = tmp_path / "data" / "ground_truth" / "product_labels.csv"
    assert all(path.is_file() for path in (customer_path, product_path, customer_labels_path, product_labels_path))

    customer_columns, customers = _read_csv(customer_path)
    product_columns, products = _read_csv(product_path)
    customer_label_columns, customer_labels = _read_csv(customer_labels_path)
    product_label_columns, product_labels = _read_csv(product_labels_path)

    assert customer_columns == [field.name for field in CUSTOMER_SCHEMA.fields]
    assert product_columns == [field.name for field in PRODUCT_SCHEMA.fields]
    assert customer_label_columns == ["record_id", "expected_match_type", "expected_group_id"]
    assert product_label_columns == ["record_id", "expected_match_type", "expected_group_id"]
    assert len(customers) == len(products) == len(customer_labels) == len(product_labels) == 30
    assert len({row["customer_id"] for row in customers}) == len(customers)
    assert len({row["product_id"] for row in products}) == len(products)
    assert "expected_match_type" not in customer_columns + product_columns
    assert "expected_group_id" not in customer_columns + product_columns
    assert {row["record_id"] for row in customer_labels} == {row["customer_id"] for row in customers}
    assert {row["record_id"] for row in product_labels} == {row["product_id"] for row in products}


def test_categories_optional_nulls_and_invalid_required_values(tmp_path: Path) -> None:
    distributions = generate_datasets(tmp_path, record_count=30, seed=42)
    assert set(distributions["customers"]) == {"unique", "exact", "probable", "invalid"}
    assert set(distributions["products"]) == {"unique", "exact", "probable", "invalid"}

    _, customers = _read_csv(tmp_path / "data" / "input" / "customers.csv")
    _, products = _read_csv(tmp_path / "data" / "input" / "products.csv")
    _, customer_labels = _read_csv(tmp_path / "data" / "ground_truth" / "customer_labels.csv")
    _, product_labels = _read_csv(tmp_path / "data" / "ground_truth" / "product_labels.csv")
    customer_types = {row["record_id"]: row["expected_match_type"] for row in customer_labels}
    product_types = {row["record_id"]: row["expected_match_type"] for row in product_labels}

    assert any(not row["email"] for row in customers)
    assert any(not row["phone"] for row in customers)
    assert any(not row["sku"] for row in products)
    assert any(not row["brand"] for row in products)
    assert any(not row["name"] for row in customers if customer_types[row["customer_id"]] == "invalid")
    assert any(not row["product_name"] for row in products if product_types[row["product_id"]] == "invalid")


def test_exact_and_probable_groups_have_deliberate_variations(tmp_path: Path) -> None:
    generate_datasets(tmp_path, record_count=30, seed=42)
    _, customers = _read_csv(tmp_path / "data" / "input" / "customers.csv")
    _, products = _read_csv(tmp_path / "data" / "input" / "products.csv")
    _, customer_labels = _read_csv(tmp_path / "data" / "ground_truth" / "customer_labels.csv")
    _, product_labels = _read_csv(tmp_path / "data" / "ground_truth" / "product_labels.csv")

    customer_by_id = {row["customer_id"]: row for row in customers}
    product_by_id = {row["product_id"]: row for row in products}
    for labels, records in (
        (customer_labels, customer_by_id),
        (product_labels, product_by_id),
    ):
        groups: dict[tuple[str, str], list[dict[str, str]]] = {}
        for label in labels:
            groups.setdefault((label["expected_match_type"], label["expected_group_id"]), []).append(
                records[label["record_id"]]
            )
        exact_groups = [group for (kind, _), group in groups.items() if kind == "exact"]
        assert exact_groups and all(len(group) == 2 for group in exact_groups)
        for group in exact_groups:
            first = {key: value for key, value in group[0].items() if key not in ("customer_id", "product_id", "source_system")}
            second = {key: value for key, value in group[1].items() if key not in ("customer_id", "product_id", "source_system")}
            assert first == second
        assert any(kind == "probable" and len(group) == 2 for (kind, _), group in groups.items())

    customer_probable = next(
        group for label in customer_labels if label["expected_match_type"] == "probable"
        for group in [[customer_by_id[other["record_id"]] for other in customer_labels
                       if other["expected_group_id"] == label["expected_group_id"]]]
        if len(group) == 2 and group[0]["city"] != group[1]["city"]
    )
    assert customer_probable[0]["name"] != customer_probable[1]["name"]
    assert customer_probable[0]["address"] != customer_probable[1]["address"]

    product_probable = next(
        group for label in product_labels if label["expected_match_type"] == "probable"
        for group in [[product_by_id[other["record_id"]] for other in product_labels
                       if other["expected_group_id"] == label["expected_group_id"]]]
        if len(group) == 2 and group[0]["price"] != group[1]["price"]
    )
    assert product_probable[0]["product_name"] != product_probable[1]["product_name"]


def test_fixed_seed_reproduces_all_files(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    generate_datasets(first_root, record_count=40, seed=7)
    generate_datasets(second_root, record_count=40, seed=7)
    for relative_path in (
        "data/input/customers.csv",
        "data/input/products.csv",
        "data/ground_truth/customer_labels.csv",
        "data/ground_truth/product_labels.csv",
    ):
        assert (first_root / relative_path).read_bytes() == (second_root / relative_path).read_bytes()


def test_small_and_large_record_counts(tmp_path: Path) -> None:
    small = tmp_path / "small"
    large = tmp_path / "large"
    generate_datasets(small, record_count=6)
    generate_datasets(large, record_count=10_000)
    for root, expected_count in ((small, 6), (large, 10_000)):
        for relative_path in (
            "data/input/customers.csv",
            "data/input/products.csv",
            "data/ground_truth/customer_labels.csv",
            "data/ground_truth/product_labels.csv",
        ):
            _, rows = _read_csv(root / relative_path)
            assert len(rows) == expected_count
