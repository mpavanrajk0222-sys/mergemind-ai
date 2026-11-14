import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.ingestion import (
    FileNotFoundIngestionError,
    IngestedRecord,
    InvalidJSONLLineError,
    InvalidJSONSyntaxError,
    MalformedCSVStructureError,
    MalformedJSONStructureError,
    UnreadableFileError,
    UnsupportedEntityTypeError,
    UnsupportedFileFormatError,
    load_records,
)


def test_customer_csv_preserves_fields_and_provenance(tmp_path: Path) -> None:
    path = tmp_path / "customers.csv"
    path.write_text("customer_id,name,email\nC-1,Ada,ada@example.test\nC-2,Lin,\n", encoding="utf-8")

    records = load_records(path, "customer")

    assert len(records) == 2
    assert records[0].source_fields == {"customer_id": "C-1", "name": "Ada", "email": "ada@example.test"}
    assert records[1].source_fields["email"] == ""
    assert records[0].entity_type == "customer"
    assert records[0].source_file == "customers.csv"
    assert [record.source_record_number for record in records] == [1, 2]
    assert records[0].source_format == "csv"
    assert records[0].ingested_at.tzinfo == timezone.utc


def test_product_csv_ingestion(tmp_path: Path) -> None:
    path = tmp_path / "products.csv"
    path.write_text("product_id,product_name,price\nP-1,Desk Lamp,12.50\n", encoding="utf-8")

    record = load_records(path, "product")[0]

    assert record.entity_type == "product"
    assert record.source_fields == {"product_id": "P-1", "product_name": "Desk Lamp", "price": "12.50"}


@pytest.mark.parametrize("entity_type,record,numeric_field,numeric_type", [
    ("customer", {"customer_id": "C-1", "name": "Mira", "score": 3}, "score", int),
    ("product", {"product_id": "P-1", "product_name": "Mug", "price": 8.25}, "price", float),
])
def test_json_array_ingestion_preserves_json_types(
    tmp_path: Path, entity_type: str, record: dict, numeric_field: str, numeric_type: type
) -> None:
    path = tmp_path / f"{entity_type}.json"
    path.write_text(json.dumps([record]), encoding="utf-8")

    ingested = load_records(path, entity_type)

    assert len(ingested) == 1
    assert ingested[0].source_fields == record
    assert type(ingested[0].source_fields[numeric_field]) is numeric_type
    assert ingested[0].source_format == "json"
    assert ingested[0].source_record_number == 1


def test_jsonl_ingestion_numbers_physical_lines(tmp_path: Path) -> None:
    path = tmp_path / "products.jsonl"
    path.write_text('{"product_id":"P-1"}\n{"product_id":"P-2"}\n', encoding="utf-8")

    records = load_records(path, "product")

    assert [record.source_record_number for record in records] == [1, 2]
    assert all(record.source_format == "jsonl" for record in records)


@pytest.mark.parametrize("content", ["", "\n\r\n"])
def test_empty_csv_returns_empty_collection(tmp_path: Path, content: str) -> None:
    path = tmp_path / "empty.csv"
    path.write_text(content, encoding="utf-8")

    assert load_records(path, "customer") == []


def test_empty_json_array_returns_empty_collection(tmp_path: Path) -> None:
    path = tmp_path / "empty.json"
    path.write_text("[]", encoding="utf-8")

    assert load_records(path, "customer") == []


def test_malformed_json_has_file_and_syntax_context(tmp_path: Path) -> None:
    path = tmp_path / "broken.json"
    path.write_text('[{"customer_id": }]', encoding="utf-8")

    with pytest.raises(InvalidJSONSyntaxError, match=r"broken\.json at line 1, column"):
        load_records(path, "customer")


def test_malformed_jsonl_line_has_line_context(tmp_path: Path) -> None:
    path = tmp_path / "broken.jsonl"
    path.write_text('{"customer_id":"C-1"}\nnot-json\n', encoding="utf-8")

    with pytest.raises(InvalidJSONLLineError, match=r"broken\.jsonl at line 2"):
        load_records(path, "customer")


def test_missing_file_has_specific_error(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundIngestionError, match="missing.csv"):
        load_records(tmp_path / "missing.csv", "customer")


def test_unsupported_extension_has_specific_error(tmp_path: Path) -> None:
    path = tmp_path / "records.txt"
    path.write_text("anything", encoding="utf-8")

    with pytest.raises(UnsupportedFileFormatError, match="records.txt"):
        load_records(path, "customer")


def test_malformed_csv_row_width_has_record_context(tmp_path: Path) -> None:
    path = tmp_path / "broken.csv"
    path.write_text("customer_id,name\nC-1,Ada,extra\n", encoding="utf-8")

    with pytest.raises(MalformedCSVStructureError, match=r"broken\.csv at source record 1"):
        load_records(path, "customer")


def test_unreadable_file_has_specific_error(tmp_path: Path) -> None:
    path = tmp_path / "directory.csv"
    path.mkdir()

    with pytest.raises(UnreadableFileError, match="directory.csv"):
        load_records(path, "customer")


def test_missing_required_fields_are_not_validated(tmp_path: Path) -> None:
    path = tmp_path / "partial.json"
    path.write_text('[{"customer_id":"C-1"}]', encoding="utf-8")

    records = load_records(path, "customer")

    assert records[0].source_fields == {"customer_id": "C-1"}

    csv_path = tmp_path / "partial.csv"
    csv_path.write_text("customer_id,name\nC-2,\n", encoding="utf-8")
    csv_records = load_records(csv_path, "customer")
    assert csv_records[0].source_fields == {"customer_id": "C-2", "name": ""}


def test_json_non_array_or_non_object_records_are_rejected_as_malformed_structure(tmp_path: Path) -> None:
    path = tmp_path / "not-array.json"
    path.write_text('{"customer_id":"C-1"}', encoding="utf-8")
    with pytest.raises(MalformedJSONStructureError, match="expected an array"):
        load_records(path, "customer")

    path.write_text('[{"customer_id":"C-1"}, null]', encoding="utf-8")
    with pytest.raises(MalformedJSONStructureError, match="source record 2"):
        load_records(path, "customer")


def test_invalid_entity_type_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "records.csv"
    path.write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(UnsupportedEntityTypeError, match="customer.*product"):
        load_records(path, "supplier")


def test_ingested_record_has_datetime_timestamp() -> None:
    record = IngestedRecord({}, "customer", "sample.csv", 1, "csv", datetime.now(timezone.utc))
    assert isinstance(record.ingested_at, datetime)
