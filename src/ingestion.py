"""Read source records from CSV, JSON arrays, and JSON Lines files.

Ingestion preserves source values and attaches provenance metadata. It does
not perform schema validation, normalization, matching, or persistence.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal


SourceFormat = Literal["csv", "json", "jsonl"]


class IngestionError(Exception):
    """Base class for source-file ingestion failures."""


class FileNotFoundIngestionError(IngestionError):
    """Raised when the requested input file does not exist."""


class UnsupportedFileFormatError(IngestionError):
    """Raised when a file extension does not identify a supported format."""


class UnreadableFileError(IngestionError):
    """Raised when a file cannot be opened or decoded as UTF-8."""


class MalformedCSVStructureError(IngestionError):
    """Raised when a CSV has invalid headers, row widths, or quoting."""


class InvalidJSONSyntaxError(IngestionError):
    """Raised when a JSON document is syntactically invalid."""


class MalformedJSONStructureError(IngestionError):
    """Raised when valid JSON does not contain an array of objects."""


class InvalidJSONLLineError(IngestionError):
    """Raised when a JSON Lines line is not one valid JSON object."""


class UnsupportedEntityTypeError(IngestionError):
    """Raised when the entity type is not customer or product."""


@dataclass(frozen=True)
class IngestedRecord:
    """One source record with ingestion provenance.

    ``source_fields`` contains only fields present in the input record. CSV
    values remain strings; JSON values retain their decoded Python types.
    """

    source_fields: dict[str, Any]
    entity_type: Literal["customer", "product"]
    source_file: str
    source_record_number: int
    source_format: SourceFormat
    ingested_at: datetime


def _source_format(path: Path) -> SourceFormat:
    extension = path.suffix.lower()
    if extension == ".csv":
        return "csv"
    if extension == ".json":
        return "json"
    if extension in (".jsonl", ".ndjson"):
        return "jsonl"
    raise UnsupportedFileFormatError(
        f"Unsupported file extension {extension or '<none>'!r} for {path}; "
        "use .csv, .json, .jsonl, or .ndjson."
    )


def _entity_type(value: str) -> Literal["customer", "product"]:
    normalized = value.lower() if isinstance(value, str) else ""
    if normalized not in ("customer", "product"):
        raise UnsupportedEntityTypeError(
            f"Unsupported entity type {value!r}; expected 'customer' or 'product'."
        )
    return normalized  # type: ignore[return-value]


def _read_csv(file_handle: Any, path: Path) -> list[dict[str, Any]]:
    reader = csv.reader(file_handle, strict=True)
    try:
        headers: list[str] | None = None
        for row in reader:
            if row:
                headers = row
                break
        if headers is None:
            return []
        if any(header == "" for header in headers):
            raise MalformedCSVStructureError(f"Malformed CSV in {path}: header contains an empty column name.")
        if len(set(headers)) != len(headers):
            raise MalformedCSVStructureError(f"Malformed CSV in {path}: header contains duplicate column names.")

        records: list[dict[str, Any]] = []
        record_number = 0
        for row in reader:
            # A blank physical line is a separator, not a source record.
            if not row:
                continue
            record_number += 1
            if len(row) != len(headers):
                raise MalformedCSVStructureError(
                    f"Malformed CSV in {path} at source record {record_number} "
                    f"(line {reader.line_num}): expected {len(headers)} columns, found {len(row)}."
                )
            records.append(dict(zip(headers, row)))
        return records
    except csv.Error as exc:
        raise MalformedCSVStructureError(
            f"Malformed CSV in {path} near line {reader.line_num}: {exc}."
        ) from exc


def _read_json(file_handle: Any, path: Path) -> list[dict[str, Any]]:
    try:
        document = json.load(file_handle)
    except json.JSONDecodeError as exc:
        raise InvalidJSONSyntaxError(
            f"Invalid JSON syntax in {path} at line {exc.lineno}, column {exc.colno}: {exc.msg}."
        ) from exc
    if not isinstance(document, list):
        raise MalformedJSONStructureError(f"Malformed JSON structure in {path}: expected an array of objects.")
    records: list[dict[str, Any]] = []
    for record_number, item in enumerate(document, start=1):
        if not isinstance(item, dict):
            raise MalformedJSONStructureError(
                f"Malformed JSON structure in {path} at source record {record_number}: expected an object."
            )
        records.append(item)
    return records


def _read_jsonl(file_handle: Any, path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(file_handle, start=1):
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise InvalidJSONLLineError(
                f"Invalid JSONL line in {path} at line {line_number}, column {exc.colno}: {exc.msg}."
            ) from exc
        if not isinstance(item, dict):
            raise InvalidJSONLLineError(
                f"Invalid JSONL line in {path} at line {line_number}: expected a JSON object."
            )
        records.append(item)
    return records


def load_records(file_path: Path, entity_type: str) -> list[IngestedRecord]:
    """Load records from a CSV, JSON array, or JSONL file.

    The returned record numbers are one-based. CSV and JSON numbers count
    source records (excluding the CSV header); JSONL numbers are physical line
    numbers so malformed-line errors and provenance point to the same line.
    No business-field validation is performed.
    """

    path = Path(file_path)
    normalized_entity = _entity_type(entity_type)
    source_format = _source_format(path)
    ingested_at = datetime.now(timezone.utc)

    try:
        with path.open("r", encoding="utf-8", newline="") as file_handle:
            if source_format == "csv":
                source_records = _read_csv(file_handle, path)
            elif source_format == "json":
                source_records = _read_json(file_handle, path)
            else:
                source_records = _read_jsonl(file_handle, path)
    except FileNotFoundError as exc:
        raise FileNotFoundIngestionError(f"Input file not found: {path}.") from exc
    except (OSError, UnicodeError) as exc:
        raise UnreadableFileError(f"Cannot read input file {path}: {exc}.") from exc

    return [
        IngestedRecord(
            source_fields=fields,
            entity_type=normalized_entity,
            source_file=path.name,
            source_record_number=record_number,
            source_format=source_format,
            ingested_at=ingested_at,
        )
        for record_number, fields in enumerate(source_records, start=1)
    ]


__all__ = (
    "IngestedRecord",
    "IngestionError",
    "FileNotFoundIngestionError",
    "UnsupportedFileFormatError",
    "UnreadableFileError",
    "MalformedCSVStructureError",
    "InvalidJSONSyntaxError",
    "MalformedJSONStructureError",
    "InvalidJSONLLineError",
    "UnsupportedEntityTypeError",
    "load_records",
)
