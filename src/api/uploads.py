from __future__ import annotations

import shutil
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from src.ingestion import IngestionError, load_records
from src.validation import validate_records


EntityType = Literal["customer", "product"]
ALLOWED_EXTENSIONS = {".csv", ".json", ".jsonl", ".ndjson"}
MAX_UPLOAD_BYTES = 25 * 1024 * 1024
MAX_RECORDS = 100_000
UPLOAD_TTL_SECONDS = 2 * 60 * 60


class UploadError(ValueError):
    def __init__(self, message: str, status_code: int = 422) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class StagedUpload:
    upload_id: str
    entity_type: EntityType
    path: Path
    original_filename: str
    created_at: float


class UploadStore:
    def __init__(self) -> None:
        self._root = Path(tempfile.gettempdir()) / "mergemind-ai-uploads"
        self._root.mkdir(parents=True, exist_ok=True)
        self._items: dict[str, StagedUpload] = {}
        self._lock = threading.Lock()

    def _expire(self) -> None:
        cutoff = time.time() - UPLOAD_TTL_SECONDS
        for upload_id, item in list(self._items.items()):
            if item.created_at < cutoff:
                item.path.unlink(missing_ok=True)
                self._items.pop(upload_id, None)

    def stage(self, entity_type: EntityType, filename: str | None, content: bytes) -> StagedUpload:
        original_name = Path(filename or "upload").name
        extension = Path(original_name).suffix.lower()
        if extension not in ALLOWED_EXTENSIONS:
            raise UploadError("Use a .csv, .json, .jsonl, or .ndjson file.")
        if not content:
            raise UploadError("The uploaded file is empty.")
        if len(content) > MAX_UPLOAD_BYTES:
            raise UploadError("File exceeds the 25 MB upload limit.", 413)

        upload_id = uuid.uuid4().hex
        safe_path = self._root / f"{upload_id}{extension}"
        safe_path.write_bytes(content)
        item = StagedUpload(upload_id, entity_type, safe_path, original_name, time.time())
        with self._lock:
            self._expire()
            self._items[upload_id] = item
        return item

    def get(self, upload_id: str, entity_type: EntityType | None = None) -> StagedUpload:
        with self._lock:
            self._expire()
            item = self._items.get(upload_id)
        if item is None or not item.path.is_file():
            raise UploadError("Upload validation has expired. Upload the file again.", 404)
        if entity_type is not None and item.entity_type != entity_type:
            raise UploadError("Upload ID does not match the requested entity type.", 422)
        return item

    def remove(self, upload_id: str) -> None:
        with self._lock:
            item = self._items.pop(upload_id, None)
        if item is not None:
            item.path.unlink(missing_ok=True)


uploads = UploadStore()


def validate_staged_upload(item: StagedUpload) -> dict:
    try:
        records = load_records(item.path, item.entity_type)
    except IngestionError as exc:
        raise UploadError("File could not be parsed. Check its extension, encoding, and record structure.") from exc
    if len(records) > MAX_RECORDS:
        raise UploadError("File exceeds the 100,000 record upload limit.", 413)
    result = validate_records(records, item.entity_type)
    issues = [
        {
            "record_id": issue.record_id,
            "field_name": issue.field_name,
            "error_code": issue.error_code,
            "message": issue.message,
        }
        for rejected in result.rejected_records
        for issue in rejected.errors
    ]
    return {
        "upload_id": item.upload_id,
        "entity_type": item.entity_type,
        "filename": item.original_filename,
        "source_format": records[0].source_format if records else item.path.suffix.lstrip("."),
        "total_records": result.validation_summary.total_records,
        "accepted_records": result.validation_summary.valid_records,
        "rejected_records": result.validation_summary.rejected_records,
        "schema_drift_records": result.validation_summary.schema_drift_records,
        "error_counts_by_code": result.validation_summary.error_counts_by_code,
        "issues": issues[:1000],
        "issues_truncated": len(issues) > 1000,
    }
