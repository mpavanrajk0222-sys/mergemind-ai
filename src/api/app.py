from __future__ import annotations

import asyncio
import hashlib
import re
import sqlite3
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.config import database_path, report_directory
from src.api.migrations import prepare_api_database
from src.api.models import (
    DashboardResponse,
    PageResponse,
    PipelineExecutionRequest,
    PipelineExecutionResponse,
    QuarantineResolutionRequest,
    ReviewDecisionRequest,
    UploadValidationResponse,
)
from src.api.repository import DashboardDataUnavailable, get_dashboard
from src.api.uploads import UploadError, uploads, validate_staged_upload
from src.api.workflows import (
    WorkflowDataUnavailable,
    get_lineage,
    get_master,
    get_pair_sources,
    get_quarantine_detail,
    get_review_detail,
    get_run,
    get_run_errors,
    get_duplicate_page,
    list_masters,
    list_quarantine,
    list_reports,
    list_reviews,
    list_runs,
    read_report,
)
from src.database import RunInputMismatchError
from src.pipeline import run_pipeline
from src.quarantine import discard_quarantine_record, resolve_quarantine_record
from src.review_queue import accept_review, defer_review, reject_review, retrieve_review_history


SAFE_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
_PIPELINE_LOCK = asyncio.Lock()


@asynccontextmanager
async def lifespan(_: FastAPI):
    prepare_api_database()
    yield


app = FastAPI(
    title="MergeMind AI API",
    version="1.1.0",
    description="HTTP adapters over existing MergeMind pipeline, review, quarantine, and SQLite persistence.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "http://localhost:5174", "http://127.0.0.1:5174"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _handle_data_error(exc: Exception) -> HTTPException:
    if isinstance(exc, (DashboardDataUnavailable, WorkflowDataUnavailable, sqlite3.Error)):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc).strip("'"))
    if isinstance(exc, RunInputMismatchError):
        return HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, ValueError):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=500, detail="The requested operation could not be completed.")


def _paged(items: list[dict], total: int, limit: int, offset: int) -> PageResponse:
    return PageResponse(items=items, total=total, limit=limit, offset=offset)


def _sanitize_execution_result(result: dict, staged_paths: list[str]) -> dict:
    result.pop("report_path", None)
    for key in ("operational_errors", "errors"):
        rows = result.get(key)
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and isinstance(row.get("error_message"), str):
                    for staged_path in staged_paths:
                        row["error_message"] = row["error_message"].replace(staged_path, "uploaded input")
    return result


@app.get("/api/v1/dashboard", response_model=DashboardResponse, tags=["dashboard"])
def dashboard(
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> DashboardResponse:
    try:
        return get_dashboard(days=days, limit=limit, offset=offset)
    except (DashboardDataUnavailable, OSError) as exc:
        raise _handle_data_error(exc) from exc


@app.post("/api/v1/ingestion/uploads", response_model=UploadValidationResponse, tags=["ingestion"])
@app.post("/api/v1/ingestion/validate", response_model=UploadValidationResponse, include_in_schema=False)
async def upload_and_validate(
    entity_type: Literal["customer", "product"] = Form(...),
    file: UploadFile = File(...),
) -> UploadValidationResponse:
    content = bytearray()
    while True:
        chunk = await file.read(64 * 1024)
        if not chunk:
            break
        content.extend(chunk)
        if len(content) > 25 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="File exceeds the 25 MB upload limit.")
    try:
        staged = uploads.stage(entity_type, file.filename, bytes(content))
        payload = validate_staged_upload(staged)
        return UploadValidationResponse(**payload)
    except UploadError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    finally:
        await file.close()


@app.post("/api/v1/pipeline-runs", response_model=PipelineExecutionResponse, tags=["pipeline runs"])
async def execute_pipeline(
    request: PipelineExecutionRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=8, max_length=128),
) -> PipelineExecutionResponse:
    if not SAFE_IDEMPOTENCY_KEY.fullmatch(idempotency_key):
        raise HTTPException(status_code=422, detail="Idempotency-Key must use safe letters, digits, dot, underscore, colon, or hyphen characters.")
    if not request.customer_upload_id and not request.product_upload_id:
        raise HTTPException(status_code=422, detail="Upload at least one customer or product file.")
    try:
        customer = uploads.get(request.customer_upload_id, "customer") if request.customer_upload_id else None
        product = uploads.get(request.product_upload_id, "product") if request.product_upload_id else None
    except UploadError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    run_id = request.run_id or f"upload-{hashlib.sha256(idempotency_key.encode()).hexdigest()[:32]}"
    async with _PIPELINE_LOCK:
        try:
            result = await asyncio.to_thread(
                run_pipeline,
                customers_path=customer.path if customer else None,
                products_path=product.path if product else None,
                run_id=run_id,
                database_path=database_path(),
                report_directory=report_directory(),
                replace_existing=request.replace_existing,
            )
        except RunInputMismatchError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Pipeline execution could not be completed.") from exc
    result_payload = _sanitize_execution_result(
        result.to_dict(), [str(item.path) for item in (customer, product) if item]
    )
    return PipelineExecutionResponse(
        run_id=result.run_id,
        status=result.status,
        result=result_payload,
        report_url=f"/api/v1/reports/{result.run_id}/download",
    )


@app.get("/api/v1/pipeline-runs", response_model=PageResponse, tags=["pipeline runs"])
def pipeline_runs(
    status: Literal["RUNNING", "COMPLETED", "FAILED"] | None = None,
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> PageResponse:
    try:
        items, total = list_runs(status, limit, offset)
        return _paged(items, total, limit, offset)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}", tags=["pipeline runs"])
def pipeline_run_detail(run_id: str) -> dict:
    try:
        item = get_run(run_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Pipeline run was not found.")
        return item
    except HTTPException:
        raise
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/errors", response_model=PageResponse, tags=["pipeline runs"])
def pipeline_run_errors(
    run_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> PageResponse:
    try:
        items, total = get_run_errors(run_id, limit, offset)
        return _paged(items, total, limit, offset)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/duplicate-decisions", response_model=PageResponse, tags=["duplicate explorer"])
def duplicate_decisions(
    run_id: str,
    entity_type: Literal["customer", "product"] | None = None,
    decision: Literal["EXACT", "IDENTITY_CONFLICT", "STRONG_PROBABLE_MATCH", "REVIEW_REQUIRED", "NO_MATCH"] | None = None,
    search: str | None = Query(default=None, max_length=120),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PageResponse:
    try:
        items, total = get_duplicate_page(run_id, entity_type, decision, search, limit, offset)
        for item in items:
            item["source_records"] = get_pair_sources(
                run_id, item["entity_type"], [item["record_id_1"], item["record_id_2"]]
            )
        return _paged(items, total, limit, offset)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/reviews", response_model=PageResponse, tags=["review queue"])
def review_items(
    run_id: str,
    entity_type: Literal["customer", "product"] | None = None,
    status: Literal["PENDING", "ACCEPTED_MATCH", "REJECTED_MATCH", "DEFERRED"] | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PageResponse:
    try:
        items, total = list_reviews(run_id, entity_type, status, limit, offset)
        return _paged(items, total, limit, offset)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/reviews/{review_id}", tags=["review queue"])
def review_detail(run_id: str, review_id: str) -> dict:
    try:
        item = get_review_detail(run_id, review_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Review item was not found.")
        return item
    except HTTPException:
        raise
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.post("/api/v1/pipeline-runs/{run_id}/reviews/{review_id}/decision", tags=["review queue"])
def decide_review(run_id: str, review_id: str, request: ReviewDecisionRequest) -> dict:
    actions = {
        "ACCEPTED_MATCH": accept_review,
        "REJECTED_MATCH": reject_review,
        "DEFERRED": defer_review,
        "PENDING": None,
    }
    action = actions[request.decision]
    if action is None:
        try:
            from src.database import update_review_item
            updated = update_review_item(
                run_id, review_id, "PENDING", reviewer_id=request.reviewer_id,
                reviewer_notes=request.notes, db_path=database_path(),
            )
        except Exception as exc:
            raise _handle_data_error(exc) from exc
    else:
        try:
            updated = action(run_id, review_id, reviewer_id=request.reviewer_id, notes=request.notes, db_path=database_path())
        except Exception as exc:
            raise _handle_data_error(exc) from exc
    updated["history"] = retrieve_review_history(run_id, review_id, db_path=database_path())
    return updated


@app.get("/api/v1/pipeline-runs/{run_id}/quarantine", response_model=PageResponse, tags=["quarantine"])
def quarantine_items(
    run_id: str,
    entity_type: Literal["customer", "product"] | None = None,
    error_code: str | None = Query(default=None, max_length=80),
    status: Literal["QUARANTINED", "RESOLVED", "DISCARDED"] | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PageResponse:
    try:
        items, total = list_quarantine(run_id, entity_type, error_code, status, limit, offset)
        return _paged(items, total, limit, offset)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/quarantine/{quarantine_id}", tags=["quarantine"])
def quarantine_detail(run_id: str, quarantine_id: str) -> dict:
    try:
        item = get_quarantine_detail(run_id, quarantine_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Quarantine record was not found.")
        return item
    except HTTPException:
        raise
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.post("/api/v1/pipeline-runs/{run_id}/quarantine/{quarantine_id}/resolution", tags=["quarantine"])
def resolve_quarantine(run_id: str, quarantine_id: str, request: QuarantineResolutionRequest) -> dict:
    try:
        if request.action == "DISCARD":
            updated = discard_quarantine_record(run_id, quarantine_id, notes=request.notes, db_path=database_path())
        else:
            action = (request.resolution_action or "MANUAL_FOLLOW_UP").strip()
            updated = resolve_quarantine_record(
                run_id, quarantine_id, resolution_action=action, notes=request.notes, db_path=database_path()
            )
        item = get_quarantine_detail(run_id, quarantine_id)
        updated["history"] = item["history"] if item else []
        return updated
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/masters", response_model=PageResponse, tags=["master records"])
def masters(
    run_id: str,
    entity_type: Literal["customer", "product"] | None = None,
    search: str | None = Query(default=None, max_length=120),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PageResponse:
    try:
        items, total = list_masters(run_id, entity_type, search, limit, offset)
        return _paged(items, total, limit, offset)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/masters/{master_id}", tags=["master records"])
def master_detail(run_id: str, master_id: str) -> dict:
    try:
        item = get_master(run_id, master_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Master record was not found.")
        item["source_records"] = get_pair_sources(
            run_id, item["entity_type"], item.get("contributing_source_record_ids", [])
        )
        return item
    except HTTPException:
        raise
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/pipeline-runs/{run_id}/masters/{master_id}/lineage", tags=["data lineage"])
def master_lineage(run_id: str, master_id: str) -> dict:
    try:
        return {"run_id": run_id, "master_id": master_id, "fields": get_lineage(run_id, master_id)}
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/reports", response_model=PageResponse, tags=["reports"])
def reports(
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> PageResponse:
    try:
        items, total = list_reports(limit, offset)
        return _paged(items, total, limit, offset)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/reports/summary", tags=["reports"])
def reports_summary(days: int = Query(default=90, ge=1, le=365)) -> DashboardResponse:
    try:
        return get_dashboard(days=days, limit=100, offset=0)
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/reports/{run_id}", tags=["reports"])
def report_detail(run_id: str) -> dict:
    try:
        payload = read_report(run_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Report was not found.")
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        raise _handle_data_error(exc) from exc


@app.get("/api/v1/reports/{run_id}/download", tags=["reports"])
def download_report(run_id: str) -> JSONResponse:
    try:
        payload = read_report(run_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Report was not found.")
        return JSONResponse(
            content=payload,
            headers={"Content-Disposition": f'attachment; filename="{run_id}_report.json"'},
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise _handle_data_error(exc) from exc
