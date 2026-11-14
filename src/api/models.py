from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class DashboardKpis(BaseModel):
    source_records: int
    accepted_records: int
    rejected_records: int
    master_records: int
    exact_duplicate_groups: int
    exact_duplicate_pairs: int
    probable_match_decisions: dict[str, int]
    pending_reviews: int | None
    quarantine_records: int | None
    accepted_rate_percent: float | None


class PipelineActivity(BaseModel):
    run_id: str
    started_at: str
    completed_at: str | None
    status: str
    source_records: int
    accepted_records: int
    rejected_records: int
    master_records: int
    entities: list[str]
    exact_duplicate_pairs: int
    probable_match_decisions: dict[str, int]


class DashboardResponse(BaseModel):
    database_status: Literal["available"]
    review_data_available: bool
    quarantine_data_available: bool
    generated_at: str
    days: int
    kpis: DashboardKpis
    total_runs: int
    limit: int
    offset: int
    runs: list[PipelineActivity]
    activity: list[PipelineActivity]


class PageResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


class UploadValidationResponse(BaseModel):
    upload_id: str
    entity_type: Literal["customer", "product"]
    filename: str
    source_format: str
    total_records: int
    accepted_records: int
    rejected_records: int
    schema_drift_records: int
    error_counts_by_code: dict[str, int]
    issues: list[dict[str, Any]]
    issues_truncated: bool


class PipelineExecutionRequest(BaseModel):
    customer_upload_id: str | None = None
    product_upload_id: str | None = None
    run_id: str | None = Field(default=None, min_length=1, max_length=128)
    replace_existing: bool = False


class PipelineExecutionResponse(BaseModel):
    run_id: str
    status: str
    result: dict[str, Any]
    report_url: str


class ReviewDecisionRequest(BaseModel):
    decision: Literal["ACCEPTED_MATCH", "REJECTED_MATCH", "DEFERRED", "PENDING"]
    reviewer_id: str | None = Field(default=None, max_length=120)
    notes: str | None = Field(default=None, max_length=2000)


class QuarantineResolutionRequest(BaseModel):
    action: Literal["RESOLVE", "DISCARD"]
    resolution_action: str | None = Field(default=None, max_length=120)
    notes: str | None = Field(default=None, max_length=2000)
