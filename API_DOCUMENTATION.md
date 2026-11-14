# MergeMind AI API Reference

Base URL in local development: `http://127.0.0.1:8000`. Interactive OpenAPI documentation is available at `/docs`; the generated schema is at `/openapi.json`. The API uses JSON responses except for multipart upload requests. Paginated collections use `{ "items": [], "total": 0, "limit": 50, "offset": 0 }`.

The current public route list below was checked against `app.openapi()`. FastAPI also has a compatibility alias `POST /api/v1/ingestion/validate`; it is intentionally excluded from OpenAPI. Use the documented `/api/v1/ingestion/uploads` route.

## Common behavior

- CORS origins: `http://localhost:5173`, `http://127.0.0.1:5173`, `http://localhost:5174`, and `http://127.0.0.1:5174`.
- `MERGEMIND_DATABASE_PATH` selects the SQLite file; otherwise the backend default is `data/output/mergemind.db`.
- `MERGEMIND_REPORT_DIRECTORY` selects the report directory; otherwise it is `reports/`.
- On startup, an existing database receives the backend's additive schema setup; existing rows are retained.
- Query list endpoints cap page sizes. `limit` and `offset` errors are FastAPI `422` responses.
- Common errors are JSON `{ "detail": "..." }`. `422` is request validation, `404` is a missing detail item/run/report, `409` is a state-transition or input-fingerprint conflict, `413` is an upload limit, `503` is unavailable database/report data, and unexpected failures return `500`. Filtered collection requests for an unknown run can return an empty page rather than `404`.
- The service currently runs pipeline execution in the request and serializes execution with a process-local lock. Clients must allow a long request; no progress/polling endpoint exists.

## Dashboard

### `GET /api/v1/dashboard`

Parameters: `days` integer 1–365 (default 30), `limit` 1–100 (default 10), `offset` non-negative (default 0).

Returns `DashboardResponse`: `database_status`, review/quarantine table availability flags, `generated_at`, `days`, `kpis`, `total_runs`, page metadata, `runs`, and up to 90 recent `activity` items. KPIs include persisted sources, accepted/rejected counts, masters, exact pairs and derived exact connected groups, probable decision counts, optional pending-review/quarantine totals, and accepted-record rate. Review/quarantine counts are `null` if the table is unavailable; after API startup migration they normally exist and may have zero rows.

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/dashboard?days=90&limit=10&offset=0"
```

## Ingestion and Pipeline Execution

### `POST /api/v1/ingestion/uploads`

Content type: `multipart/form-data`.

Fields: `entity_type` (`customer` or `product`), `file` (CSV, JSON array, JSON Lines, or NDJSON). Limits: 25 MiB, 100,000 rows. File contents are parsed through `load_records` and checked by `validate_records`. The response contains `upload_id`, entity, submitted basename, format, row counts, schema-drift count, error counts, up to 1,000 issue records, and `issues_truncated`.

```powershell
curl.exe -F "entity_type=customer" -F "file=@data/input/customers.csv" http://127.0.0.1:8000/api/v1/ingestion/uploads
```

Unsupported extension, malformed file, empty file, or invalid entity returns `422`; size/row-limit overflow returns `413`. Uploads are staged under generated server filenames in the OS temporary directory; the original path is not accepted from the client.

### `POST /api/v1/pipeline-runs`

Headers: required `Idempotency-Key` (8–128 safe characters: letters, digits, `.`, `_`, `:`, `-`). JSON body:

```json
{
  "customer_upload_id": "<upload id or null>",
  "product_upload_id": "<upload id or null>",
  "run_id": "optional-safe-run-id",
  "replace_existing": false
}
```

At least one upload ID is required. The endpoint calls the existing `run_pipeline` with staged paths, configured database, and report directory. Response: `run_id`, pipeline `status`, `result` (pipeline report payload without local `report_path`), and `report_url`. Pipeline stage failures are returned as a result with `status: FAILED`; they are not an HTTP transport failure. A changed input under an existing fingerprinted `run_id` returns `409`. Invalid/expired upload IDs return `404` or `422`. A missing idempotency header is `422`.

When `run_id` is omitted, its value is derived from the idempotency key. The key is not a response-cache store: retrying with the same staged IDs targets the same run and the backend fingerprint/idempotency logic applies. A new upload receives a new staged filename, so re-uploading identical bytes can have a different fingerprint. The staged-upload registry is process-local and expires entries after two hours.

## Pipeline Runs

### `GET /api/v1/pipeline-runs`

Parameters: optional `status` (`RUNNING`, `COMPLETED`, `FAILED`), `limit` 1–100 (default 25), `offset` (default 0). Returns a `PageResponse` of run metadata and `report_available`. The API omits the private input fingerprint.

### `GET /api/v1/pipeline-runs/{run_id}`

Returns persisted run summary/artifact counts and report fields when a report file is available: stage order/durations/metrics, throughput and execution summary, customer/product metrics, data-quality issues, operational errors, unresolved conflicts, review/quarantine statistics, persistence counts, and duration. The `pipeline_errors` table remains separately exposed by the next endpoint. A missing run returns `404`.

### `GET /api/v1/pipeline-runs/{run_id}/errors`

Parameters: `limit` 1–500 (default 100), `offset` (default 0). Returns paginated persisted operational error rows. Data-quality validation findings remain separate in the report.

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/pipeline-runs?status=COMPLETED&limit=25&offset=0"
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/pipeline-runs/<run_id>"
```

## Duplicate Explorer

### `GET /api/v1/pipeline-runs/{run_id}/duplicate-decisions`

Parameters: optional `entity_type` (`customer`/`product`), `decision` (`EXACT`, `IDENTITY_CONFLICT`, `STRONG_PROBABLE_MATCH`, `REVIEW_REQUIRED`, `NO_MATCH`), `search` (source IDs, max 120 characters), `limit` 1–200 (default 50), and `offset` (default 0).

Each item includes pair/run/entity IDs, decision, confidence, matching and conflicting evidence, serialized evidence columns, and `source_records` for the two source IDs. Persisted confidence values above `1` are normalized to fractional scores at this API boundary for the UI.

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/pipeline-runs/<run_id>/duplicate-decisions?decision=REVIEW_REQUIRED&entity_type=customer&limit=25"
```

## Review Queue

### `GET /api/v1/pipeline-runs/{run_id}/reviews`

Parameters: optional entity type and status (`PENDING`, `ACCEPTED_MATCH`, `REJECTED_MATCH`, `DEFERRED`), `limit` 1–200 (default 50), `offset` (default 0). Returns page metadata and each review's evidence, explanation, current status, and reviewer fields.

### `GET /api/v1/pipeline-runs/{run_id}/reviews/{review_id}`

Returns the review item, its history, and both source records.

### `POST /api/v1/pipeline-runs/{run_id}/reviews/{review_id}/decision`

JSON body: `decision` is required and one of `ACCEPTED_MATCH`, `REJECTED_MATCH`, `DEFERRED`, `PENDING`; optional `reviewer_id` (max 120) and `notes` (max 2,000).

```json
{"decision":"DEFERRED","reviewer_id":"reviewer-1","notes":"Check source evidence"}
```

Calls the existing review transition functions and returns updated item plus full history. Invalid transitions return `409`; `PENDING` is valid only for a deferred item. Acceptance records an auditable decision only and does not merge masters.

## Quarantine

### `GET /api/v1/pipeline-runs/{run_id}/quarantine`

Parameters: optional `entity_type`, `error_code` (max 80), `status` (`QUARANTINED`, `RESOLVED`, `DISCARDED`), `limit` 1–200 (default 50), `offset` (default 0). Returns original record JSON, validation issues, status, timestamps, and resolution fields.

### `GET /api/v1/pipeline-runs/{run_id}/quarantine/{quarantine_id}`

Returns a record plus resolution history.

### `POST /api/v1/pipeline-runs/{run_id}/quarantine/{quarantine_id}/resolution`

JSON body: `action` is `RESOLVE` or `DISCARD`; optional `resolution_action` (max 120) and `notes` (max 2,000).

```json
{"action":"RESOLVE","resolution_action":"CONTACT_SOURCE","notes":"Await corrected source file"}
```

Calls existing quarantine operations and returns updated item plus history. This does not edit the source or re-run validation/pipeline processing. Invalid state transitions return `409`.

## Master Records and Lineage

### `GET /api/v1/pipeline-runs/{run_id}/masters`

Parameters: optional `entity_type`, `search` (master ID or JSON field text, max 120), `limit` 1–200 (default 50), `offset` (default 0). Returns paginated decoded masters with merged fields, merge method, contributing IDs/systems, and field lineage.

### `GET /api/v1/pipeline-runs/{run_id}/masters/{master_id}`

Returns one master with field lineage and contributing source records.

### `GET /api/v1/pipeline-runs/{run_id}/masters/{master_id}/lineage`

Returns `{ "run_id": ..., "master_id": ..., "fields": [...] }`. Each field includes selected value, source record/system, selection rule, and rejected values.

## Reports and Analytics

### `GET /api/v1/reports`

Parameters: `limit` 1–100 (default 25), `offset` (default 0). Returns report-available run IDs and report URLs.

### `GET /api/v1/reports/summary`

Parameter: `days` 1–365 (default 90). Returns the same database-backed `DashboardResponse` structure as the dashboard endpoint; it is a persisted aggregate, not a separate analytical warehouse.

### `GET /api/v1/reports/{run_id}`

Returns sanitized JSON report content. Missing report: `404`; invalid/unreadable report: `503`.

### `GET /api/v1/reports/{run_id}/download`

Returns the same JSON report with an attachment filename `<run_id>_report.json`. Run IDs are validated before constructing report paths; local filesystem paths are not returned.

```powershell
Invoke-WebRequest "http://127.0.0.1:8000/api/v1/reports/<run_id>/download" -OutFile .\run-report.json
```

## Not Exposed as HTTP Endpoints

The API does not expose a source-record-only collection/detail route, pipeline progress/cancel endpoint, automatic merge after review acceptance, or corrected-record reprocessing endpoint. The existing functions do not provide those workflows. Use duplicate/review/master detail responses for related source records.
