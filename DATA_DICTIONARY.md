# MergeMind AI Data Dictionary

This dictionary describes the source contracts and persisted SQLite artifacts implemented by `src/schemas.py`, `src/database.py`, and the modules that create their payloads. Types shown as Python `str`, `datetime`, and `Decimal` describe expected in-memory values; CSV ingestion initially reads every cell as a string, and validation checks rather than coercing timestamps and decimals.

## Source Entity Schemas

### Customer

| Field | Type | Required | Nullable | Meaning / validation |
|---|---|---:|---:|---|
| `customer_id` | string | Yes | No | Source identifier; must be non-empty and unique within the entity batch. |
| `name` | string | Yes | No | Customer name; whitespace-only and empty values are rejected. |
| `email` | string | No | Yes | If present and nonblank, must have a basic `local@domain.tld` form. |
| `phone` | string | No | Yes | If present and nonblank, must contain 7–15 digits and only common international separators. |
| `city` | string | No | Yes | Optional city. |
| `address` | string | No | Yes | Optional address. |
| `updated_at` | ISO-8601 string / `datetime` | Yes | No | Must parse as an ISO-8601 timestamp; timezone awareness is not required by validation. |
| `source_system` | string | Yes | No | Origin system identifier; must be non-empty. |

### Product

| Field | Type | Required | Nullable | Meaning / validation |
|---|---|---:|---:|---|
| `product_id` | string | Yes | No | Source identifier; must be non-empty and unique within the entity batch. |
| `product_name` | string | Yes | No | Product name; must be non-empty. |
| `sku` | string | No | Yes | Optional stock keeping unit. |
| `brand` | string | No | Yes | Optional brand. |
| `category` | string | No | Yes | Optional category. |
| `price` | decimal-compatible value | No | Yes | If present, must be finite and non-negative; currency and scale are unspecified. |
| `updated_at` | ISO-8601 string / `datetime` | Yes | No | Must parse as an ISO-8601 timestamp. |
| `source_system` | string | Yes | No | Origin system identifier; must be non-empty. |

Unexpected fields are accepted and recorded as schema drift by default. `validate_records(..., strict_unexpected_fields=True)` rejects records with unexpected fields. Each rejected row can have multiple validation issues. The original source values are retained.

## SQLite Database

Default path: `data/output/mergemind.db`. Override via CLI `--database` or API environment variable `MERGEMIND_DATABASE_PATH`. The API startup applies the current additive schema setup to an existing database; it creates missing tables and adds `pipeline_runs.input_fingerprint` when absent. Existing rows are not deleted by this migration.

All operational rows are run-scoped. The database module enables SQLite foreign keys on its connections. JSON columns use deterministic JSON serialization; datetimes are ISO-8601 strings and `Decimal` values are serialized as strings.

### `pipeline_runs`

| Column | Type | Nullability / constraint | Meaning |
|---|---|---|---|
| `run_id` | TEXT | Primary key | Stable pipeline run identifier. |
| `started_at` | TEXT | NOT NULL | Run start timestamp. |
| `completed_at` | TEXT | Nullable | Run completion timestamp. |
| `status` | TEXT | NOT NULL | `RUNNING`, `COMPLETED`, or `FAILED`. |
| `customer_input_count` | INTEGER | NOT NULL, default 0, non-negative | Customer rows ingested. |
| `product_input_count` | INTEGER | NOT NULL, default 0, non-negative | Product rows ingested. |
| `accepted_count` | INTEGER | NOT NULL, default 0, non-negative | Accepted source rows across entities. |
| `rejected_count` | INTEGER | NOT NULL, default 0, non-negative | Rejected source rows across entities. |
| `master_record_count` | INTEGER | NOT NULL, default 0, non-negative | Number of created masters. |
| `error_count` | INTEGER | NOT NULL, default 0, non-negative | Operational errors for the latest run attempt. |
| `input_fingerprint` | TEXT | Nullable | SHA-256 fingerprint of entity roles, input basenames and bytes; added additively to older databases. |

### `source_records`

Composite primary key: `(run_id, entity_type, source_record_id)`. `run_id` references `pipeline_runs` with cascade deletion when that run is explicitly replaced.

| Column | Type | Nullability / meaning |
|---|---|---|
| `source_record_id` | TEXT | NOT NULL; original source ID when usable, otherwise a storage-only rejected-row key. |
| `run_id` | TEXT | NOT NULL; parent run. |
| `entity_type` | TEXT | NOT NULL; `customer` or `product`. |
| `source_system` | TEXT | Nullable; source field if supplied. |
| `source_file` | TEXT | NOT NULL; input basename recorded by ingestion. For API uploads this is the generated staged filename. |
| `source_record_number` | INTEGER | NOT NULL, positive, one-based row/record provenance. |
| `original_record_json` | TEXT | NOT NULL; original fields without normalization changes. |
| `normalized_record_json` | TEXT | Nullable; accepted record plus normalized comparison fields. |
| `validation_status` | TEXT | NOT NULL; `ACCEPTED` or `REJECTED`. |

Validation details are in pipeline reports and, for rejected records, `quarantine_records.validation_issues_json`.

### `duplicate_pairs`

`pair_id` is the primary key. Unique by `(run_id, entity_type, record_id_1, record_id_2)`; IDs are stored in sorted order. Both source IDs reference the corresponding source rows.

| Column | Type | Nullability / meaning |
|---|---|---|
| `pair_id` | TEXT | Primary key; deterministic pair identifier. |
| `run_id` | TEXT | NOT NULL; parent run. |
| `entity_type` | TEXT | NOT NULL; `customer` or `product`. |
| `record_id_1`, `record_id_2` | TEXT | NOT NULL; sorted source IDs. |
| `decision` | TEXT | NOT NULL; currently `EXACT`, `IDENTITY_CONFLICT`, `STRONG_PROBABLE_MATCH`, `REVIEW_REQUIRED`, or `NO_MATCH`. |
| `confidence_score` | REAL | Nullable; probable confidence uses a 0–100 scale in persisted matching decisions; exact decisions have no score. The HTTP duplicate endpoint normalizes values above 1 to fractions for presentation. |
| `matching_evidence_json` | TEXT | NOT NULL; serialized matching attributes/scores/evidence. |
| `conflicting_evidence_json` | TEXT | NOT NULL; serialized contradictory attribute evidence. |

### `master_records`

Composite primary key: `(run_id, master_id)`. `merge_method` is constrained to `EXACT`, `STRONG_PROBABLE`, or `SINGLETON`.

| Column | Type | Nullability / meaning |
|---|---|---|
| `run_id` | TEXT | NOT NULL; parent run. |
| `master_id` | TEXT | NOT NULL; deterministic identifier from entity and contributing source IDs. |
| `entity_type` | TEXT | NOT NULL; `customer` or `product`. |
| `merge_method` | TEXT | NOT NULL; exact, isolated strong-probable, or singleton master. |
| `merged_record_json` | TEXT | NOT NULL; master payload, including `merged_fields`, contributing IDs/systems, field decisions, and creation time data. |
| `created_at` | TEXT | NOT NULL; deterministic master timestamp based on contributing records. |

The API detail response adds `run_id`, `master_id`, `entity_type`, `merge_method`, `created_at`, `field_lineage`, and contributing `source_records` around this payload.

### `field_lineage`

Unique by `(run_id, master_id, field_name)` and cascades with its master.

| Column | Type | Nullability / meaning |
|---|---|---|
| `lineage_id` | TEXT | Primary key. |
| `run_id`, `master_id` | TEXT | NOT NULL; parent master reference. |
| `field_name` | TEXT | NOT NULL; selected master field. |
| `selected_value` | TEXT | Nullable JSON-serialized selected source value. |
| `selected_source_record_id` | TEXT | Nullable contributing source record. |
| `selected_source_system` | TEXT | Nullable originating system. |
| `selection_rule` | TEXT | NOT NULL; survivorship policy and any unresolved-recency note. |
| `rejected_values_json` | TEXT | NOT NULL; non-selected original values with source provenance. |

### `review_items`

Composite primary key `(run_id, review_id)`, unique by run/entity/source pair. `review_id` format is `review-` followed by 24 lowercase hexadecimal characters. Pair IDs are sorted and reference source records.

| Column | Type | Nullability / meaning |
|---|---|---|
| `run_id`, `review_id` | TEXT | NOT NULL composite key. |
| `entity_type` | TEXT | NOT NULL; customer or product. |
| `record_id_1`, `record_id_2` | TEXT | NOT NULL; uncertain source pair. |
| `confidence_score` | REAL | Nullable; pair confidence when available. |
| `matching_evidence_json` | TEXT | NOT NULL; candidate sources and match evidence. |
| `conflicting_evidence_json` | TEXT | NOT NULL; candidate sources and conflicts. |
| `decision_explanation` | TEXT | NOT NULL; reason for human review. |
| `review_status` | TEXT | NOT NULL, default `PENDING`; `PENDING`, `ACCEPTED_MATCH`, `REJECTED_MATCH`, or `DEFERRED`. |
| `created_at` | TEXT | NOT NULL. |
| `reviewed_at` | TEXT | Nullable. |
| `reviewer_id` | TEXT | Nullable. |
| `reviewer_notes` | TEXT | Nullable. |

Review acceptance records a decision only; it does not change match groups or regenerate master records.

### `review_history`

| Column | Type | Nullability / meaning |
|---|---|---|
| `history_id` | TEXT | Primary key. |
| `run_id`, `review_id` | TEXT | NOT NULL; composite foreign key to `review_items`, cascade on parent deletion. |
| `previous_status` | TEXT | Nullable for initial creation. |
| `new_status` | TEXT | NOT NULL status after the event. |
| `reviewer_id`, `reviewer_notes` | TEXT | Nullable audit context. |
| `changed_at` | TEXT | NOT NULL event timestamp. |

Allowed state transitions: `PENDING` to accepted, rejected, or deferred; `DEFERRED` to pending, accepted, or rejected. Accepted and rejected are terminal. Every successful transition appends history transactionally.

### `quarantine_records`

Composite primary key `(run_id, quarantine_id)`, unique per run/entity/source-file/record-number. Rows are generated only from validation-rejected source records.

| Column | Type | Nullability / meaning |
|---|---|---|
| `run_id`, `quarantine_id` | TEXT | NOT NULL composite key. |
| `entity_type` | TEXT | NOT NULL; customer or product. |
| `source_record_id` | TEXT | Nullable when the source ID is absent/unusable. |
| `source_file` | TEXT | NOT NULL source basename. |
| `source_record_number` | INTEGER | NOT NULL, positive. |
| `original_record_json` | TEXT | NOT NULL original source fields. |
| `validation_issues_json` | TEXT | NOT NULL list of `record_id`, `field_name`, `error_code`, and `message`. |
| `quarantine_status` | TEXT | NOT NULL, default `QUARANTINED`; `QUARANTINED`, `RESOLVED`, or `DISCARDED`. |
| `quarantined_at` | TEXT | NOT NULL. |
| `resolved_at` | TEXT | Nullable. |
| `resolution_action` | TEXT | Nullable disposition label. |
| `resolution_notes` | TEXT | Nullable audit notes. |

Resolve/discard operations do not change the original source row, repair it, or re-run validation.

### `quarantine_history`

| Column | Type | Nullability / meaning |
|---|---|---|
| `history_id` | TEXT | Primary key. |
| `run_id`, `quarantine_id` | TEXT | NOT NULL; composite foreign key to `quarantine_records`. |
| `previous_status` | TEXT | Nullable for initial creation. |
| `new_status` | TEXT | NOT NULL. |
| `resolution_action`, `resolution_notes` | TEXT | Nullable audit details. |
| `changed_at` | TEXT | NOT NULL event timestamp. |

### `pipeline_errors`

| Column | Type | Nullability / meaning |
|---|---|---|
| `error_id` | TEXT | Primary key; deterministic for identical error content unless caller-supplied. |
| `run_id` | TEXT | NOT NULL; parent run. |
| `stage` | TEXT | NOT NULL failing pipeline stage. |
| `error_code` | TEXT | NOT NULL exception/error category. |
| `error_message` | TEXT | NOT NULL operational detail; source-value logging is separately restricted. |
| `source_record_id` | TEXT | Nullable associated source row. |
| `created_at` | TEXT | NOT NULL. |

Validation findings belong in report `data_quality_issues`, not in `pipeline_errors`; `pipeline_runs.error_count` represents operational errors.

## JSON Reports and Logs

Reports are files named `<run_id>_report.json` in `reports/` by default, not SQLite tables. Current report fields include `stage_order`, `stage_durations_seconds`, `stage_metrics`, `throughput_metrics`, `execution_summary`, per-entity metrics, rejected/data-quality findings, operational errors, unresolved conflicts, review and quarantine statistics, persistence counts, and execution duration. Older checked-in report files predate the current report contract and may omit newer sections.

Structured logs are JSON Lines with timestamp, level, logger, message, and optional run/stage/entity/count/error context. Source record field values and exception messages are not emitted by the structured pipeline logger.

## Checked-In Database Snapshot

Read-only inspection on 2026-11-14 found two completed runs (`task11-generated-demo` and `task11-1-classification-check`), 4,000 source rows, 28,896 pair-decision rows, 3,882 masters, 23,292 lineage rows, and 20 operational-error rows. The source rows comprise 1,980 accepted and 20 rejected rows per run. The API startup migration had created the review and quarantine tables, but they contained no records at inspection time. Counts describe this checked-in demo database only and change when runs are executed or workflow actions are recorded.
