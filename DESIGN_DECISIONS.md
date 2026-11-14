# Design Decisions

This document records behavior implemented in the current Python modules. Thresholds and rules below come from `src/duplicate_detection.py`, `src/probable_matching.py`, `src/survivorship.py`, `src/pipeline.py`, and `src/database.py`; they are not frontend approximations.

## Exact Matching

- Exact matching runs after validation and normalization.
- Customer exact groups require all three normalized values to be present and equal: `email`, `phone`, and `name`. Shared email/phone/name combinations with conflicting identifiers are emitted as review candidates rather than silently grouped.
- Product exact grouping begins with normalized `sku`, then uses normalized `product_name` and brand compatibility. Conflicting brand/name assertions and ambiguous missing-brand cases are surfaced as conflicts.
- Exact results include deterministic group IDs, sorted source IDs, matching evidence, and unique pair decisions.
- If one source pair receives both an exact `IDENTITY_CONFLICT` and a probable decision, the pair-level SQLite table keeps the exact conflict as its primary decision. A probable review candidate is still stored separately in `review_items` with its own confidence and evidence.
- Trade-off: strict exact rules avoid broad merges but miss duplicate records with missing or changed identifiers; probable matching is a separate later stage.

## Probable Matching and Blocking

Probable matching uses RapidFuzz `fuzz.ratio` for text similarities and fixed weighted scoring on a 0–100 scale. Missing measurements contribute zero; weights are not renormalized.

| Entity | Attribute | Weight |
|---|---|---:|
| Customer | name | 0.45 |
| Customer | email | 0.25 |
| Customer | phone | 0.25 |
| Customer | city | 0.05 |
| Product | product_name | 0.50 |
| Product | sku | 0.20 |
| Product | brand | 0.15 |
| Product | category | 0.10 |
| Product | price | 0.05 |

Default `MatchingConfig` uses name thresholds of 88, overall strong threshold 75, review threshold 55, and blocking prefix length 4. Customer blocks are exact normalized email, exact normalized phone, or first-name-token prefix plus city. Product blocks are exact SKU, brand plus product-name tokens, or category plus product-name tokens. Exact-group members are excluded from probable candidate scoring.

Customer strong matches require a matching contact identifier, sufficient name similarity and confidence, and no conflicting attributes. Product strong matches require product-name similarity, confidence at least 75, two independent supporting SKU/brand/category/price signals, and no conflicts. Other candidates may become review-required or no-match according to the module's explicit conditions.

Trade-offs: blocking reduces comparisons but can miss pairs that share no configured block. Candidate pairs are materialized and scored in memory, which is the main measured scale limit. Confidence is a deterministic ranking signal, not a calibrated probability.

## Survivorship

- Exact groups are accepted as confirmed.
- Only an isolated strong-probable pair of otherwise ungrouped records can merge. A strong edge connecting existing groups or sharing a record with another strong edge remains an unresolved conflict to avoid unsafe transitive clustering.
- All other accepted records become singleton masters.
- Customer `name`, `city`, and `address` choose the most complete non-empty text (token count, then length), then latest comparable `updated_at`, then stable source-system/ID ordering. Email and phone use latest valid non-empty value. `updated_at` uses latest comparable timestamp.
- Product name, brand, and category use completeness then recency. A single normalized SKU assertion set uses latest source value; conflicting normalized SKUs select no value and create a conflict. Price uses latest valid decimal and retains alternatives in lineage.
- Mixed timezone-aware and timezone-naive timestamps are not ordered under an invented timezone policy; ambiguity is retained in lineage/conflicts.
- `source_system` in a master is the sorted set of contributing systems, not a single selected value.

## Human Review and Quarantine

Review items aggregate candidate sources/evidence for uncertain pairs. Review states are `PENDING`, `ACCEPTED_MATCH`, `REJECTED_MATCH`, and `DEFERRED`. Pending may transition to accepted/rejected/deferred; deferred may return to pending or transition to accepted/rejected; accepted and rejected are terminal. Every successful transition is written to append-only history in the same transaction. An accepted review is not an instruction to merge records.

Quarantine contains snapshots of validation-rejected source rows and their validation issues. States are `QUARANTINED`, `RESOLVED`, and `DISCARDED`. Resolve/discard append history but do not alter original source data, repair records, retry validation, or rerun the pipeline.

## Input Fingerprints, Idempotency, and Recovery

- The pipeline SHA-256 fingerprint includes each entity role, input basename, file size, and file bytes. Directory path is excluded; basename is included because it is persisted as source provenance.
- Reusing a run ID with an unchanged fingerprint resumes/upserts while preserving child artifacts and workflow history. A mismatch fails before changing that run or its report.
- `replace_existing=True` replaces only the selected run and cascades that run's records/history.
- Failed same-input runs can resume. Writes are run-scoped and idempotent; status becomes `COMPLETED` only after all stages succeed. Prior operational error rows remain for diagnosis.
- The HTTP API derives a default run ID from the required `Idempotency-Key`, but does not keep a separate idempotency response ledger. Replaying a request with the same staged upload IDs resumes the same run. A new staged upload has a generated basename and can therefore produce a different fingerprint.
- SQLite is used because this project is a local/demo pipeline with transactional run-scoped persistence and no separate database service requirement. Larger concurrent deployments would need a more scalable shared service and migration discipline.

## Data Safety and Trade-offs

- Original source JSON is retained separately from normalized comparison fields.
- Validation issues are separate from operational errors. Obvious secret-like keys are refused by persistence helpers.
- Upload names are replaced with generated filenames under an OS temporary directory; configured paths are server-side only.
- Database foreign keys and transactions protect run-scoped relationships and atomic audit changes.
- API startup uses the existing additive schema initializer for an existing DB. It adds missing current tables and the fingerprint column but does not delete records. Back up the DB before deployment changes as a general operational precaution.
- API pipeline execution is synchronous and serialized within one process. It lacks background jobs, progress/cancel endpoints, distributed locks, and cross-worker upload storage.
- Benchmarking is based on one Windows machine and seeded synthetic data; the scenario distribution is not an accuracy or production-prevalence estimate.
