# MergeMind AI Performance Benchmark

Generated 2026-11-14T12:29:11+00:00.

## Method

- Generated customers and products with `scripts/generate_data.py`, seed 42, at 10,000 and 100,000 rows per entity.
- Benchmarked each entity separately. All database, logs and per-run reports stayed in OS temporary directories; the project database was not used.
- Sampled process-tree RSS every 100 ms. A run stopped after sampled RSS crossed 1,250 MiB or after 180 seconds.
- Completed stage timings come from pipeline reports; partial timings come from structured logs. Partial attempts are not represented as successful pipeline timings.

## Results

| Entity | Input | Outcome | Wall time | Peak RSS | Accepted/rejected | Probable candidates | Masters |
|---|---:|---|---:|---:|---:|---:|---:|
| customer | 10,000 | COMPLETED | 28.281s | 425.2 MiB | 9,900/100 | 195,808 | 9,701 |
| product | 10,000 | RESOURCE_LIMIT | 75.591s | 1251.2 MiB | 9,900/100 | 1,240,398 | 9,640 |
| customer | 100,000 | RESOURCE_LIMIT | 16.680s | 1266.2 MiB | 99,000/1,000 | not completed | not completed |
| product | 100,000 | RESOURCE_LIMIT | 18.950s | 1522.0 MiB | 99,000/1,000 | not completed | not completed |

### Customer 10K completed run

Pipeline duration was **28.281 s**, or **353.59 input records/s**. Peak sampled RSS was **425.1 MiB**. SQLite write stages took **21.675s**. Probable matching evaluated **195,808 / 49,000,050** possible pairs, a **99.60% blocking reduction**.

| Stage | Seconds |
|---|---:|
| `initialize_database` | 0.064 |
| `fingerprint_inputs` | 0.014 |
| `create_pipeline_run` | 0.009 |
| `ingest` | 0.053 |
| `validate` | 0.100 |
| `create_quarantine_entries` | 0.002 |
| `normalize` | 0.112 |
| `detect_exact_duplicates` | 0.406 |
| `detect_probable_duplicates` | 4.180 |
| `create_master_records` | 1.319 |
| `generate_review_items` | 0.247 |
| `persist_source_records` | 0.664 |
| `persist_quarantine_entries` | 0.030 |
| `persist_review_items` | 0.012 |
| `persist_duplicate_decisions` | 16.599 |
| `persist_master_records_and_lineage` | 4.366 |
| `persist_pipeline_errors` | 0.005 |
| `update_run_status_and_summary` | 0.006 |
| `generate_report` | 0.011 |

### Product 10K partial run

Probable matching completed in **50.743 s**, evaluating **1,240,398** candidates (blocking reduction **97.47%**): 917 strong, 28,555 review-required and 1,210,926 no-match candidates. The run generated and persisted 29,303 review items and 100 quarantines. At **75.591 s**, sampled memory crossed the threshold while `persist_duplicate_decisions` was active, with 1,240,581 decisions submitted to that stage. It did not complete.

### 100K runs

Both runs ingested 100,000 rows (99,000 accepted and 1,000 rejected), normalized them and completed exact detection. The customer case found 999 exact groups and the product case found 922. Before the memory stop, customer throughput was about 96,392 records/s for ingestion, 54,824/s for validation, 84,524/s for normalization and 18,478/s for exact matching. Product throughput was about 63,881/s, 95,281/s, 95,300/s and 25,061/s for those stages respectively. Both crossed the memory threshold as probable matching began: customer at 16.680 s / 1,266 MiB sampled RSS; product at 18.950 s / 1,522 MiB. The probable stage did not return candidate totals, and neither pipeline completed result persistence.

## Correctness

The completed customer 10K database passed `PRAGMA integrity_check` (`ok`) and `PRAGMA foreign_key_check` (zero violations). Report counts reconciled with SQLite: 10,000 source records, 195,907 duplicate decisions, 9,701 masters, 58,206 lineage rows, 1 review item, 100 quarantine entries and zero operational errors. All 9,900 accepted source IDs appeared exactly once among masters; 100 rejected source rows remained persisted and quarantined. Master IDs were unique.

The other runs are partial and are not claimed as fully reconciled.

## Determinism

Regenerating the 100K datasets with seed 42 reproduced SHA-256 hashes for both source CSVs and both label CSVs. The repeated 10K customer run also produced the same matching and master counts. Synthetic distributions are scenario-driven and should not be interpreted as real-world duplicate prevalence.

## Bottlenecks

**Probable matching candidate materialization** is the primary scaling limit: the product 10K dataset produced over 1.24 million candidates and required about 1.25 GiB RSS; both 100K cases exceeded the guard at probable matching. **Duplicate-decision persistence** is a secondary bottleneck: customer 10K spent 16.599 s persisting 195,907 rows, and its total SQLite write stages took 21.675 s. Product 10K also spent 11.474 s persisting its 29,303 review rows.

No matching or persistence optimization was made from this benchmark; candidate generation needs an accuracy-equivalent streaming design before changing the memory behavior. The code change only adds already available candidate and blocking-reduction metrics to pipeline output.

## Tests

`.\.venv\Scripts\python.exe -m pytest -q`: **215 passed, 1 warning** in 12.50 seconds.

See [`performance_benchmark.json`](performance_benchmark.json) for complete stage timings, hashes, case records and validation details.
