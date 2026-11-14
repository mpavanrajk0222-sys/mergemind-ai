"""Command-line entry point for the MergeMind AI pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.database import RunInputMismatchError
from src.pipeline import DEFAULT_CUSTOMER_INPUT, DEFAULT_PRODUCT_INPUT, run_pipeline
from src.structured_logging import configure_logging


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run MergeMind AI ingestion, data quality, entity resolution, survivorship, and persistence."
    )
    parser.add_argument("--customers", type=Path, help="Customer CSV, JSON, or JSONL input (default: bundled customers.csv).")
    parser.add_argument("--products", type=Path, help="Product CSV, JSON, or JSONL input (default: bundled products.csv).")
    parser.add_argument("--run-id", help="Optional run identifier. A unique ID is generated when omitted.")
    parser.add_argument("--database", type=Path, help="SQLite database path (default: data/output/mergemind.db).")
    parser.add_argument("--report-directory", type=Path, default=PROJECT_ROOT / "reports",
                        help="Directory for per-run JSON reports (default: reports/).")
    parser.add_argument("--replace-existing", action="store_true",
                        help="Replace data for this run ID only; by default the run is resumed/upserted.")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
                        default="INFO", help="Minimum structured log level (default: INFO).")
    parser.add_argument("--log-file", type=Path,
                        help="Optional rotating JSON-lines log file. Console logging remains enabled unless --quiet is used.")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress structured console logs; the execution summary is still printed.")
    entity_mode = parser.add_mutually_exclusive_group()
    entity_mode.add_argument("--customers-only", action="store_true", help="Run only the customer input.")
    entity_mode.add_argument("--products-only", action="store_true", help="Run only the product input.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.customers_only and args.products is not None:
        _parser().error("--customers-only cannot be combined with --products.")
    if args.products_only and args.customers is not None:
        _parser().error("--products-only cannot be combined with --customers.")

    customer_path = None if args.products_only else (args.customers or DEFAULT_CUSTOMER_INPUT)
    product_path = None if args.customers_only else (args.products or DEFAULT_PRODUCT_INPUT)
    configure_logging(log_level=args.log_level, log_file=args.log_file, quiet=args.quiet)
    try:
        result = run_pipeline(
            customers_path=customer_path,
            products_path=product_path,
            run_id=args.run_id,
            database_path=args.database,
            report_directory=args.report_directory,
            replace_existing=args.replace_existing,
        )
    except RunInputMismatchError as exc:
        print(f"Cannot resume pipeline run: {exc}", file=sys.stderr)
        return 2
    print(f"MergeMind AI run {result.run_id}: {result.status}")
    print(
        "Customers: "
        f"{result.customer_metrics['records_ingested']} ingested, "
        f"{result.customer_metrics['records_accepted']} accepted, "
        f"{result.customer_metrics['records_rejected']} rejected, "
        f"{result.customer_metrics['master_records_created']} masters"
    )
    print(
        "Products: "
        f"{result.product_metrics['records_ingested']} ingested, "
        f"{result.product_metrics['records_accepted']} accepted, "
        f"{result.product_metrics['records_rejected']} rejected, "
        f"{result.product_metrics['master_records_created']} masters"
    )
    print(
        f"Data-quality issues: {result.data_quality_issue_count} across "
        f"{result.rejected_record_count} rejected records; "
        f"operational errors: {result.operational_error_count}; "
        f"unresolved conflicts: {len(result.unresolved_conflicts)}"
    )
    print(
        f"Reviews: {result.review_statistics['total_review_items']} total, "
        f"{result.review_statistics['pending_reviews']} pending "
        f"(customers {result.review_statistics['customer_review_count']}, "
        f"products {result.review_statistics['product_review_count']})"
    )
    print(
        f"Quarantine: {result.quarantine_statistics['total_quarantined_records']} records "
        f"(customers {result.quarantine_statistics['customer_quarantine_count']}, "
        f"products {result.quarantine_statistics['product_quarantine_count']})"
    )
    print(f"Execution: {result.execution_duration_seconds:.3f}s; report: {result.report_path}")
    return 0 if result.status == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
