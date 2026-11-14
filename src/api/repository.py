from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from src.database import DEFAULT_DATABASE_PATH
from src.api.models import DashboardKpis, DashboardResponse, PipelineActivity


class DashboardDataUnavailable(RuntimeError):
    """The configured database cannot provide dashboard data."""


def _database_path() -> Path:
    configured = os.environ.get("MERGEMIND_DATABASE_PATH")
    return Path(configured).expanduser().resolve() if configured else DEFAULT_DATABASE_PATH


def _open_readonly(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise DashboardDataUnavailable("The configured MergeMind database is unavailable.")
    try:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection
    except sqlite3.Error as exc:
        raise DashboardDataUnavailable("The configured MergeMind database is unavailable.") from exc


def _exact_group_count(connection: sqlite3.Connection, cutoff: str) -> int:
    edges = connection.execute(
        "SELECT p.run_id, p.entity_type, p.record_id_1, p.record_id_2 "
        "FROM duplicate_pairs p JOIN pipeline_runs r ON r.run_id = p.run_id "
        "WHERE p.decision = ? AND julianday(r.started_at) >= julianday(?)",
        ("EXACT", cutoff),
    ).fetchall()
    parents: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    sizes: dict[tuple[str, str, str], int] = defaultdict(int)

    def find(item: tuple[str, str, str]) -> tuple[str, str, str]:
        parents.setdefault(item, item)
        if parents[item] != item:
            parents[item] = find(parents[item])
        return parents[item]

    for edge in edges:
        first = (edge["run_id"], edge["entity_type"], edge["record_id_1"])
        second = (edge["run_id"], edge["entity_type"], edge["record_id_2"])
        first_root = find(first)
        second_root = find(second)
        if first_root != second_root:
            parents[second_root] = first_root
    for item in parents:
        sizes[find(item)] += 1
    return sum(size > 1 for size in sizes.values())


def _activity_for_run(
    row: sqlite3.Row,
    entities: list[str],
    exact_pairs: int,
    probable_decisions: dict[str, int],
) -> PipelineActivity:
    return PipelineActivity(
        run_id=row["run_id"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        status=row["status"],
        source_records=int(row["source_records"] or 0),
        accepted_records=int(row["accepted_records"] or 0),
        rejected_records=int(row["rejected_records"] or 0),
        master_records=int(row["master_records"] or 0),
        entities=entities,
        exact_duplicate_pairs=exact_pairs,
        probable_match_decisions=probable_decisions,
    )


def get_dashboard(days: int = 30, limit: int = 10, offset: int = 0) -> DashboardResponse:
    """Aggregate persisted pipeline data using a read-only SQLite connection.

    Counts are run-scoped persisted artifacts within the requested date window.
    Re-running identical input under another run ID therefore contributes a
    separate run's records, matching the database's run-scoped storage model.
    """

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    connection = _open_readonly(_database_path())
    try:
        required = {"pipeline_runs", "source_records", "duplicate_pairs", "master_records"}
        existing = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = ?", ("table",))
        }
        if not required.issubset(existing):
            raise DashboardDataUnavailable("The configured database does not contain required MergeMind run data.")

        total_runs = int(connection.execute(
            "SELECT COUNT(*) FROM pipeline_runs "
            "WHERE julianday(started_at) >= julianday(?)",
            (cutoff,),
        ).fetchone()[0])
        source_counts = connection.execute(
            "SELECT s.validation_status, COUNT(*) AS count FROM source_records s "
            "JOIN pipeline_runs r ON r.run_id = s.run_id "
            "WHERE julianday(r.started_at) >= julianday(?) GROUP BY s.validation_status",
            (cutoff,),
        ).fetchall()
        source_by_status = {row["validation_status"]: int(row["count"]) for row in source_counts}
        decision_rows = connection.execute(
            "SELECT p.decision, COUNT(*) AS count FROM duplicate_pairs p "
            "JOIN pipeline_runs r ON r.run_id = p.run_id "
            "WHERE julianday(r.started_at) >= julianday(?) GROUP BY p.decision",
            (cutoff,),
        ).fetchall()
        decision_counts = {row["decision"]: int(row["count"]) for row in decision_rows}
        master_records = int(connection.execute(
            "SELECT COUNT(*) FROM master_records m JOIN pipeline_runs r ON r.run_id = m.run_id "
            "WHERE julianday(r.started_at) >= julianday(?)",
            (cutoff,),
        ).fetchone()[0])
        exact_pairs = decision_counts.get("EXACT", 0)
        review_available = "review_items" in existing
        quarantine_available = "quarantine_records" in existing
        pending_reviews = None
        quarantine_records = None
        if review_available:
            pending_reviews = int(connection.execute(
                "SELECT COUNT(*) FROM review_items i JOIN pipeline_runs r ON r.run_id = i.run_id "
                "WHERE i.review_status = ? AND julianday(r.started_at) >= julianday(?)",
                ("PENDING", cutoff),
            ).fetchone()[0])
        if quarantine_available:
            quarantine_records = int(connection.execute(
                "SELECT COUNT(*) FROM quarantine_records q JOIN pipeline_runs r ON r.run_id = q.run_id "
                "WHERE q.quarantine_status = ? AND julianday(r.started_at) >= julianday(?)",
                ("QUARANTINED", cutoff),
            ).fetchone()[0])

        run_rows = connection.execute(
            "SELECT r.run_id, r.started_at, r.completed_at, r.status, "
            "(SELECT COUNT(*) FROM source_records s WHERE s.run_id = r.run_id) AS source_records, "
            "(SELECT COUNT(*) FROM source_records s WHERE s.run_id = r.run_id AND s.validation_status = 'ACCEPTED') AS accepted_records, "
            "(SELECT COUNT(*) FROM source_records s WHERE s.run_id = r.run_id AND s.validation_status = 'REJECTED') AS rejected_records, "
            "(SELECT COUNT(*) FROM master_records m WHERE m.run_id = r.run_id) AS master_records "
            "FROM pipeline_runs r WHERE julianday(r.started_at) >= julianday(?) "
            "ORDER BY r.started_at DESC, r.run_id DESC LIMIT ? OFFSET ?",
            (cutoff, limit, offset),
        ).fetchall()
        activity_rows = connection.execute(
            "SELECT r.run_id, r.started_at, r.completed_at, r.status, "
            "(SELECT COUNT(*) FROM source_records s WHERE s.run_id = r.run_id) AS source_records, "
            "(SELECT COUNT(*) FROM source_records s WHERE s.run_id = r.run_id AND s.validation_status = 'ACCEPTED') AS accepted_records, "
            "(SELECT COUNT(*) FROM source_records s WHERE s.run_id = r.run_id AND s.validation_status = 'REJECTED') AS rejected_records, "
            "(SELECT COUNT(*) FROM master_records m WHERE m.run_id = r.run_id) AS master_records "
            "FROM pipeline_runs r WHERE julianday(r.started_at) >= julianday(?) "
            "ORDER BY r.started_at DESC, r.run_id DESC LIMIT 90",
            (cutoff,),
        ).fetchall()
        entity_rows = connection.execute(
            "SELECT s.run_id, s.entity_type, COUNT(*) AS count FROM source_records s "
            "JOIN pipeline_runs r ON r.run_id = s.run_id "
            "WHERE julianday(r.started_at) >= julianday(?) GROUP BY s.run_id, s.entity_type",
            (cutoff,),
        ).fetchall()
        entity_names: dict[str, list[str]] = defaultdict(list)
        for row in entity_rows:
            if row["count"]:
                entity_names[row["run_id"]].append(row["entity_type"])
        run_decision_rows = connection.execute(
            "SELECT p.run_id, p.decision, COUNT(*) AS count FROM duplicate_pairs p "
            "JOIN pipeline_runs r ON r.run_id = p.run_id "
            "WHERE julianday(r.started_at) >= julianday(?) GROUP BY p.run_id, p.decision",
            (cutoff,),
        ).fetchall()
        run_decisions: dict[str, dict[str, int]] = defaultdict(dict)
        for row in run_decision_rows:
            run_decisions[row["run_id"]][row["decision"]] = int(row["count"])
        decision_names = ("STRONG_PROBABLE_MATCH", "REVIEW_REQUIRED", "NO_MATCH")
        all_activity = [
            _activity_for_run(
                row,
                entity_names[row["run_id"]],
                run_decisions[row["run_id"]].get("EXACT", 0),
                {name: run_decisions[row["run_id"]].get(name, 0) for name in decision_names},
            )
            for row in activity_rows
        ]
        paged_runs = [
            _activity_for_run(
                row,
                entity_names[row["run_id"]],
                run_decisions[row["run_id"]].get("EXACT", 0),
                {name: run_decisions[row["run_id"]].get(name, 0) for name in decision_names},
            )
            for row in run_rows
        ]

        total_sources = sum(source_by_status.values())
        accepted = source_by_status.get("ACCEPTED", 0)
        rejected = source_by_status.get("REJECTED", 0)
        probable_counts = {name: decision_counts.get(name, 0) for name in decision_names}
        return DashboardResponse(
            database_status="available",
            review_data_available=review_available,
            quarantine_data_available=quarantine_available,
            generated_at=datetime.now(timezone.utc).isoformat(),
            days=days,
            kpis=DashboardKpis(
                source_records=total_sources,
                accepted_records=accepted,
                rejected_records=rejected,
                master_records=master_records,
                exact_duplicate_groups=_exact_group_count(connection, cutoff),
                exact_duplicate_pairs=exact_pairs,
                probable_match_decisions=probable_counts,
                pending_reviews=pending_reviews,
                quarantine_records=quarantine_records,
                accepted_rate_percent=round(accepted * 100 / total_sources, 2) if total_sources else None,
            ),
            total_runs=total_runs,
            limit=limit,
            offset=offset,
            runs=paged_runs,
            activity=all_activity,
        )
    except sqlite3.Error as exc:
        raise DashboardDataUnavailable("Dashboard data could not be read from the configured database.") from exc
    finally:
        connection.close()
