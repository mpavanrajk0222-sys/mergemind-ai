"""Generate, persist, and audit manual duplicate-review candidates."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Literal

from src.database import (
    get_pending_review_items,
    get_review_history,
    get_review_item,
    get_review_items,
    get_review_items_by_entity,
    get_review_statistics,
    save_review_items,
    update_review_item,
)
from src.duplicate_detection import DetectionResult
from src.normalization import NormalizedRecord
from src.probable_matching import ProbableMatchingResult, ProbablePair
from src.survivorship import MasterRecord, SurvivorshipResult


EntityType = Literal["customer", "product"]
ReviewStatus = Literal["PENDING", "ACCEPTED_MATCH", "REJECTED_MATCH", "DEFERRED"]
_AMBIGUOUS_CODES = {"AMBIGUOUS_PROBABLE_COMPONENT", "PROBABLE_MATCH_CONNECTS_EXISTING_GROUPS"}


@dataclass(frozen=True)
class ReviewItem:
    """One deterministic, run-scoped pair requiring a human decision."""

    review_id: str
    run_id: str
    entity_type: EntityType
    record_id_1: str
    record_id_2: str
    confidence_score: float | None
    matching_evidence: dict[str, Any]
    conflicting_evidence: dict[str, Any]
    decision_explanation: str
    review_status: ReviewStatus
    created_at: datetime
    reviewed_at: datetime | None = None
    reviewer_id: str | None = None
    reviewer_notes: str | None = None


def _pair_key(entity_type: str, first: str, second: str) -> tuple[str, str, str]:
    if first == second:
        raise ValueError("Review candidates must reference two distinct source record IDs.")
    first, second = sorted((first, second))
    return entity_type, first, second


def _review_id(entity_type: str, first: str, second: str) -> str:
    identity = json.dumps([entity_type, first, second], ensure_ascii=True, separators=(",", ":"))
    return "review-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def _source_pair_values(
    source_by_id: dict[tuple[str, str], NormalizedRecord],
    entity_type: str,
    first: str,
    second: str,
    fields: Iterable[str],
) -> dict[str, dict[str, Any]]:
    values: dict[str, dict[str, Any]] = {}
    for field in fields:
        pair_values: dict[str, Any] = {}
        for record_id in (first, second):
            record = source_by_id.get((entity_type, record_id))
            if record is not None:
                pair_values[record_id] = record.source_fields.get(field)
        values[field] = pair_values
    return values


def generate_review_items(
    run_id: str,
    *,
    exact_result: DetectionResult | None = None,
    probable_result: ProbableMatchingResult | None = None,
    survivorship_result: SurvivorshipResult | None = None,
    normalized_records: Iterable[NormalizedRecord] = (),
) -> list[ReviewItem]:
    """Collect uncertain pairs without including no-matches or merged pairs.

    Sources include probable ``REVIEW_REQUIRED`` pairs, exact detector conflict
    candidates, and survivorship conflicts describing unsafe probable edges.
    Duplicate candidates from multiple sources become one item with combined
    evidence and explanations.
    """

    source_by_id: dict[tuple[str, str], NormalizedRecord] = {}
    for record in normalized_records:
        source_id = record.source_fields.get("customer_id" if record.entity_type == "customer" else "product_id")
        if isinstance(source_id, str) and source_id.strip():
            source_by_id[(record.entity_type, source_id)] = record

    probable_by_pair: dict[tuple[str, str, str], ProbablePair] = {}
    no_match_pairs: set[tuple[str, str, str]] = set()
    if probable_result is not None:
        for pair in probable_result.probable_pairs:
            key = _pair_key(pair.entity_type, pair.source_record_id_1, pair.source_record_id_2)
            probable_by_pair[key] = pair
            if pair.decision == "NO_MATCH":
                no_match_pairs.add(key)

    exact_pairs: set[tuple[str, str, str]] = set()
    if exact_result is not None:
        exact_pairs.update(
            _pair_key(pair.entity_type, *pair.source_record_ids)
            for pair in exact_result.duplicate_pairs
        )

    merged_pairs: set[tuple[str, str, str]] = set()
    if survivorship_result is not None:
        for master in survivorship_result.master_records:
            if len(master.contributing_source_record_ids) < 2:
                continue
            ids = master.contributing_source_record_ids
            for index, first in enumerate(ids):
                for second in ids[index + 1:]:
                    merged_pairs.add(_pair_key(master.entity_type, first, second))

    pending: dict[tuple[str, str, str], dict[str, Any]] = {}

    def add_candidate(
        entity_type: str,
        first: str,
        second: str,
        source: str,
        explanation: str,
        *,
        confidence: float | None = None,
        matching: dict[str, Any] | None = None,
        conflicting: dict[str, Any] | None = None,
    ) -> None:
        key = _pair_key(entity_type, first, second)
        if key in exact_pairs or key in merged_pairs or key in no_match_pairs:
            return
        entity_type, first, second = key
        item = pending.setdefault(key, {
            "sources": set(), "explanations": set(), "scores": [],
            "matching": {}, "conflicting": {},
        })
        item["sources"].add(source)
        if explanation:
            item["explanations"].add(explanation)
        if confidence is not None:
            item["scores"].append(confidence)
        if matching:
            item["matching"][source] = matching
        if conflicting:
            item["conflicting"][source] = conflicting

    if probable_result is not None:
        for pair in probable_result.review_candidates:
            key = _pair_key(pair.entity_type, pair.source_record_id_1, pair.source_record_id_2)
            if pair.decision != "REVIEW_REQUIRED" or key in exact_pairs:
                continue
            add_candidate(
                pair.entity_type, pair.source_record_id_1, pair.source_record_id_2,
                "PROBABLE_REVIEW_REQUIRED", pair.explanation,
                confidence=pair.overall_confidence_score,
                matching={"matching_attributes": pair.matching_attributes,
                          "similarity_scores": pair.similarity_scores},
                conflicting={"conflicting_attributes": pair.conflicting_attributes},
            )

    if exact_result is not None:
        for candidate in exact_result.conflict_candidates:
            first, second = sorted(candidate.source_record_ids)
            field_values = _source_pair_values(
                source_by_id, candidate.entity_type, first, second, candidate.conflicting_fields
            )
            add_candidate(
                candidate.entity_type, first, second, "EXACT_DETECTION_CONFLICT",
                candidate.explanation,
                conflicting={"conflicting_fields": candidate.conflicting_fields,
                             "field_values_by_record": field_values},
            )

    if survivorship_result is not None:
        for conflict in survivorship_result.unresolved_conflicts:
            if conflict.conflict_code not in _AMBIGUOUS_CODES or len(conflict.source_record_ids) != 2:
                continue
            first, second = conflict.source_record_ids
            pair = probable_by_pair.get(_pair_key(conflict.entity_type, first, second))
            add_candidate(
                conflict.entity_type, first, second, "AMBIGUOUS_PROBABLE_CONNECTION",
                conflict.explanation,
                confidence=None if pair is None else pair.overall_confidence_score,
                matching=None if pair is None else {
                    "matching_attributes": pair.matching_attributes,
                    "similarity_scores": pair.similarity_scores,
                },
                conflicting=None if pair is None else {
                    "conflicting_attributes": pair.conflicting_attributes,
                },
            )

    created_at = datetime.now(timezone.utc)
    result: list[ReviewItem] = []
    for (entity, first, second), item in sorted(pending.items()):
        explanations = sorted(item["explanations"])
        result.append(ReviewItem(
            review_id=_review_id(entity, first, second),
            run_id=run_id,
            entity_type=entity,  # type: ignore[arg-type]
            record_id_1=first,
            record_id_2=second,
            confidence_score=max(item["scores"]) if item["scores"] else None,
            matching_evidence={
                "candidate_sources": sorted(item["sources"]),
                "evidence_by_source": item["matching"],
            },
            conflicting_evidence={
                "candidate_sources": sorted(item["sources"]),
                "evidence_by_source": item["conflicting"],
            },
            decision_explanation=" | ".join(explanations),
            review_status="PENDING",
            created_at=created_at,
        ))
    return result


def save_review_queue(run_id: str, review_items: Iterable[ReviewItem], *, db_path=None) -> int:
    """Persist new review candidates without resetting existing decisions."""

    return save_review_items(run_id, review_items, db_path=db_path)


def retrieve_pending_reviews(run_id: str, *, db_path=None) -> list[dict[str, Any]]:
    return get_pending_review_items(run_id, db_path=db_path)


def retrieve_reviews_by_entity(run_id: str, entity_type: str, *, db_path=None) -> list[dict[str, Any]]:
    return get_review_items_by_entity(run_id, entity_type, db_path=db_path)


def retrieve_review(run_id: str, review_id: str, *, db_path=None) -> dict[str, Any] | None:
    return get_review_item(run_id, review_id, db_path=db_path)


def retrieve_review_history(run_id: str, review_id: str, *, db_path=None) -> list[dict[str, Any]]:
    return get_review_history(run_id, review_id, db_path=db_path)


def accept_review(
    run_id: str, review_id: str, *, reviewer_id: str | None = None, notes: str | None = None, db_path=None
) -> dict[str, Any]:
    return update_review_item(run_id, review_id, "ACCEPTED_MATCH", reviewer_id=reviewer_id,
                              reviewer_notes=notes, db_path=db_path)


def reject_review(
    run_id: str, review_id: str, *, reviewer_id: str | None = None, notes: str | None = None, db_path=None
) -> dict[str, Any]:
    return update_review_item(run_id, review_id, "REJECTED_MATCH", reviewer_id=reviewer_id,
                              reviewer_notes=notes, db_path=db_path)


def defer_review(
    run_id: str, review_id: str, *, reviewer_id: str | None = None, notes: str | None = None, db_path=None
) -> dict[str, Any]:
    return update_review_item(run_id, review_id, "DEFERRED", reviewer_id=reviewer_id,
                              reviewer_notes=notes, db_path=db_path)


def retrieve_review_statistics(run_id: str, *, db_path=None) -> dict[str, Any]:
    return get_review_statistics(run_id, db_path=db_path)


__all__ = (
    "ReviewItem", "generate_review_items", "save_review_queue", "retrieve_pending_reviews",
    "retrieve_reviews_by_entity", "retrieve_review", "retrieve_review_history",
    "accept_review", "reject_review", "defer_review", "retrieve_review_statistics",
)
