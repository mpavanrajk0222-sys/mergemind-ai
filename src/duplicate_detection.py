"""Deterministic exact-duplicate detection over normalized records.

This module identifies exact groups and review-worthy identifier conflicts.
It does not perform fuzzy matching, choose masters, or modify source records.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from typing import Any, Iterable, Literal

from src.normalization import NormalizedRecord
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA


EntityType = Literal["customer", "product"]
MatchType = Literal["EXACT"]
RecommendedAction = Literal["REVIEW"]


@dataclass(frozen=True)
class DuplicateGroup:
    """A set of records sharing the required deterministic exact evidence."""

    group_id: str
    entity_type: EntityType
    source_record_ids: tuple[str, ...]
    matching_attributes: tuple[str, ...]
    matching_evidence: dict[str, str]
    match_type: MatchType = "EXACT"


@dataclass(frozen=True)
class DuplicatePair:
    """One unique record pair contained in an exact duplicate group."""

    entity_type: EntityType
    source_record_ids: tuple[str, str]
    group_id: str
    match_type: MatchType = "EXACT"


@dataclass(frozen=True)
class ConflictCandidate:
    """A blocked pair with contradictory identifiers that needs review."""

    entity_type: EntityType
    source_record_ids: tuple[str, str]
    conflicting_fields: tuple[str, ...]
    explanation: str
    recommended_action: RecommendedAction = "REVIEW"


@dataclass(frozen=True)
class DetectionSummary:
    """Counts from one exact-detection batch."""

    entity_type: EntityType
    records_processed: int
    duplicate_group_count: int
    duplicate_record_count: int
    duplicate_pair_count: int
    conflict_candidate_count: int
    unmatched_record_count: int


@dataclass(frozen=True)
class DetectionResult:
    """Structured exact groups, pairs, review candidates, and unmatched input."""

    duplicate_groups: list[DuplicateGroup]
    duplicate_pairs: list[DuplicatePair]
    conflict_candidates: list[ConflictCandidate]
    unmatched_records: list[NormalizedRecord]
    detection_summary: DetectionSummary


def _entity_type(value: str) -> EntityType:
    normalized = value.lower() if isinstance(value, str) else ""
    if normalized not in ("customer", "product"):
        raise ValueError(f"Unsupported entity type {value!r}; expected 'customer' or 'product'.")
    return normalized  # type: ignore[return-value]


def _text(record: NormalizedRecord, field: str) -> str | None:
    value = record.normalized_fields.get(field)
    return value if isinstance(value, str) and value.strip() else None


def _source_id(record: NormalizedRecord, entity_type: EntityType) -> str:
    schema = CUSTOMER_SCHEMA if entity_type == "customer" else PRODUCT_SCHEMA
    value = record.source_fields.get(schema.primary_key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Normalized {entity_type} record from {record.source_file!r} "
            f"record {record.source_record_number} has no usable source identifier."
        )
    return value


def _stable_record_key(record: NormalizedRecord, entity_type: EntityType) -> tuple[Any, ...]:
    source_id = _source_id(record, entity_type)
    stable_source = json.dumps(record.source_fields, sort_keys=True, default=str, ensure_ascii=True)
    stable_normalized = json.dumps(record.normalized_fields, sort_keys=True, ensure_ascii=True)
    timestamp = record.ingested_at.isoformat() if isinstance(record.ingested_at, datetime) else str(record.ingested_at)
    return (
        source_id,
        str(record.source_fields.get("source_system", "")),
        record.source_file,
        record.source_record_number,
        record.source_format,
        timestamp,
        stable_normalized,
        stable_source,
    )


def _group_id(entity_type: EntityType, source_ids: tuple[str, ...]) -> str:
    stable_payload = json.dumps([entity_type, list(source_ids)], separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()[:20]
    return f"{entity_type}-exact-{digest}"


def _make_group(
    entity_type: EntityType,
    records: list[NormalizedRecord],
    indices: list[int],
    matching_attributes: tuple[str, ...],
) -> DuplicateGroup:
    member_ids = tuple(sorted(_source_id(records[index], entity_type) for index in indices))
    evidence = {
        attribute: _text(records[indices[0]], attribute) or ""
        for attribute in matching_attributes
    }
    return DuplicateGroup(
        group_id=_group_id(entity_type, member_ids),
        entity_type=entity_type,
        source_record_ids=member_ids,
        matching_attributes=matching_attributes,
        matching_evidence=evidence,
    )


def _pair_ids(records: list[NormalizedRecord], entity_type: EntityType, pair: tuple[int, int]) -> tuple[str, str]:
    first_id = _source_id(records[pair[0]], entity_type)
    second_id = _source_id(records[pair[1]], entity_type)
    return (first_id, second_id) if first_id <= second_id else (second_id, first_id)


def _add_conflict(
    conflicts: dict[tuple[int, int], dict[str, set[str]]],
    first: int,
    second: int,
    field: str,
    explanation: str,
) -> None:
    pair = (first, second) if first < second else (second, first)
    details = conflicts.setdefault(pair, {"fields": set(), "explanations": set()})
    details["fields"].add(field)
    details["explanations"].add(explanation)


def _customer_result(records: list[NormalizedRecord]) -> DetectionResult:
    exact_blocks: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    email_phone_blocks: dict[tuple[str, str], list[int]] = defaultdict(list)
    email_name_blocks: dict[tuple[str, str], list[int]] = defaultdict(list)
    phone_name_blocks: dict[tuple[str, str], list[int]] = defaultdict(list)

    for index, record in enumerate(records):
        email = _text(record, "email")
        phone = _text(record, "phone")
        name = _text(record, "name")
        if email and phone and name:
            exact_blocks[(email, phone, name)].append(index)
            email_phone_blocks[(email, phone)].append(index)
            email_name_blocks[(email, name)].append(index)
            phone_name_blocks[(phone, name)].append(index)

    groups: list[DuplicateGroup] = []
    group_members: set[int] = set()
    for key in sorted(exact_blocks):
        indices = exact_blocks[key]
        if len(indices) >= 2:
            groups.append(_make_group("customer", records, indices, ("email", "phone", "name")))
            group_members.update(indices)

    conflicts: dict[tuple[int, int], dict[str, set[str]]] = {}
    for (email, phone), indices in sorted(email_phone_blocks.items()):
        for first, second in combinations(indices, 2):
            first_name, second_name = _text(records[first], "name"), _text(records[second], "name")
            if first_name != second_name:
                _add_conflict(conflicts, first, second, "name", "The same normalized email and phone are associated with conflicting normalized names.")
    for (email, name), indices in sorted(email_name_blocks.items()):
        for first, second in combinations(indices, 2):
            first_phone, second_phone = _text(records[first], "phone"), _text(records[second], "phone")
            if first_phone and second_phone and first_phone != second_phone:
                _add_conflict(conflicts, first, second, "phone", "The same normalized email and name are associated with different non-empty phones.")
    for (phone, name), indices in sorted(phone_name_blocks.items()):
        for first, second in combinations(indices, 2):
            first_email, second_email = _text(records[first], "email"), _text(records[second], "email")
            if first_email and second_email and first_email != second_email:
                _add_conflict(conflicts, first, second, "email", "The same normalized phone and name are associated with different non-empty emails.")

    groups.sort(key=lambda group: group.group_id)
    duplicate_pairs = _pairs_from_groups(records, "customer", groups)
    conflict_candidates = _conflict_results(records, "customer", conflicts)
    return _assemble_result("customer", records, groups, duplicate_pairs, conflict_candidates, group_members)


def _product_result(records: list[NormalizedRecord]) -> DetectionResult:
    sku_blocks: dict[str, list[int]] = defaultdict(list)
    for index, record in enumerate(records):
        sku = _text(record, "sku")
        if sku:
            sku_blocks[sku].append(index)

    conflicts: dict[tuple[int, int], dict[str, set[str]]] = {}
    groups: list[DuplicateGroup] = []
    group_members: set[int] = set()

    for sku in sorted(sku_blocks):
        sku_indices = sku_blocks[sku]
        # Any same-SKU disagreement in brand or product name is review-worthy.
        for first, second in combinations(sku_indices, 2):
            first_brand, second_brand = _text(records[first], "brand"), _text(records[second], "brand")
            first_name, second_name = _text(records[first], "product_name"), _text(records[second], "product_name")
            if first_brand and second_brand and first_brand != second_brand:
                _add_conflict(conflicts, first, second, "brand", "The same normalized SKU is associated with conflicting non-empty brands.")
            if first_name and second_name and first_name != second_name:
                _add_conflict(conflicts, first, second, "product_name", "The same normalized SKU is associated with conflicting normalized product names.")

        by_name: dict[str, list[int]] = defaultdict(list)
        for index in sku_indices:
            name = _text(records[index], "product_name")
            if name:
                by_name[name].append(index)

        for product_name in sorted(by_name):
            name_indices = by_name[product_name]
            by_brand: dict[str | None, list[int]] = defaultdict(list)
            for index in name_indices:
                by_brand[_text(records[index], "brand")].append(index)
            nonempty_brands = sorted(brand for brand in by_brand if brand)

            if len(nonempty_brands) <= 1:
                compatible_indices = sorted(index for indices in by_brand.values() for index in indices)
                if len(compatible_indices) >= 2:
                    attributes = ("sku", "product_name")
                    if nonempty_brands and all(_text(records[index], "brand") == nonempty_brands[0] for index in compatible_indices):
                        attributes += ("brand",)
                    groups.append(_make_group("product", records, compatible_indices, attributes))
                    group_members.update(compatible_indices)
            else:
                # Separate known brands into disjoint groups; an unknown brand
                # cannot safely be assigned to more than one conflicting group.
                for brand in nonempty_brands:
                    indices = by_brand[brand]
                    if len(indices) >= 2:
                        groups.append(_make_group("product", records, indices, ("sku", "product_name", "brand")))
                        group_members.update(indices)
                for unknown_index in by_brand.get(None, []):
                    for brand in nonempty_brands:
                        for known_index in by_brand[brand]:
                            _add_conflict(
                                conflicts,
                                unknown_index,
                                known_index,
                                "brand",
                                "A missing brand cannot disambiguate this SKU/name because multiple brands are present; review the source records.",
                            )

    groups.sort(key=lambda group: group.group_id)
    duplicate_pairs = _pairs_from_groups(records, "product", groups)
    conflict_candidates = _conflict_results(records, "product", conflicts)
    return _assemble_result("product", records, groups, duplicate_pairs, conflict_candidates, group_members)


def _pairs_from_groups(
    records: list[NormalizedRecord], entity_type: EntityType, groups: list[DuplicateGroup]
) -> list[DuplicatePair]:
    pairs: list[DuplicatePair] = []
    index_by_id = {_source_id(record, entity_type): index for index, record in enumerate(records)}
    seen: set[tuple[str, str]] = set()
    for group in groups:
        indices = [index_by_id[source_id] for source_id in group.source_record_ids]
        for first, second in combinations(indices, 2):
            pair_ids = _pair_ids(records, entity_type, (first, second))
            if pair_ids in seen:
                continue
            seen.add(pair_ids)
            pairs.append(DuplicatePair(entity_type, pair_ids, group.group_id))
    return sorted(pairs, key=lambda pair: (pair.group_id, pair.source_record_ids))


def _conflict_results(
    records: list[NormalizedRecord],
    entity_type: EntityType,
    conflicts: dict[tuple[int, int], dict[str, set[str]]],
) -> list[ConflictCandidate]:
    candidates = [
        ConflictCandidate(
            entity_type=entity_type,
            source_record_ids=_pair_ids(records, entity_type, pair),
            conflicting_fields=tuple(sorted(details["fields"])),
            explanation=" ".join(sorted(details["explanations"])),
        )
        for pair, details in conflicts.items()
    ]
    unique: dict[tuple[str, str], ConflictCandidate] = {}
    for candidate in candidates:
        previous = unique.get(candidate.source_record_ids)
        if previous is None:
            unique[candidate.source_record_ids] = candidate
        else:
            unique[candidate.source_record_ids] = ConflictCandidate(
                entity_type=entity_type,
                source_record_ids=candidate.source_record_ids,
                conflicting_fields=tuple(sorted(set(previous.conflicting_fields) | set(candidate.conflicting_fields))),
                explanation=" ".join(sorted(set(previous.explanation.split(". ")) | set(candidate.explanation.split(". ")))),
            )
    return sorted(unique.values(), key=lambda candidate: (candidate.source_record_ids, candidate.conflicting_fields))


def _assemble_result(
    entity_type: EntityType,
    records: list[NormalizedRecord],
    groups: list[DuplicateGroup],
    pairs: list[DuplicatePair],
    conflicts: list[ConflictCandidate],
    grouped_indices: set[int],
) -> DetectionResult:
    conflicted_ids = {
        record_id
        for candidate in conflicts
        for record_id in candidate.source_record_ids
    }
    unmatched = [
        record for index, record in enumerate(records)
        if index not in grouped_indices and _source_id(record, entity_type) not in conflicted_ids
    ]
    unmatched.sort(key=lambda record: _stable_record_key(record, entity_type))
    duplicate_record_count = sum(len(group.source_record_ids) for group in groups)
    summary = DetectionSummary(
        entity_type=entity_type,
        records_processed=len(records),
        duplicate_group_count=len(groups),
        duplicate_record_count=duplicate_record_count,
        duplicate_pair_count=len(pairs),
        conflict_candidate_count=len(conflicts),
        unmatched_record_count=len(unmatched),
    )
    return DetectionResult(groups, pairs, conflicts, unmatched, summary)


def detect_exact_duplicates(records: Iterable[NormalizedRecord], entity_type: str) -> DetectionResult:
    """Find exact duplicate groups and identifier conflicts for one entity type.

    Input records should be normalized records from accepted validation output.
    Blocking indexes avoid comparing unrelated records; pair combinations are
    generated only within shared-identifier blocks. Results are sorted so the
    same batch yields identical output regardless of input order.
    """

    normalized_entity = _entity_type(entity_type)
    source_records = list(records)
    seen_source_ids: set[str] = set()
    for batch_number, record in enumerate(source_records, start=1):
        if not isinstance(record, NormalizedRecord):
            raise TypeError(f"Input record {batch_number} must be a NormalizedRecord.")
        if record.entity_type != normalized_entity:
            raise ValueError(
                f"Input record {batch_number} has entity type {record.entity_type!r}; "
                f"detection batch is for {normalized_entity!r}."
            )
        source_id = _source_id(record, normalized_entity)
        if source_id in seen_source_ids:
            raise ValueError(
                f"Duplicate source identifier {source_id!r} in detection input; "
                "validate the combined batch before duplicate detection."
            )
        seen_source_ids.add(source_id)

    source_records.sort(key=lambda record: _stable_record_key(record, normalized_entity))
    if normalized_entity == "customer":
        return _customer_result(source_records)
    return _product_result(source_records)


__all__ = (
    "DuplicateGroup",
    "DuplicatePair",
    "ConflictCandidate",
    "DetectionSummary",
    "DetectionResult",
    "detect_exact_duplicates",
)
