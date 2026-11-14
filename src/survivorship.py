"""Create deterministic master records from confirmed duplicate groups.

Inputs are accepted normalized records plus exact groups and probable pair
decisions. Review-required and no-match pairs are never merged. Probable edges
are merged only as isolated pairs; an edge that would extend/connect groups or
participate in a multi-edge component is sent for review to avoid transitivity.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Literal

from src.duplicate_detection import DetectionResult
from src.normalization import NormalizedRecord
from src.probable_matching import ProbableMatchingResult, ProbablePair
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA


EntityType = Literal["customer", "product"]
MergeMethod = Literal["EXACT", "STRONG_PROBABLE", "SINGLETON"]
RecommendedAction = Literal["REVIEW"]


@dataclass(frozen=True)
class RejectedFieldValue:
    """A non-selected source assertion retained for field-level lineage."""

    value: Any
    source_record_id: str
    source_system: str


@dataclass(frozen=True)
class FieldSurvivorshipDecision:
    """Selected field value and the provenance and alternatives behind it."""

    selected_value: Any
    selected_source_record_id: str | None
    selected_source_system: str | None
    selection_rule: str
    rejected_conflicting_values: tuple[RejectedFieldValue, ...] = ()


@dataclass(frozen=True)
class MasterRecord:
    """A deterministic master representation with source-level lineage."""

    master_id: str
    entity_type: EntityType
    merged_fields: dict[str, Any]
    contributing_source_record_ids: tuple[str, ...]
    contributing_source_systems: tuple[str, ...]
    field_level_decisions: dict[str, FieldSurvivorshipDecision]
    merge_method: MergeMethod
    created_at: datetime


@dataclass(frozen=True)
class UnresolvedSurvivorshipConflict:
    """A pair or group that must not be automatically combined."""

    entity_type: EntityType
    source_record_ids: tuple[str, ...]
    conflict_code: str
    explanation: str
    recommended_action: RecommendedAction = "REVIEW"


@dataclass(frozen=True)
class SurvivorshipSummary:
    entity_type: EntityType
    source_record_count: int
    master_record_count: int
    merged_group_count: int
    singleton_count: int
    unresolved_conflict_count: int


@dataclass(frozen=True)
class SurvivorshipResult:
    master_records: list[MasterRecord]
    unresolved_conflicts: list[UnresolvedSurvivorshipConflict]
    summary: SurvivorshipSummary


def _entity_type(value: str) -> EntityType:
    normalized = value.lower() if isinstance(value, str) else ""
    if normalized not in ("customer", "product"):
        raise ValueError(f"Unsupported entity type {value!r}; expected 'customer' or 'product'.")
    return normalized  # type: ignore[return-value]


def _schema(entity_type: EntityType):
    return CUSTOMER_SCHEMA if entity_type == "customer" else PRODUCT_SCHEMA


def _record_id(record: NormalizedRecord, entity_type: EntityType) -> str:
    value = record.source_fields.get(_schema(entity_type).primary_key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"A normalized {entity_type} record has no usable source primary key.")
    return value


def _source_system(record: NormalizedRecord) -> str:
    value = record.source_fields.get("source_system")
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _non_empty(value: Any) -> bool:
    return value is not None and not (isinstance(value, str) and not value.strip())


def _raw_value(record: NormalizedRecord, field: str) -> Any:
    value = record.source_fields.get(field)
    return value if _non_empty(value) else None


def _comparison_value(record: NormalizedRecord, field: str) -> Any:
    if field == "price":
        value = _raw_value(record, field)
        if value is None or isinstance(value, bool):
            return None
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return decimal_value if decimal_value.is_finite() else None
    normalized = record.normalized_fields.get(field)
    if _non_empty(normalized):
        return normalized
    return _raw_value(record, field)


def _timestamp_value(record: NormalizedRecord) -> datetime | None:
    value = record.source_fields.get("updated_at")
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def _timestamps_have_mixed_timezone_awareness(records: list[NormalizedRecord]) -> bool:
    modes = {
        timestamp.utcoffset() is not None
        for record in records
        if (timestamp := _timestamp_value(record)) is not None
    }
    return len(modes) > 1


def _timestamp_order(timestamp: datetime) -> datetime:
    if timestamp.utcoffset() is None:
        return timestamp
    return timestamp.astimezone(timezone.utc)


def _stable_tie_key(record: NormalizedRecord, entity_type: EntityType) -> tuple[str, str, str, int]:
    return (
        _source_system(record),
        _record_id(record, entity_type),
        record.source_file,
        record.source_record_number,
    )


def _latest_record(
    records: list[NormalizedRecord], entity_type: EntityType
) -> tuple[NormalizedRecord | None, bool]:
    timestamped = [(record, _timestamp_value(record)) for record in records]
    timestamped = [(record, timestamp) for record, timestamp in timestamped if timestamp is not None]
    if not timestamped:
        return None, False
    mixed_awareness = _timestamps_have_mixed_timezone_awareness([record for record, _ in timestamped])
    if mixed_awareness:
        return None, True
    latest = max(_timestamp_order(timestamp) for _, timestamp in timestamped)
    candidates = [record for record, timestamp in timestamped if _timestamp_order(timestamp) == latest]
    return min(candidates, key=lambda record: _stable_tie_key(record, entity_type)), False


def _completeness(record: NormalizedRecord, field: str) -> tuple[int, int]:
    value = record.normalized_fields.get(field)
    if not isinstance(value, str) or not value.strip():
        raw = _raw_value(record, field)
        value = raw if isinstance(raw, str) else ""
    tokens = value.split()
    return (len(tokens), len(value)) if tokens else (-1, -1)


def _distinct_nonempty_values(records: list[NormalizedRecord], field: str) -> set[Any]:
    return {
        comparison
        for record in records
        if (comparison := _comparison_value(record, field)) is not None
    }


def _rejected_values(
    records: list[NormalizedRecord],
    field: str,
    selected_comparison: Any | None,
    *,
    include_all: bool = False,
) -> tuple[RejectedFieldValue, ...]:
    rejected: list[RejectedFieldValue] = []
    for record in records:
        value = _raw_value(record, field)
        comparison = _comparison_value(record, field)
        if value is None or comparison is None:
            continue
        if include_all or comparison != selected_comparison:
            rejected.append(RejectedFieldValue(value, _record_id(record, record.entity_type), _source_system(record)))
    return tuple(sorted(rejected, key=lambda item: (item.source_system, item.source_record_id, repr(item.value))))


def _select_latest(
    records: list[NormalizedRecord],
    entity_type: EntityType,
    field: str,
    rule: str,
) -> tuple[Any, FieldSurvivorshipDecision, bool]:
    candidates = [record for record in records if _raw_value(record, field) is not None]
    if not candidates:
        return None, FieldSurvivorshipDecision(None, None, None, "no_non_empty_value"), False
    selected, mixed_awareness = _latest_record(candidates, entity_type)
    if mixed_awareness:
        rejected = _rejected_values(records, field, None, include_all=True)
        decision = FieldSurvivorshipDecision(
            None, None, None, "unresolved_mixed_timezone_awareness; no recency winner selected", rejected
        )
        return None, decision, True
    if selected is None:
        # Validated records should have an updated_at value. If not, do not
        # choose an arbitrary field value; retain the alternatives for review.
        decision = FieldSurvivorshipDecision(
            None, None, None, "unresolved_missing_recency_timestamp", _rejected_values(records, field, None, include_all=True)
        )
        return None, decision, True
    comparison = _comparison_value(selected, field)
    decision = FieldSurvivorshipDecision(
        selected_value=_raw_value(selected, field),
        selected_source_record_id=_record_id(selected, entity_type),
        selected_source_system=_source_system(selected),
        selection_rule=rule,
        rejected_conflicting_values=_rejected_values(records, field, comparison),
    )
    return decision.selected_value, decision, False


def _select_most_complete(
    records: list[NormalizedRecord],
    entity_type: EntityType,
    field: str,
    rule: str,
) -> tuple[Any, FieldSurvivorshipDecision, bool]:
    candidates = [record for record in records if _raw_value(record, field) is not None]
    if not candidates:
        return None, FieldSurvivorshipDecision(None, None, None, "no_non_empty_value"), False
    completeness = max(_completeness(record, field) for record in candidates)
    candidates = [record for record in candidates if _completeness(record, field) == completeness]
    selected, mixed_awareness = _latest_record(candidates, entity_type)
    if mixed_awareness:
        selected = min(candidates, key=lambda record: _stable_tie_key(record, entity_type))
        ambiguity = True
        rule = f"{rule}; mixed_timezone_recency_unresolved; tie_break_source_system_then_id"
    else:
        ambiguity = False
    if selected is None:
        selected = min(candidates, key=lambda record: _stable_tie_key(record, entity_type))
    comparison = _comparison_value(selected, field)
    decision = FieldSurvivorshipDecision(
        selected_value=_raw_value(selected, field),
        selected_source_record_id=_record_id(selected, entity_type),
        selected_source_system=_source_system(selected),
        selection_rule=rule,
        rejected_conflicting_values=_rejected_values(records, field, comparison),
    )
    return decision.selected_value, decision, ambiguity


def _select_sku(
    records: list[NormalizedRecord], entity_type: EntityType
) -> tuple[Any, FieldSurvivorshipDecision, bool]:
    candidates = [record for record in records if _raw_value(record, "sku") is not None]
    distinct = _distinct_nonempty_values(records, "sku")
    if len(distinct) > 1:
        decision = FieldSurvivorshipDecision(
            selected_value=None,
            selected_source_record_id=None,
            selected_source_system=None,
            selection_rule="unresolved_conflicting_skus; no SKU selected automatically",
            rejected_conflicting_values=_rejected_values(records, "sku", None, include_all=True),
        )
        return None, decision, True
    if not candidates:
        return None, FieldSurvivorshipDecision(None, None, None, "no_non_empty_value"), False
    selected, ambiguous = _latest_record(candidates, entity_type)
    if ambiguous:
        decision = FieldSurvivorshipDecision(
            None, None, None, "unresolved_mixed_timezone_awareness; no SKU selected", 
            _rejected_values(records, "sku", None, include_all=True),
        )
        return None, decision, True
    assert selected is not None
    value = _raw_value(selected, "sku")
    decision = FieldSurvivorshipDecision(
        value, _record_id(selected, entity_type), _source_system(selected),
        "same_normalized_sku; latest_source_updated_at_then_source_system_and_id",
        _rejected_values(records, "sku", _comparison_value(selected, "sku")),
    )
    return value, decision, False


def _select_price(
    records: list[NormalizedRecord], entity_type: EntityType
) -> tuple[Any, FieldSurvivorshipDecision, bool]:
    candidates = [record for record in records if _comparison_value(record, "price") is not None]
    if not candidates:
        return None, FieldSurvivorshipDecision(None, None, None, "no_non_empty_value"), False
    selected, mixed_awareness = _latest_record(candidates, entity_type)
    if mixed_awareness:
        decision = FieldSurvivorshipDecision(
            None, None, None, "unresolved_mixed_timezone_awareness; no price selected", 
            _rejected_values(records, "price", None, include_all=True),
        )
        return None, decision, True
    if selected is None:
        decision = FieldSurvivorshipDecision(
            None, None, None, "unresolved_missing_recency_timestamp; no price selected",
            _rejected_values(records, "price", None, include_all=True),
        )
        return None, decision, True
    selected_comparison = _comparison_value(selected, "price")
    value = _raw_value(selected, "price")
    decision = FieldSurvivorshipDecision(
        value,
        _record_id(selected, entity_type),
        _source_system(selected),
        "latest_source_updated_at_then_source_system_and_id; price conflicts retained in lineage",
        _rejected_values(records, "price", selected_comparison),
    )
    return value, decision, False


def _master_id(entity_type: EntityType, source_ids: tuple[str, ...]) -> str:
    payload = json.dumps([entity_type, list(source_ids)], separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"{entity_type}-master-{digest}"


def _created_at(records: list[NormalizedRecord]) -> datetime:
    # Ingestion timestamps are UTC-aware by the ingestion module. Fall back to
    # a deterministic ISO-string order for manually supplied mixed metadata.
    try:
        return max(record.ingested_at for record in records)
    except TypeError:
        return max(records, key=lambda record: record.ingested_at.isoformat()).ingested_at


def _build_master(
    records: list[NormalizedRecord], entity_type: EntityType, merge_method: MergeMethod
) -> tuple[MasterRecord, list[UnresolvedSurvivorshipConflict]]:
    records = sorted(records, key=lambda record: _stable_tie_key(record, entity_type))
    source_ids = tuple(sorted(_record_id(record, entity_type) for record in records))
    systems = tuple(sorted({_source_system(record) for record in records if _source_system(record)}))
    fields = (
        ("name", "email", "phone", "city", "address", "updated_at")
        if entity_type == "customer"
        else ("product_name", "sku", "brand", "category", "price", "updated_at")
    )
    merged_fields: dict[str, Any] = {}
    decisions: dict[str, FieldSurvivorshipDecision] = {}
    unresolved: list[UnresolvedSurvivorshipConflict] = []
    for field in fields:
        if field == "updated_at":
            value, decision, ambiguous = _select_latest(
                records, entity_type, field, "latest_valid_source_timestamp_then_source_system_and_id"
            )
            if ambiguous:
                unresolved.append(UnresolvedSurvivorshipConflict(
                    entity_type, source_ids, "AMBIGUOUS_TIMESTAMP_RECENCY",
                    "Timezone-aware and timezone-naive update timestamps cannot be ordered without a timezone policy.",
                ))
        elif entity_type == "customer" and field in ("email", "phone"):
            value, decision, ambiguous = _select_latest(
                records, entity_type, field, f"latest_valid_non_empty_{field}_by_source_updated_at"
            )
            if ambiguous:
                unresolved.append(UnresolvedSurvivorshipConflict(
                    entity_type, source_ids, "AMBIGUOUS_TIMESTAMP_RECENCY",
                    f"Cannot select {field} by recency because the group mixes timezone-aware and timezone-naive timestamps.",
                ))
        elif entity_type == "product" and field == "sku":
            value, decision, conflict = _select_sku(records, entity_type)
            if conflict and len(_distinct_nonempty_values(records, "sku")) > 1:
                unresolved.append(UnresolvedSurvivorshipConflict(
                    entity_type, source_ids, "CONFLICTING_SKUS",
                    "The group contains multiple non-empty normalized SKUs; no SKU was selected automatically.",
                ))
        elif entity_type == "product" and field == "price":
            value, decision, ambiguous = _select_price(records, entity_type)
            if ambiguous:
                unresolved.append(UnresolvedSurvivorshipConflict(
                    entity_type, source_ids, "AMBIGUOUS_TIMESTAMP_RECENCY",
                    "Cannot select price by recency because the group mixes timezone-aware and timezone-naive timestamps.",
                ))
        else:
            value, decision, ambiguous = _select_most_complete(
                records, entity_type, field, "most_complete_value_then_latest_source_updated_at_then_source_system_and_id"
            )
            if ambiguous:
                unresolved.append(UnresolvedSurvivorshipConflict(
                    entity_type, source_ids, "AMBIGUOUS_TIMESTAMP_RECENCY",
                    f"The {field} completeness tie could not use recency because timestamp timezone awareness differs.",
                ))
        merged_fields[field] = value
        decisions[field] = decision

    master = MasterRecord(
        master_id=_master_id(entity_type, source_ids),
        entity_type=entity_type,
        merged_fields=merged_fields,
        contributing_source_record_ids=source_ids,
        contributing_source_systems=systems,
        field_level_decisions=decisions,
        merge_method=merge_method,
        created_at=_created_at(records),
    )
    return master, unresolved


def _as_probable_inputs(
    probable_result: ProbableMatchingResult | Iterable[ProbablePair] | None,
) -> tuple[list[ProbablePair], list[ProbablePair]]:
    if probable_result is None:
        return [], []
    if isinstance(probable_result, ProbableMatchingResult):
        return list(probable_result.strong_probable_matches), list(probable_result.review_candidates)
    pairs = list(probable_result)
    strong = [pair for pair in pairs if pair.decision == "STRONG_PROBABLE_MATCH"]
    review = [pair for pair in pairs if pair.decision == "REVIEW_REQUIRED"]
    return strong, review


def _unresolved_pair(
    entity_type: EntityType,
    record_ids: Iterable[str],
    code: str,
    explanation: str,
) -> UnresolvedSurvivorshipConflict:
    return UnresolvedSurvivorshipConflict(
        entity_type=entity_type,
        source_record_ids=tuple(sorted(set(record_ids))),
        conflict_code=code,
        explanation=explanation,
    )


def create_master_records(
    records: Iterable[NormalizedRecord],
    entity_type: str,
    *,
    exact_result: DetectionResult | None = None,
    probable_result: ProbableMatchingResult | Iterable[ProbablePair] | None = None,
) -> SurvivorshipResult:
    """Create deterministic masters from validated normalized source records.

    Exact groups are confirmed. Strong probable matches are merged only when
    they form isolated pairs of otherwise ungrouped records. A strong edge that
    touches an exact group, connects two established groups, or shares a node
    with another strong edge is retained as an unresolved review conflict.
    Review-required and no-match pairs are never merged.
    """

    normalized_entity = _entity_type(entity_type)
    source_records = list(records)
    by_id: dict[str, NormalizedRecord] = {}
    for batch_number, record in enumerate(source_records, start=1):
        if not isinstance(record, NormalizedRecord):
            raise TypeError(f"Input record {batch_number} must be a NormalizedRecord.")
        if record.entity_type != normalized_entity:
            raise ValueError(
                f"Input record {batch_number} has entity type {record.entity_type!r}; "
                f"survivorship batch is for {normalized_entity!r}."
            )
        record_id = _record_id(record, normalized_entity)
        if record_id in by_id:
            raise ValueError(f"Duplicate source record ID {record_id!r} in survivorship input.")
        by_id[record_id] = record

    exact_groups = []
    unresolved: list[UnresolvedSurvivorshipConflict] = []
    if exact_result is not None:
        if exact_result.detection_summary.entity_type != normalized_entity:
            raise ValueError("The supplied exact-detection result belongs to a different entity type.")
        exact_groups = list(exact_result.duplicate_groups)
        for candidate in exact_result.conflict_candidates:
            unresolved.append(_unresolved_pair(
                normalized_entity,
                candidate.source_record_ids,
                "EXACT_DETECTION_CONFLICT",
                candidate.explanation,
            ))

    exact_groups.sort(key=lambda group: group.group_id)
    group_membership: dict[str, str] = {}
    invalid_group_ids: set[str] = set()
    for group in exact_groups:
        if group.entity_type != normalized_entity:
            raise ValueError("The exact-detection result contains a group for another entity type.")
        missing_ids = set(group.source_record_ids) - set(by_id)
        if missing_ids:
            invalid_group_ids.add(group.group_id)
            unresolved.append(_unresolved_pair(
                normalized_entity, group.source_record_ids, "EXACT_GROUP_RECORD_MISSING",
                f"Exact group references source records not present in the survivorship input: {', '.join(sorted(missing_ids))}.",
            ))
        for record_id in group.source_record_ids:
            if record_id in group_membership:
                invalid_group_ids.update((group.group_id, group_membership[record_id]))
                unresolved.append(_unresolved_pair(
                    normalized_entity,
                    group.source_record_ids,
                    "OVERLAPPING_EXACT_GROUPS",
                    f"Source record {record_id!r} appears in more than one exact group; overlapping groups were not merged.",
                ))
            else:
                group_membership[record_id] = group.group_id

    invalid_group_members = {
        record_id for record_id, group_id in group_membership.items() if group_id in invalid_group_ids
    }
    master_plans: list[tuple[MergeMethod, list[NormalizedRecord]]] = []
    for group in exact_groups:
        if group.group_id in invalid_group_ids:
            continue
        master_plans.append(("EXACT", [by_id[record_id] for record_id in group.source_record_ids]))

    exact_member_ids = {
        record_id for record_id, group_id in group_membership.items() if group_id not in invalid_group_ids
    }
    strong_pairs, review_pairs = _as_probable_inputs(probable_result)
    for pair in review_pairs:
        if pair.entity_type != normalized_entity:
            raise ValueError("The probable-matching result contains a pair for another entity type.")
        unresolved.append(_unresolved_pair(
            normalized_entity,
            (pair.source_record_id_1, pair.source_record_id_2),
            "REVIEW_REQUIRED_PAIR",
            pair.explanation,
        ))

    edge_candidates: list[ProbablePair] = []
    for pair in strong_pairs:
        if pair.entity_type != normalized_entity:
            raise ValueError("The probable-matching result contains a pair for another entity type.")
        first_id, second_id = pair.source_record_id_1, pair.source_record_id_2
        if first_id == second_id:
            unresolved.append(_unresolved_pair(normalized_entity, (first_id,), "INVALID_PROBABLE_PAIR", "A probable pair cannot connect a record to itself."))
            continue
        missing_ids = {first_id, second_id} - set(by_id)
        if missing_ids:
            unresolved.append(_unresolved_pair(
                normalized_entity, (first_id, second_id), "PROBABLE_PAIR_RECORD_MISSING",
                f"Strong probable pair references records not present in the input: {', '.join(sorted(missing_ids))}.",
            ))
            continue
        if first_id in invalid_group_members or second_id in invalid_group_members:
            unresolved.append(_unresolved_pair(
                normalized_entity, (first_id, second_id), "OVERLAPPING_EXACT_GROUPS",
                "The probable pair touches a source record in overlapping exact groups; it was not merged.",
            ))
            continue
        first_group = group_membership.get(first_id)
        second_group = group_membership.get(second_id)
        if first_group or second_group:
            if first_group == second_group:
                continue
            unresolved.append(_unresolved_pair(
                normalized_entity, (first_id, second_id), "PROBABLE_MATCH_CONNECTS_EXISTING_GROUPS",
                "The strong probable pair would connect an exact group or singleton to another established group; review is required to avoid unsafe transitive merging.",
            ))
            continue
        edge_candidates.append(pair)

    edge_candidates.sort(key=lambda pair: tuple(sorted((pair.source_record_id_1, pair.source_record_id_2))))
    degree: dict[str, int] = {}
    for pair in edge_candidates:
        degree[pair.source_record_id_1] = degree.get(pair.source_record_id_1, 0) + 1
        degree[pair.source_record_id_2] = degree.get(pair.source_record_id_2, 0) + 1
    probable_grouped_ids: set[str] = set()
    for pair in edge_candidates:
        pair_ids = tuple(sorted((pair.source_record_id_1, pair.source_record_id_2)))
        if degree[pair_ids[0]] > 1 or degree[pair_ids[1]] > 1:
            unresolved.append(_unresolved_pair(
                normalized_entity, pair_ids, "AMBIGUOUS_PROBABLE_COMPONENT",
                "This strong probable pair shares a record with another strong pair; pairwise evidence does not justify a transitive master group.",
            ))
            continue
        master_plans.append(("STRONG_PROBABLE", [by_id[pair_ids[0]], by_id[pair_ids[1]]]))
        probable_grouped_ids.update(pair_ids)

    occupied_ids = set(exact_member_ids) | probable_grouped_ids
    for record_id in sorted(set(by_id) - occupied_ids):
        master_plans.append(("SINGLETON", [by_id[record_id]]))

    masters: list[MasterRecord] = []
    for merge_method, group_records in master_plans:
        master, field_conflicts = _build_master(group_records, normalized_entity, merge_method)
        masters.append(master)
        unresolved.extend(field_conflicts)
    masters.sort(key=lambda master: master.master_id)

    unique_unresolved: dict[tuple[str, tuple[str, ...]], UnresolvedSurvivorshipConflict] = {}
    for conflict in unresolved:
        key = (conflict.conflict_code, conflict.source_record_ids)
        unique_unresolved.setdefault(key, conflict)
    unresolved = sorted(
        unique_unresolved.values(),
        key=lambda conflict: (conflict.conflict_code, conflict.source_record_ids, conflict.explanation),
    )
    merged_count = sum(len(master.contributing_source_record_ids) > 1 for master in masters)
    summary = SurvivorshipSummary(
        entity_type=normalized_entity,
        source_record_count=len(source_records),
        master_record_count=len(masters),
        merged_group_count=merged_count,
        singleton_count=sum(master.merge_method == "SINGLETON" for master in masters),
        unresolved_conflict_count=len(unresolved),
    )
    return SurvivorshipResult(masters, unresolved, summary)


__all__ = (
    "RejectedFieldValue",
    "FieldSurvivorshipDecision",
    "MasterRecord",
    "UnresolvedSurvivorshipConflict",
    "SurvivorshipSummary",
    "SurvivorshipResult",
    "create_master_records",
)
