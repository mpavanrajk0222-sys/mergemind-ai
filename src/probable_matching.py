"""Pair-level probable duplicate assessment using RapidFuzz and blocking.

This module consumes normalized records, excludes records already in exact
duplicate groups, and returns pair decisions. It does not form transitive
clusters or merge records.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from itertools import combinations
from typing import Any, Iterable, Literal

from rapidfuzz import fuzz

from src.duplicate_detection import DetectionResult, detect_exact_duplicates
from src.normalization import NormalizedRecord
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA


EntityType = Literal["customer", "product"]
Decision = Literal["STRONG_PROBABLE_MATCH", "REVIEW_REQUIRED", "NO_MATCH"]

# Confidence is a fixed weighted sum on a 0-100 scale. Missing measurements
# contribute zero rather than causing the remaining weights to be rescaled.
_CUSTOMER_CONFIDENCE_WEIGHTS = {"name": 0.45, "email": 0.25, "phone": 0.25, "city": 0.05}
_PRODUCT_CONFIDENCE_WEIGHTS = {
    "product_name": 0.50, "sku": 0.20, "brand": 0.15, "category": 0.10, "price": 0.05
}


@dataclass(frozen=True)
class MatchingConfig:
    """Thresholds and blocking rules; similarity and confidence use 0-100."""

    name_similarity_threshold: float = 88.0
    product_name_similarity_threshold: float = 88.0
    overall_confidence_threshold: float = 75.0
    review_threshold: float = 55.0
    city_conflict_threshold: float = 70.0
    city_support_threshold: float = 85.0
    category_conflict_threshold: float = 60.0
    price_conflict_threshold: float = 50.0
    brand_support_threshold: float = 90.0
    category_support_threshold: float = 85.0
    price_support_threshold: float = 85.0
    sku_conflict_review_margin: float = 5.0
    blocking_prefix_length: int = 4
    customer_blocking_rules: tuple[str, ...] = ("email", "phone", "name_city")
    product_blocking_rules: tuple[str, ...] = ("sku", "brand_name", "category_name")

    def __post_init__(self) -> None:
        numeric_thresholds = (
            self.name_similarity_threshold,
            self.product_name_similarity_threshold,
            self.overall_confidence_threshold,
            self.review_threshold,
            self.city_conflict_threshold,
            self.city_support_threshold,
            self.category_conflict_threshold,
            self.price_conflict_threshold,
            self.brand_support_threshold,
            self.category_support_threshold,
            self.price_support_threshold,
            self.sku_conflict_review_margin,
        )
        if any(not 0.0 <= value <= 100.0 for value in numeric_thresholds):
            raise ValueError("All matching thresholds must be between 0 and 100.")
        if self.review_threshold > self.overall_confidence_threshold:
            raise ValueError("review_threshold must be less than or equal to overall_confidence_threshold.")
        if self.blocking_prefix_length < 1:
            raise ValueError("blocking_prefix_length must be at least 1.")


@dataclass(frozen=True)
class ProbablePair:
    """A scored candidate pair and its conservative pair-level decision."""

    entity_type: EntityType
    source_record_id_1: str
    source_record_id_2: str
    similarity_scores: dict[str, float | None]
    overall_confidence_score: float
    matching_attributes: tuple[str, ...]
    conflicting_attributes: tuple[str, ...]
    decision: Decision
    explanation: str


@dataclass(frozen=True)
class MatchingSummary:
    """Decision counts and blocking effectiveness for a matching batch."""

    entity_type: EntityType
    records_processed: int
    exact_group_records_excluded: int
    total_possible_pairs: int
    candidate_pairs_evaluated: int
    candidate_reduction_percent: float
    strong_probable_count: int
    review_required_count: int
    no_match_count: int


@dataclass(frozen=True)
class ProbableMatchingResult:
    """All scored blocked pairs plus decision-specific views and summary.

    ``probable_pairs`` contains every unique pair evaluated after blocking.
    The other three pair lists are decision-specific views of that collection.
    They do not represent additional pair generation or transitive clusters.
    """

    probable_pairs: list[ProbablePair]
    strong_probable_matches: list[ProbablePair]
    review_candidates: list[ProbablePair]
    non_matches: list[ProbablePair]
    matching_summary: MatchingSummary


def _normalize_entity_type(entity_type: str) -> EntityType:
    normalized = entity_type.lower() if isinstance(entity_type, str) else ""
    if normalized not in ("customer", "product"):
        raise ValueError(f"Unsupported entity type {entity_type!r}; expected 'customer' or 'product'.")
    return normalized  # type: ignore[return-value]


def _source_id(record: NormalizedRecord, entity_type: EntityType) -> str:
    schema = CUSTOMER_SCHEMA if entity_type == "customer" else PRODUCT_SCHEMA
    value = record.source_fields.get(schema.primary_key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"A normalized {entity_type} record has no usable {schema.primary_key}.")
    return value


def _value(record: NormalizedRecord, field: str) -> str | None:
    value = record.normalized_fields.get(field)
    return value if isinstance(value, str) and value.strip() else None


def _similarity(first: str | None, second: str | None) -> float | None:
    if first is None or second is None:
        return None
    return round(float(fuzz.ratio(first, second)), 2)


def _identifier_similarity(first: str | None, second: str | None) -> float | None:
    if first is None or second is None:
        return None
    return 100.0 if first == second else 0.0


def _overall_score(scores: dict[str, float | None], weights: dict[str, float]) -> float:
    # Missing values contribute zero; weights are not re-normalized, so missing
    # attributes cannot increase confidence.
    return round(sum((scores.get(field) or 0.0) * weight for field, weight in weights.items()), 2)


def _decimal(record: NormalizedRecord) -> Decimal | None:
    value = record.source_fields.get("price")
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _price_similarity(first: NormalizedRecord, second: NormalizedRecord) -> float | None:
    first_price, second_price = _decimal(first), _decimal(second)
    if first_price is None or second_price is None:
        return None
    scale = max(abs(first_price), abs(second_price), Decimal(1))
    score = Decimal(100) * (Decimal(1) - abs(first_price - second_price) / scale)
    return round(float(max(Decimal(0), score)), 2)


def _customer_candidate(
    first: NormalizedRecord,
    second: NormalizedRecord,
    config: MatchingConfig,
) -> ProbablePair:
    name_score = _similarity(_value(first, "name"), _value(second, "name")) or 0.0
    email_score = _identifier_similarity(_value(first, "email"), _value(second, "email"))
    phone_score = _identifier_similarity(_value(first, "phone"), _value(second, "phone"))
    city_score = _similarity(_value(first, "city"), _value(second, "city"))
    scores = {"name": name_score, "email": email_score, "phone": phone_score, "city": city_score}
    confidence = _overall_score(scores, _CUSTOMER_CONFIDENCE_WEIGHTS)

    email_1, email_2 = _value(first, "email"), _value(second, "email")
    phone_1, phone_2 = _value(first, "phone"), _value(second, "phone")
    conflicts: set[str] = set()
    if email_1 and email_2 and email_1 != email_2:
        conflicts.add("email")
    if phone_1 and phone_2 and phone_1 != phone_2:
        conflicts.add("phone")
    if city_score is not None and city_score < config.city_conflict_threshold:
        conflicts.add("city")

    email_match = email_score == 100.0
    phone_match = phone_score == 100.0
    identifier_match = email_match or phone_match
    both_identifiers_match = email_match and phone_match
    if both_identifiers_match and name_score < config.name_similarity_threshold:
        conflicts.add("name")
    matching: set[str] = set()
    if name_score >= config.name_similarity_threshold:
        matching.add("name")
    if email_match:
        matching.add("email")
    if phone_match:
        matching.add("phone")
    if city_score is not None and city_score >= config.city_support_threshold:
        matching.add("city")

    if (
        identifier_match
        and name_score >= config.name_similarity_threshold
        and not conflicts
        and confidence >= config.overall_confidence_threshold
    ):
        decision: Decision = "STRONG_PROBABLE_MATCH"
        explanation = "Name similarity and at least one matching contact identifier provide independent support without conflicting attributes."
    elif (
        (both_identifiers_match and name_score < config.name_similarity_threshold)
        or (identifier_match and confidence >= config.review_threshold)
    ):
        decision = "REVIEW_REQUIRED"
        explanation = "Shared contact evidence is present, but similarity, confidence, or a conflicting attribute prevents an automatic probable match."
    else:
        decision = "NO_MATCH"
        explanation = "The blocked pair lacks sufficient independent evidence for a probable match."

    ids = sorted((_source_id(first, "customer"), _source_id(second, "customer")))
    return ProbablePair(
        entity_type="customer",
        source_record_id_1=ids[0],
        source_record_id_2=ids[1],
        similarity_scores=scores,
        overall_confidence_score=confidence,
        matching_attributes=tuple(sorted(matching)),
        conflicting_attributes=tuple(sorted(conflicts)),
        decision=decision,
        explanation=explanation,
    )


def _product_candidate(
    first: NormalizedRecord,
    second: NormalizedRecord,
    config: MatchingConfig,
) -> ProbablePair:
    name_score = _similarity(_value(first, "product_name"), _value(second, "product_name")) or 0.0
    sku_score = _identifier_similarity(_value(first, "sku"), _value(second, "sku"))
    brand_score = _similarity(_value(first, "brand"), _value(second, "brand"))
    category_score = _similarity(_value(first, "category"), _value(second, "category"))
    price_score = _price_similarity(first, second)
    scores = {
        "product_name": name_score,
        "sku": sku_score,
        "brand": brand_score,
        "category": category_score,
        "price": price_score,
    }
    confidence = _overall_score(scores, _PRODUCT_CONFIDENCE_WEIGHTS)

    sku_1, sku_2 = _value(first, "sku"), _value(second, "sku")
    brand_1, brand_2 = _value(first, "brand"), _value(second, "brand")
    conflicts: set[str] = set()
    if sku_1 and sku_2 and sku_1 != sku_2:
        conflicts.add("sku")
    if brand_1 and brand_2 and brand_1 != brand_2:
        conflicts.add("brand")
    if sku_1 and sku_1 == sku_2 and name_score < 100.0:
        conflicts.add("product_name")
    if category_score is not None and category_score < config.category_conflict_threshold:
        conflicts.add("category")
    if price_score is not None and price_score < config.price_conflict_threshold:
        conflicts.add("price")

    sku_match = sku_score == 100.0
    brand_support = brand_score is not None and brand_score >= config.brand_support_threshold
    category_support = category_score is not None and category_score >= config.category_support_threshold
    price_support = price_score is not None and price_score >= config.price_support_threshold
    matching: set[str] = set()
    if name_score >= config.product_name_similarity_threshold:
        matching.add("product_name")
    if sku_match:
        matching.add("sku")
    if brand_support:
        matching.add("brand")
    if category_support:
        matching.add("category")
    if price_support:
        matching.add("price")

    independent_support_count = sum((sku_match, brand_support, category_support, price_support))
    if (
        name_score >= config.product_name_similarity_threshold
        and confidence >= config.overall_confidence_threshold
        and independent_support_count >= 2
        and not conflicts
    ):
        decision: Decision = "STRONG_PROBABLE_MATCH"
        explanation = "Product-name similarity is supported by at least two independent SKU, brand, category, or price signals."
    elif sku_match:
        decision = "REVIEW_REQUIRED"
        explanation = "The same SKU appears on a pair not resolved by exact matching; inspect brand and product-name differences."
    elif (
        "sku" in conflicts
        and confidence >= config.overall_confidence_threshold
        and name_score >= min(
            100.0,
            config.product_name_similarity_threshold + config.sku_conflict_review_margin,
        )
        and sum((brand_support, category_support, price_support)) >= 2
    ) or (
        "sku" not in conflicts
        and confidence >= config.review_threshold
        and name_score >= config.product_name_similarity_threshold
        and sum((brand_support, category_support, price_support)) >= 2
    ):
        decision = "REVIEW_REQUIRED"
        explanation = "The pair shares product evidence, but conflicting identifiers or insufficient confidence require manual review."
    else:
        decision = "NO_MATCH"
        explanation = "The blocked pair lacks sufficient independent product evidence for a probable match."

    ids = sorted((_source_id(first, "product"), _source_id(second, "product")))
    return ProbablePair(
        entity_type="product",
        source_record_id_1=ids[0],
        source_record_id_2=ids[1],
        similarity_scores=scores,
        overall_confidence_score=confidence,
        matching_attributes=tuple(sorted(matching)),
        conflicting_attributes=tuple(sorted(conflicts)),
        decision=decision,
        explanation=explanation,
    )


def _name_tokens(record: NormalizedRecord, field: str) -> set[str]:
    value = _value(record, field)
    if not value:
        return set()
    return {token for token in value.split() if len(token) >= 3 and not token.isdecimal()}


def _blocking_keys(record: NormalizedRecord, entity_type: EntityType, config: MatchingConfig) -> set[tuple[str, ...]]:
    keys: set[tuple[str, ...]] = set()
    if entity_type == "customer":
        rules = config.customer_blocking_rules
        for rule in rules:
            if rule == "email":
                value = _value(record, "email")
                if value:
                    keys.add((rule, value))
            elif rule == "phone":
                value = _value(record, "phone")
                if value:
                    keys.add((rule, value))
            elif rule in ("name_prefix", "name_city"):
                name = _value(record, "name")
                city = _value(record, "city")
                if name:
                    prefix = name.split()[0][:config.blocking_prefix_length]
                    if rule == "name_prefix":
                        keys.add((rule, prefix))
                    elif city:
                        keys.add((rule, prefix, city))
            else:
                raise ValueError(f"Unknown customer blocking rule {rule!r}.")
        return keys

    rules = config.product_blocking_rules
    for rule in rules:
        if rule == "sku":
            value = _value(record, "sku")
            if value:
                keys.add((rule, value))
        elif rule == "name_prefix":
            for token in _name_tokens(record, "product_name"):
                keys.add((rule, token[:config.blocking_prefix_length]))
        elif rule in ("brand_name", "category_name"):
            attribute = "brand" if rule == "brand_name" else "category"
            value = _value(record, attribute)
            if value:
                for token in _name_tokens(record, "product_name"):
                    keys.add((rule, value, token[:config.blocking_prefix_length]))
        else:
            raise ValueError(f"Unknown product blocking rule {rule!r}.")
    return keys


def _validate_blocking_rules(config: MatchingConfig, entity_type: EntityType) -> None:
    allowed = {
        "customer": {"email", "phone", "name_prefix", "name_city"},
        "product": {"sku", "name_prefix", "brand_name", "category_name"},
    }[entity_type]
    selected = config.customer_blocking_rules if entity_type == "customer" else config.product_blocking_rules
    unknown = sorted(set(selected) - allowed)
    if unknown:
        raise ValueError(f"Unsupported {entity_type} blocking rule(s): {', '.join(unknown)}.")


def _candidate_index_pairs(
    records: list[NormalizedRecord],
    entity_type: EntityType,
    config: MatchingConfig,
    excluded_ids: set[str],
) -> set[tuple[int, int]]:
    blocks: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for index, record in enumerate(records):
        if _source_id(record, entity_type) in excluded_ids:
            continue
        for key in _blocking_keys(record, entity_type, config):
            blocks[key].append(index)
    candidates: set[tuple[int, int]] = set()
    for key in sorted(blocks):
        for first, second in combinations(blocks[key], 2):
            candidates.add((first, second) if first < second else (second, first))
    return candidates


def _record_sort_key(record: NormalizedRecord, entity_type: EntityType) -> tuple[Any, ...]:
    return (
        _source_id(record, entity_type),
        record.source_file,
        record.source_record_number,
        str(record.source_fields.get("source_system", "")),
        record.source_format,
        record.ingested_at.isoformat(),
        tuple(sorted((str(key), repr(value)) for key, value in record.normalized_fields.items())),
    )


def detect_probable_duplicates(
    records: Iterable[NormalizedRecord],
    entity_type: str,
    *,
    config: MatchingConfig | None = None,
    exact_result: DetectionResult | None = None,
) -> ProbableMatchingResult:
    """Score blocked record pairs and classify strong, review, or no-match.

    Pass normalized records from accepted validation output. If no exact result
    is supplied, the exact detector is run first. Members of exact duplicate
    groups are excluded from the probable candidate pool. Every scored result
    remains a pair; no transitive groups are inferred.
    """

    normalized_entity = _normalize_entity_type(entity_type)
    active_config = config or MatchingConfig()
    _validate_blocking_rules(active_config, normalized_entity)
    source_records = list(records)
    seen_ids: set[str] = set()
    for batch_number, record in enumerate(source_records, start=1):
        if not isinstance(record, NormalizedRecord):
            raise TypeError(f"Input record {batch_number} must be a NormalizedRecord.")
        if record.entity_type != normalized_entity:
            raise ValueError(
                f"Input record {batch_number} has entity type {record.entity_type!r}; "
                f"matching batch is for {normalized_entity!r}."
            )
        source_id = _source_id(record, normalized_entity)
        if source_id in seen_ids:
            raise ValueError(f"Duplicate source identifier {source_id!r} in probable-matching input.")
        seen_ids.add(source_id)

    source_records.sort(key=lambda record: _record_sort_key(record, normalized_entity))
    if exact_result is None:
        exact_result = detect_exact_duplicates(source_records, normalized_entity)
    elif exact_result.detection_summary.entity_type != normalized_entity:
        raise ValueError("The supplied exact-detection result belongs to a different entity type.")

    exact_member_ids = {
        source_id
        for group in exact_result.duplicate_groups
        for source_id in group.source_record_ids
    }
    candidate_pairs = _candidate_index_pairs(source_records, normalized_entity, active_config, exact_member_ids)
    score_candidate = _customer_candidate if normalized_entity == "customer" else _product_candidate
    pair_results = [
        score_candidate(source_records[first], source_records[second], active_config)
        for first, second in sorted(
            candidate_pairs,
            key=lambda pair: (
                _source_id(source_records[pair[0]], normalized_entity),
                _source_id(source_records[pair[1]], normalized_entity),
            ),
        )
    ]
    strong_matches = [pair for pair in pair_results if pair.decision == "STRONG_PROBABLE_MATCH"]
    review_candidates = [pair for pair in pair_results if pair.decision == "REVIEW_REQUIRED"]
    non_matches = [pair for pair in pair_results if pair.decision == "NO_MATCH"]
    total_possible_pairs = len(source_records) * (len(source_records) - 1) // 2
    reduction = (
        round(100.0 * (1.0 - len(pair_results) / total_possible_pairs), 2)
        if total_possible_pairs
        else 0.0
    )
    summary = MatchingSummary(
        entity_type=normalized_entity,
        records_processed=len(source_records),
        exact_group_records_excluded=len(exact_member_ids & seen_ids),
        total_possible_pairs=total_possible_pairs,
        candidate_pairs_evaluated=len(pair_results),
        candidate_reduction_percent=reduction,
        strong_probable_count=len(strong_matches),
        review_required_count=len(review_candidates),
        no_match_count=len(non_matches),
    )
    return ProbableMatchingResult(pair_results, strong_matches, review_candidates, non_matches, summary)


match_probable_duplicates = detect_probable_duplicates


__all__ = (
    "MatchingConfig",
    "ProbablePair",
    "MatchingSummary",
    "ProbableMatchingResult",
    "detect_probable_duplicates",
    "match_probable_duplicates",
)
