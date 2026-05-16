"""
PII / PHI / PCI classifier.

Two-stage detection per column:
    1. Column-name match against curated regex patterns (cheap, high-precision)
    2. Sample-value match where a `sample_rule` is configured (catches unlabeled cols)

Output is a list of `Classification` records that the caller can push to a
catalog (OpenMetadata, DataHub, Collibra) as tags.

Designed to be deterministic and reviewable — adding a new pattern is a PR.
"""

from __future__ import annotations

import logging
import random
import re
from dataclasses import dataclass
from pathlib import Path

import yaml


logger = logging.getLogger(__name__)

RULES_FILE = Path(__file__).parent / "rules" / "pii_patterns.yaml"


@dataclass(frozen=True)
class Classification:
    table_fqn: str
    column: str
    tag: str
    sensitivity: str
    confidence: float
    reason: str


@dataclass(frozen=True)
class Category:
    name: str
    sensitivity: str
    patterns: list[re.Pattern]
    sample_regex: re.Pattern | None
    sample_min_fraction: float


def load_categories(path: Path = RULES_FILE) -> list[Category]:
    raw = yaml.safe_load(path.read_text())
    out: list[Category] = []
    for c in raw["categories"]:
        out.append(
            Category(
                name=c["name"],
                sensitivity=c["sensitivity"],
                patterns=[re.compile(p) for p in c["patterns"]],
                sample_regex=(
                    re.compile(c["sample_rule"]["regex"])
                    if c.get("sample_rule") else None
                ),
                sample_min_fraction=(
                    c["sample_rule"]["min_match_fraction"]
                    if c.get("sample_rule") else 0.0
                ),
            )
        )
    return out


def classify_by_name(column_name: str, categories: list[Category]) -> list[Classification]:
    """High-precision column-name matches."""
    hits = []
    for cat in categories:
        if any(p.search(column_name) for p in cat.patterns):
            hits.append(
                Classification(
                    table_fqn="",  # filled in by caller
                    column=column_name,
                    tag=cat.name,
                    sensitivity=cat.sensitivity,
                    confidence=0.95,
                    reason=f"column-name match: {column_name}",
                )
            )
    return hits


def classify_by_sample(
    column_name: str,
    samples: list[str],
    categories: list[Category],
    sample_size: int = 100,
) -> list[Classification]:
    """Value-based detection for cases column-name didn't flag."""
    if not samples:
        return []

    sampled = random.sample(samples, min(sample_size, len(samples)))
    non_null = [str(s) for s in sampled if s is not None and str(s).strip() != ""]
    if not non_null:
        return []

    hits = []
    for cat in categories:
        if cat.sample_regex is None:
            continue
        matches = sum(1 for v in non_null if cat.sample_regex.fullmatch(v))
        fraction = matches / len(non_null)
        if fraction >= cat.sample_min_fraction:
            hits.append(
                Classification(
                    table_fqn="",
                    column=column_name,
                    tag=cat.name,
                    sensitivity=cat.sensitivity,
                    confidence=round(fraction, 2),
                    reason=f"{matches}/{len(non_null)} sample values matched",
                )
            )
    return hits


def classify_column(
    table_fqn: str,
    column_name: str,
    samples: list[str] | None,
    categories: list[Category],
) -> list[Classification]:
    """Combine name + sample detection. Higher-confidence hit wins per tag."""
    name_hits = classify_by_name(column_name, categories)
    sample_hits = classify_by_sample(column_name, samples or [], categories)

    best_by_tag: dict[str, Classification] = {}
    for h in name_hits + sample_hits:
        h_with_fqn = Classification(
            table_fqn=table_fqn,
            column=h.column,
            tag=h.tag,
            sensitivity=h.sensitivity,
            confidence=h.confidence,
            reason=h.reason,
        )
        existing = best_by_tag.get(h.tag)
        if existing is None or h_with_fqn.confidence > existing.confidence:
            best_by_tag[h.tag] = h_with_fqn
    return list(best_by_tag.values())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cats = load_categories()
    demo_samples = [
        "jane@example.com", "j.smith@example.com", "x@y.io",
        "not-an-email", None, "another@example.com",
    ]
    out = classify_column("snowflake.demo.users", "user_contact", demo_samples, cats)
    for c in out:
        logger.info("%s", c)
