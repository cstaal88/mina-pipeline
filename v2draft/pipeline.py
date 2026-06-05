"""
RSS Pipeline v2 — Main entrypoint.

Fetches RSS feeds, applies global filters, and stores in NeonDB.
Replaces both fetch-raw.py and clean.py from v1.

Usage:
    python pipeline.py              # fetch and store
    python pipeline.py --dry-run    # fetch and filter, but don't write to DB
"""

import argparse
import sys

from langdetect import detect, LangDetectException

from config import GLOBAL_FILTERS
from fetch import fetch_rss
from db import ensure_schema, upsert_articles


# ── Global filters ───────────────────────────────────────────────────

def is_english(text: str) -> bool:
    """Check if text is English. Returns True if too short to detect."""
    if not text or len(text) < 20:
        return True
    try:
        return detect(text) == "en"
    except LangDetectException:
        return True


def passes_global_filters(article: dict) -> tuple[bool, str]:
    """
    Check article against global filters.
    Returns (passed, reason) — reason is empty string if passed.
    """
    title = (article.get("title") or "").lower()
    desc = (article.get("description") or "").lower()

    if GLOBAL_FILTERS.get("require_description") and not desc.strip():
        return False, "no_description"

    if GLOBAL_FILTERS.get("require_english"):
        text = f"{article.get('title', '')} {article.get('description', '')}"
        if not is_english(text):
            return False, "not_english"

    for term in GLOBAL_FILTERS.get("promo_terms", []):
        if term in title or term in desc:
            return False, f"promo:{term}"

    return True, ""


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RSS Pipeline v2")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and filter but don't write to DB")
    args = parser.parse_args()

    # Fetch
    print("Fetching RSS feeds...")
    raw = fetch_rss()
    print(f"Fetched {len(raw)} articles total\n")

    # Filter
    print("Applying global filters...")
    passed = []
    reasons = {}
    for article in raw:
        ok, reason = passes_global_filters(article)
        if ok:
            passed.append(article)
        else:
            reasons[reason] = reasons.get(reason, 0) + 1

    print(f"  passed: {len(passed)}")
    for reason, count in sorted(reasons.items()):
        print(f"  filtered ({reason}): {count}")
    print()

    if args.dry_run:
        print("Dry run — skipping DB write")
        print(f"Would insert {len(passed)} articles")
        return

    # Store
    print("Writing to database...")
    ensure_schema()
    inserted = upsert_articles(passed)
    skipped = len(passed) - inserted

    print(f"  new: {inserted}")
    print(f"  already existed: {skipped}")
    print("Done.")


if __name__ == "__main__":
    main()
