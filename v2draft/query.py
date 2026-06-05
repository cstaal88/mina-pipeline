"""
Topic query utility — filter stored articles by topic keywords.

Usage:
    python query.py --topic example-topic --days 30
    python query.py --topic example-topic --days 7 --format jsonl > export.jsonl
    python query.py --topic example-topic --since 2025-01-01 --until 2025-01-31
"""

import argparse
import json
import sys
from datetime import date, timedelta

from config import TOPICS
from db import query_articles


def matches_strict_keywords(article: dict, keywords: list[str]) -> bool:
    """
    Strict keyword matching (same logic as v1 clean.py):
      - keyword in title → match
      - keyword appears 2+ times in description → match
    """
    title = (article.get("title") or "").lower()
    desc = (article.get("description") or "").lower()

    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower in title:
            return True
        if desc.count(kw_lower) >= 2:
            return True

    return False


def has_excluded_terms(article: dict, exclude_terms: list[str]) -> bool:
    """Check if article contains any excluded terms."""
    title = (article.get("title") or "").lower()
    desc = (article.get("description") or "").lower()

    for term in exclude_terms:
        if term.lower() in title or term.lower() in desc:
            return True
    return False


def dedup_articles(articles: list[dict]) -> list[dict]:
    """
    Content-based dedup (same as v1):
      - exact (title, description) match → skip
      - same long title (>40 chars) → skip (catches AP wire syndication)
    """
    seen_content = set()
    seen_titles = set()
    result = []

    for a in articles:
        title = (a.get("title") or "").strip().lower()
        desc = (a.get("description") or "").strip().lower()

        if (title, desc) in seen_content:
            continue
        if title and len(title) > 40 and title in seen_titles:
            continue

        seen_content.add((title, desc))
        if title and len(title) > 40:
            seen_titles.add(title)
        result.append(a)

    return result


def serialize(article: dict) -> dict:
    """Convert article to JSON-serializable dict."""
    out = {}
    for k, v in article.items():
        if isinstance(v, (date,)):
            out[k] = v.isoformat()
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def main():
    parser = argparse.ArgumentParser(description="Query articles by topic")
    parser.add_argument("--topic", required=True, help="Topic name from config.py")
    parser.add_argument("--days", type=int, default=30, help="Look back N days (default: 30)")
    parser.add_argument("--since", help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--until", help="End date YYYY-MM-DD")
    parser.add_argument("--format", choices=["summary", "jsonl"], default="summary",
                        help="Output format (default: summary)")
    args = parser.parse_args()

    if args.topic not in TOPICS:
        print(f"Unknown topic: {args.topic}", file=sys.stderr)
        print(f"Available: {', '.join(TOPICS.keys())}", file=sys.stderr)
        sys.exit(1)

    topic = TOPICS[args.topic]
    keywords = topic["keywords"]
    exclude_terms = topic.get("exclude_terms", [])

    # Date range
    since = args.since or (date.today() - timedelta(days=args.days)).isoformat()
    until = args.until

    # Fetch from DB
    articles = query_articles(since=since, until=until)

    # Apply topic filters
    matched = []
    for a in articles:
        if not matches_strict_keywords(a, keywords):
            continue
        if exclude_terms and has_excluded_terms(a, exclude_terms):
            continue
        matched.append(a)

    # Dedup
    matched = dedup_articles(matched)

    # Output
    if args.format == "jsonl":
        for a in matched:
            print(json.dumps(serialize(a)))
    else:
        print(f"Topic: {args.topic}")
        print(f"Date range: {since} to {until or 'now'}")
        print(f"Total articles in DB: {len(articles)}")
        print(f"Matched: {len(matched)}")
        print()

        # Breakdown by outlet
        by_outlet = {}
        for a in matched:
            outlet = a.get("outlet", "unknown")
            by_outlet[outlet] = by_outlet.get(outlet, 0) + 1

        if by_outlet:
            print("By outlet:")
            for outlet, count in sorted(by_outlet.items(), key=lambda x: -x[1]):
                print(f"  {outlet}: {count}")
            print()

        # Show recent articles
        for a in matched[:20]:
            pub = a.get("publish_date", "?")
            outlet = a.get("outlet", "?")
            title = a.get("title", "(no title)")
            print(f"  [{pub}] [{outlet}] {title}")

        if len(matched) > 20:
            print(f"  ... and {len(matched) - 20} more")


if __name__ == "__main__":
    main()
