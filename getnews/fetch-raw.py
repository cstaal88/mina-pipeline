#!/usr/bin/env python3
"""
Fetch stories from RSS feeds.

Usage:
    python fetch-raw.py              # Fetch all outlets
    python fetch-raw.py --test       # Limit to 3 per outlet for testing

Output:
    data/stories.json - All fetched stories with metadata
"""

import gzip
import html
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.request import Request, urlopen

import xmltodict

from config import (
    OUTLETS,
    ACTIVE_OUTLETS,
    DAYS_BACK,
    MAX_STORIES,
    MAX_PER_OUTLET,
    USER_AGENT,
    RAW_STORIES_FILE,
)


def parse_date(date_str: str | None) -> datetime | None:
    """Parse various RSS date formats into datetime."""
    if not date_str:
        return None
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def format_date(date_str: str | None) -> str | None:
    """Convert RSS date to YYYY-MM-DD format."""
    dt = parse_date(date_str)
    if dt:
        return dt.strftime("%Y-%m-%d")
    return None


def http_get(url: str, timeout: float = 15.0) -> bytes:
    """Fetch URL with gzip handling."""
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
    req = Request(url, headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return raw


def strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_rss(max_per_outlet: int | None = None) -> list[dict[str, Any]]:
    """Fetch stories from all configured RSS feeds."""
    outlet_keys = ACTIVE_OUTLETS or list(OUTLETS.keys())
    limit = max_per_outlet or MAX_PER_OUTLET

    cutoff = None
    if DAYS_BACK is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
        print(f"Filter: last {DAYS_BACK} day(s) (after {cutoff.strftime('%Y-%m-%d')})")

    all_stories: list[dict[str, Any]] = []

    for key in outlet_keys:
        outlet = OUTLETS.get(key)
        if not outlet:
            print(f"  Unknown outlet: {key}")
            continue

        try:
            content = http_get(outlet["url"])
            data = xmltodict.parse(content)
            channel = data.get("rss", {}).get("channel", {})
            items = channel.get("item", [])
            if isinstance(items, dict):
                items = [items]

            stories = []
            for entry in items:
                pub_date_str = entry.get("pubDate")
                pub_date = parse_date(pub_date_str)

                # Filter by date
                if cutoff and pub_date and pub_date < cutoff:
                    continue

                summary = entry.get("description", "")
                if summary:
                    summary = strip_html(summary)

                stories.append({
                    "source": outlet["name"],
                    "domain": outlet["domain"],
                    "title": entry.get("title", "").strip(),
                    "url": entry.get("link", "").strip(),
                    "pub_date": pub_date_str,
                    "publish_date": format_date(pub_date_str),
                    "summary": summary,
                })

            if limit:
                stories = stories[:limit]

            print(f"  {outlet['name']:<15} {len(stories):>3} stories")
            all_stories.extend(stories)

        except Exception as e:
            print(f"  {outlet['name']:<15} FAILED: {str(e)[:50]}")

    return all_stories


def main() -> int:
    # Check for --test flag
    test_mode = "--test" in sys.argv
    max_per = 3 if test_mode else None

    outlet_keys = ACTIVE_OUTLETS or list(OUTLETS.keys())

    print("=== FETCH RAW ===")
    print(f"Outlets: {len(outlet_keys)}")
    if test_mode:
        print("Mode: TEST (max 3 per outlet)")
    print()

    stories = fetch_rss(max_per_outlet=max_per)

    if MAX_STORIES:
        stories = stories[:MAX_STORIES]

    print(f"\nTotal: {len(stories)} stories")

    if not stories:
        print("No stories found.")
        return 0

    # Build output with metadata
    output = {
        "_meta": {
            "script": "fetch-raw.py",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "config": {
                "outlets": outlet_keys,
                "days_back": DAYS_BACK,
                "max_stories": MAX_STORIES,
                "max_per_outlet": max_per or MAX_PER_OUTLET,
            },
            "count": len(stories),
        },
        "stories": stories,
    }

    RAW_STORIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    RAW_STORIES_FILE.write_text(
        json.dumps(output, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )
    print(f"Saved to {RAW_STORIES_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
