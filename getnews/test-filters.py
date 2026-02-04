#!/usr/bin/env python3
"""
Test filter rules - shows what stories would be filtered out and why.

Usage:
    python getnews/test-filters.py              # Test against local tmp-news.json
    python getnews/test-filters.py --gist       # Test against raw.jsonl from gist
    python getnews/test-filters.py --limit 50   # Limit output
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from collections import defaultdict

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import TOPICS, EXCLUDED_FROM_CLEAN, GLOBAL_FILTERS, GIST_ID, RAW_STORIES_FILE
from clean import (
    matches_keywords,
    matches_strict_keywords,
    has_excluded_terms,
    passes_global_filters,
    is_english,
)


def load_gist_raw() -> list[dict]:
    """Download raw.jsonl from gist."""
    result = subprocess.run(
        ["gh", "gist", "view", GIST_ID, "-f", "raw.jsonl"],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode != 0:
        print(f"Failed to download gist: {result.stderr}")
        sys.exit(1)
    
    records = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta"):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


def load_local() -> list[dict]:
    """Load from tmp-news.json."""
    if not RAW_STORIES_FILE.exists():
        print(f"No local file: {RAW_STORIES_FILE}")
        sys.exit(1)
    
    data = json.loads(RAW_STORIES_FILE.read_text())
    if isinstance(data, dict) and "stories" in data:
        return data["stories"]
    return data


def analyze_story(story: dict, topic_name: str, keywords: list, exclude_terms: list) -> dict | None:
    """Analyze why a story would be filtered. Returns None if no keyword match."""
    
    # First check if story even matches topic keywords
    if not matches_keywords(story, keywords):
        return None  # Not relevant to this topic at all
    
    result = {
        "url": story.get("url", "")[:80],
        "title": (story.get("title") or "")[:60],
        "passed": True,
        "reasons": [],
    }
    
    # Check strict keywords
    if not matches_strict_keywords(story, keywords):
        result["passed"] = False
        result["reasons"].append("strict_keyword_fail")
    
    # Check excluded domain
    if story.get("media_url", "") in EXCLUDED_FROM_CLEAN:
        result["passed"] = False
        result["reasons"].append(f"excluded_domain:{story.get('media_url')}")
    
    # Check global filters
    passed, reason = passes_global_filters(story)
    if not passed:
        result["passed"] = False
        result["reasons"].append(reason)
    
    # Check topic-specific exclusions
    if has_excluded_terms(story, exclude_terms):
        result["passed"] = False
        result["reasons"].append("topic_exclude_terms")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Test filter rules")
    parser.add_argument("--gist", action="store_true", help="Load from gist instead of local")
    parser.add_argument("--topic", default="minneapolis-ice", help="Topic to test")
    parser.add_argument("--limit", type=int, default=100, help="Max stories to show")
    parser.add_argument("--show-passed", action="store_true", help="Also show passed stories")
    args = parser.parse_args()
    
    # Load data
    print(f"Loading {'gist' if args.gist else 'local'} data...")
    stories = load_gist_raw() if args.gist else load_local()
    print(f"Loaded {len(stories)} stories\n")
    
    # Get topic config
    if args.topic not in TOPICS:
        print(f"Unknown topic: {args.topic}")
        print(f"Available: {list(TOPICS.keys())}")
        sys.exit(1)
    
    keywords = TOPICS[args.topic]["keywords"]
    exclude_terms = TOPICS[args.topic].get("exclude_terms", [])
    
    print(f"Topic: {args.topic}")
    print(f"Keywords: {keywords}")
    print(f"Exclude terms: {exclude_terms}")
    print(f"Global promo terms: {GLOBAL_FILTERS.get('promo_terms', [])[:5]}...")
    print()
    
    # Analyze each story
    results = []
    for story in stories:
        result = analyze_story(story, args.topic, keywords, exclude_terms)
        if result is not None:  # Has keyword match
            results.append(result)
    
    # Count by reason
    reason_counts = defaultdict(int)
    filtered = [r for r in results if not r["passed"]]
    passed = [r for r in results if r["passed"]]
    
    for r in filtered:
        for reason in r["reasons"]:
            reason_counts[reason] += 1
    
    # Summary
    print("=" * 70)
    print("FILTER SUMMARY")
    print("=" * 70)
    print(f"Total matching topic keywords: {len(results)}")
    print(f"  ✓ Passed all filters: {len(passed)}")
    print(f"  ✗ Filtered out: {len(filtered)}")
    print()
    
    print("Reasons for filtering:")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")
    print()
    
    # Show filtered stories
    if filtered:
        print("=" * 70)
        print(f"FILTERED STORIES (showing {min(len(filtered), args.limit)})")
        print("=" * 70)
        for r in filtered[:args.limit]:
            print(f"\n✗ {r['title']}")
            print(f"  Reasons: {', '.join(r['reasons'])}")
            print(f"  URL: {r['url']}")
    
    # Optionally show passed
    if args.show_passed and passed:
        print()
        print("=" * 70)
        print(f"PASSED STORIES (showing {min(len(passed), args.limit)})")
        print("=" * 70)
        for r in passed[:args.limit]:
            print(f"\n✓ {r['title']}")
            print(f"  URL: {r['url']}")


if __name__ == "__main__":
    main()
