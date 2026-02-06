#!/usr/bin/env python3
"""
Check for duplicate records in raw.jsonl from the gist.
Shows how many unique vs duplicate records exist, broken down by source.
"""

import json
import os
import subprocess
import sys
from collections import Counter, defaultdict

GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"


def download_from_gist(filename):
    """Download a file from gist using gh CLI."""
    result = subprocess.run(
        ["gh", "gist", "view", GIST_ID, "-f", filename],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode == 0:
        return result.stdout
    return None


def parse_jsonl(content):
    """Parse JSONL string, skip _meta lines."""
    records = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta"):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


def main():
    print("=" * 70)
    print("DUPLICATE CHECK: raw.jsonl")
    print("=" * 70)
    print()

    # Download or use local file
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        print(f"Reading local file: {filepath}")
        with open(filepath, 'r') as f:
            content = f.read()
    else:
        print("Downloading raw.jsonl from gist...")
        content = download_from_gist("raw.jsonl")
        if not content:
            print("❌ Failed to download raw.jsonl")
            return

    records = parse_jsonl(content)
    print(f"Total records: {len(records):,}")
    print()

    # Count by URL
    url_counts = Counter(r.get("url") for r in records)
    unique_urls = len(url_counts)
    duplicated_urls = {url: cnt for url, cnt in url_counts.items() if cnt > 1}

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Total records:     {len(records):,}")
    print(f"  Unique URLs:       {unique_urls:,}")
    print(f"  Duplicate records: {len(records) - unique_urls:,}")
    print(f"  Bloat factor:      {len(records) / unique_urls:.1f}x")
    print()

    # Breakdown by collected_with
    source_total = Counter()
    source_unique = defaultdict(set)
    for r in records:
        source = r.get("collected_with") or "null"
        source_total[source] += 1
        source_unique[source].add(r.get("url"))

    print("BY SOURCE:")
    print(f"  {'Source':<15} {'Total':>8} {'Unique':>8} {'Dupes':>8} {'Bloat':>8}")
    print(f"  {'-'*15} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for source in sorted(source_total.keys()):
        total = source_total[source]
        uniq = len(source_unique[source])
        dupes = total - uniq
        bloat = f"{total / uniq:.1f}x" if uniq > 0 else "N/A"
        print(f"  {source:<15} {total:>8,} {uniq:>8,} {dupes:>8,} {bloat:>8}")
    print()

    # Show worst offenders
    if duplicated_urls:
        worst = sorted(duplicated_urls.items(), key=lambda x: -x[1])[:10]
        print("MOST DUPLICATED URLs:")
        for url, cnt in worst:
            print(f"  {cnt:>4}x  {url[:80]}")
        print()

    # Distribution of duplication counts
    dup_dist = Counter(url_counts.values())
    print("DUPLICATION DISTRIBUTION:")
    for copies in sorted(dup_dist.keys()):
        count = dup_dist[copies]
        label = "unique" if copies == 1 else f"{copies} copies"
        print(f"  {label:>12}: {count:>6,} URLs")

    print()


if __name__ == "__main__":
    main()
