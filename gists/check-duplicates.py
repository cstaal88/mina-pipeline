#!/usr/bin/env python3
"""
Check ALL JSONL files in the gist for duplicate records (by URL).

Usage:
    python check-duplicates.py          # Check all files in gist
    python check-duplicates.py FILE     # Check a local file
"""

import json
import os
import subprocess
import sys
from collections import Counter, defaultdict

import requests

GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"


def get_gist_files():
    """Fetch all JSONL files from the gist. Returns {filename: content}."""
    token = os.getenv("GITHUB_TOKEN") or os.getenv("GIST_PAT")
    headers = {"Authorization": f"token {token}"} if token else {}

    url = f"https://api.github.com/gists/{GIST_ID}"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"❌ Failed to fetch gist: {resp.status_code}")
        return {}

    files = {}
    for filename, info in resp.json().get("files", {}).items():
        if not filename.endswith(".jsonl"):
            continue
        if info.get("truncated"):
            raw = requests.get(info["raw_url"], headers=headers)
            files[filename] = raw.text if raw.status_code == 200 else ""
        else:
            files[filename] = info.get("content", "")
    return files


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


def check_file(filename, records):
    """Check one file for duplicates and print results."""
    if not records:
        print(f"  (empty)\n")
        return

    url_counts = Counter(r.get("url") for r in records)
    unique = len(url_counts)
    dupes = len(records) - unique

    status = "✓ clean" if dupes == 0 else f"⚠ {dupes:,} duplicates ({len(records)/unique:.1f}x bloat)"
    print(f"  Records: {len(records):,}  |  Unique URLs: {unique:,}  |  {status}")

    # Source breakdown
    source_counts = defaultdict(int)
    source_unique = defaultdict(set)
    for r in records:
        src = r.get("collected_with") or "–"
        source_counts[src] += 1
        source_unique[src].add(r.get("url"))

    parts = []
    for src in sorted(source_counts.keys()):
        total = source_counts[src]
        uniq = len(source_unique[src])
        if total == uniq:
            parts.append(f"{src}: {uniq:,}")
        else:
            parts.append(f"{src}: {uniq:,} unique / {total:,} total")
    print(f"  Sources: {', '.join(parts)}")

    if dupes > 0:
        worst = [(url, cnt) for url, cnt in url_counts.most_common(5) if cnt > 1]
        if worst:
            print(f"  Worst offenders:")
            for url, cnt in worst:
                print(f"    {cnt:>4}x  {url[:75]}")
    print()


def main():
    # Local file mode
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        print(f"Checking local file: {filepath}\n")
        with open(filepath, 'r') as f:
            records = parse_jsonl(f.read())
        check_file(filepath, records)
        return

    # Gist mode: check all files
    print("=" * 70)
    print("DUPLICATE CHECK: ALL GIST FILES")
    print(f"Gist: {GIST_ID}")
    print("=" * 70)
    print()

    print("Downloading all JSONL files from gist...")
    files = get_gist_files()

    if not files:
        print("No JSONL files found.")
        return

    print(f"Found {len(files)} files\n")

    for filename in sorted(files.keys()):
        print(f"📄 {filename}")
        records = parse_jsonl(files[filename])
        check_file(filename, records)

    print("=" * 70)


if __name__ == "__main__":
    main()
