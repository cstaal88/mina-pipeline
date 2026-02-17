#!/usr/bin/env python3
"""
Check ALL JSONL files in the gist for duplicate records.

Usage:
    python check-duplicates.py              # Check by URL (default)
    python check-duplicates.py --content    # Check by title+description (content dupes)
    python check-duplicates.py FILE         # Check a local file
    python check-duplicates.py FILE --content
"""

import json
import os
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
        print(f"Failed to fetch gist: {resp.status_code}")
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


def content_key(record):
    """Generate a key from title + description for content-based dedup."""
    title = (record.get("title") or "").strip().lower()
    desc = (record.get("description") or "").strip().lower()
    return (title, desc)


def check_file_url(filename, records):
    """Check one file for URL duplicates."""
    if not records:
        print(f"  (empty)\n")
        return

    url_counts = Counter(r.get("url") for r in records)
    unique = len(url_counts)
    dupes = len(records) - unique

    status = "clean" if dupes == 0 else f"{dupes:,} duplicates ({len(records)/unique:.1f}x bloat)"
    print(f"  Records: {len(records):,}  |  Unique URLs: {unique:,}  |  {status}")

    # Source breakdown
    source_counts = defaultdict(int)
    source_unique = defaultdict(set)
    for r in records:
        src = r.get("collected_with") or "-"
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


def check_file_content(filename, records):
    """Check one file for content duplicates (same title + description, different URLs)."""
    if not records:
        print(f"  (empty)\n")
        return

    # Group by content key
    by_content = defaultdict(list)
    for r in records:
        key = content_key(r)
        if key[0]:  # skip records with no title
            by_content[key].append(r)

    # Find groups with multiple URLs
    content_dupes = []
    for key, group in by_content.items():
        urls = set(r.get("url", "") for r in group)
        if len(urls) > 1:
            content_dupes.append((key, group))

    # Also count exact content dupes (same title+desc, same URL — true dupes)
    exact_dupes = sum(len(group) - 1 for group in by_content.values() if len(group) > 1)

    unique_content = len(by_content)
    print(f"  Records: {len(records):,}  |  Unique title+desc: {unique_content:,}")
    print(f"  Exact content dupes (same title+desc+url): {exact_dupes}")
    print(f"  Syndication dupes (same title+desc, different URLs): {len(content_dupes)}")

    if content_dupes:
        # Sort by group size descending
        content_dupes.sort(key=lambda x: -len(x[1]))
        show = content_dupes[:10]
        print(f"\n  Top syndication dupes:")
        for (title, _desc), group in show:
            urls = set(r.get("url", "") for r in group)
            outlets = set(r.get("media_url", "") or r.get("media_name", "") for r in group)
            print(f"    [{len(urls)} URLs] {title[:80]}")
            print(f"      Outlets: {', '.join(sorted(outlets))}")
            for u in sorted(urls):
                print(f"        {u[:90]}")
            print()
    else:
        print(f"  No syndication dupes found.\n")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a.startswith("-")]
    content_mode = "--content" in flags

    mode_label = "CONTENT (title+description)" if content_mode else "URL"

    # Local file mode
    if args:
        filepath = args[0]
        print(f"Checking local file: {filepath}")
        print(f"Mode: {mode_label}\n")
        with open(filepath, 'r') as f:
            records = parse_jsonl(f.read())
        if content_mode:
            check_file_content(filepath, records)
        else:
            check_file_url(filepath, records)
        return

    # Gist mode: check all files
    print("=" * 70)
    print(f"DUPLICATE CHECK: ALL GIST FILES ({mode_label})")
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
        print(f"  {filename}")
        records = parse_jsonl(files[filename])
        if content_mode:
            check_file_content(filename, records)
        else:
            check_file_url(filename, records)

    print("=" * 70)


if __name__ == "__main__":
    main()
