#!/usr/bin/env python3
"""
Deduplicate ALL JSONL files in the gist (by URL).
Downloads each file, removes duplicate URLs, uploads cleaned versions.

Usage:
    python dedupe-raw-gist.py          # interactive (asks before upload)
    python dedupe-raw-gist.py --yes    # skip confirmation
"""

import json
import os
import requests
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone

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
    """Parse JSONL, returning (meta, records)."""
    meta = None
    records = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and obj.get("_meta"):
                meta = obj
            elif isinstance(obj, dict):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return meta, records


def dedupe_records(records):
    """Deduplicate by URL, keeping first occurrence."""
    seen = set()
    unique = []
    for r in records:
        url = r.get("url")
        if url and url not in seen:
            seen.add(url)
            unique.append(r)
    return unique


def main():
    print("=" * 70)
    print("DEDUPLICATE ALL GIST FILES")
    print(f"Gist: {GIST_ID}")
    print("=" * 70)
    print()

    print("Downloading all JSONL files from gist...")
    files = get_gist_files()
    if not files:
        print("No JSONL files found.")
        return 1

    print(f"Found {len(files)} files\n")

    # Analyze each file
    to_fix = {}  # filename -> unique_records
    total_dupes = 0

    for filename in sorted(files.keys()):
        meta, records = parse_jsonl(files[filename])
        unique = dedupe_records(records)
        dupes = len(records) - len(unique)
        total_dupes += dupes

        status = "✓ clean" if dupes == 0 else f"⚠ {dupes:,} dupes → {len(unique):,} unique"
        print(f"  {filename:40s} {len(records):>6,} records  {status}")

        if dupes > 0:
            to_fix[filename] = (meta, unique)

    print()

    if not to_fix:
        print("✓ All files are clean — nothing to do!")
        return 0

    print(f"Files to fix: {len(to_fix)}")
    print(f"Total duplicates to remove: {total_dupes:,}")
    print()

    # Confirm
    if "--yes" not in sys.argv:
        resp = input(f"Upload {len(to_fix)} deduplicated files to gist? [y/N] ")
        if resp.lower() != "y":
            print("Aborted.")
            return 0

    # Upload each fixed file
    print()
    for filename, (old_meta, unique_records) in to_fix.items():
        # Build new meta
        meta = {
            "_meta": True,
            "record_count": len(unique_records),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "deduplicated": True,
        }
        # Preserve topic from original meta
        if old_meta and "topic" in old_meta:
            meta["topic"] = old_meta["topic"]

        # Write temp file
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
        try:
            tmp.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for r in unique_records:
                tmp.write(json.dumps(r, ensure_ascii=False) + "\n")
            tmp.close()

            result = subprocess.run(
                ["gh", "gist", "edit", GIST_ID, "-f", filename, tmp.name],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0:
                print(f"  ✓ {filename}: uploaded {len(unique_records):,} records")
            else:
                print(f"  ✗ {filename}: {result.stderr.strip()}")
        finally:
            os.unlink(tmp.name)

    print(f"\n✓ Done — removed {total_dupes:,} total duplicates")
    return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
