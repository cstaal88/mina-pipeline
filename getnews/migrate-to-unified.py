#!/usr/bin/env python3
"""
Migrate data from old per-topic gists to new unified gist.

This script:
1. Downloads raw.jsonl from both old gists → merges into unified raw.jsonl
2. Downloads clean.jsonl from both old gists → renames to clean-{topic}.jsonl
3. Uploads everything to new unified gist

Run once to migrate, then delete this script.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Old gist IDs
OLD_GISTS = {
    "minneapolis-ice": "839f9f409d36d715d277095886ced536",
    "greenland-trump": "a046f4a9233ff2e499dfeb356e081d79",
}

# New unified gist
NEW_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"


def gist_download(gist_id: str, filename: str) -> str | None:
    """Download a file from a gist."""
    try:
        result = subprocess.run(
            ["gh", "gist", "view", gist_id, "-f", filename],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except Exception as e:
        print(f"  Error downloading: {e}")
        return None


def gist_upload(gist_id: str, filename: str, filepath: Path) -> bool:
    """Upload a file to a gist."""
    try:
        result = subprocess.run(
            ["gh", "gist", "edit", gist_id, "-f", filename, str(filepath)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"  Error uploading: {e}")
        return False


def parse_jsonl(content: str) -> list[dict]:
    """Parse JSONL content into records."""
    records = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta") and not obj.get("_manifest"):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


def save_jsonl(path: Path, records: list[dict], meta: dict | None = None) -> None:
    """Save records as JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    if meta:
        lines.append(json.dumps(meta, ensure_ascii=False))
    for record in records:
        lines.append(json.dumps(record, ensure_ascii=False))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    print("=== MIGRATE TO UNIFIED GIST ===")
    print(f"New gist: {NEW_GIST_ID}")
    print()

    tmp_dir = Path("data/migration")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Download and merge raw.jsonl files
    all_raw_records = []
    for topic, gist_id in OLD_GISTS.items():
        print(f"Downloading raw.jsonl from {topic}...")
        content = gist_download(gist_id, "raw.jsonl")
        if content:
            records = parse_jsonl(content)
            print(f"  Found {len(records)} records")
            all_raw_records.extend(records)

            # Save backup
            backup = tmp_dir / f"backup-{topic}-raw.jsonl"
            backup.write_text(content, encoding="utf-8")
        else:
            print(f"  No data or download failed")

    # Dedupe raw by URL
    seen_urls = set()
    unique_raw = []
    for record in all_raw_records:
        url = record.get("url")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_raw.append(record)

    print(f"\nMerged raw: {len(all_raw_records)} → {len(unique_raw)} unique")

    # Save merged raw.jsonl
    raw_path = tmp_dir / "raw.jsonl"
    raw_meta = {
        "_meta": True,
        "record_count": len(unique_raw),
        "migrated_at": datetime.now(timezone.utc).isoformat(),
    }
    save_jsonl(raw_path, unique_raw, raw_meta)

    # Download clean.jsonl files (just copy, don't regenerate)
    clean_files = {}
    for topic, gist_id in OLD_GISTS.items():
        print(f"Downloading clean.jsonl from {topic}...")
        content = gist_download(gist_id, "clean.jsonl")
        if content:
            records = parse_jsonl(content)
            print(f"  Found {len(records)} records")

            # Save as clean-{topic}.jsonl
            clean_filename = f"clean-{topic}.jsonl"
            clean_path = tmp_dir / clean_filename
            clean_path.write_text(content, encoding="utf-8")
            clean_files[clean_filename] = clean_path

            # Also save backup
            backup = tmp_dir / f"backup-{topic}-clean.jsonl"
            backup.write_text(content, encoding="utf-8")
        else:
            print(f"  No data or download failed")

    # Summary before upload
    print("\n" + "=" * 50)
    print("Ready to upload:")
    print(f"  raw.jsonl: {len(unique_raw)} records")
    for filename in clean_files:
        print(f"  {filename}")
    print()

    confirm = input("Proceed with upload? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted. Files saved in data/migration/")
        return 1

    # Upload to new gist
    print("\nUploading...")

    if gist_upload(NEW_GIST_ID, "raw.jsonl", raw_path):
        print("  ✓ raw.jsonl")
    else:
        print("  ✗ raw.jsonl (failed)")

    for filename, filepath in clean_files.items():
        if gist_upload(NEW_GIST_ID, filename, filepath):
            print(f"  ✓ {filename}")
        else:
            print(f"  ✗ {filename} (failed)")

    print(f"\n✓ Migration complete!")
    print(f"Gist: https://gist.github.com/{NEW_GIST_ID}")

    return 0


if __name__ == "__main__":
    sys.exit(main())