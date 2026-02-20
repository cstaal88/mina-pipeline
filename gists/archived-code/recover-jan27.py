#!/usr/bin/env python3
"""
Recover Jan 27 data from old gist revisions and merge into unified gist.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

UNIFIED_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
OWNER = "cstaal88"

# Old gist revisions with Jan 27 data
RECOVER_FROM = {
    "minneapolis-ice": {
        "gist_id": "839f9f409d36d715d277095886ced536",
        "version": "ec11557b378bd08ea092faaf394ed73abd6b5d14",  # 118 records, Jan 27
    },
    # Add greenland-trump if it has recoverable data
}


def fetch_url(url: str) -> str | None:
    try:
        with urlopen(url, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"  Error: {e}")
        return None


def fetch_gist_file(gist_id: str, filename: str, version: str = None) -> str | None:
    if version:
        url = f"https://gist.githubusercontent.com/{OWNER}/{gist_id}/raw/{version}/{filename}"
    else:
        url = f"https://gist.githubusercontent.com/{OWNER}/{gist_id}/raw/{filename}"
    return fetch_url(url)


def parse_jsonl(content: str) -> list[dict]:
    records = []
    for line in content.strip().split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if not obj.get("_meta") and not obj.get("_manifest"):
                records.append(obj)
        except:
            pass
    return records


def save_jsonl(path: Path, records: list[dict], meta: dict = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    if meta:
        lines.append(json.dumps(meta, ensure_ascii=False))
    for r in records:
        lines.append(json.dumps(r, ensure_ascii=False))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def gist_upload(gist_id: str, filename: str, filepath: Path) -> bool:
    result = subprocess.run(
        ["gh", "gist", "edit", gist_id, "-f", filename, str(filepath)],
        capture_output=True, text=True, timeout=120
    )
    return result.returncode == 0


def main():
    print("=" * 70)
    print("RECOVER JAN 27 DATA")
    print("=" * 70)

    out_dir = Path("data/jan27-recovery")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Download current unified gist
    print("\n1. Downloading current unified gist raw.jsonl...")
    current_content = fetch_gist_file(UNIFIED_GIST_ID, "raw.jsonl")
    if not current_content:
        print("   ERROR: Could not download current data")
        return 1

    current_records = parse_jsonl(current_content)
    print(f"   Current records: {len(current_records)}")

    # Get current date range
    current_dates = sorted(set(r.get("publish_date") for r in current_records if r.get("publish_date")))
    print(f"   Current date range: {current_dates[0]} to {current_dates[-1]}")

    # Backup current
    backup_path = out_dir / "backup-current-raw.jsonl"
    backup_path.write_text(current_content, encoding="utf-8")
    print(f"   Backup saved: {backup_path}")

    # Step 2: Download Jan 27 data from old revisions
    print("\n2. Downloading Jan 27 data from old gist revisions...")
    recovered_records = []

    for topic, info in RECOVER_FROM.items():
        print(f"\n   {topic}:")
        content = fetch_gist_file(info["gist_id"], "raw.jsonl", info["version"])
        if content:
            records = parse_jsonl(content)
            dates = sorted(set(r.get("publish_date") for r in records if r.get("publish_date")))
            print(f"   Found {len(records)} records ({dates[0] if dates else '?'} to {dates[-1] if dates else '?'})")
            recovered_records.extend(records)

            # Save backup
            backup = out_dir / f"recovered-{topic}.jsonl"
            backup.write_text(content, encoding="utf-8")
        else:
            print(f"   Could not download")

    print(f"\n   Total recovered: {len(recovered_records)} records")

    # Step 3: Merge and dedupe
    print("\n3. Merging and deduping...")
    all_records = current_records + recovered_records

    seen_urls = set()
    unique_records = []
    for r in all_records:
        url = r.get("url")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_records.append(r)

    print(f"   Before merge: {len(current_records)} + {len(recovered_records)} = {len(all_records)}")
    print(f"   After dedupe: {len(unique_records)}")

    new_count = len(unique_records) - len(current_records)
    print(f"   New records added: {new_count}")

    # Check new date range
    new_dates = sorted(set(r.get("publish_date") for r in unique_records if r.get("publish_date")))
    print(f"   New date range: {new_dates[0]} to {new_dates[-1]}")

    # Step 4: Save merged file
    merged_path = out_dir / "merged-raw.jsonl"
    meta = {
        "_meta": True,
        "record_count": len(unique_records),
        "recovered_at": datetime.now(timezone.utc).isoformat(),
    }
    save_jsonl(merged_path, unique_records, meta)
    print(f"\n4. Saved merged file: {merged_path}")

    # Step 5: Confirm upload
    print("\n" + "=" * 70)
    print(f"Ready to upload {len(unique_records)} records (+{new_count} new)")
    print(f"Date range: {new_dates[0]} to {new_dates[-1]}")
    print("=" * 70)

    confirm = input("\nUpload to unified gist? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted. Merged file saved locally.")
        return 0

    print("\nUploading...")
    if gist_upload(UNIFIED_GIST_ID, "raw.jsonl", merged_path):
        print("✓ raw.jsonl uploaded!")
        print(f"\nDone! https://gist.github.com/{UNIFIED_GIST_ID}")
    else:
        print("✗ Upload failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
