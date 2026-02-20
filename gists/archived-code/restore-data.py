#!/usr/bin/env python3
"""
Restore lost data from gist history and upload to unified gist.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

# Good versions (before data loss at 18:15 on Jan 27)
RESTORE_FROM = {
    "minneapolis-ice": {
        "gist_id": "839f9f409d36d715d277095886ced536",
        "version": "10dcf63f500c48730f7677d28a91abbf5ed19547",  # 5103 records
    },
    "greenland-trump": {
        "gist_id": "a046f4a9233ff2e499dfeb356e081d79",
        "version": "cb91e7c4487d3a0e0676a0e334475964cfc0bb92",  # 2046 records
    },
}

# New unified gist
NEW_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
OWNER = "cstaal88"

def fetch_gist_file(gist_id: str, version: str, filename: str) -> str | None:
    """Fetch file from specific gist version."""
    url = f"https://gist.githubusercontent.com/{OWNER}/{gist_id}/raw/{version}/{filename}"
    print(f"  Fetching: {url[:80]}...")
    try:
        with urlopen(url, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"  Error: {e}")
        return None

def parse_jsonl(content: str) -> list[dict]:
    """Parse JSONL content, skip meta lines."""
    records = []
    for line in content.strip().split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta") and not obj.get("_manifest"):
                records.append(obj)
        except:
            pass
    return records

def save_jsonl(path: Path, records: list[dict], meta: dict | None = None) -> None:
    """Save as JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    if meta:
        lines.append(json.dumps(meta, ensure_ascii=False))
    for r in records:
        lines.append(json.dumps(r, ensure_ascii=False))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def gist_upload(gist_id: str, filename: str, filepath: Path) -> bool:
    """Upload file to gist."""
    result = subprocess.run(
        ["gh", "gist", "edit", gist_id, "-f", filename, str(filepath)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.returncode == 0

def main():
    print("=" * 70)
    print("RESTORE DATA FROM GIST HISTORY")
    print("=" * 70)
    print()

    out_dir = Path("data/restored")
    out_dir.mkdir(parents=True, exist_ok=True)

    all_raw_records = []

    # Download old versions with full data
    for topic, info in RESTORE_FROM.items():
        print(f"\n--- {topic} ---")

        # Get raw.jsonl from good version
        raw_content = fetch_gist_file(info["gist_id"], info["version"], "raw.jsonl")
        if raw_content:
            records = parse_jsonl(raw_content)
            print(f"  raw.jsonl: {len(records)} records")
            all_raw_records.extend(records)

            # Save backup
            backup = out_dir / f"{topic}-raw-restored.jsonl"
            backup.write_text(raw_content, encoding="utf-8")
            print(f"  Saved: {backup}")

        # Get clean.jsonl from good version
        clean_content = fetch_gist_file(info["gist_id"], info["version"], "clean.jsonl")
        if clean_content:
            records = parse_jsonl(clean_content)
            print(f"  clean.jsonl: {len(records)} records")

            # Save as clean-{topic}.jsonl
            clean_path = out_dir / f"clean-{topic}.jsonl"
            clean_path.write_text(clean_content, encoding="utf-8")
            print(f"  Saved: {clean_path}")

    # Dedupe merged raw by URL
    print("\n--- Merging raw data ---")
    seen = set()
    unique_raw = []
    for r in all_raw_records:
        url = r.get("url")
        if url and url not in seen:
            seen.add(url)
            unique_raw.append(r)

    print(f"Total raw records: {len(all_raw_records)}")
    print(f"After dedupe: {len(unique_raw)}")

    # Save merged raw
    raw_path = out_dir / "raw.jsonl"
    raw_meta = {
        "_meta": True,
        "record_count": len(unique_raw),
        "restored_at": datetime.now(timezone.utc).isoformat(),
        "sources": list(RESTORE_FROM.keys()),
    }
    save_jsonl(raw_path, unique_raw, raw_meta)
    print(f"Saved: {raw_path}")

    # Summary
    print("\n" + "=" * 70)
    print("RESTORED FILES:")
    for f in sorted(out_dir.glob("*.jsonl")):
        lines = len(f.read_text().strip().split("\n"))
        print(f"  {f.name}: {lines} lines")

    print()
    print(f"Ready to upload to unified gist: {NEW_GIST_ID}")
    print()

    confirm = input("Upload to gist? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted. Files are in data/restored/")
        return 0

    # Upload
    print("\nUploading to unified gist...")

    files_to_upload = [
        ("raw.jsonl", out_dir / "raw.jsonl"),
        ("clean-minneapolis-ice.jsonl", out_dir / "clean-minneapolis-ice.jsonl"),
        ("clean-greenland-trump.jsonl", out_dir / "clean-greenland-trump.jsonl"),
    ]

    for filename, filepath in files_to_upload:
        if filepath.exists():
            if gist_upload(NEW_GIST_ID, filename, filepath):
                print(f"  ✓ {filename}")
            else:
                print(f"  ✗ {filename} (failed)")
        else:
            print(f"  - {filename} (not found)")

    print(f"\n✓ Done! https://gist.github.com/{NEW_GIST_ID}")
    return 0

if __name__ == "__main__":
    sys.exit(main())