#!/usr/bin/env python3
"""
One-off recovery: merge data/restored/raw.jsonl back into the gist raw.jsonl.

Usage:
    python gists/recover-jan-data.py           # dry run (preview only)
    python gists/recover-jan-data.py --push    # actually push to gist

What it does:
    1. Downloads current gist raw.jsonl
    2. Loads local backup (data/restored/raw.jsonl)
    3. Merges with URL-based dedup (same logic as the pipeline)
    4. Shows before/after date distribution
    5. If --push: backs up current gist locally, then uploads merged file
"""

import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
BACKUP_PATH = Path(__file__).resolve().parent.parent / "data" / "restored" / "raw.jsonl"
LOCAL_DIR = Path(__file__).resolve().parent.parent / "data" / "recovery"


def normalize_url(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip('/'), '', '', ''))


def load_jsonl(text: str) -> tuple[list[dict], dict | None]:
    """Parse JSONL text. Returns (records, meta_line_or_None)."""
    records = []
    meta = None
    for line in text.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if obj.get('_meta'):
                meta = obj
            else:
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records, meta


def save_jsonl(path: Path, records: list[dict], meta: dict | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    if meta:
        lines.append(json.dumps(meta, ensure_ascii=False))
    for r in records:
        lines.append(json.dumps(r, ensure_ascii=False))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def gist_download(filename: str) -> str | None:
    result = subprocess.run(
        ["gh", "gist", "view", GIST_ID, "-f", filename],
        capture_output=True, text=True, timeout=60,
    )
    return result.stdout if result.returncode == 0 else None


def gist_upload(filename: str, filepath: Path) -> bool:
    result = subprocess.run(
        ["gh", "gist", "edit", GIST_ID, "-f", filename, str(filepath)],
        capture_output=True, text=True, timeout=60,
    )
    return result.returncode == 0


def date_histogram(records: list[dict]) -> dict[str, int]:
    dates = [r.get('publish_date', 'unknown') for r in records]
    return dict(Counter(dates))


def print_date_comparison(before: dict[str, int], after: dict[str, int]):
    all_dates = sorted(set(list(before.keys()) + list(after.keys())) - {'unknown', None})
    print(f"\n  {'date':>12}  {'before':>7}  {'after':>7}  {'diff':>6}")
    print(f"  {'-'*12}  {'-'*7}  {'-'*7}  {'-'*6}")
    total_before = total_after = 0
    for d in all_dates:
        b = before.get(d, 0)
        a = after.get(d, 0)
        total_before += b
        total_after += a
        diff = a - b
        marker = f"+{diff}" if diff > 0 else str(diff) if diff < 0 else ""
        print(f"  {d:>12}  {b:>7}  {a:>7}  {marker:>6}")
    diff_total = total_after - total_before
    print(f"  {'-'*12}  {'-'*7}  {'-'*7}  {'-'*6}")
    print(f"  {'TOTAL':>12}  {total_before:>7}  {total_after:>7}  +{diff_total:>5}")


def main():
    push = "--push" in sys.argv

    print("=" * 60)
    print("RECOVERY: Merge restored backup into gist raw.jsonl")
    print(f"Mode: {'PUSH (will upload)' if push else 'DRY RUN (preview only)'}")
    print("=" * 60)

    # 1. Load local backup
    if not BACKUP_PATH.exists():
        print(f"\nERROR: Backup not found at {BACKUP_PATH}")
        return 1

    backup_text = BACKUP_PATH.read_text(encoding="utf-8")
    backup_records, _ = load_jsonl(backup_text)
    print(f"\nLocal backup: {len(backup_records)} records")

    # 2. Download current gist
    print("Downloading gist raw.jsonl...")
    gist_text = gist_download("raw.jsonl")
    if gist_text is None:
        print("ERROR: Could not download gist raw.jsonl")
        return 1

    gist_records, _ = load_jsonl(gist_text)
    print(f"Gist raw.jsonl: {len(gist_records)} records")

    # 3. Merge with dedup (gist records take priority, backup fills gaps)
    seen_urls = set()
    merged = []

    for r in gist_records:
        url = normalize_url(r.get("url", ""))
        if url and url not in seen_urls:
            seen_urls.add(url)
            merged.append(r)

    new_from_backup = 0
    for r in backup_records:
        url = normalize_url(r.get("url", ""))
        if url and url not in seen_urls:
            seen_urls.add(url)
            merged.append(r)
            new_from_backup += 1

    print(f"\nMerged: {len(merged)} records ({new_from_backup} new from backup)")

    # 4. Show date comparison
    before_dates = date_histogram(gist_records)
    after_dates = date_histogram(merged)
    print_date_comparison(before_dates, after_dates)

    if new_from_backup == 0:
        print("\nNothing to recover - gist already has all backup data.")
        return 0

    # 5. Push if requested
    if not push:
        print(f"\nDry run complete. To apply, run:")
        print(f"  python {sys.argv[0]} --push")
        return 0

    # Back up current gist locally before overwriting
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    backup_dest = LOCAL_DIR / f"raw-before-recovery-{datetime.now().strftime('%Y%m%d-%H%M%S')}.jsonl"
    backup_dest.write_text(gist_text, encoding="utf-8")
    print(f"\nBacked up current gist to {backup_dest}")

    # Save merged file
    meta = {
        "_meta": True,
        "record_count": len(merged),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "recovery_note": f"Merged {new_from_backup} records from data/restored/raw.jsonl",
    }
    merged_path = LOCAL_DIR / "raw.jsonl"
    save_jsonl(merged_path, merged, meta)

    # Upload
    print("Uploading merged raw.jsonl to gist...")
    if gist_upload("raw.jsonl", merged_path):
        print("Done. Gist updated successfully.")
    else:
        print("ERROR: Upload failed. Local backup preserved at:")
        print(f"  {backup_dest}")
        return 1

    print(f"\nNext step: re-run the pipeline to regenerate clean files:")
    print(f"  cd getnews && python clean.py --push")

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
