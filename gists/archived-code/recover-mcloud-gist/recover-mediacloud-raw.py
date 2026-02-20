#!/usr/bin/env python3
"""
Recovery script for mediacloud_raw.jsonl

Problem: Local upload overwrote gist, losing records that were only in gist.
Solution: Merge current gist + previous gist version + local file → dedupe → re-upload.

Usage:
    python gists/recover-mediacloud-raw.py
    
This script will:
1. Download CURRENT gist version (includes any new records from Actions)
2. Download PREVIOUS gist version (before the overwrite)
3. Load LOCAL _combined.jsonl (has backfill records)
4. Merge all three sources, dedupe by URL
5. Show dry-run stats
6. Prompt before uploading
"""

import json
import subprocess
import sys
from collections import Counter
from pathlib import Path


GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
FILENAME = "mediacloud_raw.jsonl"
LOCAL_COMBINED = Path(__file__).parent.parent / "mediacloud" / "raw" / "minneapolis-ice" / "_combined.jsonl"


def run_gh(args: list[str], check=True) -> str:
    """Run gh CLI command."""
    result = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Error running gh {' '.join(args)}: {result.stderr}")
        sys.exit(1)
    return result.stdout


def get_gist_history(limit=20) -> list[dict]:
    """Get recent gist commits."""
    output = run_gh(["api", f"/gists/{GIST_ID}/commits?per_page={limit}"])
    return json.loads(output)


def download_gist_version(version_sha: str) -> str:
    """Download a specific version of the gist file."""
    output = run_gh(["api", f"/gists/{GIST_ID}/{version_sha}"])
    data = json.loads(output)
    files = data.get("files", {})
    
    # Handle both naming conventions
    for fname in [FILENAME, FILENAME.replace(".", "_")]:
        if fname in files:
            file_data = files[fname]
            if file_data.get("truncated"):
                # Need to fetch raw content
                raw_url = file_data.get("raw_url")
                if raw_url:
                    result = subprocess.run(["curl", "-sL", raw_url], capture_output=True, text=True)
                    return result.stdout
            return file_data.get("content", "")
    return ""


def download_current_gist() -> str:
    """Download current gist file."""
    return run_gh(["gist", "view", GIST_ID, "-f", FILENAME], check=False)


def parse_jsonl(content: str) -> list[dict]:
    """Parse JSONL content, skip meta lines."""
    records = []
    for line in content.strip().split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta"):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


def load_local_file() -> list[dict]:
    """Load local _combined.jsonl."""
    if not LOCAL_COMBINED.exists():
        return []
    return parse_jsonl(LOCAL_COMBINED.read_text())


def merge_records(sources: dict[str, list[dict]]) -> list[dict]:
    """Merge records from multiple sources, dedupe by URL."""
    seen_urls = set()
    merged = []
    
    # Process all sources, keeping first occurrence of each URL
    all_records = []
    for name, records in sources.items():
        for r in records:
            r["_source"] = name  # Tag for debugging
            all_records.append(r)
    
    for r in all_records:
        url = r.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            # Remove debug tag before saving
            r.pop("_source", None)
            merged.append(r)
    
    return merged


def get_stats(records: list[dict]) -> dict:
    """Get descriptive stats for records."""
    dates = Counter()
    outlets = Counter()
    
    for r in records:
        d = r.get("publish_date", "")[:10]
        dates[d] += 1
        outlets[r.get("media_name", "unknown")] += 1
    
    dec_jan_early = sum(c for d, c in dates.items() if d >= "2025-12-26" and d <= "2026-01-05")
    
    return {
        "total": len(records),
        "unique_dates": len(dates),
        "date_range": (min(dates.keys()) if dates else "N/A", max(dates.keys()) if dates else "N/A"),
        "dec26_jan05_count": dec_jan_early,
        "top_outlets": outlets.most_common(5),
        "top_dates": sorted(dates.items())[-10:],
    }


def print_stats(name: str, stats: dict):
    """Print stats for a source."""
    print(f"\n  {name}:")
    print(f"    Total records: {stats['total']:,}")
    print(f"    Date range: {stats['date_range'][0]} → {stats['date_range'][1]}")
    print(f"    Dec 26 - Jan 5 records: {stats['dec26_jan05_count']}")
    print(f"    Top outlets: {', '.join(f'{o}({c})' for o, c in stats['top_outlets'][:3])}")


def create_merged_jsonl(records: list[dict]) -> str:
    """Create JSONL content with meta header."""
    from datetime import datetime, timezone
    
    # Collect dates
    dates = sorted(set(r.get("publish_date", "")[:10] for r in records if r.get("publish_date")))
    
    meta = {
        "_meta": True,
        "topic": "minneapolis-ice",
        "record_count": len(records),
        "dates_collected": dates,
        "date_range": {"start": dates[0] if dates else None, "end": dates[-1] if dates else None},
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "recovered_by": "recover-mediacloud-raw.py",
    }
    
    lines = [json.dumps(meta)]
    for r in records:
        lines.append(json.dumps(r, ensure_ascii=False))
    
    return "\n".join(lines) + "\n"


def main():
    print("=" * 70)
    print("MEDIACLOUD_RAW.JSONL RECOVERY")
    print("=" * 70)
    
    # Step 1: Get gist history
    print("\n[1/5] Fetching gist history...")
    history = get_gist_history(20)
    
    print(f"  Found {len(history)} recent commits:")
    for i, h in enumerate(history[:5]):
        sha = h["version"][:8]
        date = h["committed_at"]
        print(f"    {i}: {sha} @ {date}")
    
    # Find the version before the overwrite (we need at least 2 versions back)
    current_sha = history[0]["version"]
    previous_sha = history[1]["version"] if len(history) > 1 else None
    
    print(f"\n  Current version: {current_sha[:8]}")
    print(f"  Previous version: {previous_sha[:8] if previous_sha else 'N/A'}")
    
    # Step 2: Download current version
    print("\n[2/5] Downloading CURRENT gist version...")
    current_content = download_current_gist()
    current_records = parse_jsonl(current_content)
    print(f"  Downloaded {len(current_records)} records")
    
    # Step 3: Download previous version
    print("\n[3/5] Downloading PREVIOUS gist version...")
    if previous_sha:
        previous_content = download_gist_version(previous_sha)
        previous_records = parse_jsonl(previous_content)
        print(f"  Downloaded {len(previous_records)} records")
    else:
        previous_records = []
        print("  No previous version available")
    
    # Step 4: Load local file
    print("\n[4/5] Loading LOCAL _combined.jsonl...")
    local_records = load_local_file()
    print(f"  Loaded {len(local_records)} records from {LOCAL_COMBINED}")
    
    # Step 5: Merge all sources
    print("\n[5/5] Merging all sources (dedupe by URL)...")
    sources = {
        "current_gist": current_records,
        "previous_gist": previous_records,
        "local_file": local_records,
    }
    merged = merge_records(sources)
    print(f"  Merged total: {len(merged)} unique records")
    
    # Stats comparison
    print("\n" + "=" * 70)
    print("DRY RUN - STATS COMPARISON")
    print("=" * 70)
    
    print_stats("CURRENT GIST (what's there now)", get_stats(current_records))
    print_stats("PREVIOUS GIST (before overwrite)", get_stats(previous_records))
    print_stats("LOCAL FILE (your backfill)", get_stats(local_records))
    print_stats("MERGED RESULT (what we'll upload)", get_stats(merged))
    
    # Delta analysis
    current_urls = {r.get("url") for r in current_records}
    previous_urls = {r.get("url") for r in previous_records}
    local_urls = {r.get("url") for r in local_records}
    merged_urls = {r.get("url") for r in merged}
    
    print("\n" + "-" * 70)
    print("DELTA ANALYSIS")
    print("-" * 70)
    print(f"  Records only in PREVIOUS (would be lost): {len(previous_urls - current_urls)}")
    print(f"  Records only in LOCAL (backfill): {len(local_urls - previous_urls)}")
    print(f"  Records only in CURRENT (new from Actions): {len(current_urls - previous_urls)}")
    print(f"  Net gain from merge: +{len(merged) - len(current_records)} records")
    
    # Show some recovered records
    recovered_urls = previous_urls - current_urls
    if recovered_urls:
        print(f"\n  Sample RECOVERED records (from previous version):")
        recovered = [r for r in previous_records if r.get("url") in list(recovered_urls)[:3]]
        for r in recovered[:3]:
            print(f"    - {r.get('publish_date', '')[:10]} | {r.get('media_name', '')} | {r.get('title', '')[:50]}")
    
    # Prompt
    print("\n" + "=" * 70)
    print("READY TO UPLOAD")
    print("=" * 70)
    print(f"  Will upload {len(merged):,} records to gist")
    print(f"  This will ADD {len(merged) - len(current_records):+} records")
    
    response = input("\nDry run complete. Implement this now? [y/N]: ").strip().lower()
    
    if response != "y":
        print("\nAborted. No changes made.")
        return 1
    
    # Create merged content
    print("\nCreating merged JSONL...")
    merged_content = create_merged_jsonl(merged)
    
    # Save to temp file
    temp_file = Path("/tmp/recovered_mediacloud_raw.jsonl")
    temp_file.write_text(merged_content)
    print(f"  Saved to {temp_file}")
    
    # Upload
    print("\nUploading to gist...")
    result = subprocess.run(
        ["gh", "gist", "edit", GIST_ID, "-f", FILENAME, str(temp_file)],
        capture_output=True, text=True
    )
    
    if result.returncode == 0:
        print(f"\n✅ SUCCESS! Uploaded {len(merged):,} records to gist")
        print(f"   https://gist.github.com/{GIST_ID}")
        
        # Also update local _combined.jsonl
        LOCAL_COMBINED.write_text(merged_content)
        print(f"   Also updated: {LOCAL_COMBINED}")
    else:
        print(f"\n❌ FAILED: {result.stderr}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
