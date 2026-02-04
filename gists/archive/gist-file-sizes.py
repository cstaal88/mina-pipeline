#!/usr/bin/env python3
"""
Show gist file sizes over time - when did files grow?

This fetches the gist revision history and shows line counts per file
at each revision, so you can see when files "exploded" in size.

Usage:
    python gists/gist-file-sizes.py              # Show last 20 revisions
    python gists/gist-file-sizes.py --limit 500  # Show last 500 revisions
    python gists/gist-file-sizes.py --since 2026-01-25  # Only show revisions since date
    python gists/gist-file-sizes.py --daily      # Aggregate by day (one row per day)
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from collections import defaultdict


GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
FILES_OF_INTEREST = ["raw.jsonl", "clean-minneapolis-ice.jsonl", "clean-greenland-trump.jsonl", "mediacloud_raw.jsonl"]


def run_gh(args: list[str]) -> str:
    """Run gh CLI command and return output."""
    result = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def get_gist_history(gist_id: str, limit: int = 100) -> list[dict]:
    """Get revision history for a gist, using pagination."""
    revisions = []
    page = 1
    per_page = min(100, limit)  # GitHub max is 100 per page
    
    while len(revisions) < limit:
        output = run_gh(["api", f"/gists/{gist_id}/commits?per_page={per_page}&page={page}"])
        batch = json.loads(output)
        if not batch:
            break
        revisions.extend(batch)
        page += 1
        if len(batch) < per_page:
            break  # No more pages
    
    return revisions[:limit]


def get_gist_revision(gist_id: str, version_sha: str) -> dict:
    """Get a specific revision of a gist."""
    output = run_gh(["api", f"/gists/{gist_id}/{version_sha}"])
    return json.loads(output)


def format_timestamp(ts: str) -> str:
    """Format ISO timestamp to readable format."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d %H:%M")


def get_file_size(file_meta: dict) -> int:
    """Get file size in bytes."""
    return file_meta.get("size", 0)


def count_lines_from_content(file_meta: dict) -> int | None:
    """Count lines from content if available (not truncated)."""
    content = file_meta.get("content")
    if content and not file_meta.get("truncated", False):
        return content.count("\n")
    return None


def main():
    parser = argparse.ArgumentParser(description='Show gist file sizes over time')
    parser.add_argument('--limit', type=int, default=30, help='Number of revisions to fetch (use 500+ to see weeks back)')
    parser.add_argument('--since', default=None, help='Only show revisions since date (YYYY-MM-DD)')
    parser.add_argument('--daily', action='store_true', help='Aggregate by day (one row per day, shows end-of-day size)')
    parser.add_argument('--detailed', action='store_true', help='Fetch actual line counts (slower)')
    args = parser.parse_args()

    print(f"Gist File Sizes Over Time")
    print(f"Gist ID: {GIST_ID}")
    print("=" * 90)
    
    # Fetch history
    print(f"Fetching last {args.limit} revisions...")
    history = get_gist_history(GIST_ID, args.limit)
    
    # Filter by date if specified
    if args.since:
        since_dt = datetime.fromisoformat(args.since)
        history = [
            rev for rev in history
            if datetime.fromisoformat(rev["committed_at"].replace("Z", "+00:00")).date() >= since_dt.date()
        ]
    
    print(f"Processing {len(history)} revisions...\n")
    
    # Collect data
    rows = []
    prev_sizes = {}
    daily_data = defaultdict(dict)  # for --daily mode
    
    for i, rev in enumerate(reversed(history)):  # oldest first
        sha = rev["version"]
        committed_at = rev["committed_at"]
        date_str = format_timestamp(committed_at)
        day_str = date_str[:10]  # YYYY-MM-DD
        
        # Get full revision to see file sizes
        try:
            full_rev = get_gist_revision(GIST_ID, sha)
        except:
            continue
            
        files = full_rev.get("files", {})
        
        row = {"date": date_str, "day": day_str, "sha": sha[:8]}
        changes = []
        
        for fname in FILES_OF_INTEREST:
            if fname in files:
                size = get_file_size(files[fname])
                
                # Check for significant change
                prev = prev_sizes.get(fname, 0)
                if prev > 0:
                    pct_change = ((size - prev) / prev) * 100
                    if abs(pct_change) > 20:  # >20% change
                        changes.append(f"{fname.split('.')[0][:10]}: {pct_change:+.0f}%")
                
                row[fname] = size
                prev_sizes[fname] = size
                
                # For daily mode, keep the latest (largest) size for each day
                daily_data[day_str][fname] = size
            else:
                row[fname] = None
        
        row["changes"] = ", ".join(changes) if changes else ""
        rows.append(row)
    
    # In daily mode, aggregate to one row per day
    if args.daily:
        rows = []
        for day in sorted(daily_data.keys()):
            row = {"date": day, "day": day}
            for fname in FILES_OF_INTEREST:
                row[fname] = daily_data[day].get(fname)
            rows.append(row)
    
    # Print table header
    print(f"{'Date':<16} {'raw.jsonl':>12} {'clean-minn':>12} {'clean-grnl':>12} {'mc_raw':>12}  Notes")
    print("-" * 90)
    
    # Print rows
    for row in rows:
        raw = f"{row.get('raw.jsonl', 0) // 1024:,}KB" if row.get('raw.jsonl') else "-"
        minn = f"{row.get('clean-minneapolis-ice.jsonl', 0) // 1024:,}KB" if row.get('clean-minneapolis-ice.jsonl') else "-"
        grnl = f"{row.get('clean-greenland-trump.jsonl', 0) // 1024:,}KB" if row.get('clean-greenland-trump.jsonl') else "-"
        mc = f"{row.get('mediacloud_raw.jsonl', 0) // 1024:,}KB" if row.get('mediacloud_raw.jsonl') else "-"
        
        notes = row.get("changes", "")
        print(f"{row['date']:<16} {raw:>12} {minn:>12} {grnl:>12} {mc:>12}  {notes}")
    
    # Summary
    print("-" * 90)
    print("\nSize = file size in KB.")
    if args.daily:
        print("Showing end-of-day sizes (aggregated by day).")
    print("Tip: Use --limit 500 --daily to see ~2 weeks of history.")


if __name__ == "__main__":
    main()
