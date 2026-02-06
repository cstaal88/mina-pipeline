#!/usr/bin/env python3
"""
Download all versions of mediacloud_raw.jsonl from gist history.

Creates a folder with one file per version for manual inspection.

Usage:
    python gists/download-gist-history.py
    
Output:
    gists/mediacloud-history/
    ├── 00_bce4e23_2026-02-05T16-57.jsonl   (newest)
    ├── 01_118a162_2026-02-05T16-56.jsonl
    ├── 02_abc1234_2026-02-04T10-00.jsonl
    └── ...
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
FILENAME = "mediacloud_raw.jsonl"
OUTPUT_DIR = Path(__file__).parent / "mediacloud-history"


def run_gh(args: list[str], check=True) -> str:
    """Run gh CLI command."""
    result = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Error running gh {' '.join(args)}: {result.stderr}")
        sys.exit(1)
    return result.stdout


def get_gist_history(limit=None) -> list[dict]:
    """Get gist commit history, paginating through all available commits."""
    all_commits = []
    page = 1
    per_page = 100
    
    while True:
        output = run_gh(["api", f"/gists/{GIST_ID}/commits?per_page={per_page}&page={page}"], check=False)
        if not output:
            break
        
        try:
            batch = json.loads(output)
        except json.JSONDecodeError:
            break
        
        if not batch:
            break
        
        all_commits.extend(batch)
        
        # Stop if we've hit our limit or got fewer than per_page (last page)
        if limit and len(all_commits) >= limit:
            return all_commits[:limit]
        
        if len(batch) < per_page:
            break  # No more pages
        
        page += 1
    
    return all_commits


def download_gist_version(version_sha: str) -> str | None:
    """Download a specific version of the gist file."""
    output = run_gh(["api", f"/gists/{GIST_ID}/{version_sha}"], check=False)
    if not output:
        return None
    
    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return None
    
    files = data.get("files", {})
    
    # Handle both naming conventions (dots become underscores in API)
    for fname in [FILENAME, FILENAME.replace(".", "_")]:
        if fname in files:
            file_data = files[fname]
            if file_data.get("truncated"):
                # Need to fetch raw content via URL
                raw_url = file_data.get("raw_url")
                if raw_url:
                    result = subprocess.run(["curl", "-sL", raw_url], capture_output=True, text=True)
                    return result.stdout
            return file_data.get("content", "")
    
    return None


def count_records(content: str) -> int:
    """Count non-meta records in JSONL."""
    count = 0
    for line in content.strip().split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta"):
                count += 1
        except json.JSONDecodeError:
            continue
    return count


def main():
    print("=" * 70)
    print("DOWNLOAD GIST HISTORY: mediacloud_raw.jsonl")
    print("=" * 70)
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nOutput folder: {OUTPUT_DIR}")
    
    # Get history
    print("\nFetching full gist history (paginating through all commits)...")
    history = get_gist_history()  # No limit - get everything
    print(f"Found {len(history)} total versions")
    
    # Filter to only versions that have mediacloud_raw.jsonl
    # (earlier versions might not have this file)
    print("\nDownloading versions...")
    downloaded = []
    
    for i, commit in enumerate(history):
        sha = commit["version"]
        date_str = commit["committed_at"]
        
        # Format date for filename
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        date_safe = dt.strftime("%Y-%m-%dT%H-%M")
        
        # Filename: 00_sha_date.jsonl
        filename = f"{i:02d}_{sha[:7]}_{date_safe}.jsonl"
        filepath = OUTPUT_DIR / filename
        
        # Skip if already downloaded
        if filepath.exists():
            records = count_records(filepath.read_text())
            print(f"  {filename} - already exists ({records:,} records)")
            downloaded.append((filename, records, date_str))
            continue
        
        # Download
        print(f"  {filename} - downloading...", end=" ", flush=True)
        content = download_gist_version(sha)
        
        if content is None:
            print("(file not in this version)")
            continue
        
        records = count_records(content)
        filepath.write_text(content)
        print(f"({records:,} records)")
        downloaded.append((filename, records, date_str))
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\nDownloaded {len(downloaded)} versions to: {OUTPUT_DIR}/")
    
    if downloaded:
        print("\nVersions by record count:")
        for fname, records, date in downloaded[:10]:
            print(f"  {fname}: {records:,} records")
        if len(downloaded) > 10:
            print(f"  ... and {len(downloaded) - 10} more")
        
        # Find max
        max_version = max(downloaded, key=lambda x: x[1])
        print(f"\nLargest version: {max_version[0]} ({max_version[1]:,} records)")
    
    print(f"\nNext step: inspect the files, then run merge script")
    print(f"  ls -la {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
