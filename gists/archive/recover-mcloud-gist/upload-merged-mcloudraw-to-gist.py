#!/usr/bin/env python3
"""
Upload merged mediacloud_raw.jsonl to gist.
"""

import json
import os
import requests
from pathlib import Path

# Configuration
GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
MERGED_FILE = Path(__file__).parent / "mediacloud-raw-merged.jsonl"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

def upload_to_gist():
    """Upload merged file to gist."""
    
    print("=" * 70)
    print("UPLOAD MERGED FILE TO GIST")
    print("=" * 70)
    print()
    
    # Check token
    if not GITHUB_TOKEN:
        print("❌ GITHUB_TOKEN environment variable not set")
        print("   Set it with: export GITHUB_TOKEN=your_token")
        return
    
    # Check file exists
    if not MERGED_FILE.exists():
        print(f"❌ Merged file not found: {MERGED_FILE}")
        return
    
    # Read merged file
    print(f"Reading: {MERGED_FILE}")
    with open(MERGED_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
    
    record_count = len(content.strip().split('\n'))
    file_size = len(content.encode('utf-8'))
    
    print(f"  Records: {record_count:,}")
    print(f"  Size:    {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
    print()
    
    # Prepare gist update
    print(f"Uploading to gist {GIST_ID}...")
    
    url = f"https://api.github.com/gists/{GIST_ID}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    data = {
        "files": {
            "mediacloud_raw.jsonl": {
                "content": content
            }
        }
    }
    
    response = requests.patch(url, headers=headers, json=data)
    
    if response.status_code == 200:
        print("✓ Successfully uploaded to gist!")
        print()
        print(f"View at: https://gist.github.com/{GIST_ID}")
    else:
        print(f"❌ Upload failed: {response.status_code}")
        print(f"   {response.text}")
    
    print()

if __name__ == "__main__":
    upload_to_gist()
