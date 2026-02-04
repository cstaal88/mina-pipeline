#!/usr/bin/env python3
"""
quota.py - Check MediaCloud API quota usage

Usage:
    python mediacloud/quota.py

Requires MEDIACLOUD_API_KEY environment variable.
"""

import os
import sys

try:
    import mediacloud.api
except ImportError:
    print("Error: mediacloud package not installed")
    print("Run: pip install mediacloud")
    sys.exit(1)


def main():
    api_key = os.getenv('MEDIACLOUD_API_KEY')
    if not api_key:
        print("Error: MEDIACLOUD_API_KEY not set")
        sys.exit(1)

    try:
        search_api = mediacloud.api.SearchApi(api_key)
        profile = search_api.user_profile()
    except Exception as e:
        print(f"Error fetching quota: {e}")
        sys.exit(1)

    quota = profile.get('quota', {})
    hits = quota.get('hits', 0)
    limit = quota.get('limit', 0)
    week = quota.get('week', 'unknown')
    remaining = limit - hits
    pct_used = (hits / limit * 100) if limit > 0 else 0

    print("=" * 50)
    print("MEDIACLOUD API QUOTA")
    print("=" * 50)
    print(f"Week of:    {week}")
    print(f"Used:       {hits:,}")
    print(f"Limit:      {limit:,}")
    print(f"Remaining:  {remaining:,} ({100 - pct_used:.1f}%)")
    print("=" * 50)

    # Exit with warning if getting low
    if remaining < 100:
        print("⚠️  WARNING: Quota nearly exhausted!")
        sys.exit(2)
    elif remaining < 500:
        print("⚠️  Note: Quota getting low")


if __name__ == "__main__":
    main()
