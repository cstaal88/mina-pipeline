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

    # The API started returning null for quota fields: the 2026-08-29 and
    # 2026-08-30 runs both died here on `None - int`. dict.get(key, default)
    # only supplies the default when the key is ABSENT, not when it is present
    # and null, so every field has to be coerced explicitly.
    quota = profile.get('quota') or {}
    hits = quota.get('hits')
    limit = quota.get('limit')
    week = quota.get('week') or 'unknown'

    def fmt(n):
        return f"{n:,}" if isinstance(n, (int, float)) else "unknown"

    print("=" * 50)
    print("MEDIACLOUD API QUOTA")
    print("=" * 50)
    print(f"Week of:    {week}")
    print(f"Used:       {fmt(hits)}")
    print(f"Limit:      {fmt(limit)}")

    usable = (isinstance(hits, (int, float))
              and isinstance(limit, (int, float)) and limit > 0)
    if not usable:
        # Report the gap rather than inventing a number. This step only tells
        # you how much headroom is left; a wrong figure is worse than none, and
        # collection is entirely unaffected either way.
        print("Remaining:  unknown")
        print("=" * 50)
        print("Note: the API returned no usable quota figure. Collection is "
              "unaffected — this step only reports headroom.")
        return

    remaining = limit - hits
    pct_used = hits / limit * 100
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
