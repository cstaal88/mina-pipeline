#!/usr/bin/env python3
"""
Check date ranges in gist data and find revisions with specific dates.
"""

import json
import subprocess
from collections import Counter
from urllib.request import urlopen

UNIFIED_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
OWNER = "cstaal88"

OLD_GISTS = {
    "minneapolis-ice": "839f9f409d36d715d277095886ced536",
    "greenland-trump": "a046f4a9233ff2e499dfeb356e081d79",
}


def fetch_gist_file(gist_id: str, filename: str, version: str = None) -> str | None:
    """Fetch file from gist, optionally at specific version."""
    if version:
        url = f"https://gist.githubusercontent.com/{OWNER}/{gist_id}/raw/{version}/{filename}"
    else:
        url = f"https://gist.githubusercontent.com/{OWNER}/{gist_id}/raw/{filename}"
    try:
        with urlopen(url, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        return None


def get_all_versions(gist_id: str) -> list[dict]:
    """Get ALL versions of a gist."""
    result = subprocess.run(
        ["gh", "api", f"gists/{gist_id}/commits", "--paginate"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        return json.loads(result.stdout)
    return []


def analyze_dates(content: str) -> dict:
    """Extract date stats from JSONL content."""
    dates = []
    for line in content.strip().split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if obj.get("_meta"):
                continue
            pd = obj.get("publish_date")
            if pd:
                dates.append(pd)
        except:
            pass

    if not dates:
        return {"count": 0, "earliest": None, "latest": None}

    sorted_dates = sorted(set(dates))
    return {
        "count": len(dates),
        "earliest": sorted_dates[0],
        "latest": sorted_dates[-1],
    }


def main():
    print("=" * 70)
    print("SEARCHING FOR JAN 26 DATA IN GIST HISTORY")
    print("=" * 70)

    for topic, gist_id in OLD_GISTS.items():
        print(f"\n{'='*70}")
        print(f"GIST: {topic} ({gist_id[:8]}...)")
        print("=" * 70)

        versions = get_all_versions(gist_id)
        print(f"Total versions: {len(versions)}")

        # Look for versions from Jan 26 or with Jan 26 data
        print("\nSearching for Jan 26 data...")
        print(f"{'Commit Date':<20} {'SHA':<10} {'Records':<8} {'Date Range'}")
        print("-" * 70)

        found_jan26 = False
        best_version = None
        best_latest_date = None

        for v in versions:
            commit_date = v["committed_at"][:16]
            sha = v["version"]

            # Check versions from Jan 26 or earlier Jan 27
            if "2026-01-26" in commit_date or ("2026-01-27" in commit_date and commit_date < "2026-01-27T18:00"):
                content = fetch_gist_file(gist_id, "raw.jsonl", sha)
                if content:
                    stats = analyze_dates(content)
                    latest = stats['latest'] or "?"
                    print(f"{commit_date:<20} {sha[:8]:<10} {stats['count']:<8} {stats['earliest']} to {latest}")

                    if stats['latest'] and stats['latest'] >= "2026-01-26":
                        found_jan26 = True
                        if best_latest_date is None or stats['latest'] > best_latest_date:
                            best_latest_date = stats['latest']
                            best_version = sha

        if found_jan26:
            print(f"\n✓ Found Jan 26 data! Best version: {best_version[:8]} (data to {best_latest_date})")
            print(f"\nTo restore, run:")
            print(f"  curl 'https://gist.githubusercontent.com/{OWNER}/{gist_id}/raw/{best_version}/raw.jsonl' > recovered-{topic}.jsonl")
        else:
            print("\n✗ No Jan 26 data found in history")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
