#!/usr/bin/env python3
"""
Investigate gist history to find when data was lost.
"""

import subprocess
import json
import sys
from urllib.request import urlopen

GISTS = {
    "minneapolis-ice": "839f9f409d36d715d277095886ced536",
    "greenland-trump": "a046f4a9233ff2e499dfeb356e081d79",
}

def get_commits(gist_id: str) -> list[dict]:
    """Get all commits for a gist."""
    result = subprocess.run(
        ["gh", "api", f"gists/{gist_id}/commits", "--paginate"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error getting commits: {result.stderr}")
        return []
    return json.loads(result.stdout)

def get_file_content(gist_id: str, version: str, filename: str) -> str | None:
    """Fetch file content for a specific version."""
    # Find the owner from gh api
    result = subprocess.run(
        ["gh", "api", f"gists/{gist_id}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    data = json.loads(result.stdout)
    owner = data.get("owner", {}).get("login", "")

    url = f"https://gist.githubusercontent.com/{owner}/{gist_id}/raw/{version}/{filename}"
    try:
        with urlopen(url, timeout=10) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        return None

def count_records(content: str) -> int:
    """Count non-meta records in JSONL content."""
    if not content:
        return 0
    count = 0
    for line in content.strip().split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta") and not obj.get("_manifest"):
                count += 1
        except:
            pass
    return count

def main():
    print("=" * 70)
    print("GIST HISTORY INVESTIGATION")
    print("=" * 70)

    for topic, gist_id in GISTS.items():
        print(f"\n{'='*70}")
        print(f"TOPIC: {topic}")
        print(f"Gist: {gist_id}")
        print("=" * 70)

        commits = get_commits(gist_id)
        if not commits:
            print("No commits found")
            continue

        print(f"Total commits: {len(commits)}")
        print()

        # Sample commits at different points
        sample_indices = [0]  # current
        if len(commits) > 10:
            sample_indices.extend([10, len(commits)//2, len(commits)-1])
        elif len(commits) > 5:
            sample_indices.extend([5, len(commits)-1])
        else:
            sample_indices.extend(range(1, len(commits)))

        sample_indices = sorted(set(sample_indices))

        print(f"{'#':<5} {'Date':<22} {'raw.jsonl':<12} {'clean.jsonl':<12}")
        print("-" * 55)

        max_raw = 0
        max_raw_date = ""
        max_raw_idx = 0

        for idx in sample_indices:
            if idx >= len(commits):
                continue
            commit = commits[idx]
            date = commit["committed_at"][:19].replace("T", " ")
            version = commit["version"]

            raw_content = get_file_content(gist_id, version, "raw.jsonl")
            clean_content = get_file_content(gist_id, version, "clean.jsonl")

            raw_count = count_records(raw_content)
            clean_count = count_records(clean_content)

            if raw_count > max_raw:
                max_raw = raw_count
                max_raw_date = date
                max_raw_idx = idx

            print(f"{idx:<5} {date:<22} {raw_count:<12} {clean_count:<12}")

        print()
        print(f"Max raw.jsonl records seen: {max_raw} (at revision #{max_raw_idx}, {max_raw_date})")

        # If current is much less than max, find when it dropped
        current_count = count_records(get_file_content(gist_id, commits[0]["version"], "raw.jsonl"))
        if max_raw > current_count * 2:
            print(f"\n⚠️  DATA LOSS DETECTED: Had {max_raw} records, now only {current_count}")
            print("   Scanning to find when data was lost...")

            # Binary search for when the drop happened
            for i in range(len(commits)):
                commit = commits[i]
                version = commit["version"]
                content = get_file_content(gist_id, version, "raw.jsonl")
                count = count_records(content)
                if count > current_count * 1.5:  # Found a version with more data
                    date = commit["committed_at"][:19].replace("T", " ")
                    print(f"   → Revision #{i} ({date}) had {count} records")
                    print(f"   → Version SHA: {version}")
                    break

    print("\n" + "=" * 70)
    print("To restore data from a specific revision:")
    print("  curl 'https://gist.githubusercontent.com/USER/GIST_ID/raw/SHA/raw.jsonl' > restored.jsonl")
    print("=" * 70)

if __name__ == "__main__":
    main()