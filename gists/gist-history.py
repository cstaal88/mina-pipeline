#!/usr/bin/env python3
"""Browse and recover gist revision history.

Usage:
    python3 gists/gist-history.py                      # List revisions for all topics
    python3 gists/gist-history.py --topic minneapolis-ice
    python3 gists/gist-history.py --topic minneapolis-ice --revision 3  # Show revision #3
    python3 gists/gist-history.py --topic minneapolis-ice --revision 3 --file raw.jsonl --restore
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime

# Gist IDs by topic
GISTS = {
    "minneapolis-ice": "839f9f409d36d715d277095886ced536",
    "greenland-trump": "a046f4a9233ff2e499dfeb356e081d79",
}


def run_gh(args: list[str]) -> str:
    """Run gh CLI command and return output."""
    result = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def get_gist_history(gist_id: str, limit: int = 10) -> list[dict]:
    """Get revision history for a gist."""
    jq_query = f".history[:limit] | .[] | {{version, committed_at, additions: .change_status.additions, deletions: .change_status.deletions}}"
    jq_query = jq_query.replace("limit", str(limit))
    output = run_gh(["api", f"/gists/{gist_id}", "--jq", f".history[:{limit}]"])
    return json.loads(output)


def get_gist_revision(gist_id: str, version_sha: str) -> dict:
    """Get a specific revision of a gist."""
    output = run_gh(["api", f"/gists/{gist_id}/{version_sha}"])
    return json.loads(output)


def format_timestamp(ts: str) -> str:
    """Format ISO timestamp to readable format."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def list_revisions(topic: str, limit: int = 10):
    """List recent revisions for a topic's gist."""
    gist_id = GISTS.get(topic)
    if not gist_id:
        print(f"Unknown topic: {topic}")
        print(f"Available: {', '.join(GISTS.keys())}")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"GIST HISTORY: {topic}")
    print(f"Gist ID: {gist_id}")
    print(f"{'='*60}\n")
    
    history = get_gist_history(gist_id, limit)
    
    print(f"{'#':<4} {'Date':<20} {'Changes':<15} {'SHA (first 8)'}")
    print("-" * 60)
    
    for i, rev in enumerate(history):
        date = format_timestamp(rev["committed_at"])
        cs = rev.get("change_status", {})
        changes = f"+{cs.get('additions', 0)}/-{cs.get('deletions', 0)}"
        sha = rev["version"][:8]
        print(f"{i:<4} {date:<20} {changes:<15} {sha}")
    
    print(f"\nTo view a revision: python3 gists/gist-history.py --topic {topic} --revision N")
    print(f"To restore a file:  python3 gists/gist-history.py --topic {topic} --revision N --file raw.jsonl --restore")


def show_revision(topic: str, revision_num: int, filename: str = None, restore: bool = False):
    """Show or restore a specific revision."""
    gist_id = GISTS.get(topic)
    if not gist_id:
        print(f"Unknown topic: {topic}")
        sys.exit(1)
    
    # Get the SHA for this revision number
    history = get_gist_history(gist_id, revision_num + 1)
    if revision_num >= len(history):
        print(f"Revision {revision_num} not found. Max is {len(history) - 1}")
        sys.exit(1)
    
    rev = history[revision_num]
    sha = rev["version"]
    date = format_timestamp(rev["committed_at"])
    cs = rev.get("change_status", {})
    
    print(f"\nRevision #{revision_num}: {sha[:8]}")
    print(f"Date: {date}")
    print(f"Changes: +{cs.get('additions', 0)}/-{cs.get('deletions', 0)}")
    
    # Get the full revision
    full_rev = get_gist_revision(gist_id, sha)
    files = full_rev.get("files", {})
    
    print(f"\nFiles in this revision:")
    for fname, fdata in files.items():
        size = fdata.get("size", 0)
        lines = fdata.get("content", "").count("\n")
        print(f"  - {fname}: {lines} lines, {size:,} bytes")
    
    if filename:
        if filename not in files:
            print(f"\nFile '{filename}' not found in this revision.")
            print(f"Available: {', '.join(files.keys())}")
            sys.exit(1)
        
        content = files[filename].get("content", "")
        
        if restore:
            # Restore to current gist
            confirm = input(f"\nRestore {filename} from revision #{revision_num} to current gist? [y/N] ")
            if confirm.lower() == "y":
                # Write to temp file and upload
                import tempfile
                with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
                    f.write(content)
                    tmp_path = f.name
                
                run_gh(["gist", "edit", gist_id, "-f", filename, tmp_path])
                print(f"✓ Restored {filename} from revision #{revision_num}")
                
                import os
                os.unlink(tmp_path)
            else:
                print("Cancelled.")
        else:
            # Just show preview
            lines = content.split("\n")
            print(f"\nPreview of {filename} (first 5 lines):")
            print("-" * 40)
            for line in lines[:5]:
                print(line[:100] + ("..." if len(line) > 100 else ""))
            print("-" * 40)
            print(f"Total: {len(lines)} lines")


def main():
    parser = argparse.ArgumentParser(description="Browse and recover gist revision history")
    parser.add_argument("--topic", "-t", help="Topic name (minneapolis-ice, greenland-trump)")
    parser.add_argument("--revision", "-r", type=int, help="Revision number (0 = current)")
    parser.add_argument("--file", "-f", help="File to view/restore (raw.jsonl or clean.jsonl)")
    parser.add_argument("--restore", action="store_true", help="Restore file from revision")
    parser.add_argument("--limit", "-n", type=int, default=10, help="Number of revisions to show")
    
    args = parser.parse_args()
    
    if args.topic is None:
        # Show all topics
        for topic in GISTS:
            list_revisions(topic, args.limit)
    elif args.revision is not None:
        show_revision(args.topic, args.revision, args.file, args.restore)
    else:
        list_revisions(args.topic, args.limit)


if __name__ == "__main__":
    main()
