#!/usr/bin/env python3
"""Browse and recover gist revision history.

Example -- download most recent version to archive/
    python3 gists/gist-history.py --file clean-minneapolis-ice.jsonl --download

Usage:
    python3 gists/gist-history.py                      # Show unified gist history (shows revision 0 by default: the most recent one)
    python3 gists/gist-history.py --revision 3         # Show revision #3
    python3 gists/gist-history.py --revision 3 --file minneapolis-ice.jsonl --restore
    python3 gists/gist-history.py --revision 3 --file minneapolis-ice.jsonl --download
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Unified gist (primary)
UNIFIED_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"

# Old per-topic gists (archived, kept for reference)
# OLD_GISTS = {
#     "minneapolis-ice": "839f9f409d36d715d277095886ced536",
#     "greenland-trump": "a046f4a9233ff2e499dfeb356e081d79",
# }


def run_gh(args: list[str]) -> str:
    """Run gh CLI command and return output."""
    result = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def fetch_raw_content(raw_url: str) -> str | None:
    """Fetch raw file content via gh api. Returns None on failure."""
    result = subprocess.run(
        ["gh", "api", raw_url, "-H", "Accept: application/vnd.github.raw"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        err = result.stderr.strip()
        if err:
            print(f"Warning: raw download failed: {err}", file=sys.stderr)
        return None
    return result.stdout


def get_file_content(file_meta: dict, prefer_raw: bool = False) -> str:
    """Get file content, preferring raw_url for large files."""
    content = file_meta.get("content")
    raw_url = file_meta.get("raw_url")
    truncated = file_meta.get("truncated", False)

    if prefer_raw or truncated:
        if raw_url:
            raw_content = fetch_raw_content(raw_url)
            if raw_content is not None:
                return raw_content
            if content is not None:
                print("Warning: falling back to API content; may be truncated", file=sys.stderr)
                return content
        elif content is not None:
            if truncated:
                print("Warning: file is truncated and raw_url missing", file=sys.stderr)
            return content

    if content is not None:
        return content

    if raw_url:
        raw_content = fetch_raw_content(raw_url)
        if raw_content is not None:
            return raw_content

    print("Error: no content available for file", file=sys.stderr)
    sys.exit(1)


def file_line_count(file_meta: dict, prefer_raw: bool = False) -> tuple[int | None, bool]:
    """Return (line_count, is_truncated)."""
    content = file_meta.get("content")
    truncated = file_meta.get("truncated", False)
    raw_url = file_meta.get("raw_url")

    if prefer_raw or truncated:
        if raw_url:
            raw_content = fetch_raw_content(raw_url)
            if raw_content is not None:
                return raw_content.count("\n"), False
        if content is not None:
            return content.count("\n"), truncated
        return None, truncated

    if content is not None:
        return content.count("\n"), truncated

    if raw_url:
        raw_content = fetch_raw_content(raw_url)
        if raw_content is not None:
            return raw_content.count("\n"), False

    return None, truncated


def get_gist_history(gist_id: str, limit: int = 10) -> list[dict]:
    """Get revision history for a gist."""
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


def archive_path(filename: str, committed_at: str, revision_num: int) -> Path:
    """Build archive path under gists/archive with date in filename."""
    dt = datetime.fromisoformat(committed_at.replace("Z", "+00:00"))
    date_str = dt.strftime("%Y-%m-%d")
    p = Path(filename)
    if p.suffix:
        archive_name = f"{p.stem}-{date_str}{p.suffix}"
    else:
        archive_name = f"{filename}-{date_str}"

    archive_dir = Path(__file__).resolve().parent / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    path = archive_dir / archive_name

    if path.exists():
        suffix = p.suffix if p.suffix else ""
        path = archive_dir / f"{p.stem}-{date_str}-r{revision_num}{suffix}"

    return path


def list_revisions(gist_id: str, limit: int = 10):
    """List recent revisions for the unified gist."""
    print(f"\n{'='*60}")
    print(f"UNIFIED GIST HISTORY")
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

    print(f"\nTo view a revision:    python3 gists/gist-history.py --revision N")
    print(f"To restore a file:     python3 gists/gist-history.py --revision N --file raw.jsonl --restore")
    print(f"To download a file:    python3 gists/gist-history.py --revision N --file raw.jsonl --download")


def show_revision(
    gist_id: str,
    revision_num: int,
    filename: str = None,
    restore: bool = False,
    download: bool = False,
):
    """Show or restore a specific revision."""
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
        lines, truncated = file_line_count(fdata, prefer_raw=True)
        if lines is None:
            line_str = "unknown"
        elif truncated:
            line_str = f"{lines} (truncated)"
        else:
            line_str = f"{lines}"
        print(f"  - {fname}: {line_str} lines, {size:,} bytes")

    if filename:
        if filename not in files:
            print(f"\nFile '{filename}' not found in this revision.")
            print(f"Available: {', '.join(files.keys())}")
            sys.exit(1)

        content = get_file_content(files[filename], prefer_raw=(restore or download))

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
        elif download:
            path = archive_path(filename, rev["committed_at"], revision_num)
            path.write_text(content)
            print(f"✓ Downloaded {filename} from revision #{revision_num}")
            print(f"  Saved to {path}")
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
    parser.add_argument("--revision", "-r", type=int, help="Revision number (0 = current)")
    parser.add_argument("--file", "-f", help="File to view/restore (raw.jsonl, clean-*.jsonl)")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--restore", action="store_true", help="Restore file from revision to gist")
    action.add_argument(
        "--download",
        action="store_true",
        help="Download file from revision to gists/archive",
    )
    parser.add_argument("--limit", "-n", type=int, default=10, help="Number of revisions to show")

    args = parser.parse_args()

    if args.revision is not None:
        show_revision(UNIFIED_GIST_ID, args.revision, args.file, args.restore, args.download)
    else:
        list_revisions(UNIFIED_GIST_ID, args.limit)


if __name__ == "__main__":
    main()
