#!/usr/bin/env python3
# prior file name: gist-commit-audit
"""
List every commit for a gist, with optional descriptive stats per file.

Usage:
    # main example:
    python3 gists/gist-commit-audit.py --at "08:00" -f raw.jsonl clean-minneapolis-ice.jsonl
    
    # more
    python3 gists/gist-commit-audit.py
    python3 gists/gist-commit-audit.py --limit 5 --describe
    python3 gists/gist-commit-audit.py --gist 16c75a94d276d2800a44e3c2437f40e4 --order oldest --describe-files raw.jsonl clean.jsonl

    # Time-based lookup (shows lines, records, bytes for each file)
    python3 gists/gist-commit-audit.py --at "08:00" -f raw.jsonl clean-minneapolis-ice.jsonl
    python3 gists/gist-commit-audit.py --at "2026-02-01 14:00" -f raw.jsonl clean-greenland-trump.jsonl
    python3 gists/gist-commit-audit.py --at "2 days ago" -f raw.jsonl

    # Extract file contents
    python3 gists/gist-commit-audit.py --at "yesterday" --dump -f raw.jsonl
    python3 gists/gist-commit-audit.py --rev 5 --dump -f raw.jsonl clean-minneapolis-ice.jsonl
    python3 gists/gist-commit-audit.py --sha 69c8fe2e --dump -f raw.jsonl
"""

import argparse
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

UNIFIED_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"


# =============================================================================
# Analysis functions (from gist-overview.py)
# =============================================================================


def parse_jsonl(content: str) -> list[dict]:
    """Parse JSONL content, skip meta lines."""
    entries = []
    for line in content.split("\n"):
        line = line.strip()
        if line:
            try:
                entry = json.loads(line)
                if not entry.get("_meta") and not entry.get("_manifest"):
                    entries.append(entry)
            except json.JSONDecodeError:
                continue
    return entries


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    if not url:
        return "unknown"
    try:
        parsed = urlparse(url)
        return parsed.netloc.replace("www.", "") or "unknown"
    except Exception:
        return "unknown"


def analyze_entries(entries: list[dict]) -> dict:
    """Analyze a list of entries and return stats."""
    if not entries:
        return {"count": 0, "date_range": {}, "date_counts": {}, "media_stats": []}

    dates = [e.get("publish_date", "unknown") for e in entries]
    media = [e.get("media_url") or extract_domain(e.get("url", "")) for e in entries]

    date_counts = Counter(dates)
    media_counts = Counter(media)

    media_stats = [(m, cnt) for m, cnt in media_counts.items()]
    media_stats.sort(key=lambda x: -x[1])

    valid_dates = sorted([d for d in date_counts.keys() if d and d != "unknown"])
    date_range = {
        "earliest": valid_dates[0] if valid_dates else None,
        "latest": valid_dates[-1] if valid_dates else None,
    }

    return {
        "count": len(entries),
        "date_range": date_range,
        "date_counts": dict(date_counts),
        "media_stats": media_stats[:10],  # Top 10
    }


def print_histogram(title: str, counts: dict, bar_width: int = 40, indent: str = "    "):
    """Print ASCII histogram."""
    sorted_keys = sorted([k for k in counts.keys() if k and k != "unknown"])
    if not sorted_keys:
        return

    max_count = max(counts[k] for k in sorted_keys)
    print(f"\n{indent}{title}:")
    for k in sorted_keys:
        c = counts[k]
        length = int((c / max_count) * bar_width) if max_count > 0 else 0
        bar = "█" * max(1, length)
        print(f"{indent}  {k} | {bar:<{bar_width}} {c}")


def print_media_stats(media_stats: list, bar_width: int = 30, indent: str = "    "):
    """Print media statistics."""
    if not media_stats:
        return

    max_cnt = max(c for _, c in media_stats)
    print(f"\n{indent}Top outlets:")
    for m, c in media_stats:
        length = int((c / max_cnt) * bar_width) if max_cnt > 0 else 0
        bar = "█" * max(1, length)
        print(f"{indent}  {m:25} | {bar:<{bar_width}} {c}")


def print_file_analysis(fname: str, content: str, indent: str = "  "):
    """Print full analysis for a file's content."""
    entries = parse_jsonl(content)
    stats = analyze_entries(entries)

    print(f"\n{indent}{fname}:")
    print(f"{indent}  Total entries: {stats['count']}")
    if stats["date_range"].get("earliest"):
        print(f"{indent}  Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")

    print_histogram("Stories per date", stats["date_counts"], indent=indent + "  ")
    print_media_stats(stats["media_stats"], indent=indent + "  ")


# =============================================================================
# GH API functions
# =============================================================================


def run_gh(args: list[str]) -> str:
    """Run a GH API command and return stdout."""
    result = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"gh error: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def get_commits(gist_id: str) -> list[dict]:
    """Return every commit for the gist (newest first)."""
    output = run_gh(["api", f"/gists/{gist_id}/commits", "--paginate"])
    return json.loads(output)


def get_revision(gist_id: str, version: str) -> dict:
    """Return a single revision's metadata (files + content)."""
    output = run_gh(["api", f"/gists/{gist_id}/{version}"])
    return json.loads(output)


def format_timestamp(ts: str) -> str:
    """Format ISO timestamp to a readable string."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def parse_time_string(time_str: str) -> datetime:
    """Parse a time string into a datetime object.

    Supports:
    - ISO format: "2026-02-01 14:00", "2026-02-01T14:00:00"
    - Time only (today): "08:00", "14:30"
    - Relative: "2 days ago", "3 hours ago", "yesterday", "1 week ago"
    """
    time_str = time_str.strip().lower()
    now = datetime.now(timezone.utc)

    # Handle relative time strings
    if time_str == "now":
        return now
    if time_str == "yesterday":
        return now - timedelta(days=1)
    if time_str == "today":
        return now

    # Pattern: "N unit(s) ago"
    relative_pattern = r"^(\d+)\s+(second|minute|hour|day|week|month)s?\s+ago$"
    match = re.match(relative_pattern, time_str)
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        if unit == "second":
            return now - timedelta(seconds=amount)
        elif unit == "minute":
            return now - timedelta(minutes=amount)
        elif unit == "hour":
            return now - timedelta(hours=amount)
        elif unit == "day":
            return now - timedelta(days=amount)
        elif unit == "week":
            return now - timedelta(weeks=amount)
        elif unit == "month":
            return now - timedelta(days=amount * 30)  # Approximate

    # Try ISO format parsing
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ]:
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    # Try time-only format (assumes today)
    for fmt in ["%H:%M:%S", "%H:%M"]:
        try:
            t = datetime.strptime(time_str, fmt).time()
            return now.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)
        except ValueError:
            continue

    raise ValueError(f"Cannot parse time string: {time_str!r}")


def find_commit_at_time(commits: list[dict], target_time: datetime) -> dict | None:
    """Find the most recent commit at or before the target time.

    Commits should be in newest-first order (as returned by API).
    """
    # Iterate from newest to oldest, find first commit <= target_time
    for commit in commits:
        commit_time = datetime.fromisoformat(commit["committed_at"].replace("Z", "+00:00"))
        if commit_time <= target_time:
            return commit
    return None


def find_commit_by_rev(commits: list[dict], rev: int, order: str) -> dict | None:
    """Find a commit by revision number."""
    if order == "oldest":
        commits = list(reversed(commits))
    if 0 <= rev < len(commits):
        return commits[rev]
    return None


def find_commit_by_sha(commits: list[dict], sha_prefix: str) -> dict | None:
    """Find a commit by SHA prefix."""
    sha_prefix = sha_prefix.lower()
    for commit in commits:
        if commit["version"].lower().startswith(sha_prefix):
            return commit
    return None


def count_jsonl_records(content: str) -> int:
    """Count records while skipping _meta/_manifest lines."""
    if not content:
        return 0
    count = 0
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("_meta") or obj.get("_manifest"):
            continue
        count += 1
    return count


def describe_files(
    gist_id: str, version: str, filenames: list[str], fetch_truncated: bool = False
) -> dict[str, dict[str, int | str | bool]]:
    """Return descriptive stats per file for a revision.

    If fetch_truncated is True, fetches full content for truncated files to count records/lines.
    """
    rev = get_revision(gist_id, version)
    files = rev.get("files", {})
    stats: dict[str, dict[str, int | str | bool]] = {}
    for fname in filenames:
        fdata = files.get(fname)
        if not fdata:
            continue
        truncated = fdata.get("truncated", False)
        if fetch_truncated and truncated:
            content = get_file_content(fdata)
        else:
            content = fdata.get("content", "")
        stats[fname] = {
            "size": fdata.get("size", 0),
            "lines": content.count("\n") if content else 0,
            "records": count_jsonl_records(content),
            "truncated": truncated,
        }
    return stats


def fetch_raw_content(raw_url: str) -> str:
    """Fetch content from a raw URL (for truncated files)."""
    result = subprocess.run(["curl", "-sL", raw_url], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"curl error: {result.stderr.strip()}", file=sys.stderr)
        return ""
    return result.stdout


def get_file_content(fdata: dict) -> str:
    """Get file content, fetching from raw_url if truncated."""
    if fdata.get("truncated"):
        raw_url = fdata.get("raw_url")
        if raw_url:
            return fetch_raw_content(raw_url)
    return fdata.get("content", "")


def dump_files(gist_id: str, version: str, filenames: list[str], to_stdout: bool = True) -> None:
    """Dump file contents from a revision.

    If to_stdout is True and there's only one file, print content to stdout.
    Otherwise, save each file to disk with a version prefix.
    """
    rev = get_revision(gist_id, version)
    files = rev.get("files", {})
    sha_short = version[:8]

    available_files = list(files.keys())
    matched_files = [f for f in filenames if f in files]

    if not matched_files:
        print(f"No matching files found. Available: {', '.join(available_files)}", file=sys.stderr)
        sys.exit(1)

    # Single file to stdout
    if to_stdout and len(matched_files) == 1:
        content = get_file_content(files[matched_files[0]])
        print(content, end="")
        return

    # Multiple files or explicit save mode: write to disk
    for fname in matched_files:
        content = get_file_content(files[fname])
        out_name = f"{sha_short}_{fname}"
        with open(out_name, "w") as f:
            f.write(content)
        print(f"Saved: {out_name} ({len(content)} bytes)", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Audit gist commits + optional file stats")
    parser.add_argument("--gist", default=UNIFIED_GIST_ID, help="Gist ID to inspect")
    parser.add_argument("--limit", "-n", type=int, help="Show only the first N commits in the chosen order")
    parser.add_argument(
        "--order",
        choices=["newest", "oldest"],
        default="newest",
        help="Whether to start from the newest commit (default) or oldest",
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Fetch each commit's file contents and print stats for the requested files",
    )
    parser.add_argument(
        "--describe-files",
        "-f",
        nargs="+",
        default=["raw.jsonl", "clean.jsonl"],
        help="Files to describe/dump (default: raw.jsonl clean.jsonl)",
    )

    # New: revision selection
    parser.add_argument(
        "--at",
        metavar="TIME",
        help="Find commit at a specific time (e.g., '2026-02-01 14:00', 'yesterday', '2 days ago')",
    )
    parser.add_argument(
        "--rev",
        type=int,
        metavar="N",
        help="Select commit by revision number (as shown in list output)",
    )
    parser.add_argument(
        "--sha",
        metavar="PREFIX",
        help="Select commit by SHA prefix",
    )

    # New: dump file contents
    parser.add_argument(
        "--dump",
        action="store_true",
        help="Dump file contents (to stdout if single file, otherwise saves to disk)",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Force saving to disk even for single file (used with --dump)",
    )

    args = parser.parse_args()

    # Fetch all commits (always newest-first from API)
    commits = get_commits(args.gist)

    # Handle single-commit selection modes
    selected_commit = None
    selection_mode = sum(bool(x) for x in [args.at, args.rev is not None, args.sha])

    if selection_mode > 1:
        print("Error: Use only one of --at, --rev, or --sha", file=sys.stderr)
        sys.exit(1)

    if args.at:
        try:
            target_time = parse_time_string(args.at)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        selected_commit = find_commit_at_time(commits, target_time)
        if not selected_commit:
            print(f"No commit found at or before {target_time}", file=sys.stderr)
            sys.exit(1)
        print(f"Commit at {args.at!r}: {selected_commit['version'][:8]} ({format_timestamp(selected_commit['committed_at'])})", file=sys.stderr)

    elif args.rev is not None:
        selected_commit = find_commit_by_rev(commits, args.rev, args.order)
        if not selected_commit:
            print(f"Error: Revision {args.rev} not found", file=sys.stderr)
            sys.exit(1)
        print(f"Revision {args.rev}: {selected_commit['version'][:8]} ({format_timestamp(selected_commit['committed_at'])})", file=sys.stderr)

    elif args.sha:
        selected_commit = find_commit_by_sha(commits, args.sha)
        if not selected_commit:
            print(f"Error: No commit matching SHA prefix {args.sha!r}", file=sys.stderr)
            sys.exit(1)
        print(f"SHA {args.sha}: {selected_commit['version'][:8]} ({format_timestamp(selected_commit['committed_at'])})", file=sys.stderr)

    # If we have a selected commit, handle --describe or --dump
    if selected_commit:
        version = selected_commit["version"]
        if args.dump:
            dump_files(args.gist, version, args.describe_files, to_stdout=not args.save)
        else:
            # Default: show full analysis for the selected commit
            rev = get_revision(args.gist, version)
            files = rev.get("files", {})
            matched = [f for f in args.describe_files if f in files]
            if not matched:
                print(f"(no requested files found, available: {', '.join(files.keys())})")
            else:
                for fname in matched:
                    content = get_file_content(files[fname])
                    print_file_analysis(fname, content)
        return

    # No single commit selected: list mode
    if args.dump:
        print("Error: --dump requires --at, --rev, or --sha to select a commit", file=sys.stderr)
        sys.exit(1)

    if args.order == "oldest":
        commits = list(reversed(commits))

    if args.limit:
        commits = commits[: args.limit]

    print(f"Gist: {args.gist}")
    print(f"Showing {len(commits)} commits ({args.order})")
    print(f"{'Rev':<4} {'Date':<20} {'SHA':<8} {'+/-':>7} {'Files':>5}")
    print("-" * 60)

    for idx, commit in enumerate(commits):
        date = format_timestamp(commit["committed_at"])
        cs = commit.get("change_status", {})
        change_summary = f"+{cs.get('additions', 0)}/-{cs.get('deletions', 0)}"
        sha = commit["version"][:8]
        files = commit.get("files", {})
        file_count = len(files) if isinstance(files, list) else len(files.keys()) if isinstance(files, dict) else 0
        print(f"{idx:<4} {date:<20} {sha:<8} {change_summary:>7} {file_count:>5}")

        if args.describe:
            stats = describe_files(args.gist, commit["version"], args.describe_files)
            if not stats:
                print("    (no requested files found)")
                continue
            for fname, meta in stats.items():
                trunc_marker = " (truncated)" if meta.get("truncated") else ""
                print(f"    {fname}: {meta['lines']} lines, {meta['records']} records, {meta['size']:,} bytes{trunc_marker}")


if __name__ == "__main__":
    main()
