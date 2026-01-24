#!/usr/bin/env python3
"""Fetch news stories mentioning 'gaza' for a specific date range.

Fetches from multiple major news outlets and saves to raw/<date>/urls.jsonl.

Usage:
    python3 fetch.py
    python3 fetch.py --stats   # just show counts, no fetching
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

# Import local mcloud helper
_LOCAL_MC_PATH = Path(__file__).resolve().parent / "mcloud.py"
if _LOCAL_MC_PATH.exists():
    spec = importlib.util.spec_from_file_location("local_mcloud", str(_LOCAL_MC_PATH))
    mcloud = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mcloud)
else:
    raise ImportError("mcloud.py not found")


# ---------------------- CONFIG ----------------------
# Search query for MediaCloud
QUERY = '(' \
        '"Renée Good" OR "Renee Good" OR "Renée Nicole Good" ' \
        'OR (Minneapolis AND ICE) ' \
        'OR (Minnesota AND ICE) ' \
        'OR (ICE AND (shooting OR shot OR killed OR fatal OR death)) ' \
        'OR (Minneapolis AND (shooting OR shot OR killed OR fatal OR death)) ' \
        ')'

_ALL_SOURCE_IDS = {
    "foxnews.com": 1092,
     "abcnews.go.com": 19260,
     "apnews.com": 106145,
     # "bbc.com": 932549,
     "cbsnews.com": 1752,
     "cnn.com": 1095,
     "dailywire.com": 269352,
     # "theguardian.com": 300560,  # UK main; use 1751 for Guardian US
     "msnbc.com": 293951,
     "nbcnews.com": 25499,
     "newsmax.com": 25349,
     "nypost.com": 7,
     "nytimes.com": 1,
     "npr.org": 1096,
     # "pbs.org": 1093,
     "usatoday.com": 4,
     "wsj.com": 22732,
     "washingtonpost.com": 2,
}

# Active outlets for fetching
SOURCE_IDS = {
    "nypost.com": 7,
    "nytimes.com": 1,
    "foxnews.com": 1092,
}

OUT_FILE = "urls.jsonl"
MAX_PER_SOURCE = 100

print("By default, this script fetches news urls for the last 2 days ...")

END_DATE = dt.date.today()
START_DATE = END_DATE - dt.timedelta(days=2)  # Short window; use --start for backfills

# Output paths - write to ../raw/<today>/
SCRIPT_DIR = Path(__file__).resolve().parent
RAW_DIR = SCRIPT_DIR.parent / "raw"
TODAY_DIR = RAW_DIR / dt.date.today().isoformat()
CHECKPOINT_FILE = SCRIPT_DIR / ".fetch-checkpoint.json"
# ----------------------------------------------------


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (dt.datetime, dt.date)):
            return obj.isoformat()
        return super().default(obj)


# Retry settings
INITIAL_WAIT = 40
MAX_WAIT = 600


def get_query_hash(query: str) -> str:
    """Return a short hash of the query for change detection."""
    return hashlib.md5(query.encode()).hexdigest()[:12]


def load_checkpoint() -> dict:
    current_hash = get_query_hash(QUERY)

    if CHECKPOINT_FILE.exists():
        try:
            with CHECKPOINT_FILE.open("r") as f:
                data = json.load(f)

            # Check if query changed - if so, reset checkpoint
            stored_hash = data.get("query_hash")
            if stored_hash and stored_hash != current_hash:
                print(f"  Query changed (hash {stored_hash} -> {current_hash}), resetting checkpoint")
                return {"completed": {}, "expected_counts": {}, "query_hash": current_hash}

            # Add hash if missing (backwards compat)
            if not stored_hash:
                data["query_hash"] = current_hash

            return data
        except (json.JSONDecodeError, IOError):
            return {"completed": {}, "query_hash": current_hash}
    return {"completed": {}, "query_hash": current_hash}


def save_checkpoint(data: dict):
    with CHECKPOINT_FILE.open("w") as f:
        json.dump(data, f, indent=2)


def is_complete(checkpoint: dict, day: dt.date, source: str) -> bool:
    return day.isoformat() in checkpoint.get("completed", {}).get(source, [])


def mark_complete(checkpoint: dict, day: dt.date, source: str):
    day_str = day.isoformat()
    checkpoint.setdefault("completed", {}).setdefault(source, [])
    if day_str not in checkpoint["completed"][source]:
        checkpoint["completed"][source].append(day_str)


def get_expected_count_cached(client, checkpoint: dict, query: str, day: dt.date, source_id: int) -> int:
    """Get expected story count, using cache for historical dates."""
    cache_key = f"{day.isoformat()}_{source_id}"
    cached = checkpoint.get("expected_counts", {}).get(cache_key)
    if cached is not None:
        return cached

    count = get_expected_count(client, query, day, source_id)
    if count >= 0:  # Only cache successful lookups
        checkpoint.setdefault("expected_counts", {})[cache_key] = count
    return count


def prescan_and_mark_complete(client, out_path: Path, checkpoint: dict, start_date: dt.date, end_date: dt.date) -> dict:
    """Scan output file and mark already-complete day/source combos in checkpoint.

    This avoids re-fetching days that were already completed in previous runs.
    """
    from collections import defaultdict

    print("Pre-scanning output file to update checkpoint...")

    # Count existing entries by day + source
    counts: dict[str, dict[str, int]] = {src: defaultdict(int) for src in SOURCE_IDS.keys()}

    if out_path.exists():
        with out_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    pub = obj.get("publish_date", "")[:10]
                    media_url = obj.get("media_url", "")
                    if pub and media_url:
                        for src in SOURCE_IDS.keys():
                            if src in media_url:
                                counts[src][pub] += 1
                                break
                except json.JSONDecodeError:
                    continue

    # Check each day/source combo and mark complete if we have >= expected
    newly_marked = 0
    current = start_date
    while current <= end_date:
        day_str = current.isoformat()
        for src, sid in SOURCE_IDS.items():
            if is_complete(checkpoint, current, src):
                continue  # Already marked complete

            have = counts[src].get(day_str, 0)
            if have == 0:
                continue  # No data, definitely not complete

            # Check expected count (use cache to avoid repeated API calls)
            expected = get_expected_count_cached(client, checkpoint, QUERY, current, sid)
            if expected >= 0 and have >= expected:
                mark_complete(checkpoint, current, src)
                newly_marked += 1

        current += dt.timedelta(days=1)

    if newly_marked > 0:
        save_checkpoint(checkpoint)
        print(f"  Marked {newly_marked} day/source combos as complete from existing data")
    else:
        print("  No new completions found")

    return checkpoint


def load_existing_ids(filepath: Path) -> set[str]:
    ids = set()
    if filepath.exists():
        with filepath.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        if "id" in obj:
                            ids.add(obj["id"])
                    except json.JSONDecodeError:
                        pass
    return ids


def iter_stories(client, query: str, start: dt.date, end: dt.date, source_ids: list[int]):
    """Paginate through stories with retry on rate limits."""
    pagination_token = None
    more = True
    consecutive_errors = 0

    while more:
        try:
            page, pagination_token = client.story_list(
                query, start, end,
                source_ids=source_ids,
                page_size=100,
                pagination_token=pagination_token,
            )
            consecutive_errors = 0
        except Exception as e:
            err = str(e).lower()
            # Retry on: rate limits, connection issues, timeouts, and JSON parse errors
            # (empty response from API often causes "Expecting value" JSON error)
            if "429" in str(e) or "connection" in err or "timeout" in err or "expecting value" in err:
                consecutive_errors += 1
                wait = min(INITIAL_WAIT * (2 ** (consecutive_errors - 1)), MAX_WAIT)
                print(f"  Rate limited/error. Retry #{consecutive_errors} in {wait}s...")
                time.sleep(wait)
                continue
            raise

        if not page:
            break
        for s in page:
            yield s
        more = pagination_token is not None


def get_expected_count(client, query: str, day: dt.date, source_id: int) -> int:
    """Get expected story count for a day/source."""
    for attempt in range(1, 4):
        try:
            res = client.story_count(query, day, day, source_ids=[source_id])
            if isinstance(res, dict):
                # 'relevant' is the count matching the query, 'total' is all stories
                return res.get("relevant") or res.get("count") or 0
            return int(res)
        except Exception as e:
            if "429" in str(e):
                time.sleep(30 * attempt)
                continue
            return -1
    return -1


def run_stats(client):
    """Show what's available vs downloaded across all sources (no fetching)."""
    from collections import defaultdict

    out_path = TODAY_DIR / OUT_FILE
    downloaded: dict[str, dict[str, int]] = {src: defaultdict(int) for src in SOURCE_IDS.keys()}

    if out_path.exists():
        print(f"Scanning {out_path.name}...")
        with out_path.open("r") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    pub = obj.get("publish_date", "")[:10]
                    media_name = obj.get("media_name") or obj.get("media_url", "")
                    if pub and media_name:
                        for src in SOURCE_IDS.keys():
                            if src in media_name:
                                downloaded[src][pub] += 1
                                break
                except:
                    pass
        total_found = sum(sum(day_counts.values()) for day_counts in downloaded.values())
        print(f"  Found {total_found} stories\n")

    # ANSI colors
    GREEN = "\033[32m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    print(f"Query: '{QUERY}' | Sources: {len(SOURCE_IDS)} | {START_DATE} to {END_DATE}\n")
    print(f"{'date':<12} {'downloaded':>12} {'available':>12} {'status':>10}")
    print("-" * 50)

    total_dl = 0
    total_avail = 0

    current = START_DATE
    while current <= END_DATE:
        day_str = current.isoformat()
        have = sum(day_counts.get(day_str, 0) for day_counts in downloaded.values())
        avail = 0
        for _, sid in SOURCE_IDS.items():
            count = get_expected_count(client, QUERY, current, sid)
            if count < 0:
                avail = -1
                break
            avail += count

        total_dl += have
        if avail >= 0:
            total_avail += avail

        if avail < 0:
            status = "?"
            row = f"{day_str:<12} {have:>12} {'?':>12} {status:>10}"
        elif have >= avail:
            status = f"{GREEN}complete{RESET}"
            row = f"{day_str:<12} {GREEN}{have:>12}{RESET} {GREEN}{avail:>12}{RESET} {status}"
        else:
            status = f"{DIM}missing{RESET}"
            row = f"{day_str:<12} {DIM}{have:>12}{RESET} {DIM}{avail:>12}{RESET} {status}"

        print(row)
        current += dt.timedelta(days=1)

    print("-" * 50)
    print(f"{'TOTAL':<12} {total_dl:>12} {total_avail:>12}")
    print()

def run_stats_table_mode(client):
    """Compact table mode: date rows x outlet columns showing downloaded/available."""
    from collections import defaultdict
    # ANSI colors (match run_stats)
    GREEN = "\033[32m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    out_path = TODAY_DIR / OUT_FILE
    downloaded: dict[str, dict[str, int]] = {src: defaultdict(int) for src in SOURCE_IDS.keys()}

    print(f"\nScanning {out_path.name}...")

    if out_path.exists():
        with out_path.open("r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    pub = obj.get("publish_date", "")[:10]
                    media_name = obj.get("media_name") or obj.get("media_url", "")
                    if pub and media_name:
                        for src in SOURCE_IDS.keys():
                            if src in media_name:
                                downloaded[src][pub] += 1
                                break
                except json.JSONDecodeError:
                    continue
        total_found = sum(sum(day_counts.values()) for day_counts in downloaded.values())
        print(f"Found {total_found} stories in JSONL")
    else:
        print(f"No JSONL file found at {out_path}")

    days = []
    current = START_DATE
    while current <= END_DATE:
        days.append(current)
        current += dt.timedelta(days=1)

    print(f"Querying MediaCloud for expected counts ({len(days)} days x {len(SOURCE_IDS)} outlets)...")

    expected: dict[str, dict[str, int]] = {src: {} for src in SOURCE_IDS.keys()}
    for day in days:
        day_str = day.isoformat()
        for src, sid in SOURCE_IDS.items():
            exp = get_expected_count(client, QUERY, day, sid)
            expected[src][day_str] = exp

    outlets = list(SOURCE_IDS.keys())
    # Ensure columns are wide enough for outlet labels and "downloaded/available"
    short_names = []
    for src in outlets:
        base = src.split(".")[0]
        short_names.append(base[:3])
    col_width = max(12, max(len(n) for n in short_names) + 1)

    print(f"\n{'='*(12 + len(outlets) * col_width)}")
    header = f"{'date':<12}"
    for short in short_names:
        header += f"{short:>{col_width}}"
    print(header)
    print("-" * (12 + len(outlets) * col_width))

    total_complete = 0
    total_incomplete = 0
    totals = {src: {"dl": 0, "avail": 0} for src in outlets}

    for day in days:
        day_str = day.isoformat()
        row = f"{day_str:<12}"
        for src in outlets:
            have = downloaded[src].get(day_str, 0)
            avail = expected[src].get(day_str, -1)

            totals[src]["dl"] += have
            if avail >= 0:
                totals[src]["avail"] += avail

            cell_text = f"{have}/{avail}" if avail >= 0 else f"{have}/?"
            if avail < 0:
                cell = cell_text
            elif have >= avail:
                cell = f"{GREEN}{cell_text}{RESET}"
                total_complete += 1
            else:
                cell = f"{DIM}{cell_text}{RESET}"
                total_incomplete += 1

            visible_len = len(cell_text)
            padding = col_width - visible_len
            row += " " * padding + cell
        print(row)

    print("-" * (12 + len(outlets) * col_width))
    totals_row = f"{'TOTAL':<12}"
    for src in outlets:
        cell = f"{totals[src]['dl']}/{totals[src]['avail']}"
        totals_row += f"{cell:>{col_width}}"
    print(totals_row)
    print(f"{'='*(12 + len(outlets) * col_width)}")

    print(f"\n  {GREEN}green{RESET} = complete | {DIM}dim{RESET} = incomplete")
    print(f"  Complete: {total_complete} | Incomplete: {total_incomplete} | Total combos: {total_complete + total_incomplete}")
    print()


def main():
    parser = argparse.ArgumentParser(description=f"Fetch news stories with query '{QUERY}'")
    parser.add_argument("--stats", action="store_true", help="Show stats only, no fetching")
    parser.add_argument("--statstbl", action="store_true", help="Compact table: date rows x outlet columns showing downloaded/available")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    global START_DATE, END_DATE
    if args.start:
        START_DATE = dt.date.fromisoformat(args.start)
    if args.end:
        END_DATE = dt.date.fromisoformat(args.end)

    try:
        client = mcloud.require_client()
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

    if args.stats:
        run_stats(client)
        return
    if args.statstbl:
        run_stats_table_mode(client)
        return

    TODAY_DIR.mkdir(parents=True, exist_ok=True)
    out_path = TODAY_DIR / OUT_FILE

    print(f"\nFetching '{QUERY}' from all sources: {START_DATE} to {END_DATE}")
    print(f"Output: {out_path}\n")

    existing_ids = load_existing_ids(out_path)
    print(f"Loaded {len(existing_ids)} existing story IDs")

    checkpoint = load_checkpoint()

    # Pre-scan output to mark already-complete day/source combos
    checkpoint = prescan_and_mark_complete(client, out_path, checkpoint, START_DATE, END_DATE)

    # Build day list
    days = []
    current = END_DATE
    while current >= START_DATE:
        days.append(current)
        current -= dt.timedelta(days=1)

    total_new = 0
    total_skipped = 0

    with out_path.open("a", encoding="utf-8") as outf:
        for day in days:
            for name, sid in SOURCE_IDS.items():
                if is_complete(checkpoint, day, name):
                    print(f"  {day} {name}: already complete, skipping")
                    continue

                print(f"  {day} {name}: fetching...", end=" ", flush=True)
                day_new = 0
                day_skipped = 0

                try:
                    for story in iter_stories(client, QUERY, day, day, [sid]):
                        story_id = story.get("id")
                        if story_id in existing_ids:
                            day_skipped += 1
                            continue
                        outf.write(json.dumps(story, ensure_ascii=False, cls=DateTimeEncoder) + "\n")
                        outf.flush()
                        existing_ids.add(story_id)
                        day_new += 1
                except Exception as e:
                    print(f"ERROR: {e}")
                    continue

                print(f"+{day_new} new, {day_skipped} skipped")
                total_new += day_new
                total_skipped += day_skipped

                # Check if complete (use cached expected count)
                expected = get_expected_count_cached(client, checkpoint, QUERY, day, sid)
                # Count what we have for this day
                have = day_new + day_skipped  # just fetched + already had
                if expected >= 0 and have >= expected:
                    mark_complete(checkpoint, day, name)
                    save_checkpoint(checkpoint)

    print(f"\nDone! {total_new} new stories, {total_skipped} skipped")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
