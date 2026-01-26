#!/usr/bin/env python3
"""Fetch news stories from MediaCloud based on configured topic.

Fetches from configured outlets and saves to raw/{topic}/{date}/urls.jsonl.
Adds my_topic field to each record for later filtering.

Usage:
    python3 mcloud-fetch-urls.py --topic minneapolis-ice
    python3 mcloud-fetch-urls.py --topic minneapolis-ice --stats   # show counts only
    python3 mcloud-fetch-urls.py --topic minneapolis-ice --start 2026-01-01  # backfill
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
_LOCAL_MC_PATH = Path(__file__).resolve().parent / "mcloud_setup.py"
if _LOCAL_MC_PATH.exists():
    spec = importlib.util.spec_from_file_location("mcloud_setup", str(_LOCAL_MC_PATH))
    mcloud = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mcloud)
else:
    raise ImportError("mcloud_setup.py not found in collect/")

# Import config from parent directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import get_topic_config, list_topics, DEFAULT_TOPIC

# ---------------------- PATHS ----------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent
OUT_FILE = "urls.jsonl"
MAX_PER_SOURCE = 100
# ---------------------------------------------------


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (dt.datetime, dt.date)):
            return obj.isoformat()
        return super().default(obj)


# Retry settings
INITIAL_WAIT = 40
MAX_WAIT = 600


def get_checkpoint_file(topic: str) -> Path:
    """Get checkpoint file path for a topic."""
    return SCRIPT_DIR / f".fetch-checkpoint-{topic}.json"


def get_raw_dir(topic: str) -> Path:
    """Get raw output directory for a topic."""
    return REPO_DIR / "raw" / topic


def get_query_hash(query: str) -> str:
    """Return a short hash of the query for change detection."""
    return hashlib.md5(query.encode()).hexdigest()[:12]


def load_checkpoint(topic: str, query: str) -> dict:
    """Load checkpoint for a topic, resetting if query changed."""
    checkpoint_file = get_checkpoint_file(topic)
    current_hash = get_query_hash(query)

    if checkpoint_file.exists():
        try:
            with checkpoint_file.open("r") as f:
                data = json.load(f)

            stored_hash = data.get("query_hash")
            if stored_hash and stored_hash != current_hash:
                print(f"  Query changed (hash {stored_hash} -> {current_hash}), resetting checkpoint")
                return {"completed": {}, "expected_counts": {}, "query_hash": current_hash}

            if not stored_hash:
                data["query_hash"] = current_hash

            return data
        except (json.JSONDecodeError, IOError):
            return {"completed": {}, "query_hash": current_hash}
    return {"completed": {}, "query_hash": current_hash}


def save_checkpoint(topic: str, data: dict):
    """Save checkpoint for a topic."""
    checkpoint_file = get_checkpoint_file(topic)
    with checkpoint_file.open("w") as f:
        json.dump(data, f, indent=2)


def is_complete(checkpoint: dict, day: dt.date, source: str) -> bool:
    return day.isoformat() in checkpoint.get("completed", {}).get(source, [])


def mark_complete(checkpoint: dict, day: dt.date, source: str):
    day_str = day.isoformat()
    checkpoint.setdefault("completed", {}).setdefault(source, [])
    if day_str not in checkpoint["completed"][source]:
        checkpoint["completed"][source].append(day_str)


def get_expected_count(client, query: str, day: dt.date, source_id: int) -> int:
    """Get expected story count for a day/source."""
    for attempt in range(1, 4):
        try:
            res = client.story_count(query, day, day, source_ids=[source_id])
            if isinstance(res, dict):
                return res.get("relevant") or res.get("count") or 0
            return int(res)
        except Exception as e:
            if "429" in str(e):
                time.sleep(30 * attempt)
                continue
            return -1
    return -1


def get_expected_count_cached(client, checkpoint: dict, query: str, day: dt.date, source_id: int) -> int:
    """Get expected story count, using cache for historical dates."""
    cache_key = f"{day.isoformat()}_{source_id}"
    cached = checkpoint.get("expected_counts", {}).get(cache_key)
    if cached is not None:
        return cached

    count = get_expected_count(client, query, day, source_id)
    if count >= 0:
        checkpoint.setdefault("expected_counts", {})[cache_key] = count
    return count


def prescan_and_mark_complete(
    client, out_path: Path, checkpoint: dict, query: str, 
    source_ids: dict, start_date: dt.date, end_date: dt.date
) -> dict:
    """Scan output file and mark already-complete day/source combos in checkpoint."""
    from collections import defaultdict

    print("Pre-scanning output file to update checkpoint...")

    counts: dict[str, dict[str, int]] = {src: defaultdict(int) for src in source_ids.keys()}

    if out_path.exists():
        with out_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    # Skip manifest/meta entries
                    if obj.get("_manifest") or obj.get("_meta"):
                        continue
                    pub = obj.get("publish_date", "")[:10]
                    media_url = obj.get("media_url", "")
                    if pub and media_url:
                        for src in source_ids.keys():
                            if src in media_url:
                                counts[src][pub] += 1
                                break
                except json.JSONDecodeError:
                    continue

    newly_marked = 0
    current = start_date
    while current <= end_date:
        day_str = current.isoformat()
        for src, sid in source_ids.items():
            if is_complete(checkpoint, current, src):
                continue

            have = counts[src].get(day_str, 0)
            if have == 0:
                continue

            expected = get_expected_count_cached(client, checkpoint, query, current, sid)
            if expected >= 0 and have >= expected:
                mark_complete(checkpoint, current, src)
                newly_marked += 1

        current += dt.timedelta(days=1)

    if newly_marked > 0:
        print(f"  Marked {newly_marked} day/source combos as complete from existing data")
    else:
        print("  No new completions found")

    return checkpoint


def load_existing_ids(filepath: Path) -> set[str]:
    """Load existing story IDs from output file."""
    ids = set()
    if filepath.exists():
        with filepath.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        if obj.get("_manifest") or obj.get("_meta"):
                            continue
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


def run_stats(client, topic_config: dict, start_date: dt.date, end_date: dt.date):
    """Show what's available vs downloaded across all sources (no fetching)."""
    from collections import defaultdict

    topic = topic_config["name"]
    query = topic_config["query"]
    source_ids = topic_config["outlets"]

    raw_dir = get_raw_dir(topic)
    
    # Collect from all date directories
    downloaded: dict[str, dict[str, int]] = {src: defaultdict(int) for src in source_ids.keys()}
    
    if raw_dir.exists():
        for date_dir in raw_dir.iterdir():
            if not date_dir.is_dir():
                continue
            out_path = date_dir / OUT_FILE
            if out_path.exists():
                with out_path.open("r") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            obj = json.loads(line)
                            if obj.get("_manifest") or obj.get("_meta"):
                                continue
                            pub = obj.get("publish_date", "")[:10]
                            media_name = obj.get("media_name") or obj.get("media_url", "")
                            if pub and media_name:
                                for src in source_ids.keys():
                                    if src in media_name:
                                        downloaded[src][pub] += 1
                                        break
                        except:
                            pass

    total_found = sum(sum(day_counts.values()) for day_counts in downloaded.values())
    print(f"\nFound {total_found} existing stories for topic '{topic}'\n")

    # ANSI colors
    GREEN = "\033[32m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    print(f"Topic: {topic}")
    print(f"Query: '{query[:60]}...' | Sources: {len(source_ids)} | {start_date} to {end_date}\n")
    print(f"{'date':<12} {'downloaded':>12} {'available':>12} {'status':>10}")
    print("-" * 50)

    total_dl = 0
    total_avail = 0

    current = start_date
    while current <= end_date:
        day_str = current.isoformat()
        have = sum(day_counts.get(day_str, 0) for day_counts in downloaded.values())
        avail = 0
        for _, sid in source_ids.items():
            count = get_expected_count(client, query, current, sid)
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


def main():
    parser = argparse.ArgumentParser(description="Fetch news stories from MediaCloud for a topic")
    parser.add_argument("--topic", type=str, default=None,
                        help=f"Topic to collect (default: {DEFAULT_TOPIC})")
    parser.add_argument("--stats", action="store_true", help="Show stats only, no fetching")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD (overrides topic config)")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--days", type=int, help="Only collect N most recent days (for trial runs)")
    parser.add_argument("--list-topics", action="store_true", help="List available topics and exit")
    args = parser.parse_args()

    if args.list_topics:
        list_topics()
        return

    # Get topic config
    try:
        topic_config = get_topic_config(args.topic)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        list_topics()
        sys.exit(1)

    topic = topic_config["name"]
    query = topic_config["query"]
    source_ids = topic_config["outlets"]

    # Determine date range
    end_date = dt.date.today()
    if args.end:
        end_date = dt.date.fromisoformat(args.end)

    start_date = topic_config["start_date"]
    if args.start:
        start_date = dt.date.fromisoformat(args.start)
    
    # --days flag: only collect N most recent days (for trial runs)
    if args.days:
        start_date = max(start_date, end_date - dt.timedelta(days=args.days - 1))
        print(f"Trial mode: limiting to {args.days} days ({start_date} to {end_date})")

    # Get MediaCloud client
    try:
        client = mcloud.require_client()
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

    if args.stats:
        run_stats(client, topic_config, start_date, end_date)
        return

    # Setup output directory (topic/date structure)
    today_dir = get_raw_dir(topic) / dt.date.today().isoformat()
    today_dir.mkdir(parents=True, exist_ok=True)
    out_path = today_dir / OUT_FILE

    print(f"\nTopic: {topic}")
    print(f"Fetching '{query[:60]}...' from {len(source_ids)} sources: {start_date} to {end_date}")
    print(f"Output: {out_path}\n")

    existing_ids = load_existing_ids(out_path)
    print(f"Loaded {len(existing_ids)} existing story IDs")

    checkpoint = load_checkpoint(topic, query)

    # Pre-scan output to mark already-complete day/source combos
    checkpoint = prescan_and_mark_complete(
        client, out_path, checkpoint, query, source_ids, start_date, end_date
    )

    # Build day list (newest first)
    days = []
    current = end_date
    while current >= start_date:
        days.append(current)
        current -= dt.timedelta(days=1)

    total_new = 0
    total_skipped = 0

    with out_path.open("a", encoding="utf-8") as outf:
        for day in days:
            for name, sid in source_ids.items():
                if is_complete(checkpoint, day, name):
                    print(f"  {day} {name}: already complete, skipping")
                    continue

                print(f"  {day} {name}: fetching...", end=" ", flush=True)
                day_new = 0
                day_skipped = 0

                try:
                    for story in iter_stories(client, query, day, day, [sid]):
                        story_id = story.get("id")
                        if story_id in existing_ids:
                            day_skipped += 1
                            continue
                        
                        # Add my_topic field
                        story["my_topic"] = topic
                        
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

                expected = get_expected_count_cached(client, checkpoint, query, day, sid)
                have = day_new + day_skipped
                if expected >= 0 and have >= expected:
                    mark_complete(checkpoint, day, name)
                    save_checkpoint(topic, checkpoint)

    save_checkpoint(topic, checkpoint)

    print(f"\nDone! {total_new} new stories, {total_skipped} skipped")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
