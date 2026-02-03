#!/usr/bin/env python3
"""Fetch news stories from MediaCloud based on configured topic.

Fetches from configured outlets and saves to raw/{topic}/{date}/urls.jsonl.
Adds my_topic field to each record for later filtering.

SIMPLE APPROACH:
- Always fetch the full date range (typically last 2-3 days via --days flag)
- Deduplicate by story ID within the session
- Let the downstream combine_raw_files() handle cross-session deduplication

Usage:
    python3 mcloud-fetch-urls.py --topic minneapolis-ice
    python3 mcloud-fetch-urls.py --topic minneapolis-ice --days 3  # last 3 days
    python3 mcloud-fetch-urls.py --topic minneapolis-ice --stats   # show counts only
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
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
# ---------------------------------------------------

# Retry settings
INITIAL_WAIT = 40
MAX_WAIT = 600

# ANSI color codes
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (dt.datetime, dt.date)):
            return obj.isoformat()
        return super().default(obj)


def get_raw_dir(topic: str) -> Path:
    """Get raw output directory for a topic."""
    return REPO_DIR / "raw" / topic


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
    """Show what's available across all sources (no fetching)."""
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
                with out_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            if obj.get("_manifest") or obj.get("_meta"):
                                continue
                            pub = obj.get("publish_date", "")[:10]
                            media_url = obj.get("media_url", "")
                            if pub and media_url:
                                for src in source_ids.keys():
                                    if src in media_url:
                                        downloaded[src][pub] += 1
                                        break
                        except json.JSONDecodeError:
                            continue

    print(f"\n{'Date':<12} {'Downloaded':>12} {'Available':>12} Status")
    print("-" * 50)

    total_dl = 0
    total_avail = 0
    current = start_date
    while current <= end_date:
        day_str = current.isoformat()
        have = sum(downloaded[src].get(day_str, 0) for src in source_ids.keys())
        
        # Try to get expected count (one sample source)
        avail = 0
        try:
            for name, sid in list(source_ids.items())[:1]:
                res = client.story_count(query, current, current, source_ids=[sid])
                if isinstance(res, dict):
                    avail = res.get("relevant") or res.get("count") or 0
                else:
                    avail = int(res) if res else 0
        except Exception:
            avail = -1

        total_dl += have
        if avail > 0:
            total_avail += avail

        if avail == 0 and have == 0:
            status = ""
            row = f"{DIM}{day_str:<12} {have:>12} {avail:>12}{RESET} {status}"
        elif have >= avail and avail > 0:
            status = f"{GREEN}✓{RESET}"
            row = f"{day_str:<12} {have:>12} {avail:>12} {status}"
        elif avail < 0:
            status = f"{YELLOW}?{RESET}"
            row = f"{day_str:<12} {have:>12} {'?':>12} {status}"
        else:
            status = ""
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
    parser.add_argument("--days", type=int, help="Only collect N most recent days (recommended: 2-3)")
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
    
    # --days flag: only collect N most recent days (recommended for daily runs)
    if args.days:
        start_date = max(start_date, end_date - dt.timedelta(days=args.days - 1))
        print(f"Fetching last {args.days} days: {start_date} to {end_date}")

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

    # Load existing IDs from today's output file (for deduplication within session)
    existing_ids = load_existing_ids(out_path)
    print(f"Loaded {len(existing_ids)} existing story IDs")

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

    print(f"\nDone! {total_new} new stories, {total_skipped} skipped")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
