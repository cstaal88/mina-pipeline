#!/usr/bin/env python3
"""
clean.py

Combines data from raw/<date>/ directories to create cleaned news descriptions for MINA.

Input files (from all date directories in raw/):
  - raw/<date>/descriptions.jsonl (scraped descriptions)
  - raw/<date>/urls.jsonl (media_url, publish_date from MediaCloud)

Output file:
  - /tmp/newsdata.jsonl (uploaded to gist by workflow)

Logic:
  1. Only entries with "success": true are included
  2. Final output contains: description, title, url, media_url, publish_date
  3. Entries already in output (by URL) are skipped
  4. Throws error if URL in descriptions.jsonl can't be found in urls.jsonl
  5. Filters for Gaza-related content and English language

CLI:
  --stats / --dry-run  Print stats about the output file and exit (no writes, no prompts).
  --stats --all        Print stats for ALL raw collected items (before filtering).
"""

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# === Configuration ===
SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent  # mina/

RAW_DIR = SCRIPT_DIR / "raw"
LOG_FILE = SCRIPT_DIR / "clean.log"

# Output path (ephemeral - uploaded to gist, not stored in repo)
OUTPUT_FILE = Path("/tmp/newsdata.jsonl")

# Keys to retain in final output (in order)
OUTPUT_KEY_ORDER = ["media_url", "title", "description", "publish_date", "url"]

# Keywords to filter content (case-insensitive)
# Matches against title + description
FILTER_KEYWORDS = [
    "renée good", "renee good", "renée nicole good",
    "minneapolis", "minnesota", "ice",
    "shooting", "shot", "killed", "fatal", "death",
]


def setup_logging():
    """Configure logging to file."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


def load_jsonl(filepath: Path) -> list[dict]:
    """Load all entries from a JSONL file."""
    entries = []
    if not filepath.exists():
        return entries
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def load_all_from_raw(filename: str) -> list[dict]:
    """Load all entries from raw/<date>/<filename> across all date directories."""
    all_entries = []
    if not RAW_DIR.exists():
        return all_entries
    for date_dir in sorted(RAW_DIR.iterdir()):
        if date_dir.is_dir():
            file_path = date_dir / filename
            if file_path.exists():
                all_entries.extend(load_jsonl(file_path))
    return all_entries


def build_url_index(entries: list[dict], key: str = "url") -> dict[str, dict]:
    """Build a dict mapping URL -> entry for fast lookups."""
    return {entry[key]: entry for entry in entries if key in entry}


def is_gaza_related(entry: dict) -> bool:
    """Check if entry is related to Gaza conflict based on title + description."""
    title = entry.get("title") or ""
    description = entry.get("description") or ""
    text = (title + " " + description).lower()
    return any(keyword in text for keyword in FILTER_KEYWORDS)


def entry_sort_key(entry: dict) -> tuple[str, str]:
    """Return a deterministic sort key: (inverted_date, url_hash).

    This ensures:
    - Recent items come first (newest dates sort earliest due to inversion)
    - Items from the same day have stable pseudo-random order (via URL hash)
    - Fully deterministic: adding new items doesn't change existing items' positions
    - When budget truncates, oldest items are cut first
    """
    # Invert date so newest sorts first (e.g., "2026-01-22" -> "7973-98-77")
    date = entry.get("publish_date", "0000-00-00")
    inverted_date = "".join(str(9 - int(c)) if c.isdigit() else c for c in date)

    url_hash = hashlib.md5(entry.get("url", "").encode()).hexdigest()
    return (inverted_date, url_hash)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build MINA cleaned-news-descriptions JSONL from raw data."
    )
    p.add_argument(
        "--stats",
        action="store_true",
        help="Print stats about the current output file and exit (no cleaning, no prompts).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Alias for --stats (print output stats and exit).",
    )
    p.add_argument(
        "--all",
        action="store_true",
        help="With --stats: show stats for ALL collected items (before filtering), not just cleaned output.",
    )
    p.add_argument(
        "--auto",
        action="store_true",
        help="Automatically integrate without prompting.",
    )
    return p.parse_args(argv)


def print_output_stats(show_all: bool = False) -> int:
    """Describe the current output file without modifying anything.

    If show_all=True, shows stats for ALL raw collected items (before filtering).
    """
    from collections import Counter

    if show_all:
        # Show stats for ALL raw collected data (before filtering)
        urls_entries = load_all_from_raw("urls.jsonl")
        descriptions_entries = load_all_from_raw("descriptions.jsonl")

        if not urls_entries:
            print("No raw data found in raw/<date>/urls.jsonl", file=sys.stderr)
            return 2

        # Build URL index for joining
        desc_by_url = build_url_index(descriptions_entries)

        print(f"Raw data stats (ALL collected items, before filtering)")
        print(f"=" * 60)
        print(f"Total URLs collected: {len(urls_entries)}")
        print(f"Total descriptions scraped: {len(descriptions_entries)}")

        # Count successful descriptions
        success_count = sum(1 for e in descriptions_entries if e.get("success", False))
        print(f"Successful scrapes: {success_count}")

        # Stats by outlet and date from urls.jsonl (raw MediaCloud data)
        media_counts = Counter(e.get("media_url", "unknown") for e in urls_entries)
        date_counts = Counter(e.get("publish_date", "unknown")[:10] if e.get("publish_date") else "unknown"
                              for e in urls_entries)

        # Also count what would be filtered
        english_count = sum(1 for e in urls_entries if e.get("language", "").lower() == "en")
        non_english_count = len(urls_entries) - english_count

        # Count Gaza-related (need to join with descriptions for title/description)
        gaza_count = 0
        for entry in urls_entries:
            url = entry.get("url")
            desc_entry = desc_by_url.get(url, {})
            combined = {**entry, **desc_entry}
            if is_gaza_related(combined):
                gaza_count += 1

        print(f"\nFilter breakdown:")
        print(f"   English language: {english_count}")
        print(f"   Non-English: {non_english_count}")
        print(f"   Gaza-related (of all): {gaza_count}")

        print("\nStories per media outlet (ALL collected):")
        for media, count in sorted(media_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"   {media}: {count}")

        print("\nStories per date (ALL collected):")
        for date, count in sorted(date_counts.items()):
            print(f"   {date}: {count}")

        return 0

    # Default: show stats for cleaned output file
    if not OUTPUT_FILE.exists():
        print(f"Not found: {OUTPUT_FILE}", file=sys.stderr)
        return 2

    final_entries = load_jsonl(OUTPUT_FILE)
    print(f"Output: {OUTPUT_FILE} ({len(final_entries)} entries)")

    media_counts = Counter(e.get("media_url", "unknown") for e in final_entries)
    date_counts = Counter(e.get("publish_date", "unknown") for e in final_entries)

    print("\nStories per media outlet:")
    for media, count in sorted(media_counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"   {media}: {count}")

    print("\nStories per date:")
    for date, count in sorted(date_counts.items()):
        print(f"   {date}: {count}")

    return 0


def main():
    args = parse_args()
    if args.stats or args.dry_run:
        raise SystemExit(print_output_stats(show_all=args.all))

    start_time = datetime.now()
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("RUN STARTED")

    problems = []

    # Load all data sources from raw/<date>/ directories
    try:
        descriptions_entries = load_all_from_raw("descriptions.jsonl")
        urls_entries = load_all_from_raw("urls.jsonl")
        existing_entries = load_jsonl(OUTPUT_FILE)
    except Exception as e:
        logger.error(f"Failed to load input files: {e}")
        raise

    # Build URL indexes
    urls_by_url = build_url_index(urls_entries)
    existing_urls = {entry["url"] for entry in existing_entries if "url" in entry}

    # Stats
    count_descriptions = len(descriptions_entries)
    count_urls = len(urls_entries)
    count_existing = len(existing_entries)
    count_added = 0
    count_skipped_not_success = 0
    count_skipped_already_exists = 0
    count_skipped_non_english = 0
    count_skipped_not_gaza = 0
    missing_urls = []

    # Process descriptions
    new_entries = []

    for entry in descriptions_entries:
        # Only process successful scrapes
        if not entry.get("success", False):
            count_skipped_not_success += 1
            continue

        url = entry.get("url")
        if not url:
            problems.append("Entry missing URL field")
            continue

        # Skip if already in output
        if url in existing_urls:
            count_skipped_already_exists += 1
            continue

        # Look up supplementary data from urls file
        if url not in urls_by_url:
            missing_urls.append(url)
            continue

        urls_data = urls_by_url[url]

        # Filter out non-English entries
        if urls_data.get("language", "").lower() != "en":
            count_skipped_non_english += 1
            continue

        # Filter out non-Gaza-related entries
        if not is_gaza_related(entry):
            count_skipped_not_gaza += 1
            continue

        # Build cleaned entry with specified key order
        combined = {**urls_data, **entry}  # entry overwrites urls_data for shared keys
        cleaned_entry = {k: combined[k] for k in OUTPUT_KEY_ORDER if k in combined}

        new_entries.append(cleaned_entry)
        existing_urls.add(url)  # Prevent duplicates within this run
        count_added += 1

    # CRITICAL ERROR: URLs not found in urls file
    if missing_urls:
        error_msg = (
            "\n" + "=" * 80 + "\n"
            "CRITICAL ERROR\n"
            "=" * 80 + "\n"
            f"Found {len(missing_urls)} URL(s) in descriptions.jsonl that DO NOT EXIST in urls.jsonl!\n"
            "This is a data integrity issue that must be resolved.\n\n"
            "Missing URLs:\n"
        )
        for url in missing_urls[:10]:  # Show first 10
            error_msg += f"  - {url}\n"
        if len(missing_urls) > 10:
            error_msg += f"  ... and {len(missing_urls) - 10} more\n"
        error_msg += "=" * 80 + "\n"

        logger.error(f"CRITICAL: {len(missing_urls)} URLs not found in urls.jsonl")
        for url in missing_urls:
            logger.error(f"  Missing URL: {url}")

        print(error_msg, file=sys.stderr)
        sys.exit(1)

    # Calculate runtime
    end_time = datetime.now()
    duration = end_time - start_time

    # Log summary
    logger.info(f"Input (descriptions): {count_descriptions} entries")
    logger.info(f"Input (urls): {count_urls} entries")
    logger.info(f"Output file (before run): {count_existing} entries")
    logger.info(f"Skipped (success=false): {count_skipped_not_success}")
    logger.info(f"Skipped (non-English): {count_skipped_non_english}")
    logger.info(f"Skipped (not Gaza-related): {count_skipped_not_gaza}")
    logger.info(f"Skipped (already in output): {count_skipped_already_exists}")
    logger.info(f"Added to output: {count_added}")
    logger.info(f"Output file (after run): {count_existing + count_added} entries")
    if problems:
        for p in problems:
            logger.warning(f"Problem: {p}")
    logger.info(f"Duration: {duration.total_seconds():.2f} seconds")
    logger.info("RUN COMPLETED")

    # Print summary to stdout
    print(f"Completed in {duration.total_seconds():.2f}s")
    print(f"   - Entries in descriptions: {count_descriptions}")
    print(f"   - Entries in urls: {count_urls}")
    print(f"   - Entries already in output: {count_existing}")
    print(f"   - Skipped (not successful): {count_skipped_not_success}")
    print(f"   - Skipped (non-English): {count_skipped_non_english}")
    print(f"   - Skipped (not Gaza-related): {count_skipped_not_gaza}")
    print(f"   - Skipped (already exists): {count_skipped_already_exists}")
    print(f"   - Added to output: {count_added}")
    print(f"   - Total in output now: {count_existing + count_added}")

    if count_added == 0:
        print("\nNo new entries to add.")
        return

    # Write new entries (combine with existing, sort by date desc + URL hash for stable recency-first order)
    all_entries = existing_entries + new_entries
    all_entries.sort(key=entry_sort_key)

    # Print stories per media_url and date
    from collections import Counter
    media_counts = Counter(e.get("media_url", "unknown") for e in all_entries)
    date_counts = Counter(e.get("publish_date", "unknown") for e in all_entries)

    print("\nStories per media outlet:")
    for media, count in sorted(media_counts.items(), key=lambda x: -x[1]):
        print(f"   {media}: {count}")

    print("\nStories per date:")
    for date, count in sorted(date_counts.items()):
        print(f"   {date}: {count}")

    # Prompt for integration (or auto-integrate)
    print("\n" + "=" * 50)

    if args.auto:
        response = "y"
    else:
        response = input("Write to MINA's knowledge base? (y/n): ").strip().lower()

    if response != "y":
        print("Integration skipped. No changes made.")
    else:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for entry in all_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        print(f"Written to: {OUTPUT_FILE}")
        print(f"   ({len(all_entries)} entries)")


if __name__ == "__main__":
    main()
