#!/usr/bin/env python3
"""
clean.py

Combines data from raw/{topic}/{date}/ directories to create cleaned news articles.

Input files (from all date directories in raw/{topic}/):
  - raw/{topic}/{date}/articles.jsonl (scraped articles with my_topic field)
  - raw/{topic}/{date}/urls.jsonl (media_url, publish_date from MediaCloud)

Output file:
  - clean/articles-{topic}.jsonl (for local use)
  - /tmp/newsdata-{topic}.jsonl (for gist upload)

Logic:
  1. Only entries with "success": true are included
  2. Final output contains: description, title, url, media_url, publish_date, my_topic
  3. Entries already in output (by URL) are skipped
  4. Throws error if URL in articles.jsonl can't be found in urls.jsonl
  5. Filters for topic-related content and English language

CLI:
  --topic NAME         Topic to clean (default: DEFAULT_TOPIC from config)
  --stats / --dry-run  Print stats about the output file and exit (no writes, no prompts).
  --stats --all        Print stats for ALL raw collected items (before filtering).
  --auto               Automatically write without prompting.
"""

import argparse
import hashlib
import json
import logging
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

from config import get_topic_config, list_topics, DEFAULT_TOPIC

# === Configuration ===
SCRIPT_DIR = Path(__file__).parent.resolve()

LOG_FILE = SCRIPT_DIR / "clean.log"

# Output directories
CLEAN_DIR = SCRIPT_DIR / "clean"
TMP_OUTPUT_DIR = Path("/tmp")

# Keys to retain in final output (in order)
OUTPUT_KEY_ORDER = ["media_url", "title", "description", "publish_date", "url", "my_topic"]

# Max words for description truncation
MAX_DESCRIPTION_WORDS = 50


def truncate_description(text: str, max_words: int = MAX_DESCRIPTION_WORDS) -> str:
    """Truncate description to first N words, adding '...' if truncated."""
    if not text:
        return text
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "..."


def get_raw_dir(topic: str) -> Path:
    """Get raw directory for a topic."""
    return SCRIPT_DIR / "raw" / topic


def get_combined_raw_file(topic: str) -> Path:
    """Get combined raw file path for gist upload."""
    return SCRIPT_DIR / "raw" / topic / "_combined.jsonl"


def get_output_file(topic: str) -> Path:
    """Get clean output file path for a topic."""
    return CLEAN_DIR / f"articles-{topic}.jsonl"


def get_tmp_output_file(topic: str) -> Path:
    """Get temp output file path for gist upload."""
    return TMP_OUTPUT_DIR / f"newsdata-{topic}.jsonl"


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
    """Load all entries from a JSONL file, skipping metadata entries."""
    entries = []
    if not filepath.exists():
        return entries
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                entry = json.loads(line)
                # Skip metadata entries
                if entry.get("_meta") or entry.get("_manifest"):
                    continue
                entries.append(entry)
    return entries


def load_all_from_raw(topic: str, filename: str) -> list[dict]:
    """Load all entries from raw/{topic}/{date}/{filename} across all date directories."""
    all_entries = []
    raw_dir = get_raw_dir(topic)
    if not raw_dir.exists():
        return all_entries
    for date_dir in sorted(raw_dir.iterdir()):
        if date_dir.is_dir():
            file_path = date_dir / filename
            if file_path.exists():
                all_entries.extend(load_jsonl(file_path))
    return all_entries


def get_dates_collected(topic: str) -> list[str]:
    """Get list of date directories that have been collected."""
    raw_dir = get_raw_dir(topic)
    if not raw_dir.exists():
        return []
    dates = []
    for date_dir in sorted(raw_dir.iterdir()):
        if date_dir.is_dir() and not date_dir.name.startswith("_"):
            # Check if it has articles
            if (date_dir / "articles.jsonl").exists():
                dates.append(date_dir.name)
    return dates


def create_meta_header(topic: str, record_count: int, dates_collected: list[str]) -> dict:
    """Create a metadata header for JSONL files."""
    return {
        "_meta": True,
        "topic": topic,
        "record_count": record_count,
        "dates_collected": dates_collected,
        "date_range": {
            "start": dates_collected[0] if dates_collected else None,
            "end": dates_collected[-1] if dates_collected else None,
        },
        "last_updated": datetime.now().isoformat(),
    }


def combine_raw_files(topic: str) -> Path:
    """Combine all raw data into a single file with meta header.
    
    Merges urls.jsonl (publish_date, media_url, language, etc.) with
    articles.jsonl (description, success, etc.) by URL to create
    complete self-contained records.
    """
    articles = load_all_from_raw(topic, "articles.jsonl")
    urls = load_all_from_raw(topic, "urls.jsonl")
    dates = get_dates_collected(topic)
    
    # Build URL index from urls.jsonl
    urls_by_url = build_url_index(urls)
    
    # Merge: start with urls data, overlay articles data
    combined_records = []
    for article in articles:
        url = article.get("url")
        if not url:
            continue
        
        # Merge: urls fields first, then article fields (article overwrites shared keys like 'title')
        url_data = urls_by_url.get(url, {})
        merged = {**url_data, **article}
        combined_records.append(merged)
    
    combined_file = get_combined_raw_file(topic)
    
    with open(combined_file, "w", encoding="utf-8") as f:
        # Write meta header first
        meta = create_meta_header(topic, len(combined_records), dates)
        f.write(json.dumps(meta, ensure_ascii=False) + "\n")
        
        # Write all merged records
        for record in combined_records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    
    return combined_file


def build_url_index(entries: list[dict], key: str = "url") -> dict[str, dict]:
    """Build a dict mapping URL -> entry for fast lookups."""
    return {entry[key]: entry for entry in entries if key in entry}


def is_topic_related(entry: dict, filter_keywords: list[str], exclude_keywords: list[str] | None = None) -> bool:
    """Check if entry is related to topic based on title + description.
    
    Args:
        entry: Article entry dict
        filter_keywords: Keywords that MUST be present (at least one)
        exclude_keywords: Keywords that will EXCLUDE the article if present
    """
    title = entry.get("title") or ""
    description = entry.get("description") or ""
    text = (title + " " + description).lower()
    
    # Must match at least one include keyword
    if not any(keyword.lower() in text for keyword in filter_keywords):
        return False
    
    # Must NOT match any exclude keyword
    if exclude_keywords:
        if any(keyword.lower() in text for keyword in exclude_keywords):
            return False
    
    return True


def is_topic_relevant(entry: dict, topic_keywords: list[str]) -> bool:
    """Strict topic relevance check - article must be PRIMARILY about the topic.
    
    Heuristic:
      KEEP if ANY topic keyword appears in TITLE (case-insensitive), OR
      KEEP if ANY topic keyword appears 2+ times in DESCRIPTION (case-insensitive)
    
    This filters out articles that mention the topic only in passing.
    
    Args:
        entry: Article entry dict
        topic_keywords: Core keywords that define the topic (e.g., ["greenland", "denmark"])
    
    Returns:
        True if article is primarily about the topic
    """
    if not topic_keywords:
        return True  # No topic keywords configured = no filtering
    
    title = (entry.get("title") or "").lower()
    description = (entry.get("description") or "").lower()
    
    for keyword in topic_keywords:
        kw = keyword.lower()
        # Check if keyword in title (substring match)
        if kw in title:
            return True
        # Check if keyword appears 2+ times in description
        if description.count(kw) >= 2:
            return True
    
    return False


def entry_sort_key(entry: dict) -> tuple[str, str]:
    """Return a deterministic sort key: (inverted_date, url_hash).

    This ensures:
    - Recent items come first (newest dates sort earliest due to inversion)
    - Items from the same day have stable pseudo-random order (via URL hash)
    - Fully deterministic: adding new items doesn't change existing items' positions
    - When budget truncates, oldest items are cut first
    """
    date = entry.get("publish_date", "0000-00-00")
    inverted_date = "".join(str(9 - int(c)) if c.isdigit() else c for c in date)
    url_hash = hashlib.md5(entry.get("url", "").encode()).hexdigest()
    return (inverted_date, url_hash)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build cleaned news articles JSONL from raw data."
    )
    p.add_argument("--topic", type=str, default=None,
                   help=f"Topic to clean (default: {DEFAULT_TOPIC})")
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
        help="With --stats: show stats for ALL collected items (before filtering).",
    )
    p.add_argument(
        "--append",
        action="store_true",
        help="Append to existing clean.jsonl instead of regenerating from scratch.",
    )
    p.add_argument(
        "--no-write",
        action="store_true",
        help="Show what would be written but don't actually write.",
    )
    p.add_argument(
        "--list-topics",
        action="store_true",
        help="List available topics and exit.",
    )
    return p.parse_args(argv)


def print_output_stats(topic_config: dict, show_all: bool = False) -> int:
    """Describe the current output file without modifying anything."""
    topic = topic_config["name"]
    filter_keywords = topic_config["filter_keywords"]
    exclude_keywords = topic_config.get("exclude_keywords", [])
    output_file = get_output_file(topic)

    if show_all:
        # Show stats for ALL raw collected data (before filtering)
        urls_entries = load_all_from_raw(topic, "urls.jsonl")
        articles_entries = load_all_from_raw(topic, "articles.jsonl")

        if not urls_entries:
            print(f"No raw data found for topic '{topic}'", file=sys.stderr)
            return 2

        desc_by_url = build_url_index(articles_entries)

        print(f"Raw data stats for topic '{topic}' (ALL collected items, before filtering)")
        print(f"=" * 60)
        print(f"Total URLs collected: {len(urls_entries)}")
        print(f"Total articles scraped: {len(articles_entries)}")

        success_count = sum(1 for e in articles_entries if e.get("success", False))
        print(f"Successful scrapes: {success_count}")

        media_counts = Counter(e.get("media_url", "unknown") for e in urls_entries)
        date_counts = Counter(
            e.get("publish_date", "unknown")[:10] if e.get("publish_date") else "unknown"
            for e in urls_entries
        )

        english_count = sum(1 for e in urls_entries if e.get("language", "").lower() == "en")
        non_english_count = len(urls_entries) - english_count

        topic_related_count = 0
        for entry in urls_entries:
            url = entry.get("url")
            desc_entry = desc_by_url.get(url, {})
            combined = {**entry, **desc_entry}
            if is_topic_related(combined, filter_keywords, exclude_keywords):
                topic_related_count += 1

        print(f"\nFilter breakdown:")
        print(f"   English language: {english_count}")
        print(f"   Non-English: {non_english_count}")
        print(f"   Topic-related (of all): {topic_related_count}")

        print("\nStories per media outlet (ALL collected):")
        for media, count in sorted(media_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"   {media}: {count}")

        print("\nStories per date (ALL collected):")
        for date, count in sorted(date_counts.items()):
            print(f"   {date}: {count}")

        return 0

    # Default: show stats for cleaned output file
    if not output_file.exists():
        print(f"Not found: {output_file}", file=sys.stderr)
        return 2

    final_entries = load_jsonl(output_file)
    print(f"Output: {output_file} ({len(final_entries)} entries)")

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
    filter_keywords = topic_config["filter_keywords"]
    topic_keywords = topic_config.get("topic_keywords", [])
    exclude_keywords = topic_config.get("exclude_keywords", [])
    output_file = get_output_file(topic)
    tmp_output_file = get_tmp_output_file(topic)

    if args.stats or args.dry_run:
        raise SystemExit(print_output_stats(topic_config, show_all=args.all))

    start_time = datetime.now()
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info(f"RUN STARTED - Topic: {topic}")
    if args.append:
        logger.info("APPEND MODE: keeping existing clean file entries")
    else:
        logger.info("REGENERATE MODE: rebuilding clean file from scratch")

    problems = []

    # Ensure output directory exists
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # Load all data sources from raw/{topic}/{date}/ directories
    try:
        articles_entries = load_all_from_raw(topic, "articles.jsonl")
        urls_entries = load_all_from_raw(topic, "urls.jsonl")
        # Default: regenerate from scratch. --append: keep existing entries
        if args.append:
            existing_entries = load_jsonl(output_file)
        else:
            existing_entries = []
    except Exception as e:
        logger.error(f"Failed to load input files: {e}")
        raise

    # Build URL indexes
    urls_by_url = build_url_index(urls_entries)
    existing_urls = {entry["url"] for entry in existing_entries if "url" in entry}

    # Stats
    count_articles = len(articles_entries)
    count_urls = len(urls_entries)
    count_existing = len(existing_entries)
    count_added = 0
    count_skipped_not_success = 0
    count_skipped_already_exists = 0
    count_skipped_non_english = 0
    count_skipped_not_topic = 0
    count_skipped_not_relevant = 0
    missing_urls = []

    # Process articles
    new_entries = []

    for entry in articles_entries:
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

        # Filter out non-topic-related entries (broad filter)
        if not is_topic_related(entry, filter_keywords, exclude_keywords):
            count_skipped_not_topic += 1
            continue

        # Build cleaned entry with specified key order
        combined = {**urls_data, **entry}  # entry overwrites urls_data for shared keys
        
        # Ensure my_topic is set
        combined["my_topic"] = topic
        
        # Truncate long descriptions (e.g., Daily Wire puts full article in meta description)
        if combined.get("description"):
            combined["description"] = truncate_description(combined["description"])
        
        cleaned_entry = {k: combined[k] for k in OUTPUT_KEY_ORDER if k in combined}

        # Strict topic relevance filter (must be PRIMARILY about the topic)
        if topic_keywords and not is_topic_relevant(cleaned_entry, topic_keywords):
            count_skipped_not_relevant += 1
            continue

        new_entries.append(cleaned_entry)
        existing_urls.add(url)  # Prevent duplicates within this run
        count_added += 1

    # CRITICAL ERROR: URLs not found in urls file
    if missing_urls:
        error_msg = (
            "\n" + "=" * 80 + "\n"
            "CRITICAL ERROR\n"
            "=" * 80 + "\n"
            f"Found {len(missing_urls)} URL(s) in articles.jsonl that DO NOT EXIST in urls.jsonl!\n"
            "This is a data integrity issue that must be resolved.\n\n"
            "Missing URLs:\n"
        )
        for url in missing_urls[:10]:
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
    logger.info(f"Topic: {topic}")
    logger.info(f"Input (articles): {count_articles} entries")
    logger.info(f"Input (urls): {count_urls} entries")
    logger.info(f"Output file (before run): {count_existing} entries")
    logger.info(f"Skipped (success=false): {count_skipped_not_success}")
    logger.info(f"Skipped (non-English): {count_skipped_non_english}")
    logger.info(f"Skipped (not topic-related): {count_skipped_not_topic}")
    logger.info(f"Skipped (not relevant - off-topic): {count_skipped_not_relevant}")
    logger.info(f"Skipped (already in output): {count_skipped_already_exists}")
    logger.info(f"Added to output: {count_added}")
    logger.info(f"Output file (after run): {count_existing + count_added} entries")
    if problems:
        for p in problems:
            logger.warning(f"Problem: {p}")
    logger.info(f"Duration: {duration.total_seconds():.2f} seconds")
    logger.info("RUN COMPLETED")

    # Print summary to stdout
    print(f"\nTopic: {topic}")
    print(f"Completed in {duration.total_seconds():.2f}s")
    print(f"   - Entries in articles: {count_articles}")
    print(f"   - Entries in urls: {count_urls}")
    print(f"   - Entries already in output: {count_existing}")
    print(f"   - Skipped (not successful): {count_skipped_not_success}")
    print(f"   - Skipped (non-English): {count_skipped_non_english}")
    print(f"   - Skipped (not topic-related): {count_skipped_not_topic}")
    print(f"   - Skipped (not relevant - off-topic): {count_skipped_not_relevant}")
    print(f"   - Skipped (already exists): {count_skipped_already_exists}")
    print(f"   - Added to output: {count_added}")
    print(f"   - Total in output now: {count_existing + count_added}")

    if count_added == 0:
        print("\nNo new entries to add.")
        return

    # Write new entries (combine with existing, sort by date desc + URL hash)
    all_entries = existing_entries + new_entries
    all_entries.sort(key=entry_sort_key)

    # Print stories per media_url and date
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

    if args.no_write:
        print("No-write mode - no changes written.")
    else:
        # Get dates collected for meta header
        dates = get_dates_collected(topic)
        meta = create_meta_header(topic, len(all_entries), dates)
        
        # Write to output file with meta header
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for entry in all_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        print(f"Written to: {output_file}")

        # Also write to /tmp for gist upload (same content)
        with open(tmp_output_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for entry in all_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        print(f"Written to: {tmp_output_file} (for gist upload)")
        
        # Also combine raw files for gist upload
        combined_raw = combine_raw_files(topic)
        print(f"Combined raw: {combined_raw}")
        
        print(f"   ({len(all_entries)} clean entries)")


if __name__ == "__main__":
    main()
