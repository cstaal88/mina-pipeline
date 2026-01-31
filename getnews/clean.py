#!/usr/bin/env python3
"""
Filter stories and manage unified gist.

Usage:
    python clean.py                  # Process locally (no gist operations)
    python clean.py --push           # Download from gist, append new, upload

Flow:
    1. Read tmp-news.json from fetch-raw.py
    2. Filter stories matching ANY topic's keywords → raw.jsonl
    3. For each topic: strict filter → clean-{topic}.jsonl
    4. If --push: upload all files to unified gist
"""

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import (
    GIST_ID,
    TOPICS,
    ACTIVE_TOPICS,
    RAW_STORIES_FILE,
    TEST_DIR,
    EXCLUDED_FROM_CLEAN,
)


def generate_id(url: str) -> str:
    """Generate a unique ID from URL."""
    return hashlib.sha256(url.encode()).hexdigest()


def matches_keywords(story: dict, keywords: list[str]) -> bool:
    """Check if story title or summary contains any keyword (loose match)."""
    title = (story.get("title") or "").lower()
    summary = (story.get("summary") or "").lower()
    text = f"{title} {summary}"

    for kw in keywords:
        if kw.lower() in text:
            return True
    return False


def matches_strict_keywords(story: dict, keywords: list[str]) -> bool:
    """
    Strict matching for clean files:
    - Keyword in title, OR
    - Keyword appears 2+ times in summary
    """
    title = (story.get("title") or "").lower()
    summary = (story.get("summary") or story.get("description") or "").lower()

    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower in title:
            return True
        if summary.count(kw_lower) >= 2:
            return True
    return False


def get_all_keywords() -> list[str]:
    """Collect all keywords from all topics."""
    all_kw = set()
    for topic_config in TOPICS.values():
        all_kw.update(topic_config["keywords"])
    return list(all_kw)


def format_story_for_raw(story: dict) -> dict:
    """Convert RSS story to raw.jsonl schema."""
    url = story.get("url", "")

    return {
        "id": generate_id(url),
        "indexed_date": None,
        "language": "en",
        "media_name": story.get("domain", ""),
        "media_url": story.get("domain", ""),
        "publish_date": story.get("publish_date"),
        "title": story.get("title", ""),
        "url": url,
        "description": story.get("summary", ""),
        "collected_with": "rss",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "final_url": url,
        "http_status": None,
        "success": True,
        "error": None,
        "scraped_at": None,
    }


def load_stories() -> list[dict]:
    """Load stories from fetch-raw.py output."""
    if not RAW_STORIES_FILE.exists():
        print(f"ERROR: {RAW_STORIES_FILE} not found. Run fetch-raw.py first.")
        sys.exit(1)

    data = json.loads(RAW_STORIES_FILE.read_text(encoding="utf-8"))

    if isinstance(data, dict) and "stories" in data:
        return data["stories"]
    elif isinstance(data, list):
        return data
    else:
        print(f"ERROR: Unexpected format in {RAW_STORIES_FILE}")
        sys.exit(1)


def load_jsonl(path: Path) -> list[dict]:
    """Load JSONL file, skipping _meta lines."""
    if not path.exists():
        return []

    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta"):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


def save_jsonl(path: Path, records: list[dict], meta: dict | None = None) -> None:
    """Save records as JSONL with optional meta header."""
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    if meta:
        lines.append(json.dumps(meta, ensure_ascii=False))

    for record in records:
        lines.append(json.dumps(record, ensure_ascii=False))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def gist_download(filename: str) -> str | None:
    """Download a file from the unified gist. Returns content or None."""
    try:
        result = subprocess.run(
            ["gh", "gist", "view", GIST_ID, "-f", filename],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except Exception as e:
        print(f"  Warning: Could not download gist: {e}")
        return None


def gist_upload(filename: str, filepath: Path) -> bool:
    """Upload a file to the unified gist. Returns success."""
    try:
        result = subprocess.run(
            ["gh", "gist", "edit", GIST_ID, "-f", filename, str(filepath)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"  Warning: Could not upload to gist: {e}")
        return False


def parse_jsonl_content(content: str) -> list[dict]:
    """Parse JSONL string into list of records."""
    records = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta"):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


def main() -> int:
    push = "--push" in sys.argv

    print("=== CLEAN (Unified) ===")
    print(f"Mode: {'PUSH (will update gist)' if push else 'LOCAL (test only)'}")
    print(f"Gist: {GIST_ID}")
    print()

    # Load stories from fetch-raw.py
    stories = load_stories()
    print(f"Loaded {len(stories)} stories from {RAW_STORIES_FILE}")

    # Collect all keywords from all topics
    all_keywords = get_all_keywords()
    print(f"Keywords from all topics: {len(all_keywords)}")

    # Filter stories matching ANY topic's keywords
    matching = [s for s in stories if matches_keywords(s, all_keywords)]
    print(f"Stories matching any topic: {len(matching)}")

    # Format for raw.jsonl
    formatted = [format_story_for_raw(s) for s in matching]

    # Load existing raw data
    existing_raw = []
    local_dir = TEST_DIR / "unified"
    local_raw = local_dir / "raw.jsonl"

    if push:
        print("\nDownloading existing raw.jsonl from gist...")
        content = gist_download("raw.jsonl")
        if content is None:
            print("  No existing raw.jsonl (or download failed) - starting fresh")
        else:
            # Save backup
            backup_file = local_dir / "raw-backup-before-push.jsonl"
            backup_file.parent.mkdir(parents=True, exist_ok=True)
            backup_file.write_text(content, encoding="utf-8")
            print(f"  Backup saved: {backup_file}")

            existing_raw = parse_jsonl_content(content)
            print(f"  Existing records: {len(existing_raw)}")
    else:
        existing_raw = load_jsonl(local_raw)
        if existing_raw:
            print(f"Existing local records: {len(existing_raw)}")

    # Merge and dedupe by URL
    existing_urls = {r.get("url") for r in existing_raw}
    new_records = [r for r in formatted if r.get("url") not in existing_urls]
    print(f"New records (after dedupe): {len(new_records)}")

    all_raw = existing_raw + new_records

    # Save raw.jsonl
    raw_meta = {
        "_meta": True,
        "record_count": len(all_raw),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    save_jsonl(local_raw, all_raw, raw_meta)
    print(f"\nSaved raw.jsonl: {len(all_raw)} records")

    # Generate clean files for each active topic
    topic_keys = ACTIVE_TOPICS or list(TOPICS.keys())
    clean_files = {}

    print(f"\nGenerating clean files for {len(topic_keys)} topic(s)...")
    for topic_name in topic_keys:
        if topic_name not in TOPICS:
            print(f"  Unknown topic: {topic_name}")
            continue

        keywords = TOPICS[topic_name]["keywords"]

        # Filter raw records for this topic (loose match first, then strict)
        # Also exclude domains in EXCLUDED_FROM_CLEAN
        topic_raw = [r for r in all_raw if matches_keywords(r, keywords)]
        topic_clean = [
            r for r in topic_raw
            if matches_strict_keywords(r, keywords)
            and r.get("media_url", "") not in EXCLUDED_FROM_CLEAN
        ]

        clean_filename = f"clean-{topic_name}.jsonl"
        local_clean = local_dir / clean_filename

        clean_meta = {
            "_meta": True,
            "topic": topic_name,
            "record_count": len(topic_clean),
            "filtered_from": len(topic_raw),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        save_jsonl(local_clean, topic_clean, clean_meta)
        clean_files[clean_filename] = local_clean

        print(f"  {topic_name}: {len(topic_raw)} raw → {len(topic_clean)} clean")

    print(f"\nAll files saved to {local_dir}/")

    # Push to gist if requested
    if push:
        print("\nUploading to gist...")

        if gist_upload("raw.jsonl", local_raw):
            print("  ✓ raw.jsonl")
        else:
            print("  ✗ raw.jsonl (failed)")

        for filename, filepath in clean_files.items():
            if gist_upload(filename, filepath):
                print(f"  ✓ {filename}")
            else:
                print(f"  ✗ {filename} (failed)")

        print(f"\nGist: https://gist.github.com/{GIST_ID}")

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"  Raw: +{len(new_records)} new → {len(all_raw)} total")
    for topic_name in topic_keys:
        if topic_name in TOPICS:
            keywords = TOPICS[topic_name]["keywords"]
            topic_raw = [r for r in all_raw if matches_keywords(r, keywords)]
            topic_clean = [r for r in topic_raw if matches_strict_keywords(r, keywords)]
            print(f"  {topic_name}: {len(topic_clean)} clean (from {len(topic_raw)} matching)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
