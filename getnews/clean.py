#!/usr/bin/env python3
"""
Unified data processing pipeline for RSS and MediaCloud data.

Usage:
    python clean.py                  # Process locally (no gist operations)
    python clean.py --push           # Download from gist, merge all sources, upload

Flow:
    1. Read tmp-news.json from fetch-raw.py (RSS data)
    2. Download existing raw.jsonl and mediacloud_raw.jsonl from gist  
    3. Merge RSS + MediaCloud data → unified raw.jsonl
    4. For each topic: strict filter → clean-{topic}.jsonl
    5. If --push: upload unified raw.jsonl and all clean files

Data Sources:
    - RSS feeds (via fetch-raw.py) → immediate processing
    - MediaCloud (via separate workflow) → merged from mediacloud_raw.jsonl
"""

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from langdetect import detect, LangDetectException

from config import (
    GIST_ID,
    TOPICS,
    ACTIVE_TOPICS,
    RAW_STORIES_FILE,
    TEST_DIR,
    EXCLUDED_FROM_CLEAN,
    GLOBAL_FILTERS,
)

# raw.jsonl passed 17 MB in mid-2026 and grows daily. The old 30s read timeout
# was half the write budget for the same bytes, and a timeout here used to be
# indistinguishable from "file not found" -- see GistError.
GIST_TIMEOUT = 120
GIST_UPLOAD_TIMEOUT = 120


def generate_id(url: str) -> str:
    """Generate a unique ID from URL."""
    return hashlib.sha256(url.encode()).hexdigest()


def normalize_url(url: str) -> str:
    """Normalize URL for deduplication: strip trailing slash, query params, fragments."""
    if not url:
        return url
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(url)
    # Keep scheme, netloc, path; drop query params and fragments
    normalized = urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip('/'), '', '', ''))
    return normalized


def domain_from_url(url: str) -> str:
    """Extract actual domain from URL (e.g. pagesix.com from pagesix.com/...)."""
    if not url:
        return ""
    from urllib.parse import urlparse
    netloc = urlparse(url).netloc
    return netloc.replace('www.', '') if netloc else ""


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


def has_excluded_terms(story: dict, exclude_terms: list[str]) -> bool:
    """Check if story contains any excluded terms."""
    if not exclude_terms:
        return False
    title = (story.get("title") or "").lower()
    summary = (story.get("summary") or story.get("description") or "").lower()
    text = f"{title} {summary}"
    return any(term in text for term in exclude_terms)


def matches_gated_issues(story: dict, topic: dict) -> bool:
    """
    Context-gated issue match, for tiers that want issue coverage without the
    noise of bare issue terms. A story qualifies only if it mentions BOTH an
    issue term (context_issues) AND an election-context term (context_terms) --
    e.g. "immigration" is kept only alongside "midterm"/"election"/"campaign".

    Topics without both lists (focused, broad) get no gating and this is a
    no-op, so they keep matching on plain keywords exactly as before.
    """
    issues = topic.get("context_issues")
    context = topic.get("context_terms")
    if not issues or not context:
        return False
    title = (story.get("title") or "").lower()
    summary = (story.get("summary") or story.get("description") or "").lower()
    text = f"{title} {summary}"
    return any(i in text for i in issues) and any(c in text for c in context)


def is_english(story: dict) -> bool:
    """Check if story is in English using langdetect."""
    title = story.get("title") or ""
    summary = story.get("summary") or story.get("description") or ""
    text = f"{title} {summary}".strip()

    if len(text) < 20:  # Too short to detect reliably
        return True  # Assume English if too short

    try:
        return detect(text) == "en"
    except LangDetectException:
        return True  # Assume English on detection failure


def passes_global_filters(story: dict) -> tuple[bool, str]:
    """
    Check if story passes all global filters.
    Returns (passed, reason) where reason explains why it failed.
    """
    # Check description requirement
    if GLOBAL_FILTERS.get("require_description", False):
        desc = story.get("summary") or story.get("description") or ""
        if not desc.strip():
            return False, "no_description"
    
    # Check English requirement
    if GLOBAL_FILTERS.get("require_english", False):
        if not is_english(story):
            return False, "not_english"
    
    # Check promo terms
    promo_terms = GLOBAL_FILTERS.get("promo_terms", [])
    if promo_terms:
        title = (story.get("title") or "").lower()
        summary = (story.get("summary") or story.get("description") or "").lower()
        text = f"{title} {summary}"
        for term in promo_terms:
            if term in text:
                return False, f"promo:{term}"
    
    return True, ""



def format_story_for_raw(story: dict) -> dict:
    """Convert RSS story to raw.jsonl schema."""
    url = story.get("url", "")
    # Use actual domain from URL (catches e.g. pagesix.com from nypost feed)
    actual_domain = domain_from_url(url) or story.get("domain", "")

    return {
        "id": generate_id(url),
        "indexed_date": None,
        "language": "en",
        "media_name": actual_domain,
        "media_url": actual_domain,
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


class GistError(RuntimeError):
    """
    A gist operation failed for a reason other than the file being absent.
    """


def gist_list_files() -> set[str]:
    """Filenames currently in the unified gist. Raises GistError if unknowable."""
    try:
        result = subprocess.run(
            ["gh", "gist", "view", GIST_ID, "--files"],
            capture_output=True,
            text=True,
            timeout=GIST_TIMEOUT,
        )
    except Exception as e:
        raise GistError(f"could not list gist files: {e}") from e

    if result.returncode != 0:
        raise GistError(f"could not list gist files: {result.stderr.strip()}")

    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def gist_download(filename: str) -> str | None:
    """
    Download a file from the unified gist.

    Returns the file's content, or None ONLY if the gist genuinely has no such
    file. Raises GistError on any other failure (timeout, rate limit, auth),
    because callers use None to mean "start fresh" -- see GistError.
    """
    try:
        result = subprocess.run(
            ["gh", "gist", "view", GIST_ID, "-f", filename],
            capture_output=True,
            text=True,
            timeout=GIST_TIMEOUT,
        )
    except subprocess.TimeoutExpired as e:
        raise GistError(
            f"timed out after {GIST_TIMEOUT}s downloading {filename}"
        ) from e
    except Exception as e:
        raise GistError(f"error downloading {filename}: {e}") from e

    if result.returncode == 0:
        return result.stdout

    # Non-zero: distinguish "no such file" from a broken call by asking the gist
    # what it holds. Don't sniff stderr text -- gh's wording is not a contract.
    if filename in gist_list_files():
        raise GistError(
            f"{filename} exists in the gist but could not be downloaded: "
            f"{result.stderr.strip()}"
        )
    return None


def gist_upload(filename: str, filepath: Path) -> bool:
    """
    Upload a file to the unified gist. Returns success.

    `gh gist edit -f` only SELECTS an existing gist file, so the first upload of
    a new topic's clean file fails. Fall back to --add, which creates it. --add
    takes the gist filename from the local file's basename, so stage a copy
    under the right name when they differ.
    """
    try:
        result = subprocess.run(
            ["gh", "gist", "edit", GIST_ID, "-f", filename, str(filepath)],
            capture_output=True,
            text=True,
            timeout=GIST_UPLOAD_TIMEOUT,
        )
        if result.returncode == 0:
            return True

        if filename in gist_list_files():
            print(f"  Warning: gist edit failed: {result.stderr.strip()}")
            return False

        print(f"  {filename} not in gist yet - adding it")
        with tempfile.TemporaryDirectory() as tmpdir:
            staged = Path(tmpdir) / filename
            shutil.copyfile(filepath, staged)
            add_result = subprocess.run(
                ["gh", "gist", "edit", GIST_ID, "--add", str(staged)],
                capture_output=True,
                text=True,
                timeout=GIST_UPLOAD_TIMEOUT,
            )
        if add_result.returncode != 0:
            print(f"  Warning: gist add failed: {add_result.stderr.strip()}")
        return add_result.returncode == 0
    except GistError:
        raise
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

    # Save ALL stories to raw.jsonl (no topic filtering)
    print(f"\n=== SAVING ALL STORIES TO RAW ARCHIVE ===")
    print(f"Archiving all {len(stories)} stories (no topic filtering)")
    print("  → Future-proof: enables any topic analysis retroactively")

    # Format all stories for raw.jsonl
    formatted = [format_story_for_raw(s) for s in stories]

    # Load existing raw data (RSS + MediaCloud unified dataset)
    existing_raw = []
    # How many records the gist's raw.jsonl held before this run. The upload is
    # blocked below if we somehow ended up with fewer -- the archive must never
    # shrink.
    gist_raw_count = 0
    local_dir = TEST_DIR / "unified"
    local_raw = local_dir / "raw.jsonl"

    if push:
        print("\nDownloading existing raw.jsonl from gist...")
        # Raises GistError if the download fails; only returns None when the
        # gist truly has no raw.jsonl yet (i.e. a genuinely fresh start).
        content = gist_download("raw.jsonl")
        if content is None:
            print("  Gist has no raw.jsonl yet - starting fresh")
        else:
            # Save backup
            backup_file = local_dir / "raw-backup-before-push.jsonl"
            backup_file.parent.mkdir(parents=True, exist_ok=True)
            backup_file.write_text(content, encoding="utf-8")
            print(f"  Backup saved: {backup_file}")

            existing_raw = parse_jsonl_content(content)
            gist_raw_count = len(existing_raw)
            print(f"  Existing records: {len(existing_raw)}")

        # Also download and merge MediaCloud data (dedupe by URL)
        print("\nDownloading MediaCloud data to merge...")
        mediacloud_content = gist_download("mediacloud_raw.jsonl")
        if mediacloud_content:
            mediacloud_records = parse_jsonl_content(mediacloud_content)
            print(f"  MediaCloud records available: {len(mediacloud_records)}")
            
            # Add source identifier to MediaCloud records if not present
            for record in mediacloud_records:
                if "collected_with" not in record:
                    record["collected_with"] = "mediacloud"
            
            # Only add MC records not already in raw.jsonl (by normalized URL)
            existing_urls = {normalize_url(r.get("url", "")) for r in existing_raw}
            new_mc = [r for r in mediacloud_records if normalize_url(r.get("url", "")) not in existing_urls]
            existing_raw.extend(new_mc)
            print(f"  New MediaCloud records merged: {len(new_mc)}")
            print(f"  Skipped (already in raw): {len(mediacloud_records) - len(new_mc)}")
            print(f"  Total after merge: {len(existing_raw)}")
        else:
            print("  No MediaCloud data found to merge")
    else:
        existing_raw = load_jsonl(local_raw)
        if existing_raw:
            print(f"Existing local records: {len(existing_raw)}")

    # Merge and dedupe by normalized URL
    existing_urls = {normalize_url(r.get("url", "")) for r in existing_raw}
    new_records = [r for r in formatted if normalize_url(r.get("url", "")) not in existing_urls]
    
    print(f"\n=== NEW DATA SUMMARY ===")
    print(f"New records (after dedupe): {len(new_records)}")
    if len(new_records) == 0:
        print(f"  → All {len(formatted)} stories were already in the dataset")
        print("  → No duplicates - RSS feeds contained no new content")
    else:
        print(f"  → Added {len(new_records)} new stories to archive")
    
    all_raw = existing_raw + new_records

    # Save raw.jsonl
    raw_meta = {
        "_meta": True,
        "record_count": len(all_raw),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    save_jsonl(local_raw, all_raw, raw_meta)
    print(f"\nSaved raw.jsonl: {len(all_raw)} total records")

    # Generate clean files for each active topic
    topic_keys = ACTIVE_TOPICS or list(TOPICS.keys())
    clean_files = {}

    print(f"\n=== REGENERATING CLEAN FILES (from all {len(all_raw)} records) ===")
    print(f"Processing {len(topic_keys)} topic(s)...")
    for topic_name in topic_keys:
        if topic_name not in TOPICS:
            print(f"  Unknown topic: {topic_name}")
            continue

        topic_cfg = TOPICS[topic_name]
        keywords = topic_cfg["keywords"]
        exclude_terms = topic_cfg.get("exclude_terms", [])

        # Filter raw records for this topic (loose match first, then strict).
        # A record qualifies via plain keywords OR via a context-gated issue
        # match (issue term + election-context term) -- the latter is how the
        # "medium" tier picks up issue coverage; focused/broad have no gate.
        # Apply: strict keywords, excluded domains, global filters, topic-specific exclusions
        topic_raw = [
            r for r in all_raw
            if matches_keywords(r, keywords) or matches_gated_issues(r, topic_cfg)
        ]
        topic_clean = [
            r for r in topic_raw
            if (matches_strict_keywords(r, keywords) or matches_gated_issues(r, topic_cfg))
            and r.get("media_url", "") not in EXCLUDED_FROM_CLEAN
            and passes_global_filters(r)[0]
            and not has_excluded_terms(r, exclude_terms)
        ]

        # Content-based dedup: remove stories with identical title+description pair,
        # OR identical title alone (catches AP wire syndication across outlets where
        # the same story gets slightly different descriptions on apnews.com vs
        # abcnews.go.com etc.). Minimum title length avoids false positives on
        # short/generic headlines.
        before_dedup = len(topic_clean)
        seen_content = set()  # (title, desc) pairs
        seen_titles = set()   # titles alone
        deduped = []
        for r in topic_clean:
            title = (r.get("title") or "").strip().lower()
            desc = (r.get("description") or "").strip().lower()
            if (title, desc) in seen_content:
                continue
            if title and len(title) > 40 and title in seen_titles:
                continue
            seen_content.add((title, desc))
            if title and len(title) > 40:
                seen_titles.add(title)
            deduped.append(r)
        topic_clean = deduped
        content_dupes = before_dedup - len(topic_clean)

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

        filtered_out = len(topic_raw) - len(topic_clean)
        print(f"  {topic_name}: {len(topic_raw)} raw → {len(topic_clean)} clean")
        if content_dupes > 0:
            print(f"    └─ {content_dupes} content duplicates removed (syndication/URL variants)")
        if filtered_out > 0:
            print(f"    └─ {filtered_out} filtered out (non-English, excluded domains, or strict keyword mismatch)")

    print(f"\nAll files saved to {local_dir}/")

    # Push to gist if requested
    upload_failed = False
    if push:
        # Last line of defence: whatever else went wrong, never publish a
        # raw.jsonl smaller than the one we downloaded.
        if len(all_raw) < gist_raw_count:
            print(
                f"\nABORT: refusing to upload - raw.jsonl would shrink from "
                f"{gist_raw_count} to {len(all_raw)} records. "
                f"Nothing was pushed; the gist is untouched."
            )
            return 1

        print("\nUploading to gist...")

        if gist_upload("raw.jsonl", local_raw):
            print("  ✓ raw.jsonl")
        else:
            print("  ✗ raw.jsonl (failed)")
            upload_failed = True

        for filename, filepath in clean_files.items():
            if gist_upload(filename, filepath):
                print(f"  ✓ {filename}")
            else:
                print(f"  ✗ {filename} (failed)")
                upload_failed = True

        print(f"\nGist: https://gist.github.com/{GIST_ID}")

    # Summary
    print("\n" + "=" * 50)
    print("FINAL SUMMARY")
    print("=" * 50)
    print(f"  Fetched: {len(stories)} stories from RSS feeds")
    print(f"  Archived: {len(formatted)} stories (saves everything for future analysis)")
    print(f"  New records: +{len(new_records)} added → {len(all_raw)} total in archive")
    print("")
    print("  Clean files (from entire archive):")
    for topic_name in topic_keys:
        if topic_name in TOPICS:
            cfg = TOPICS[topic_name]
            keywords = cfg["keywords"]
            topic_raw = [r for r in all_raw
                         if matches_keywords(r, keywords) or matches_gated_issues(r, cfg)]
            topic_clean = [r for r in topic_raw
                           if matches_strict_keywords(r, keywords) or matches_gated_issues(r, cfg)]
            print(f"    {topic_name}: {len(topic_clean)} stories (from {len(topic_raw)} raw matches)")

    if upload_failed:
        print("\nOne or more gist uploads FAILED (see ✗ above).")
        return 1

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except GistError as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        print(
            "Refusing to continue: raw.jsonl is the only copy of the RSS archive, "
            "and rebuilding it from scratch would destroy it. Nothing was pushed.",
            file=sys.stderr,
        )
        sys.exit(1)
