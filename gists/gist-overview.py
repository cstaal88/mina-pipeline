#!/usr/bin/env python3
"""
gist-overview.py

Overview of unified gist contents for MINA news data pipeline.

Fetches and analyzes the raw.jsonl and clean-*.jsonl files from the unified gist,
providing stats, samples, and evaluation of cleaning procedures.
"""

import json
import os
import sys
from collections import Counter
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

# Unified gist (primary)
UNIFIED_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"

# Topics (for clean file names)
TOPICS = ["minneapolis-ice", "greenland-trump"]

# Old per-topic gists (archived, kept for reference)
# OLD_GISTS = {
#     "minneapolis-ice": "839f9f409d36d715d277095886ced536",
#     "greenland-trump": "a046f4a9233ff2e499dfeb356e081d79",
# }


def get_gist_files(gist_id: str) -> dict[str, str]:
    """Fetch all files from a gist. Returns {filename: content}."""
    token = os.getenv('GITHUB_TOKEN') or os.getenv('GIST_PAT')
    headers = {'Authorization': f'token {token}'} if token else {}

    url = f'https://api.github.com/gists/{gist_id}'
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error fetching gist {gist_id}: {response.status_code}")
        return {}

    gist_data = response.json()
    files = {}

    for filename, file_info in gist_data.get('files', {}).items():
        # If file is truncated (>1MB), fetch from raw_url instead
        if file_info.get('truncated', False):
            raw_url = file_info.get('raw_url')
            if raw_url:
                raw_response = requests.get(raw_url, headers=headers)
                if raw_response.status_code == 200:
                    files[filename] = raw_response.text
        else:
            files[filename] = file_info.get('content', '')

    return files


def parse_jsonl(content: str) -> list[dict]:
    """Parse JSONL content, skip meta lines."""
    entries = []
    for line in content.split('\n'):
        line = line.strip()
        if line:
            try:
                entry = json.loads(line)
                if not entry.get('_meta'):
                    entries.append(entry)
            except json.JSONDecodeError:
                continue
    return entries


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    if not url:
        return 'unknown'
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc.replace('www.', '') or 'unknown'
    except Exception:
        return 'unknown'


def analyze_entries(entries: list[dict], is_raw: bool = False) -> dict:
    """Analyze a list of entries and return stats."""
    if not entries:
        return {'count': 0, 'date_range': {}, 'date_counts': {}, 'media_stats': []}

    dates = [e.get('publish_date', 'unknown') for e in entries]
    media = [e.get('media_url') or extract_domain(e.get('url', '')) for e in entries]

    date_counts = Counter(dates)
    media_counts = Counter(media)

    # Compute per-media summary lengths
    media_word_lengths: dict[str, list[int]] = defaultdict(list)
    for e in entries:
        m = e.get('media_url') or extract_domain(e.get('url', ''))
        desc = e.get('description') or ''
        media_word_lengths[m].append(len(desc.split()) if desc else 0)

    media_stats = []
    for m, cnt in media_counts.items():
        lengths = media_word_lengths.get(m, [])
        mean_words = sum(lengths) / len(lengths) if lengths else 0
        media_stats.append((m, cnt, mean_words))
    media_stats.sort(key=lambda x: -x[1])

    valid_dates = sorted([d for d in date_counts.keys() if d and d != 'unknown'])
    date_range = {
        'earliest': valid_dates[0] if valid_dates else None,
        'latest': valid_dates[-1] if valid_dates else None,
    }

    return {
        'count': len(entries),
        'date_range': date_range,
        'date_counts': dict(date_counts),
        'media_stats': media_stats,
        'samples': entries[:2],
    }


def print_histogram(title: str, counts: dict, bar_width: int = 40):
    """Print ASCII histogram."""
    sorted_keys = sorted([k for k in counts.keys() if k and k != 'unknown'])
    if not sorted_keys:
        return

    max_count = max(counts[k] for k in sorted_keys)
    print(f"\n  {title}:")
    for k in sorted_keys:
        c = counts[k]
        length = int((c / max_count) * bar_width) if max_count > 0 else 0
        bar = '█' * max(1, length)
        print(f"    {k} | {bar:<{bar_width}} {c}")


def print_media_stats(media_stats: list, bar_width: int = 30):
    """Print media statistics."""
    if not media_stats:
        return

    max_cnt = max(c for _, c, _ in media_stats)
    print("\n  Stories per outlet:")
    for m, c, _ in media_stats:
        length = int((c / max_cnt) * bar_width) if max_cnt > 0 else 0
        bar = '█' * max(1, length)
        print(f"    {m:25} | {bar:<{bar_width}} {c}")


def main():
    """Main function."""
    print("=" * 80)
    print("MINA News Data Pipeline - Unified Gist Overview")
    print(f"Generated: {datetime.now().isoformat()}")
    print(f"Gist ID: {UNIFIED_GIST_ID}")
    print("=" * 80)

    files = get_gist_files(UNIFIED_GIST_ID)
    if not files:
        print("No files found in gist")
        return

    print(f"\nFiles in gist: {', '.join(sorted(files.keys()))}")

    # Analyze raw.jsonl
    if 'raw.jsonl' in files:
        print(f"\n{'='*60}")
        print("RAW DATA (all topics combined)")
        print("=" * 60)

        raw_entries = parse_jsonl(files['raw.jsonl'])
        stats = analyze_entries(raw_entries, is_raw=True)

        print(f"\n  Total entries: {stats['count']}")
        if stats['date_range'].get('earliest'):
            print(f"  Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")

        print_histogram("Stories per date", stats['date_counts'])
        print_media_stats(stats['media_stats'])

    # Analyze each clean file
    for topic in TOPICS:
        filename = f"clean-{topic}.jsonl"
        if filename in files:
            print(f"\n{'='*60}")
            print(f"CLEAN DATA: {topic}")
            print("=" * 60)

            entries = parse_jsonl(files[filename])
            stats = analyze_entries(entries)

            print(f"\n  Total entries: {stats['count']}")
            if stats['date_range'].get('earliest'):
                print(f"  Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")

            print_histogram("Stories per date", stats['date_counts'])
            print_media_stats(stats['media_stats'])

            # Sample
            if stats['samples']:
                print(f"\n  Sample record:")
                s = stats['samples'][0]
                print(f"    Title: {s.get('title', 'N/A')[:70]}...")
                print(f"    Date: {s.get('publish_date', 'N/A')}")
                print(f"    URL: {s.get('url', 'N/A')[:60]}...")

    print("\n" + "=" * 80)
    print(f"View gist: https://gist.github.com/{UNIFIED_GIST_ID}")
    print("=" * 80)


if __name__ == "__main__":
    main()