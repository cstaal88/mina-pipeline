#!/usr/bin/env python3
"""
gist-overview.py

Overview of gist contents for MINA news data pipeline.

Fetches and analyzes the raw.jsonl and clean.jsonl files from each topic's gist,
providing stats, samples, and evaluation of cleaning procedures.

Requires GITHUB_TOKEN or GIST_PAT environment variable for private gists.
"""

import json
import os
import sys
from collections import Counter
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import TOPICS


def get_gist_content(gist_id: str, filename: str) -> list[dict]:
    """Fetch and parse a JSONL file from a gist."""
    token = os.getenv('GITHUB_TOKEN') or os.getenv('GIST_PAT')
    headers = {'Authorization': f'token {token}'} if token else {}

    url = f'https://api.github.com/gists/{gist_id}'
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error fetching gist {gist_id}: {response.status_code}")
        return []

    gist_data = response.json()
    if filename not in gist_data['files']:
        print(f"File {filename} not found in gist {gist_id}")
        return []

    file_info = gist_data['files'][filename]
    
    # If file is truncated (>1MB), fetch from raw_url instead
    if file_info.get('truncated', False):
        raw_url = file_info.get('raw_url')
        if raw_url:
            raw_response = requests.get(raw_url, headers=headers)
            if raw_response.status_code == 200:
                content = raw_response.text
            else:
                print(f"Error fetching raw content for {filename}: {raw_response.status_code}")
                return []
        else:
            print(f"File {filename} is truncated but no raw_url available")
            return []
    else:
        content = file_info['content']
    
    entries = []
    for line in content.split('\n'):
        line = line.strip()
        if line:
            try:
                entry = json.loads(line)
                if not entry.get('_meta'):  # Skip meta headers
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


def extract_date(entry: dict, is_raw: bool = False) -> str:
    """Extract date from entry - both raw and clean now have publish_date."""
    # Both raw and clean have publish_date (raw was merged from urls.jsonl)
    return entry.get('publish_date', 'unknown')


def extract_media(entry: dict, is_raw: bool = False) -> str:
    """Extract media from entry, handling raw vs clean data."""
    if is_raw:
        # Raw data: extract domain from url
        return extract_domain(entry.get('url', ''))
    else:
        return entry.get('media_url', 'unknown')


def analyze_entries(entries: list[dict], label: str, is_raw: bool = False) -> dict:
    """Analyze a list of entries and return stats."""
    if not entries:
        return {
            'count': 0,
            'date_range': {'earliest': None, 'latest': None},
            'date_counts': {},
            'all_media': [],
            'samples': [],
        }
    dates = [extract_date(e, is_raw) for e in entries]
    media = [extract_media(e, is_raw) for e in entries]

    date_counts = Counter(dates)
    media_counts = Counter(media)

    # Compute per-media summary lengths (words)
    media_word_lengths: dict[str, list[int]] = defaultdict(list)
    for e in entries:
        m = extract_media(e, is_raw)
        desc = (e.get('description') or '')
        word_len = len(desc.split()) if desc else 0
        media_word_lengths[m].append(word_len)

    media_stats: list[tuple[str, int, float]] = []
    for m, cnt in media_counts.items():
        lengths = media_word_lengths.get(m, [])
        mean_words = float(sum(lengths) / len(lengths)) if lengths else 0.0
        media_stats.append((m, cnt, mean_words))
    # sort by count desc
    media_stats.sort(key=lambda x: -x[1])

    # Sort dates (exclude 'unknown')
    valid_dates = sorted([d for d in date_counts.keys() if d and d != 'unknown'])
    date_range = {
        'earliest': valid_dates[0] if valid_dates else None,
        'latest': valid_dates[-1] if valid_dates else None,
    }

    # Sample records (first 1)
    samples = entries[:1]

    return {
        'count': len(entries),
        'date_range': date_range,
        'date_counts': dict(date_counts),
        'all_media': media_stats,  # List of (media, count, mean_words)
        'samples': samples,
    }


def print_topic_overview(topic_name: str, gist_id: str):
    """Print overview for a topic's gist."""
    print(f"\n{'='*80}")
    print(f"TOPIC: {topic_name.upper()}")
    print(f"Gist ID: {gist_id}")
    print(f"{'='*80}")

    # Fetch data
    raw_entries = get_gist_content(gist_id, 'raw.jsonl')
    clean_entries = get_gist_content(gist_id, 'clean.jsonl')

    # Analyze
    raw_stats = analyze_entries(raw_entries, 'Raw', is_raw=True)
    clean_stats = analyze_entries(clean_entries, 'Clean', is_raw=False)

    # Print stats (RAW)
    print(f"\nRAW DATA (unfiltered scraped articles):")
    print(f"  Total entries: {raw_stats['count']}")
    if raw_stats['date_range']['earliest']:
        print(f"  Date range: {raw_stats['date_range']['earliest']} to {raw_stats['date_range']['latest']}")

    # Date histogram (ASCII bars)
    if raw_stats.get('date_counts'):
        counts = raw_stats['date_counts']
        # Build ordered list of dates
        dates_sorted = sorted(d for d in counts.keys() if d and d != 'unknown')
        if dates_sorted:
            max_count = max(counts[d] for d in dates_sorted)
            bar_width = 40
            print("\n  Stories per date:")
            for d in dates_sorted:
                c = counts[d]
                length = int((c / max_count) * bar_width) if max_count > 0 else 0
                # Use full block characters for denser bars
                bar_char = '█'
                bar = bar_char * max(1, length)
                print(f"    {d} | {bar:<{bar_width}} {c}")

    # Media sources: stories per outlet and mean summary length (all outlets)
    media_list = raw_stats.get('all_media', [])
    if media_list:
        bar_width = 30
        max_cnt = max(c for _, c, _ in media_list) if media_list else 0
        print("\n  Stories per outlet:")
        for m, c, _ in media_list:
            length = int((c / max_cnt) * bar_width) if max_cnt > 0 else 0
            bar = '█' * max(1, length)
            print(f"    {m:20} | {bar:<{bar_width}} {c}")

        max_mean = max(mean for _, _, mean in media_list) if media_list else 0.0
        print("\n  Mean summary length (words) per outlet:")
        for m, _, mean in media_list:
            length = int((mean / max_mean) * bar_width) if max_mean > 0 else 0
            bar = '█' * max(1, length)
            print(f"    {m:20} | {bar:<{bar_width}} {mean:.0f}")

    # Print stats (CLEAN)
    print(f"\nCLEAN DATA (filtered articles):")
    print(f"  Total entries: {clean_stats['count']}")
    if clean_stats['date_range']['earliest']:
        print(f"  Date range: {clean_stats['date_range']['earliest']} to {clean_stats['date_range']['latest']}")

    # Clean date histogram
    if clean_stats.get('date_counts'):
        counts = clean_stats['date_counts']
        dates_sorted = sorted(d for d in counts.keys() if d and d != 'unknown')
        if dates_sorted:
            max_count = max(counts[d] for d in dates_sorted)
            bar_width = 40
            print("\n  Stories per date (clean):")
            for d in dates_sorted:
                c = counts[d]
                length = int((c / max_count) * bar_width) if max_count > 0 else 0
                bar_char = '█'
                bar = bar_char * max(1, length)
                print(f"    {d} | {bar:<{bar_width}} {c}")

    # Media sources (top 10)
    media_list = clean_stats.get('all_media', [])
    if media_list:
        bar_width = 30
        max_cnt = max(c for _, c, _ in media_list) if media_list else 0
        print("\n  Stories per outlet (clean):")
        for m, c, _ in media_list:
            length = int((c / max_cnt) * bar_width) if max_cnt > 0 else 0
            bar = '█' * max(1, length)
            print(f"    {m:20} | {bar:<{bar_width}} {c}")

        max_mean = max(mean for _, _, mean in media_list) if media_list else 0.0
        print("\n  Mean summary length (words) per outlet (clean):")
        for m, _, mean in media_list:
            length = int((mean / max_mean) * bar_width) if max_mean > 0 else 0
            bar = '█' * max(1, length)
            print(f"    {m:20} | {bar:<{bar_width}} {mean:.0f}")

    # Simple retention metric (no warnings)
    if raw_stats['count'] > 0:
        retention_rate = clean_stats['count'] / raw_stats['count'] * 100
        print(f"\nCLEANING EFFECTIVENESS: Retention rate: {retention_rate:.1f}% ({clean_stats['count']}/{raw_stats['count']})")

    # Sample records
    if clean_stats['samples']:
        print(f"\nSAMPLE CLEAN RECORDS:")
        for i, sample in enumerate(clean_stats['samples'], 1):
            print(f"  {i}. {sample.get('title', 'No title')[:60]}...")
            print(f"     URL: {sample.get('url', 'N/A')}")
            print(f"     Date: {sample.get('publish_date', 'N/A')}")
            print(f"     Media: {sample.get('media_url', 'N/A')}")
            print()


def main():
    """Main function."""
    print("MINA News Data Pipeline - Gist Overview")
    print(f"Generated: {datetime.now().isoformat()}")
    print()

    for topic_name, config in TOPICS.items():
        gist_id = config['gist_id']
        print_topic_overview(topic_name, gist_id)
        # Add spacing between topics for readability
        print("\n")



if __name__ == "__main__":
    main()