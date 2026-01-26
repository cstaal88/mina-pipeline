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


def analyze_entries(entries: list[dict], label: str) -> dict:
    """Analyze a list of entries and return stats."""
    if not entries:
        return {
            'count': 0,
            'date_range': {'earliest': None, 'latest': None},
            'top_dates': {},
            'top_media': {},
            'samples': [],
        }

    dates = [e.get('publish_date', 'unknown') for e in entries]
    media = [e.get('media_url', 'unknown') for e in entries]

    date_counts = Counter(dates)
    media_counts = Counter(media)

    # Sort dates
    valid_dates = [d for d in dates if d != 'unknown']
    date_range = {
        'earliest': min(valid_dates) if valid_dates else None,
        'latest': max(valid_dates) if valid_dates else None,
    }

    # Sample records (first 3)
    samples = entries[:3]

    return {
        'count': len(entries),
        'date_range': date_range,
        'top_dates': dict(date_counts.most_common(5)),
        'top_media': dict(media_counts.most_common(5)),
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
    raw_stats = analyze_entries(raw_entries, 'Raw')
    clean_stats = analyze_entries(clean_entries, 'Clean')

    # Print stats
    print(f"\nRAW DATA (unfiltered scraped articles):")
    print(f"  Total entries: {raw_stats['count']}")
    if raw_stats['date_range']['earliest']:
        print(f"  Date range: {raw_stats['date_range']['earliest']} to {raw_stats['date_range']['latest']}")
    print(f"  Top dates: {raw_stats['top_dates']}")
    print(f"  Top media: {raw_stats['top_media']}")

    print(f"\nCLEAN DATA (filtered articles):")
    print(f"  Total entries: {clean_stats['count']}")
    if clean_stats['date_range']['earliest']:
        print(f"  Date range: {clean_stats['date_range']['earliest']} to {clean_stats['date_range']['latest']}")
    print(f"  Top dates: {clean_stats['top_dates']}")
    print(f"  Top media: {clean_stats['top_media']}")

    # Cleaning effectiveness
    if raw_stats['count'] > 0:
        retention_rate = clean_stats['count'] / raw_stats['count'] * 100
        print(f"\nCLEANING EFFECTIVENESS:")
        print(f"  Retention rate: {retention_rate:.1f}% ({clean_stats['count']}/{raw_stats['count']})")
        if retention_rate < 50:
            print("  ⚠️  Low retention - check filter keywords or scraping success")
        elif retention_rate > 90:
            print("  ✅ High retention - filters may be too permissive")
        else:
            print("  ✅ Reasonable retention - filters working well")

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



if __name__ == "__main__":
    main()