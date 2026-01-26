#!/usr/bin/env python3
"""
stats.py - Visualize and describe news data from gists or local files.

Usage:
    python stats.py                          # analyze all topics from gists
    python stats.py --topic minneapolis-ice  # analyze specific topic from gist
    python stats.py --local                  # use local clean/ files instead
    python stats.py --raw                    # analyze raw data (before filtering)
"""

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import requests

# Add parent directory to path to import config
SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR))
from config import TOPICS, get_topic_config

CLEAN_DIR = SCRIPT_DIR / "clean"
OUTPUT_DIR = SCRIPT_DIR / "stats-output"


def setup_publication_style():
    """Configure matplotlib for publication-quality charts (538/Economist style)."""
    plt.rcParams.update({
        # Typography
        'font.family': 'sans-serif',
        'font.sans-serif': ['Helvetica Neue', 'Helvetica', 'Arial', 'DejaVu Sans'],
        'font.size': 11,
        'axes.titlesize': 16,
        'axes.titleweight': 'bold',
        'axes.labelsize': 11,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,

        # Colors & style
        'axes.facecolor': '#f0f0f0',
        'figure.facecolor': 'white',
        'axes.edgecolor': 'none',
        'axes.grid': True,
        'grid.color': 'white',
        'grid.linewidth': 1.5,
        'axes.axisbelow': True,

        # Ticks
        'xtick.major.size': 0,
        'ytick.major.size': 0,
        'xtick.color': '#555555',
        'ytick.color': '#555555',

        # Figure
        'figure.dpi': 120,
        'savefig.dpi': 150,
        'savefig.bbox': 'tight',
        'savefig.pad_inches': 0.2,
    })


# Color palette inspired by quality publications
COLORS = {
    'primary': '#E6553A',      # Economist red-orange
    'secondary': '#3C91C2',    # 538 blue
    'tertiary': '#7CB5A0',     # Muted green
    'accent': '#F2B134',       # Warm yellow
    'dark': '#333333',
    'muted': '#888888',
}


def get_gist_content(gist_id: str, filename: str) -> list[dict]:
    """Fetch and parse a JSONL file from a gist (handles truncation)."""
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


def load_stories_from_file(jsonl_path: Path) -> list[dict]:
    """Load all stories from a single JSONL file."""
    stories = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                if not entry.get('_meta'):  # Skip meta headers
                    stories.append(entry)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse {jsonl_path.name} line {line_num}: {e}")
    return stories


def load_stories_local(topic: str) -> list[dict]:
    """Load stories from local clean/ directory."""
    clean_file = CLEAN_DIR / f"articles-{topic}.jsonl"
    if clean_file.exists():
        return load_stories_from_file(clean_file)
    return []


def load_stories_gist(topic: str, use_raw: bool = False) -> list[dict]:
    """Load stories from gist."""
    config = get_topic_config(topic)
    gist_id = config['gist_id']
    filename = 'raw.jsonl' if use_raw else 'clean.jsonl'
    return get_gist_content(gist_id, filename)


def create_stories_per_date_chart(dates_counter: Counter, output_path: Path, topic: str) -> None:
    """Create a time-series bar chart of stories per date."""
    setup_publication_style()

    # Sort dates and filter out 'unknown'
    sorted_items = sorted(
        [(d, c) for d, c in dates_counter.items() if d != 'unknown'],
        key=lambda x: x[0]
    )

    if not sorted_items:
        print("  No valid dates to visualize.")
        return

    dates = [datetime.strptime(d, '%Y-%m-%d') for d, _ in sorted_items]
    counts = [c for _, c in sorted_items]

    fig, ax = plt.subplots(figsize=(12, 5))

    # Create bars with uniform color
    bars = ax.bar(dates, counts, width=0.8, color=COLORS['secondary'], edgecolor='none')

    # Formatting
    ax.set_xlabel('')
    ax.set_ylabel('Number of Stories', color=COLORS['dark'])
    ax.set_title(f'Stories per date — {topic}', loc='left', pad=15, color=COLORS['dark'])

    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
    plt.xticks(rotation=45, ha='right')

    # Clean up spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color('#cccccc')

    plt.tight_layout()
    plt.subplots_adjust(top=0.92, bottom=0.18)

    chart_path = output_path / f'stories_per_date_{topic}.png'
    plt.savefig(chart_path, facecolor='white')
    plt.close()
    print(f"  Saved: {chart_path}")


def create_stories_per_outlet_chart(outlets_counter: Counter, output_path: Path, topic: str, top_n: int = 15) -> None:
    """Create a horizontal bar chart of stories per outlet."""
    setup_publication_style()

    # Get top outlets
    top_outlets = outlets_counter.most_common(top_n)

    if not top_outlets:
        print("  No outlets to visualize.")
        return

    # Reverse for horizontal bar chart (top item at top)
    outlets = [o for o, _ in reversed(top_outlets)]
    counts = [c for _, c in reversed(top_outlets)]

    fig, ax = plt.subplots(figsize=(10, max(6, len(outlets) * 0.4)))

    max_count = max(counts)

    # Create horizontal bars with uniform color
    y_pos = np.arange(len(outlets))
    bars = ax.barh(y_pos, counts, color=COLORS['secondary'], edgecolor='none', height=0.7)

    # Add value labels
    for bar, count in zip(bars, counts):
        width = bar.get_width()
        label_x = width + max_count * 0.01
        ax.text(label_x, bar.get_y() + bar.get_height()/2,
                f'{count:,}', va='center', ha='left',
                fontsize=9, color=COLORS['dark'], fontweight='medium')

    # Clean up outlet names for display
    clean_outlets = []
    for outlet in outlets:
        name = outlet.replace('www.', '').replace('.com', '').replace('.org', '')
        name = name.replace('.co.uk', '').replace('.net', '')
        if len(name) > 25:
            name = name[:22] + '...'
        clean_outlets.append(name)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(clean_outlets)
    ax.set_xlabel('')
    ax.set_title(f'Stories per outlet — {topic}', loc='left', pad=15, color=COLORS['dark'])

    # Extend x-axis for labels
    ax.set_xlim(0, max_count * 1.15)

    # Clean up spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)

    # Only horizontal grid
    ax.xaxis.grid(True)
    ax.yaxis.grid(False)

    plt.tight_layout()
    plt.subplots_adjust(top=0.92, bottom=0.08)

    chart_path = output_path / f'stories_per_outlet_{topic}.png'
    plt.savefig(chart_path, facecolor='white')
    plt.close()
    print(f"  Saved: {chart_path}")


def describe_data(stories: list[dict], topic: str, source_label: str) -> None:
    """Print statistics about news data."""
    print(f"\n{'='*60}")
    print(f"News Data Analysis: {topic}")
    print(f"Source: {source_label}")
    print(f"{'='*60}\n")

    if not stories:
        print("No stories found.")
        return

    # Basic counts
    print(f"Total stories: {len(stories):,}")
    print()

    # Stories per outlet
    outlets = Counter(story.get("media_url", "unknown") for story in stories)
    print(f"Number of unique outlets: {len(outlets)}")
    print("\nStories per outlet (top 20):")
    print("-" * 40)
    for outlet, count in outlets.most_common(20):
        print(f"  {outlet:<30} {count:>5}")
    if len(outlets) > 20:
        print(f"  ... and {len(outlets) - 20} more outlets")
    print()

    # Stories per date
    dates = Counter(story.get("publish_date", "unknown") for story in stories)
    sorted_dates = sorted(dates.items(), key=lambda x: x[0] if x[0] != "unknown" else "0000-00-00")

    valid_dates = [d for d, _ in sorted_dates if d != "unknown"]
    if valid_dates:
        print(f"Date range: {valid_dates[0]} to {valid_dates[-1]}")
    print(f"Number of unique dates: {len(dates)}")
    print("\nStories per date:")
    print("-" * 40)
    for date, count in sorted_dates:
        print(f"  {date:<15} {count:>5}")
    print()

    # Content statistics
    descriptions = [story.get("description", "") or "" for story in stories]
    titles = [story.get("title", "") or "" for story in stories]

    desc_lengths = [len(d) for d in descriptions]
    title_lengths = [len(t) for t in titles]

    print("Content statistics:")
    print("-" * 40)
    if desc_lengths:
        print(f"  Average description length: {sum(desc_lengths) / len(desc_lengths):,.0f} chars")
        print(f"  Min description length:     {min(desc_lengths):,} chars")
        print(f"  Max description length:     {max(desc_lengths):,} chars")
    if title_lengths:
        print(f"  Average title length:       {sum(title_lengths) / len(title_lengths):,.0f} chars")
    print()

    # Missing data check
    missing_desc = sum(1 for d in descriptions if not d)
    missing_title = sum(1 for t in titles if not t)
    missing_url = sum(1 for s in stories if not s.get("url"))
    missing_date = sum(1 for s in stories if not s.get("publish_date"))
    missing_outlet = sum(1 for s in stories if not s.get("media_url"))

    print("Data completeness:")
    print("-" * 40)
    print(f"  Stories missing description: {missing_desc}")
    print(f"  Stories missing title:       {missing_title}")
    print(f"  Stories missing URL:         {missing_url}")
    print(f"  Stories missing date:        {missing_date}")
    print(f"  Stories missing outlet:      {missing_outlet}")
    print()

    # Duplicate check
    urls = [story.get("url", "") for story in stories]
    url_counts = Counter(urls)
    duplicates = {url: count for url, count in url_counts.items() if count > 1 and url}

    if duplicates:
        print(f"Duplicate URLs found: {len(duplicates)}")
        print("  Top duplicates:")
        for url, count in sorted(duplicates.items(), key=lambda x: -x[1])[:5]:
            print(f"    {count}x: {url[:60]}...")
    else:
        print("No duplicate URLs found.")
    print()

    # Sample stories
    print("Sample stories (first 3):")
    print("-" * 40)
    for idx, story in enumerate(stories[:3], 1):
        print(f"\n  [{idx}] {story.get('title', 'No title')[:70]}...")
        print(f"      Source: {story.get('media_url', 'unknown')}")
        print(f"      Date:   {story.get('publish_date', 'unknown')}")

    # Generate visualizations
    print("\n" + "=" * 60)
    print("Generating visualizations...")
    print("-" * 40)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    create_stories_per_date_chart(dates, OUTPUT_DIR, topic)
    create_stories_per_outlet_chart(outlets, OUTPUT_DIR, topic)
    print("\nDone! Charts saved to:", OUTPUT_DIR)


def main():
    parser = argparse.ArgumentParser(description="Visualize and describe news data")
    parser.add_argument("--topic", type=str, help="Topic to analyze (default: all topics)")
    parser.add_argument("--local", action="store_true", help="Use local files instead of gists")
    parser.add_argument("--raw", action="store_true", help="Analyze raw data (before filtering)")
    args = parser.parse_args()

    topics_to_analyze = [args.topic] if args.topic else list(TOPICS.keys())

    for topic in topics_to_analyze:
        if args.local:
            source_label = f"local clean/articles-{topic}.jsonl"
            stories = load_stories_local(topic)
        else:
            file_type = "raw" if args.raw else "clean"
            source_label = f"gist {file_type}.jsonl"
            stories = load_stories_gist(topic, use_raw=args.raw)

        describe_data(stories, topic, source_label)


if __name__ == "__main__":
    main()
