#!/usr/bin/env python3
"""Analyze which dates in the gist raw.jsonl have mediacloud data."""

import json
from collections import defaultdict


def is_mediacloud_record(data):
    """
    Determine if a record is from MediaCloud vs RSS.
    
    MediaCloud data: indexed_date has an actual value (not null), no collected_with field
    RSS data: indexed_date is null, has collected_with: "rss"
    """
    # If collected_with exists, it's RSS data
    if data.get('collected_with'):
        return False
    # If indexed_date is not null, it's mediacloud
    if data.get('indexed_date') is not None:
        return True
    return False


def analyze_gist_file(filepath):
    """Analyze the gist raw.jsonl file for mediacloud data."""
    dates_with_mediacloud = defaultdict(int)
    dates_with_rss = defaultdict(int)
    total_mediacloud = 0
    total_rss = 0
    
    with open(filepath, 'r') as f:
        for line in f:
            data = json.loads(line)
            
            # Skip metadata lines
            if data.get('_meta'):
                continue
            
            # Get the date (check different possible date fields)
            date = data.get('publish_date') or data.get('published_at') or data.get('date') or 'unknown'
            if isinstance(date, str) and len(date) >= 10:
                date = date[:10]  # Just the YYYY-MM-DD part
            
            if is_mediacloud_record(data):
                dates_with_mediacloud[date] += 1
                total_mediacloud += 1
            else:
                dates_with_rss[date] += 1
                total_rss += 1
    
    return dates_with_mediacloud, dates_with_rss, total_mediacloud, total_rss

def main():
    filepath = '/tmp/gist-raw.jsonl'
    
    dates_with_mc, dates_with_rss, total_mc, total_rss = analyze_gist_file(filepath)
    
    print(f'Total MediaCloud records: {total_mc}')
    print(f'Total RSS records: {total_rss}')
    print()
    
    # Get all unique dates
    all_dates = sorted(set(list(dates_with_mc.keys()) + list(dates_with_rss.keys())))
    
    print('=== ALL DATES (sorted) ===')
    print(f'{"Date":<15} {"MediaCloud":<15} {"RSS":<15}')
    print('-' * 45)
    for date in all_dates:
        mc_count = dates_with_mc.get(date, 0)
        rss_count = dates_with_rss.get(date, 0)
        marker = '' if mc_count > 0 else ' <-- NO MEDIACLOUD'
        print(f'{date:<15} {mc_count:<15} {rss_count:<15}{marker}')
    
    print()
    print('=== DATES WITHOUT MEDIACLOUD DATA ===')
    dates_only_rss = [d for d in all_dates if d not in dates_with_mc]
    if dates_only_rss:
        for date in dates_only_rss:
            print(f'{date}: {dates_with_rss[date]} RSS records (NO MEDIACLOUD)')
    else:
        print('All dates have MediaCloud data!')

if __name__ == '__main__':
    main()
