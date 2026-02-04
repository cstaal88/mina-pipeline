#!/usr/bin/env python3
"""
Show stories per file by date (since a given date).

Usage:
    python gists/gist-stats-by-date.py              # default: since 2026-01-15
    python gists/gist-stats-by-date.py --since 2026-01-27
"""

import argparse
import json
import subprocess
from collections import Counter

GIST_ID = '16c75a94d276d2800a44e3c2437f40e4'
FILES = ['raw.jsonl', 'clean-minneapolis-ice.jsonl', 'clean-greenland-trump.jsonl', 'mediacloud_raw.jsonl']


def main():
    parser = argparse.ArgumentParser(description='Show stories per file by date')
    parser.add_argument('--since', default='2026-01-15', help='Start date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    since = args.since
    
    print(f'Stories per file by date (since {since})')
    print('=' * 70)
    
    for filename in FILES:
        result = subprocess.run(
            ['gh', 'gist', 'view', GIST_ID, '-f', filename],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f'\n{filename}: not found')
            continue
        
        dates = []
        for line in result.stdout.strip().split('\n'):
            try:
                r = json.loads(line)
                if r.get('_meta'):
                    continue
                d = r.get('publish_date', '')[:10]
                if d >= since:
                    dates.append(d)
            except:
                pass
        
        counts = Counter(dates)
        print(f'\n{filename} ({len(dates)} total since {since})')
        print('-' * 50)
        
        if not counts:
            print('  (no data)')
            continue
            
        max_count = max(counts.values())
        for d in sorted(counts.keys()):
            bar_len = int((counts[d] / max_count) * 30) if max_count > 0 else 0
            bar = '█' * max(1, bar_len)
            print(f'  {d}: {counts[d]:>5}  {bar}')


if __name__ == '__main__':
    main()
