#!/usr/bin/env python3
"""Find duplicate URLs present in both the Gist raw.jsonl and MediaCloud _combined.jsonl.

Output: JSON array written to /tmp/duplicate-urls.json with entries like:
[
  {
    "url": "https://...",
    "mediacloud": [ {...}, ... ],
    "rss": [ {...}, ... ]
  },
  ...
]

The script reads `/tmp/gist-raw.jsonl` (current gist dump) and
`mediacloud/raw/minneapolis-ice/_combined.jsonl` (MediaCloud archive).
"""

import json
from collections import defaultdict
from pathlib import Path

GIST_PATH = Path('/tmp/gist-raw.jsonl')
MC_PATH = Path('mediacloud/raw/minneapolis-ice/_combined.jsonl')
OUT_PATH = Path('/tmp/duplicate-urls.json')


def is_mediacloud_record(data):
    # If the record has a collected_with field it's RSS
    if data.get('collected_with'):
        return False
    if data.get('indexed_date') is not None:
        return True
    return False


def load_jsonl(path):
    records = []
    if not path.exists():
        return records
    with path.open('r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except Exception:
                continue
            # skip gist/mediacloud metadata objects
            if data.get('_meta'):
                continue
            records.append(data)
    return records


def main():
    gist = load_jsonl(GIST_PATH)
    mc = load_jsonl(MC_PATH)

    gist_by_url = defaultdict(list)
    mc_by_url = defaultdict(list)

    for r in gist:
        url = r.get('url') or r.get('final_url')
        if not url:
            continue
        # classify
        if is_mediacloud_record(r):
            mc_by_url[url].append(r)
        else:
            gist_by_url[url].append(r)

    for r in mc:
        url = r.get('url') or r.get('final_url')
        if not url:
            continue
        mc_by_url[url].append(r)

    # Find URLs present in BOTH mc_by_url and gist_by_url
    duplicates = []
    for url in sorted(set(mc_by_url.keys()) & set(gist_by_url.keys())):
        entry = {
            'url': url,
            'mediacloud': mc_by_url[url],
            'rss': gist_by_url[url]
        }
        duplicates.append(entry)

    with OUT_PATH.open('w') as out:
        json.dump(duplicates, out, indent=2)

    print(f'Gist records: {len(gist)}')
    print(f'MediaCloud records: {len(mc)}')
    print(f'Duplicate URLs (both sources): {len(duplicates)}')
    print(f'Wrote duplicates to: {OUT_PATH}')


if __name__ == '__main__':
    main()
