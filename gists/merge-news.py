#!/usr/bin/env python3
"""
Merge every raw news file in the gist backups into one deduplicated JSONL.

    python3 merge-news.py [backup-dir] [output-file]

Defaults: ~/gist-backups  ->  ~/gist-backups/all-news-deduped.jsonl

What it reads: raw.jsonl, raw-YYYY-MM.jsonl and mediacloud_raw.jsonl, in every
gist folder. That is the frozen archive, the monthly shards, the MediaCloud
staging file, and the two old-topic gists (greenland-trump, minneapolis-ice) --
all the same record schema.

What it skips, on purpose:
  clean-*.jsonl      keyword-filtered subsets of the raw archive, zero new records
  newsdata.jsonl     the chatbot knowledge base; verified 100% contained in raw
  *.txt              collection logs, not news

Dedup key is the normalized URL -- scheme + host + path, no query or fragment --
the same rule getnews/clean.py uses. The record `id` field is a sha256 of the
*raw* URL, so a ?utm_source= tail would sneak the same story through as unique.

Files are read oldest first and the first record for a URL wins, so the earliest
collection of a story is the one kept.
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

backup_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "~/gist-backups").expanduser()
out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else backup_dir / "all-news-deduped.jsonl"


def normalize_url(url: str) -> str:
    """Same normalization as getnews/clean.py: drop query, fragment, trailing slash."""
    p = urlparse(url or "")
    return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))


def rank(path: Path) -> tuple:
    """Frozen archive, then monthly shards in date order, then MediaCloud."""
    if path.name == "raw.jsonl":
        return (0, path.name)
    if path.name.startswith("raw-"):
        return (1, path.name)
    return (2, path.name)


files = sorted(
    (p for p in backup_dir.glob("*/*.jsonl")
     if p.name in ("raw.jsonl", "mediacloud_raw.jsonl") or re.match(r"^raw-\d{4}-\d{2}\.jsonl$", p.name)),
    key=lambda p: (rank(p), p.parent.name),
)

if not files:
    sys.exit(f"No raw news files found under {backup_dir}")

seen = set()
kept = dropped = no_url = 0

with out_path.open("w", encoding="utf-8") as out:
    for path in files:
        file_kept = 0
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record.get("_meta"):  # every file starts with a metadata line
                    continue
                key = normalize_url(record.get("url", ""))
                if not key:
                    no_url += 1
                    continue
                if key in seen:
                    dropped += 1
                    continue
                seen.add(key)
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                kept += 1
                file_kept += 1
        print(f"  {path.parent.name}/{path.name}: +{file_kept:,} new")

print(f"\n{kept:,} unique records -> {out_path}")
print(f"{dropped:,} duplicates dropped" + (f", {no_url:,} records had no URL" if no_url else ""))
