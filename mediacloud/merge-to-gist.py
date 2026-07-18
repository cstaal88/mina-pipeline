#!/usr/bin/env python3
"""
Merge locally-collected MediaCloud raw for a topic into the gist's
mediacloud_raw.jsonl. Everything downstream is automatic: the next RSS run's
getnews/clean.py pulls mediacloud_raw.jsonl into raw.jsonl and regenerates the
clean tiers, so this script's only job is to update that one gist file.

The actual urls.jsonl + articles.jsonl field-merge (dedup by URL, accumulate)
is done by the existing mediacloud/clean.py::combine_raw_files(). Gist I/O here
mirrors the never-shrink / absent-vs-failed guard from the mcloud workflow: a
failed download must never be mistaken for "no data" and rebuild from empty.

Dry run by default (prints the delta, touches nothing). Pass --push to upload.

    python3 mediacloud/merge-to-gist.py            # dry run
    python3 mediacloud/merge-to-gist.py --push     # upload
"""
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from clean import combine_raw_files, get_combined_raw_file  # mediacloud/clean.py

GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
FILE = "mediacloud_raw.jsonl"
TOPIC = "midterms"
TIMEOUT = 120


def count_records(text: str) -> int:
    """Non-metadata JSONL records in a blob."""
    n = 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            if not json.loads(line).get("_meta"):
                n += 1
        except json.JSONDecodeError:
            pass
    return n


def gist_has_file() -> bool:
    r = subprocess.run(["gh", "gist", "view", GIST_ID, "--files"],
                       capture_output=True, text=True, timeout=TIMEOUT)
    if r.returncode:
        sys.exit(f"FATAL: cannot list gist files: {r.stderr.strip()}")
    return FILE in {l.strip() for l in r.stdout.splitlines() if l.strip()}


def main() -> int:
    # 1. Download the current archive. A non-zero exit means "no such file"
    #    ONLY if the gist truly lacks it; otherwise the call failed and we must
    #    NOT proceed (proceeding would rebuild mediacloud_raw.jsonl from empty).
    r = subprocess.run(["gh", "gist", "view", GIST_ID, "-f", FILE],
                       capture_output=True, text=True, timeout=TIMEOUT)
    if r.returncode == 0:
        existing = r.stdout
    elif gist_has_file():
        sys.exit(f"FATAL: {FILE} is in the gist but download failed: {r.stderr.strip()}")
    else:
        existing = ""  # genuinely absent -> legitimate fresh start

    before = count_records(existing)

    # 2. Seed the topic's _combined.jsonl with the gist archive, then let the
    #    existing combine step merge the locally-collected days into it.
    combined = get_combined_raw_file(TOPIC)
    combined.parent.mkdir(parents=True, exist_ok=True)
    combined.write_text(existing, encoding="utf-8")
    combine_raw_files(TOPIC)

    after = count_records(combined.read_text(encoding="utf-8"))
    print(f"{FILE}: {before} existing -> {after} after merge  (+{after - before} new)")

    # 3. Never shrink.
    if after < before:
        sys.exit(f"ABORT: merge would shrink {before} -> {after}; nothing uploaded")

    # 4. Upload, gated behind --push.
    if "--push" in sys.argv:
        up = subprocess.run(["gh", "gist", "edit", GIST_ID, "-f", FILE, str(combined)],
                            capture_output=True, text=True, timeout=TIMEOUT)
        if up.returncode:
            sys.exit(f"UPLOAD FAILED: {up.stderr.strip()}")
        print(f"[OK] uploaded to https://gist.github.com/{GIST_ID}")
    else:
        print("dry run (no --push): gist unchanged")
    return 0


if __name__ == "__main__":
    sys.exit(main())
