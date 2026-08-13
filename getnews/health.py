#!/usr/bin/env python3
"""
One-command health check for the MINA pipeline.

    python3 getnews/health.py           # fast (~10s), no large downloads
    python3 getnews/health.py --deep    # also re-checksums the 42 MB frozen archive

Exits 0 if healthy (warnings allowed), 1 if anything is broken, so it can be run
by hand or wired into a workflow.

Each check answers a question you would otherwise have to ask someone else:
is the archive intact, is new data still arriving, did the last run work, is the
study corpus fresh. Read the VERDICT line; read the rest only if it complains.
"""

import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (  # noqa: E402
    GIST_ID,
    ACTIVE_TOPICS,
    LEGACY_ARCHIVE,
    SHARD_PATTERN,
    SHARD_SIZE_WARN,
    SHARD_SIZE_ABORT,
    FROZEN_ARCHIVE_RECORDS,
    FROZEN_ARCHIVE_BYTES,
    FROZEN_ARCHIVE_SHA256,
)

WORKFLOW = "rss-pipeline.yml"

# Runs are triggered every 30 minutes. Allow a couple of missed slots before
# calling it stale -- a single hiccup is noise, an hour of silence is not.
FRESH_OK_MINUTES = 90
FRESH_WARN_MINUTES = 180

OK, WARN, FAIL = "OK", "WARN", "FAIL"


class Check:
    def __init__(self, name, status, detail, note=""):
        self.name = name
        self.status = status
        self.detail = detail
        self.note = note


def gh(args: list[str], timeout: int = 120) -> str:
    """
    Run a gh command and return stdout, or raise RuntimeError.

    GITHUB_TOKEN is stripped: gh gives env tokens absolute priority over the
    keyring, and this machine exports a long-expired PAT under that name which
    shadows the working credential. GH_TOKEN (what GitHub Actions sets) is left
    alone, so this is correct both locally and on a runner.
    """
    env = dict(os.environ)
    env.pop("GITHUB_TOKEN", None)
    result = subprocess.run(
        ["gh", *args], capture_output=True, text=True, timeout=timeout, env=env
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "gh failed")
    return result.stdout


def gist_files() -> dict[str, int]:
    """Filename -> size in bytes, straight from the API (no file downloads)."""
    out = gh(["api", f"gists/{GIST_ID}", "--jq",
              '.files | to_entries[] | "\\(.key)\\t\\(.value.size)"'])
    files = {}
    for line in out.splitlines():
        if "\t" in line:
            name, size = line.rsplit("\t", 1)
            files[name] = int(size)
    return files


def gist_text(filename: str) -> str:
    return gh(["gist", "view", GIST_ID, "-f", filename])


def meta_line(text: str) -> dict:
    """First line of a JSONL file, if it is a _meta header."""
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return {}
        return obj if obj.get("_meta") else {}
    return {}


def records(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and not obj.get("_meta"):
            out.append(obj)
    return out


def age_minutes(iso: str) -> float | None:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 60


def human_age(minutes: float) -> str:
    if minutes < 90:
        return f"{minutes:.0f} minutes ago"
    if minutes < 48 * 60:
        return f"{minutes / 60:.1f} hours ago"
    return f"{minutes / 60 / 24:.1f} days ago"


# ── checks ───────────────────────────────────────────────────────────────────

def check_frozen(files: dict[str, int], deep: bool) -> Check:
    """The frozen archive must be byte-identical forever. Any change is a fault."""
    size = files.get(LEGACY_ARCHIVE)
    if size is None:
        return Check("Frozen archive", FAIL, f"{LEGACY_ARCHIVE} is MISSING from the gist",
                     "This file holds 46,461 historical records. Restore it from "
                     "~/mina-backups-0813/ before doing anything else.")
    if size != FROZEN_ARCHIVE_BYTES:
        return Check("Frozen archive", FAIL,
                     f"size changed: {size:,} bytes, expected {FROZEN_ARCHIVE_BYTES:,}",
                     "This file must never be written. Something wrote to it.")
    if not deep:
        return Check("Frozen archive", OK,
                     f"{FROZEN_ARCHIVE_RECORDS:,} records, {size:,} bytes (size matches)")

    text = gist_text(LEGACY_ARCHIVE)
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    n = len(records(text))
    if digest != FROZEN_ARCHIVE_SHA256:
        return Check("Frozen archive", FAIL, f"checksum MISMATCH ({digest[:16]}…)",
                     "Byte-level change detected. Compare against ~/mina-backups-0813/.")
    if n != FROZEN_ARCHIVE_RECORDS:
        return Check("Frozen archive", FAIL,
                     f"{n:,} records, expected {FROZEN_ARCHIVE_RECORDS:,}")
    return Check("Frozen archive", OK,
                 f"{n:,} records, checksum verified byte-for-byte")


def check_shard(files: dict[str, int]) -> tuple[Check, str | None]:
    """The month currently being written: exists, growing, well under the ceiling."""
    shards = sorted(f for f in files if re.match(SHARD_PATTERN, f))
    if not shards:
        return Check("Current shard", FAIL, "no raw-YYYY-MM.jsonl files in the gist",
                     "Nothing is being archived. Check the last workflow run."), None

    newest = shards[-1]
    size = files[newest]
    mb = size / 1024 / 1024
    detail = f"{newest} — {mb:.1f} MB of 40 MB ceiling"

    if size >= SHARD_SIZE_ABORT:
        return Check("Current shard", FAIL, detail,
                     "Over the abort threshold; uploads are being refused. "
                     "Split this month into a -part2 file."), newest
    if size >= SHARD_SIZE_WARN:
        return Check("Current shard", WARN, detail,
                     "Approaching the ceiling. Plan to split this month."), newest
    return Check("Current shard", OK, detail), newest


def check_freshness(shard_meta: dict) -> Check:
    """Is new data actually landing, or has collection quietly stopped?"""
    updated = shard_meta.get("last_updated")
    if not updated:
        return Check("Last write", WARN, "no timestamp in the shard header")
    mins = age_minutes(updated)
    if mins is None:
        return Check("Last write", WARN, f"unreadable timestamp: {updated}")
    detail = human_age(mins)
    if mins <= FRESH_OK_MINUTES:
        return Check("Last write", OK, detail)
    if mins <= FRESH_WARN_MINUTES:
        return Check("Last write", WARN, detail, "Expected a run every 30 minutes.")
    return Check("Last write", FAIL, detail,
                 "Collection has stopped. Check the workflow runs below.")


def check_continuity(shard_records: list[dict]) -> Check:
    """Days this month with zero records collected -- silent gaps in the corpus."""
    days = {(r.get("collected_at") or "")[:10] for r in shard_records}
    days.discard("")
    if not days:
        return Check("Collection gaps", WARN, "no collection dates in the shard")

    start = datetime.strptime(min(days), "%Y-%m-%d").date()
    today = datetime.now(timezone.utc).date()
    missing = []
    d = start
    while d < today:
        if d.isoformat() not in days:
            missing.append(d.isoformat())
        d += timedelta(days=1)

    span = f"{min(days)} → {max(days)}"
    if not missing:
        return Check("Collection gaps", OK, f"none since {min(days)} ({len(days)} days)")
    return Check("Collection gaps", WARN, f"{len(missing)} missing day(s) in {span}",
                 "Missing: " + ", ".join(missing[:8]) +
                 ("…" if len(missing) > 8 else ""))


def check_runs() -> Check:
    """Did the pipeline actually run, and did it work?"""
    try:
        out = gh(["run", "list", f"--workflow={WORKFLOW}", "--limit", "20",
                  "--json", "conclusion,status,createdAt",
                  "--jq", '.[] | "\\(.createdAt)\\t\\(.status)\\t\\(.conclusion // "-")"'])
    except RuntimeError as e:
        return Check("Workflow runs", WARN, f"could not read run history ({e})")

    rows = [l.split("\t") for l in out.splitlines() if l.strip()]
    if not rows:
        return Check("Workflow runs", WARN, "no runs found")

    finished = [r for r in rows if r[1] == "completed"]
    if not finished:
        return Check("Workflow runs", OK, "a run is in progress")

    latest = finished[0]
    consecutive_failures = 0
    for r in finished:
        if r[2] == "success":
            break
        consecutive_failures += 1

    recent = finished[:20]
    failures = sum(1 for r in recent if r[2] != "success")

    if latest[2] == "success":
        detail = f"last run OK ({failures} of last {len(recent)} failed)"
        status = OK if failures <= 2 else WARN
        note = ("Recovering from a recent outage — the last run worked."
                if failures > 2 else "")
        return Check("Workflow runs", status, detail, note)

    return Check("Workflow runs", FAIL,
                 f"{consecutive_failures} consecutive failures, latest {latest[0]}",
                 f"Inspect with:  gh run list --workflow={WORKFLOW}")


def check_corpus(files: dict[str, int]) -> Check:
    """The three study tiers: present, populated, and regenerated recently."""
    topics = ACTIVE_TOPICS or []
    parts, worst, notes = [], OK, []
    for topic in topics:
        name = f"clean-{topic}.jsonl"
        if name not in files:
            worst = FAIL
            notes.append(f"{name} is missing from the gist")
            continue
        meta = meta_line(gist_text(name))
        count = meta.get("record_count", "?")
        parts.append(f"{topic.replace('midterms-', '')} {count:,}"
                     if isinstance(count, int) else f"{topic} ?")
        mins = age_minutes(meta.get("last_updated", ""))
        if mins is None:
            worst = WARN if worst == OK else worst
            notes.append(f"{name} has no readable timestamp")
        elif mins > FRESH_WARN_MINUTES:
            worst = WARN if worst == OK else worst
            notes.append(f"{name} last rebuilt {human_age(mins)}")

    return Check("Study corpus", worst, " · ".join(parts) or "no active topics",
                 "; ".join(notes))


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    deep = "--deep" in sys.argv
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\nMINA PIPELINE HEALTH — {now}")
    print("=" * 72)

    try:
        files = gist_files()
    except Exception as e:
        print(f"\n  Could not reach the gist: {e}\n")
        print("  VERDICT: UNKNOWN — could not check. Fix access, then re-run.\n")
        return 1

    checks = [check_frozen(files, deep)]

    shard_check, shard_name = check_shard(files)
    checks.append(shard_check)

    if shard_name:
        text = gist_text(shard_name)
        checks.append(check_freshness(meta_line(text)))
        checks.append(check_continuity(records(text)))

    checks.append(check_runs())
    checks.append(check_corpus(files))

    width = max(len(c.name) for c in checks)
    for c in checks:
        print(f"  {c.status:<4}  {c.name:<{width}}   {c.detail}")
        if c.note:
            print(f"        {'':<{width}}   → {c.note}")

    print("=" * 72)

    failed = [c for c in checks if c.status == FAIL]
    warned = [c for c in checks if c.status == WARN]

    if failed:
        print(f"  VERDICT: PROBLEM — {len(failed)} check(s) failed: "
              f"{', '.join(c.name.lower() for c in failed)}")
        print()
        return 1
    if warned:
        print(f"  VERDICT: HEALTHY, with {len(warned)} warning(s) — "
              "nothing is broken, but read the WARN lines above.")
        print()
        return 0

    print("  VERDICT: HEALTHY — archive intact, data arriving, corpus fresh.")
    if not deep:
        print("  (run with --deep to re-verify the frozen archive byte-for-byte)")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
