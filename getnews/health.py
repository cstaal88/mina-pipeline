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

import collections
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import time
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
MCLOUD_WORKFLOW = "mcloud-pipeline.yml"

# GitHub throws transient errors several times a day -- 429 on the raw-content
# reads, 502/503 on the API, and plain TCP resets. On 2026-08-19 one reset
# mid-check crashed this script and opened a "pipeline health check failing"
# issue while the pipeline was fine: 28 of 28 RSS runs were green either side of
# it. A watchdog more fragile than the thing it watches trains you to ignore it,
# so every read is retried.
#
# Deliberately NOT shared with clean.py's identical gist_run. This script has to
# work when the pipeline module is broken -- which is precisely when it matters
# -- so it must not import from it.
GH_ATTEMPTS = 4
GH_RETRY_DELAY = 5  # seconds; doubles per attempt (5s, 10s, 20s) plus jitter

# mediacloud_raw.jsonl is still a single monolithic file written by
# mcloud-pipeline.yml, which has a shrink guard but NO size guard. It is the
# same unexploded bug that took raw.jsonl down on 2026-08-11: past the write
# ceiling the Gist API starts refusing it with an opaque HTTP 422 and collection
# stops silently. Watch it here until that pipeline is sharded too.
MEDIACLOUD_ARCHIVE = "mediacloud_raw.jsonl"
BYTES_PER_RECORD = 928  # measured: 22,381,313 bytes / 24,116 records

# Under-collection detection. On 2026-08-01 and 2026-08-06 MediaCloud collected 3
# records against a ~140/day median -- the fetch ran while those days were still
# unindexed, the window moved on, and nobody noticed for two weeks. A day with 3
# records is not an empty day, so the gaps check sails straight past it.
#
# Only MediaCloud is checked, deliberately:
#   - it is where the failure mode lives (indexing lag vs a fixed fetch window)
#   - a missed MC day is FIXABLE -- MediaCloud is a historical index, so it can be
#     re-fetched. Alerting on something actionable is the whole point.
#   - RSS under-collection is already implied by the freshness check, and cannot
#     be repaired anyway, so an alarm would only nag.
COVERAGE_LOOKBACK_DAYS = 21
COVERAGE_FLOOR = 0.25   # fraction of the trailing median that counts as collected
# A day can still be topped up while it sits inside the pipeline's --days 4
# window, so only days older than this are final enough to judge.
MC_SETTLE_DAYS = 5
MC_MIN_SAMPLE = 7       # need this many settled days before a median means anything

# Runs are triggered every 30 minutes. Allow a couple of missed slots before
# calling it stale -- a single hiccup is noise, an hour of silence is not.
FRESH_OK_MINUTES = 90
FRESH_WARN_MINUTES = 180

# This repo sees a few transient run failures a day (GitHub 502s, feed timeouts)
# that clear on their own. Alerting on those trains you to ignore alerts, so a
# single failure is only a WARN -- it takes a sustained run of them to FAIL.
CONSECUTIVE_FAILURES_FAIL = 3

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

    Transient failures are retried -- see GH_ATTEMPTS. Every call this script
    makes is a read, so a retry can never damage anything.
    """
    env = dict(os.environ)
    env.pop("GITHUB_TOKEN", None)

    for attempt in range(1, GH_ATTEMPTS + 1):
        try:
            result = subprocess.run(
                ["gh", *args], capture_output=True, text=True, timeout=timeout,
                env=env,
            )
        except subprocess.TimeoutExpired:
            # Already `timeout` seconds spent. A hung read is not the momentary
            # blip this guards against, and retrying it would push a routine
            # check into minutes of dead waiting.
            raise RuntimeError(f"timed out after {timeout}s: gh {' '.join(args)}")

        if result.returncode == 0:
            return result.stdout

        err = result.stderr.strip() or "gh failed"
        if attempt == GH_ATTEMPTS:
            raise RuntimeError(err)

        # Jitter so the several reads in one check do not resynchronise onto
        # whatever rate limit knocked them back.
        delay = GH_RETRY_DELAY * 2 ** (attempt - 1)
        delay += random.uniform(0, delay / 2)
        print(
            f"  (transient: {err.splitlines()[0][:120]} — "
            f"retrying in {delay:.0f}s, attempt {attempt}/{GH_ATTEMPTS})",
            file=sys.stderr,
        )
        time.sleep(delay)


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


def check_mediacloud(files: dict[str, int]) -> Check:
    """
    Headroom on the one archive still stored as a single file.

    Reported in records rather than megabytes because the thing most likely to
    blow it is the pending MediaCloud midterms backfill, which adds records in
    bulk -- "21,000 records left" is actionable before a backfill in a way that
    "19 MB left" is not.
    """
    size = files.get(MEDIACLOUD_ARCHIVE)
    if size is None:
        return Check("MediaCloud archive", WARN,
                     f"{MEDIACLOUD_ARCHIVE} not found in the gist")

    mb = size / 1024 / 1024
    headroom = (SHARD_SIZE_ABORT - size) // BYTES_PER_RECORD
    detail = f"{mb:.1f} MB — room for roughly {headroom:,} more records"

    if size >= SHARD_SIZE_ABORT:
        return Check("MediaCloud archive", FAIL, f"{mb:.1f} MB — past the write ceiling",
                     "mcloud-pipeline.yml has no size guard: its uploads are "
                     "failing silently with HTTP 422. Shard it, as raw.jsonl was.")
    if size >= SHARD_SIZE_WARN:
        return Check("MediaCloud archive", WARN, detail,
                     "Approaching the ceiling that broke raw.jsonl. Shard this "
                     "pipeline before running any further backfill.")
    return Check("MediaCloud archive", OK, detail)


def check_mc_volume(mc_records: list[dict], topic: str = "midterms") -> Check:
    """
    Days MediaCloud collected far below its own norm -- silent under-collection.

    Compares each settled day against the trailing median rather than a fixed
    number, so weekends (which legitimately run at ~45% of a weekday) stay quiet
    while a day at 2% does not. Days still inside the top-up window are skipped:
    they may yet fill in, and flagging them would cry wolf every morning.
    """
    counts = collections.Counter()
    for r in mc_records:
        if topic and r.get("my_topic") != topic:
            continue
        day = (r.get("publish_date") or "")[:10]
        if day:
            counts[day] += 1

    today = datetime.now(timezone.utc).date()
    window = [
        (today - timedelta(days=n)).isoformat()
        for n in range(MC_SETTLE_DAYS, COVERAGE_LOOKBACK_DAYS + 1)
    ]
    settled = [(d, counts.get(d, 0)) for d in window]
    if len(settled) < MC_MIN_SAMPLE:
        return Check("MediaCloud volume", OK, "not enough settled days to judge yet")

    values = sorted(n for _, n in settled)
    median = values[len(values) // 2]
    if median == 0:
        return Check("MediaCloud volume", FAIL,
                     "no MediaCloud records at all in the trailing window",
                     "Collection has stopped entirely. Check the MediaCloud runs.")

    floor = max(1, int(median * COVERAGE_FLOOR))
    low = [(d, n) for d, n in settled if n < floor]

    detail = (f"median {median}/day over {len(settled)} settled days "
              f"(flag below {floor})")
    if not low:
        return Check("MediaCloud volume", OK, detail)

    worst = ", ".join(f"{d} ({n})" for d, n in sorted(low)[:5])
    return Check("MediaCloud volume", FAIL,
                 f"{len(low)} under-collected day(s) — {detail}",
                 f"Days: {worst}. These are past the top-up window but MediaCloud "
                 f"is a historical index, so re-fetch them:  gh workflow run "
                 f"mcloud-pipeline.yml -f since=DATE -f until=DATE")


def check_runs(workflow: str = WORKFLOW, label: str = "Workflow runs",
               fail_after: int = CONSECUTIVE_FAILURES_FAIL) -> Check:
    """Did the pipeline actually run, and did it work?"""
    try:
        out = gh(["run", "list", f"--workflow={workflow}", "--limit", "20",
                  "--json", "conclusion,status,createdAt",
                  "--jq", '.[] | "\\(.createdAt)\\t\\(.status)\\t\\(.conclusion // "-")"'])
    except RuntimeError as e:
        return Check(label, WARN, f"could not read run history ({e})")

    rows = [l.split("\t") for l in out.splitlines() if l.strip()]
    if not rows:
        return Check(label, WARN, "no runs found")

    finished = [r for r in rows if r[1] == "completed"]
    if not finished:
        return Check(label, OK, "a run is in progress")

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
        return Check(label, status, detail, note)

    # Failing now. One or two in a row is the usual transient noise; a sustained
    # run of them is the real thing. Only the latter is worth waking anyone for.
    detail = f"{consecutive_failures} consecutive failure(s), latest {latest[0]}"
    hint = f"Inspect with:  gh run list --workflow={workflow}"
    if consecutive_failures < fail_after:
        return Check(label, WARN, detail,
                     "Transient so far — data is still arriving (see Last write). " + hint)
    return Check(label, FAIL, detail, hint)


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
        try:
            meta = meta_line(gist_text(name))
        except RuntimeError as e:
            # Unreadable is not the same as broken. This check reports corpus
            # counts; failing to fetch one says nothing about whether collection
            # is still running -- "Last write" and "Workflow runs" answer that,
            # and the latter reads the Actions API, so it survives a gist
            # outage. WARN (exit 0, no issue) so a transient read cannot page
            # anyone, matching what check_runs and check_mc_volume already do.
            # A genuinely stalled pipeline is still caught, by those checks.
            worst = WARN if worst == OK else worst
            notes.append(f"{name} could not be read ({str(e).splitlines()[0][:80]})")
            parts.append(f"{topic.replace('midterms-', '')} ?")
            continue
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
    checks.append(check_mediacloud(files))
    # Daily cadence, so two failures in a row already means two days of no
    # backfill -- a lower bar than the 30-minute RSS pipeline warrants.
    checks.append(check_runs(MCLOUD_WORKFLOW, "MediaCloud runs", fail_after=2))

    if MEDIACLOUD_ARCHIVE in files:
        # Costs a ~22 MB download. Worth it: this is the only check that sees
        # a day collected at 2% of normal, which is invisible to every other one.
        try:
            checks.append(check_mc_volume(records(gist_text(MEDIACLOUD_ARCHIVE))))
        except Exception as e:
            checks.append(Check("MediaCloud volume", WARN, f"could not check ({e})"))

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
