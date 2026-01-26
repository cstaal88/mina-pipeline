#!/usr/bin/env python3
"""
MINA News Data Pipeline - Main Entry Point

Unified pipeline that supports both ad-hoc and automated runs.
Runs the full pipeline:
1. Fetch URLs from MediaCloud
2. Scrape articles from URLs
3. Clean and filter data

Usage:
    python3 run-pipeline.py                          # uses DEFAULT_TOPIC, full pipeline
    python3 run-pipeline.py --topic minneapolis-ice  # specify topic
    python3 run-pipeline.py --collect-only           # skip cleaning
    python3 run-pipeline.py --clean-only             # skip collection
    python3 run-pipeline.py --auto                   # no prompts (for cron/GitHub Actions)
    python3 run-pipeline.py --wait                   # wait for other mcloud processes
    python3 run-pipeline.py --at 03:00               # run at specific time

IMPORTANT: Automated workflows (GitHub Actions) should ALWAYS pass --topic explicitly.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from config import get_topic_config, list_topics, DEFAULT_TOPIC

print("Note to self: use venv: ve1 ...")

# Paths relative to this script's directory
SCRIPT_DIR = Path(__file__).resolve().parent

FETCH_SCRIPT = SCRIPT_DIR / "collect" / "mcloud-fetch-urls.py"
SCRAPE_SCRIPT = SCRIPT_DIR / "collect" / "scrape-articles.py"
CLEAN_SCRIPT = SCRIPT_DIR / "clean.py"

BREAK_SECONDS = 0  # seconds between fetch and scrape

# Track pipeline start time
_pipeline_start: datetime | None = None

print("Let's goooooo!")


def push_to_gist(gist_id: str, filename: str, local_file: Path) -> int:
    """Push a file to a gist using gh CLI.
    
    Returns exit code (0 = success).
    """
    print(f"   Pushing {filename}...")
    try:
        result = subprocess.run(
            ["gh", "gist", "edit", gist_id, "-f", filename, str(local_file)],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"   ✓ {filename} pushed successfully")
        else:
            print(f"   ✗ Failed: {result.stderr.strip()}")
        return result.returncode
    except FileNotFoundError:
        print("   ✗ Error: 'gh' CLI not found. Install with: brew install gh")
        return 1


def elapsed_str() -> str:
    """Return human-readable elapsed time since pipeline started."""
    if _pipeline_start is None:
        return "0s"
    delta = datetime.now() - _pipeline_start
    total_secs = int(delta.total_seconds())
    hours, remainder = divmod(total_secs, 3600)
    mins, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {mins}m {secs}s"
    elif mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def countdown(total_seconds: int, label: str = "Starting") -> None:
    """Countdown with minute-by-minute updates."""
    remaining = total_seconds
    while remaining > 0:
        mins_left = remaining // 60
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"   [{now_str}] {label} in {mins_left} min...", flush=True)
        sleep_time = min(60, remaining)
        time.sleep(sleep_time)
        remaining -= sleep_time
    print(f"   Countdown complete!", flush=True)


def other_mcloud_running() -> bool:
    """Check if another mediacloud-related Python process is running."""
    try:
        my_pid = str(os.getpid())
        result = subprocess.run(
            ["pgrep", "-f", "python.*(mediacloud|mcloud|fetch-urls)"],
            capture_output=True, text=True
        )
        pids = [p.strip() for p in result.stdout.strip().split("\n") if p.strip()]
        other_pids = [p for p in pids if p != my_pid]
        return len(other_pids) > 0
    except Exception:
        return False


def wait_for_clear() -> None:
    """Wait until no other MediaCloud processes are running."""
    check_interval = 5 * 60

    if not other_mcloud_running():
        print("No other MediaCloud processes detected.")
        return

    print("Waiting for other MediaCloud process(es) to finish...")
    while other_mcloud_running():
        now = datetime.now().strftime("%H:%M:%S")
        print(f"   [{now}] Still running... checking again in {check_interval // 60} min", flush=True)
        time.sleep(check_interval)

    print("Other process(es) finished!")


def wait_until_time(target: str) -> None:
    """Wait until a specific time (HH:MM format)."""
    hour, minute = map(int, target.split(":"))
    now = datetime.now()
    target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if target_time <= now:
        target_time += timedelta(days=1)

    wait_seconds = (target_time - now).total_seconds()
    print(f"Waiting until {target_time.strftime('%Y-%m-%d %H:%M')} ({wait_seconds/3600:.1f} hours)...")
    time.sleep(wait_seconds)
    print("Time reached!")


def run_step(name: str, script: Path, extra_args: list[str] | None = None) -> int:
    """Run a pipeline step, returning the exit code."""
    step_start = datetime.now()
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"   Script: {script}")
    print(f"   Started: {step_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Pipeline elapsed: {elapsed_str()}")
    print("=" * 60, flush=True)

    if not script.exists():
        print(f"ERROR: Script not found: {script}", file=sys.stderr)
        return 1

    cmd = [sys.executable, str(script)]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(cmd, cwd=script.parent, env=os.environ.copy())

    step_elapsed = datetime.now() - step_start
    step_mins = int(step_elapsed.total_seconds() // 60)
    step_secs = int(step_elapsed.total_seconds() % 60)
    print(f"\n   Step '{name}' finished in {step_mins}m {step_secs}s")
    print(f"   Pipeline elapsed: {elapsed_str()}", flush=True)

    return result.returncode


def main(argv: list[str] | None = None) -> int:
    global _pipeline_start

    parser = argparse.ArgumentParser(
        description="MINA News Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 run-pipeline.py                          # Full pipeline with default topic
  python3 run-pipeline.py --topic greenland-trump  # Specific topic
  python3 run-pipeline.py --collect-only           # Just fetch + scrape
  python3 run-pipeline.py --clean-only             # Just clean existing data
  python3 run-pipeline.py --auto                   # No prompts (for automation)
  python3 run-pipeline.py --days 2                 # Trial run: only last 2 days
"""
    )
    parser.add_argument("--topic", type=str, default=None,
                        help=f"Topic to process (default: {DEFAULT_TOPIC})")
    parser.add_argument("--days", type=int, default=None,
                        help="Only collect N most recent days (for trial runs)")
    parser.add_argument("--collect-only", action="store_true",
                        help="Only run data collection, skip cleaning")
    parser.add_argument("--clean-only", action="store_true",
                        help="Only run cleaning, skip collection")
    parser.add_argument("--wait", action="store_true",
                        help="Wait for other MediaCloud processes to finish first")
    parser.add_argument("--at", metavar="HH:MM",
                        help="Wait until specific time before starting")
    parser.add_argument("--list-topics", action="store_true",
                        help="List available topics and exit")
    parser.add_argument("--push-gist", action="store_true",
                        help="Push results to gist after completion (requires gh CLI)")
    args = parser.parse_args(argv)

    if args.list_topics:
        list_topics()
        return 0

    # Validate topic
    try:
        topic_config = get_topic_config(args.topic)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        list_topics()
        return 1

    topic = topic_config["name"]

    _pipeline_start = datetime.now()

    print("\n" + "=" * 60)
    print("MINA NEWS DATA PIPELINE")
    print(f"Topic: {topic}")
    print(f"Started: {_pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Handle waiting conditions
    if args.wait and args.at:
        # Wait for whichever comes first
        print(f"\nWill start when: no other mcloud processes OR time reaches {args.at}")
        target_hour, target_minute = map(int, args.at.split(":"))
        while True:
            if not other_mcloud_running():
                print("No other MediaCloud processes. Starting now!")
                break
            now = datetime.now()
            if now.hour == target_hour and now.minute >= target_minute:
                print(f"Time reached ({args.at}). Starting now!")
                break
            time.sleep(60)
    elif args.wait:
        wait_for_clear()
    elif args.at:
        wait_until_time(args.at)

    # Build args for fetch script (--days only applies to fetch)
    fetch_args = ["--topic", topic]
    if args.days:
        fetch_args.extend(["--days", str(args.days)])
    
    # Build args for other scripts (just topic)
    topic_args = ["--topic", topic]

    # Step 1: Fetch URLs
    if not args.clean_only:
        rc = run_step("Fetch URLs from MediaCloud", FETCH_SCRIPT, fetch_args)
        if rc != 0:
            print(f"\nFetch failed with code {rc}", file=sys.stderr)
            return rc

        # Brief pause between fetch and scrape
        if BREAK_SECONDS > 0:
            print(f"\nPausing {BREAK_SECONDS}s before scraping...")
            time.sleep(BREAK_SECONDS)

        # Step 2: Scrape articles
        rc = run_step("Scrape article metadata", SCRAPE_SCRIPT, topic_args)
        if rc != 0:
            print(f"\nScraping failed with code {rc}", file=sys.stderr)
            return rc

    # Step 3: Clean data
    if not args.collect_only:
        clean_args = topic_args.copy()

        rc = run_step("Clean and filter data", CLEAN_SCRIPT, clean_args)
        if rc != 0:
            print(f"\nCleaning failed with code {rc}", file=sys.stderr)
            return rc

    # Step 4: Push to gist (optional)
    if args.push_gist:
        gist_id = topic_config.get("gist_id")
        if gist_id:
            print(f"\n{'='*60}")
            print("PUSHING TO GIST")
            print(f"Gist ID: {gist_id}")
            print("=" * 60)
            
            # Push raw.jsonl
            raw_file = SCRIPT_DIR / "raw" / topic / "_combined.jsonl"
            if raw_file.exists():
                rc = push_to_gist(gist_id, "raw.jsonl", raw_file)
                if rc != 0:
                    print(f"Warning: Failed to push raw.jsonl (exit code {rc})")
            
            # Push clean.jsonl
            clean_file = SCRIPT_DIR / "clean" / f"articles-{topic}.jsonl"
            if clean_file.exists():
                rc = push_to_gist(gist_id, "clean.jsonl", clean_file)
                if rc != 0:
                    print(f"Warning: Failed to push clean.jsonl (exit code {rc})")
        else:
            print(f"\nWarning: No gist_id configured for topic '{topic}', skipping push")

    end_time = datetime.now()
    duration = end_time - _pipeline_start

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print(f"Topic: {topic}")
    print(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    if args.push_gist:
        gist_id = topic_config.get("gist_id")
        if gist_id:
            print(f"Gist: https://gist.github.com/{gist_id}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
