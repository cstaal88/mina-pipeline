#!/usr/bin/env python3
"""Run the full MediaCloud → descriptions pipeline.

1. (Optional) Wait for other MediaCloud processes to finish
2. Fetch URLs from MediaCloud (collect/fetch.py)
3. Wait 5 minutes
4. Scrape descriptions for each URL (collect/describe.py)

Usage:
    python3 pipeline-dat-collect.py                 # run immediately
    python3 pipeline-dat-collect.py --wait          # wait for other mcloud processes first
    python3 pipeline-dat-collect.py --at 03:00      # run at 3 AM
    python3 pipeline-dat-collect.py --wait --at 03:00  # whichever comes first
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

print("Note to self: use venv: ve1 ...")

# Paths relative to this script's directory
SCRIPT_DIR = Path(__file__).resolve().parent

FETCH_SCRIPT = SCRIPT_DIR / "collect" / "fetch.py"
DESCRIBE_SCRIPT = SCRIPT_DIR / "collect" / "describe.py"

BREAK_SECONDS = 0 # 5 * 60  # 5 minutes
WARMUP_SECONDS = 0 # 10 * 60  # 10 minutes before starting

# Track pipeline start time
_pipeline_start: datetime | None = None

print("Let's goooooo!")

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
        # Get our own PID to exclude
        my_pid = str(subprocess.os.getpid())

        # Find python processes with mediacloud/mcloud/fetch-urls in the command
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
    check_interval = 5 * 60  # check every 5 minutes

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

    # If target time already passed today, schedule for tomorrow
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
    parser = argparse.ArgumentParser(description="Run MediaCloud -> descriptions pipeline")
    parser.add_argument("--wait", action="store_true",
                        help="Wait for other MediaCloud processes to finish first")
    parser.add_argument("--at", metavar="HH:MM",
                        help="Wait until specific time (e.g., 03:00)")
    args = parser.parse_args(argv)

    # Handle waiting logic
    if args.wait and args.at:
        # Wait for whichever comes first: time reached OR other process finishes
        print(f"Will start when other mcloud processes finish OR at {args.at}")
        hour, minute = map(int, args.at.split(":"))
        now = datetime.now()
        target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target_time <= now:
            target_time += timedelta(days=1)

        while other_mcloud_running() and datetime.now() < target_time:
            now_str = datetime.now().strftime("%H:%M:%S")
            print(f"   [{now_str}] Still waiting... (other process running, checking in 5 min)", flush=True)
            time.sleep(5 * 60)

        if not other_mcloud_running():
            print("Other process(es) finished!")
        else:
            print(f"Time {args.at} reached!")
    elif args.wait:
        wait_for_clear()
    elif args.at:
        wait_until_time(args.at)

    # === Pipeline officially starting ===
    global _pipeline_start
    _pipeline_start = datetime.now()

    print("\n" + "=" * 60)
    print("PIPELINE STARTING")
    print(f"   Time: {_pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60, flush=True)

    # Warmup countdown (10 min)
    print(f"\nWarmup period: {WARMUP_SECONDS // 60} minutes")
    countdown(WARMUP_SECONDS, label="Pipeline")

    # Step 1: Fetch from MediaCloud
    rc = run_step("Fetch URLs from MediaCloud", FETCH_SCRIPT)
    if rc != 0:
        print(f"\nFetch step failed with code {rc}", file=sys.stderr)
        return rc

    # Break between steps
    print(f"\nBreak: {BREAK_SECONDS // 60} minutes before scraping descriptions...")
    countdown(BREAK_SECONDS, label="Next step")

    # Step 2: Scrape descriptions
    rc = run_step("Scrape descriptions", DESCRIBE_SCRIPT)
    if rc != 0:
        print(f"\nDescribe step failed with code {rc}", file=sys.stderr)
        return rc

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE!")
    print(f"   Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Total time: {elapsed_str()}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
