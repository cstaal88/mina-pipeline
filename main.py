#!/usr/bin/env python3
"""
MINA News Data Pipeline - Main Entry Point

Runs the full pipeline:
1. pipeline-dat-collect.py: Fetch URLs from MediaCloud, then scrape descriptions
2. clean.py: Clean and filter data, write to knowledge-base/

Usage:
    python3 main.py                    # run full pipeline
    python3 main.py --collect-only     # only run collection (skip cleaning)
    python3 main.py --clean-only       # only run cleaning (skip collection)
    python3 main.py --auto             # run everything without prompts
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

COLLECT_SCRIPT = SCRIPT_DIR / "pipeline-dat-collect.py"
CLEAN_SCRIPT = SCRIPT_DIR / "clean.py"


def run_script(name: str, script: Path, extra_args: list[str] | None = None) -> int:
    """Run a script and return its exit code."""
    print(f"\n{'='*60}")
    print(f"Running: {name}")
    print(f"Script: {script}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60, flush=True)

    if not script.exists():
        print(f"ERROR: Script not found: {script}", file=sys.stderr)
        return 1

    cmd = [sys.executable, str(script)]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(cmd, cwd=script.parent)
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="MINA News Data Pipeline")
    parser.add_argument("--collect-only", action="store_true",
                        help="Only run data collection, skip cleaning")
    parser.add_argument("--clean-only", action="store_true",
                        help="Only run cleaning, skip collection")
    parser.add_argument("--auto", action="store_true",
                        help="Run everything without prompts")
    parser.add_argument("--wait", action="store_true",
                        help="Pass --wait to collection script")
    parser.add_argument("--at", metavar="HH:MM",
                        help="Pass --at to collection script")
    args = parser.parse_args()

    start_time = datetime.now()
    print("\n" + "=" * 60)
    print("MINA NEWS DATA PIPELINE")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Step 1: Collection
    if not args.clean_only:
        collect_args = []
        if args.wait:
            collect_args.append("--wait")
        if args.at:
            collect_args.extend(["--at", args.at])

        rc = run_script("Data Collection", COLLECT_SCRIPT, collect_args or None)
        if rc != 0:
            print(f"\nCollection failed with code {rc}", file=sys.stderr)
            return rc

    # Step 2: Cleaning
    if not args.collect_only:
        clean_args = []
        if args.auto:
            clean_args.append("--auto")

        rc = run_script("Data Cleaning", CLEAN_SCRIPT, clean_args or None)
        if rc != 0:
            print(f"\nCleaning failed with code {rc}", file=sys.stderr)
            return rc

    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
