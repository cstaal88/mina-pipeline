#!/usr/bin/env python3
"""
Manifest utilities for tracking data collection coverage.

The manifest tracks:
- Which dates have been collected
- Daily run summaries (count, first/last run times)
- Any gaps in coverage
- Total record count

Manifest is stored as the first line of JSONL files (with "_manifest": true).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


def create_empty_manifest(topic: str, start_date: date) -> dict:
    """Create a new empty manifest for a topic."""
    return {
        "_manifest": True,
        "topic": topic,
        "coverage": {
            "start_date": start_date.isoformat(),
            "end_date": None,
            "last_updated": None,
        },
        "dates_collected": [],
        "daily_runs": {},
        "record_count": 0,
        "gaps": [],
    }


def parse_manifest(first_line: str) -> dict | None:
    """
    Parse manifest from first line of JSONL file.
    
    Returns None if first line is not a manifest.
    """
    try:
        obj = json.loads(first_line.strip())
        if isinstance(obj, dict) and obj.get("_manifest"):
            return obj
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def load_manifest_from_file(filepath: Path) -> dict | None:
    """Load manifest from a JSONL file, if present."""
    if not filepath.exists():
        return None
    
    with filepath.open("r", encoding="utf-8") as f:
        first_line = f.readline()
        return parse_manifest(first_line)


def load_manifest_from_jsonl(content: str) -> dict | None:
    """Load manifest from JSONL string content."""
    if not content or not content.strip():
        return None
    
    first_line = content.strip().split("\n")[0]
    return parse_manifest(first_line)


def get_dates_to_collect(manifest: dict | None, topic_start_date: date) -> list[date]:
    """
    Calculate which dates need to be collected.
    
    Args:
        manifest: Existing manifest (or None if new)
        topic_start_date: When collection should begin
        
    Returns:
        List of dates that need collection (sorted ascending)
    """
    today = date.today()
    
    # Generate all dates from start to today
    all_dates = set()
    current = topic_start_date
    while current <= today:
        all_dates.add(current)
        current = date(current.year, current.month, current.day + 1) if current.day < 28 else _next_date(current)
    
    # Subtract already collected dates
    if manifest:
        collected = set(date.fromisoformat(d) for d in manifest.get("dates_collected", []))
    else:
        collected = set()
    
    missing = sorted(all_dates - collected)
    return missing


def _next_date(d: date) -> date:
    """Helper to get next date (handles month/year boundaries)."""
    from datetime import timedelta
    return d + timedelta(days=1)


def detect_gaps(dates_collected: list[str], start_date: date, end_date: date) -> list[str]:
    """
    Detect gaps in collected dates.
    
    Returns list of missing date strings.
    """
    if not dates_collected:
        return []
    
    collected_set = set(dates_collected)
    gaps = []
    
    current = start_date
    while current <= end_date:
        if current.isoformat() not in collected_set:
            gaps.append(current.isoformat())
        current = _next_date(current)
    
    return gaps


def update_manifest_after_run(
    manifest: dict,
    dates_added: list[date],
    new_record_count: int,
    mode: str = "adhoc",
) -> dict:
    """
    Update manifest after a collection run.
    
    Args:
        manifest: Existing manifest to update
        dates_added: Dates that were collected in this run
        new_record_count: Number of new records added
        mode: "automated" or "adhoc"
        
    Returns:
        Updated manifest
    """
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    today_str = now.date().isoformat()
    time_str = now.strftime("%H:%M:%SZ")
    
    # Update dates_collected
    existing_dates = set(manifest.get("dates_collected", []))
    for d in dates_added:
        existing_dates.add(d.isoformat() if isinstance(d, date) else d)
    manifest["dates_collected"] = sorted(existing_dates)
    
    # Update daily_runs
    daily_runs = manifest.get("daily_runs", {})
    if today_str not in daily_runs:
        daily_runs[today_str] = {"count": 0, "first": time_str, "last": time_str, "mode": mode}
    
    daily_runs[today_str]["count"] += 1
    daily_runs[today_str]["last"] = time_str
    manifest["daily_runs"] = daily_runs
    
    # Update coverage
    if manifest["dates_collected"]:
        manifest["coverage"]["end_date"] = max(manifest["dates_collected"])
    manifest["coverage"]["last_updated"] = now_iso
    
    # Update record count
    manifest["record_count"] = manifest.get("record_count", 0) + new_record_count
    
    # Detect gaps
    if manifest["dates_collected"]:
        start = date.fromisoformat(manifest["coverage"]["start_date"])
        end = date.fromisoformat(manifest["coverage"]["end_date"])
        manifest["gaps"] = detect_gaps(manifest["dates_collected"], start, end)
    
    return manifest


def manifest_to_jsonl_line(manifest: dict) -> str:
    """Convert manifest to a JSONL line."""
    return json.dumps(manifest, ensure_ascii=False, sort_keys=False)


def prepend_manifest_to_content(manifest: dict, content: str) -> str:
    """
    Prepend manifest to JSONL content, replacing any existing manifest.
    
    Args:
        manifest: Manifest dict
        content: Existing JSONL content (may or may not have manifest)
        
    Returns:
        JSONL content with manifest as first line
    """
    lines = content.strip().split("\n") if content.strip() else []
    
    # Remove existing manifest if present
    if lines and parse_manifest(lines[0]):
        lines = lines[1:]
    
    # Prepend new manifest
    manifest_line = manifest_to_jsonl_line(manifest)
    return manifest_line + "\n" + "\n".join(lines) + ("\n" if lines else "")


def load_records_skip_manifest(filepath: Path) -> list[dict]:
    """Load all records from JSONL file, skipping manifest."""
    records = []
    if not filepath.exists():
        return records
    
    with filepath.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                # Skip manifest and meta entries
                if isinstance(obj, dict) and (obj.get("_manifest") or obj.get("_meta")):
                    continue
                records.append(obj)
            except json.JSONDecodeError:
                continue
    
    return records


def count_records_in_content(content: str) -> int:
    """Count non-manifest records in JSONL content."""
    count = 0
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_manifest") and not obj.get("_meta"):
                count += 1
        except json.JSONDecodeError:
            continue
    return count
