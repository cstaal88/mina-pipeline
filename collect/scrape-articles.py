#!/usr/bin/env python3
"""Scrape article metadata from news URLs.

Reads from raw/{topic}/{date}/urls.jsonl and writes to raw/{topic}/{date}/articles.jsonl.
Adds my_topic field to each record for later filtering.

Usage:
    python3 scrape-articles.py --topic minneapolis-ice
    python3 scrape-articles.py --topic minneapolis-ice --trial3  # Sample 3 URLs
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from bs4 import BeautifulSoup
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

# Import config from parent directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import get_topic_config, list_topics, DEFAULT_TOPIC

try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    tqdm = None

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class FetchError(RuntimeError):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_jsonl(path: Path) -> bool:
    return path.suffix.lower() in {".jsonl", ".ndjson"}


def _normalize_loaded_obj(obj: Any) -> list[dict[str, Any]]:
    if isinstance(obj, dict) and "urls" in obj and isinstance(obj["urls"], list):
        return [{"url": u} for u in obj["urls"]]

    if isinstance(obj, list):
        if len(obj) == 0:
            return []
        if all(isinstance(x, str) for x in obj):
            return [{"url": x} for x in obj]
        if all(isinstance(x, dict) for x in obj):
            return [x for x in obj]

    if isinstance(obj, dict):
        return [obj]

    raise ValueError("Unsupported input JSON structure")


def load_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(str(path))

    if _is_jsonl(path):
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                # Skip manifest/meta entries
                if isinstance(obj, dict) and (obj.get("_manifest") or obj.get("_meta")):
                    continue
                records.extend(_normalize_loaded_obj(obj))
        return records

    obj = json.loads(path.read_text(encoding="utf-8"))
    return _normalize_loaded_obj(obj)


def get_url(record: dict[str, Any]) -> Optional[str]:
    for key in ("url", "link", "uri"):
        v = record.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def read_urls_from_output(path: Path) -> set[str]:
    if not path.exists():
        return set()

    urls: set[str] = set()
    if _is_jsonl(path):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    # Skip manifest/meta
                    if obj.get("_manifest") or obj.get("_meta"):
                        continue
                    if isinstance(obj.get("url"), str):
                        urls.add(obj["url"].strip())
        return urls

    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict) and isinstance(item.get("url"), str):
                urls.add(item["url"].strip())
    return urls


def write_jsonl(path: Path, records: Iterable[dict[str, Any]], *, append: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with path.open(mode, encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def extract_description_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    for key, val in (
        ("name", "description"),
        ("property", "og:description"),
        ("name", "twitter:description"),
    ):
        tag = soup.find("meta", attrs={key: val})
        if tag and tag.get("content"):
            c = str(tag.get("content")).strip()
            if c:
                return c
    return None


def extract_title_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    for key, val in (("property", "og:title"), ("name", "twitter:title")):
        tag = soup.find("meta", attrs={key: val})
        if tag and tag.get("content"):
            c = str(tag.get("content")).strip()
            if c:
                return c
    if soup.title and soup.title.string:
        t = soup.title.string.strip()
        return t or None
    return None


@dataclass(frozen=True)
class FetchConfig:
    timeout: float
    retries: int
    backoff_max: float
    user_agent: str


def fetch_html(url: str, *, cfg: FetchConfig) -> tuple[str, str, int]:
    headers = {
        "User-Agent": cfg.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    def _do_get() -> tuple[str, str, int]:
        resp = requests.get(url, headers=headers, timeout=cfg.timeout, allow_redirects=True)
        status = int(resp.status_code)
        if status >= 400:
            raise FetchError(f"HTTP {status}")
        resp.encoding = resp.encoding or resp.apparent_encoding
        return resp.text, resp.url, status

    for attempt in Retrying(
        reraise=True,
        stop=stop_after_attempt(max(1, int(cfg.retries))),
        wait=wait_exponential(multiplier=1, min=1, max=float(cfg.backoff_max)),
        retry=retry_if_exception_type((requests.RequestException, FetchError)),
    ):
        with attempt:
            return _do_get()

    raise RuntimeError("unreachable")


def scrape_article(url: str, topic: str, *, cfg: FetchConfig) -> dict[str, Any]:
    """Scrape article metadata and add my_topic field."""
    scraped_at = _now_iso()
    try:
        html, final_url, status = fetch_html(url, cfg=cfg)
        return {
            "url": url,
            "final_url": final_url,
            "http_status": status,
            "description": extract_description_from_html(html),
            "title": extract_title_from_html(html),
            "success": True,
            "error": None,
            "scraped_at": scraped_at,
            "my_topic": topic,
        }
    except Exception as e:
        return {
            "url": url,
            "final_url": None,
            "http_status": None,
            "description": None,
            "title": None,
            "success": False,
            "error": str(e),
            "scraped_at": scraped_at,
            "my_topic": topic,
        }


def _progress(total: int, desc: str):
    if tqdm is None:
        return None
    return tqdm(total=total, desc=desc, unit="url")


# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent


def get_raw_dir(topic: str) -> Path:
    """Get raw directory for a topic."""
    return REPO_DIR / "raw" / topic


def find_date_dirs(topic: str) -> list[Path]:
    """Find all date directories for a topic."""
    raw_dir = get_raw_dir(topic)
    if not raw_dir.exists():
        return []
    return sorted([d for d in raw_dir.iterdir() if d.is_dir()])


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape article metadata from news URLs.")
    p.add_argument("--topic", type=str, default=None,
                   help=f"Topic to process (default: {DEFAULT_TOPIC})")
    p.add_argument("--date", type=str, default=None,
                   help="Specific date to process (YYYY-MM-DD). Default: all dates.")
    p.add_argument("--trial3", action="store_true", help="Sample 3 random URLs and print descriptions.")
    p.add_argument("--workers", "-w", type=int, default=2, help="Parallel workers.")
    p.add_argument("--delay-min", type=float, default=0.3, help="Min delay between requests (seconds).")
    p.add_argument("--delay-max", type=float, default=0.8, help="Max delay between requests (seconds).")
    p.add_argument("--timeout", type=float, default=20, help="Per-request timeout in seconds.")
    p.add_argument("--retries", type=int, default=5, help="Max attempts per URL (includes first try).")
    p.add_argument("--backoff-max", type=float, default=60, help="Max exponential backoff between retries.")
    p.add_argument("--no-resume", action="store_true", help="Re-fetch all URLs, ignoring already present in output.")
    p.add_argument("--limit", type=int, default=None, help="Only process first N records.")
    p.add_argument("--user-agent", type=str, default=DEFAULT_UA, help="Custom User-Agent.")
    p.add_argument("--list-topics", action="store_true", help="List available topics and exit.")
    return p.parse_args(argv)


def process_date_dir(
    date_dir: Path,
    topic: str,
    cfg: FetchConfig,
    args: argparse.Namespace,
) -> tuple[int, int]:
    """Process a single date directory. Returns (ok_count, fail_count)."""
    input_path = date_dir / "urls.jsonl"
    output_path = date_dir / "articles.jsonl"  # Changed from descriptions.jsonl

    if not input_path.exists():
        print(f"  Skipping {date_dir.name}: no urls.jsonl")
        return 0, 0

    records = load_records(input_path)
    if args.limit is not None:
        records = records[: max(0, int(args.limit))]

    urls_raw = [get_url(r) for r in records]
    urls = list(dict.fromkeys([u for u in urls_raw if u]))
    if not urls:
        print(f"  Skipping {date_dir.name}: no URLs found")
        return 0, 0

    if args.trial3:
        picked = urls if len(urls) <= 3 else random.sample(urls, 3)
        for u in picked:
            res = scrape_article(u, topic, cfg=cfg)
            print(f"\nURL: {u}", flush=True)
            if res.get("success") is False:
                print(f"ERROR: {res.get('error')}", flush=True)
            else:
                print(f"TITLE: {res.get('title') or ''}", flush=True)
                print(f"DESCRIPTION: {res.get('description') or ''}", flush=True)
        return 0, 0

    already = read_urls_from_output(output_path) if not args.no_resume else set()
    work = [u for u in urls if u not in already]
    if not work:
        print(f"  {date_dir.name}: nothing to do ({len(already)} already scraped)")
        return 0, 0

    print(f"  {date_dir.name}: scraping {len(work)} URLs ({len(already)} already done)")

    delay_min = float(args.delay_min)
    delay_max = float(args.delay_max)

    def do_one(u: str) -> dict[str, Any]:
        if args.workers > 1 and (delay_min > 0 or delay_max > 0):
            time.sleep(random.uniform(delay_min, max(delay_min, delay_max)))
        return scrape_article(u, topic, cfg=cfg)

    out: list[dict[str, Any]] = []
    ok = 0
    fail = 0
    pbar = _progress(total=len(work), desc=f"Scraping {date_dir.name}")

    if int(args.workers) <= 1:
        for idx, u in enumerate(work):
            res = do_one(u)
            out.append(res)
            if res.get("success") is True:
                ok += 1
            else:
                fail += 1
            if pbar is not None:
                pbar.update(1)
                pbar.set_postfix(ok=ok, fail=fail)
            if idx < len(work) - 1 and (delay_min > 0 or delay_max > 0):
                time.sleep(random.uniform(delay_min, max(delay_min, delay_max)))
    else:
        with ThreadPoolExecutor(max_workers=int(args.workers)) as ex:
            futures = [ex.submit(do_one, u) for u in work]
            for fut in as_completed(futures):
                res = fut.result()
                out.append(res)
                if res.get("success") is True:
                    ok += 1
                else:
                    fail += 1
                if pbar is not None:
                    pbar.update(1)
                    pbar.set_postfix(ok=ok, fail=fail)

    if pbar is not None:
        pbar.close()

    append = bool(not args.no_resume and output_path.exists() and _is_jsonl(output_path))
    write_jsonl(output_path, out, append=append)
    print(f"    Wrote {len(out)} records (ok={ok}, fail={fail})")

    return ok, fail


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    if args.list_topics:
        list_topics()
        return 0

    # Get topic config
    try:
        topic_config = get_topic_config(args.topic)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        list_topics()
        return 1

    topic = topic_config["name"]

    cfg = FetchConfig(
        timeout=float(args.timeout),
        retries=int(args.retries),
        backoff_max=float(args.backoff_max),
        user_agent=str(args.user_agent),
    )

    print(f"\nTopic: {topic}")

    # Determine which date directories to process
    if args.date:
        date_dirs = [get_raw_dir(topic) / args.date]
        if not date_dirs[0].exists():
            print(f"ERROR: Date directory not found: {date_dirs[0]}", file=sys.stderr)
            return 1
    else:
        date_dirs = find_date_dirs(topic)

    if not date_dirs:
        print(f"No date directories found for topic '{topic}'")
        return 0

    print(f"Processing {len(date_dirs)} date directories...")

    total_ok = 0
    total_fail = 0

    for date_dir in date_dirs:
        ok, fail = process_date_dir(date_dir, topic, cfg, args)
        total_ok += ok
        total_fail += fail

    print(f"\nDone! Total: {total_ok} ok, {total_fail} failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
