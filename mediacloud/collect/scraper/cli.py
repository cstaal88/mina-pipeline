from __future__ import annotations

import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from tqdm import tqdm

from .io_utils import get_url, load_records, read_urls_from_output, write_output
from .scraper import DEFAULT_UA, scrape_url

app = typer.Typer(add_completion=False, help="Scrape news URLs with multiple methods.")

DEFAULT_INPUT = Path("mcloud4mina-get-urls/data/gaza-2026-all.jsonl")
DEFAULT_OUTPUT = Path("output/descriptions-gaza-2026-all.jsonl")


def _default_input_path() -> Path:
    candidates = [DEFAULT_INPUT]
    # Repo layout: <root>/mcloud-urls2scrape/src/news_scraper_cli/cli.py
    try:
        candidates.append(Path(__file__).resolve().parents[4] / DEFAULT_INPUT)
    except Exception:
        pass
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    return DEFAULT_INPUT


def _default_output_path() -> Path:
    return DEFAULT_OUTPUT


def main() -> None:
    app()


@app.command("scrape")
def scrape(
    input: Optional[Path] = typer.Option(None, "--input", "-i", help="Input JSON/JSONL file."),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output JSON/JSONL file."),
    method: str = typer.Option(
        "trafilatura",
        "--method",
        "-m",
        help="Scraping method: trafilatura | newspaper4k | beautifulsoup | try-all | run-all",
    ),
    only_meta: bool = typer.Option(True, "--only-meta", help="Only fetch metadata (e.g., description)."),
    timeout: int = typer.Option(20, "--timeout", help="HTTP timeout (seconds)."),
    workers: int = typer.Option(2, "--workers", "-w", help="Parallel workers."),
    delay_min: float = typer.Option(0.3, "--delay-min", help="Min delay between requests (seconds)."),
    delay_max: float = typer.Option(0.8, "--delay-max", help="Max delay between requests (seconds)."),
    trial3: bool = typer.Option(
        False,
        "--trial3",
        help="Randomly sample 3 URLs and print fetched descriptions (no output file written).",
    ),
    resume: bool = typer.Option(False, "--resume", help="Skip URLs already in output."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Only process first N records."),
    user_agent: str = typer.Option(DEFAULT_UA, "--user-agent", help="Custom User-Agent."),
) -> None:
    if input is None:
        input = _default_input_path()
    if output is None:
        output = _default_output_path()

    if not input.exists():
        raise typer.BadParameter(f"Input file does not exist: {input}", param_hint="--input")

    method = method.strip().lower()
    allowed = {"trafilatura", "newspaper4k", "beautifulsoup", "try-all", "run-all"}
    if method not in allowed:
        typer.echo(f"Unknown method: {method}. Using run-all.", err=True)
        method = "run-all"

    records = load_records(input)
    if limit is not None:
        records = records[: max(0, int(limit))]

    if trial3:
        urls = [get_url(r) for r in records]
        unique_urls = list(dict.fromkeys([u for u in urls if u]))
        if not unique_urls:
            typer.echo("No URLs found in input.", err=True)
            raise typer.Exit(code=2)

        picked = unique_urls if len(unique_urls) <= 3 else random.sample(unique_urls, 3)
        for u in picked:
            res = scrape_url(
                u,
                method=method,
                only_meta=True,
                timeout=timeout,
                user_agent=user_agent,
            )
            desc = None
            if isinstance(res, dict):
                desc = res.get("description")
                if not desc and isinstance(res.get("meta"), dict):
                    desc = res["meta"].get("description")

            typer.echo(f"\nURL: {u}")
            if res.get("success") is False:
                typer.echo(f"ERROR: {res.get('error')}")
            else:
                typer.echo(f"DESCRIPTION: {desc or ''}")
        return

    already = read_urls_from_output(output) if resume else set()

    # Keep order stable, but skip already-seen
    work: List[Dict[str, Any]] = []
    for r in records:
        u = get_url(r)
        if not u:
            work.append(r)
            continue
        if u in already:
            continue
        work.append(r)

    if not work:
        typer.echo("Nothing to do.")
        return

    out_records: List[Dict[str, Any]] = []

    def do_one(rec: Dict[str, Any]) -> Dict[str, Any]:
        u = get_url(rec)
        if not u:
            return {**rec, "success": False, "error": "Missing url/link/uri", "method": method}

        # For parallel runs, apply a small random staggering delay per task
        if workers > 1 and (delay_max > 0 or delay_min > 0):
            time.sleep(random.uniform(delay_min, max(delay_min, delay_max)))

        res = scrape_url(
            u,
            method=method,
            only_meta=only_meta,
            timeout=timeout,
            user_agent=user_agent,
        )
        # Merge (scrape fields override)
        return {**rec, **res}

    if workers <= 1:
        for idx, rec in enumerate(tqdm(work, desc="Scraping", unit="url")):
            out_records.append(do_one(rec))
            if idx < len(work) - 1 and (delay_max > 0 or delay_min > 0):
                time.sleep(random.uniform(delay_min, max(delay_min, delay_max)))
    else:
        with ThreadPoolExecutor(max_workers=int(workers)) as ex:
            futures = [ex.submit(do_one, rec) for rec in work]
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Scraping", unit="url"):
                out_records.append(fut.result())

    # If resume and output exists, append to it (JSONL only). For JSON arrays, rewrite.
    if resume and output.exists() and output.suffix.lower() in {".jsonl", ".ndjson"}:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("a", encoding="utf-8") as f:
            import json

            for r in out_records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        typer.echo(f"Appended {len(out_records)} records to {output}")
        return

    write_output(output, out_records)
    typer.echo(f"Wrote {len(out_records)} records to {output}")
