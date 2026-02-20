#!/usr/bin/env python3
"""
clean-report-minneapolis-ice.py

Fetches the current clean-minneapolis-ice.jsonl from the unified gist and produces:
  - describe-kb/minneapolis/YYYY-MM-DD.md   — stats report (Pandoc/LaTeX format)
  - describe-kb/minneapolis/images/by-outlet.png
  - describe-kb/minneapolis/images/by-date.png
  - describe-kb/minneapolis/images/by-date-7d-rolling.png

Usage:
    python clean-report-minneapolis-ice.py
"""

import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import requests

GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"
CLEAN_FILE = "clean-minneapolis-ice.jsonl"

SCRIPT_DIR = Path(__file__).parent.resolve()
TODAY = datetime.now().strftime("%Y-%m-%d")
OUT_DIR     = SCRIPT_DIR / "describe-kb" / "minneapolis" / TODAY
REPORT_PATH = OUT_DIR / f"{TODAY}.md"
IMAGES_DIR  = OUT_DIR / "4e" / "myapx"
DATA_DIR    = OUT_DIR / "data"

def img(stem: str) -> Path:
    """Full path for a chart PNG following the naming convention."""
    return IMAGES_DIR / f"mina-kb-study1-{stem}-{TODAY}.png"

def img_ref(stem: str) -> str:
    """Relative markdown image reference from the report file."""
    return f"4e/myapx/mina-kb-study1-{stem}-{TODAY}.png"


# ---------------------------------------------------------------------------
# Gist / parsing helpers
# ---------------------------------------------------------------------------

def get_gist_file(gist_id: str, filename: str) -> str:
    token = os.getenv("GITHUB_TOKEN") or os.getenv("GIST_PAT")
    headers = {"Authorization": f"token {token}"} if token else {}

    resp = requests.get(f"https://api.github.com/gists/{gist_id}", headers=headers)
    if resp.status_code != 200:
        print(f"Error fetching gist: HTTP {resp.status_code}")
        sys.exit(1)

    files = resp.json().get("files", {})
    if filename not in files:
        print(f"File {filename!r} not found in gist. Available: {sorted(files.keys())}")
        sys.exit(1)

    info = files[filename]
    if info.get("truncated"):
        raw = requests.get(info["raw_url"], headers=headers)
        return raw.text if raw.status_code == 200 else ""
    return info.get("content", "")


def parse_jsonl(content: str) -> list[dict]:
    records = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and not obj.get("_meta"):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def dup_count(records: list[dict], key_fn) -> int:
    keys = [key_fn(r) for r in records if key_fn(r)]
    counter = Counter(keys)
    return sum(cnt - 1 for cnt in counter.values() if cnt > 1)


def rolling_average(date_counts: dict, window: int = 7) -> dict:
    valid_dates = sorted(d for d in date_counts if d and d != "unknown")
    if not valid_dates:
        return {}
    start = datetime.strptime(valid_dates[0], "%Y-%m-%d").date()
    end   = datetime.strptime(valid_dates[-1], "%Y-%m-%d").date()
    all_dates = []
    d = start
    while d <= end:
        all_dates.append(d)
        d += timedelta(days=1)
    counts = {d: date_counts.get(d.strftime("%Y-%m-%d"), 0) for d in all_dates}
    result = {}
    for i, d in enumerate(all_dates):
        ws = max(0, i - window + 1)
        window_dates = all_dates[ws: i + 1]
        result[d.strftime("%Y-%m-%d")] = sum(counts[wd] for wd in window_dates) / len(window_dates)
    return result


def outlet_label(r: dict) -> str:
    return r.get("media_url") or r.get("media_name") or "unknown"


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------

def save_outlet_chart(labels: list, values: list, output_path: Path):
    """Horizontal bar chart, largest-first, with value labels."""
    n = len(labels)
    fig_h = max(7, n * 0.85)
    fig, ax = plt.subplots(figsize=(16, fig_h))

    y_pos = range(n)
    bars = ax.barh(list(y_pos), values, color="#4C72B0", height=0.6)
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(labels, fontsize=25)
    ax.invert_yaxis()
    ax.tick_params(axis="x", labelsize=25)

    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + max(values) * 0.008,
                bar.get_y() + bar.get_height() / 2,
                str(val), va="center", ha="left", fontsize=25)

    ax.set_title("Minneapolis ICE Clean — Stories per Outlet",
                 fontsize=30, fontweight="bold", pad=20)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def save_date_chart(dates: list, values: list, title: str,
                    output_path: Path, color: str = "#2196F3", is_float: bool = False):
    """Vertical bar chart with a count label on every bar."""
    n = len(dates)
    fig_w = max(24, n * 0.6)
    fig, ax = plt.subplots(figsize=(fig_w, 11))

    x_pos = list(range(n))
    bars = ax.bar(x_pos, values, color=color, width=0.8)

    # X-axis ticks: every 7th date
    step = 7
    tick_pos = list(range(0, n, step))
    ax.set_xticks(tick_pos)
    ax.set_xticklabels([dates[i] for i in tick_pos], rotation=90, fontsize=25)
    ax.tick_params(axis="y", labelsize=25)

    # Count label on every bar, rotated vertically
    max_val = max(values) if values else 1
    for bar, val in zip(bars, values):
        if val == 0:
            continue
        label = f"{val:.1f}" if is_float else str(int(val))
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_val * 0.008,
            label,
            ha="center", va="bottom",
            fontsize=25, rotation=90,
        )

    ax.set_title(title, fontsize=30, fontweight="bold", pad=20)
    # Generous headroom for the rotated count labels
    ax.set_ylim(0, max_val * 1.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Fetching {CLEAN_FILE} from gist {GIST_ID}...")
    content = get_gist_file(GIST_ID, CLEAN_FILE)
    records = parse_jsonl(content)
    total = len(records)
    print(f"  Loaded {total:,} records")

    dup_url   = dup_count(records, lambda r: r.get("url", "").strip())
    dup_title = dup_count(records, lambda r: (r.get("title") or "").strip().lower())
    dup_desc  = dup_count(records, lambda r: (r.get("description") or "").strip().lower())

    outlet_counter = Counter(outlet_label(r) for r in records)
    outlet_sorted  = outlet_counter.most_common()

    date_counter = Counter(r.get("publish_date", "unknown") for r in records)
    valid_dates  = sorted(d for d in date_counter if d and d != "unknown")
    date_sorted  = [(d, date_counter[d]) for d in valid_dates]

    rolling        = rolling_average(dict(date_counter))
    rolling_sorted = sorted(rolling.items())

    date_range_str = f"{valid_dates[0]} → {valid_dates[-1]}" if valid_dates else "N/A"

    # -------------------------------------------------------------------
    # Markdown report (Pandoc format)
    # -------------------------------------------------------------------
    lines = [
        "```{=latex}",
        r"\myapx{mina-kb-study1}{MINA's Knowledge Base (Study 1)}",
        "```",
        "",
        "Topic: The ICE-related unrest in Minneapolis in January and February of 2026.",
        "",
        "Appendix based on:",
        "",
        f"- Source: `{CLEAN_FILE}` (in Gist `{GIST_ID}`)",
        "- Script: `mina-pipeline/gists/clean-report-minneapolis-ice.py`",
        f"- Time: {generated_at}",
        "",
        "## Summary",
        "",
        "```{=latex}",
        r"\begin{table}[!htbp]",
        r"  \centering",
        r"  \begin{tabularx}{\textwidth}{@{}Xl@{}}",
        r"    \toprule",
        r"    \textbf{Metric} & \textbf{Value} \\",
        r"    \midrule",
        r"    \rowcolor{gray!15}",
        f"    \\textbf{{Total stories}} & {total:,} \\\\",
        r"    \addlinespace",
        r"    \rowcolor{white}",
        f"    \\textbf{{Date range}} & {date_range_str} \\\\",
        r"    \addlinespace",
        r"    \rowcolor{gray!15}",
        f"    \\textbf{{Duplicate URLs}} & {dup_url:,} \\\\",
        r"    \addlinespace",
        r"    \rowcolor{white}",
        f"    \\textbf{{Duplicate titles}} & {dup_title:,} \\\\",
        r"    \addlinespace",
        r"    \rowcolor{gray!15}",
        f"    \\textbf{{Duplicate descriptions}} & {dup_desc:,} \\\\",
        r"    \bottomrule",
        r"  \end{tabularx}",
        r"\end{table}",
        "```",
        "",
        "## Stories per Outlet",
        "",
        f"![]({img_ref('by-outlet')})",
        "",
        "## Stories per Day",
        "",
        "Renée Good was fatally shot by an ICE officer on January 7, 2026, "
        "and Alex Pretti was killed by Border Patrol agents on January 24, 2026; "
        "spikes following those dates are expected.",
        "",
        f"![]({img_ref('by-date')})",
        "",
        "## Stories per Day (7-Day Rolling Average)",
        "",
        f"![]({img_ref('by-date-7d-rolling')})",
    ]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Saved: {REPORT_PATH}")

    # -------------------------------------------------------------------
    # Data copy
    # -------------------------------------------------------------------
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    data_copy = DATA_DIR / f"clean-minneapolis-ice-{TODAY}.jsonl"
    data_copy.write_text(content, encoding="utf-8")
    print(f"  Saved: {data_copy}")

    # -------------------------------------------------------------------
    # Charts
    # -------------------------------------------------------------------
    out_labels = [o for o, _ in outlet_sorted]
    out_vals   = [c for _, c in outlet_sorted]
    save_outlet_chart(out_labels, out_vals, img("by-outlet"))

    d_labels = [d for d, _ in date_sorted]
    d_vals   = [c for _, c in date_sorted]
    save_date_chart(
        d_labels, d_vals,
        title="Minneapolis ICE Clean — Stories per Day",
        output_path=img("by-date"),
        color="#2196F3",
    )

    r_labels = [d for d, _ in rolling_sorted]
    r_vals   = [v for _, v in rolling_sorted]
    save_date_chart(
        r_labels, r_vals,
        title="Minneapolis ICE Clean — Stories per Day (7-Day Rolling Avg)",
        output_path=img("by-date-7d-rolling"),
        color="#4CAF50",
        is_float=True,
    )

    print(f"\nDone.")
    print(f"  Report : {REPORT_PATH}")
    print(f"  Images : {IMAGES_DIR}/")


if __name__ == "__main__":
    main()
