"""
RSS fetcher — downloads and parses RSS feeds from configured outlets.

Adapted from v1 fetch-raw.py. Returns a list of dicts instead of writing files.
"""

import gzip
import html
import re
import urllib.request
from datetime import datetime, timedelta, timezone

import xmltodict

from config import DAYS_BACK, MAX_PER_OUTLET, RSS_OUTLETS, USER_AGENT


# ── Date parsing ─────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%a, %d %b %Y %H:%M:%S %z",
    "%a, %d %b %Y %H:%M:%S %Z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]


def parse_date(date_str: str | None) -> datetime | None:
    """Try multiple date formats. Returns timezone-aware UTC datetime or None."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def format_date(date_str: str | None) -> str | None:
    """Parse an RSS date and return YYYY-MM-DD, or None."""
    dt = parse_date(date_str)
    return dt.strftime("%Y-%m-%d") if dt else None


# ── HTTP + HTML helpers ──────────────────────────────────────────────

def http_get(url: str, timeout: float = 15.0) -> bytes:
    """Fetch URL with gzip support. Returns raw bytes."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    # Auto-decompress gzip
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data


def strip_html(text: str) -> str:
    """Remove HTML tags, unescape entities, normalize whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def domain_from_url(url: str) -> str:
    """Extract domain from URL, stripping www. prefix."""
    from urllib.parse import urlparse
    try:
        host = urlparse(url).netloc
        return host.removeprefix("www.")
    except Exception:
        return ""


# ── Main fetch ───────────────────────────────────────────────────────

def fetch_rss() -> list[dict]:
    """
    Fetch articles from all configured RSS outlets.
    Filters to articles published within the last DAYS_BACK days.
    Returns list of article dicts.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    articles = []

    for key, outlet in RSS_OUTLETS.items():
        try:
            raw = http_get(outlet["url"])
            feed = xmltodict.parse(raw)
        except Exception as e:
            print(f"  [{key}] fetch error: {e}")
            continue

        # Navigate to items — handles both RSS 2.0 and Atom-ish feeds
        items = None
        if "rss" in feed:
            channel = feed["rss"].get("channel", {})
            items = channel.get("item", [])
        elif "feed" in feed:
            items = feed["feed"].get("entry", [])

        if items is None:
            print(f"  [{key}] no items found in feed")
            continue

        # xmltodict returns a dict (not list) if there's exactly one item
        if isinstance(items, dict):
            items = [items]

        count = 0
        for item in items:
            # Extract fields
            title = item.get("title", "")
            if isinstance(title, dict):
                title = title.get("#text", "")
            title = strip_html(title)

            link = item.get("link", "")
            if isinstance(link, dict):
                link = link.get("@href", "") or link.get("#text", "")
            if isinstance(link, list):
                for l in link:
                    if isinstance(l, dict) and l.get("@rel") == "alternate":
                        link = l.get("@href", "")
                        break
                else:
                    link = link[0] if link else ""
                    if isinstance(link, dict):
                        link = link.get("@href", "")

            if not link:
                continue

            # Date filter
            pub_str = item.get("pubDate") or item.get("published") or item.get("dc:date")
            pub_dt = parse_date(pub_str)
            if pub_dt and pub_dt < cutoff:
                continue

            # Description/summary
            desc = item.get("description") or item.get("summary") or item.get("content:encoded") or ""
            if isinstance(desc, dict):
                desc = desc.get("#text", "")
            desc = strip_html(desc)

            actual_domain = domain_from_url(link)

            articles.append({
                "url": link,
                "title": title,
                "description": desc,
                "outlet": outlet["name"],
                "domain": actual_domain or outlet["domain"],
                "publish_date": format_date(pub_str),
            })

            count += 1
            if MAX_PER_OUTLET and count >= MAX_PER_OUTLET:
                break

        print(f"  [{key}] {count} articles")

    return articles
