"""
RSS Pipeline v2 — Configuration

Fill in your topics, keywords, and outlet list below.
Set DATABASE_URL as an environment variable (or in .env for local dev).
"""

import os

# ── Database ─────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL")

# ── Fetch settings ───────────────────────────────────────────────────
DAYS_BACK = 5               # Only keep articles from the last N days
MAX_PER_OUTLET = None        # None = no limit; set to 3 for quick tests

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ── Topics ───────────────────────────────────────────────────────────
# Each topic is a named set of keywords used at query time.
# Articles are stored once; topic filtering happens when you query.
#
# keyword matching (strict):
#   - keyword in title  → match
#   - keyword appears 2+ times in description  → match
#
# exclude_terms: reject articles containing these (case-insensitive)

TOPICS = {
    # ── Example topic — replace with your own ──────────────────────
    "example-topic": {
        "keywords": [
            "example keyword",
            "another keyword",
        ],
        "exclude_terms": [
            "sports", "entertainment",
        ],
    },
    # ── Second example ─────────────────────────────────────────────
    "another-topic": {
        "keywords": [
            "some phrase",
            "related term",
        ],
        # exclude_terms is optional
    },
}

# ── RSS outlets ──────────────────────────────────────────────────────
# Tested and working from GitHub Actions as of early 2025.
# Disabled outlets noted in comments.

RSS_OUTLETS = {
    "abc":       {"name": "ABC News",    "domain": "abcnews.go.com",    "url": "https://abcnews.go.com/abcnews/topstories"},
    "breitbart": {"name": "Breitbart",   "domain": "breitbart.com",     "url": "http://feeds.feedburner.com/breitbart"},
    "cbs":       {"name": "CBS News",    "domain": "cbsnews.com",       "url": "https://www.cbsnews.com/latest/rss/main"},
    "cnn":       {"name": "CNN",         "domain": "cnn.com",           "url": "http://rss.cnn.com/rss/cnn_topstories.rss"},
    "dailywire": {"name": "Daily Wire",  "domain": "dailywire.com",     "url": "https://www.dailywire.com/feeds/rss.xml"},
    "fox":       {"name": "Fox News",    "domain": "foxnews.com",       "url": "https://moxie.foxnews.com/google-publisher/latest.xml"},
    "nbc":       {"name": "NBC News",    "domain": "nbcnews.com",       "url": "https://feeds.nbcnews.com/nbcnews/public/news"},
    "npr":       {"name": "NPR",         "domain": "npr.org",           "url": "https://feeds.npr.org/1001/rss.xml"},
    "nypost":    {"name": "NY Post",     "domain": "nypost.com",        "url": "https://nypost.com/feed/"},
    "nyt":       {"name": "NY Times",    "domain": "nytimes.com",       "url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml"},
    "wapo":      {"name": "Wash Post",   "domain": "washingtonpost.com","url": "https://feeds.washingtonpost.com/rss/politics"},

    # ── Danish outlets ───────────────────────────────────────────────
    "dr":        {"name": "DR",          "domain": "dr.dk",             "url": "https://www.dr.dk/nyheder/service/feeds/senestenyt"},
    # "tv2dk":     {"name": "TV 2",        "domain": "tv2.dk",            "url": "..."},  # No public RSS feed found
    "politiken": {"name": "Politiken",   "domain": "politiken.dk",      "url": "https://politiken.dk/rss/senestenyt.rss"},
    # "jp":        {"name": "Jyllands-Posten", "domain": "jyllands-posten.dk", "url": "..."},  # No public RSS feed found
    "berlingske":{"name": "Berlingske",  "domain": "berlingske.dk",     "url": "https://www.berlingske.dk/content/rss"},
    # "eb":        {"name": "Ekstra Bladet","domain": "ekstrabladet.dk",  "url": "https://ekstrabladet.dk/rssfeed/all"},
    # "bt":        {"name": "BT",          "domain": "bt.dk",             "url": "https://www.bt.dk/bt/seneste/rss"},
    "information":{"name":"Information", "domain": "information.dk",    "url": "https://www.information.dk/feed"},
    "feltet":    {"name": "Feltet.dk",   "domain": "feltet.dk",         "url": "https://feltet.dk/rss"},

    # Disabled — broken from GitHub Actions:
    # "ap":      {"name": "AP News",     "domain": "apnews.com",        "url": "..."},  # 401 from datacenter IPs
    # "msnbc":   {"name": "MSNBC",       "domain": "msnbc.com",         "url": "..."},  # Returns error page
    # "newsmax": {"name": "Newsmax",     "domain": "newsmax.com",       "url": "..."},  # Timeouts
    # "washtimes":{"name":"Wash Times",  "domain": "washingtontimes.com","url": "..."},  # 403
    # "wsj":     {"name": "WSJ",         "domain": "wsj.com",           "url": "..."},  # Feeds frozen since Jan 2025
    # "usatoday":{"name":"USA Today",    "domain": "usatoday.com",      "url": "..."},  # RSS discontinued
}

# ── Global filters ───────────────────────────────────────────────────
# Applied to ALL articles before storing.

GLOBAL_FILTERS = {
    "require_description": True,
    "require_english": False,

    "promo_terms": [
        "watch live", "live stream", "tune in",
        "tonight on", "this week on", "coming up on",
        "don't miss", "catch the",
        "subscribe now", "sign up for",
        "exclusive interview", "full episode",
        "sponsored content", "paid content", "advertisement",
        "listen to the podcast", "new episode",
        "newsletter", "breaking news alert",
        "5 things to know",
        "weekly student news quiz",
    ],
}
