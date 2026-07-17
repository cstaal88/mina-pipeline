# ─────────────────────────────────────────────────────────────────────────────
# GETNEWS PIPELINE CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
#
# This is the RSS-based news collection pipeline.
# - fetch-raw.py: Fetches stories from RSS feeds
# - clean.py: Filters for topics, formats, uploads to gists
#
# For historical backfill, use the MediaCloud scripts in mediacloud/
# ─────────────────────────────────────────────────────────────────────────────

from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
TEST_DIR = DATA_DIR / "test"

# ─────────────────────────────────────────────────────────────────────────────
# GIST STORAGE
# ─────────────────────────────────────────────────────────────────────────────
# Single gist with all data:
#   - raw.jsonl: EVERY story fetched, no topic filtering (RSS + MediaCloud merged)
#   - clean-{topic}.jsonl: strict filtered per topic, regenerated from raw each run

GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"

# Old per-topic gists (kept for migration reference):
# minneapolis-ice: 839f9f409d36d715d277095886ced536
# greenland-trump: a046f4a9233ff2e499dfeb356e081d79

# ─────────────────────────────────────────────────────────────────────────────
# FETCH SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

DAYS_BACK = 5           # Only stories from last N days (None = no filter)
MAX_STORIES = None      # Limit total stories (None = no limit)
MAX_PER_OUTLET = None   # Limit per outlet - handy for testing (None = no limit)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ─────────────────────────────────────────────────────────────────────────────
# TOPICS
# ─────────────────────────────────────────────────────────────────────────────
# Keywords select what lands in clean-{topic}.jsonl only; raw.jsonl is
# unfiltered. Strictness:
#   • loose pre-pass: any keyword match anywhere in title/summary
#   • clean-{topic}.jsonl: keyword in title, OR 2+ times in summary

TOPICS = {
    "minneapolis-ice": {
        "keywords": [
            "minneapolis", "minnesota",
            "renée good", "renee good",
            "pretti", "alex pretti",
            "ice shooting", "ice raid",
        ],
        "exclude_terms": [
            "hockey", "nhl", "wild", "puck", "rink", "skate",
            "gophers", "matchup", "big ten", "iihf",
        ],
    },
    "greenland-trump": {
        "keywords": [
            "greenland", "denmark", "danish",
            "nuuk", "arctic",
        ],
    },
    # ── 2026 US midterms: THREE tiers over the same raw archive ──────────────
    # We don't yet know the right recall/precision threshold for the study's
    # chatbot, so we build three clean files at different widths and choose
    # empirically (descriptives + chatbot eval). All three filter the same
    # raw.jsonl; only the keyword breadth differs. Naming -> clean-{topic}.jsonl.
    #
    # Design rule: COLLECT BROAD, FILTER TO TIERS. The MediaCloud query
    # (mediacloud/config.py) is the broadest of the three so every tier has data
    # to draw from; you can filter down at clean-time but not filter in what was
    # never collected.

    # FOCUSED -- horse-race only. High precision; every hit is about a contest.
    "midterms-focused": {
        "keywords": [
            "midterm", "midterms", "midterm election", "midterm elections",
            "senate race", "senate races",
            "house race", "house races",
            "congressional race", "congressional races",
            "gubernatorial", "governor's race",
            "control of congress", "control of the senate",
            "control of the house",
            "2026 election", "2026 elections", "2026 midterms",
        ],
        "exclude_terms": [
            "hall of fame", "conclave", "papal", "pope",
            "nascar", "derby", "grand prix", "marathon",
        ],
    },

    # MEDIUM -- focused PLUS policy issues, but only when they co-occur with an
    # election-context term (context_issues AND context_terms). Picks up "what
    # the election is about" without ingesting every immigration story ever.
    "midterms-medium": {
        "keywords": [
            "midterm", "midterms", "midterm election", "midterm elections",
            "senate race", "senate races",
            "house race", "house races",
            "congressional race", "congressional races",
            "gubernatorial", "governor's race",
            "control of congress", "control of the senate",
            "control of the house",
            "2026 election", "2026 elections", "2026 midterms",
        ],
        "context_issues": [
            "immigration", "economy", "inflation", "abortion", "border",
            "tariff", "tariffs", "healthcare", "health care", "crime",
            "gun", "deportation", "housing", "jobs", "wages",
        ],
        "context_terms": [
            "midterm", "midterms", "election", "elections", "campaign",
            "candidate", "ballot", "voters", "primary",
            "senate race", "house race",
        ],
        "exclude_terms": [
            "hall of fame", "conclave", "papal", "pope",
            "nascar", "derby", "grand prix", "marathon",
        ],
    },

    # BROAD -- any election/campaign/issue term. Max recall, accepts noise
    # (general political stories that merely mention an issue). No gating.
    "midterms-broad": {
        "keywords": [
            "midterm", "midterms", "midterm election", "midterm elections",
            "senate race", "senate races", "house race", "house races",
            "congressional race", "congressional races",
            "gubernatorial", "governor's race",
            "control of congress", "control of the senate", "control of the house",
            "2026 election", "2026 elections", "2026 midterms",
            "election", "elections", "campaign", "candidate", "primary",
            "ballot", "voters", "turnout", "senate", "house", "congress",
            "congressional", "poll", "polls", "democrat", "republican",
            "immigration", "economy", "inflation", "abortion", "border",
            "tariff", "healthcare",
        ],
        "exclude_terms": [
            "hall of fame", "conclave", "papal", "pope",
            "nascar", "derby", "grand prix", "marathon",
        ],
    },
}

# Which topics to process (list of keys, or None for all).
# Focus on the midterms study: only the three tiers regenerate each run. The
# old topics (minneapolis-ice, greenland-trump) stay defined and their raw data
# is retained, but their clean files freeze in place -- kept, not deleted.
ACTIVE_TOPICS = ["midterms-focused", "midterms-medium", "midterms-broad"]

# ─────────────────────────────────────────────────────────────────────────────
# FILE PATHS
# ─────────────────────────────────────────────────────────────────────────────

RAW_STORIES_FILE = DATA_DIR / "tmp-news.json"  # Output of fetch-raw.py (working file, overwritten each run)

# ─────────────────────────────────────────────────────────────────────────────
# RSS OUTLETS
# ─────────────────────────────────────────────────────────────────────────────
# key: used internally
# name: display name (stored in output)
# domain: for matching with existing raw.jsonl format
# url: RSS feed URL

OUTLETS = {
    "abc": {
        "name": "ABC News",
        "domain": "abcnews.go.com",
        "url": "https://abcnews.go.com/abcnews/topstories",
    },
    # AP: Returns 401 on GitHub Actions (datacenter IP blocked)
    # "ap": {
    #     "name": "AP News",
    #     "domain": "apnews.com",
    #     "url": "https://apnews.com/index.rss",
    # },
    "breitbart": {
        "name": "Breitbart",
        "domain": "breitbart.com",
        "url": "http://feeds.feedburner.com/breitbart",
    },
    "cbs": {
        "name": "CBS News",
        "domain": "cbsnews.com",
        "url": "https://www.cbsnews.com/latest/rss/main",
    },
    "cnn": {
        "name": "CNN",
        "domain": "cnn.com",
        "url": "http://rss.cnn.com/rss/cnn_topstories.rss",
    },
    "dailywire": {
        "name": "Daily Wire",
        "domain": "dailywire.com",
        "url": "https://www.dailywire.com/feeds/rss.xml",
    },
    "fox": {
        "name": "Fox News",
        "domain": "foxnews.com",
        "url": "https://moxie.foxnews.com/google-publisher/latest.xml",
    },
    # MSNBC: Returns error page instead of RSS on GitHub Actions
    # "msnbc": {
    #     "name": "MSNBC",
    #     "domain": "msnbc.com",
    #     "url": "https://www.msnbc.com/feeds/latest",
    # },
    "nbc": {
        "name": "NBC News",
        "domain": "nbcnews.com",
        "url": "https://feeds.nbcnews.com/nbcnews/public/news",
    },
    # Newsmax: Timeouts on GitHub Actions
    # "newsmax": {
    #     "name": "Newsmax",
    #     "domain": "newsmax.com",
    #     "url": "https://www.newsmax.com/rss/Newsfront/1/",
    # },
    "npr": {
        "name": "NPR",
        "domain": "npr.org",
        "url": "https://feeds.npr.org/1001/rss.xml",
    },
    "nypost": {
        "name": "NY Post",
        "domain": "nypost.com",
        "url": "https://nypost.com/feed/",
    },
    "nyt": {
        "name": "NY Times",
        "domain": "nytimes.com",
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    },
    "wapo": {
        "name": "Wash Post",
        "domain": "washingtonpost.com",
        "url": "https://feeds.washingtonpost.com/rss/politics",
    },
    # Wash Times: Returns 403 on GitHub Actions
    # "washtimes": {
    #     "name": "Wash Times",
    #     "domain": "washingtontimes.com",
    #     "url": "https://www.washingtontimes.com/rss/headlines/news/politics/",
    # },
    # WSJ: RSS feeds frozen since Jan 2025 - use MediaCloud for WSJ
    # USA Today: RSS feeds discontinued - redirects to homepage
}

# ─────────────────────────────────────────────────────────────────────────────
# CLEAN FILE EXCLUSIONS
# ─────────────────────────────────────────────────────────────────────────────
# Domains to exclude from clean-*.jsonl files (but keep in raw.jsonl)
# These may have historical data from MediaCloud that we don't want in clean output

EXCLUDED_FROM_CLEAN = [
    "usatoday.com",
    "ms.now",
]

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL FILTERS (applied to ALL topics)
# ─────────────────────────────────────────────────────────────────────────────
# These rules filter out non-news content regardless of topic

GLOBAL_FILTERS = {
    # Must have a description/summary (not null or empty)
    "require_description": True,
    
    # Must be in English
    "require_english": True,
    
    # Terms that indicate TV promos, ads, or non-news content
    "promo_terms": [
        "watch live", "live stream", "tune in",
        "tonight on", "this week on", "coming up on",
        "don't miss", "catch the",
        "subscribe now", "sign up for",
        "exclusive interview", "full episode",
        "sponsored content", "paid content", "advertisement",
        "listen to the podcast", "new episode",
        "newsletter", "breaking news alert",
        # Daily news digest / quiz formats (not standalone stories about the topic)
        "5 things to know",
        "weekly student news quiz",
    ],
}

# Which outlets to fetch (list of keys, or None for all)
ACTIVE_OUTLETS = None
