#!/usr/bin/env python3
"""
======================================================
MINA News Data Pipeline - Configuration
======================================================

TOPIC SELECTION
  No --topic flag  →  uses DEFAULT_TOPIC hardcoded below
  --topic NAME     →  uses that topic
  --list-topics    →  shows all available topics

RUN MODES
  (no flags)       →  full pipeline, writes locally only
  --push-gist      →  also uploads to gist after completion
  --days N         →  trial run: only last N days
  --clean-only     →  skip collection, just regenerate clean.jsonl
  --collect-only   →  skip cleaning

DATA FILES
  raw/{topic}/{date}/  →  APPEND-ONLY, never deleted
  clean/articles-*.jsonl  →  REGENERATED each run from raw

CRASH RECOVERY
  Media Cloud URLs checkpointed per source/day  →  restart continues where it left off

EXAMPLES
  python3 run-pipeline.py                         # full run, default topic
  python3 run-pipeline.py --topic greenland-trump # specific topic
  python3 run-pipeline.py --days 2                # quick 2-day trial
  python3 run-pipeline.py --days 2 --push-gist    # trial + upload
  python3 run-pipeline.py --clean-only            # just regen clean.jsonl

=============================================================================
"""

from datetime import date

# =============================================================================
# DEFAULT TOPIC
# =============================================================================
# Used when no --topic flag is provided (convenience for ad-hoc runs)
# Automated workflows should always pass --topic explicitly
DEFAULT_TOPIC = "minneapolis-ice"

# =============================================================================
# TOPIC CONFIGURATIONS
# =============================================================================
# Each topic has:
#   - start_date: When to begin collecting (for backfills)
#   - query: MediaCloud search query
#   - outlets: Dict of {domain: source_id} to search
#   - filter_keywords: Keywords for cleaning/filtering (case-insensitive)
#   - gist_id_raw: Gist ID for raw collected data
#   - gist_id_clean: Gist ID for cleaned/filtered data

# Common outlets used across topics
ALL_OUTLETS = {
    "breitbart.com": 19334,
    "foxnews.com": 1092,
    "abcnews.go.com": 19260,
    "apnews.com": 106145,
    # "bbc.com": 932549,  # UK outlet - skip for US focus
    "cbsnews.com": 1752,
    "cnn.com": 1095,
    "dailywire.com": 269352,
    # "theguardian.com": 300560,  # UK outlet - skip for US focus
    "msnbc.com": 293951,
    "nbcnews.com": 25499,
    "newsmax.com": 25349,
    "nypost.com": 7,
    "nytimes.com": 1,
    "npr.org": 1096,
    # "pbs.org": 1093,  # Low volume, skip
    "usatoday.com": 4,
    "wsj.com": 22732,
    "washingtonpost.com": 2,
}

TOPICS = {
    "minneapolis-ice": {
        "start_date": date(2026, 1, 1),
        "query": (
            '('
            '"Renée Good" OR "Renee Good" OR "Renée Nicole Good" '
            'OR (Minneapolis AND ICE) '
            'OR (Minnesota AND ICE) '
            'OR (ICE AND (shooting OR shot OR killed OR fatal OR death)) '
            'OR (Minneapolis AND (shooting OR shot OR killed OR fatal OR death)) '
            ')'
        ),
        "outlets": ALL_OUTLETS,
        "filter_keywords": [
            "renée good", "renee good", "renée nicole good",
            "minneapolis", "minnesota", "ice",
            "shooting", "shot", "killed", "fatal", "death",
        ],
        # Topic keywords for strict relevance filtering
        # KEEP if: keyword in TITLE, OR keyword appears 2+ times in DESCRIPTION
        "topic_keywords": [
            "minneapolis", "minnesota", "pretti", "ice shooting", "ice raid",
            "renée good", "renee good",
        ],
        # Gist contains: raw.jsonl, clean.jsonl
        "gist_id": "839f9f409d36d715d277095886ced536",
    },
    "greenland-trump": {
        "start_date": date(2026, 1, 1),
        "query": (
            '('
            '(Trump AND Greenland) '
            'OR (Trump AND "buy greenland") '
            'OR (Trump AND "purchase greenland") '
            'OR (Greenland AND acquisition) '
            'OR ("Greenland" AND "United States") '
            ')'
        ),
        "outlets": ALL_OUTLETS,
        "filter_keywords": [
            "trump", "greenland", "buy", "purchase", "acquisition",
            "united states", "deal", "agreement",
        ],
        # Topic keywords for strict relevance filtering
        # KEEP if: keyword in TITLE, OR keyword appears 2+ times in DESCRIPTION
        "topic_keywords": [
            "greenland", "denmark", "danish", "nuuk", "arctic",
        ],
        # Gist contains: raw.jsonl, clean.jsonl
        "gist_id": "a046f4a9233ff2e499dfeb356e081d79",
    },
    "midterms": {
        # 2026 US midterm elections (general: 2026-11-03).
        # Backfill earlier by lowering this, but note every extra day costs
        # API quota across all 17 outlets -- check mediacloud/quota.py first.
        "start_date": date(2026, 7, 1),
        # COLLECTION filter: the only thing limiting what the API returns,
        # since the workflow runs with --nofilter. Built to feed the MEDIUM
        # clean tier (getnews/config.py): a high-precision horse-race core, PLUS
        # policy issues gated by an election-context term so bare "immigration"
        # etc. don't flood in. Measured ~166 stories/day across ~16 outlets
        # (~2x the horse-race-only core); ~16 quota hits/day, well within budget.
        # Collect-broad-filter-to-tiers: this is the broadest we collect, and the
        # focused/medium clean filters subset it downstream.
        "query": (
            '('
            # -- horse-race core --
            '"midterm election" OR "midterm elections" OR midterms '
            'OR "Senate race" OR "Senate races" '
            'OR "House race" OR "House races" '
            'OR "gubernatorial race" OR "governor\'s race" '
            'OR "control of Congress" OR "control of the Senate" '
            'OR "control of the House" '
            'OR ("2026" AND (election OR elections) '
            'AND (Senate OR House OR governor OR congressional)) '
            # -- policy issues, gated by election context --
            'OR ( '
            '(immigration OR economy OR inflation OR abortion OR border '
            'OR tariffs OR healthcare OR deportation OR crime) '
            'AND (midterm OR midterms OR election OR campaign OR candidate '
            'OR "Senate race" OR "House race" OR ballot OR primary OR voters) '
            ') '
            ')'
        ),
        "outlets": ALL_OUTLETS,
        "filter_keywords": [
            "midterm", "midterms", "election", "elections",
            "senate", "house", "congress", "congressional",
            "governor", "gubernatorial", "campaign", "candidate",
            "primary", "ballot", "voters", "turnout",
        ],
        # Topic keywords for strict relevance filtering
        # KEEP if: keyword in TITLE, OR keyword appears 2+ times in DESCRIPTION
        # Phrases only -- a bare "election" in a title admits far too much.
        "topic_keywords": [
            "midterm", "midterms", "midterm election",
            "senate race", "house race", "congressional race",
            "gubernatorial", "governor's race",
            "control of congress", "control of the senate",
            "control of the house",
            "2026 election", "2026 elections",
        ],
        # No gist_id on purpose: the workflow owns gist I/O for this topic and
        # writes to the unified gist. Setting gist_id here would let a manual
        # --push-gist run overwrite the unified raw.jsonl with MC-only data.
    },
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_topic_config(topic_name: str | None = None) -> dict:
    """
    Get configuration for a topic.
    
    Args:
        topic_name: Topic name, or None to use DEFAULT_TOPIC
        
    Returns:
        Topic configuration dict
        
    Raises:
        ValueError: If topic not found
    """
    name = topic_name or DEFAULT_TOPIC
    
    if name not in TOPICS:
        available = ", ".join(sorted(TOPICS.keys()))
        raise ValueError(f"Unknown topic '{name}'. Available: {available}")
    
    config = TOPICS[name].copy()
    config["name"] = name
    return config


def list_topics() -> None:
    """Print available topics for CLI help."""
    print("\nAvailable topics:")
    for name, cfg in sorted(TOPICS.items()):
        start = cfg["start_date"].isoformat()
        print(f"  - {name} (from {start})")
    print()
