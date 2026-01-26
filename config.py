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
    "foxnews.com": 1092,
    "abcnews.go.com": 19260,
    "apnews.com": 106145,
    # "bbc.com": 932549,
    "cbsnews.com": 1752,
    "cnn.com": 1095,
    "dailywire.com": 269352,
    # "theguardian.com": 300560,  # UK main; use 1751 for Guardian US
    "msnbc.com": 293951,
    "nbcnews.com": 25499,
    "newsmax.com": 25349,
    "nypost.com": 7,
    "nytimes.com": 1,
    "npr.org": 1096,
    # "pbs.org": 1093,
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
        # Gist contains: raw.jsonl, clean.jsonl
        "gist_id": "a046f4a9233ff2e499dfeb356e081d79",
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
