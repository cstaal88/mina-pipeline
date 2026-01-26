# MINA News Data Pipeline

A unified pipeline for collecting and cleaning news data from MediaCloud.
Supports multiple topics with automatic backfill and incremental collection.

## Quick Start

```bash
# List available topics
python3 run-pipeline.py --list-topics

# Run full pipeline for default topic
python3 run-pipeline.py

# Run for specific topic
python3 run-pipeline.py --topic minneapolis-ice

# Collection only (skip cleaning)
python3 run-pipeline.py --topic minneapolis-ice --collect-only

# Cleaning only
python3 run-pipeline.py --topic minneapolis-ice --clean-only

# Automated mode (no prompts)
python3 run-pipeline.py --topic minneapolis-ice --auto
```

## Directory Structure

```
mina-pipeline/
├── run-pipeline.py         # Main entry point
├── config.py               # Topic configurations (queries, outlets, gist IDs)
├── manifest.py             # Manifest utilities for tracking coverage
├── clean.py                # Data cleaning script
├── collect/
│   ├── mcloud-fetch-urls.py    # Fetch URLs from MediaCloud
│   ├── scrape-articles.py      # Scrape article metadata
│   └── mcloud_setup.py         # MediaCloud client setup
├── raw/
│   └── {topic}/
│       └── {date}/
│           ├── urls.jsonl      # MediaCloud URLs
│           └── articles.jsonl  # Scraped article metadata
├── clean/
│   └── articles-{topic}.jsonl  # Cleaned output
└── .github/workflows/
    └── fetch.yml               # GitHub Actions workflow
```

## Topics

Topics are configured in `config.py`. Each topic has:
- `start_date`: When to begin collecting (for backfills)
- `query`: MediaCloud search query
- `outlets`: News outlets to search
- `filter_keywords`: Keywords for filtering articles
- `gist_id_raw`: Gist ID for raw data (optional)
- `gist_id_clean`: Gist ID for cleaned data (optional)

### Adding a New Topic

1. Add topic configuration to `config.py`:
   ```python
   TOPICS = {
       "my-new-topic": {
           "start_date": date(2026, 1, 1),
           "query": "(your MediaCloud query)",
           "outlets": ALL_OUTLETS,
           "filter_keywords": ["keyword1", "keyword2"],
           "gist_id_raw": None,  # Create gist and add ID
           "gist_id_clean": None,
       },
   }
   ```

2. Optionally update `DEFAULT_TOPIC` for ad-hoc convenience

3. Create gists and add their IDs to config (if using GitHub Actions)

## Data Fields

Each article record includes:
- `url`: Original article URL
- `title`: Article title
- `description`: Article description/summary
- `media_url`: News outlet domain
- `publish_date`: Publication date
- `my_topic`: Topic identifier (for filtering combined datasets)

## Manifest Tracking

The pipeline tracks collection coverage to ensure no gaps:

```json
{
  "_manifest": true,
  "topic": "minneapolis-ice",
  "coverage": {
    "start_date": "2026-01-01",
    "end_date": "2026-01-25",
    "last_updated": "2026-01-25T14:36:00Z"
  },
  "dates_collected": ["2026-01-01", "2026-01-02", ...],
  "daily_runs": {
    "2026-01-25": {"count": 48, "first": "00:06:00Z", "last": "23:36:00Z"}
  },
  "record_count": 5432,
  "gaps": []
}
```

## GitHub Actions

The workflow runs via cron-job.org every 30 minutes:

```bash
# Manually trigger via GitHub CLI
gh workflow run fetch.yml -f topic=minneapolis-ice
```

## Environment Variables

Required:
- `MEDIACLOUD_API_KEY`: MediaCloud API key

For GitHub Actions:
- `GIST_PAT`: GitHub token with gist write permissions

## Ad-hoc vs Automated Runs

| Mode | How | Topic Selection |
|------|-----|-----------------|
| Ad-hoc | `python3 run-pipeline.py` | Uses `DEFAULT_TOPIC` from config |
| Automated | GitHub Actions | Always pass `--topic` explicitly |

This separation ensures ad-hoc runs can't accidentally affect automated collection.
