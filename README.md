# MINA News Data Pipeline

Two pipelines for collecting news:
- **getnews/** - RSS-based collection (primary, runs daily)
- **mediacloud/** - MediaCloud API collection (for historical backfill)

## Quick Start (RSS Pipeline)

```bash
cd getnews

# Fetch stories from all RSS feeds
python3 fetch-raw.py

# Filter by topic and format for output
python3 clean.py minneapolis-ice

# Push to gist (after testing)
python3 clean.py minneapolis-ice --push
```

## Quick Start (MediaCloud Pipeline)

```bash
cd mediacloud

# List available topics
python3 run-pipeline.py --list-topics

# Run full pipeline for specific topic
python3 run-pipeline.py --topic minneapolis-ice

# Automated mode (no prompts)
python3 run-pipeline.py --topic minneapolis-ice --auto
```

## Directory Structure

```
mina-pipeline/
├── getnews/                    # RSS-based pipeline (primary)
│   ├── config.py               # Outlets, topics, keywords
│   ├── fetch-raw.py            # Fetch from RSS feeds
│   └── clean.py                # Filter and upload to gists
├── mediacloud/                 # MediaCloud pipeline (backfill)
│   ├── run-pipeline.py         # Main entry point
│   ├── config.py               # Topic configurations
│   ├── clean.py                # Data cleaning
│   ├── manifest.py             # Coverage tracking
│   └── collect/                # MediaCloud fetch scripts
│       ├── mcloud-fetch-urls.py
│       ├── scrape-articles.py
│       └── mcloud_setup.py
├── gists/                      # Gist tools and docs
└── .github/workflows/
    └── fetch.yml               # GitHub Actions workflow
```

## Topics

Topics are configured in `getnews/config.py` (RSS) and `mediacloud/config.py` (MediaCloud). Each topic has:
- `start_date`: When to begin collecting (for backfills)
- `query`: MediaCloud search query
- `outlets`: News outlets to search
- `filter_keywords`: Keywords for filtering articles
- `gist_id_raw`: Gist ID for raw data (optional)
- `gist_id_clean`: Gist ID for cleaned data (optional)

### Adding a New Topic

1. Add topic configuration to both `getnews/config.py` and `mediacloud/config.py`:
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
