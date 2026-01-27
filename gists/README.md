# Gist Tools

## gist-overview.py
**Purpose:** Check current state of data  
```bash
python3 gists/gist-overview.py
```
Shows: record counts, date ranges, stories per date/outlet, mean summary length

## gist-history.py
**Purpose:** Browse/recover past versions  
```bash
# List recent revisions
python3 gists/gist-history.py --topic minneapolis-ice

# Preview a specific revision
python3 gists/gist-history.py -t minneapolis-ice -r 2 -f raw.jsonl

# Restore from old revision
python3 gists/gist-history.py -t minneapolis-ice -r 2 -f raw.jsonl --restore
```
