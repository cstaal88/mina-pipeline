#!/usr/bin/env python3
"""
Merge all historical versions of mediacloud_raw.jsonl into single deduplicated file.
Deduplicates by URL, keeping the most complete record for each URL.
"""

import json
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Configuration
HISTORY_DIR = Path(__file__).parent / "mediacloud-history"
OUTPUT_FILE = Path(__file__).parent / "mediacloud-raw-merged.jsonl"

def count_fields(record):
    """Count non-empty fields in a record to determine completeness."""
    count = 0
    for key, value in record.items():
        if value:  # Not None, not empty string, not empty list
            count += 1
    return count

def merge_histories():
    """Merge all historical versions, deduplicating by URL."""
    
    print("=" * 70)
    print("MERGE MEDIACLOUD HISTORY")
    print("=" * 70)
    print()
    
    if not HISTORY_DIR.exists():
        print(f"❌ History directory not found: {HISTORY_DIR}")
        return
    
    # Get all history files, sorted by filename (newest first)
    history_files = sorted(HISTORY_DIR.glob("*.jsonl"))
    
    if not history_files:
        print(f"❌ No JSONL files found in {HISTORY_DIR}")
        return
    
    print(f"Found {len(history_files)} history files")
    print()
    
    # Track records by URL
    # Key: URL, Value: (record, source_file, field_count)
    records_by_url = {}
    
    # Track stats
    total_records_read = 0
    files_with_records = 0
    empty_files = 0
    
    # Read all files
    print("Reading history files...")
    for i, filepath in enumerate(history_files):
        with open(filepath, 'r', encoding='utf-8') as f:
            file_records = [json.loads(line) for line in f if line.strip()]
        
        total_records_read += len(file_records)
        
        if len(file_records) > 0:
            files_with_records += 1
        else:
            empty_files += 1
        
        # Process each record
        for record in file_records:
            url = record.get('url')
            if not url:
                continue
            
            # If we haven't seen this URL, add it
            if url not in records_by_url:
                records_by_url[url] = (record, filepath.name, count_fields(record))
            else:
                # Keep the record with more fields (more complete)
                existing_record, existing_file, existing_count = records_by_url[url]
                new_count = count_fields(record)
                
                if new_count > existing_count:
                    records_by_url[url] = (record, filepath.name, new_count)
        
        # Progress update every 50 files
        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(history_files)} files...")
    
    print(f"✓ Processed all {len(history_files)} files")
    print()
    
    # Prepare final records
    final_records = [record for record, _, _ in records_by_url.values()]
    
    # Sort by published_date (newest first)
    final_records.sort(
        key=lambda r: r.get('published_date', ''),
        reverse=True
    )
    
    # Write merged file
    print(f"Writing merged file: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for record in final_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"✓ Wrote {len(final_records)} records")
    print()
    
    # Print statistics
    print("=" * 70)
    print("STATISTICS")
    print("=" * 70)
    print()
    print(f"Files processed:        {len(history_files)}")
    print(f"  - With records:       {files_with_records}")
    print(f"  - Empty:              {empty_files}")
    print()
    print(f"Total records read:     {total_records_read:,}")
    print(f"Unique URLs:            {len(records_by_url):,}")
    print(f"Duplicates removed:     {total_records_read - len(records_by_url):,}")
    print()
    
    # Date range
    if final_records:
        dates = [r.get('published_date') for r in final_records if r.get('published_date')]
        if dates:
            dates.sort()
            print(f"Date range:             {dates[0]} → {dates[-1]}")
    
    # Source distribution
    print()
    print("Records by source:")
    sources = defaultdict(int)
    for record in final_records:
        source = record.get('source', 'unknown')
        sources[source] += 1
    
    for source in sorted(sources.keys()):
        print(f"  {source:20s} {sources[source]:4d} records")
    
    print()
    print(f"Output saved to: {OUTPUT_FILE}")
    print()

if __name__ == "__main__":
    merge_histories()
