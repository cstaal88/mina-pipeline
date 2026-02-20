#!/usr/bin/env python3
import json
from urllib.parse import urlparse
from collections import Counter

GIST='/tmp/gist-raw.jsonl'
COMBINED='mediacloud/raw/minneapolis-ice/_combined.jsonl'
OUT_MERGED='/tmp/gist-merged.jsonl'
OUT_REPORT='gists/ingest-report.json'

def load_jsonl(path, skip_meta=True):
    items=[]
    with open(path,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                obj=json.loads(line)
            except Exception:
                continue
            if skip_meta and isinstance(obj, dict) and obj.get('_meta'):
                continue
            items.append(obj)
    return items

gist_items = load_jsonl(GIST)
combined_items = load_jsonl(COMBINED)

gist_count = len(gist_items)
combined_count = len(combined_items)

gist_ids = set(i.get('id') for i in gist_items if i.get('id'))
combined_ids = [i.get('id') for i in combined_items]
combined_ids_set = set(x for x in combined_ids if x)

# dedupe by id: if combined id in gist_ids -> skipped
already_present_ids = combined_ids_set & gist_ids
skipped_by_id = len(already_present_ids)
to_add_by_id = len(combined_ids_set - gist_ids)

# Create merged file
print(f"Creating merged file: {OUT_MERGED}")
with open(OUT_MERGED, 'w', encoding='utf-8') as f:
    # Write all existing gist entries
    for item in gist_items:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    # Write only new entries from combined (not in gist by ID)
    added_count = 0
    for item in combined_items:
        item_id = item.get('id')
        if item_id and item_id not in gist_ids:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
            added_count += 1

print(f"Added {added_count} new entries to merged file")

# URLs info
gist_urls = set(i.get('url') for i in gist_items if i.get('url'))
combined_urls = [i.get('url') for i in combined_items if i.get('url')]
combined_urls_set = set(combined_urls)
url_overlap = len(gist_urls & combined_urls_set)

# domain breakdown for additions (by URL domain)
add_ids = combined_ids_set - gist_ids
add_domain_counter = Counter()
for i in combined_items:
    if not i.get('id'):
        # treat as add
        url=i.get('url')
        if url:
            dom=urlparse(url).netloc.lower()
            add_domain_counter[dom]+=1
    else:
        if i.get('id') in add_ids:
            url=i.get('url')
            if url:
                dom=urlparse(url).netloc.lower()
                add_domain_counter[dom]+=1

report = {
    'operation': 'completed_ingestion',
    'output_file': OUT_MERGED,
    'gist_existing_entries': gist_count,
    'combined_total_entries': combined_count,
    'combined_unique_ids': len(combined_ids_set),
    'added_entries': added_count,
    'skipped_by_id': skipped_by_id,
    'final_total_entries': gist_count + added_count,
    'removed_from_gist': 0,
    'gist_unique_urls': len(gist_urls),
    'combined_unique_urls': len(combined_urls_set),
    'url_overlap_count': url_overlap,
    'top_domains_for_adds': add_domain_counter.most_common(10),
}

with open(OUT_REPORT,'w',encoding='utf-8') as f:
    json.dump(report,f,indent=2)

print(f"Wrote report: {OUT_REPORT}")
print(f"Wrote merged file: {OUT_MERGED}")
print(f"Final stats: {gist_count} existing + {added_count} added = {gist_count + added_count} total entries")
print(json.dumps(report,indent=2))
