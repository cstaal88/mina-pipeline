#!/usr/bin/env python3
import json
from collections import defaultdict

IN='/tmp/duplicate-urls.json'
OUT='gists/duplicate-field-stats.json'

with open(IN,'r',encoding='utf-8') as f:
    data=json.load(f)

fields=defaultdict(lambda: {
    'both_equal':0,'both_different':0,'only_mediacloud':0,'only_rss':0,'examples':[]
})

total=0
for entry in data:
    total+=1
    m = entry.get('mediacloud')
    r = entry.get('rss')
    if not m or not r:
        continue
    # compare first items
    ma = m[0]
    ra = r[0]
    keys = set(ma.keys()) | set(ra.keys())
    for k in sorted(keys):
        mv = ma.get(k)
        rv = ra.get(k)
        if mv is None and rv is None:
            continue
        if mv is None:
            fields[k]['only_rss'] += 1
        elif rv is None:
            fields[k]['only_mediacloud'] += 1
        else:
            # both present
            if mv == rv:
                fields[k]['both_equal'] += 1
            else:
                fields[k]['both_different'] += 1
                if len(fields[k]['examples']) < 87:
                    fields[k]['examples'].append({'url': entry.get('url'), 'mediacloud': mv, 'rss': rv})

out = {'total_urls': total, 'num_urls_analyzed': total, 'fields': {}}
for k,v in fields.items():
    out['fields'][k]=v

with open(OUT,'w',encoding='utf-8') as f:
    json.dump(out,f,indent=2,ensure_ascii=False)

# Print concise summary
print(f"Wrote {OUT} (analyzed {total} URLs)")
for k,v in sorted(out['fields'].items(), key=lambda kv: (-(kv[1]['both_different']), kv[0])):
    be=v['both_equal']; bd=v['both_different']; om=v['only_mediacloud']; orv=v['only_rss']
    print(f"{k}: equal={be} different={bd} only_mediacloud={om} only_rss={orv}")
    if bd>0:
        for ex in v['examples']:
            print(f"  ex {ex['url']}: mediacloud={ex['mediacloud']!r} rss={ex['rss']!r}")

