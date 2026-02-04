#!/usr/bin/env python3
"""
Debug helper script - shows all pipeline config info in one place.
Run with: python gists/debug-pipeline.py
"""

import subprocess
import json
import sys
import re
from pathlib import Path

WORKSPACE = Path(__file__).parent.parent
UNIFIED_GIST_ID = "16c75a94d276d2800a44e3c2437f40e4"

def header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def run_cmd(cmd):
    """Run shell command and return output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return result.stdout.strip(), result.returncode
    except Exception as e:
        return str(e), 1

def main():
    print("MINA Pipeline Debug Info")
    print("="*60)
    
    # 1. Check gist contents
    header("UNIFIED GIST FILES")
    print(f"Gist ID: {UNIFIED_GIST_ID}")
    out, code = run_cmd(f"gh gist view {UNIFIED_GIST_ID} --files")
    if code == 0:
        files = out.strip().split('\n')
        print(f"Files ({len(files)}):")
        for f in files:
            print(f"  - {f}")
    else:
        print(f"Error: {out}")
    
    # 2. Check gist ID references in codebase
    header("GIST ID REFERENCES IN CODE")
    out, _ = run_cmd(f"grep -r '16c75a94d276d2800a44e3c2437f40e4' {WORKSPACE} --include='*.py' --include='*.yml' --include='*.yaml' -l 2>/dev/null")
    if out:
        for f in out.split('\n'):
            if f:
                rel = Path(f).relative_to(WORKSPACE) if f.startswith(str(WORKSPACE)) else f
                print(f"  ✓ {rel}")
    
    # Check for other gist IDs that might be stale
    out, _ = run_cmd(f"grep -rE 'GIST_ID|gist_id|gist.github.com/[a-f0-9]{{32}}' {WORKSPACE} --include='*.py' --include='*.yml' -h 2>/dev/null | grep -v '16c75a94d276d2800a44e3c2437f40e4' | head -20")
    if out.strip():
        print("\n  ⚠️  Other gist references (may be stale):")
        for line in out.strip().split('\n')[:10]:
            print(f"    {line.strip()[:80]}")
    
    # 3. Check config.py outlets
    header("ACTIVE OUTLETS (config.py)")
    config_path = WORKSPACE / "mediacloud" / "config.py"
    if config_path.exists():
        content = config_path.read_text()
        # Find ALL_OUTLETS dict
        in_outlets = False
        active = []
        commented = []
        for line in content.split('\n'):
            if 'ALL_OUTLETS' in line and '=' in line:
                in_outlets = True
                continue
            if in_outlets:
                if line.strip() == '}':
                    break
                if '":' in line:
                    domain = line.split('"')[1]
                    if line.strip().startswith('#'):
                        commented.append(domain)
                    else:
                        active.append(domain)
        print(f"Active outlets ({len(active)}): {', '.join(active) or 'NONE!'}")
        print(f"Commented out ({len(commented)}): {len(commented)} outlets")
    
    # 4. Check workflow days setting
    header("WORKFLOW CONFIG (.github/workflows/mcloud-pipeline.yml)")
    wf_path = WORKSPACE / ".github" / "workflows" / "mcloud-pipeline.yml"
    if wf_path.exists():
        content = wf_path.read_text()
        # Find --days
        match = re.search(r'--days\s+(\d+)', content)
        if match:
            print(f"Days window: {match.group(1)}")
        # Find topics
        match = re.search(r'TOPICS="([^"]+)"', content)
        if match:
            print(f"Topics: {match.group(1)}")
        # Find raw file pattern
        if 'mediacloud_raw_${TOPIC}' in content:
            print("Raw file pattern: mediacloud_raw_{topic}.jsonl (per-topic)")
        elif 'mediacloud_raw.jsonl' in content:
            print("Raw file pattern: mediacloud_raw.jsonl (shared)")
    
    # 5. Check for checkpoint files
    header("CHECKPOINT FILES (should be empty/ignored)")
    out, _ = run_cmd(f"find {WORKSPACE} -name '.fetch-checkpoint*.json' -o -name '.scrape-checkpoint*.json' 2>/dev/null")
    if out.strip():
        for f in out.strip().split('\n'):
            if f:
                rel = Path(f).relative_to(WORKSPACE)
                print(f"  ⚠️  {rel}")
    else:
        print("  None found (good!)")
    
    # 6. Check gh gist edit syntax
    header("GH GIST COMMANDS")
    out, _ = run_cmd("gh gist edit --help 2>&1 | grep -A2 'add-filename'")
    if 'add-filename' in out:
        print("gh gist edit -a/--add-filename: YES (can add new files)")
    else:
        print("gh gist edit -a: checking...")
        out, _ = run_cmd("gh gist edit --help 2>&1")
        if '-a' in out or 'add' in out.lower():
            print("  Has -a flag for adding files")
        else:
            print("  ⚠️  May not support adding new files directly")
            print("  Workaround: create file first, then edit")
    
    # 7. Quick test: can we add a file to gist?
    header("GIST WRITE TEST")
    print("To add a new file to the gist, you can use:")
    print(f"  echo 'test' > /tmp/test.txt && gh gist edit {UNIFIED_GIST_ID} -a /tmp/test.txt")
    print("  (then delete with: gh gist edit {UNIFIED_GIST_ID} -d test.txt)")
    
    # 8. Local raw data
    header("LOCAL RAW DATA")
    raw_dir = WORKSPACE / "mediacloud" / "raw"
    if raw_dir.exists():
        for topic_dir in sorted(raw_dir.iterdir()):
            if topic_dir.is_dir():
                combined = topic_dir / "_combined.jsonl"
                if combined.exists():
                    lines = len(combined.read_text().strip().split('\n')) if combined.stat().st_size > 0 else 0
                    print(f"  {topic_dir.name}/_combined.jsonl: {lines} lines")
                else:
                    date_dirs = [d for d in topic_dir.iterdir() if d.is_dir()]
                    print(f"  {topic_dir.name}/: {len(date_dirs)} date folders (no _combined.jsonl)")
    else:
        print("  No raw directory found")
    
    header("SUMMARY")
    print("Expected gist files (and only these):")
    print("  - raw.jsonl (RSS data)")
    print("  - clean-minneapolis-ice.jsonl")
    print("  - clean-greenland-trump.jsonl") 
    print("  - mediacloud_raw.jsonl (shared, all topics)")

if __name__ == "__main__":
    main()
