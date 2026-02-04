#!/bin/bash
# test-pipeline-local.sh
# Test the MediaCloud pipeline locally before pushing to GitHub Actions
#
# Usage: ./test-pipeline-local.sh
#
# Prerequisites:
#   - MEDIACLOUD_API_KEY env var set
#   - gh CLI authenticated
#   - Python deps installed (mediacloud, requests, beautifulsoup4, etc.)

set -e  # Exit on error

GIST_ID="16c75a94d276d2800a44e3c2437f40e4"
TOPIC="minneapolis-ice"

echo "=============================================="
echo "  LOCAL PIPELINE TEST"
echo "  Topic: $TOPIC"
echo "  Started: $(date)"
echo "=============================================="

# Check prerequisites
echo ""
echo "=== Step 0: Checking prerequisites ==="
if [ -z "$MEDIACLOUD_API_KEY" ]; then
    echo "❌ MEDIACLOUD_API_KEY not set"
    echo "   Run: export MEDIACLOUD_API_KEY=your_key"
    exit 1
fi
echo "✓ MEDIACLOUD_API_KEY is set"

if ! gh auth status &>/dev/null; then
    echo "❌ gh CLI not authenticated"
    exit 1
fi
echo "✓ gh CLI authenticated"

# Step 1: Download existing raw from gist
echo ""
echo "=== Step 1: Download existing mediacloud_raw.jsonl from gist ==="
mkdir -p mediacloud/raw
SHARED_RAW="mediacloud/raw/_combined.jsonl"

if gh gist view "$GIST_ID" -f "mediacloud_raw.jsonl" > "$SHARED_RAW" 2>/dev/null; then
    EXISTING=$(wc -l < "$SHARED_RAW" | tr -d ' ')
    echo "✓ Downloaded $EXISTING existing records"
else
    echo "⚠ No existing mediacloud_raw.jsonl (starting fresh)"
    touch "$SHARED_RAW"
fi

# Step 2: Copy to topic folder
echo ""
echo "=== Step 2: Setup topic folder ==="
RAW_DIR="mediacloud/raw/${TOPIC}"
mkdir -p "$RAW_DIR"
cp "$SHARED_RAW" "$RAW_DIR/_combined.jsonl"
BEFORE=$(wc -l < "$RAW_DIR/_combined.jsonl" | tr -d ' ')
echo "✓ Copied $BEFORE records to $RAW_DIR/_combined.jsonl"

# Step 3: Run pipeline
echo ""
echo "=== Step 3: Run pipeline (--days 1, dailywire only) ==="
echo "   This will fetch URLs from MediaCloud and scrape articles..."
echo ""
python mediacloud/run-pipeline.py --topic "$TOPIC" --days 1

# Step 4: Check results
echo ""
echo "=== Step 4: Check results ==="
AFTER=$(wc -l < "$RAW_DIR/_combined.jsonl" | tr -d ' ')
NEW=$((AFTER - BEFORE))
echo "Before: $BEFORE records"
echo "After:  $AFTER records"
echo "New:    $NEW records"

if [ "$NEW" -gt 0 ]; then
    echo ""
    echo "Sample of new records (last 3):"
    tail -3 "$RAW_DIR/_combined.jsonl" | python -c "
import sys, json
for line in sys.stdin:
    try:
        r = json.loads(line)
        if not r.get('_meta'):
            print(f\"  - {r.get('title', 'N/A')[:60]}...\")
            print(f\"    {r.get('url', 'N/A')[:70]}\")
    except: pass
"
fi

# Step 5: Test gist upload (actually uploads)
echo ""
echo "=== Step 5: Test gist upload ==="
echo "Uploading $RAW_DIR/_combined.jsonl → mediacloud_raw.jsonl"
read -p "Proceed? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    gh gist edit "$GIST_ID" -f "mediacloud_raw.jsonl" "$RAW_DIR/_combined.jsonl"
    echo "✓ Upload successful!"
else
    echo "Skipped upload"
fi

echo ""
echo "=============================================="
echo "  TEST COMPLETE"
echo "  Finished: $(date)"
echo "=============================================="
