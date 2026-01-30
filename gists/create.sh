#!/bin/bash
# Create unified gist for RSS news pipeline
# Run this once to create the gist, then add the ID to getnews/config.py

set -e

# Create placeholder files
TMPDIR=$(mktemp -d)
echo '{"_meta": true, "note": "placeholder"}' > "$TMPDIR/raw.jsonl"
echo '{"_meta": true, "note": "placeholder"}' > "$TMPDIR/clean-minneapolis-ice.jsonl"
echo '{"_meta": true, "note": "placeholder"}' > "$TMPDIR/clean-greenland-trump.jsonl"

echo "Creating unified gist..."
gh gist create \
    "$TMPDIR/raw.jsonl" \
    "$TMPDIR/clean-minneapolis-ice.jsonl" \
    "$TMPDIR/clean-greenland-trump.jsonl" \
    --desc "MINA RSS news data (unified)" \
    --public

echo ""
echo "Copy the gist ID above and add it to getnews/config.py as GIST_ID"

# Cleanup
rm -rf "$TMPDIR"


# - - - - - - - - - - - - OLD per-topic gists (for reference) - - - - - - - - -
# minneapolis-ice: 839f9f409d36d715d277095886ced536
# greenland-trump: a046f4a9233ff2e499dfeb356e081d79
