#!/bin/bash

# - - - - - - - - - - - - Create minneapolis-ice gist with both files
echo '{}' > /tmp/raw.jsonl
echo '{}' > /tmp/clean.jsonl
gh gist create --filename "raw.jsonl" /tmp/raw.jsonl --filename "clean.jsonl" /tmp/clean.jsonl -d "minneapolis-ice news data"
#minneapolis: https://gist.githubusercontent.com/cstaal88/839f9f409d36d715d277095886ced536/raw/clean.jsonl


# - - - - - - - - - - - -  trump / greenland
echo '{}' > /tmp/raw.jsonl
echo '{}' > /tmp/clean.jsonl
gh gist create --filename "raw.jsonl" /tmp/raw.jsonl --filename "clean.jsonl" /tmp/clean.jsonl -d "greenland-trump news data"
#trump/ greenland: https://gist.githubusercontent.com/cstaal88/a046f4a9233ff2e499dfeb356e081d79/raw/clean.jsonl