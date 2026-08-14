#!/usr/bin/env bash
#
# Download the current contents of every gist on the account into the current
# directory — full files, no git history.
#
#   cd /where/you/want/the/copy
#   ~/c1/08-apps/mina-pipeline/gists/download-all-gists.sh
#
# Optionally restrict to specific gists:
#   download-all-gists.sh 16c75a94d276d2800a44e3c2437f40e4
#
# One subdirectory per gist. Each file is size-checked against the API.
#
# Two non-obvious bits:
#   - env -u GITHUB_TOKEN: a dead GITHUB_TOKEN in the environment shadows the
#     working keyring credential gh needs (4 of the 6 gists are secret).
#   - Files over ~1 MB come back truncated in the gist API JSON, so we pull
#     raw_url from the API and curl that instead of reading .content.

set -euo pipefail

gh() { command env -u GITHUB_TOKEN gh "$@"; }

if [ $# -gt 0 ]; then
  ids="$*"
else
  ids=$(gh gist list --limit 100 | cut -f1)
fi

for id in $ids; do
  desc=$(gh api "gists/$id" --jq '.description // ""')
  slug=$(printf '%s' "$desc" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -e 's/[^a-z0-9]\{1,\}/-/g' -e 's/^-//' -e 's/-$//')
  dir="${slug:-gist}-${id:0:8}"
  mkdir -p "$dir"
  echo "==> $dir"

  gh api "gists/$id" --jq '.files[] | [.filename, .raw_url, (.size|tostring)] | @tsv' \
  | while IFS=$(printf '\t') read -r name url size; do
      if ! curl -fsSL "$url" -o "$dir/$name"; then
        printf '    %-34s DOWNLOAD FAILED\n' "$name"
        continue
      fi
      got=$(wc -c < "$dir/$name" | tr -d ' ')
      if [ "$got" = "$size" ]; then
        printf '    %-34s %12s bytes  ok\n' "$name" "$got"
      else
        printf '    %-34s %12s bytes  SIZE MISMATCH (expected %s)\n' "$name" "$got" "$size"
      fi
    done
done

echo "Done."
