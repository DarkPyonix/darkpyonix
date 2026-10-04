#!/usr/bin/env bash
# Brings develop's public tree onto the `release` branch, append-only.
#
# Usage: release-sync.sh [--push]
#
# Run inside a full clone (fetch-depth: 0) that has origin/develop. Environment:
#   SOURCE  branch to publish from (default: develop)
#   TARGET  branch to write (default: release)
#   MAIN    branch the release PR targets (default: main)
#
# Guarantees:
#   - Never force-pushes and never rewrites history. Each run adds at most one commit
#     on top of the current release tip, so `git push` is always a fast-forward.
#   - release always has origin/main as an ancestor (merged with `-s ours`, then the
#     tree is replaced), so a release -> main pull request cannot conflict, whether
#     main took the previous PR by merge commit, squash or rebase.
#   - Internal planning documents are dropped: PROJECT.md, AGENTS.md, CLAUDE.md and every
#     file directly under docs/ (INTENT.md, SPEC.md, ...). docs/guide/ and other
#     subdirectories are kept, so the Pages site is published.
#   - Prints `changed=true|false` lines for the caller (also appended to $GITHUB_OUTPUT).
set -euo pipefail

push=0
[[ "${1:-}" == "--push" ]] && push=1
SOURCE="${SOURCE:-develop}"
TARGET="${TARGET:-release}"
MAIN="${MAIN:-main}"

out() { echo "$1"; [[ -n "${GITHUB_OUTPUT:-}" ]] && echo "$1" >> "$GITHUB_OUTPUT" || true; }

git rev-parse --verify -q "refs/remotes/origin/$SOURCE" >/dev/null \
  || { echo "error: origin/$SOURCE not found" >&2; exit 1; }
src="$(git rev-parse "refs/remotes/origin/$SOURCE")"

have_main=0; have_target=0
git rev-parse --verify -q "refs/remotes/origin/$MAIN" >/dev/null && have_main=1
git rev-parse --verify -q "refs/remotes/origin/$TARGET" >/dev/null && have_target=1

# 1. Start from the current release tip, else from main, else from develop alone.
if (( have_target )); then
  git checkout -q -B "$TARGET" "origin/$TARGET"
elif (( have_main )); then
  git checkout -q -B "$TARGET" "origin/$MAIN"
else
  git checkout -q --orphan "$TARGET"
fi

# 2. Record main as merged, keeping the current content (tree is replaced below).
if (( have_main )); then
  git merge -s ours --no-commit --no-ff "origin/$MAIN" >/dev/null 2>&1 || true
fi

# 3. Replace the content with develop's tree, minus the internal documents.
git read-tree --reset -u "$src"
private=(PROJECT.md AGENTS.md CLAUDE.md)
while IFS= read -r f; do private+=("$f"); done < <(git ls-tree --name-only -r "$src" -- docs | awk -F/ 'NF==2')
git rm -q --cached --ignore-unmatch -- "${private[@]}"
for f in "${private[@]}"; do rm -f -- "$f"; done

# 4. Commit when there is something to record: a tree change or a pending main merge.
merging=0
git rev-parse -q --verify MERGE_HEAD >/dev/null && merging=1
if (( ! merging )) && git rev-parse -q --verify HEAD >/dev/null 2>&1 && git diff --cached --quiet; then
  echo "$TARGET is already up to date with $SOURCE"
  out "changed=false"
  exit 0
fi
excluded="$(printf '  %s\n' "${private[@]}")"
git commit -q -m "Publish: $SOURCE $(git rev-parse --short "$src") to $TARGET" \
  -m "Internal planning documents kept on $SOURCE only:
$excluded"
out "changed=true"

if (( push )); then
  git push origin "$TARGET"   # plain push: a non fast-forward fails instead of overwriting
fi
