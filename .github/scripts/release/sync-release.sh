#!/usr/bin/env bash
# Brings develop's public tree onto the `release` branch, append-only, keeping develop's
# commits (and their authors) in the history that reaches main.
#
# Usage: sync-release.sh [--push]
#
# Run inside a full clone (fetch-depth: 0) that has origin/develop. Environment:
#   SOURCE  branch to publish from (default: develop)
#   TARGET  branch to write (default: release)
#   MAIN    branch the release PR targets (default: main)
#
# Approach: a commit built with plumbing, never a checkout. develop's tree is read into a
# temporary index (GIT_INDEX_FILE), the internal documents are dropped from that index, and
# `git commit-tree` writes the commit. The working tree and the real index are not touched.
#
# The release commit's parents, in order:
#   1. the previous release tip (linear first-parent history; absent on the first run);
#   2. origin/main, only when main is not already an ancestor of the release tip, so a
#      release -> main pull request never conflicts, whether main took the previous PR by
#      merge commit, squash or rebase (main's tree is not used);
#   3. origin/develop, so every develop commit is an ancestor of release and, once the PR
#      merges, of main: the original authors stay visible on main.
#
# Guarantees:
#   - Never force-pushes and never rewrites history; `git push` is always a fast-forward.
#   - Internal planning documents are dropped: PROJECT.md, AGENTS.md, CLAUDE.md,
#     docs/INTENT.md and docs/SPEC.md. The public references and docs/guide/ are kept.
#   - No commit when the tree is unchanged AND main is already absorbed AND develop is
#     already an ancestor of release.
#   - Prints `changed=true|false` lines for the caller (also appended to $GITHUB_OUTPUT).
set -euo pipefail

push=0
[[ "${1:-}" == "--push" ]] && push=1
SOURCE="${SOURCE:-develop}"
TARGET="${TARGET:-release}"
MAIN="${MAIN:-main}"

out() { echo "$1"; [[ -n "${GITHUB_OUTPUT:-}" ]] && echo "$1" >> "$GITHUB_OUTPUT" || true; }
resolve() { git rev-parse --verify -q "refs/remotes/origin/$1^{commit}" || true; }

src="$(resolve "$SOURCE")"
[[ -n "$src" ]] || { echo "error: origin/$SOURCE not found" >&2; exit 1; }
tip="$(resolve "$TARGET")"
main="$(resolve "$MAIN")"

# main joins only when the release tip does not contain it yet.
if [[ -n "$main" && -n "$tip" ]] && git merge-base --is-ancestor "$main" "$tip"; then
  main=""
fi

# develop's tree minus the internal documents, in a scratch index.
private=(PROJECT.md AGENTS.md CLAUDE.md docs/INTENT.md docs/SPEC.md)
tmp_index="$(mktemp -u "${TMPDIR:-/tmp}/release-index.XXXXXX")"
trap 'rm -f "$tmp_index"' EXIT
tree="$(
  export GIT_INDEX_FILE="$tmp_index"
  git read-tree "$src"
  git rm -r --cached --force --quiet --ignore-unmatch -- "${private[@]}"
  git write-tree
)"

# Nothing to record: same tree, main absorbed, develop already in history.
if [[ -n "$tip" && -z "$main" && "$(git rev-parse "$tip^{tree}")" == "$tree" ]] \
   && git merge-base --is-ancestor "$src" "$tip"; then
  echo "$TARGET is already up to date with $SOURCE"
  out "changed=false"
  exit 0
fi

parents=()
[[ -n "$tip" ]] && parents+=(-p "$tip")
[[ -n "$main" ]] && parents+=(-p "$main")
parents+=(-p "$src")

excluded="$(printf '  %s\n' "${private[@]}")"
new="$(git commit-tree "$tree" "${parents[@]}" \
  -m "Publish: $SOURCE $(git rev-parse --short "$src") to $TARGET" \
  -m "Internal planning documents kept on $SOURCE only:
$excluded")"
git update-ref "refs/heads/$TARGET" "$new"
out "changed=true"

if (( push )); then
  git push origin "refs/heads/$TARGET:refs/heads/$TARGET"   # plain push: non fast-forward fails
fi
