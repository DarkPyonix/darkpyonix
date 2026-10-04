#!/usr/bin/env bash
# Tests for .github/scripts/release/sync-release.sh.
#
# Every case runs against throwaway repositories (a bare "origin" and a clone) under this
# repository's ignored .scratch/. Never against this repository or any real remote.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
sync="$here/sync-release.sh"

failures=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; failures=$((failures + 1)); }
check() { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1: expected [$3], got [$2]"; fi; }
check_contains() { if [[ "$2" == *"$3"* ]]; then pass "$1"; else fail "$1: [$2] does not contain [$3]"; fi; }
check_absent() { if [[ "$2" != *"$3"* ]]; then pass "$1"; else fail "$1: [$2] unexpectedly contains [$3]"; fi; }

mkdir -p "$root/.scratch"
tmp="$(mktemp -d "$root/.scratch/sync-release-test.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

# commit <repo> <author name> <message> <file> <content>
commit() {
  local repo="$1" who="$2" msg="$3" file="$4" content="$5"
  mkdir -p "$repo/$(dirname "$file")"
  echo "$content" > "$repo/$file"
  git -C "$repo" add -A
  git -C "$repo" -c user.name="$who" -c user.email="${who// /.}@example.invalid" commit -q -m "$msg"
}

# A bare origin with develop (two authors, internal docs included) and an unrelated main,
# plus a clone of it standing where the Release sync workflow runs.
make_world() {
  local w="$1"
  git init -q --bare -b develop "$w/origin.git"
  git clone -q "$w/origin.git" "$w/dev" 2>/dev/null
  git -C "$w/dev" checkout -q -b develop
  commit "$w/dev" "Alice Dev" "Feat: first" README.md readme
  commit "$w/dev" "Alice Dev" "Docs: internal" docs/SPEC.md spec
  commit "$w/dev" "Bob Dev" "Feat: code" src/lib.py code
  for f in PROJECT.md AGENTS.md CLAUDE.md docs/INTENT.md; do
    commit "$w/dev" "Bob Dev" "Docs: $f" "$f" internal
  done
  commit "$w/dev" "Bob Dev" "Docs: public" docs/ARCHITECTURE.md arch
  git -C "$w/dev" push -q origin develop
  # main: an old history that develop does not contain.
  git -C "$w/dev" checkout -q --orphan main
  git -C "$w/dev" rm -rfq --cached . && find "$w/dev" -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
  commit "$w/dev" "github-actions[bot]" "Squash of an older release" README.md old
  git -C "$w/dev" push -q origin main
  git -C "$w/dev" checkout -q develop
  git clone -q "$w/origin.git" "$w/ci" 2>/dev/null
  git -C "$w/ci" config user.name "github-actions[bot]"
  git -C "$w/ci" config user.email "bot@example.invalid"
}

run_sync() { (cd "$1" && git fetch -q origin && "$sync" --push 2>&1); }
tree_files() { git -C "$1" ls-tree -r --name-only "$2" | sort; }

w="$tmp/w1"; mkdir -p "$w"; make_world "$w"; ci="$w/ci"

# --- first sync -----------------------------------------------------------
out="$(run_sync "$ci")"; status=$?
check "first sync succeeds" "$status" "0"
check_contains "first sync reports a change" "$out" "changed=true"
git -C "$ci" fetch -q origin
rel="origin/release"
dev="origin/develop"

missing="$(for c in $(git -C "$ci" rev-list "$dev"); do
  git -C "$ci" merge-base --is-ancestor "$c" "$rel" || echo "$c"; done)"
check "fr-release: every develop commit is an ancestor of release" "$missing" ""
authors="$(git -C "$ci" log --format=%an "$rel" | sort -u)"
check_contains "fr-release: develop authors are in release history (Alice)" "$authors" "Alice Dev"
check_contains "fr-release: develop authors are in release history (Bob)" "$authors" "Bob Dev"
git -C "$ci" merge-base --is-ancestor origin/main "$rel" \
  && pass "fr-release: main is an ancestor of release" || fail "fr-release: main is not an ancestor of release"

files="$(tree_files "$ci" "$rel")"
for f in PROJECT.md AGENTS.md CLAUDE.md docs/INTENT.md docs/SPEC.md; do
  check_absent "fr-release: release tree lacks $f" "$files" "$f"
done
check_contains "fr-release: release keeps README.md" "$files" "README.md"
check_contains "fr-release: release keeps src/lib.py" "$files" "src/lib.py"
check_contains "fr-release: release keeps docs/ARCHITECTURE.md" "$files" "docs/ARCHITECTURE.md"
check "fr-release: release tree is develop's tree (README from develop, not main)" \
  "$(git -C "$ci" show "$rel:README.md")" "readme"
check "fr-release: remote release equals the pushed commit" \
  "$(git -C "$w/origin.git" rev-parse release)" "$(git -C "$ci" rev-parse "$rel")"

# --- second run with no change is a no-op ---------------------------------
before="$(git -C "$w/origin.git" rev-parse release)"
out="$(run_sync "$ci")"
check_contains "no-op: reports unchanged" "$out" "changed=false"
check "no-op: release does not move" "$(git -C "$w/origin.git" rev-parse release)" "$before"

# --- develop advances: one new commit, parents (release tip, develop) -----
commit "$w/dev" "Carol Dev" "Feat: more" src/more.py more
git -C "$w/dev" push -q origin develop
run_sync "$ci" >/dev/null
git -C "$ci" fetch -q origin
parents="$(git -C "$ci" rev-list --parents -n1 origin/release)"
check "advance: parents are (previous release, develop)" "$parents" \
  "$(git -C "$ci" rev-parse origin/release) $before $(git -C "$ci" rev-parse origin/develop)"
check_contains "advance: new author is in release history" \
  "$(git -C "$ci" log --format=%an origin/release)" "Carol Dev"
check "advance: the previous release is the first parent" \
  "$(git -C "$ci" rev-parse origin/release^1)" "$before"

# --- main moves (a squash merge release does not contain), tree unchanged -
before="$(git -C "$w/origin.git" rev-parse release)"
git -C "$w/dev" checkout -q main
commit "$w/dev" "github-actions[bot]" "Publish develop to main (#1)" README.md squashed
git -C "$w/dev" push -q origin main
git -C "$w/dev" checkout -q develop
out="$(run_sync "$ci")"
check_contains "main moved: a commit is written even though the tree is unchanged" "$out" "changed=true"
git -C "$ci" fetch -q origin
git -C "$ci" merge-base --is-ancestor origin/main origin/release \
  && pass "main moved: release absorbed the new main" || fail "main moved: release lacks the new main"
check "main moved: tree still develop's" "$(git -C "$ci" show origin/release:README.md)" "readme"
check "main moved: three parents (release, main, develop)" \
  "$(git -C "$ci" rev-list --parents -n1 origin/release | wc -w | tr -d ' ')" "4"
# and then settles
before="$(git -C "$w/origin.git" rev-parse release)"
out="$(run_sync "$ci")"
check_contains "settled: next run is a no-op" "$out" "changed=false"
check "settled: release does not move" "$(git -C "$w/origin.git" rev-parse release)" "$before"

# --- the working tree is never touched ------------------------------------
check "the checkout is still clean" "$(git -C "$ci" status --porcelain)" ""

echo
if (( failures )); then echo "$failures check(s) failed"; exit 1; fi
echo "all checks passed"
