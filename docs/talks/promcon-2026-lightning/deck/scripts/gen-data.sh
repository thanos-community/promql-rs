#!/usr/bin/env bash
# Regenerates src/data/plans.json and src/data/scoreboard.json from the real
# planner and the committed conformance roadmap. Run before building the deck.
set -euo pipefail

root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
data="$root/docs/talks/promcon-2026-lightning/deck/src/data"
# One target dir per worktree: the conformance binary bakes its testdata path
# in at compile time, so a shared dir builds another worktree's tree.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"

mkdir -p "$data"
cd "$root"

# The test is a no-op without the variable and passes either way, so only the
# file it leaves behind says it ran. Absolute, because cargo runs the test
# from the crate directory.
plans="$data/plans.json"
rm -f "$plans.tmp"
PROMQL_TALK_PLANS_OUT="$plans.tmp" cargo test -p promql-engine --test talk_plans
test -s "$plans.tmp" || { echo "talk_plans wrote nothing to $plans.tmp" >&2; exit 1; }
mv "$plans.tmp" "$plans"

python3 "$root/docs/talks/promcon-2026-lightning/deck/scripts/scoreboard.py"

echo "wrote $plans and $data/scoreboard.json"
