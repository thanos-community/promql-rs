#!/usr/bin/env bash
# Regenerates src/data/plans.json, stepthrough.json, scoreboard.json and
# conformance-history.json from the real planner and engine and the committed
# conformance roadmap and its history.
# Run before building the deck.
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

steps="$data/stepthrough.json"
rm -f "$steps.tmp"
PROMQL_TALK_STEPTHROUGH_OUT="$steps.tmp" cargo test -p promql-engine --test talk_stepthrough
test -s "$steps.tmp" || { echo "talk_stepthrough wrote nothing to $steps.tmp" >&2; exit 1; }
mv "$steps.tmp" "$steps"

python3 "$root/docs/talks/promcon-2026-lightning/deck/scripts/scoreboard.py"
python3 "$root/docs/talks/promcon-2026-lightning/deck/scripts/conformance-history.py"

echo "wrote $plans, $steps, $data/scoreboard.json and $data/conformance-history.json"
