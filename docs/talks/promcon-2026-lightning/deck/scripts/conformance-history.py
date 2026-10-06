#!/usr/bin/env python3
"""Write the deck's conformance history: the pass count at every state of the
promqltest allowlist that the mainline held.

SUPPORTED.toml and UNSUPPORTED.md are blessed together with the code, so each
commit that changed them is a candidate point, and a point's numbers are the
ones that commit's UNSUPPORTED.md states, read with scoreboard.py's parser.

The walk is --first-parent from HEAD, so a pull request is one point: its
merge commit, which carries the state it merged. The blessing commits on a PR
branch are not points. Each was blessed against that branch's own base, and
parallel PRs re-bless after rebasing onto each other, so a branch commit's
count can describe a tree main never held.

A commit whose UNSUPPORTED.md is absent or unreadable is left out, never
interpolated, and named on stderr. SUPPORTED.toml cannot stand in: a file marked
"all" counts as many evals as that commit's corpus held, which the allowlist
does not say.
"""

import json
import os
import re
import subprocess
import sys

import scoreboard

ALLOWLIST = "promql-conformance/testdata/prometheus/SUPPORTED.toml"
OUT = scoreboard.OUT.with_name("conformance-history.json")


def git(*args):
    return subprocess.run(
        ["git", *args], cwd=scoreboard.ROOT, capture_output=True, text=True, check=True
    ).stdout


def main():
    # %b's first line is the PR title GitHub writes into a merge commit.
    log = git(
        "log", "--first-parent", "--reverse", "--format=%H%x00%h%x00%cs%x00%s%x00%b%x01",
        "HEAD", "--", ALLOWLIST, scoreboard.SOURCE,
    )
    points = []
    for record in filter(str.strip, log.split("\x01")):
        sha, short, date, subject, body = record.strip("\n").split("\0")
        merged = re.match(r"Merge pull request #(\d+)", subject)
        squashed = re.search(r"\(#(\d+)\)$", subject)
        pr = merged or squashed
        title = body.strip().split("\n")[0] if merged and body.strip() else subject

        shown = subprocess.run(
            ["git", "show", f"{sha}:{scoreboard.SOURCE}"],
            cwd=scoreboard.ROOT, capture_output=True, text=True,
        )
        try:
            if shown.returncode:
                raise ValueError("no UNSUPPORTED.md")
            passing, total, _ = scoreboard.counts(shown.stdout)
        except (ValueError, KeyError) as e:
            print(f"left out {short} {subject!r}: {e}", file=sys.stderr)
            continue
        points.append({
            "sha": short, "date": date, "subject": title,
            "pr": int(pr.group(1)) if pr else None,
            "passing": passing, "total": total,
        })

    tmp = OUT.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(points, indent=2) + "\n")
    os.replace(tmp, OUT)


if __name__ == "__main__":
    main()
