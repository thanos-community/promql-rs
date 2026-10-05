#!/usr/bin/env python3
"""Write the deck's conformance history: the pass count at every commit that
re-blessed the promqltest allowlist.

SUPPORTED.toml and UNSUPPORTED.md are blessed together with the code, so each
commit touching them is a point, and each point's numbers are the ones that
commit's UNSUPPORTED.md states, read with scoreboard.py's parser.

Every commit is walked, not --first-parent: the blessing commits form one
ancestry chain, so each count includes the one before it, and a merge-only walk
would fold several blessings into one point. The script refuses history where
that stops holding, because a side branch's count is relative to its own base
and would zigzag on an axis that reads as progress.

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


def merged_by():
    """Commit -> number of the pull request whose merge brought it onto HEAD's
    first-parent line; a commit's own subject carries no PR number."""
    prs = {}
    for line in git("log", "--first-parent", "--merges", "--format=%H %s", "HEAD").splitlines():
        sha, subject = line.split(" ", 1)
        m = re.match(r"Merge pull request #(\d+)", subject)
        if m:
            for c in git("rev-list", f"{sha}^1..{sha}^2").split():
                prs[c] = int(m.group(1))
    return prs


def main():
    log = git(
        "log", "--topo-order", "--reverse", "--format=%H%x00%h%x00%cs%x00%s",
        "HEAD", "--", ALLOWLIST, scoreboard.SOURCE,
    )
    commits = [line.split("\0") for line in log.splitlines()]
    for (a, *_), (b, *_) in zip(commits, commits[1:]):
        if subprocess.run(["git", "merge-base", "--is-ancestor", a, b], cwd=scoreboard.ROOT).returncode:
            sys.exit(f"{a[:7]} is not an ancestor of {b[:7]}: the blessings no longer form one line")

    prs = merged_by()
    points = []
    for sha, short, date, subject in commits:
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
            "sha": short, "date": date, "subject": subject, "pr": prs.get(sha),
            "passing": passing, "total": total,
        })

    tmp = OUT.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(points, indent=2) + "\n")
    os.replace(tmp, OUT)


if __name__ == "__main__":
    main()
