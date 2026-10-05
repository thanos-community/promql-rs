#!/usr/bin/env python3
"""Write the deck's conformance scoreboard from the promqltest roadmap.

UNSUPPORTED.md is regenerated with SUPPORTED.toml on every bless, so reading
it here keeps the slide's numbers equal to the committed pass count. The
counts come from its two tables; the headline sentence above them is only a
cross-check, and a disagreement fails the run instead of picking one side.
"""

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
SOURCE = "promql-conformance/testdata/prometheus/UNSUPPORTED.md"
OUT = Path(__file__).resolve().parents[1] / "src" / "data" / "scoreboard.json"

HEADLINE = re.compile(
    r"\*\*(\d+) of (\d+) evals pass\*\*.*?\*\*(\d+)\*\* are blocked", re.S
)


def tables(text):
    """The table under each `## heading`, as rows of stripped cells."""
    found = {}
    heading = None
    for line in text.splitlines():
        if line.startswith("## "):
            heading = line[3:].strip()
        elif heading and line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            found.setdefault(heading, []).append(cells)
    # Drop each table's header and |---| rule.
    return {h: rows[2:] for h, rows in found.items()}


def short_name(feature):
    """The roadmap's row text as a slide label: `the histogram_quantile
    function` is `histogram_quantile`, `a subquery` is `subqueries`."""
    m = re.fullmatch(r"the (\S+) (?:function|aggregation)", feature)
    if m:
        return m.group(1)
    m = re.fullmatch(r"an? (.+)", feature)
    if m:
        noun = m.group(1)
        if re.search(r"[^aeiou]y$", noun):
            return noun[:-1] + "ies"
        return noun + "s"
    return feature


def passing_count(cell):
    # "559 (26%)": the percentage is rounded, the count is not.
    return int(cell.split()[0])


def main():
    text = (ROOT / SOURCE).read_text()
    t = tables(text)
    missing = [(int(n), feature) for n, feature in t["Missing features"]]
    by_file = t["By file"]

    total = sum(int(row[1]) for row in by_file)
    passing = sum(passing_count(row[2]) for row in by_file)
    blocked = sum(n for n, _ in missing)

    head = HEADLINE.search(text)
    if not head:
        sys.exit(f"{SOURCE}: no headline to cross-check the tables against")
    stated = tuple(int(g) for g in head.groups())
    if stated != (passing, total, blocked):
        sys.exit(
            f"{SOURCE}: headline says {stated[0]} of {stated[1]} pass, "
            f"{stated[2]} blocked; tables sum to {passing} of {total}, {blocked}"
        )

    # sorted() is stable, so a tie keeps the roadmap's own order.
    top = sorted(missing, key=lambda row: -row[0])[:3]
    board = {
        "passing": passing,
        "total": total,
        "percent": round(100 * passing / total, 1),
        "blocked_total": blocked,
        "top_blockers": [
            {"name": short_name(feature), "feature": feature, "count": n}
            for n, feature in top
        ],
        "source": SOURCE,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(board, indent=2) + "\n")
    os.replace(tmp, OUT)


if __name__ == "__main__":
    main()
