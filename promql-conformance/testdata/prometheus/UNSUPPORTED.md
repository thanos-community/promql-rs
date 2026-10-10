# What the engine is missing

Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

Nothing here gates CI. It is a roadmap: every row is a count of Prometheus's own promqltest evals that one missing feature blocks, so the top of the table is the cheapest coverage available.

Against the vendored corpus: **660 of 1825 evals pass** (36.2%), and **322** are blocked on the features below.

## Missing features

| evals blocked | feature |
|---:|---|
| 86 | the histogram_quantile function |
| 60 | a subquery |
| 56 | the histogram_fraction function |
| 21 | the label_replace function |
| 20 | a unary operator |
| 19 | the topk aggregation |
| 9 | the predict_linear function |
| 9 | the quantile_over_time function |
| 7 | the bottomk aggregation |
| 7 | the label_join function |
| 6 | the quantile aggregation |
| 4 | the deriv function |
| 4 | the stddev_over_time function |
| 3 | the stdvar_over_time function |
| 2 | a range selector |
| 2 | the histogram_count function |
| 2 | the histogram_sum function |
| 1 | the changes function over a parenthesized expression |
| 1 | the histogram_avg function |
| 1 | the histogram_stddev function |
| 1 | the histogram_stdvar function |
| 1 | the rate function over a parenthesized expression |

## Left out: experimental syntax

**273** more evals use syntax that stock Prometheus refuses unless an `--enable-feature` value is on. They are not part of the core language yet, so they are in no total above, in no row of the tables, and not in `SUPPORTED.toml`. The harness still evaluates them with every gate open; `docs/feature-flags.md` lists the flags.

| evals | needs |
|---:|---|
| 141 | `promql-experimental-functions` |
| 88 | `promql-extended-range-selectors` |
| 44 | `promql-binop-fill-modifiers` |

## By file

| file | evals | passing | |
|---|---:|---:|---|
| selectors | 31 | 31 (100%) | fully green |
| extended_vectors | 30 | 30 (100%) | fully green |
| literals | 25 | 25 (100%) | fully green |
| trig_functions | 19 | 19 (100%) | fully green |
| staleness | 17 | 17 (100%) | fully green |
| collision | 2 | 2 (100%) | fully green |
| fill-modifier | 1 | 1 (100%) | fully green |
| range_queries | 16 | 11 (69%) |  |
| type_and_unit | 58 | 39 (67%) |  |
| operators | 213 | 132 (62%) |  |
| aggregators | 158 | 93 (59%) |  |
| name_label_dropping | 29 | 16 (55%) |  |
| functions | 383 | 193 (50%) |  |
| at_modifier | 69 | 33 (48%) |  |
| duration_expression | 59 | 8 (14%) |  |
| subquery | 34 | 2 (6%) |  |
| histograms | 167 | 8 (5%) |  |
| native_histograms | 514 | 0 (0%) |  |
| info | 0 | 0 (0%) | all gated — experimental syntax |
| limit | 0 | 0 (0%) | all gated — experimental syntax |

---

Some failures are one cause wearing many hats — a single lexer gap can account for hundreds of rows above. Read a handful before picking, with:

```sh
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/
```
