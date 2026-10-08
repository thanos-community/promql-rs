# What the engine is missing

Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

Nothing here gates CI. It is a roadmap: every row is a count of Prometheus's own promqltest evals that one missing feature blocks, so the top of the table is the cheapest coverage available.

Against the vendored corpus: **740 of 2098 evals pass** (35.3%), and **492** are blocked on the features below.

## Missing features

| evals blocked | feature |
|---:|---|
| 86 | the histogram_quantile function |
| 80 | the anchored and smoothed modifiers |
| 60 | a subquery |
| 56 | the histogram_fraction function |
| 41 | the info function |
| 21 | the label_replace function |
| 20 | a unary operator |
| 19 | the topk aggregation |
| 16 | the histogram_quantiles function |
| 13 | a scalar literal |
| 9 | the predict_linear function |
| 9 | the quantile_over_time function |
| 7 | the bottomk aggregation |
| 7 | the label_join function |
| 6 | the quantile aggregation |
| 5 | a string literal |
| 4 | the deriv function |
| 4 | the stddev_over_time function |
| 3 | the stdvar_over_time function |
| 3 | the ts_of_first_over_time function |
| 2 | a range selector |
| 2 | the double_exponential_smoothing function |
| 2 | the first_over_time function |
| 2 | the histogram_count function |
| 2 | the histogram_sum function |
| 2 | the mad_over_time function |
| 2 | the ts_of_last_over_time function |
| 1 | the changes function over a parenthesized expression |
| 1 | the histogram_avg function |
| 1 | the histogram_stddev function |
| 1 | the histogram_stdvar function |
| 1 | the limit_ratio aggregation |
| 1 | the limitk aggregation |
| 1 | the rate function over a parenthesized expression |
| 1 | the ts_of_max_over_time function |
| 1 | the ts_of_min_over_time function |

## By file

| file | evals | passing | |
|---|---:|---:|---|
| fill-modifier | 45 | 45 (100%) | fully green |
| selectors | 31 | 31 (100%) | fully green |
| trig_functions | 19 | 19 (100%) | fully green |
| staleness | 17 | 17 (100%) | fully green |
| collision | 2 | 2 (100%) | fully green |
| range_queries | 18 | 13 (72%) |  |
| type_and_unit | 58 | 39 (67%) |  |
| operators | 213 | 131 (62%) |  |
| aggregators | 160 | 93 (58%) |  |
| functions | 413 | 237 (57%) |  |
| name_label_dropping | 30 | 16 (53%) |  |
| at_modifier | 71 | 33 (46%) |  |
| extended_vectors | 118 | 38 (32%) |  |
| literals | 25 | 8 (32%) |  |
| duration_expression | 59 | 8 (14%) |  |
| subquery | 34 | 2 (6%) |  |
| histograms | 185 | 8 (4%) |  |
| native_histograms | 521 | 0 (0%) |  |
| info | 42 | 0 (0%) |  |
| limit | 37 | 0 (0%) | all skipped — native histograms |

---

Some failures are one cause wearing many hats — a single lexer gap can account for hundreds of rows above. Read a handful before picking, with:

```sh
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/
```
