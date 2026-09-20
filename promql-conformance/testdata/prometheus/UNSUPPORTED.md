# What the engine is missing

Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

Nothing here gates CI. It is a roadmap: every row is a count of Prometheus's own promqltest evals that one missing feature blocks, so the top of the table is the cheapest coverage available.

Against the vendored corpus: **409 of 2098 evals pass** (19.5%), and **548** are blocked on the features below.

## Missing features

| evals blocked | feature |
|---:|---|
| 78 | the histogram_quantile function |
| 57 | a subquery |
| 56 | the histogram_fraction function |
| 41 | the info function |
| 33 | the fill modifier |
| 21 | the label_replace function |
| 20 | the group_left modifier |
| 16 | the absent_over_time function |
| 16 | the histogram_quantiles function |
| 16 | the topk aggregation |
| 13 | the absent function |
| 13 | the timestamp function |
| 12 | a scalar literal |
| 12 | a unary operator |
| 9 | the predict_linear function |
| 9 | the quantile_over_time function |
| 9 | the sort_by_label function |
| 7 | the label_join function |
| 7 | the on modifier |
| 7 | the or set operator |
| 6 | the and set operator |
| 6 | the quantile aggregation |
| 5 | a string literal |
| 5 | the group_right modifier |
| 4 | the day_of_year function |
| 4 | the month function |
| 4 | the sort_by_label_desc function |
| 4 | the stddev_over_time function |
| 4 | the year function |
| 3 | the bottomk aggregation |
| 3 | the deriv function |
| 3 | the minute function |
| 3 | the stdvar_over_time function |
| 3 | the ts_of_first_over_time function |
| 2 | a range selector |
| 2 | the day_of_month function |
| 2 | the day_of_week function |
| 2 | the days_in_month function |
| 2 | the double_exponential_smoothing function |
| 2 | the first_over_time function |
| 2 | the histogram_count function |
| 2 | the histogram_sum function |
| 2 | the hour function |
| 2 | the ignoring modifier |
| 2 | the mad_over_time function |
| 2 | the sort function |
| 2 | the time function |
| 2 | the ts_of_last_over_time function |
| 1 | the changes function over a parenthesized expression |
| 1 | the histogram_avg function |
| 1 | the histogram_stddev function |
| 1 | the histogram_stdvar function |
| 1 | the limit_ratio aggregation |
| 1 | the limitk aggregation |
| 1 | the pi function |
| 1 | the rate function over a parenthesized expression |
| 1 | the sort_desc function |
| 1 | the ts_of_max_over_time function |
| 1 | the ts_of_min_over_time function |

## By file

| file | evals | passing | |
|---|---:|---:|---|
| staleness | 17 | 17 (100%) | fully green |
| trig_functions | 19 | 18 (95%) |  |
| collision | 2 | 1 (50%) |  |
| name_label_dropping | 30 | 14 (47%) |  |
| aggregators | 160 | 65 (41%) |  |
| range_queries | 18 | 7 (39%) |  |
| selectors | 31 | 12 (39%) |  |
| functions | 413 | 140 (34%) |  |
| extended_vectors | 118 | 38 (32%) |  |
| at_modifier | 71 | 20 (28%) |  |
| literals | 25 | 7 (28%) |  |
| operators | 213 | 54 (25%) |  |
| duration_expression | 59 | 8 (14%) |  |
| subquery | 34 | 2 (6%) |  |
| histograms | 185 | 6 (3%) |  |
| native_histograms | 521 | 0 (0%) |  |
| type_and_unit | 58 | 0 (0%) |  |
| fill-modifier | 45 | 0 (0%) |  |
| info | 42 | 0 (0%) |  |
| limit | 37 | 0 (0%) | all skipped — native histograms |

---

Some failures are one cause wearing many hats — a single lexer gap can account for hundreds of rows above. Read a handful before picking, with:

```sh
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/
```
