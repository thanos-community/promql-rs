# Upstream sources

Histogram behavior in this crate is derived from Prometheus at commit
`40ea54d0b1d265b8dc830c88ef96b0b65f3ba0be`:

- `model/histogram/generic.go`
- `model/histogram/histogram.go`
- `model/histogram/float_histogram.go`
- `promql/functions.go`
- `promql/quantile.go`
- `promql/quantile_test.go`
- `promql/promqltest/testdata/histograms.test`
- `promql/promqltest/testdata/native_histograms.test`
- `util/almost/almost.go`
- `util/kahansum/kahansum.go`
- `util/jsonutil/marshal.go`

Prometheus is Copyright 2012-2026 The Prometheus Authors and is licensed under
the Apache License, Version 2.0.
