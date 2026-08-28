// This is a stub module that shadows the parent module's package discovery
// for this directory. Files under upstream/ are verbatim copies from
// prometheus/prometheus used as the source of truth for our Rust port; they
// are not compiled, and this go.mod exists solely to keep them out of the
// parent Go build while preserving their byte-for-byte fidelity.
module promql-parser-upstream-reference

go 1.24
