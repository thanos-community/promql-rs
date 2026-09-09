//! Phase 1, the scan layer.
//!
//! In a real deployment this reads object storage, calls another service, or mmaps a file. Here
//! it invents series out of thin air, because phase 2 cannot tell the difference: the only thing
//! crossing the boundary is a batch in one of the two layouts.
//!
//! What a scan layer owes phase 2, faked or not:
//!
//! 1. apply the matchers and the time range,
//! 2. group samples by labelset, contiguously,
//! 3. sort by timestamp within each series.
//!
//! Everything here satisfies all three by construction. The bottom of the file turns the fake
//! scan into the Arrow arrays both layouts are built from; arranging them is the layouts' job.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, StructArray, TimestampMillisecondArray};

use super::format::{dict, label_fields, LABEL_NAMES};

/// Scrape interval of the cluster the design note is based on.
pub const SCRAPE_MS: i64 = 30_000;

/// Shape of one scan handed from phase 1 to phase 2.
#[derive(Debug, Clone, Copy)]
pub struct Spec {
    /// Distinct series in the scan.
    pub series: usize,
    /// Samples per series, i.e. `window / scrape_interval`.
    pub samples: usize,
}

impl Spec {
    pub fn new(series: usize, samples: usize) -> Self {
        Self { series, samples }
    }

    /// Total samples in the scan.
    pub fn total(&self) -> usize {
        self.series * self.samples
    }

    /// The evaluation timestamp a query over this scan is asked at: the last sample, so that a
    /// window of `samples * SCRAPE_MS` ending there covers exactly the samples in the scan.
    pub fn at(&self) -> i64 {
        self.samples.saturating_sub(1) as i64 * SCRAPE_MS
    }
}

/// The order phase 1 hands series over in: sorted by labelset, which is how a TSDB index walks
/// them. Series `i` keeps its values, `values_for(i)`; only the position it is emitted at moves.
///
/// Contiguous-but-unsorted is all phase 1 actually owes; sorting is only what a TSDB index does,
/// and nothing in the benchmark depends on it.
pub fn series_order(series: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..series).collect();
    order.sort_by_cached_key(|&i| labels_for(i));
    order
}

/// Label values for series `i`. Some labels are constant across the metric, which is what makes
/// dictionary and run-end encoding worth anything; others vary per series.
pub fn labels_for(i: usize) -> Vec<String> {
    let codes = ["200", "201", "404", "409", "422", "500", "503"];
    let verbs = ["GET", "LIST", "WATCH", "POST", "PUT", "PATCH", "DELETE"];
    let resources = [
        "pods",
        "configmaps",
        "secrets",
        "deployments",
        "nodes",
        "events",
        "leases",
        "customresourcedefinitions",
    ];
    let scopes = ["cluster", "namespace", "resource"];
    let namespaces = ["kube-system", "monitoring", "default", "polarsignals"];

    vec![
        codes[i % codes.len()].to_string(),
        "apiserver".to_string(),
        "kube-apiserver".to_string(),
        "".to_string(),
        "https".to_string(),
        if i.is_multiple_of(3) { "" } else { "apps" }.to_string(),
        format!("10.0.{}.{}:6443", i / 256, i % 256),
        "apiserver".to_string(),
        namespaces[i % namespaces.len()].to_string(),
        format!("kube-apiserver-node-{}", i % 5),
        "monitoring/k8s".to_string(),
        format!("prometheus-k8s-{}", i % 2),
        resources[i % resources.len()].to_string(),
        scopes[i % scopes.len()].to_string(),
        "kubernetes".to_string(),
        if i.is_multiple_of(4) { "" } else { "status" }.to_string(),
        verbs[i % verbs.len()].to_string(),
        "v1".to_string(),
    ]
}

/// Timestamps for one series: ascending, one per scrape interval.
pub fn timestamps_for(spec: &Spec) -> Vec<i64> {
    (0..spec.samples).map(|s| s as i64 * SCRAPE_MS).collect()
}

/// Values for series `i`: a monotonically increasing counter, which is what `rate` expects.
pub fn values_for(i: usize, spec: &Spec) -> Vec<f64> {
    (0..spec.samples)
        .map(|s| (i * 1000 + s * 7) as f64)
        .collect()
}

/// One struct entry per series, dictionary encoded at the leaf.
pub fn labels_per_series(series: &[usize]) -> StructArray {
    let mut columns: Vec<Vec<String>> = vec![Vec::with_capacity(series.len()); LABEL_NAMES.len()];
    for &i in series {
        for (f, v) in labels_for(i).into_iter().enumerate() {
            columns[f].push(v);
        }
    }
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|c| Arc::new(dict(c)) as ArrayRef)
        .collect();
    StructArray::try_new(label_fields(), arrays, None).expect("labels per series")
}

/// Samples of the given series concatenated, series contiguous and timestamps ascending within
/// each. Identical bytes in both layouts; only what sits alongside them changes.
pub fn flat_samples(spec: &Spec, series: &[usize]) -> (TimestampMillisecondArray, Float64Array) {
    let mut ts = Vec::with_capacity(series.len() * spec.samples);
    let mut vs = Vec::with_capacity(series.len() * spec.samples);
    for &i in series {
        ts.extend(timestamps_for(spec));
        vs.extend(values_for(i, spec));
    }
    (TimestampMillisecondArray::from(ts), Float64Array::from(vs))
}
