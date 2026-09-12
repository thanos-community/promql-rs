//! Generated Store API types, named after Thanos's `storepb` package.
//!
//! `thanos` holds the `storepb` and `labelpb` messages, which share one
//! proto package, plus the `Store` service; `thanos::info` holds the
//! `infopb` messages and the `Info` service. The `.proto` sources are
//! vendored under `proto/`; `build.rs` compiles them.

#[allow(clippy::all, missing_docs, rustdoc::all)]
pub mod thanos {
    tonic::include_proto!("thanos");

    pub mod info {
        tonic::include_proto!("thanos.info");
    }
}
