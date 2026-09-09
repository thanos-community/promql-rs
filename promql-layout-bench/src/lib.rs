//! Two Arrow layouts for handing series to a PromQL engine, compared on equal footing.
//!
//! Start with the two files that matter. [`list`] is rows-as-series and [`struct_ree`] is
//! rows-as-samples; each holds exactly what is unique to producing and processing that layout, and
//! nothing else. Everything they share, the operator, the kernel, the plans, the fake scan and the
//! benchmark harness, lives in [`support`].

pub mod list;
pub mod struct_ree;
pub mod support;
