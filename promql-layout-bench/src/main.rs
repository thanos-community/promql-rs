//! The benchmark binary. The work is in `support::harness`; this file exists to install the
//! counting allocator, which has to live in the binary because the tests install their own.

use promql_layout_bench::support::alloc::Counting;

/// Counting every allocation is the only way to see a materialisation that the allocator hides
/// from RSS. It stays on for the whole run, so every timing carries two relaxed atomics per
/// allocation.
#[global_allocator]
static ALLOC: Counting = Counting;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    promql_layout_bench::support::harness::run().await
}
