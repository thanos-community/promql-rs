//! Compiles the vendored Thanos protos under `proto/` with protox, a
//! pure-Rust protobuf compiler, so neither contributors nor CI need
//! `protoc`. The server side is only generated for the `testutil`
//! feature, which runs a fake store in tests.

use std::path::PathBuf;

/// Paths relative to the include root, in Thanos's `pkg/` layout so the
/// upstream `import` lines resolve unchanged.
const FILES: &[&str] = &[
    "store/storepb/rpc.proto",
    "store/storepb/types.proto",
    "store/labelpb/types.proto",
    "info/infopb/rpc.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    for file in FILES {
        println!("cargo:rerun-if-changed={}", root.join(file).display());
    }
    let build_server = std::env::var_os("CARGO_FEATURE_TESTUTIL").is_some();

    let descriptors = protox::compile(FILES, [&root])?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(build_server)
        .emit_rerun_if_changed(false)
        .compile_fds(descriptors)?;
    Ok(())
}
