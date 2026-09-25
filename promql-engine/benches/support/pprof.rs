//! pprof-rs as criterion's profiler, so `--profile-time` writes a
//! `profile.pb` that `go tool pprof` opens.
//!
//! pprof's own `criterion` feature implements the `Profiler` trait of
//! criterion 0.5, which is a different trait from the 0.7 one these benches
//! link, so it cannot be passed to `with_profiler`. This is that adapter,
//! for the protobuf output only, against 0.7.

use std::os::raw::c_int;
use std::path::Path;

use criterion::profiler::Profiler;
use pprof::protos::Message;
use pprof::ProfilerGuard;

pub struct PProfProfiler {
    frequency: c_int,
    active: Option<ProfilerGuard<'static>>,
}

impl PProfProfiler {
    pub fn new(frequency: c_int) -> Self {
        Self {
            frequency,
            active: None,
        }
    }
}

impl Profiler for PProfProfiler {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active = Some(ProfilerGuard::new(self.frequency).expect("start pprof"));
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        let Some(guard) = self.active.take() else {
            return;
        };
        let profile = guard
            .report()
            .build()
            .expect("build pprof report")
            .pprof()
            .expect("encode pprof profile");
        let mut content = Vec::new();
        profile
            .write_to_vec(&mut content)
            .expect("encode pprof protobuf");
        std::fs::create_dir_all(benchmark_dir).expect("create benchmark dir");
        let path = benchmark_dir.join("profile.pb");
        std::fs::write(&path, content).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    }
}
