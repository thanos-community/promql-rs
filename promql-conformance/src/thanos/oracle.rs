//! Client for the Go oracle in `scripts/promql-oracle`.
//!
//! One process is spawned per test binary and shared by every test
//! through a mutex. At roughly 4 ms per query that is a second or so for
//! the whole corpus, so a single process is ample; the protocol carries
//! a request id partly so a pool could be added later without a format
//! change.
//!
//! The oracle exits when its stdin closes, so it dies with the test
//! process and is never left orphaned.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Mutex, OnceLock};

use serde::Serialize;

use crate::result::{QueryResult, WireResponse};

/// A range query to ask the oracle about.
#[derive(Debug, Clone, Serialize)]
pub struct Request {
    pub id: i64,
    /// A Prometheus test-script `load` block, passed through verbatim so
    /// that Go does its own parsing. Comparing against upstream's
    /// reading of the input rather than our own is the entire point.
    pub load: String,
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    /// Engine lookback delta. Zero means Prometheus's 5m default.
    pub lookback_ms: i64,
}

/// Why the oracle is unavailable.
///
/// Distinct from a query failing: an unreachable oracle means the
/// harness is broken, and the differential tests should skip rather than
/// report failures indistinguishable from a missing engine.
#[derive(Debug, Clone, thiserror::Error)]
pub enum OracleError {
    #[error("go toolchain not found on PATH: {0}")]
    NoGo(String),
    #[error("building the oracle failed:\n{0}")]
    Build(String),
    #[error("spawning the oracle failed: {0}")]
    Spawn(String),
    #[error("oracle protocol failure: {0}")]
    Protocol(String),
}

struct Process {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: i64,
}

impl Process {
    fn request(&mut self, req: &Request) -> Result<QueryResult, OracleError> {
        let line = serde_json::to_string(req)
            .map_err(|e| OracleError::Protocol(format!("encoding request: {e}")))?;

        self.stdin
            .write_all(line.as_bytes())
            .and_then(|()| self.stdin.write_all(b"\n"))
            .and_then(|()| self.stdin.flush())
            .map_err(|e| OracleError::Protocol(format!("writing request: {e}")))?;

        let mut resp_line = String::new();
        let n = self
            .stdout
            .read_line(&mut resp_line)
            .map_err(|e| OracleError::Protocol(format!("reading response: {e}")))?;
        if n == 0 {
            return Err(OracleError::Protocol(
                "oracle closed its output; it likely crashed".to_string(),
            ));
        }

        let wire: WireResponse = serde_json::from_str(resp_line.trim())
            .map_err(|e| OracleError::Protocol(format!("decoding response {resp_line:?}: {e}")))?;

        // The protocol is strictly request/response, so a mismatched id
        // means the stream has desynchronised. Failing here beats
        // silently comparing one case against another's answer.
        if wire.id != req.id {
            return Err(OracleError::Protocol(format!(
                "response id {} does not match request id {}",
                wire.id, req.id
            )));
        }

        wire.into_result().map_err(OracleError::Protocol)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // Closing stdin is the graceful path; the kill is for the case
        // where the oracle is wedged rather than waiting on input.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// How many times [`Oracle::query_probing_stability`] asks before
/// calling an answer stable.
///
/// Some queries have no deterministic answer, so agreement between two
/// runs can be luck: of the ten corpus cases using `limitk` or
/// `limit_ratio`, a given run typically sees only two disagree. Three
/// probes make a false "stable" verdict unlikely without making the
/// failure path expensive.
pub const STABILITY_PROBES: usize = 3;

/// The shared oracle process.
pub struct Oracle {
    inner: Mutex<Process>,
}

impl Oracle {
    /// Ask for one range query.
    pub fn query(
        &self,
        load: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult, OracleError> {
        let mut proc = self
            .inner
            .lock()
            .map_err(|e| OracleError::Protocol(format!("oracle mutex poisoned: {e}")))?;
        proc.next_id += 1;
        let req = Request {
            id: proc.next_id,
            load: load.to_string(),
            query: query.to_string(),
            start_ms,
            end_ms,
            step_ms,
            lookback_ms: 0,
        };
        proc.request(&req)
    }

    /// Ask the same question [`STABILITY_PROBES`] times, returning the
    /// first answer and whether every answer agreed.
    ///
    /// Not every PromQL query has a single right answer. `limitk` and
    /// `limit_ratio` return an arbitrary subset, and `topk`/`bottomk`
    /// fall back on input order when values tie or are NaN — one corpus
    /// case is literally two `NaN` series under `topk(1, ...)`. Which
    /// series wins follows the storage's series order, and promqltest
    /// seeds from `loadCmd.defs`, a Go map iterated with `range`, so a
    /// fresh storage orders differently every request.
    ///
    /// `promql-engine`'s own differential test never sees this: it
    /// builds one storage per case and shares it between both engines.
    /// An out-of-process oracle cannot, because every request seeds
    /// anew. Such a case can hold no engine to account, so callers use
    /// this to tell "the engine is wrong" from "the question has no
    /// answer".
    pub fn query_probing_stability(
        &self,
        load: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<(QueryResult, bool), OracleError> {
        let first = self.query(load, query, start_ms, end_ms, step_ms)?;
        for _ in 1..STABILITY_PROBES {
            let again = self.query(load, query, start_ms, end_ms, step_ms)?;
            if crate::compare::compare(&first, &again).is_err() {
                return Ok((first, false));
            }
        }
        Ok((first, true))
    }
}

/// The process-wide oracle, built and spawned on first use.
pub fn shared() -> Result<&'static Oracle, &'static OracleError> {
    static ORACLE: OnceLock<Result<Oracle, OracleError>> = OnceLock::new();
    ORACLE.get_or_init(start).as_ref()
}

fn start() -> Result<Oracle, OracleError> {
    let bin = build()?;
    let mut child = Command::new(&bin)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| OracleError::Spawn(format!("{}: {e}", bin.display())))?;

    let stdin = child.stdin.take().expect("stdin was piped");
    let stdout = BufReader::new(child.stdout.take().expect("stdout was piped"));

    Ok(Oracle {
        inner: Mutex::new(Process {
            child,
            stdin,
            stdout,
            next_id: 0,
        }),
    })
}

/// Compile the oracle, skipping the build when the binary is already
/// newer than every source file.
fn build() -> Result<PathBuf, OracleError> {
    let src = source_dir();
    let out = target_dir().join("promql-oracle").join("oracle");

    if is_fresh(&out, &src) {
        return Ok(out);
    }

    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| OracleError::Build(format!("creating {}: {e}", parent.display())))?;
    }

    let output = Command::new("go")
        .arg("build")
        .arg("-o")
        .arg(&out)
        .arg(".")
        .current_dir(&src)
        .output()
        .map_err(|e| OracleError::NoGo(e.to_string()))?;

    if !output.status.success() {
        return Err(OracleError::Build(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }
    Ok(out)
}

/// Whether `bin` is newer than every file in the oracle's source
/// directory. A missing binary or any unreadable timestamp means rebuild,
/// since a needless rebuild is cheap and a stale oracle is not.
fn is_fresh(bin: &Path, src: &Path) -> bool {
    let Ok(bin_time) = bin.metadata().and_then(|m| m.modified()) else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(src) else {
        return false;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            return false;
        };
        if !meta.is_file() {
            continue;
        }
        match meta.modified() {
            Ok(t) if t <= bin_time => {}
            _ => return false,
        }
    }
    true
}

fn source_dir() -> PathBuf {
    workspace_root().join("scripts").join("promql-oracle")
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"))
}

/// `CARGO_MANIFEST_DIR` is this crate; the workspace is its parent.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory has a parent")
        .to_path_buf()
}
