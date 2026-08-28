//! Developer-invoked sync tool for `promql-parser`.
//!
//! Per `docs/parser-sync.md` this tool:
//!
//! - `--check`: verifies that every file listed in
//!   `promql-parser/upstream/MANIFEST.toml` matches its recorded
//!   sha256. Runs in CI to catch accidental edits to vendored files.
//! - `--target <sha-or-tag>`: fetches the upstream vendored files at
//!   the target revision, diffs them against the currently-vendored
//!   copies, and writes a Markdown + JSON report with a change
//!   summary. Mutates only the files under `upstream/` and
//!   `sync-meta.toml` — never commits, branches, or pushes.
//!
//! Structural classification (Green / Yellow / Red per the design doc)
//! is a follow-up. Today the tool gives an honest "what changed, pass
//! to a human" surface rather than attempting mechanical apply of
//! token-table / AST-field additions. The report it produces is the
//! starting point for manual translation.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod emitter;
mod sidecar;
mod yacc;

const UPSTREAM_OWNER: &str = "prometheus";
const UPSTREAM_REPO: &str = "prometheus";
const UPSTREAM_SUBDIR: &str = "promql/parser";

/// Files vendored under `upstream/`. Keep in sync with `UPSTREAM.md`.
const VENDORED_FILES: &[&str] = &[
    "generated_parser.y",
    "lex.go",
    "ast.go",
    "parse.go",
    "functions.go",
    "printer.go",
];

#[derive(Debug, Parser)]
#[command(name = "promql-sync", about = "Upstream sync tool for promql-parser")]
struct Cli {
    /// Path to the crate root. Defaults to `promql-parser`
    /// relative to the current working directory.
    #[arg(long, value_name = "PATH", global = true)]
    crate_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Verify vendored-file hashes match MANIFEST.toml. No network
    /// access, no mutations. Intended for CI.
    Check,

    /// Fetch upstream at the given git SHA (or tag) and compare with
    /// the currently-vendored files. When not in dry-run mode,
    /// rewrites `upstream/<file>`, `upstream/MANIFEST.toml`, and
    /// `sync-meta.toml`; otherwise just reports.
    Pull {
        /// Target git SHA or tag.
        #[arg(long, value_name = "SHA_OR_TAG")]
        target: String,

        /// Show the plan but mutate nothing.
        #[arg(long)]
        dry_run: bool,

        /// Report output path. Defaults to
        /// `target/promql-sync-report.md`.
        #[arg(long, value_name = "PATH")]
        report: Option<PathBuf>,
    },

    /// Regenerate `src/grammar.y` from the vendored upstream grammar
    /// (`upstream/generated_parser.y`) plus the `grammar-actions.toml`
    /// + `grammar-tokens.toml` sidecars.
    GenerateGrammar {
        /// Show the generated grammar on stdout instead of writing it.
        #[arg(long)]
        dry_run: bool,

        /// Output path. Defaults to `src/grammar.y` under the crate
        /// root.
        #[arg(long, value_name = "PATH")]
        out: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode> {
    let crate_dir = cli
        .crate_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("promql-parser"));
    let upstream_dir = crate_dir.join("upstream");
    let manifest_path = upstream_dir.join("MANIFEST.toml");

    match cli.command {
        Command::Check => check_integrity(&upstream_dir, &manifest_path),
        Command::Pull {
            target,
            dry_run,
            report,
        } => {
            let report_path =
                report.unwrap_or_else(|| PathBuf::from("target/promql-sync-report.md"));
            sync(
                &upstream_dir,
                &manifest_path,
                &crate_dir.join("sync-meta.toml"),
                &target,
                &report_path,
                dry_run,
            )
        }
        Command::GenerateGrammar { dry_run, out } => {
            let out_path = out.unwrap_or_else(|| crate_dir.join("src/grammar.y"));
            generate_grammar(&crate_dir, &upstream_dir, &out_path, dry_run)
        }
    }
}

fn generate_grammar(
    crate_dir: &Path,
    upstream_dir: &Path,
    out_path: &Path,
    dry_run: bool,
) -> Result<ExitCode> {
    let actions_toml = crate_dir.join("grammar-actions.toml");
    let tokens_toml = crate_dir.join("grammar-tokens.toml");
    let sc = sidecar::Sidecar::load(&actions_toml, &tokens_toml).context("load sidecars")?;
    let upstream_y = upstream_dir.join("generated_parser.y");
    let result = emitter::emit_from_paths(&upstream_y, &sc).context("emit grammar.y")?;

    if dry_run {
        println!("{}", result.text);
    } else {
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).ok();
        }
        fs::write(out_path, &result.text)
            .with_context(|| format!("write {}", out_path.display()))?;
        eprintln!("wrote {} ({} bytes)", out_path.display(), result.text.len());
    }

    eprintln!(
        "emitted: {} rules; skipped as unreachable/filtered: {}",
        result.rules_emitted.len(),
        result.rules_skipped_unreachable.len(),
    );
    if !result.unmapped.is_empty() {
        eprintln!(
            "\n{} unmapped alt(s) — placeholder emitted:",
            result.unmapped.len()
        );
        for u in &result.unmapped {
            eprintln!("  {}: {}", u.rule, u.upstream_signature);
        }
        eprintln!(
            "\nFill these in `{}` to replace the placeholders.",
            crate_dir.join("grammar-actions.toml").display()
        );
    }

    Ok(ExitCode::SUCCESS)
}

// ---------------- --check mode ----------------

fn check_integrity(upstream_dir: &Path, manifest_path: &Path) -> Result<ExitCode> {
    let manifest = Manifest::load(manifest_path)
        .with_context(|| format!("load manifest at {}", manifest_path.display()))?;
    let mut mismatches = Vec::new();
    for file in VENDORED_FILES {
        let disk = sha256_file(&upstream_dir.join(file)).with_context(|| format!("hash {file}"))?;
        match manifest.file_hashes.get(*file) {
            Some(recorded) if recorded == &disk => {
                println!("ok     {file}");
            }
            Some(recorded) => {
                println!("drift  {file}\n       disk={disk}\n       toml={recorded}");
                mismatches.push(*file);
            }
            None => {
                println!("absent {file} (not in MANIFEST)");
                mismatches.push(*file);
            }
        }
    }
    if mismatches.is_empty() {
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!(
            "\n{} vendored file(s) have drifted. Either revert the edit or run `promql-sync --target <sha>` to adopt the change.",
            mismatches.len()
        );
        Ok(ExitCode::from(1))
    }
}

// ---------------- --target mode ----------------

#[derive(Debug)]
struct FileDelta {
    name: String,
    old_hash: Option<String>,
    new_hash: String,
    bytes_old: usize,
    bytes_new: usize,
}

fn sync(
    upstream_dir: &Path,
    manifest_path: &Path,
    sync_meta_path: &Path,
    target: &str,
    report_path: &Path,
    dry_run: bool,
) -> Result<ExitCode> {
    let client = reqwest::blocking::Client::builder()
        .user_agent("promql-sync/0.1")
        .build()?;
    let prior = Manifest::load(manifest_path).unwrap_or_default();

    let mut deltas = Vec::new();
    let mut new_manifest = Manifest::default();
    for file in VENDORED_FILES {
        let url = format!(
            "https://raw.githubusercontent.com/{UPSTREAM_OWNER}/{UPSTREAM_REPO}/{target}/{UPSTREAM_SUBDIR}/{file}"
        );
        let body = client
            .get(&url)
            .send()
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .with_context(|| format!("status of {url}"))?
            .bytes()
            .with_context(|| format!("read {url}"))?;
        let new_hash = sha256_bytes(&body);
        let old_path = upstream_dir.join(file);
        let old_bytes_len = fs::read(&old_path).map(|b| b.len()).unwrap_or(0);
        let old_hash = prior.file_hashes.get(*file).cloned();
        deltas.push(FileDelta {
            name: (*file).to_string(),
            old_hash: old_hash.clone(),
            new_hash: new_hash.clone(),
            bytes_old: old_bytes_len,
            bytes_new: body.len(),
        });
        new_manifest
            .file_hashes
            .insert((*file).to_string(), new_hash);
        if !dry_run {
            fs::write(&old_path, &body).with_context(|| format!("write {file}"))?;
        }
    }

    // Report.
    fs::create_dir_all(report_path.parent().unwrap_or_else(|| Path::new(".")))?;
    let mut md = String::new();
    md.push_str(&format!("# promql-sync report — target `{target}`\n\n"));
    md.push_str(if dry_run {
        "Mode: `--dry-run`. No files were modified.\n\n"
    } else {
        "Mode: apply. Vendored files have been rewritten in place; review `git diff` before committing.\n\n"
    });
    md.push_str("| file | prior sha256 | new sha256 | bytes |\n");
    md.push_str("| --- | --- | --- | --- |\n");
    let mut any_changed = false;
    for d in &deltas {
        let changed = d.old_hash.as_deref() != Some(d.new_hash.as_str());
        if changed {
            any_changed = true;
        }
        md.push_str(&format!(
            "| `{}` | `{}` | `{}`{} | {} → {} |\n",
            d.name,
            d.old_hash.as_deref().unwrap_or("-"),
            d.new_hash,
            if changed { " ⚠️" } else { "" },
            d.bytes_old,
            d.bytes_new,
        ));
    }
    md.push_str("\n## Next steps\n\n");
    if any_changed {
        md.push_str(
            "1. Inspect the diff: `git diff promql-parser/upstream/`.\n\
             2. Re-run `cargo test -p promql-parser` to see which translation steps are needed.\n\
             3. Translate any new grammar rules / action bodies / lexer states / AST fields into the corresponding Rust modules.\n\
             4. Update `sync-meta.toml` with the translation hashes (manual for now; structural classification is a follow-up).\n\
             5. Commit the vendored-file bump alongside the Rust translations in a single reviewable change.\n\n\
             Note: structural Green/Yellow/Red classification (design doc) is not yet implemented. This produces a byte-level diff; human handles the translation call.\n",
        );
    } else {
        md.push_str("No vendored files changed vs the prior manifest.\n");
    }
    fs::write(report_path, &md)
        .with_context(|| format!("write report {}", report_path.display()))?;
    println!("wrote report to {}", report_path.display());

    if !dry_run && any_changed {
        new_manifest.save(manifest_path)?;
        update_sync_meta(sync_meta_path, target)?;
        println!("updated MANIFEST and sync-meta.toml");
    }

    Ok(ExitCode::SUCCESS)
}

// ---------------- MANIFEST + sync-meta I/O ----------------

#[derive(Debug, Default, Serialize, Deserialize)]
struct Manifest {
    #[serde(default)]
    file_hashes: BTreeMap<String, String>,
}

impl Manifest {
    fn load(path: &Path) -> Result<Self> {
        let body = fs::read_to_string(path)?;
        let m: Manifest = toml::from_str(&body).context("parse MANIFEST.toml")?;
        Ok(m)
    }

    fn save(&self, path: &Path) -> Result<()> {
        // Re-emit a hand-friendly MANIFEST.toml that matches the format
        // humans wrote. `toml::to_string_pretty` produces the right
        // shape for a `[file_hashes]` table.
        let mut out = String::new();
        out.push_str(
            "# SHA-256 of every vendored upstream file.\n\
             #\n\
             # The promql-sync tool maintains this file. See\n\
             # `docs/parser-sync.md`.\n\n\
             [file_hashes]\n",
        );
        for (k, v) in &self.file_hashes {
            out.push_str(&format!(
                "\"{k}\" = \"sha256:{}\"\n",
                v.trim_start_matches("sha256:")
            ));
        }
        fs::write(path, out)?;
        Ok(())
    }
}

fn update_sync_meta(path: &Path, target: &str) -> Result<()> {
    // Minimal update: rewrite pinned_upstream_sha and last_sync_date,
    // preserve existing translations tables. To avoid a fragile TOML
    // edit, read-mutate-write via the toml crate's Value type.
    let existing = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut doc: toml::Value =
        toml::from_str(&existing).with_context(|| format!("parse {}", path.display()))?;
    if let Some(table) = doc.as_table_mut() {
        table.insert(
            "pinned_upstream_sha".into(),
            toml::Value::String(target.into()),
        );
        table.insert(
            "last_sync_date".into(),
            toml::Value::String(chrono_date_utc()),
        );
    } else {
        bail!("sync-meta.toml is not a table");
    }
    let out = toml::to_string(&doc)?;
    fs::write(path, out)?;
    Ok(())
}

/// RFC-3339 date component in UTC, without pulling in the chrono
/// workspace dep. Produces e.g. `2026-04-21`.
fn chrono_date_utc() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Civil-date conversion from POSIX seconds (Howard Hinnant's
    // algorithm, stable-Rust safe).
    let days = (secs / 86_400) as i64;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146_096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

// ---------------- hashing ----------------

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("sha256:{:x}", h.finalize())
}
