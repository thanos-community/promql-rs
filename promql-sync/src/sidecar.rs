//! Sidecar configuration for `promql-sync generate-grammar`.
//!
//! The sidecar tells the generator how to map upstream's yacc model
//! into our grmtools grammar:
//!
//! - `grammar-tokens.toml` — rename map for tokens (`LEFT_PAREN →
//!   LPAREN`, etc.). Keys are upstream names, values are what we emit.
//! - `grammar-actions.toml` — per-rule return type, per-alt action
//!   body, and meta settings (emit_roots, epilogue).
//!
//! Missing entries are never fatal. The generator instead emits a
//! placeholder body and records the gap in the generation report so
//! a human fills it in.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Sidecar {
    pub epilogue: String,
    pub start_symbol: String,
    pub emit_roots: Vec<String>,
    pub return_types: BTreeMap<String, String>,
    pub actions: Vec<ActionEntry>,
    /// Per-rule fallback action body used when an alt has no explicit
    /// [[actions]] entry. Lets us handle rules with many single-token
    /// alternatives (e.g. `maybe_label : AVG | BOOL | BOTTOMK | ...`)
    /// in one line. Falls through to `unmapped_placeholder` if neither
    /// the alt nor the rule has a mapping.
    pub default_actions: BTreeMap<String, String>,
    pub token_renames: BTreeMap<String, String>,
    /// Upstream rule names to skip entirely, even if reachable. Used
    /// for rules whose content we don't parse (series descriptions,
    /// histogram descriptors) but which sneak in through alts of an
    /// otherwise-reachable rule.
    pub skip_rules: Vec<String>,
    /// Upstream *terminals* that no alt may reference. An alt using one
    /// is dropped, exactly like an alt referencing a skipped rule. Used
    /// for `EOF`, which upstream lexes explicitly but grmtools handles
    /// implicitly, so it has no token id on our side.
    pub skip_tokens: Vec<String>,
    /// Placeholder body emitted for any unmapped alt. Must produce the
    /// rule's declared return type. Default is `Err(())`.
    pub unmapped_placeholder: String,
}

#[derive(Debug, Clone)]
pub struct ActionEntry {
    pub rule: String,
    pub alt: String,
    pub action: String,
}

impl Sidecar {
    pub fn load(actions_path: &Path, tokens_path: &Path) -> Result<Self> {
        let tokens: TokensFile = load_toml(tokens_path)?;
        let actions: ActionsFile = load_toml(actions_path)?;

        let mut return_types = BTreeMap::new();
        for (k, v) in actions.return_types.unwrap_or_default() {
            return_types.insert(k, v);
        }
        let actions_list = actions
            .actions
            .unwrap_or_default()
            .into_iter()
            .map(|a| ActionEntry {
                rule: a.rule,
                alt: a.alt,
                action: a.action,
            })
            .collect();
        Ok(Self {
            epilogue: actions.epilogue.unwrap_or_default(),
            start_symbol: actions.start_symbol.unwrap_or_else(|| "Expr".to_string()),
            emit_roots: actions.emit_roots.unwrap_or_default(),
            return_types,
            actions: actions_list,
            default_actions: actions.default_actions.unwrap_or_default(),
            token_renames: tokens.renames.unwrap_or_default(),
            skip_rules: actions.skip_rules.unwrap_or_default(),
            skip_tokens: actions.skip_tokens.unwrap_or_default(),
            unmapped_placeholder: actions
                .unmapped_placeholder
                .unwrap_or_else(|| "Err(())".to_string()),
        })
    }

    pub fn default_action_for(&self, rule: &str) -> Option<&str> {
        self.default_actions.get(rule).map(String::as_str)
    }

    pub fn find_action(&self, rule: &str, alt_sig: &str) -> Option<&str> {
        self.actions
            .iter()
            .find(|a| a.rule == rule && a.alt == alt_sig)
            .map(|a| a.action.as_str())
    }

    pub fn return_type_for(&self, rule: &str) -> Option<&str> {
        self.return_types.get(rule).map(String::as_str)
    }

    pub fn rename_token<'a>(&'a self, upstream: &'a str) -> &'a str {
        self.token_renames
            .get(upstream)
            .map(String::as_str)
            .unwrap_or(upstream)
    }
}

// ---------------- TOML shapes ----------------

#[derive(Debug, Deserialize)]
struct TokensFile {
    renames: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct ActionsFile {
    epilogue: Option<String>,
    start_symbol: Option<String>,
    emit_roots: Option<Vec<String>>,
    return_types: Option<BTreeMap<String, String>>,
    #[serde(rename = "actions")]
    actions: Option<Vec<RawAction>>,
    default_actions: Option<BTreeMap<String, String>>,
    skip_rules: Option<Vec<String>>,
    skip_tokens: Option<Vec<String>>,
    unmapped_placeholder: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawAction {
    rule: String,
    alt: String,
    action: String,
}

fn load_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let body = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    toml::from_str(&body).with_context(|| format!("parse {}", path.display()))
}
