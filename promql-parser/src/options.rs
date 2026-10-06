//! Parser feature gates. Upstream `parser.Options` (`upstream/parse.go`).
//!
//! Stock Prometheus parses a smaller language than the grammar accepts:
//! each experimental construct is rejected by the grammar action that
//! builds it unless a `--enable-feature` value switched its gate on
//! (`cmd/prometheus/main.go`). All four gates default to off, here as
//! there, so [`crate::parse_expr`] rejects what `ParseExpr` rejects.
//! Upstream's promqltest turns all four on (`TestParserOpts`), which is
//! what [`ParserOptions::all`] is for.
//!
//! `docs/feature-flags.md` lists the gates against the constructs they
//! guard.

/// Upstream `parser.Options`, field for field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParserOptions {
    /// `EnableExperimentalFunctions`, `--enable-feature=promql-experimental-functions`.
    /// Experimental functions (`Experimental: true` in `functions.go`) and
    /// the `limitk` / `limit_ratio` aggregators.
    pub enable_experimental_functions: bool,
    /// `ExperimentalDurationExpr`, `--enable-feature=promql-duration-expr`.
    /// Arithmetic, `step()`, `range()` and `min`/`max` inside durations.
    pub experimental_duration_expr: bool,
    /// `EnableExtendedRangeSelectors`, `--enable-feature=promql-extended-range-selectors`.
    /// The `anchored` and `smoothed` selector modifiers.
    pub enable_extended_range_selectors: bool,
    /// `EnableBinopFillModifiers`, `--enable-feature=promql-binop-fill-modifiers`.
    /// `fill`, `fill_left` and `fill_right` on binary operators.
    pub enable_binop_fill_modifiers: bool,
}

impl ParserOptions {
    /// Every gate on: upstream's `TestParserOpts` in
    /// `promql/promqltest/test.go`.
    pub const fn all() -> Self {
        Self {
            enable_experimental_functions: true,
            experimental_duration_expr: true,
            enable_extended_range_selectors: true,
            enable_binop_fill_modifiers: true,
        }
    }
}

/// One `--enable-feature` value and the option it sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureFlag {
    /// The value after `--enable-feature=`, upstream `cmd/prometheus/main.go`.
    pub flag: &'static str,
    /// Upstream's `parser.Options` field.
    pub upstream_option: &'static str,
    /// This crate's [`ParserOptions`] field.
    pub option: &'static str,
}

/// The four gates, in `parser.Options` order. The conformance reports
/// name a missing feature's flag from this table, so a flag added here
/// shows up there.
pub const FEATURE_FLAGS: [FeatureFlag; 4] = [
    FeatureFlag {
        flag: "promql-experimental-functions",
        upstream_option: "EnableExperimentalFunctions",
        option: "enable_experimental_functions",
    },
    FeatureFlag {
        flag: "promql-duration-expr",
        upstream_option: "ExperimentalDurationExpr",
        option: "experimental_duration_expr",
    },
    FeatureFlag {
        flag: "promql-extended-range-selectors",
        upstream_option: "EnableExtendedRangeSelectors",
        option: "enable_extended_range_selectors",
    },
    FeatureFlag {
        flag: "promql-binop-fill-modifiers",
        upstream_option: "EnableBinopFillModifiers",
        option: "enable_binop_fill_modifiers",
    },
];

/// The flag that gates the language feature the engine calls `feature`
/// in `EngineError::Unsupported` (`the mad_over_time function`,
/// `the limitk aggregation`, `the anchored and smoothed modifiers`), or
/// `None` for one stock Prometheus parses.
pub fn flag_for_feature(feature: &str) -> Option<&'static str> {
    let [functions, _duration, selectors, _fill] = FEATURE_FLAGS.map(|f| f.flag);
    if feature == "the anchored and smoothed modifiers" {
        return Some(selectors);
    }
    if let Some(name) = feature
        .strip_prefix("the ")
        .and_then(|f| f.strip_suffix(" function"))
    {
        return crate::functions::is_experimental(name).then_some(functions);
    }
    let name = feature
        .strip_prefix("the ")
        .and_then(|f| f.strip_suffix(" aggregation"))?;
    matches!(name, "limitk" | "limit_ratio").then_some(functions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_gate_defaults_off_and_all_turns_every_gate_on() {
        assert_eq!(
            ParserOptions::default(),
            ParserOptions {
                enable_experimental_functions: false,
                experimental_duration_expr: false,
                enable_extended_range_selectors: false,
                enable_binop_fill_modifiers: false,
            }
        );
        let all = ParserOptions::all();
        assert!(
            all.enable_experimental_functions
                && all.experimental_duration_expr
                && all.enable_extended_range_selectors
                && all.enable_binop_fill_modifiers
        );
    }

    /// `docs/feature-flags.md` is the reviewer's table; a flag renamed
    /// here and not there fails this.
    #[test]
    fn the_doc_lists_every_flag_under_its_names() {
        let doc = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../docs/feature-flags.md"
        ))
        .expect("docs/feature-flags.md");
        for f in FEATURE_FLAGS {
            for name in [f.flag, f.upstream_option, f.option] {
                assert!(
                    doc.contains(&format!("`{name}`")),
                    "{name} is not in the doc"
                );
            }
        }
    }

    #[test]
    fn features_name_the_flag_that_gates_them() {
        assert_eq!(
            flag_for_feature("the anchored and smoothed modifiers"),
            Some("promql-extended-range-selectors")
        );
        assert_eq!(
            flag_for_feature("the mad_over_time function"),
            Some("promql-experimental-functions")
        );
        assert_eq!(
            flag_for_feature("the limit_ratio aggregation"),
            Some("promql-experimental-functions")
        );
        assert_eq!(flag_for_feature("the rate function"), None);
        assert_eq!(flag_for_feature("the topk aggregation"), None);
        assert_eq!(flag_for_feature("a subquery"), None);
    }
}
