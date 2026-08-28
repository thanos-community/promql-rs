use cfgrammar::yacc::YaccKind;
use lrlex::CTLexerBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The PromQL grammar carries the same inherent shift/reduce
    // conflicts as upstream's goyacc grammar: one per binary operator
    // resolved by %left/%right precedence, plus a handful from the
    // postfix [range]/[range:step]/offset/@ productions. The grammar
    // uses `%expect` to opt in to that count; grmtools then accepts
    // those conflicts as designed.
    CTLexerBuilder::new()
        .lrpar_config(|ctp| {
            ctp.yacckind(YaccKind::Grmtools)
                .grammar_in_src_dir("grammar.y")
                .expect("grammar.y in src/ dir")
                // The emitted grammar mirrors upstream's, which is
                // ambiguous by design — operator precedence and the
                // matrix/subquery [..] productions produce a known
                // set of shift/reduce conflicts resolved by the
                // `%left`/`%right` declarations. grmtools surfaces
                // them as warnings under this flag; goyacc accepts
                // them silently.
                .error_on_conflicts(false)
        })
        .lexer_in_src_dir("lexer.l")?
        .build()?;
    Ok(())
}
