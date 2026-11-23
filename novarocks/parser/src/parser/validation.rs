// SPDX-License-Identifier: Apache-2.0

use super::Parser;
use crate::ParserError;

/// Verify a complete statement with the original lexer and grammar.
/// No AST escapes validation, so parameter event tracking is unnecessary.
pub fn validate_statement_syntax(source: &str) -> Result<(), ParserError> {
    Parser::for_syntax_validation(source)?.parse_complete_statement()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_validation_discards_parameter_events_and_preserves_parser_results() {
        let tracked_sql = "SELECT TRIM(? FROM ?), ?, ?, ?, ? FROM \
                           TABLE(f(TABLE(t) PARTITION BY (?) + ? ORDER BY (?) DESC))";
        let mut parser = Parser::for_syntax_validation(tracked_sql).unwrap();
        parser.parse_complete_statement().unwrap();
        assert_eq!(parser.next_parameter, 9);
        assert_eq!(parser.parameter_events.capacity(), 0);

        for source in [
            tracked_sql,
            "SELECT TRIM(? FROM ?),",
            "SELECT * FROM TABLE(f(TABLE(t) copartition(a,b)))",
            "SELECT ?; SELECT ?",
            "SELECT ?, 'unterminated",
        ] {
            let public_parse = super::super::parse_statement(source).map(|_| ());
            assert_eq!(validate_statement_syntax(source), public_parse, "{source}");
        }
    }
}
