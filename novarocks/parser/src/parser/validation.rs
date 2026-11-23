// SPDX-License-Identifier: Apache-2.0

use std::alloc::{Layout, alloc};
use std::ptr::NonNull;

use super::Parser;
use crate::{ParserError, lexer, token::Token};

/// A syntax failure or refusal to retain the canonical lexer's token buffer.
#[derive(Debug)]
pub enum StatementValidationError {
    Syntax(ParserError),
    TokenCapacity,
}

impl From<ParserError> for StatementValidationError {
    fn from(error: ParserError) -> Self {
        Self::Syntax(error)
    }
}

/// Verify a complete statement with the original grammar and a token buffer
/// funded by the caller. Each grant stays alive until its buffer is released.
/// No AST escapes validation, so parameter event tracking is unnecessary.
pub fn validate_statement_syntax<Grant>(
    source: &str,
    mut reserve_tokens: impl FnMut(usize) -> Option<Grant>,
) -> Result<(), StatementValidationError> {
    let mut grant = None;
    let mut tokens = Vec::new();
    lexer::visit_tokens(source, |token| {
        if tokens.len() == tokens.capacity() {
            let capacity = tokens.capacity().max(4).saturating_mul(2);
            let layout = Layout::array::<Token>(capacity)
                .map_err(|_| StatementValidationError::TokenCapacity)?;
            let replacement_grant =
                reserve_tokens(layout.size()).ok_or(StatementValidationError::TokenCapacity)?;
            // SAFETY: The nonempty layout belongs to exactly `capacity` tokens.
            // The pointer comes from the same global allocator used by Vec, and
            // the Vec starts with zero initialized elements.
            let mut replacement = unsafe {
                let pointer = NonNull::new(alloc(layout).cast::<Token>())
                    .ok_or(StatementValidationError::TokenCapacity)?;
                Vec::from_raw_parts(pointer.as_ptr(), 0, capacity)
            };
            replacement.extend_from_slice(&tokens);
            // Both grants are live while the buffers coexist.
            tokens = replacement;
            grant = Some(replacement_grant);
        }
        tokens.push(token);
        Ok::<_, StatementValidationError>(())
    })?;
    let mut parser = Parser::for_syntax_validation(source, tokens);
    let result = parser
        .parse_complete_statement()
        .map(|_| ())
        .map_err(|error| StatementValidationError::Syntax(error.into()));
    drop(parser);
    drop(grant);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::mem::size_of;

    struct Charge<'a> {
        current: &'a Cell<usize>,
        bytes: usize,
    }

    impl Drop for Charge<'_> {
        fn drop(&mut self) {
            self.current.set(self.current.get() - self.bytes);
        }
    }

    #[test]
    fn syntax_validation_discards_parameter_events_and_preserves_parser_results() {
        let tracked_sql = "SELECT TRIM(? FROM ?), ?, ?, ?, ? FROM \
                           TABLE(f(TABLE(t) PARTITION BY (?) + ? ORDER BY (?) DESC))";
        let mut parser =
            Parser::for_syntax_validation(tracked_sql, lexer::lex(tracked_sql).unwrap());
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
            let syntax_validation =
                validate_statement_syntax(source, |_| Some(())).map_err(|error| match error {
                    StatementValidationError::Syntax(error) => error,
                    StatementValidationError::TokenCapacity => panic!("unexpected capacity error"),
                });
            assert_eq!(syntax_validation, public_parse, "{source}");
        }
    }

    #[test]
    fn syntax_validation_charges_the_replacement_peak_and_releases_the_buffer() {
        let allocated = Cell::new(0);
        let sql = "SELECT ?, 1, 2, 3, 4, 5";
        let reserve = |limit: usize| {
            let allocated = &allocated;
            move |bytes| {
                let used = allocated.get() + bytes;
                if used > limit {
                    return None;
                }
                allocated.set(used);
                Some(Charge {
                    current: allocated,
                    bytes,
                })
            }
        };
        let peak = (16 + 32) * size_of::<Token>();
        assert!(matches!(
            validate_statement_syntax(sql, reserve(peak - 1)),
            Err(StatementValidationError::TokenCapacity)
        ));
        assert_eq!(allocated.get(), 0);
        validate_statement_syntax(sql, reserve(peak)).unwrap();
        assert_eq!(allocated.get(), 0);
        assert!(matches!(
            validate_statement_syntax("SELECT 1; SELECT 2", reserve(peak)),
            Err(StatementValidationError::Syntax(_))
        ));
        assert_eq!(allocated.get(), 0);
    }
}
