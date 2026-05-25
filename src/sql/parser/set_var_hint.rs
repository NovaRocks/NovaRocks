//! Optimizer-hint extraction for `/*+ SET_VAR(...) */` comments embedded
//! in raw SQL text.
//!
//! StarRocks lets clients tweak session variables for a single statement
//! via `SELECT /*+ SET_VAR(name=value) */ ...`. NovaRocks's standalone
//! server doesn't yet have a full session-variable engine, but a handful
//! of execution-affecting flags surface in SQL tests. This module scans
//! the SQL text for those flags before parse-time so the caller can wire
//! them into `TQueryOptions` (which the executor already honours).
//!
//! Currently supported:
//! - `sql_mode='ALLOW_THROW_EXCEPTION'` → enables strict overflow errors
//!   for `CAST` and arithmetic, matching `arena.allow_throw_exception()`.

/// Returns `true` if any `/*+ ... SET_VAR(... sql_mode = '...' ... ) ... */`
/// hint in `sql` mentions `ALLOW_THROW_EXCEPTION` in its value.
///
/// The scan respects single-quoted, double-quoted, and back-ticked strings
/// so a literal containing `/*+` doesn't open a phantom hint. It is
/// deliberately permissive: any hint comment whose body contains the
/// substring `sql_mode` followed by `=` and a value containing
/// `ALLOW_THROW_EXCEPTION` (case-insensitive) is enough to enable the
/// flag — no full SQL grammar parse is required.
pub(crate) fn extract_allow_throw_exception(sql: &str) -> bool {
    for hint in iter_hint_bodies(sql) {
        if hint_enables_allow_throw_exception(hint) {
            return true;
        }
    }
    false
}

fn iter_hint_bodies(sql: &str) -> impl Iterator<Item = &str> {
    HintIter {
        bytes: sql.as_bytes(),
        sql,
        idx: 0,
        single: false,
        double: false,
        backtick: false,
    }
}

struct HintIter<'a> {
    bytes: &'a [u8],
    sql: &'a str,
    idx: usize,
    single: bool,
    double: bool,
    backtick: bool,
}

impl<'a> Iterator for HintIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        while self.idx < self.bytes.len() {
            let byte = self.bytes[self.idx];
            if self.single {
                if byte == b'\'' && self.bytes.get(self.idx.wrapping_sub(1)).copied() != Some(b'\\')
                {
                    self.single = false;
                }
                self.idx += 1;
                continue;
            }
            if self.double {
                if byte == b'"' && self.bytes.get(self.idx.wrapping_sub(1)).copied() != Some(b'\\')
                {
                    self.double = false;
                }
                self.idx += 1;
                continue;
            }
            if self.backtick {
                if byte == b'`' {
                    self.backtick = false;
                }
                self.idx += 1;
                continue;
            }
            match byte {
                b'\'' => self.single = true,
                b'"' => self.double = true,
                b'`' => self.backtick = true,
                b'/' if self.bytes.get(self.idx + 1) == Some(&b'*')
                    && self.bytes.get(self.idx + 2) == Some(&b'+') =>
                {
                    let start = self.idx + 3;
                    let end_rel = self.sql.get(start..).and_then(|s| s.find("*/"));
                    let end = match end_rel {
                        Some(off) => start + off,
                        None => return None,
                    };
                    self.idx = end + 2;
                    return self.sql.get(start..end);
                }
                _ => {}
            }
            self.idx += 1;
        }
        None
    }
}

fn hint_enables_allow_throw_exception(hint: &str) -> bool {
    let lower = hint.to_ascii_lowercase();
    // Look for `sql_mode` followed by `=` (possibly with spaces) followed by
    // a quoted value containing `allow_throw_exception`.
    let key = "sql_mode";
    let mut search = 0usize;
    while let Some(rel) = lower[search..].find(key) {
        let key_idx = search + rel;
        let mut cursor = key_idx + key.len();
        while lower
            .as_bytes()
            .get(cursor)
            .is_some_and(u8::is_ascii_whitespace)
        {
            cursor += 1;
        }
        if lower.as_bytes().get(cursor) != Some(&b'=') {
            search = key_idx + key.len();
            continue;
        }
        cursor += 1;
        while lower
            .as_bytes()
            .get(cursor)
            .is_some_and(u8::is_ascii_whitespace)
        {
            cursor += 1;
        }
        // Take up to the next quote, comma, or closing paren as the value.
        let value_start = cursor;
        let mut quote = None;
        if matches!(lower.as_bytes().get(cursor), Some(&b'\'' | &b'"')) {
            quote = Some(lower.as_bytes()[cursor]);
            cursor += 1;
        }
        let value_inner_start = cursor;
        while cursor < lower.len() {
            let b = lower.as_bytes()[cursor];
            if let Some(q) = quote {
                if b == q {
                    break;
                }
            } else if matches!(b, b',' | b')' | b' ' | b'\t' | b'\n') {
                break;
            }
            cursor += 1;
        }
        let value = &lower[value_inner_start..cursor];
        if value.contains("allow_throw_exception") {
            return true;
        }
        search = value_start.max(key_idx + key.len());
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_quoted_sql_mode_allow_throw_exception() {
        let sql = "SELECT /*+SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION')*/ 1";
        assert!(extract_allow_throw_exception(sql));
    }

    #[test]
    fn detects_double_quoted_value() {
        let sql = r#"SELECT /*+ SET_VAR(sql_mode = "allow_throw_exception") */ 1"#;
        assert!(extract_allow_throw_exception(sql));
    }

    #[test]
    fn ignores_hint_inside_string_literal() {
        // The fake hint is INSIDE a string literal — should not be picked up.
        let sql = "SELECT '/*+SET_VAR(sql_mode=ALLOW_THROW_EXCEPTION)*/' AS x";
        assert!(!extract_allow_throw_exception(sql));
    }

    #[test]
    fn ignores_unrelated_hint() {
        let sql = "SELECT /*+ SET_VAR(recursive_cte_max_depth=10) */ 1";
        assert!(!extract_allow_throw_exception(sql));
    }

    #[test]
    fn returns_false_for_no_hint() {
        assert!(!extract_allow_throw_exception("SELECT 1"));
    }

    #[test]
    fn detects_among_other_set_vars() {
        let sql = "SELECT /*+ SET_VAR(query_timeout=60, sql_mode='ALLOW_THROW_EXCEPTION', x=1) */ 1";
        assert!(extract_allow_throw_exception(sql));
    }
}
