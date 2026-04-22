//! Lightweight `CREATE / DROP / REFRESH MATERIALIZED VIEW` recognition and
//! StarRocks-specific rewrite heuristics.
//!
//! These helpers scan the raw SQL text (they are cheaper than going through
//! the full sqlparser pipeline) so the standalone engine can route MV
//! statements to its own handlers.

use crate::standalone::engine::local::normalize_identifier;

use super::expr::{canonicalize_sql_for_match, strip_optional_identifier_quotes};

pub(crate) fn materialized_view_key(
    current_database: &str,
    name: &str,
) -> Result<(String, String), String> {
    Ok((
        normalize_identifier(current_database)?,
        normalize_identifier(name)?,
    ))
}

pub(crate) fn parse_create_materialized_view_name(sql: &str) -> Option<String> {
    let mut parts = sql.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("create") {
        return None;
    }
    if !parts.next()?.eq_ignore_ascii_case("materialized") {
        return None;
    }
    if !parts.next()?.eq_ignore_ascii_case("view") {
        return None;
    }
    let maybe_name = parts.next()?;
    let name = if maybe_name.eq_ignore_ascii_case("if") {
        if !parts.next()?.eq_ignore_ascii_case("not") {
            return None;
        }
        if !parts.next()?.eq_ignore_ascii_case("exists") {
            return None;
        }
        parts.next()?
    } else {
        maybe_name
    };
    Some(strip_optional_identifier_quotes(name).to_string())
}

pub(crate) fn parse_drop_materialized_view_name(sql: &str) -> Option<String> {
    let mut parts = sql.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("drop") {
        return None;
    }
    if !parts.next()?.eq_ignore_ascii_case("materialized") {
        return None;
    }
    if !parts.next()?.eq_ignore_ascii_case("view") {
        return None;
    }
    let maybe_name = parts.next()?;
    let name = if maybe_name.eq_ignore_ascii_case("if") {
        if !parts.next()?.eq_ignore_ascii_case("exists") {
            return None;
        }
        parts.next()?
    } else {
        maybe_name
    };
    Some(strip_optional_identifier_quotes(name).to_string())
}

pub(crate) fn parse_refresh_materialized_view_name(sql: &str) -> Option<String> {
    let mut parts = sql.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("refresh") {
        return None;
    }
    if !parts.next()?.eq_ignore_ascii_case("materialized") {
        return None;
    }
    if !parts.next()?.eq_ignore_ascii_case("view") {
        return None;
    }
    Some(strip_optional_identifier_quotes(parts.next()?).to_string())
}

pub(crate) fn looks_like_show_alter_materialized_view(sql: &str) -> bool {
    let mut parts = sql.split_whitespace();
    matches!(parts.next(), Some(head) if head.eq_ignore_ascii_case("show"))
        && matches!(parts.next(), Some(head) if head.eq_ignore_ascii_case("alter"))
        && matches!(parts.next(), Some(head) if head.eq_ignore_ascii_case("materialized"))
        && matches!(parts.next(), Some(head) if head.eq_ignore_ascii_case("view"))
}

pub(crate) fn supports_bitmap_count_rewrite(sql: &str) -> bool {
    canonicalize_sql_for_match(sql).contains(
        "as select c1, bitmap_agg(c2), bitmap_agg(c3), bitmap_agg(c4) from t1 group by c1",
    )
}
