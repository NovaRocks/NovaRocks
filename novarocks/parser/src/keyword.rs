// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! MySQL/StarRocks keyword classification for the parser foundation.

use crate::Keyword;

/// Whether a recognized keyword is reserved in the initial parser dialect.
///
/// The lexer records only [`Keyword`] in a token. This classification is kept
/// separately so grammar families can decide which non-reserved words they
/// accept as identifiers without changing lexical recognition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeywordClass {
    Reserved,
    NonReserved,
}

/// Looks up a keyword using ASCII-only case folding.
///
/// Identifiers retain their original source spelling; only the comparison here
/// is case-insensitive.
pub fn lookup(word: &str) -> Option<Keyword> {
    KEYWORDS
        .iter()
        .find(|(spelling, _)| word.eq_ignore_ascii_case(spelling))
        .map(|(_, keyword)| *keyword)
}

/// Returns the reservedness recorded for a recognized keyword.
pub const fn class(keyword: Keyword) -> KeywordClass {
    match keyword {
        Keyword::Add
        | Keyword::Alter
        | Keyword::Analyze
        | Keyword::And
        | Keyword::As
        | Keyword::Between
        | Keyword::Call
        | Keyword::Cancel
        | Keyword::Case
        | Keyword::Cast
        | Keyword::Create
        | Keyword::Cross
        | Keyword::Distinct
        | Keyword::Drop
        | Keyword::Else
        | Keyword::End
        | Keyword::Except
        | Keyword::Explain
        | Keyword::Fetch
        | Keyword::False
        | Keyword::From
        | Keyword::For
        | Keyword::Group
        | Keyword::Having
        | Keyword::In
        | Keyword::Inner
        | Keyword::Intersect
        | Keyword::Is
        | Keyword::Join
        | Keyword::Kill
        | Keyword::Like
        | Keyword::Limit
        | Keyword::Not
        | Keyword::Null
        | Keyword::On
        | Keyword::Only
        | Keyword::Or
        | Keyword::Order
        | Keyword::Outer
        | Keyword::Select
        | Keyword::Show
        | Keyword::Then
        | Keyword::True
        | Keyword::Truncate
        | Keyword::Union
        | Keyword::Using
        | Keyword::Values
        | Keyword::When
        | Keyword::Where
        | Keyword::With => KeywordClass::Reserved,
        _ => KeywordClass::NonReserved,
    }
}

const KEYWORDS: &[(&str, Keyword)] = &[
    ("ADD", Keyword::Add),
    ("ALTER", Keyword::Alter),
    ("ANALYZE", Keyword::Analyze),
    ("AND", Keyword::And),
    ("ANTI", Keyword::Anti),
    ("ANY", Keyword::Any),
    ("ARRAY", Keyword::Array),
    ("AS", Keyword::As),
    ("ASC", Keyword::Asc),
    ("AT", Keyword::At),
    ("ASYNC", Keyword::Async),
    ("BACKEND", Keyword::Backend),
    ("BACKENDS", Keyword::Backends),
    ("BASIC", Keyword::Basic),
    ("BETWEEN", Keyword::Between),
    ("BRANCH", Keyword::Branch),
    ("BUCKETS", Keyword::Buckets),
    ("BY", Keyword::By),
    ("CALL", Keyword::Call),
    ("CANCEL", Keyword::Cancel),
    ("CATALOG", Keyword::Catalog),
    ("CASE", Keyword::Case),
    ("CAST", Keyword::Cast),
    ("COLUMN", Keyword::Column),
    ("COLUMNS", Keyword::Columns),
    ("COMMENT", Keyword::Comment),
    ("COSTS", Keyword::Costs),
    ("CREATE", Keyword::Create),
    ("CROSS", Keyword::Cross),
    ("CUBE", Keyword::Cube),
    ("CURRENT", Keyword::Current),
    ("DATABASE", Keyword::Database),
    ("DATE", Keyword::Date),
    ("DEFAULT", Keyword::Default),
    ("DESC", Keyword::Desc),
    ("DISTINCT", Keyword::Distinct),
    ("DROP", Keyword::Drop),
    ("ELSE", Keyword::Else),
    ("END", Keyword::End),
    ("ESCAPE", Keyword::Escape),
    ("EXCEPT", Keyword::Except),
    ("EXISTS", Keyword::Exists),
    ("EXPIRE", Keyword::Expire),
    ("EXPLAIN", Keyword::Explain),
    ("EXTERNAL", Keyword::External),
    ("FALSE", Keyword::False),
    ("FETCH", Keyword::Fetch),
    ("FILES", Keyword::Files),
    ("FILTER", Keyword::Filter),
    ("FIRST", Keyword::First),
    ("FOLLOWING", Keyword::Following),
    ("FOR", Keyword::For),
    ("FORCE", Keyword::Force),
    ("FROM", Keyword::From),
    ("FULL", Keyword::Full),
    ("GROUP", Keyword::Group),
    ("GROUPING", Keyword::Grouping),
    ("GROUPS", Keyword::Groups),
    ("HAVING", Keyword::Having),
    ("HISTOGRAM", Keyword::Histogram),
    ("IF", Keyword::If),
    ("IGNORE", Keyword::Ignore),
    ("ILIKE", Keyword::Ilike),
    ("IN", Keyword::In),
    ("INNER", Keyword::Inner),
    ("INTERSECT", Keyword::Intersect),
    ("INTERVAL", Keyword::Interval),
    ("IS", Keyword::Is),
    ("JOBS", Keyword::Jobs),
    ("JOIN", Keyword::Join),
    ("KILL", Keyword::Kill),
    ("LATERAL", Keyword::Lateral),
    ("LAST", Keyword::Last),
    ("LEFT", Keyword::Left),
    ("LIKE", Keyword::Like),
    ("LIMIT", Keyword::Limit),
    ("LOGICAL", Keyword::Logical),
    ("MANIFESTS", Keyword::Manifests),
    ("MATERIALIZED", Keyword::Materialized),
    ("META", Keyword::Meta),
    ("NATURAL", Keyword::Natural),
    ("NEXT", Keyword::Next),
    ("NOT", Keyword::Not),
    ("NULL", Keyword::Null),
    ("NULLS", Keyword::Nulls),
    ("OF", Keyword::Of),
    ("OFFSET", Keyword::Offset),
    ("ON", Keyword::On),
    ("ONLY", Keyword::Only),
    ("ORDER", Keyword::Order),
    ("ORDINALITY", Keyword::Ordinality),
    ("OR", Keyword::Or),
    ("ORPHAN", Keyword::Orphan),
    ("OUTER", Keyword::Outer),
    ("OVER", Keyword::Over),
    ("PARTITION", Keyword::Partition),
    ("POSITION", Keyword::Position),
    ("PRECEDING", Keyword::Preceding),
    ("PERCENT", Keyword::Percent),
    ("PROPERTIES", Keyword::Properties),
    ("QUALIFY", Keyword::Qualify),
    ("RANGE", Keyword::Range),
    ("RECURSIVE", Keyword::Recursive),
    ("REFRESH", Keyword::Refresh),
    ("REMOVE", Keyword::Remove),
    ("RESPECT", Keyword::Respect),
    ("REWRITE", Keyword::Rewrite),
    ("RIGHT", Keyword::Right),
    ("RLIKE", Keyword::Rlike),
    ("ROLLUP", Keyword::Rollup),
    ("ROW", Keyword::Row),
    ("ROWS", Keyword::Rows),
    ("SAMPLE", Keyword::Sample),
    ("SELECT", Keyword::Select),
    ("SEMI", Keyword::Semi),
    ("SET", Keyword::Set),
    ("SETS", Keyword::Sets),
    ("SHOW", Keyword::Show),
    ("SIMILAR", Keyword::Similar),
    ("SOME", Keyword::Some),
    ("SNAPSHOTS", Keyword::Snapshots),
    ("STATS", Keyword::Stats),
    ("STRUCT", Keyword::Struct),
    ("SUBSTRING", Keyword::Substring),
    ("SYNC", Keyword::Sync),
    ("SYSTEM", Keyword::System),
    ("SYSTEM_TIME", Keyword::SystemTime),
    ("TABLE", Keyword::Table),
    ("TABLES", Keyword::Tables),
    ("TAG", Keyword::Tag),
    ("THEN", Keyword::Then),
    ("TIME", Keyword::Time),
    ("TIMESTAMP", Keyword::Timestamp),
    ("TIES", Keyword::Ties),
    ("TO", Keyword::To),
    ("TRUE", Keyword::True),
    ("TRUNCATE", Keyword::Truncate),
    ("TRY_CAST", Keyword::TryCast),
    ("UNBOUNDED", Keyword::Unbounded),
    ("UNION", Keyword::Union),
    ("UNNEST", Keyword::Unnest),
    ("UNSET", Keyword::Unset),
    ("UNKNOWN", Keyword::Unknown),
    ("USING", Keyword::Using),
    ("VERBOSE", Keyword::Verbose),
    ("VERSION", Keyword::Version),
    ("VALUES", Keyword::Values),
    ("VIEW", Keyword::View),
    ("VIEWS", Keyword::Views),
    ("WITH", Keyword::With),
    ("WHEN", Keyword::When),
    ("WHERE", Keyword::Where),
    ("WINDOW", Keyword::Window),
    ("WITHIN", Keyword::Within),
    ("ZONE", Keyword::Zone),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_is_ascii_case_insensitive_without_normalizing_identifiers() {
        assert_eq!(lookup("sHoW"), Some(Keyword::Show));
        assert_eq!(lookup("backends"), Some(Keyword::Backends));
        assert_eq!(lookup("SeLeCt"), Some(Keyword::Select));
        assert_eq!(lookup("try_cast"), Some(Keyword::TryCast));
        assert_eq!(lookup("backends_x"), None);
        assert_eq!(class(Keyword::Backends), KeywordClass::NonReserved);
        assert_eq!(class(Keyword::Window), KeywordClass::NonReserved);
        assert_eq!(class(Keyword::Select), KeywordClass::Reserved);
        assert_eq!(class(Keyword::Show), KeywordClass::Reserved);
    }
}
