//! Parser probe + parse for `TRUNCATE TABLE <name>`.
//!
//! Only the bare form `TRUNCATE TABLE <table>` (with optional branch suffix
//! `t.branch_<name>`) is accepted. PARTITION and WHERE clauses are explicitly
//! rejected with descriptive errors.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use super::{convert_object_name, peek_word_eq};
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::parser::ast::{ObjectName, Statement};

/// Return `true` when the parser is positioned at `TRUNCATE TABLE …`.
pub(crate) fn looks_like_truncate_table(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::TRUNCATE) && peek_word_eq(parser, 1, "TABLE")
}

/// Parse `TRUNCATE TABLE <name>` into a `Statement::Truncate`.
///
/// Branch suffix (`t.branch_<name>`) is resolved and stripped from the name;
/// tag suffixes are rejected. PARTITION and WHERE trailing tokens are rejected.
pub(crate) fn parse_truncate_table(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::TRUNCATE)
        .map_err(|e| format!("TRUNCATE: {e}"))?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| format!("TRUNCATE TABLE: {e}"))?;

    let raw = parser
        .parse_object_name(false)
        .map_err(|e| format!("TRUNCATE TABLE: {e}"))?;
    let object_name = convert_object_name(raw)?;
    let parts = object_name.parts.clone();

    // Reject PARTITION / WHERE trailing tokens before branch resolution so the
    // error messages are always produced regardless of the table name form.
    if parser.parse_keyword(Keyword::PARTITION) {
        return Err("TRUNCATE TABLE PARTITION (...) is not supported".to_string());
    }
    if parser.parse_keyword(Keyword::WHERE) {
        return Err("TRUNCATE TABLE WHERE <predicate> is not supported".to_string());
    }

    let (stripped_parts, ref_suffix) = split_ref_suffix(&parts);
    let (final_name, target_ref) = match ref_suffix {
        Some(IcebergRefSuffix::Tag(t)) => {
            return Err(format!(
                "TRUNCATE TABLE: tag '{t}' is read-only; use a branch as target"
            ));
        }
        Some(IcebergRefSuffix::Branch(b)) => (
            ObjectName {
                parts: stripped_parts,
            },
            b,
        ),
        None => (ObjectName { parts }, "main".to_string()),
    };

    Ok(Statement::Truncate {
        name: final_name,
        target_ref,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::Statement;
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse_one(sql: &str) -> Result<Statement, String> {
        let dialect = StarRocksDialect;
        let mut p = Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(|e| e.to_string())?;
        parse_truncate_table(&mut p)
    }

    #[test]
    fn parse_truncate_table_basic() {
        let stmt = parse_one("TRUNCATE TABLE t").expect("parse");
        match stmt {
            Statement::Truncate { name, target_ref } => {
                assert_eq!(name.parts, vec!["t".to_string()]);
                assert_eq!(target_ref, "main");
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn parse_truncate_table_branch() {
        let stmt = parse_one("TRUNCATE TABLE t.branch_dev").expect("parse");
        match stmt {
            Statement::Truncate { name, target_ref } => {
                assert_eq!(name.parts, vec!["t".to_string()]);
                assert_eq!(target_ref, "dev");
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn parse_truncate_table_partition_rejected() {
        let err = parse_one("TRUNCATE TABLE t PARTITION (p=1)").unwrap_err();
        assert!(
            err.to_lowercase().contains("partition"),
            "expected PARTITION rejection, got {err}",
        );
    }

    #[test]
    fn parse_truncate_table_where_rejected() {
        let err = parse_one("TRUNCATE TABLE t WHERE c=1").unwrap_err();
        assert!(
            err.to_lowercase().contains("where"),
            "expected WHERE rejection, got {err}",
        );
    }

    #[test]
    fn parse_truncate_table_two_part() {
        let stmt = parse_one("TRUNCATE TABLE mydb.mytable").expect("parse");
        match stmt {
            Statement::Truncate { name, target_ref } => {
                assert_eq!(name.parts, vec!["mydb".to_string(), "mytable".to_string()]);
                assert_eq!(target_ref, "main");
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn parse_truncate_table_three_part() {
        let stmt = parse_one("TRUNCATE TABLE mycat.mydb.mytable").expect("parse");
        match stmt {
            Statement::Truncate { name, target_ref } => {
                assert_eq!(
                    name.parts,
                    vec![
                        "mycat".to_string(),
                        "mydb".to_string(),
                        "mytable".to_string()
                    ]
                );
                assert_eq!(target_ref, "main");
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn parse_truncate_table_three_part_with_branch() {
        let stmt = parse_one("TRUNCATE TABLE mycat.mydb.mytable.branch_dev").expect("parse");
        match stmt {
            Statement::Truncate { name, target_ref } => {
                assert_eq!(
                    name.parts,
                    vec![
                        "mycat".to_string(),
                        "mydb".to_string(),
                        "mytable".to_string()
                    ]
                );
                assert_eq!(target_ref, "dev");
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }
}
