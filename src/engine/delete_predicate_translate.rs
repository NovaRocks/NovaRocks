// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

//! Translate a managed-lake DELETE's WHERE clause into
//! `DeletePredicateTerms` — a conjunctive list of column-op-literal /
//! IN / IS NULL predicates with StarRocks-compatible string-encoded
//! literal values. Mirrors StarRocks DeleteAnalyzer restrictions:
//! AND-only, no OR/functions/subqueries/joins; non-DUP tables require
//! key columns; floating-point columns reject `=`.

use sqlparser::ast as sqlast;

use crate::sql::catalog::ColumnDef;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    /// Swap the operand sides: `a OP b` becomes `b OP.flipped() a`.
    /// Used when the column is on the right side of a comparison so the
    /// resulting `BinaryTerm` (which is always `column OP value`) encodes
    /// the same predicate.
    fn flipped(self) -> Self {
        match self {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
            CmpOp::Eq | CmpOp::Ne => self,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BinaryTerm {
    pub column: String,
    pub op: CmpOp,
    /// StarRocks BinaryPredicatePb.value, already serialized per column type.
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct InTerm {
    pub column: String,
    pub is_not_in: bool,
    pub values: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct IsNullTerm {
    pub column: String,
    pub is_not_null: bool,
}

#[derive(Clone, Debug, Default)]
pub struct DeletePredicateTerms {
    pub binary: Vec<BinaryTerm>,
    pub in_list: Vec<InTerm>,
    pub is_null: Vec<IsNullTerm>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeysType {
    Dup,
    Unique,
    Agg,
}

impl KeysType {
    pub fn from_meta_str(s: &str) -> Option<Self> {
        match s {
            "DUP_KEYS" => Some(Self::Dup),
            "UNIQUE_KEYS" => Some(Self::Unique),
            "AGG_KEYS" => Some(Self::Agg),
            _ => None,
        }
    }
    fn requires_key_columns(self) -> bool {
        matches!(self, Self::Unique | Self::Agg)
    }
}

pub fn translate_to_delete_predicate(
    where_expr: &sqlast::Expr,
    schema: &[ColumnDef],
    keys: &[String],
    keys_type: KeysType,
) -> Result<DeletePredicateTerms, String> {
    let mut terms = DeletePredicateTerms::default();
    let atoms = flatten_and(where_expr)?;
    for atom in atoms {
        translate_atom(atom, schema, keys, keys_type, &mut terms)?;
    }
    Ok(terms)
}

fn flatten_and(expr: &sqlast::Expr) -> Result<Vec<&sqlast::Expr>, String> {
    let mut out = Vec::new();
    fn walk<'a>(e: &'a sqlast::Expr, out: &mut Vec<&'a sqlast::Expr>) -> Result<(), String> {
        match e {
            sqlast::Expr::BinaryOp {
                op: sqlast::BinaryOperator::And,
                left,
                right,
            } => {
                walk(left, out)?;
                walk(right, out)?;
                Ok(())
            }
            sqlast::Expr::BinaryOp {
                op: sqlast::BinaryOperator::Or,
                ..
            } => Err("DELETE on this table model does not support OR; \
                 use only AND of comparisons / IN / IS NULL"
                .to_string()),
            sqlast::Expr::Nested(inner) => walk(inner, out),
            _ => {
                out.push(e);
                Ok(())
            }
        }
    }
    walk(expr, &mut out)?;
    Ok(out)
}

fn translate_atom(
    atom: &sqlast::Expr,
    schema: &[ColumnDef],
    keys: &[String],
    keys_type: KeysType,
    out: &mut DeletePredicateTerms,
) -> Result<(), String> {
    match atom {
        sqlast::Expr::BinaryOp { left, op, right } => {
            let cmp = match op {
                sqlast::BinaryOperator::Eq => CmpOp::Eq,
                sqlast::BinaryOperator::NotEq => CmpOp::Ne,
                sqlast::BinaryOperator::Lt => CmpOp::Lt,
                sqlast::BinaryOperator::LtEq => CmpOp::Le,
                sqlast::BinaryOperator::Gt => CmpOp::Gt,
                sqlast::BinaryOperator::GtEq => CmpOp::Ge,
                other => {
                    return Err(format!(
                        "DELETE WHERE supports comparison / IN / IS NULL only; got {other:?}"
                    ));
                }
            };
            let (col_name, lit_expr, swapped) = extract_col_lit(left, right)?;
            let cmp = if swapped { cmp.flipped() } else { cmp };
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            if is_float_type(&column.data_type) && matches!(cmp, CmpOp::Eq | CmpOp::Ne) {
                return Err(format!(
                    "Don't support float column '{}' in delete condition",
                    col_name
                ));
            }
            let value = serialize_literal(lit_expr, &column.data_type, &col_name)?;
            out.binary.push(BinaryTerm {
                column: col_name,
                op: cmp,
                value,
            });
            Ok(())
        }
        sqlast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col_name = expr_to_col_name(expr)?;
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            let values = list
                .iter()
                .map(|e| serialize_literal(e, &column.data_type, &col_name))
                .collect::<Result<Vec<_>, _>>()?;
            out.in_list.push(InTerm {
                column: col_name,
                is_not_in: *negated,
                values,
            });
            Ok(())
        }
        sqlast::Expr::IsNull(inner) => {
            let col_name = expr_to_col_name(inner)?;
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            out.is_null.push(IsNullTerm {
                column: col_name,
                is_not_null: false,
            });
            Ok(())
        }
        sqlast::Expr::IsNotNull(inner) => {
            let col_name = expr_to_col_name(inner)?;
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            out.is_null.push(IsNullTerm {
                column: col_name,
                is_not_null: true,
            });
            Ok(())
        }
        sqlast::Expr::Nested(inner) => translate_atom(inner, schema, keys, keys_type, out),
        other => Err(format!(
            "DELETE WHERE atom must be col-op-lit / IN / IS NULL; got {other:?}"
        )),
    }
}

fn extract_col_lit<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Result<(String, &'a sqlast::Expr, bool /* swapped */), String> {
    if let Ok(name) = expr_to_col_name(left) {
        return Ok((name, right, false));
    }
    if let Ok(name) = expr_to_col_name(right) {
        return Ok((name, left, true));
    }
    Err("DELETE WHERE comparison must have exactly one column and one literal side".to_string())
}

fn expr_to_col_name(e: &sqlast::Expr) -> Result<String, String> {
    match e {
        sqlast::Expr::Identifier(id) => Ok(id.value.to_lowercase()),
        sqlast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.to_lowercase())
            .ok_or_else(|| "empty compound identifier".to_string()),
        other => Err(format!("expected column reference, got {other:?}")),
    }
}

fn column_or_err(schema: &[ColumnDef], name: &str) -> Result<ColumnDef, String> {
    schema
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
        .cloned()
        .ok_or_else(|| format!("column '{name}' not found in table schema"))
}

fn check_keys(
    name: &str,
    _column: &ColumnDef,
    keys: &[String],
    keys_type: KeysType,
) -> Result<(), String> {
    if keys_type.requires_key_columns() && !keys.iter().any(|k| k.eq_ignore_ascii_case(name)) {
        return Err(format!(
            "Where clause only supports key column on this table model; '{name}' is not a key column"
        ));
    }
    Ok(())
}

fn is_float_type(ty: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    matches!(ty, DataType::Float32 | DataType::Float64)
}

/// Minimal literal serializer; full per-type serialization lands in M3.T2.
fn serialize_literal(
    lit_expr: &sqlast::Expr,
    _column_type: &arrow::datatypes::DataType,
    column_name: &str,
) -> Result<String, String> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    let v = match lit_expr {
        Expr::Value(ValueWithSpan { value, .. }) => value,
        Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr,
        } => {
            if let Expr::Value(ValueWithSpan {
                value: Value::Number(n, _),
                ..
            }) = expr.as_ref()
            {
                return Ok(format!("-{n}"));
            }
            return Err(format!(
                "unsupported negated literal for column '{column_name}'"
            ));
        }
        other => {
            return Err(format!(
                "literal value expected for column '{column_name}', got {other:?}"
            ));
        }
    };
    match v {
        Value::Number(n, _) => Ok(n.clone()),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
        Value::Boolean(b) => Ok(if *b { "1".into() } else { "0".into() }),
        Value::Null => Err(format!(
            "NULL literal in DELETE WHERE for column '{column_name}'; use IS NULL/IS NOT NULL"
        )),
        other => Err(format!(
            "unsupported literal for column '{column_name}': {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn dup_schema_int_str() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                write_default: None,
            },
        ]
    }

    fn parse_where(sql: &str) -> sqlast::Expr {
        let stmt = Parser::parse_sql(&MySqlDialect {}, &format!("DELETE FROM t WHERE {sql}"))
            .expect("parse")
            .into_iter()
            .next()
            .expect("at least one statement");
        match stmt {
            sqlast::Statement::Delete(d) => d.selection.expect("WHERE clause"),
            other => panic!("unexpected stmt {other:?}"),
        }
    }

    #[test]
    fn binary_eq_int_lit() {
        let w = parse_where("id = 42");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary.len(), 1);
        assert_eq!(t.binary[0].column, "id");
        assert_eq!(t.binary[0].op, CmpOp::Eq);
        assert_eq!(t.binary[0].value, "42");
    }

    #[test]
    fn binary_ne_string_lit() {
        let w = parse_where("name != 'alice'");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary[0].op, CmpOp::Ne);
        assert_eq!(t.binary[0].value, "alice");
    }

    #[test]
    fn and_combination() {
        let w = parse_where("id = 1 AND name = 'a'");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary.len(), 2);
    }

    #[test]
    fn or_rejected() {
        let w = parse_where("id = 1 OR id = 2");
        let err = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .unwrap_err();
        assert!(err.contains("OR"), "got: {err}");
    }

    #[test]
    fn unique_non_key_rejected() {
        let w = parse_where("name = 'x'");
        let err = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Unique,
        )
        .unwrap_err();
        assert!(err.contains("key column"), "got: {err}");
    }

    #[test]
    fn dup_non_key_allowed() {
        let w = parse_where("name = 'x'");
        translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("dup allows non-key");
    }

    #[test]
    fn binary_gt_right_side_column() {
        // Regression test for the I1 bug: `5 < id` must translate to `id > 5`,
        // not `id < 5`. The comparator must be flipped when the column is on
        // the right side of a non-symmetric comparison.
        let w = parse_where("5 < id");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary[0].column, "id");
        assert_eq!(t.binary[0].op, CmpOp::Gt, "comparator must be flipped");
        assert_eq!(t.binary[0].value, "5");
    }

    #[test]
    fn binary_le_right_side_column_flips_to_ge() {
        let w = parse_where("100 >= id");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary[0].op, CmpOp::Le);
    }

    #[test]
    fn in_list_basic() {
        let w = parse_where("id IN (1, 2, 3)");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.in_list.len(), 1);
        assert_eq!(t.in_list[0].column, "id");
        assert!(!t.in_list[0].is_not_in);
        assert_eq!(t.in_list[0].values, vec!["1", "2", "3"]);
    }

    #[test]
    fn not_in_list() {
        let w = parse_where("id NOT IN (1, 2)");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert!(t.in_list[0].is_not_in);
    }

    #[test]
    fn is_null_term() {
        let w = parse_where("name IS NULL");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.is_null.len(), 1);
        assert_eq!(t.is_null[0].column, "name");
        assert!(!t.is_null[0].is_not_null);
    }

    #[test]
    fn is_not_null_term() {
        let w = parse_where("name IS NOT NULL");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert!(t.is_null[0].is_not_null);
    }

    #[test]
    fn float_column_equality_rejected() {
        // Schema with a Float64 column to trigger is_float_type rejection.
        let schema = vec![ColumnDef {
            name: "v".to_string(),
            data_type: arrow::datatypes::DataType::Float64,
            nullable: false,
            write_default: None,
        }];
        let w = parse_where("v = 1.0");
        let err = translate_to_delete_predicate(&w, &schema, &["v".to_string()], KeysType::Dup)
            .unwrap_err();
        assert!(err.contains("float"), "got: {err}");
    }

    #[test]
    fn unknown_column_rejected() {
        let w = parse_where("nonexistent = 1");
        let err = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .unwrap_err();
        assert!(err.contains("not found"), "got: {err}");
    }

    #[test]
    fn agg_keys_non_key_rejected() {
        // Mirror unique_non_key_rejected but for AGG_KEYS.
        let w = parse_where("name = 'x'");
        let err = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Agg,
        )
        .unwrap_err();
        assert!(err.contains("key column"), "got: {err}");
    }
}
