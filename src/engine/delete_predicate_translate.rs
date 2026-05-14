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

/// Serialize a SQL literal into the string form expected by
/// StarRocks `BinaryPredicatePb.value` / `InPredicatePb.values`.
/// Conventions:
///  - INT* / UINT*: reformat via i128/u128 so `1.5` for an INT column is rejected.
///  - DECIMAL(p, s): pad fractional part to scale; reject if more digits than scale.
///  - DATE: validate / canonicalize to `YYYY-MM-DD`.
///  - TIMESTAMP(unit): validate `YYYY-MM-DD HH:MM:SS[.frac]`, zero-pad fractional to
///    the unit's scale (6 digits for Microsecond), reject overflow.
///  - BOOL: `0`/`1`.
///  - FLOAT/DOUBLE: pass through canonical f64 (only reachable via `<` / `<=` / `>` / `>=`).
///  - UTF8/VARCHAR: pass through verbatim.
fn serialize_literal(
    lit_expr: &sqlast::Expr,
    column_type: &arrow::datatypes::DataType,
    column_name: &str,
) -> Result<String, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    use sqlparser::ast::{Expr, Value, ValueWithSpan};

    let (raw, was_negated): (String, bool) = match lit_expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::Number(s, _) => (s.clone(), false),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => (s.clone(), false),
            Value::Boolean(b) => return Ok(if *b { "1".into() } else { "0".into() }),
            Value::Null => {
                return Err(format!(
                    "NULL literal in DELETE WHERE for column '{column_name}'; \
                     use IS NULL / IS NOT NULL"
                ));
            }
            other => {
                return Err(format!(
                    "unsupported literal for column '{column_name}': {other:?}"
                ));
            }
        },
        Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }) => (s.clone(), true),
            _ => {
                return Err(format!(
                    "unsupported negated literal for column '{column_name}'"
                ));
            }
        },
        other => {
            return Err(format!(
                "literal value expected for column '{column_name}', got {other:?}"
            ));
        }
    };

    match column_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let signed = if was_negated {
                format!("-{raw}")
            } else {
                raw.clone()
            };
            let parsed: i128 = signed.parse().map_err(|e| {
                format!("invalid integer literal for column '{column_name}': {raw} ({e})")
            })?;
            Ok(parsed.to_string())
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            if was_negated {
                return Err(format!(
                    "negative integer literal for unsigned column '{column_name}'"
                ));
            }
            let parsed: u128 = raw.parse().map_err(|e| {
                format!("invalid integer literal for column '{column_name}': {raw} ({e})")
            })?;
            Ok(parsed.to_string())
        }
        DataType::Decimal128(_p, s) | DataType::Decimal256(_p, s) => {
            let signed = if was_negated {
                format!("-{raw}")
            } else {
                raw.clone()
            };
            let (int_part, frac_part) = match signed.split_once('.') {
                Some((i, f)) => (i.to_string(), f.to_string()),
                None => (signed.clone(), String::new()),
            };
            let scale = *s as usize;
            if frac_part.len() > scale {
                return Err(format!(
                    "decimal literal for column '{column_name}' has {} fractional digits but scale is {}",
                    frac_part.len(),
                    scale
                ));
            }
            if scale == 0 {
                Ok(int_part)
            } else {
                let mut f = frac_part;
                while f.len() < scale {
                    f.push('0');
                }
                Ok(format!("{int_part}.{f}"))
            }
        }
        DataType::Date32 | DataType::Date64 => chrono::NaiveDate::parse_from_str(&raw, "%Y-%m-%d")
            .map(|d| d.format("%Y-%m-%d").to_string())
            .map_err(|e| format!("invalid date literal for column '{column_name}': {raw} ({e})")),
        DataType::Timestamp(unit, _) => {
            let scale_digits = match unit {
                TimeUnit::Second => 0usize,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            let (datepart, mut fracpart) = split_datetime_fraction(&raw, column_name)?;
            if fracpart.len() > scale_digits {
                return Err(format!(
                    "Datetime literal '{raw}' has {} fractional digits but column \
                     '{column_name}' supports {}",
                    fracpart.len(),
                    scale_digits
                ));
            }
            while fracpart.len() < scale_digits {
                fracpart.push('0');
            }
            chrono::NaiveDateTime::parse_from_str(&datepart, "%Y-%m-%d %H:%M:%S").map_err(|e| {
                format!("invalid datetime literal for column '{column_name}': {raw} ({e})")
            })?;
            if scale_digits > 0 {
                Ok(format!("{datepart}.{fracpart}"))
            } else {
                Ok(datepart)
            }
        }
        DataType::Boolean => match raw.as_str() {
            "0" | "false" | "FALSE" => Ok("0".to_string()),
            "1" | "true" | "TRUE" => Ok("1".to_string()),
            _ => Err(format!(
                "invalid boolean literal for column '{column_name}': {raw}"
            )),
        },
        DataType::Utf8 | DataType::LargeUtf8 => Ok(raw),
        DataType::Float32 | DataType::Float64 => {
            // Float columns reject `=`/`!=` upstream in `translate_atom`, so
            // this branch only sees range comparisons. Canonicalize via f64.
            let signed = if was_negated {
                format!("-{raw}")
            } else {
                raw.clone()
            };
            let parsed: f64 = signed.parse().map_err(|e| {
                format!("invalid float literal for column '{column_name}': {raw} ({e})")
            })?;
            Ok(parsed.to_string())
        }
        other => Err(format!(
            "DELETE WHERE: unsupported column type {other:?} for column '{column_name}'"
        )),
    }
}

fn split_datetime_fraction(raw: &str, column_name: &str) -> Result<(String, String), String> {
    match raw.split_once('.') {
        Some((d, f)) => {
            if !f.chars().all(|c| c.is_ascii_digit()) {
                return Err(format!(
                    "invalid datetime fractional part for column '{column_name}': '{f}'"
                ));
            }
            Ok((d.to_string(), f.to_string()))
        }
        None => Ok((raw.to_string(), String::new())),
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

    fn schema_with(name: &str, ty: arrow::datatypes::DataType) -> Vec<ColumnDef> {
        vec![ColumnDef {
            name: name.to_string(),
            data_type: ty,
            nullable: true,
            write_default: None,
        }]
    }

    #[test]
    fn datetime_microsecond_literal_zero_padded_to_six_digits() {
        use arrow::datatypes::TimeUnit;
        let w = parse_where("ts = '2020-01-01 00:00:00.012'");
        let schema = schema_with(
            "ts",
            arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let t = translate_to_delete_predicate(&w, &schema, &["ts".to_string()], KeysType::Dup)
            .expect("translate");
        assert_eq!(t.binary[0].value, "2020-01-01 00:00:00.012000");
    }

    #[test]
    fn datetime_literal_overflow_rejected() {
        use arrow::datatypes::TimeUnit;
        let w = parse_where("ts = '2020-01-01 00:00:00.1234567'");
        let schema = schema_with(
            "ts",
            arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let err = translate_to_delete_predicate(&w, &schema, &["ts".to_string()], KeysType::Dup)
            .unwrap_err();
        assert!(
            err.contains("Datetime") || err.contains("microsecond"),
            "got: {err}"
        );
    }

    #[test]
    fn date_literal_iso_format() {
        let w = parse_where("d = '2020-01-01'");
        let schema = schema_with("d", arrow::datatypes::DataType::Date32);
        let t = translate_to_delete_predicate(&w, &schema, &["d".to_string()], KeysType::Dup)
            .expect("translate");
        assert_eq!(t.binary[0].value, "2020-01-01");
    }

    #[test]
    fn decimal_literal_padded_to_column_scale() {
        let w = parse_where("p = 12.3");
        let schema = schema_with("p", arrow::datatypes::DataType::Decimal128(10, 2));
        let t = translate_to_delete_predicate(&w, &schema, &["p".to_string()], KeysType::Dup)
            .expect("translate");
        assert_eq!(t.binary[0].value, "12.30");
    }

    #[test]
    fn negative_integer_literal() {
        let w = parse_where("v = -42");
        let schema = schema_with("v", arrow::datatypes::DataType::Int64);
        let t = translate_to_delete_predicate(&w, &schema, &["v".to_string()], KeysType::Dup)
            .expect("translate");
        assert_eq!(t.binary[0].value, "-42");
    }

    #[test]
    fn integer_column_rejects_fractional_literal() {
        let w = parse_where("v = 1.5");
        let schema = schema_with("v", arrow::datatypes::DataType::Int64);
        let err = translate_to_delete_predicate(&w, &schema, &["v".to_string()], KeysType::Dup)
            .unwrap_err();
        assert!(err.contains("integer"), "got: {err}");
    }
}
