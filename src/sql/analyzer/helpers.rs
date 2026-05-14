use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use sqlparser::ast as sqlast;

use crate::sql::analysis::JoinKind;

// ---------------------------------------------------------------------------
// SQL type -> Arrow type conversion
// ---------------------------------------------------------------------------

pub(super) fn sql_type_to_arrow(sql_type: &sqlast::DataType) -> Result<DataType, String> {
    match sql_type {
        sqlast::DataType::TinyInt(_) => Ok(DataType::Int8),
        sqlast::DataType::SmallInt(_) => Ok(DataType::Int16),
        sqlast::DataType::Int(_) | sqlast::DataType::Integer(_) => Ok(DataType::Int32),
        sqlast::DataType::BigInt(_) => Ok(DataType::Int64),
        sqlast::DataType::Float(_) => Ok(DataType::Float32),
        sqlast::DataType::Double(_) | sqlast::DataType::DoublePrecision => Ok(DataType::Float64),
        sqlast::DataType::Boolean => Ok(DataType::Boolean),
        sqlast::DataType::Varchar(_)
        | sqlast::DataType::CharVarying(_)
        | sqlast::DataType::Text => Ok(DataType::Utf8),
        sqlast::DataType::Char(_)
        | sqlast::DataType::Character(_)
        | sqlast::DataType::String(_) => Ok(DataType::Utf8),
        sqlast::DataType::JSON | sqlast::DataType::JSONB => Ok(DataType::Utf8),
        sqlast::DataType::Varbinary(_) | sqlast::DataType::Binary(_) => Ok(DataType::Binary),
        sqlast::DataType::Date => Ok(DataType::Date32),
        sqlast::DataType::Datetime(_) | sqlast::DataType::Timestamp(_, _) => Ok(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        sqlast::DataType::Time(_, _) => {
            Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond))
        }
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                Ok(DataType::Decimal128(*p as u8, *s as i8))
            }
            sqlast::ExactNumberInfo::Precision(p) => Ok(DataType::Decimal128(*p as u8, 0)),
            sqlast::ExactNumberInfo::None => Ok(DataType::Decimal128(38, 0)),
        },
        sqlast::DataType::Custom(name, _) => {
            let type_name = name.to_string().to_lowercase();
            match type_name.as_str() {
                "string" => Ok(DataType::Utf8),
                "largeint" => Ok(DataType::FixedSizeBinary(
                    crate::common::largeint::LARGEINT_BYTE_WIDTH,
                )),
                "json" | "jsonb" => Ok(DataType::Utf8),
                "varbinary" | "binary" => Ok(DataType::Binary),
                "array" => custom_array_type_to_arrow(sql_type),
                "map" => custom_map_type_to_arrow(sql_type),
                "struct" => custom_struct_type_to_arrow(sql_type),
                _ => Err(format!("unsupported SQL type: {name}")),
            }
        }
        sqlast::DataType::Array(elem_def) => {
            let inner = match elem_def {
                sqlast::ArrayElemTypeDef::AngleBracket(inner_type)
                | sqlast::ArrayElemTypeDef::SquareBracket(inner_type, _)
                | sqlast::ArrayElemTypeDef::Parenthesis(inner_type) => {
                    sql_type_to_arrow(inner_type)?
                }
                sqlast::ArrayElemTypeDef::None => {
                    return Err("ARRAY type requires an element type".to_string());
                }
            };
            Ok(DataType::List(Arc::new(Field::new("item", inner, true))))
        }
        sqlast::DataType::Map(key_type, value_type) => Ok(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", sql_type_to_arrow(key_type)?, true)),
                    Arc::new(Field::new("value", sql_type_to_arrow(value_type)?, true)),
                ])),
                false,
            )),
            false,
        )),
        sqlast::DataType::Struct(fields, _) => {
            let out_fields: Vec<Arc<Field>> = fields
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let name = field
                        .field_name
                        .as_ref()
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| format!("f{}", idx + 1));
                    Ok(Arc::new(Field::new(
                        name,
                        sql_type_to_arrow(&field.field_type)?,
                        true,
                    )))
                })
                .collect::<Result<_, String>>()?;
            Ok(DataType::Struct(Fields::from(out_fields)))
        }
        other => Err(format!("unsupported CAST target type: {other:?}")),
    }
}

fn custom_array_type_to_arrow(sql_type: &sqlast::DataType) -> Result<DataType, String> {
    let sqlast::DataType::Custom(_, modifiers) = sql_type else {
        return Err(format!("expected custom ARRAY type, got {sql_type:?}"));
    };
    if modifiers.len() != 1 {
        return Err(format!(
            "ARRAY type requires exactly one element type, got {}",
            modifiers.len()
        ));
    }
    let inner = parse_custom_type_string(&modifiers[0])?;
    Ok(DataType::List(Arc::new(Field::new("item", inner, true))))
}

fn custom_map_type_to_arrow(sql_type: &sqlast::DataType) -> Result<DataType, String> {
    let sqlast::DataType::Custom(_, modifiers) = sql_type else {
        return Err(format!("expected custom MAP type, got {sql_type:?}"));
    };
    if modifiers.len() != 2 {
        return Err(format!(
            "MAP type requires exactly two type parameters, got {}",
            modifiers.len()
        ));
    }
    let key_type = parse_custom_type_string(&modifiers[0])?;
    let value_type = parse_custom_type_string(&modifiers[1])?;
    Ok(DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", key_type, true)),
                Arc::new(Field::new("value", value_type, true)),
            ])),
            false,
        )),
        false,
    ))
}

fn custom_struct_type_to_arrow(sql_type: &sqlast::DataType) -> Result<DataType, String> {
    let sqlast::DataType::Custom(_, modifiers) = sql_type else {
        return Err(format!("expected custom STRUCT type, got {sql_type:?}"));
    };
    let fields = modifiers
        .iter()
        .enumerate()
        .map(|(idx, field_spec)| {
            let (name, field_type) = split_custom_struct_field(field_spec)?;
            Ok(Arc::new(Field::new(
                name.unwrap_or_else(|| format!("f{}", idx + 1)),
                parse_custom_type_string(field_type)?,
                true,
            )))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(DataType::Struct(Fields::from(fields)))
}

fn split_custom_struct_field(field_spec: &str) -> Result<(Option<String>, &str), String> {
    let trimmed = field_spec.trim();
    let Some(split_idx) = find_top_level_type_whitespace(trimmed) else {
        return Ok((None, trimmed));
    };
    let name = trimmed[..split_idx].trim();
    let field_type = trimmed[split_idx..].trim();
    if field_type.is_empty() {
        return Err(format!("STRUCT field missing type: {field_spec}"));
    }
    Ok((Some(name.to_string()), field_type))
}

fn parse_custom_type_string(type_sql: &str) -> Result<DataType, String> {
    let trimmed = type_sql.trim();
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "tinyint" => return Ok(DataType::Int8),
        "smallint" => return Ok(DataType::Int16),
        "int" | "integer" => return Ok(DataType::Int32),
        "bigint" => return Ok(DataType::Int64),
        "float" => return Ok(DataType::Float32),
        "double" | "double precision" => return Ok(DataType::Float64),
        "boolean" | "bool" => return Ok(DataType::Boolean),
        "string" | "varchar" | "char" | "character" | "text" => return Ok(DataType::Utf8),
        "date" => return Ok(DataType::Date32),
        "datetime" | "timestamp" => {
            return Ok(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            ));
        }
        "largeint" => {
            return Ok(DataType::FixedSizeBinary(
                crate::common::largeint::LARGEINT_BYTE_WIDTH,
            ));
        }
        "json" | "jsonb" => return Ok(DataType::Utf8),
        _ => {}
    }

    if let Some(inner) = strip_type_parameters(trimmed, "array")? {
        return Ok(DataType::List(Arc::new(Field::new(
            "item",
            parse_custom_type_string(inner)?,
            true,
        ))));
    }
    if let Some(inner) = strip_type_parameters(trimmed, "map")? {
        let parts = split_top_level_type_items(inner, b',');
        if parts.len() != 2 {
            return Err(format!("MAP type requires two type parameters: {trimmed}"));
        }
        return Ok(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", parse_custom_type_string(parts[0])?, true)),
                    Arc::new(Field::new(
                        "value",
                        parse_custom_type_string(parts[1])?,
                        true,
                    )),
                ])),
                false,
            )),
            false,
        ));
    }
    if let Some(inner) = strip_type_parameters(trimmed, "struct")? {
        let fields = split_top_level_type_items(inner, b',')
            .into_iter()
            .enumerate()
            .map(|(idx, field_spec)| {
                let (name, field_type) = split_custom_struct_field(field_spec)?;
                Ok(Arc::new(Field::new(
                    name.unwrap_or_else(|| format!("f{}", idx + 1)),
                    parse_custom_type_string(field_type)?,
                    true,
                )))
            })
            .collect::<Result<Vec<_>, String>>()?;
        return Ok(DataType::Struct(Fields::from(fields)));
    }
    if lower.starts_with("varchar(")
        || lower.starts_with("char(")
        || lower.starts_with("character(")
    {
        return Ok(DataType::Utf8);
    }
    if lower.starts_with("decimal(") || lower.starts_with("dec(") || lower.starts_with("numeric(") {
        let open_idx = trimmed
            .find('(')
            .ok_or_else(|| format!("invalid decimal type: {trimmed}"))?;
        let close_idx = find_matching_type_delimiter(trimmed, open_idx, b'(', b')')?;
        let params = split_top_level_type_items(&trimmed[open_idx + 1..close_idx], b',');
        let precision = params
            .first()
            .and_then(|value| value.trim().parse::<u8>().ok())
            .unwrap_or(38);
        let scale = params
            .get(1)
            .and_then(|value| value.trim().parse::<i8>().ok())
            .unwrap_or(0);
        return Ok(DataType::Decimal128(precision, scale));
    }

    Err(format!("unsupported SQL type: {trimmed}"))
}

fn strip_type_parameters<'a>(type_sql: &'a str, keyword: &str) -> Result<Option<&'a str>, String> {
    if !type_sql
        .get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
    {
        return Ok(None);
    }
    let bytes = type_sql.as_bytes();
    let mut cursor = keyword.len();
    while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
        cursor += 1;
    }
    if cursor >= bytes.len() {
        return Ok(None);
    }
    let (open, close) = match bytes[cursor] {
        b'<' => (b'<', b'>'),
        b'(' => (b'(', b')'),
        _ => return Ok(None),
    };
    let end_idx = find_matching_type_delimiter(type_sql, cursor, open, close)?;
    if !type_sql[end_idx + 1..].trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(&type_sql[cursor + 1..end_idx]))
}

fn find_matching_type_delimiter(
    sql: &str,
    open_idx: usize,
    open: u8,
    close: u8,
) -> Result<usize, String> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut idx = open_idx;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
        } else if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
        } else if backtick {
            if byte == b'`' {
                backtick = false;
            }
        } else {
            match byte {
                b'\'' => single_quote = true,
                b'"' => double_quote = true,
                b'`' => backtick = true,
                value if value == open => depth += 1,
                value if value == close => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| format!("unbalanced type delimiter in {sql}"))?;
                    if depth == 0 {
                        return Ok(idx);
                    }
                }
                _ => {}
            }
        }
        idx += 1;
    }
    Err(format!("unterminated type parameters in {sql}"))
}

fn split_top_level_type_items(sql: &str, delimiter: u8) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut items = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0usize;
    let mut square_depth = 0usize;
    let mut angle_depth = 0usize;
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx += 1;
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }

        match byte {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick = true,
            b'(' => paren_depth += 1,
            b')' => paren_depth = paren_depth.saturating_sub(1),
            b'[' => square_depth += 1,
            b']' => square_depth = square_depth.saturating_sub(1),
            b'<' => angle_depth += 1,
            b'>' => angle_depth = angle_depth.saturating_sub(1),
            value
                if paren_depth == 0
                    && square_depth == 0
                    && angle_depth == 0
                    && value == delimiter =>
            {
                items.push(sql[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }
    items.push(sql[start..].trim());
    items
}

fn find_top_level_type_whitespace(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut paren_depth = 0usize;
    let mut square_depth = 0usize;
    let mut angle_depth = 0usize;
    for (idx, byte) in bytes.iter().copied().enumerate() {
        match byte {
            b'(' => paren_depth += 1,
            b')' => paren_depth = paren_depth.saturating_sub(1),
            b'[' => square_depth += 1,
            b']' => square_depth = square_depth.saturating_sub(1),
            b'<' => angle_depth += 1,
            b'>' => angle_depth = angle_depth.saturating_sub(1),
            value
                if paren_depth == 0
                    && square_depth == 0
                    && angle_depth == 0
                    && value.is_ascii_whitespace() =>
            {
                return Some(idx);
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Expression display name
// ---------------------------------------------------------------------------

pub(super) fn expr_display_name(expr: &sqlast::Expr) -> String {
    match expr {
        // Strip outer parentheses: `(col)` → display name of `col`.
        // This matches how `SELECT distinct(col)` is parsed: DISTINCT is
        // the SELECT modifier and `(col)` is a Nested expression.
        sqlast::Expr::Nested(inner) => expr_display_name(inner),
        sqlast::Expr::Value(value) => format_literal_display_name(&value.value),
        sqlast::Expr::CompoundIdentifier(parts) if !parts.is_empty() => parts
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        sqlast::Expr::CompoundFieldAccess { root, access_chain } => {
            let mut out = expr_display_name_preserve_path(root);
            for access in access_chain {
                match access {
                    sqlast::AccessExpr::Dot(expr) => {
                        out.push('.');
                        out.push_str(&expr_display_name_preserve_path(expr));
                    }
                    sqlast::AccessExpr::Subscript(sqlast::Subscript::Index { index }) => {
                        out.push('[');
                        out.push_str(&expr_display_name(index));
                        out.push(']');
                    }
                    sqlast::AccessExpr::Subscript(sqlast::Subscript::Slice {
                        lower_bound,
                        upper_bound,
                        stride,
                    }) => {
                        out.push('[');
                        if let Some(lower) = lower_bound {
                            out.push_str(&expr_display_name(lower));
                        }
                        out.push(':');
                        if let Some(upper) = upper_bound {
                            out.push_str(&expr_display_name(upper));
                        }
                        if let Some(stride) = stride {
                            out.push(':');
                            out.push_str(&expr_display_name(stride));
                        }
                        out.push(']');
                    }
                }
            }
            out
        }
        sqlast::Expr::Identifier(ident) => ident.value.clone(),
        sqlast::Expr::Array(array) => format!(
            "[{}]",
            array
                .elem
                .iter()
                .map(expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        sqlast::Expr::Function(f) => format_function_display_name(f),
        sqlast::Expr::IsNull(inner) => {
            format!("{} IS NULL", expr_display_name_with_parens(inner))
        }
        sqlast::Expr::IsNotNull(inner) => {
            format!("{} IS NOT NULL", expr_display_name_with_parens(inner))
        }
        // CAST: uppercase keyword, StarRocks-style type names (DECIMAL64/DECIMAL128),
        // wrap inner with parentheses if it's not a simple identifier or literal.
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } if matches!(data_type, sqlast::DataType::Array(_))
            && matches!(inner.as_ref(), sqlast::Expr::Array(_)) =>
        {
            expr_display_name(inner)
        }
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_str = expr_display_name_with_parens(inner);
            let type_str = format_cast_type(data_type);
            format!("CAST({inner_str} AS {type_str})")
        }
        sqlast::Expr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Arrow,
            right,
        } => {
            let left_str = expr_display_name_with_parens(left);
            let right_str = expr_display_name_preserve_path(right);
            format!("{left_str} -> {right_str}")
        }
        // Binary ops: wrap each operand with parentheses unless it's a simple
        // identifier or literal, matching StarRocks AST2StringVisitor behavior.
        sqlast::Expr::BinaryOp { left, op, right } => {
            let left_str = expr_display_name_with_parens(left);
            let right_str = expr_display_name_with_parens(right);
            format!("{left_str} {op} {right_str}")
        }
        // Expressions like SUBSTR, EXTRACT are rendered in uppercase by
        // sqlparser's Display. Lowercase leading keyword to match StarRocks FE.
        other => {
            let s = format!("{other}");
            // Lowercase leading keyword (up to the first '(') if present.
            if let Some(paren) = s.find('(') {
                let prefix = &s[..paren];
                // Only lowercase if the prefix is all-ASCII-alpha (a keyword).
                if !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_alphabetic()) {
                    format!("{}{}", prefix.to_lowercase(), &s[paren..])
                } else {
                    s
                }
            } else {
                s
            }
        }
    }
}

fn expr_display_name_preserve_path(expr: &sqlast::Expr) -> String {
    match expr {
        sqlast::Expr::Nested(inner) => expr_display_name_preserve_path(inner),
        sqlast::Expr::CompoundIdentifier(parts) if !parts.is_empty() => parts
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        sqlast::Expr::CompoundFieldAccess { .. } => expr_display_name(expr),
        _ => expr_display_name(expr),
    }
}

fn format_literal_display_name(value: &sqlast::Value) -> String {
    match value {
        sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        sqlast::Value::Boolean(true) => "TRUE".to_string(),
        sqlast::Value::Boolean(false) => "FALSE".to_string(),
        other => other.to_string(),
    }
}

/// Wraps `expr_display_name(expr)` in parentheses unless the expression is
/// a simple identifier or literal — matching StarRocks `printWithParentheses`.
fn expr_display_name_with_parens(expr: &sqlast::Expr) -> String {
    match expr {
        sqlast::Expr::Identifier(_) | sqlast::Expr::CompoundIdentifier(_) => {
            expr_display_name(expr)
        }
        sqlast::Expr::Value(_) => expr_display_name(expr),
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } if matches!(inner.as_ref(), sqlast::Expr::Value(_)) => expr_display_name(expr),
        sqlast::Expr::Nested(inner) => expr_display_name_with_parens(inner),
        _ => format!("({})", expr_display_name(expr)),
    }
}

/// Format a CAST target type using StarRocks-style names.
/// DECIMAL(p,s) is promoted to DECIMAL32/DECIMAL64/DECIMAL128 to match
/// the analyzed type name that StarRocks FE emits in column aliases.
fn format_cast_type(data_type: &sqlast::DataType) -> String {
    match data_type {
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                let kind = decimal_kind(*p);
                format!("{kind}({p},{s})")
            }
            sqlast::ExactNumberInfo::Precision(p) => {
                let kind = decimal_kind(*p);
                format!("{kind}({p},0)")
            }
            sqlast::ExactNumberInfo::None => "DECIMAL128(38,0)".to_string(),
        },
        sqlast::DataType::Custom(name, modifiers)
            if name.to_string().eq_ignore_ascii_case("largeint") && modifiers.is_empty() =>
        {
            "LARGEINT".to_string()
        }
        // `STRING` is a StarRocks alias for `VARCHAR(65533)`; FE-side display
        // canonicalises it to `VARCHAR(65533)` in result column names.
        sqlast::DataType::Custom(name, modifiers)
            if name.to_string().eq_ignore_ascii_case("string") && modifiers.is_empty() =>
        {
            "VARCHAR(65533)".to_string()
        }
        sqlast::DataType::String(_) => "VARCHAR(65533)".to_string(),
        // `BINARY` and `BINARY(N)` are spelled `VARBINARY` in StarRocks FE
        // display because BE only has the variable-length variant.
        sqlast::DataType::Binary(_) => "VARBINARY".to_string(),
        other => format!("{other}"),
    }
}

fn decimal_kind(precision: u64) -> &'static str {
    if precision <= 9 {
        "DECIMAL32"
    } else if precision <= 18 {
        "DECIMAL64"
    } else {
        "DECIMAL128"
    }
}

fn canonical_display_function_name(name: &str) -> String {
    match name.to_lowercase().as_str() {
        "boolor_agg" => "bool_or".to_string(),
        "booland_agg" | "every" => "bool_and".to_string(),
        "string_agg" => "group_concat".to_string(),
        "array_agg_distinct" => "array_agg".to_string(),
        "approx_count_distinct_hll_sketch" => "ds_hll_count_distinct".to_string(),
        // StarRocks renders the typeless `STRUCT(...)` constructor with the
        // legacy `row(...)` spelling in result column names.
        "struct" => "row".to_string(),
        other => other.to_string(),
    }
}

fn format_function_display_name(function: &sqlast::Function) -> String {
    let original_name = function.name.to_string().to_lowercase();
    let canonical_name = canonical_display_function_name(&function.name.to_string());
    if canonical_name == "group_concat" {
        return format_group_concat_display_name(function, &canonical_name);
    }
    if original_name == "array_agg_distinct" {
        return format_array_agg_distinct_display_name(function, &canonical_name);
    }
    if original_name == "array_unique_agg" {
        return format_function_call_with_order_by(function, "array_unique_agg");
    }
    if canonical_name == "map" {
        return format_map_display_name(function);
    }
    let mut out = format!(
        "{}{}{}",
        canonical_name,
        function.parameters,
        format_function_arguments(&function.args)
    );
    if !function.within_group.is_empty() {
        out.push_str(" WITHIN GROUP (ORDER BY ");
        out.push_str(
            &function
                .within_group
                .iter()
                .map(format_order_by_expr_display_name)
                .collect::<Vec<_>>()
                .join(", "),
        );
        out.push(')');
    }
    if let Some(filter_cond) = &function.filter {
        out.push_str(" FILTER (WHERE ");
        out.push_str(&expr_display_name(filter_cond));
        out.push(')');
    }
    if let Some(null_treatment) = &function.null_treatment {
        out.push(' ');
        out.push_str(match null_treatment {
            sqlast::NullTreatment::IgnoreNulls => "ignore nulls",
            sqlast::NullTreatment::RespectNulls => "respect nulls",
        });
    }
    if let Some(over) = &function.over {
        out.push_str(" OVER ");
        out.push_str(&format_window_display_name(over));
    }
    out
}

fn format_array_agg_distinct_display_name(
    function: &sqlast::Function,
    canonical_name: &str,
) -> String {
    let args_display = match &function.args {
        sqlast::FunctionArguments::List(list) => list
            .args
            .iter()
            .map(format_function_arg_display_name)
            .collect::<Vec<_>>()
            .join(", "),
        other => format_function_arguments(other),
    };
    let mut out = format!("{canonical_name}(DISTINCT {args_display}");
    if let sqlast::FunctionArguments::List(list) = &function.args {
        for clause in &list.clauses {
            if let sqlast::FunctionArgumentClause::OrderBy(order_by_exprs) = clause {
                out.push_str(" ORDER BY ");
                out.push_str(
                    &order_by_exprs
                        .iter()
                        .map(|item| format_function_order_by_expr_display_name(item, &list.args))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
        }
    }
    out.push(')');
    out
}

fn format_function_call_with_order_by(function: &sqlast::Function, function_name: &str) -> String {
    let args_display = match &function.args {
        sqlast::FunctionArguments::List(list) => list
            .args
            .iter()
            .map(format_function_arg_display_name)
            .collect::<Vec<_>>()
            .join(", "),
        other => format_function_arguments(other),
    };
    let mut out = format!("{function_name}({args_display}");
    if let sqlast::FunctionArguments::List(list) = &function.args {
        for clause in &list.clauses {
            if let sqlast::FunctionArgumentClause::OrderBy(order_by_exprs) = clause {
                out.push_str(" ORDER BY ");
                out.push_str(
                    &order_by_exprs
                        .iter()
                        .map(|item| format_function_order_by_expr_display_name(item, &list.args))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
        }
    }
    out.push(')');
    out
}

fn format_map_display_name(function: &sqlast::Function) -> String {
    let sqlast::FunctionArguments::List(list) = &function.args else {
        return format!("map{}", format_function_arguments(&function.args));
    };
    let mut parts = Vec::new();
    let mut iter = list.args.iter();
    while let Some(key) = iter.next() {
        let value = iter.next();
        let key_display = format_function_arg_display_name(key);
        if let Some(value) = value {
            parts.push(format!(
                "{key_display}:{}",
                format_function_arg_display_name(value)
            ));
        } else {
            parts.push(key_display);
        }
    }
    format!("map{{{}}}", parts.join(","))
}

fn format_group_concat_display_name(function: &sqlast::Function, function_name: &str) -> String {
    let mut out = format!("{}{}", function_name, function.parameters);
    out.push_str(&format_group_concat_arguments(&function.args));
    if let Some(filter_cond) = &function.filter {
        out.push_str(" FILTER (WHERE ");
        out.push_str(&expr_display_name(filter_cond));
        out.push(')');
    }
    if let Some(null_treatment) = &function.null_treatment {
        out.push(' ');
        out.push_str(match null_treatment {
            sqlast::NullTreatment::IgnoreNulls => "ignore nulls",
            sqlast::NullTreatment::RespectNulls => "respect nulls",
        });
    }
    if let Some(over) = &function.over {
        out.push_str(" OVER ");
        out.push_str(&format_window_display_name(over));
    }
    out
}

fn format_window_display_name(over: &sqlast::WindowType) -> String {
    let sqlast::WindowType::WindowSpec(spec) = over else {
        return over.to_string();
    };

    let mut parts = Vec::new();
    if !spec.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            spec.partition_by
                .iter()
                .map(expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !spec.order_by.is_empty() {
        parts.push(format!(
            "ORDER BY {}",
            spec.order_by
                .iter()
                .map(format_order_by_expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let has_frame = spec.window_frame.is_some();
    if let Some(frame) = &spec.window_frame {
        parts.push(format_window_frame_display_name(frame));
    }

    if parts.is_empty() {
        "()".to_string()
    } else if has_frame {
        // StarRocks omits the trailing space before `)` when a frame is
        // present: `... ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`.
        format!("({})", parts.join(" "))
    } else {
        // ... but keeps it for plain PARTITION BY / ORDER BY:
        // `... ORDER BY v1 ASC, v2 ASC )`.
        format!("({} )", parts.join(" "))
    }
}

fn format_window_frame_display_name(frame: &sqlast::WindowFrame) -> String {
    let units = match frame.units {
        sqlast::WindowFrameUnits::Rows => "ROWS",
        sqlast::WindowFrameUnits::Range => "RANGE",
        sqlast::WindowFrameUnits::Groups => "GROUPS",
    };
    let start = format_window_bound_display_name(&frame.start_bound);
    if let Some(end) = &frame.end_bound {
        format!(
            "{} BETWEEN {} AND {}",
            units,
            start,
            format_window_bound_display_name(end)
        )
    } else {
        format!("{units} {start}")
    }
}

fn format_window_bound_display_name(bound: &sqlast::WindowFrameBound) -> String {
    match bound {
        sqlast::WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        sqlast::WindowFrameBound::Preceding(None) => "UNBOUNDED PRECEDING".to_string(),
        sqlast::WindowFrameBound::Preceding(Some(expr)) => {
            format!("{} PRECEDING", expr_display_name(expr))
        }
        sqlast::WindowFrameBound::Following(None) => "UNBOUNDED FOLLOWING".to_string(),
        sqlast::WindowFrameBound::Following(Some(expr)) => {
            format!("{} FOLLOWING", expr_display_name(expr))
        }
    }
}

fn format_function_arguments(args: &sqlast::FunctionArguments) -> String {
    match args {
        sqlast::FunctionArguments::None => String::new(),
        sqlast::FunctionArguments::Subquery(query) => format!("({query})"),
        sqlast::FunctionArguments::List(list) => {
            format!("({})", format_function_argument_list(list))
        }
    }
}

fn format_group_concat_arguments(args: &sqlast::FunctionArguments) -> String {
    match args {
        sqlast::FunctionArguments::None => String::new(),
        sqlast::FunctionArguments::Subquery(query) => format!("({query})"),
        sqlast::FunctionArguments::List(list) => {
            format!("({})", format_group_concat_argument_list(list))
        }
    }
}

fn format_function_argument_list(list: &sqlast::FunctionArgumentList) -> String {
    let mut out = String::new();
    if let Some(duplicate_treatment) = list.duplicate_treatment {
        out.push_str(&duplicate_treatment.to_string());
        out.push(' ');
    }
    out.push_str(
        &list
            .args
            .iter()
            .map(format_function_arg_display_name)
            .collect::<Vec<_>>()
            .join(", "),
    );
    let visible_clauses = list
        .clauses
        .iter()
        .map(|clause| format_function_clause_display_name(clause, &list.args))
        .filter(|clause| !clause.is_empty())
        .collect::<Vec<_>>();
    if !visible_clauses.is_empty() {
        if !list.args.is_empty() {
            out.push(' ');
        }
        out.push_str(&visible_clauses.join(" "));
    }
    out
}

fn format_group_concat_argument_list(list: &sqlast::FunctionArgumentList) -> String {
    let mut out = String::new();
    let (value_args, separator_arg) = list
        .args
        .split_last()
        .map(|(separator, values)| (values, Some(separator)))
        .unwrap_or((&[][..], None));

    if let Some(duplicate_treatment) = list.duplicate_treatment {
        out.push_str(&duplicate_treatment.to_string());
        out.push(' ');
    }
    out.push_str(
        &value_args
            .iter()
            .map(format_function_arg_display_name)
            .collect::<Vec<_>>()
            .join(","),
    );

    let visible_clauses = list
        .clauses
        .iter()
        .map(|clause| format_function_clause_display_name(clause, value_args))
        .filter(|clause| !clause.is_empty())
        .collect::<Vec<_>>();
    if !visible_clauses.is_empty() {
        if !value_args.is_empty() {
            out.push(' ');
        }
        out.push_str(&visible_clauses.join(" "));
    }

    let separator = separator_arg
        .map(format_function_arg_display_name)
        .unwrap_or_else(|| "','".to_string());
    if !out.is_empty() {
        out.push(' ');
    }
    out.push_str("SEPARATOR ");
    out.push_str(&separator);
    out
}

fn format_function_arg_display_name(arg: &sqlast::FunctionArg) -> String {
    match arg {
        sqlast::FunctionArg::Named {
            name,
            arg,
            operator,
        } => format!(
            "{name} {operator} {}",
            format_function_arg_expr_display_name(arg)
        ),
        sqlast::FunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => format!(
            "{} {operator} {}",
            expr_display_name(name),
            format_function_arg_expr_display_name(arg)
        ),
        sqlast::FunctionArg::Unnamed(arg) => format_function_arg_expr_display_name(arg),
    }
}

fn format_function_arg_expr_display_name(arg: &sqlast::FunctionArgExpr) -> String {
    match arg {
        sqlast::FunctionArgExpr::Expr(expr) => expr_display_name(expr),
        sqlast::FunctionArgExpr::QualifiedWildcard(prefix) => format!("{prefix}.*"),
        sqlast::FunctionArgExpr::Wildcard => "*".to_string(),
    }
}

fn format_function_clause_display_name(
    clause: &sqlast::FunctionArgumentClause,
    args: &[sqlast::FunctionArg],
) -> String {
    match clause {
        sqlast::FunctionArgumentClause::OrderBy(order_by) => {
            let visible = order_by
                .iter()
                .filter(|item| !is_constant_function_order_by_expr(item, args))
                .map(|item| format_function_order_by_expr_display_name(item, args))
                .collect::<Vec<_>>();
            if visible.is_empty() {
                String::new()
            } else {
                format!("ORDER BY {}", visible.join(", "))
            }
        }
        sqlast::FunctionArgumentClause::Limit(limit) => {
            format!("LIMIT {}", expr_display_name(limit))
        }
        // Match StarRocks display convention: lowercase keywords.
        sqlast::FunctionArgumentClause::IgnoreOrRespectNulls(t) => match t {
            sqlast::NullTreatment::IgnoreNulls => "ignore nulls".to_string(),
            sqlast::NullTreatment::RespectNulls => "respect nulls".to_string(),
        },
        _ => clause.to_string(),
    }
}

fn is_constant_function_order_by_expr(
    order_by: &sqlast::OrderByExpr,
    args: &[sqlast::FunctionArg],
) -> bool {
    match &order_by.expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(n, false),
            ..
        }) => n
            .parse::<usize>()
            .ok()
            .and_then(|pos| args.get(pos.saturating_sub(1)))
            .map(function_arg_is_constant)
            .unwrap_or(true),
        sqlast::Expr::Value(_) => true,
        _ => false,
    }
}

fn function_arg_is_constant(arg: &sqlast::FunctionArg) -> bool {
    match arg {
        sqlast::FunctionArg::Named { arg, .. }
        | sqlast::FunctionArg::ExprNamed { arg, .. }
        | sqlast::FunctionArg::Unnamed(arg) => match arg {
            sqlast::FunctionArgExpr::Expr(sqlast::Expr::Value(_)) => true,
            sqlast::FunctionArgExpr::Expr(_) => false,
            sqlast::FunctionArgExpr::QualifiedWildcard(_) | sqlast::FunctionArgExpr::Wildcard => {
                false
            }
        },
    }
}

fn format_order_by_expr_display_name(order_by: &sqlast::OrderByExpr) -> String {
    let mut out = expr_display_name(&order_by.expr);
    let asc = order_by.options.asc.unwrap_or(true);
    out.push(' ');
    out.push_str(if asc { "ASC" } else { "DESC" });
    if let Some(nulls_first) = order_by.options.nulls_first
        && nulls_first != asc
    {
        out.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    if let Some(with_fill) = &order_by.with_fill {
        out.push(' ');
        out.push_str(&with_fill.to_string());
    }
    out
}

fn format_function_order_by_expr_display_name(
    order_by: &sqlast::OrderByExpr,
    args: &[sqlast::FunctionArg],
) -> String {
    let expr = match &order_by.expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(n, false),
            ..
        }) => n
            .parse::<usize>()
            .ok()
            .and_then(|pos| args.get(pos.saturating_sub(1)))
            .map(format_function_arg_display_name)
            .unwrap_or_else(|| expr_display_name(&order_by.expr)),
        _ => expr_display_name(&order_by.expr),
    };

    let mut out = expr;
    let asc = order_by.options.asc.unwrap_or(true);
    out.push(' ');
    out.push_str(if asc { "ASC" } else { "DESC" });
    if let Some(nulls_first) = order_by.options.nulls_first
        && nulls_first != asc
    {
        out.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    if let Some(with_fill) = &order_by.with_fill {
        out.push(' ');
        out.push_str(&with_fill.to_string());
    }
    out
}

// ---------------------------------------------------------------------------
// JOIN operator parsing
// ---------------------------------------------------------------------------

pub(super) fn parse_join_operator(
    op: &sqlast::JoinOperator,
) -> Result<(JoinKind, Option<&sqlast::JoinConstraint>), String> {
    match op {
        sqlast::JoinOperator::Join(c) | sqlast::JoinOperator::Inner(c) => {
            Ok((JoinKind::Inner, Some(c)))
        }
        sqlast::JoinOperator::Left(c) | sqlast::JoinOperator::LeftOuter(c) => {
            Ok((JoinKind::LeftOuter, Some(c)))
        }
        sqlast::JoinOperator::Right(c) | sqlast::JoinOperator::RightOuter(c) => {
            Ok((JoinKind::RightOuter, Some(c)))
        }
        sqlast::JoinOperator::FullOuter(c) => Ok((JoinKind::FullOuter, Some(c))),
        sqlast::JoinOperator::CrossJoin(_) => Ok((JoinKind::Cross, None)),
        sqlast::JoinOperator::LeftSemi(c) => Ok((JoinKind::LeftSemi, Some(c))),
        sqlast::JoinOperator::RightSemi(c) => Ok((JoinKind::RightSemi, Some(c))),
        sqlast::JoinOperator::LeftAnti(c) => Ok((JoinKind::LeftAnti, Some(c))),
        sqlast::JoinOperator::RightAnti(c) => Ok((JoinKind::RightAnti, Some(c))),
        other => Err(format!("unsupported join type: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET extraction
// ---------------------------------------------------------------------------

pub(super) fn extract_limit(query: &sqlast::Query) -> Result<Option<i64>, String> {
    match &query.limit_clause {
        Some(sqlast::LimitClause::LimitOffset {
            limit:
                Some(sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                })),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid LIMIT value: {e}")),
        Some(sqlast::LimitClause::LimitOffset { limit: None, .. }) => Ok(None),
        Some(sqlast::LimitClause::LimitOffset { .. }) => {
            Err("only constant LIMIT is supported".into())
        }
        Some(sqlast::LimitClause::OffsetCommaLimit {
            limit:
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid LIMIT value: {e}")),
        Some(sqlast::LimitClause::OffsetCommaLimit { .. }) => {
            Err("only constant LIMIT is supported".into())
        }
        None => Ok(None),
    }
}

pub(super) fn extract_offset(query: &sqlast::Query) -> Result<Option<i64>, String> {
    match &query.limit_clause {
        Some(sqlast::LimitClause::LimitOffset {
            offset:
                Some(sqlast::Offset {
                    value:
                        sqlast::Expr::Value(sqlast::ValueWithSpan {
                            value: sqlast::Value::Number(n, _),
                            ..
                        }),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid OFFSET value: {e}")),
        Some(sqlast::LimitClause::LimitOffset { offset: None, .. }) => Ok(None),
        Some(sqlast::LimitClause::LimitOffset { .. }) => {
            Err("only constant OFFSET is supported".into())
        }
        Some(sqlast::LimitClause::OffsetCommaLimit {
            offset:
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid OFFSET value: {e}")),
        Some(sqlast::LimitClause::OffsetCommaLimit { .. }) => {
            Err("only constant OFFSET is supported".into())
        }
        None => Ok(None),
    }
}

/// Evaluate a constant integer expression (literals and simple arithmetic).
pub(super) fn eval_const_i64(expr: &sqlast::Expr) -> Result<i64, String> {
    match expr {
        sqlast::Expr::Value(v) => match &v.value {
            sqlast::Value::Number(n, _) => n
                .parse::<i64>()
                .map_err(|e| format!("cannot parse integer literal `{n}`: {e}")),
            _ => Err(format!("expected integer literal, got: {v}")),
        },
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => Ok(-eval_const_i64(inner)?),
        sqlast::Expr::BinaryOp { left, op, right } => {
            let l = eval_const_i64(left)?;
            let r = eval_const_i64(right)?;
            match op {
                sqlast::BinaryOperator::Plus => Ok(l + r),
                sqlast::BinaryOperator::Minus => Ok(l - r),
                sqlast::BinaryOperator::Multiply => Ok(l * r),
                sqlast::BinaryOperator::Divide if r != 0 => Ok(l / r),
                sqlast::BinaryOperator::Modulo if r != 0 => Ok(l % r),
                _ => Err(format!("unsupported operator in constant expression: {op}")),
            }
        }
        sqlast::Expr::Nested(inner) => eval_const_i64(inner),
        _ => Err(format!("expected constant integer expression, got: {expr}")),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast as sqlast;

    use super::expr_display_name;
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse_select_expr(sql: &str) -> sqlast::Expr {
        let statements =
            sqlparser::parser::Parser::parse_sql(&StarRocksDialect, sql).expect("parse sql");
        let sqlast::Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let sqlast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlast::SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("expected unnamed expr");
        };
        expr.clone()
    }

    #[test]
    fn expr_display_name_formats_distinct_function_args_recursively() {
        let expr = parse_select_expr("SELECT ARRAY_AGG(DISTINCT score > 0)");
        assert_eq!(expr_display_name(&expr), "array_agg(DISTINCT score > 0)");
    }

    #[test]
    fn expr_display_name_lowercases_nested_function_names() {
        let expr = parse_select_expr("SELECT array_min(ARRAY_UNIQUE_AGG(col_boolean))");
        assert_eq!(
            expr_display_name(&expr),
            "array_min(array_unique_agg(col_boolean))"
        );
    }

    #[test]
    fn expr_display_name_preserves_array_unique_agg_name() {
        let expr = parse_select_expr("SELECT ARRAY_UNIQUE_AGG(s_1)");
        assert_eq!(expr_display_name(&expr), "array_unique_agg(s_1)");
    }

    #[test]
    fn expr_display_name_formats_group_concat_like_starrocks() {
        let expr = parse_select_expr("SELECT group_concat(name, subject, ',' ORDER BY 1, 2)");
        assert_eq!(
            expr_display_name(&expr),
            "group_concat(name,subject ORDER BY name ASC, subject ASC SEPARATOR ',')"
        );
    }

    #[test]
    fn expr_display_name_normalizes_double_quoted_strings_to_single_quotes() {
        let expr = parse_select_expr("SELECT array_agg(\"中国\" ORDER BY 1, id)");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg('中国' ORDER BY id ASC)"
        );
    }

    #[test]
    fn expr_display_name_normalizes_array_literal_string_quotes() {
        let expr = parse_select_expr("SELECT array_agg(DISTINCT [json_object(\"2:3\")])");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg(DISTINCT [json_object('2:3')])"
        );
    }

    #[test]
    fn expr_display_name_formats_map_constructor_like_starrocks() {
        let expr = parse_select_expr("SELECT array_agg(map(2, 3))");
        assert_eq!(expr_display_name(&expr), "array_agg(map{2:3})");
    }

    #[test]
    fn expr_display_name_parenthesizes_is_not_null_inner_binary_expr() {
        let expr = parse_select_expr("SELECT count_if((v4 + v4) is not null)");
        assert_eq!(expr_display_name(&expr), "count_if((v4 + v4) IS NOT NULL)");
    }

    #[test]
    fn expr_display_name_formats_array_agg_distinct_like_starrocks() {
        let expr = parse_select_expr("SELECT array_agg_distinct(name ORDER BY 1 ASC)");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg(DISTINCT name ORDER BY name ASC)"
        );
    }

    #[test]
    fn expr_display_name_preserves_lambda_field_paths() {
        let expr = parse_select_expr("SELECT array_sortby((x) -> x.item, x)");
        assert_eq!(expr_display_name(&expr), "array_sortby(x -> x.item, x)");
    }

    #[test]
    fn expr_display_name_preserves_compound_field_access_paths() {
        let expr = parse_select_expr("SELECT c13.a");
        assert_eq!(expr_display_name(&expr), "c13.a");
    }

    #[test]
    fn expr_display_name_preserves_struct_field_paths_inside_function_args() {
        let expr =
            parse_select_expr("SELECT cast(percentile_approx_weighted(c13.a, c1, 0.5) as int)");
        assert_eq!(
            expr_display_name(&expr),
            "CAST((percentile_approx_weighted(c13.a, c1, 0.5)) AS INT)"
        );
    }
}
