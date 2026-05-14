use crate::plan_nodes;
use crate::sql::analysis::{self as query_ir, BinOp, ExprKind, TypedExpr};
use crate::sql::planner::plan::{AggregateCall, WindowExpr};

/// Split a TypedExpr on AND into a flat list of conjuncts.
pub(crate) fn split_and_conjuncts_typed(expr: &TypedExpr) -> Vec<&TypedExpr> {
    let mut result = Vec::new();
    collect_and_conjuncts_typed(expr, &mut result);
    result
}

fn collect_and_conjuncts_typed<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a TypedExpr>) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_and_conjuncts_typed(left, out);
            collect_and_conjuncts_typed(right, out);
        }
        _ => {
            out.push(expr);
        }
    }
}

/// Display name for a TypedExpr (used as scope key for group_by columns).
/// Must be deterministic — same expression always produces the same name.
pub(crate) fn typed_expr_display_name(expr: &TypedExpr) -> String {
    match &expr.kind {
        // Include the qualifier so two columns with the same bare name
        // (e.g. `t1.c2` and `t3.c2` after a join) produce distinct keys.
        // This name is used to register aggregate output slots in the
        // codegen scope; a collision causes a `SELECT count(t1.c2),
        // count(t3.c2)` projection to read both values from the same slot.
        ExprKind::ColumnRef {
            qualifier: Some(q),
            column,
        } => format!("{q}.{column}"),
        ExprKind::ColumnRef {
            qualifier: None,
            column,
        } => column.clone(),
        ExprKind::LambdaParamRef { name, .. } => name.clone(),
        ExprKind::Literal(query_ir::LiteralValue::Null) => "NULL".to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Bool(true)) => "TRUE".to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Bool(false)) => "FALSE".to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Int(v)) => v.to_string(),
        ExprKind::Literal(query_ir::LiteralValue::LargeInt(v)) => v.to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Float(v)) => v.to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Decimal(v)) => v.clone(),
        ExprKind::Literal(query_ir::LiteralValue::String(v)) => format!("'{}'", v),
        ExprKind::FunctionCall { name, args, .. } if name == "__array_literal" => {
            format!(
                "[{}]",
                args.iter()
                    .map(typed_expr_array_item_display_name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        ExprKind::FunctionCall { name, args, .. } if name == "map" => {
            let mut parts = Vec::new();
            let mut iter = args.iter();
            while let Some(key) = iter.next() {
                let value = iter.next();
                let key_display = typed_expr_array_item_display_name(key);
                if let Some(value) = value {
                    parts.push(format!(
                        "{key_display}:{}",
                        typed_expr_array_item_display_name(value)
                    ));
                } else {
                    parts.push(key_display);
                }
            }
            format!("map{{{}}}", parts.join(","))
        }
        ExprKind::FunctionCall { name, args, .. } => {
            format_typed_function_call_display_name(name, args)
        }
        ExprKind::LambdaFunction { params, body } => {
            let params = params
                .iter()
                .map(|param| param.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("({params}) -> {}", typed_expr_display_name(body))
        }
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => agg_call_display_name_from_parts(name, args, *distinct, order_by),
        ExprKind::Cast {
            expr: inner,
            target,
        } if matches!(target, arrow::datatypes::DataType::List(_))
            && matches!(
                inner.kind,
                ExprKind::FunctionCall {
                    ref name,
                    ..
                } if name == "__array_literal"
            ) =>
        {
            typed_expr_display_name(inner)
        }
        ExprKind::Cast {
            expr: inner,
            target,
        } => {
            format!("cast({} as {:?})", typed_expr_display_name(inner), target)
        }
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => {
            let inner = typed_expr_display_name_with_parens(inner);
            if *negated {
                format!("{inner} IS NOT NULL")
            } else {
                format!("{inner} IS NULL")
            }
        }
        ExprKind::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                typed_expr_display_name_with_parens(left),
                bin_op_display(*op),
                typed_expr_display_name_with_parens(right)
            )
        }
        _ => format!("{:?}", expr.kind),
    }
}

fn typed_expr_display_name_with_parens(expr: &TypedExpr) -> String {
    match &expr.kind {
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            typed_expr_display_name(expr)
        }
        ExprKind::FunctionCall { .. } | ExprKind::AggregateCall { .. } => {
            typed_expr_display_name(expr)
        }
        _ => format!("({})", typed_expr_display_name(expr)),
    }
}

fn bin_op_display(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Eq => "=",
        BinOp::Ne => "!=",
        BinOp::Lt => "<",
        BinOp::Le => "<=",
        BinOp::Gt => ">",
        BinOp::Ge => ">=",
        BinOp::EqForNull => "<=>",
        BinOp::And => "AND",
        BinOp::Or => "OR",
    }
}

fn format_typed_function_call_display_name(name: &str, args: &[TypedExpr]) -> String {
    match name {
        "__struct_subfield" => {
            if let [base, field_name] = args
                && let Some(field_name) = typed_string_literal(field_name)
            {
                format!("{}.{}", typed_expr_path_display_name(base), field_name)
            } else {
                format_typed_function_call_fallback(name, args)
            }
        }
        "__array_struct_subfield" => {
            if let [base, field_name] = args
                && let Some(field_name) = typed_string_literal(field_name)
            {
                format!("{}.{}", typed_expr_path_display_name(base), field_name)
            } else {
                format_typed_function_call_fallback(name, args)
            }
        }
        "__array_element_at" | "__map_element_at" => {
            if let [base, index] = args {
                format!(
                    "{}[{}]",
                    typed_expr_path_display_name(base),
                    typed_expr_display_name(index)
                )
            } else {
                format_typed_function_call_fallback(name, args)
            }
        }
        _ => format_typed_function_call_fallback(name, args),
    }
}

fn format_typed_function_call_fallback(name: &str, args: &[TypedExpr]) -> String {
    if args.is_empty() {
        format!("{}()", name)
    } else {
        let arg_names: Vec<String> = args.iter().map(typed_expr_display_name).collect();
        format!("{}({})", name, arg_names.join(", "))
    }
}

fn typed_expr_path_display_name(expr: &TypedExpr) -> String {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier: Some(qualifier),
            column,
        } => format!("{qualifier}.{column}"),
        ExprKind::ColumnRef {
            qualifier: None,
            column,
        } => column.clone(),
        ExprKind::LambdaParamRef { name, .. } => name.clone(),
        ExprKind::FunctionCall { name, args, .. } => {
            format_typed_function_call_display_name(name, args)
        }
        _ => typed_expr_display_name(expr),
    }
}

fn typed_string_literal(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Literal(query_ir::LiteralValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn typed_expr_array_item_display_name(expr: &TypedExpr) -> String {
    match &expr.kind {
        ExprKind::Literal(query_ir::LiteralValue::Null) => "NULL".to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Bool(v)) => v.to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Int(v)) => v.to_string(),
        ExprKind::Literal(query_ir::LiteralValue::LargeInt(v)) => v.to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Float(v)) => v.to_string(),
        ExprKind::Literal(query_ir::LiteralValue::Decimal(v)) => v.clone(),
        ExprKind::Literal(query_ir::LiteralValue::String(v)) => format!("'{}'", v),
        _ => typed_expr_display_name(expr),
    }
}

fn canonical_agg_display_name(name: &str) -> &str {
    match name {
        "string_agg" => "group_concat",
        "array_agg_distinct" => "array_agg",
        "variance_samp" => "var_samp",
        "variance_pop" => "var_pop",
        other => other,
    }
}

/// Build aggregate display name from components (used by expr_compiler for scope lookup).
pub(crate) fn agg_call_display_name_from_parts(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    order_by: &[query_ir::SortItem],
) -> String {
    if matches!(name, "group_concat" | "string_agg") {
        return group_concat_display_name_from_parts(name, args, distinct, order_by);
    }
    let distinct = distinct || matches!(name, "array_agg_distinct");
    let display_name = canonical_agg_display_name(name);

    let args_display = if args.is_empty() {
        "*".to_string()
    } else {
        args.iter()
            .map(typed_expr_display_name)
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut out = if distinct {
        format!("{}(DISTINCT {}", display_name, args_display)
    } else {
        format!("{}({}", display_name, args_display)
    };

    let visible_order_by = order_by
        .iter()
        .filter(|item| !matches!(item.expr.kind, ExprKind::Literal(_)))
        .collect::<Vec<_>>();

    if !visible_order_by.is_empty() {
        let order_by_display = visible_order_by
            .iter()
            .map(|item| {
                let mut value = typed_expr_display_name(&item.expr);
                value.push_str(if item.asc { " asc" } else { " desc" });
                if item.nulls_first != item.asc {
                    value.push_str(if item.nulls_first {
                        " nulls first"
                    } else {
                        " nulls last"
                    });
                }
                value
            })
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(" order by ");
        out.push_str(&order_by_display);
    }
    out.push(')');
    out
}

fn group_concat_display_name_from_parts(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    order_by: &[query_ir::SortItem],
) -> String {
    let (value_args, separator_arg) = args
        .split_last()
        .map(|(separator, values)| (values, Some(separator)))
        .unwrap_or((&[][..], None));
    let args_display = value_args
        .iter()
        .map(typed_expr_array_item_display_name)
        .collect::<Vec<_>>()
        .join(",");

    let mut out = if distinct {
        format!(
            "{}(DISTINCT {}",
            canonical_agg_display_name(name),
            args_display
        )
    } else {
        format!("{}({}", canonical_agg_display_name(name), args_display)
    };

    let visible_order_by = order_by
        .iter()
        .filter(|item| !matches!(item.expr.kind, ExprKind::Literal(_)))
        .collect::<Vec<_>>();
    if !visible_order_by.is_empty() {
        let order_by_display = visible_order_by
            .iter()
            .map(|item| {
                let mut value = typed_expr_array_item_display_name(&item.expr);
                value.push_str(if item.asc { " ASC" } else { " DESC" });
                if item.nulls_first != item.asc {
                    value.push_str(if item.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    });
                }
                value
            })
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(" ORDER BY ");
        out.push_str(&order_by_display);
    }

    let separator_display = separator_arg
        .map(typed_expr_array_item_display_name)
        .unwrap_or_else(|| "','".to_string());
    out.push_str(" SEPARATOR ");
    out.push_str(&separator_display);
    out.push(')');
    out
}

/// Display name for an AggregateCall.
pub(crate) fn agg_call_display_name(call: &AggregateCall) -> String {
    agg_call_display_name_from_parts(&call.name, &call.args, call.distinct, &call.order_by)
}

/// Map JoinKind to TJoinOp.
pub(crate) fn join_kind_to_op(kind: query_ir::JoinKind) -> plan_nodes::TJoinOp {
    match kind {
        query_ir::JoinKind::Inner => plan_nodes::TJoinOp::INNER_JOIN,
        query_ir::JoinKind::LeftOuter => plan_nodes::TJoinOp::LEFT_OUTER_JOIN,
        query_ir::JoinKind::RightOuter => plan_nodes::TJoinOp::RIGHT_OUTER_JOIN,
        query_ir::JoinKind::FullOuter => plan_nodes::TJoinOp::FULL_OUTER_JOIN,
        query_ir::JoinKind::Cross => plan_nodes::TJoinOp::CROSS_JOIN,
        query_ir::JoinKind::LeftSemi => plan_nodes::TJoinOp::LEFT_SEMI_JOIN,
        query_ir::JoinKind::RightSemi => plan_nodes::TJoinOp::RIGHT_SEMI_JOIN,
        query_ir::JoinKind::LeftAnti => plan_nodes::TJoinOp::LEFT_ANTI_JOIN,
        query_ir::JoinKind::RightAnti => plan_nodes::TJoinOp::RIGHT_ANTI_JOIN,
    }
}

/// Group window expressions by their (partition_by, order_by, frame) signature.
pub(crate) fn group_win_exprs_by_sig(exprs: &[WindowExpr]) -> Vec<Vec<usize>> {
    let sig = |e: &WindowExpr| -> String {
        format!(
            "{:?}|{:?}|{:?}",
            e.partition_by
                .iter()
                .map(|p| format!("{:?}", p.kind))
                .collect::<Vec<_>>(),
            e.order_by
                .iter()
                .map(|o| format!("{:?}:{}", o.expr.kind, o.asc))
                .collect::<Vec<_>>(),
            e.window_frame,
        )
    };
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (i, e) in exprs.iter().enumerate() {
        let s = sig(e);
        if let Some(g) = groups.iter_mut().find(|(gs, _)| *gs == s) {
            g.1.push(i);
        } else {
            groups.push((s, vec![i]));
        }
    }
    groups.into_iter().map(|(_, indices)| indices).collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{agg_call_display_name_from_parts, typed_expr_display_name};
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn string_lit(value: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn float_lit(value: f64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Float(value)),
            data_type: DataType::Float64,
            nullable: false,
        }
    }

    #[test]
    fn typed_expr_display_name_formats_struct_subfield_like_starrocks() {
        let expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "__struct_subfield".to_string(),
                args: vec![col("c13"), string_lit("a")],
                distinct: false,
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        assert_eq!(typed_expr_display_name(&expr), "c13.a");
    }

    #[test]
    fn typed_expr_display_name_formats_collection_access_like_starrocks() {
        let expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "__array_element_at".to_string(),
                args: vec![col("c11"), int_lit(0)],
                distinct: false,
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        assert_eq!(typed_expr_display_name(&expr), "c11[0]");
    }

    #[test]
    fn typed_expr_display_name_formats_is_not_null_with_inner_parens() {
        let expr = TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col("v4")),
                        op: BinOp::Add,
                        right: Box::new(col("v4")),
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                }),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        assert_eq!(typed_expr_display_name(&expr), "(v4 + v4) IS NOT NULL");
    }

    #[test]
    fn agg_call_display_name_preserves_struct_field_paths() {
        let arg = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "__struct_subfield".to_string(),
                args: vec![col("c13"), string_lit("a")],
                distinct: false,
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        assert_eq!(
            agg_call_display_name_from_parts(
                "percentile_approx_weighted",
                &[arg, col("c1"), float_lit(0.5)],
                false,
                &[],
            ),
            "percentile_approx_weighted(c13.a, c1, 0.5)"
        );
    }

    #[test]
    fn agg_call_display_name_preserves_array_unique_agg_name() {
        assert_eq!(
            agg_call_display_name_from_parts("array_unique_agg", &[col("s_1")], false, &[]),
            "array_unique_agg(s_1)"
        );
    }
}
