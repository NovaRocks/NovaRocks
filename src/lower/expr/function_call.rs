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
use crate::common::largeint;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue, function};
use arrow::datatypes::DataType;

use crate::service::fe_report;
use crate::thrift::exprs;
use crate::thrift::types;

fn is_numeric_like_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    ) || largeint::is_largeint_data_type(data_type)
}

fn is_varchar_castable_scalar(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::Binary
    ) || largeint::is_largeint_data_type(data_type)
}

/// Accept the Arrow carriers for VARCHAR/VARBINARY scalar-function arguments.
///
/// Both the small and large Arrow string/binary layouts are valid VARCHAR/
/// VARBINARY carriers in NovaRocks. In particular, Iceberg-backed binary
/// columns are materialized as `LargeBinary` by the Parquet read path, and the
/// standalone codegen maps `LargeUtf8` back to VARCHAR (see
/// `src/sql/codegen/type_infer.rs`). Matching only `Utf8 | Binary` here would
/// reject those Iceberg-sourced arguments.
fn is_varchar_or_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

/// Like [`is_varchar_or_binary_type`] but also accepts typed-`Null` literals,
/// which propagate to per-row NULL instead of tripping the static type check.
fn is_varchar_binary_or_null_type(data_type: &DataType) -> bool {
    is_varchar_or_binary_type(data_type) || matches!(data_type, DataType::Null)
}

fn is_null_literal(arena: &ExprArena, expr_id: ExprId) -> bool {
    matches!(
        arena.node(expr_id),
        Some(ExprNode::Literal(LiteralValue::Null))
    )
}

fn is_array_element_compatible(
    expected: &DataType,
    actual: &DataType,
    actual_is_null_literal: bool,
) -> bool {
    if expected == actual {
        return true;
    }
    if matches!(actual, DataType::Null) || actual_is_null_literal {
        return true;
    }
    match (expected, actual) {
        (DataType::List(expected_item), DataType::List(actual_item)) => {
            is_array_element_compatible(
                expected_item.data_type(),
                actual_item.data_type(),
                actual_is_null_literal,
            )
        }
        (DataType::Struct(expected_fields), DataType::Struct(actual_fields)) => {
            expected_fields.len() == actual_fields.len()
                && expected_fields.iter().zip(actual_fields.iter()).all(
                    |(expected_field, actual_field)| {
                        is_array_element_compatible(
                            expected_field.data_type(),
                            actual_field.data_type(),
                            actual_is_null_literal,
                        )
                    },
                )
        }
        (DataType::Map(expected_entries, _), DataType::Map(actual_entries, _)) => {
            is_array_element_compatible(
                expected_entries.data_type(),
                actual_entries.data_type(),
                actual_is_null_literal,
            )
        }
        (DataType::List(_), _)
        | (_, DataType::List(_))
        | (DataType::Struct(_), _)
        | (_, DataType::Struct(_))
        | (DataType::Map(_, _), _)
        | (_, DataType::Map(_, _)) => false,
        // Utf8 literals are routinely used as date/timestamp values
        // (`array_remove(array_date, '2025-01-01')`) — accept them at the
        // lowering check and let the runtime cast Utf8 → Date32 / Timestamp.
        (DataType::Date32 | DataType::Timestamp(_, _), DataType::Utf8)
        | (DataType::Utf8, DataType::Date32 | DataType::Timestamp(_, _)) => true,
        _ => is_numeric_like_type(expected) && is_numeric_like_type(actual),
    }
}

fn is_array_pair_compatible(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }
    if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
        return true;
    }
    match (left, right) {
        (DataType::List(left_item), DataType::List(right_item)) => {
            is_array_pair_compatible(left_item.data_type(), right_item.data_type())
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            left_fields.len() == right_fields.len()
                && left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .all(|(left_field, right_field)| {
                        is_array_pair_compatible(left_field.data_type(), right_field.data_type())
                    })
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            is_array_pair_compatible(left_entries.data_type(), right_entries.data_type())
        }
        (DataType::List(_), _)
        | (_, DataType::List(_))
        | (DataType::Struct(_), _)
        | (_, DataType::Struct(_))
        | (DataType::Map(_, _), _)
        | (_, DataType::Map(_, _)) => false,
        // `Utf8` literals are commonly used to spell date/timestamp values
        // (`array_contains_all(array_date, ['2025-01-01'])`); accept them
        // and let the runtime cast Utf8 → Date32 / Timestamp.
        (DataType::Date32 | DataType::Timestamp(_, _), DataType::Utf8)
        | (DataType::Utf8, DataType::Date32 | DataType::Timestamp(_, _)) => true,
        _ => is_numeric_like_type(left) && is_numeric_like_type(right),
    }
}

fn is_arrays_overlap_pair_compatible(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }
    if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
        return true;
    }
    match (left, right) {
        (DataType::List(left_item), DataType::List(right_item)) => {
            is_arrays_overlap_pair_compatible(left_item.data_type(), right_item.data_type())
        }
        (DataType::List(_), _) | (_, DataType::List(_)) => false,
        _ => {
            (is_numeric_like_type(left) && is_numeric_like_type(right))
                || (matches!(left, DataType::Utf8) && is_varchar_castable_scalar(right))
                || (matches!(right, DataType::Utf8) && is_varchar_castable_scalar(left))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn array_type(inner: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", inner, true)))
    }

    fn dummy_type_desc() -> types::TTypeDesc {
        crate::lower::type_lowering::scalar_type_desc(types::TPrimitiveType::INT)
    }

    fn default_expr_node() -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: dummy_type_desc(),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn function_node(name: &str, num_children: i32) -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: dummy_type_desc(),
            num_children,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: name.to_string(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: vec![],
                ret_type: dummy_type_desc(),
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: None,
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..default_expr_node()
        }
    }

    #[test]
    fn nested_array_element_compatible_accepts_decimal_arrays() {
        let expected = array_type(DataType::Decimal128(38, 5));
        let actual = array_type(DataType::Decimal128(2, 1));
        assert!(is_array_element_compatible(&expected, &actual, false));
    }

    #[test]
    fn nested_array_pair_compatible_accepts_decimal_arrays() {
        let left = array_type(DataType::Decimal128(38, 5));
        let right = array_type(DataType::Decimal128(2, 1));
        assert!(is_array_pair_compatible(&left, &right));
    }

    #[test]
    fn varchar_or_binary_guard_accepts_small_and_large_layouts() {
        // Iceberg-backed binary columns are materialized as `LargeBinary` by the
        // Parquet read path; previously the binary/string scalar-function guards
        // matched only `Utf8 | Binary`, which made `from_binary(c1, 'hex')` (and
        // peers like `to_binary`, `sha2`, `md5`, `aes_encrypt`, `xx_hash3_128`)
        // reject an Iceberg `VARBINARY` argument. All four carriers are valid.
        assert!(is_varchar_or_binary_type(&DataType::Utf8));
        assert!(is_varchar_or_binary_type(&DataType::Binary));
        assert!(
            is_varchar_or_binary_type(&DataType::LargeBinary),
            "LargeBinary is the Iceberg VARBINARY carrier and must be accepted"
        );
        assert!(is_varchar_or_binary_type(&DataType::LargeUtf8));
    }

    #[test]
    fn varchar_or_binary_guard_still_rejects_incompatible_types() {
        // Fail-fast for genuinely-incompatible argument types is preserved.
        assert!(!is_varchar_or_binary_type(&DataType::Int32));
        assert!(!is_varchar_or_binary_type(&DataType::Boolean));
        assert!(!is_varchar_or_binary_type(&DataType::Float64));
        assert!(!is_varchar_or_binary_type(&DataType::Null));
        assert!(!is_varchar_or_binary_type(&DataType::FixedSizeBinary(16)));
    }

    #[test]
    fn varchar_binary_or_null_guard_accepts_large_layouts_and_null() {
        // The optional aes_* IV/mode/aad arguments additionally accept typed-Null.
        assert!(is_varchar_binary_or_null_type(&DataType::Utf8));
        assert!(is_varchar_binary_or_null_type(&DataType::Binary));
        assert!(is_varchar_binary_or_null_type(&DataType::LargeBinary));
        assert!(is_varchar_binary_or_null_type(&DataType::LargeUtf8));
        assert!(is_varchar_binary_or_null_type(&DataType::Null));
        // But not arbitrary scalars.
        assert!(!is_varchar_binary_or_null_type(&DataType::Int64));
    }

    #[test]
    fn from_binary_resolves_to_encryption_guard_family() {
        // Guards against silent re-wiring: `from_binary` (the function that
        // failed on Iceberg) must dispatch through the Encryption branch whose
        // first-argument check is `is_varchar_or_binary_type`.
        assert!(matches!(
            function::lookup_function("from_binary"),
            Some(function::FunctionKind::Encryption("from_binary"))
        ));
        assert!(matches!(
            function::lookup_function("to_binary"),
            Some(function::FunctionKind::Encryption("to_binary"))
        ));
        assert!(matches!(
            function::lookup_function("sha2"),
            Some(function::FunctionKind::Encryption("sha2"))
        ));
    }

    #[test]
    fn variant_get_lowering_rejects_non_variant_or_json_input() {
        let mut arena = ExprArena::default();
        let arg0 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let arg1 = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("$.a".to_string())),
            DataType::Utf8,
        );
        let arg2 = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("bigint".to_string())),
            DataType::Utf8,
        );

        let err = lower_function_call(
            &function_node("variant_get", 3),
            &[arg0, arg1, arg2],
            &mut arena,
            DataType::Int64,
            None,
            None,
        )
        .expect_err("variant_get should reject non-VARIANT/JSON input");

        assert_eq!(
            err,
            "variant_get expects VARIANT or JSON/VARCHAR as first argument"
        );
    }
}

/// Lower FUNCTION_CALL expression to ExprNode::FunctionCall or ExprNode::Literal.
pub(crate) fn lower_function_call(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<ExprId, String> {
    let fn_name = node.fn_.as_ref().map(|f| f.name.function_name.as_str());

    // Special handling for non-standard functions that return literals
    if fn_name
        .map(|name| name.eq_ignore_ascii_case("last_query_id"))
        .unwrap_or(false)
    {
        let value = match last_query_id {
            Some(id) if !id.is_empty() => LiteralValue::Utf8(id.to_string()),
            _ => LiteralValue::Null,
        };
        Ok(arena.push_typed(ExprNode::Literal(value), data_type))
    } else if fn_name
        .map(|name| name.eq_ignore_ascii_case("get_query_profile"))
        .unwrap_or(false)
    {
        let fe_addr =
            fe_addr.ok_or_else(|| "get_query_profile requires FE coord address".to_string())?;
        let arg = children
            .first()
            .and_then(|id| arena.node(*id))
            .and_then(|node| match node {
                ExprNode::Literal(LiteralValue::Utf8(s)) => Some(s.as_str()),
                ExprNode::Literal(LiteralValue::Null) => Some(""),
                _ => None,
            })
            .ok_or_else(|| "get_query_profile expects constant string argument".to_string())?;
        if arg.is_empty() {
            Ok(arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type))
        } else {
            let profile = fe_report::fetch_query_profile(fe_addr, arg)?;
            Ok(arena.push_typed(ExprNode::Literal(LiteralValue::Utf8(profile)), data_type))
        }
    } else {
        // Use function registry for standard functions
        let fn_name_lower = fn_name
            .ok_or_else(|| "FUNCTION_CALL missing function name".to_string())?
            .to_lowercase();

        let kind = function::lookup_function(&fn_name_lower).ok_or_else(|| {
            format!(
                "unsupported function call: {}",
                fn_name.unwrap_or("<unknown>")
            )
        })?;

        // Validate argument count
        let metadata = function::function_metadata(kind);
        if children.len() < metadata.min_args || children.len() > metadata.max_args {
            return Err(format!(
                "{} expects {} to {} arguments, got {}",
                metadata.name,
                metadata.min_args,
                metadata.max_args,
                children.len()
            ));
        }
        if matches!(
            kind,
            function::FunctionKind::Variant(name)
                if matches!(
                    name,
                    "variant_query"
                        | "get_variant_bool"
                        | "get_variant_int"
                        | "get_variant_double"
                        | "get_variant_string"
                        | "get_variant_date"
                        | "get_variant_datetime"
                        | "get_variant_time"
                )
        ) {
            let variant_name = match kind {
                function::FunctionKind::Variant(name) => name,
                _ => unreachable!(),
            };
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| "variant function missing arg0 type".to_string())?;
            let arg1 = arena
                .data_type(children[1])
                .ok_or_else(|| "variant function missing arg1 type".to_string())?;
            let arg0_ok = if matches!(variant_name, "get_variant_string" | "get_variant_int") {
                matches!(arg0, DataType::LargeBinary | DataType::Utf8)
            } else {
                matches!(arg0, DataType::LargeBinary)
            };
            if !arg0_ok {
                return Err("variant functions expect VARIANT/JSON as first argument".to_string());
            }
            if !matches!(arg1, DataType::Utf8) {
                return Err("variant functions expect VARCHAR as second argument".to_string());
            }
        }
        if matches!(
            kind,
            function::FunctionKind::Variant(name)
                if matches!(name, "variant_get" | "try_variant_get")
        ) {
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| "variant_get missing arg0 type".to_string())?;
            if !matches!(arg0, DataType::LargeBinary | DataType::Utf8) {
                return Err(
                    "variant_get expects VARIANT or JSON/VARCHAR as first argument".to_string(),
                );
            }
            for (i, child) in children.iter().enumerate().skip(1) {
                let t = arena
                    .data_type(*child)
                    .ok_or_else(|| format!("variant_get missing arg{i} type"))?;
                if !matches!(t, DataType::Utf8) {
                    return Err(format!(
                        "variant_get expects VARCHAR for argument {}",
                        i + 1
                    ));
                }
            }
        }
        if matches!(kind, function::FunctionKind::Variant("variant_typeof")) {
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| "variant_typeof missing arg0 type".to_string())?;
            if !matches!(arg0, DataType::LargeBinary) {
                return Err("variant_typeof expects VARIANT argument".to_string());
            }
        }
        if matches!(kind, function::FunctionKind::Split) {
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| "split missing arg0 type".to_string())?;
            let arg1 = arena
                .data_type(children[1])
                .ok_or_else(|| "split missing arg1 type".to_string())?;
            if !matches!(arg0, DataType::Utf8) {
                return Err("split expects VARCHAR as first argument".to_string());
            }
            if !matches!(arg1, DataType::Utf8) {
                return Err("split expects VARCHAR as second argument".to_string());
            }
            match &data_type {
                DataType::List(item) if item.data_type() == &DataType::Utf8 => {}
                _ => return Err("split must return ARRAY<VARCHAR> type".to_string()),
            }
        }
        if let function::FunctionKind::Array(name) = kind {
            match name {
                "all_match" | "any_match" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| format!("{} missing arg0 type", name))?;
                    match arg0 {
                        DataType::List(item) if matches!(item.data_type(), DataType::Boolean) => {}
                        DataType::List(item) => {
                            return Err(format!(
                                "{} expects ARRAY<BOOLEAN>, got ARRAY<{:?}>",
                                name,
                                item.data_type()
                            ));
                        }
                        _ => return Err(format!("{} expects ARRAY argument", name)),
                    }
                    if !matches!(data_type, DataType::Boolean) {
                        return Err(format!("{} must return BOOLEAN type", name));
                    }
                }
                "array_append" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_append missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_append missing arg1 type".to_string())?;
                    let (DataType::List(item0), DataType::List(item1)) = (arg0, &data_type) else {
                        return Err(
                            "array_append expects ARRAY arguments and ARRAY return".to_string()
                        );
                    };
                    let return_compatible = item0.data_type() == item1.data_type()
                        || matches!(item0.data_type(), DataType::Null)
                        || matches!(item1.data_type(), DataType::Null)
                        || matches!(
                            (item0.data_type(), item1.data_type()),
                            (DataType::Decimal128(_, _), DataType::Decimal128(_, _))
                        );
                    if !return_compatible {
                        return Err(format!(
                            "array_append return element type mismatch: {:?} vs {:?}",
                            item0.data_type(),
                            item1.data_type()
                        ));
                    }
                    let target_type = item1.data_type();
                    let arg_compatible = target_type == arg1
                        || matches!(arg1, DataType::Null)
                        || matches!(
                            (target_type, arg1),
                            (DataType::Decimal128(_, _), DataType::Decimal128(_, _))
                        );
                    if !arg_compatible {
                        return Err(format!(
                            "array_append expects target type {:?}, got {:?}",
                            target_type, arg1
                        ));
                    }
                }
                "array_avg" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_avg missing arg0 type".to_string())?;
                    let DataType::List(item) = arg0 else {
                        return Err("array_avg expects ARRAY argument".to_string());
                    };
                    match item.data_type() {
                        DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Decimal128(_, _)
                        | DataType::Float32
                        | DataType::Float64 => {}
                        dt if largeint::is_largeint_data_type(dt) => {}
                        other => {
                            return Err(format!("array_avg unsupported element type: {:?}", other));
                        }
                    }
                    if !matches!(data_type, DataType::Float64 | DataType::Decimal128(_, _)) {
                        return Err("array_avg must return DOUBLE/DECIMAL type".to_string());
                    }
                }
                "array_concat" | "array_intersect" => {
                    for (idx, child) in children.iter().enumerate() {
                        let arg_ty = arena
                            .data_type(*child)
                            .ok_or_else(|| format!("{} missing arg{} type", name, idx))?;
                        if !matches!(arg_ty, DataType::List(_)) {
                            return Err(format!("{} expects ARRAY arguments", name));
                        }
                    }
                    if !matches!(data_type, DataType::List(_)) {
                        return Err(format!("{} must return ARRAY type", name));
                    }
                }
                "array_cum_sum" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_cum_sum missing arg0 type".to_string())?;
                    let (DataType::List(item0), DataType::List(item1)) = (arg0, &data_type) else {
                        return Err("array_cum_sum expects ARRAY and returns ARRAY".to_string());
                    };
                    // The output element type was already promoted by
                    // `infer_array_numeric_list_return_type` in the analyzer —
                    // any integer kind goes to BIGINT, any float-like kind
                    // goes to DOUBLE. The runtime `eval_array_cum_sum`
                    // performs the corresponding cast on input. So here we
                    // only need to verify that the *output* element type is
                    // one of the two supported summing types; the input is
                    // allowed to be a narrower numeric and will be widened
                    // at runtime.
                    let input_is_numeric_int = matches!(
                        item0.data_type(),
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Boolean
                    );
                    let input_is_numeric_float =
                        matches!(item0.data_type(), DataType::Float32 | DataType::Float64);
                    match item1.data_type() {
                        DataType::Int64 if input_is_numeric_int => {}
                        DataType::Float64 if input_is_numeric_float || input_is_numeric_int => {}
                        _ => {
                            return Err(format!(
                                "array_cum_sum only supports ARRAY<BIGINT/DOUBLE>, got input={:?} output={:?}",
                                item0.data_type(),
                                item1.data_type()
                            ));
                        }
                    }
                }
                "array_distinct" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_distinct missing arg0 type".to_string())?;
                    match (arg0, &data_type) {
                        (DataType::List(item0), DataType::List(item1))
                            if item0.data_type() == item1.data_type() => {}
                        (DataType::List(item0), DataType::List(item1)) => {
                            return Err(format!(
                                "array_distinct return element type mismatch: {:?} vs {:?}",
                                item0.data_type(),
                                item1.data_type()
                            ));
                        }
                        _ => {
                            return Err(
                                "array_distinct expects ARRAY and returns ARRAY".to_string()
                            );
                        }
                    }
                }
                "array_difference" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_difference missing arg0 type".to_string())?;
                    let (DataType::List(item0), DataType::List(item1)) = (arg0, &data_type) else {
                        return Err("array_difference expects ARRAY and returns ARRAY".to_string());
                    };
                    match item0.data_type() {
                        DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64 => {
                            if !matches!(item1.data_type(), DataType::Int64) {
                                return Err(
                                    "array_difference over BOOLEAN/INT arrays must return ARRAY<BIGINT>"
                                        .to_string(),
                                );
                            }
                        }
                        DataType::Float32 | DataType::Float64 => {
                            if !matches!(item1.data_type(), DataType::Float64) {
                                return Err(
                                    "array_difference over FLOAT/DOUBLE arrays must return ARRAY<DOUBLE>"
                                        .to_string(),
                                );
                            }
                        }
                        DataType::Decimal128(_, _) => {
                            if !matches!(item1.data_type(), DataType::Decimal128(_, _)) {
                                return Err(
                                    "array_difference over DECIMAL arrays must return ARRAY<DECIMAL>"
                                        .to_string(),
                                );
                            }
                        }
                        other => {
                            return Err(format!(
                                "array_difference unsupported element type: {:?}",
                                other
                            ));
                        }
                    }
                }
                "array_filter" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_filter missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_filter missing arg1 type".to_string())?;
                    // When arg0 is a NULL literal the analyzer's return-type
                    // inference (`arg_types.first()`) hands back `Null` —
                    // which is fine, the runtime `eval_array_filter` already
                    // short-circuits to an all-NULL output. Allow it through
                    // lowering instead of insisting on a List return type.
                    match (&data_type, arg0) {
                        (DataType::List(item_out), DataType::List(item0)) => {
                            if item0.data_type() != item_out.data_type() {
                                return Err(format!(
                                    "array_filter return element type mismatch: {:?} vs {:?}",
                                    item0.data_type(),
                                    item_out.data_type()
                                ));
                            }
                        }
                        (DataType::Null, DataType::Null) | (DataType::List(_), DataType::Null) => {}
                        _ => {
                            return Err(
                                "array_filter expects ARRAY as first argument and return ARRAY"
                                    .to_string(),
                            );
                        }
                    }
                    if !matches!(arg1, DataType::List(_) | DataType::Null) {
                        return Err("array_filter expects ARRAY as second argument".to_string());
                    }
                }
                "array_flatten" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_flatten missing arg0 type".to_string())?;
                    let (DataType::List(outer_item), DataType::List(out_item)) = (arg0, &data_type)
                    else {
                        return Err(
                            "array_flatten expects ARRAY<ARRAY<...>> and returns ARRAY<...>"
                                .to_string(),
                        );
                    };
                    let DataType::List(inner_item) = outer_item.data_type() else {
                        return Err(
                            "array_flatten expects ARRAY<ARRAY<...>> as first argument".to_string()
                        );
                    };
                    if inner_item.data_type() != out_item.data_type() {
                        return Err(format!(
                            "array_flatten return element type mismatch: {:?} vs {:?}",
                            inner_item.data_type(),
                            out_item.data_type()
                        ));
                    }
                }
                "array_length" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_length missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::List(_) | DataType::Null) {
                        return Err("array_length expects ARRAY as first argument".to_string());
                    }
                    if !matches!(data_type, DataType::Int32) {
                        return Err("array_length must return INT type".to_string());
                    }
                }
                "array_contains" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_contains missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_contains missing arg1 type".to_string())?;
                    let arg1_is_null_literal = is_null_literal(arena, children[1]);
                    match arg0 {
                        DataType::List(item)
                            if is_array_element_compatible(
                                item.data_type(),
                                arg1,
                                arg1_is_null_literal,
                            ) => {}
                        DataType::List(item) => {
                            return Err(format!(
                                "array_contains expects element type {:?}, got {:?}",
                                item.data_type(),
                                arg1
                            ));
                        }
                        DataType::Null => {}
                        _ => {
                            return Err(
                                "array_contains expects ARRAY as first argument".to_string()
                            );
                        }
                    }
                    if !matches!(data_type, DataType::Boolean) {
                        return Err("array_contains must return BOOLEAN type".to_string());
                    }
                }
                "array_contains_all" | "array_contains_seq" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| format!("{} missing arg0 type", name))?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| format!("{} missing arg1 type", name))?;
                    match (arg0, arg1) {
                        (DataType::List(item0), DataType::List(item1))
                            if is_array_pair_compatible(item0.data_type(), item1.data_type()) => {}
                        (DataType::List(item0), DataType::List(item1)) => {
                            return Err(format!(
                                "{} expects same element type, got {:?} and {:?}",
                                name,
                                item0.data_type(),
                                item1.data_type()
                            ));
                        }
                        (DataType::Null, DataType::List(_))
                        | (DataType::List(_), DataType::Null)
                        | (DataType::Null, DataType::Null) => {}
                        _ => return Err(format!("{} expects ARRAY arguments", name)),
                    }
                    if !matches!(data_type, DataType::Boolean) {
                        return Err(format!("{} must return BOOLEAN type", name));
                    }
                }
                "arrays_overlap" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "arrays_overlap missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "arrays_overlap missing arg1 type".to_string())?;
                    match (arg0, arg1) {
                        (DataType::List(item0), DataType::List(item1))
                            if is_arrays_overlap_pair_compatible(
                                item0.data_type(),
                                item1.data_type(),
                            ) => {}
                        (DataType::List(item0), DataType::List(item1)) => {
                            return Err(format!(
                                "arrays_overlap expects same element type, got {:?} and {:?}",
                                item0.data_type(),
                                item1.data_type()
                            ));
                        }
                        // FE may propagate dictionary/intermediate types for expressions such as
                        // ifnull(array_col, []). Defer exact validation to runtime in that case.
                        _ => {}
                    }
                    if !matches!(data_type, DataType::Boolean) {
                        return Err("arrays_overlap must return BOOLEAN type".to_string());
                    }
                }
                "array_position" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_position missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_position missing arg1 type".to_string())?;
                    let arg1_is_null_literal = is_null_literal(arena, children[1]);
                    match arg0 {
                        DataType::List(item)
                            if is_array_element_compatible(
                                item.data_type(),
                                arg1,
                                arg1_is_null_literal,
                            ) => {}
                        DataType::List(item) => {
                            return Err(format!(
                                "array_position expects element type {:?}, got {:?}",
                                item.data_type(),
                                arg1
                            ));
                        }
                        DataType::Null => {}
                        _ => {
                            return Err(
                                "array_position expects ARRAY as first argument".to_string()
                            );
                        }
                    }
                    if !matches!(data_type, DataType::Int32) {
                        return Err("array_position must return INT type".to_string());
                    }
                }
                "array_repeat" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_repeat missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_repeat missing arg1 type".to_string())?;
                    if !matches!(
                        arg1,
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                    ) {
                        return Err("array_repeat expects BIGINT repeat count".to_string());
                    }
                    let DataType::List(item) = &data_type else {
                        return Err("array_repeat must return ARRAY type".to_string());
                    };
                    if arg0 != item.data_type() && !matches!(arg0, DataType::Null) {
                        return Err(format!(
                            "array_repeat return element type mismatch: expected {:?}, got {:?}",
                            item.data_type(),
                            arg0
                        ));
                    }
                }
                "array_remove" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_remove missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_remove missing arg1 type".to_string())?;
                    let arg1_is_null_literal = is_null_literal(arena, children[1]);
                    match (arg0, &data_type) {
                        (DataType::List(item0), DataType::List(item1))
                            if is_array_pair_compatible(item0.data_type(), item1.data_type())
                                && is_array_element_compatible(
                                    item1.data_type(),
                                    arg1,
                                    arg1_is_null_literal,
                                ) => {}
                        (DataType::List(item0), DataType::List(item1))
                            if !is_array_element_compatible(
                                item1.data_type(),
                                arg1,
                                arg1_is_null_literal,
                            ) =>
                        {
                            return Err(format!(
                                "array_remove expects target type {:?}, got {:?}",
                                item1.data_type(),
                                arg1
                            ));
                        }
                        (DataType::List(item0), DataType::List(item1)) => {
                            return Err(format!(
                                "array_remove return element type mismatch: {:?} vs {:?}",
                                item0.data_type(),
                                item1.data_type()
                            ));
                        }
                        _ => {
                            return Err(
                                "array_remove expects ARRAY arguments and ARRAY return".to_string()
                            );
                        }
                    }
                }
                "array_slice" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_slice missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_slice missing arg1 type".to_string())?;
                    if !matches!(arg0, DataType::List(_)) {
                        return Err("array_slice expects ARRAY as first argument".to_string());
                    }
                    if !matches!(arg1, DataType::Int64) {
                        return Err("array_slice expects BIGINT offset".to_string());
                    }
                    if children.len() == 3 {
                        let arg2 = arena
                            .data_type(children[2])
                            .ok_or_else(|| "array_slice missing arg2 type".to_string())?;
                        if !matches!(arg2, DataType::Int64) {
                            return Err("array_slice expects BIGINT length".to_string());
                        }
                    }
                    if !matches!(data_type, DataType::List(_)) {
                        return Err("array_slice must return ARRAY type".to_string());
                    }
                }
                "array_sort" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_sort missing arg0 type".to_string())?;
                    match (arg0, &data_type) {
                        (DataType::List(item0), DataType::List(item1))
                            if item0.data_type() == item1.data_type() => {}
                        (DataType::List(item0), DataType::List(item1)) => {
                            return Err(format!(
                                "array_sort return element type mismatch: {:?} vs {:?}",
                                item0.data_type(),
                                item1.data_type()
                            ));
                        }
                        _ => return Err("array_sort expects ARRAY and returns ARRAY".to_string()),
                    }
                }
                "array_sort_lambda" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_sort_lambda missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::List(_)) || !matches!(data_type, DataType::List(_))
                    {
                        return Err("array_sort_lambda expects ARRAY and returns ARRAY".to_string());
                    }
                }
                "array_sortby" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_sortby missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::List(_)) {
                        return Err("array_sortby expects ARRAY arguments".to_string());
                    }
                    for (idx, child) in children.iter().enumerate().skip(1) {
                        let arg_ty = arena
                            .data_type(*child)
                            .ok_or_else(|| format!("array_sortby missing arg{} type", idx))?;
                        if !matches!(arg_ty, DataType::List(_)) {
                            return Err("array_sortby expects ARRAY arguments".to_string());
                        }
                    }
                    match (arg0, &data_type) {
                        (DataType::List(item0), DataType::List(item1))
                            if item0.data_type() == item1.data_type() => {}
                        (DataType::List(item0), DataType::List(item1)) => {
                            return Err(format!(
                                "array_sortby return element type mismatch: {:?} vs {:?}",
                                item0.data_type(),
                                item1.data_type()
                            ));
                        }
                        _ => return Err("array_sortby expects ARRAY return type".to_string()),
                    }
                }
                "array_join" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_join missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "array_join missing arg1 type".to_string())?;
                    if !matches!(arg0, DataType::List(_)) {
                        return Err("array_join expects ARRAY as first argument".to_string());
                    }
                    if !matches!(arg1, DataType::Utf8) {
                        return Err("array_join expects VARCHAR separator".to_string());
                    }
                    if children.len() == 3 {
                        let arg2 = arena
                            .data_type(children[2])
                            .ok_or_else(|| "array_join missing arg2 type".to_string())?;
                        if !matches!(arg2, DataType::Utf8) {
                            return Err("array_join expects VARCHAR null replacement".to_string());
                        }
                    }
                    if !matches!(data_type, DataType::Utf8) {
                        return Err("array_join must return VARCHAR type".to_string());
                    }
                }
                "array_generate" => {
                    let check_int = |ty: &DataType| {
                        matches!(
                            ty,
                            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                        )
                    };
                    let check_datetime_like = |ty: &DataType| {
                        matches!(
                            ty,
                            DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8
                        )
                    };
                    let is_date_signature = if children.len() >= 2 {
                        let arg0 = arena
                            .data_type(children[0])
                            .ok_or_else(|| "array_generate missing arg0 type".to_string())?;
                        let arg1 = arena
                            .data_type(children[1])
                            .ok_or_else(|| "array_generate missing arg1 type".to_string())?;
                        check_datetime_like(arg0) || check_datetime_like(arg1)
                    } else {
                        false
                    };

                    let DataType::List(item) = &data_type else {
                        return Err("array_generate must return ARRAY type".to_string());
                    };

                    if is_date_signature {
                        if children.len() < 2 {
                            return Err("array_generate has wrong input numbers".to_string());
                        }
                        if children.len() >= 3 {
                            let step_literal = match arena.node(children[2]) {
                                Some(ExprNode::Literal(LiteralValue::Int8(v))) => Some(*v as i64),
                                Some(ExprNode::Literal(LiteralValue::Int16(v))) => Some(*v as i64),
                                Some(ExprNode::Literal(LiteralValue::Int32(v))) => Some(*v as i64),
                                Some(ExprNode::Literal(LiteralValue::Int64(v))) => Some(*v),
                                Some(ExprNode::Literal(LiteralValue::LargeInt(v))) => {
                                    i64::try_from(*v).ok()
                                }
                                _ => None,
                            };
                            let Some(step_value) = step_literal else {
                                return Err(
                                    "array_generate requires step parameter must be a constant integer."
                                        .to_string(),
                                );
                            };
                            if step_value < 0 {
                                return Err(
                                    "array_generate requires step parameter must be non-negative."
                                        .to_string(),
                                );
                            }
                        }
                        if children.len() == 4 {
                            let arg3 = arena
                                .data_type(children[3])
                                .ok_or_else(|| "array_generate missing arg3 type".to_string())?;
                            if !matches!(arg3, DataType::Utf8) {
                                return Err(
                                    "array_generate step param has wrong input type".to_string()
                                );
                            }
                        }
                        if !matches!(
                            item.data_type(),
                            DataType::Date32 | DataType::Timestamp(_, _)
                        ) {
                            return Err(format!(
                                "array_generate return element type must be date/datetime, got {:?}",
                                item.data_type()
                            ));
                        }
                    } else {
                        if children.len() == 4 {
                            return Err(
                                "array_generate step param has wrong input type".to_string()
                            );
                        }
                        for (idx, child) in children.iter().enumerate() {
                            let arg_ty = arena
                                .data_type(*child)
                                .ok_or_else(|| format!("array_generate missing arg{} type", idx))?;
                            // NULL literal arguments are permitted — the
                            // function-call evaluator returns NULL for the
                            // whole row when any arg is NULL.
                            if matches!(arg_ty, DataType::Null) {
                                continue;
                            }
                            if !check_int(arg_ty) {
                                return Err(format!(
                                    "array_generate expects integer arguments, got {:?}",
                                    arg_ty
                                ));
                            }
                        }
                        if !check_int(item.data_type()) {
                            return Err(format!(
                                "array_generate return element type must be integer, got {:?}",
                                item.data_type()
                            ));
                        }
                    }
                }
                "array_min" | "array_max" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| format!("{} missing arg0 type", name))?;
                    let DataType::List(item) = arg0 else {
                        return Err(format!("{} expects ARRAY argument", name));
                    };
                    if item.data_type() != &data_type {
                        return Err(format!(
                            "{} return type must match array element type {:?}",
                            name,
                            item.data_type()
                        ));
                    }
                }
                "array_sum" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "array_sum missing arg0 type".to_string())?;
                    let DataType::List(item) = arg0 else {
                        return Err("array_sum expects ARRAY argument".to_string());
                    };
                    match item.data_type() {
                        DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64 => {
                            if !matches!(data_type, DataType::Int64) {
                                return Err("array_sum over BOOLEAN/INT arrays must return BIGINT"
                                    .to_string());
                            }
                        }
                        DataType::Float32 | DataType::Float64 => {
                            if !matches!(data_type, DataType::Float64) {
                                return Err(
                                    "array_sum over FLOAT/DOUBLE arrays must return DOUBLE"
                                        .to_string(),
                                );
                            }
                        }
                        DataType::Utf8 | DataType::LargeUtf8 => {
                            if !matches!(data_type, DataType::Float64) {
                                return Err(
                                    "array_sum over VARCHAR arrays must return DOUBLE".to_string()
                                );
                            }
                        }
                        dt if largeint::is_largeint_data_type(dt) => {
                            if !largeint::is_largeint_data_type(&data_type)
                                && !matches!(data_type, DataType::Decimal128(_, _))
                            {
                                return Err(
                                    "array_sum over LARGEINT arrays must return LARGEINT/DECIMAL"
                                        .to_string(),
                                );
                            }
                        }
                        DataType::Decimal128(_, _) => {
                            if !matches!(data_type, DataType::Decimal128(_, _)) {
                                return Err(
                                    "array_sum over DECIMAL arrays must return DECIMAL".to_string()
                                );
                            }
                        }
                        other => {
                            return Err(format!("array_sum unsupported element type: {:?}", other));
                        }
                    }
                }
                _ => {}
            }
        }
        if let function::FunctionKind::Map(name) = kind {
            match name {
                "map" | "map_from_arrays" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| format!("{} missing arg0 type", name))?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| format!("{} missing arg1 type", name))?;
                    if !matches!(arg0, DataType::List(_)) || !matches!(arg1, DataType::List(_)) {
                        return Err(format!("{} expects ARRAY arguments", name));
                    }
                    if !matches!(data_type, DataType::Map(_, _)) {
                        return Err(format!("{} must return MAP type", name));
                    }
                }
                "arrays_zip" => {
                    // FE may propagate dictionary-encoded intermediate types for array<string>
                    // slots. Keep argument validation permissive here and rely on runtime
                    // evaluation to enforce exact array-typed inputs.
                    match &data_type {
                        DataType::List(field) => match field.data_type() {
                            DataType::Struct(fields) => {
                                if fields.len() != children.len() {
                                    return Err(format!(
                                        "arrays_zip return field count mismatch: expected {}, got {}",
                                        children.len(),
                                        fields.len()
                                    ));
                                }
                            }
                            other => {
                                return Err(format!(
                                    "arrays_zip return element type must be STRUCT, got {:?}",
                                    other
                                ));
                            }
                        },
                        _ => return Err("arrays_zip must return ARRAY<STRUCT> type".to_string()),
                    }
                }
                "map_size" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "map_size missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::Map(_, _)) {
                        return Err("map_size expects MAP argument".to_string());
                    }
                    if !matches!(data_type, DataType::Int32) {
                        return Err("map_size must return INT type".to_string());
                    }
                }
                "map_keys" | "map_values" | "map_entries" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| format!("{} missing arg0 type", name))?;
                    if !matches!(arg0, DataType::Map(_, _)) {
                        return Err(format!("{} expects MAP argument", name));
                    }
                    if !matches!(data_type, DataType::List(_)) {
                        return Err(format!("{} must return ARRAY type", name));
                    }
                }
                "map_filter" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "map_filter missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "map_filter missing arg1 type".to_string())?;
                    if !matches!(arg0, DataType::Map(_, _)) {
                        return Err("map_filter expects MAP as first argument".to_string());
                    }
                    match arg1 {
                        DataType::List(item) if item.data_type() == &DataType::Boolean => {}
                        DataType::List(item) => {
                            return Err(format!(
                                "map_filter expects ARRAY<BOOLEAN> as second argument, got ARRAY<{:?}>",
                                item.data_type()
                            ));
                        }
                        _ => {
                            return Err(
                                "map_filter expects ARRAY<BOOLEAN> as second argument".to_string()
                            );
                        }
                    }
                    if !matches!(data_type, DataType::Map(_, _)) {
                        return Err("map_filter must return MAP type".to_string());
                    }
                }
                "distinct_map_keys" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "distinct_map_keys missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::Map(_, _)) {
                        return Err("distinct_map_keys expects MAP argument".to_string());
                    }
                    if !matches!(data_type, DataType::Map(_, _)) {
                        return Err("distinct_map_keys must return MAP type".to_string());
                    }
                }
                "map_concat" => {
                    for (idx, child) in children.iter().enumerate() {
                        let arg_ty = arena
                            .data_type(*child)
                            .ok_or_else(|| format!("map_concat missing arg{} type", idx))?;
                        if !matches!(arg_ty, DataType::Map(_, _)) {
                            return Err("map_concat expects MAP arguments".to_string());
                        }
                    }
                    if !matches!(data_type, DataType::Map(_, _)) {
                        return Err("map_concat must return MAP type".to_string());
                    }
                }
                "map_apply" | "transform_keys" | "transform_values" => {
                    let (lambda_expr, map_args) = if matches!(
                        arena.node(children[0]),
                        Some(ExprNode::LambdaFunction { .. })
                    ) {
                        (children[0], &children[1..])
                    } else if matches!(
                        children.last().and_then(|id| arena.node(*id)),
                        Some(ExprNode::LambdaFunction { .. })
                    ) {
                        (
                            *children
                                .last()
                                .ok_or_else(|| format!("{} missing lambda argument", name))?,
                            &children[..children.len() - 1],
                        )
                    } else {
                        return Err(format!(
                            "{} expects a lambda function as first or last argument",
                            name
                        ));
                    };
                    if map_args.is_empty() {
                        return Err(format!("{} expects at least one MAP argument", name));
                    }
                    for (idx, map_arg) in map_args.iter().enumerate() {
                        let arg_ty = arena
                            .data_type(*map_arg)
                            .ok_or_else(|| format!("{} missing map arg{} type", name, idx))?;
                        if !matches!(arg_ty, DataType::Map(_, _)) {
                            return Err(format!("{} expects MAP arguments", name));
                        }
                    }
                    if !matches!(data_type, DataType::Map(_, _)) {
                        return Err(format!("{} must return MAP type", name));
                    }
                    match arena.node(lambda_expr) {
                        Some(ExprNode::LambdaFunction {
                            body, arg_slots, ..
                        }) => {
                            let expected = map_args.len().checked_mul(2).ok_or_else(|| {
                                format!("{} lambda argument count overflow", name)
                            })?;
                            if arg_slots.len() != expected {
                                return Err(format!(
                                    "{} expects {} lambda arguments, got {}",
                                    name,
                                    expected,
                                    arg_slots.len()
                                ));
                            }
                            let body_type = arena
                                .data_type(*body)
                                .ok_or_else(|| format!("{} missing lambda body type", name))?;
                            if !matches!(body_type, DataType::Map(_, _)) {
                                return Err(format!("{} lambda body must return MAP type", name));
                            }
                        }
                        _ => return Err(format!("{} lambda argument is invalid", name)),
                    }
                }
                "element_at" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "element_at missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "element_at missing arg1 type".to_string())?;
                    match arg0 {
                        DataType::Map(field, _) => {
                            let DataType::Struct(fields) = field.data_type() else {
                                return Err(
                                    "element_at map entries type must be Struct".to_string()
                                );
                            };
                            if fields.len() != 2 {
                                return Err(
                                    "element_at map entries type must have 2 fields".to_string()
                                );
                            }
                            let key_type = fields[0].data_type();
                            if key_type != arg1 {
                                return Err(format!(
                                    "element_at key type mismatch: expected {:?}, got {:?}",
                                    key_type, arg1
                                ));
                            }
                            let value_type = fields[1].data_type();
                            if value_type != &data_type {
                                return Err(format!(
                                    "element_at return type must match map value type: expected {:?}, got {:?}",
                                    value_type, data_type
                                ));
                            }
                        }
                        _ => return Err("element_at expects MAP as first argument".to_string()),
                    }
                }
                "cardinality" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "cardinality missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::Map(_, _) | DataType::List(_)) {
                        return Err("cardinality expects ARRAY or MAP argument".to_string());
                    }
                    if !matches!(data_type, DataType::Int32) {
                        return Err("cardinality must return INT type".to_string());
                    }
                }
                _ => {}
            }
        }
        if let function::FunctionKind::StructFn(name) = kind {
            if name == "subfield" {
                let arg0 = arena
                    .data_type(children[0])
                    .ok_or_else(|| "subfield missing arg0 type".to_string())?;
                let arg1 = arena
                    .data_type(children[1])
                    .ok_or_else(|| "subfield missing arg1 type".to_string())?;
                if !matches!(arg0, DataType::Struct(_)) {
                    return Err("subfield expects STRUCT as first argument".to_string());
                }
                if !matches!(arg1, DataType::Utf8) {
                    return Err(
                        "subfield expects VARCHAR field name as second argument".to_string()
                    );
                }
            } else if !matches!(data_type, DataType::Struct(_)) {
                return Err(format!("{} must return STRUCT type", name));
            }
            if name == "named_struct" && !children.len().is_multiple_of(2) {
                return Err("named_struct expects an even number of arguments".to_string());
            }
        }

        if let function::FunctionKind::Bit(name) = kind {
            let is_varchar_or_binary = is_varchar_or_binary_type;
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| format!("{} missing arg0 type", name))?;
            let is_integer = |dt: &DataType| {
                matches!(
                    dt,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                ) || largeint::is_largeint_data_type(dt)
            };
            if name == "xx_hash3_128" {
                if !matches!(data_type, DataType::FixedSizeBinary(16)) {
                    return Err("xx_hash3_128 must return LARGEINT type".to_string());
                }
                for (idx, child) in children.iter().enumerate() {
                    let arg_ty = arena
                        .data_type(*child)
                        .ok_or_else(|| format!("xx_hash3_128 missing arg{} type", idx))?;
                    if !is_varchar_or_binary(arg_ty) {
                        return Err(format!("xx_hash3_128 arg{} must be VARCHAR/VARBINARY", idx));
                    }
                }
            } else {
                if !is_integer(arg0) {
                    return Err(format!("{} expects integer arguments", name));
                }
                if !is_integer(&data_type) {
                    return Err(format!("{} must return integer type", name));
                }

                match name {
                    "bitnot" => {}
                    "bitand" | "bitor" | "bitxor" => {
                        let arg1 = arena
                            .data_type(children[1])
                            .ok_or_else(|| format!("{} missing arg1 type", name))?;
                        if !is_integer(arg1) {
                            return Err(format!("{} expects integer arguments", name));
                        }
                    }
                    "bit_shift_left" | "bit_shift_right" | "bit_shift_right_logical" => {
                        let arg1 = arena
                            .data_type(children[1])
                            .ok_or_else(|| format!("{} missing arg1 type", name))?;
                        if !is_integer(arg1) {
                            return Err(format!("{} expects BIGINT as second argument", name));
                        }
                    }
                    _ => {}
                }
            }
        }

        if let function::FunctionKind::Matching(name) = kind {
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| format!("{} missing arg0 type", name))?;
            let arg1 = arena
                .data_type(children[1])
                .ok_or_else(|| format!("{} missing arg1 type", name))?;
            if !matches!(arg0, DataType::Utf8) || !matches!(arg1, DataType::Utf8) {
                return Err(format!("{} expects VARCHAR arguments", name));
            }
            if !matches!(data_type, DataType::Boolean) {
                return Err(format!("{} must return BOOLEAN type", name));
            }
        }

        if let function::FunctionKind::Encryption(name) = kind {
            let is_varchar_or_binary = is_varchar_or_binary_type;
            // NULL literal args (typed-Null) are valid; they propagate to
            // per-row NULL rather than tripping the static type check.
            let is_varchar_binary_or_null = is_varchar_binary_or_null_type;
            let is_integer = |dt: &DataType| {
                matches!(
                    dt,
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ) || largeint::is_largeint_data_type(dt)
            };

            match name {
                "aes_encrypt" | "aes_decrypt" => {
                    if !matches!(children.len(), 2 | 4 | 5) {
                        return Err(format!("{} expects 2, 4, or 5 arguments", name));
                    }
                    for (idx, child) in children.iter().enumerate().take(2) {
                        let arg_ty = arena
                            .data_type(*child)
                            .ok_or_else(|| format!("{} missing arg{} type", name, idx))?;
                        if !is_varchar_or_binary(arg_ty) {
                            return Err(format!("{} arg{} must be VARCHAR/VARBINARY", name, idx));
                        }
                    }
                    if children.len() >= 4 {
                        let iv_ty = arena
                            .data_type(children[2])
                            .ok_or_else(|| format!("{} missing arg2 type", name))?;
                        let mode_ty = arena
                            .data_type(children[3])
                            .ok_or_else(|| format!("{} missing arg3 type", name))?;
                        if !is_varchar_binary_or_null(iv_ty) {
                            return Err(format!("{} arg2 must be VARCHAR/VARBINARY", name));
                        }
                        if !is_varchar_binary_or_null(mode_ty) {
                            return Err(format!("{} arg3 must be VARCHAR/VARBINARY", name));
                        }
                    }
                    if children.len() == 5 {
                        let aad_ty = arena
                            .data_type(children[4])
                            .ok_or_else(|| format!("{} missing arg4 type", name))?;
                        if !is_varchar_binary_or_null(aad_ty) {
                            return Err(format!("{} arg4 must be VARCHAR/VARBINARY", name));
                        }
                    }
                    if !matches!(data_type, DataType::Utf8 | DataType::Binary) {
                        return Err(format!("{} must return VARCHAR/VARBINARY type", name));
                    }
                }
                "from_base64" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "from_base64 missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::Utf8) {
                        return Err("from_base64 expects VARCHAR argument".to_string());
                    }
                    if !matches!(data_type, DataType::Utf8 | DataType::Binary) {
                        return Err("from_base64 must return VARCHAR/VARBINARY type".to_string());
                    }
                }
                "to_base64" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "to_base64 missing arg0 type".to_string())?;
                    if !is_varchar_or_binary(arg0) {
                        return Err("to_base64 expects VARCHAR/VARBINARY argument".to_string());
                    }
                    if !matches!(data_type, DataType::Utf8) {
                        return Err("to_base64 must return VARCHAR type".to_string());
                    }
                }
                "md5" | "sm3" => {
                    for (idx, child) in children.iter().enumerate() {
                        let arg_ty = arena
                            .data_type(*child)
                            .ok_or_else(|| format!("{} missing arg{} type", name, idx))?;
                        if !is_varchar_or_binary(arg_ty) {
                            return Err(format!("{} expects VARCHAR/VARBINARY arguments", name));
                        }
                    }
                    if !matches!(data_type, DataType::Utf8) {
                        return Err(format!("{} must return VARCHAR type", name));
                    }
                }
                "md5sum" => {
                    for (idx, child) in children.iter().enumerate() {
                        let arg_ty = arena
                            .data_type(*child)
                            .ok_or_else(|| format!("md5sum missing arg{} type", idx))?;
                        if !is_varchar_castable_scalar(arg_ty) {
                            return Err(
                                "md5sum expects VARCHAR/VARBINARY or castable scalar arguments"
                                    .to_string(),
                            );
                        }
                    }
                    if !matches!(data_type, DataType::Utf8) {
                        return Err("md5sum must return VARCHAR type".to_string());
                    }
                }
                "md5sum_numeric" => {
                    for (idx, child) in children.iter().enumerate() {
                        let arg_ty = arena
                            .data_type(*child)
                            .ok_or_else(|| format!("md5sum_numeric missing arg{} type", idx))?;
                        if !is_varchar_castable_scalar(arg_ty) {
                            return Err(
                                "md5sum_numeric expects VARCHAR/VARBINARY or castable scalar arguments"
                                    .to_string(),
                            );
                        }
                    }
                    if !is_integer(&data_type) {
                        return Err("md5sum_numeric must return integer type".to_string());
                    }
                }
                "sha2" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "sha2 missing arg0 type".to_string())?;
                    let arg1 = arena
                        .data_type(children[1])
                        .ok_or_else(|| "sha2 missing arg1 type".to_string())?;
                    if !is_varchar_or_binary(arg0) {
                        return Err("sha2 expects VARCHAR/VARBINARY as first argument".to_string());
                    }
                    if !is_integer(arg1) {
                        return Err("sha2 expects INT/BIGINT as second argument".to_string());
                    }
                    if !matches!(data_type, DataType::Utf8) {
                        return Err("sha2 must return VARCHAR type".to_string());
                    }
                }
                "to_binary" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "to_binary missing arg0 type".to_string())?;
                    if !matches!(arg0, DataType::Utf8) {
                        return Err("to_binary expects VARCHAR as first argument".to_string());
                    }
                    if children.len() == 2 {
                        let arg1 = arena
                            .data_type(children[1])
                            .ok_or_else(|| "to_binary missing arg1 type".to_string())?;
                        if !matches!(arg1, DataType::Utf8) {
                            return Err("to_binary expects VARCHAR format argument".to_string());
                        }
                    }
                    if !matches!(data_type, DataType::Binary | DataType::Utf8) {
                        return Err("to_binary must return VARBINARY/VARCHAR type".to_string());
                    }
                }
                "from_binary" => {
                    let arg0 = arena
                        .data_type(children[0])
                        .ok_or_else(|| "from_binary missing arg0 type".to_string())?;
                    if !is_varchar_or_binary(arg0) {
                        return Err(
                            "from_binary expects VARBINARY/VARCHAR as first argument".to_string()
                        );
                    }
                    if children.len() == 2 {
                        let arg1 = arena
                            .data_type(children[1])
                            .ok_or_else(|| "from_binary missing arg1 type".to_string())?;
                        if !is_varchar_or_binary(arg1) {
                            return Err("from_binary expects VARCHAR format argument".to_string());
                        }
                    }
                    if !matches!(data_type, DataType::Utf8) {
                        return Err("from_binary must return VARCHAR type".to_string());
                    }
                }
                _ => {}
            }
        }

        if matches!(
            kind,
            function::FunctionKind::IcebergTransformBucket
                | function::FunctionKind::IcebergTransformTruncate
        ) {
            let arg1 = arena
                .data_type(children[1])
                .ok_or_else(|| "iceberg transform missing width arg type".to_string())?;
            if !matches!(arg1, DataType::Int32 | DataType::Int64) {
                return Err(
                    "iceberg transform width/bucket count must be INT or BIGINT".to_string()
                );
            }
        }

        if matches!(
            kind,
            function::FunctionKind::IcebergTransformYear
                | function::FunctionKind::IcebergTransformMonth
                | function::FunctionKind::IcebergTransformDay
                | function::FunctionKind::IcebergTransformHour
        ) && !matches!(data_type, DataType::Int64)
        {
            return Err("iceberg transform year/month/day/hour must return BIGINT".to_string());
        }

        if matches!(kind, function::FunctionKind::IcebergTransformBucket)
            && !matches!(data_type, DataType::Int32)
        {
            return Err("iceberg transform bucket must return INT".to_string());
        }

        if matches!(kind, function::FunctionKind::IcebergTransformTruncate) {
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| "iceberg truncate missing arg0 type".to_string())?;
            if arg0 != &data_type {
                return Err(
                    "iceberg transform truncate must return the same type as its first argument"
                        .to_string(),
                );
            }
        }

        if let function::FunctionKind::Variant(name) = kind {
            match name {
                "variant_query" => {
                    if !matches!(data_type, DataType::LargeBinary) {
                        return Err("variant_query must return VARIANT type".to_string());
                    }
                }
                "get_variant_bool" => {
                    if !matches!(data_type, DataType::Boolean) {
                        return Err("get_variant_bool must return BOOLEAN type".to_string());
                    }
                }
                "get_variant_int" => {
                    if !matches!(data_type, DataType::Int64) {
                        return Err("get_variant_int must return BIGINT type".to_string());
                    }
                }
                "get_variant_double" => {
                    if !matches!(data_type, DataType::Float64) {
                        return Err("get_variant_double must return DOUBLE type".to_string());
                    }
                }
                "get_variant_string" | "variant_typeof" => {
                    if !matches!(data_type, DataType::Utf8) {
                        return Err(format!("{} must return VARCHAR type", name));
                    }
                }
                "get_variant_date" => {
                    if !matches!(data_type, DataType::Date32) {
                        return Err("get_variant_date must return DATE type".to_string());
                    }
                }
                "get_variant_datetime" => {
                    if !matches!(
                        data_type,
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                    ) {
                        return Err("get_variant_datetime must return DATETIME type".to_string());
                    }
                }
                "get_variant_time" => {
                    if !matches!(
                        data_type,
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                    ) {
                        return Err("get_variant_time must return TIME type".to_string());
                    }
                }
                _ => {}
            }
        }

        // Create FunctionCall node
        Ok(arena.push_typed(
            ExprNode::FunctionCall {
                kind,
                args: children.to_vec(),
            },
            data_type,
        ))
    }
}
