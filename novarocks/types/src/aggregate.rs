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

//! Canonical aggregate output and intermediate Arrow type contracts.

use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::largeint;

/// Returns (output_type, intermediate_type) for aggregate functions.
/// `None` as intermediate_type means the execution layer should use its default.
pub fn infer_agg_function_types(
    name: &str,
    arg_types: &[DataType],
    _is_distinct: bool,
) -> Result<(DataType, Option<DataType>), String> {
    let first_arg = arg_types.first().cloned().unwrap_or(DataType::Null);
    match name {
        name if is_state_combinator_aggregate_function(name) => {
            Ok((DataType::Binary, Some(DataType::Binary)))
        }
        "count" => Ok((DataType::Int64, Some(DataType::Int64))),
        "sum" => {
            let out = match &first_arg {
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64 => DataType::Int64,
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                    DataType::FixedSizeBinary(*width)
                }
                DataType::Decimal128(..) => crate::canonical_agg_decimal_type("sum", &first_arg)
                    .expect("sum decimal canonical type"),
                _ => DataType::Float64,
            };
            Ok((out.clone(), Some(out)))
        }
        "avg" => {
            let out = match &first_arg {
                DataType::Decimal128(..) => crate::canonical_agg_decimal_type("avg", &first_arg)
                    .expect("avg decimal canonical type"),
                _ => DataType::Float64,
            };
            Ok((out, Some(DataType::Utf8)))
        }
        "min" | "max" => Ok((first_arg.clone(), Some(first_arg))),
        "any_value" => Ok((first_arg.clone(), Some(first_arg))),
        "group_concat" | "string_agg" => {
            let intermediate = {
                let fields = arg_types
                    .iter()
                    .enumerate()
                    .map(|(idx, data_type)| {
                        Arc::new(arrow::datatypes::Field::new(
                            format!("c{idx}"),
                            DataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                data_type.clone(),
                                true,
                            ))),
                            true,
                        ))
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(arrow::datatypes::Fields::from(fields))
            };
            Ok((DataType::Utf8, Some(intermediate)))
        }
        "count_if" => Ok((DataType::Int64, Some(DataType::Int64))),
        "bool_or" | "bool_and" | "boolor_agg" | "booland_agg" | "every" => {
            Ok((DataType::Boolean, Some(DataType::Boolean)))
        }
        "array_agg" | "array_agg_distinct" => {
            let elem = first_arg.clone();
            let list = DataType::List(Arc::new(arrow::datatypes::Field::new("item", elem, true)));
            let intermediate = if arg_types.len() <= 1 {
                list.clone()
            } else {
                let fields = arg_types
                    .iter()
                    .enumerate()
                    .map(|(idx, data_type)| {
                        Arc::new(arrow::datatypes::Field::new(
                            format!("c{idx}"),
                            DataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                data_type.clone(),
                                true,
                            ))),
                            true,
                        ))
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(arrow::datatypes::Fields::from(fields))
            };
            Ok((list, Some(intermediate)))
        }
        "array_unique_agg" => Ok((first_arg.clone(), Some(first_arg))),
        "sum_map" => {
            let map = if first_arg == DataType::Null {
                null_map_output_type()
            } else {
                first_arg.clone()
            };
            Ok((map.clone(), Some(map)))
        }
        "map_agg" => {
            let key_type = arg_types.first().cloned().unwrap_or(DataType::Null);
            let value_type = arg_types.get(1).cloned().unwrap_or(DataType::Null);
            let map = DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(arrow::datatypes::Field::new("key", key_type, true)),
                            Arc::new(arrow::datatypes::Field::new("value", value_type, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
            Ok((map.clone(), Some(map)))
        }
        "bitmap_agg" | "bitmap_union" => Ok((DataType::Binary, Some(DataType::Binary))),
        "bitmap_union_count" => Ok((DataType::Int64, Some(DataType::Binary))),
        "approx_count_distinct"
        | "ndv"
        | "approx_count_distinct_hll_sketch"
        | "ds_hll_count_distinct"
        | "ds_hll_count_distinct_merge" => Ok((DataType::Int64, Some(DataType::Binary))),
        "hll_union_agg" => Ok((DataType::Int64, Some(DataType::Binary))),
        "hll_union" | "hll_raw_agg" | "ds_hll_count_distinct_union" => {
            Ok((DataType::Binary, Some(DataType::Binary)))
        }
        "multi_distinct_count" => Ok((DataType::Int64, Some(DataType::Binary))),
        "multi_distinct_sum" => {
            let out = match &first_arg {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    DataType::Int64
                }
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                    DataType::FixedSizeBinary(*width)
                }
                DataType::Decimal128(..) => {
                    crate::canonical_agg_decimal_type("multi_distinct_sum", &first_arg)
                        .expect("multi_distinct_sum decimal canonical type")
                }
                _ => DataType::Float64,
            };
            Ok((out, Some(DataType::Binary)))
        }
        "bitmap_union_int" => Ok((DataType::Int64, Some(DataType::Binary))),
        "dict_merge" => Ok((DataType::Utf8, Some(DataType::Utf8))),
        "mann_whitney_u_test" => Ok((DataType::Utf8, Some(DataType::Binary))),
        "max_by" | "min_by" => Ok((first_arg, Some(DataType::Binary))),
        "covar_pop" | "covar_samp" | "corr" | "var_pop" | "var_samp" | "variance"
        | "variance_pop" | "variance_samp" | "stddev" | "stddev_pop" | "stddev_samp" | "std" => {
            Ok((DataType::Float64, Some(DataType::Binary)))
        }
        "percentile_cont" | "percentile_disc" | "percentile_disc_lc" => {
            Ok((first_arg, Some(DataType::Binary)))
        }
        "percentile_union" => Ok((DataType::Binary, Some(DataType::Binary))),
        "percentile_approx" => {
            let output = if matches!(arg_types.get(1), Some(DataType::List(_))) {
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Float64,
                    true,
                )))
            } else {
                DataType::Float64
            };
            Ok((output, Some(DataType::Binary)))
        }
        "percentile_approx_weighted" => {
            let output = if matches!(arg_types.get(2), Some(DataType::List(_))) {
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Float64,
                    true,
                )))
            } else {
                DataType::Float64
            };
            Ok((output, Some(DataType::Binary)))
        }
        "approx_top_k" => Ok((approx_top_k_output_type(first_arg), Some(DataType::Binary))),
        "min_n" | "max_n" => Ok((list_output_type(first_arg), Some(DataType::Binary))),
        _ => Err(format!("unknown aggregate function `{name}`")),
    }
}

fn is_state_combinator_aggregate_function(name: &str) -> bool {
    matches!(
        name,
        "count_state"
            | "count_state_signed"
            | "sum_state"
            | "sum_state_merge"
            | "sum_state_signed"
            | "avg_state"
            | "avg_state_merge"
            | "avg_state_signed"
            | "count_state_merge"
            | "min_state"
            | "min_state_merge"
            | "min_state_signed"
            | "max_state"
            | "max_state_merge"
            | "max_state_signed"
            | "bool_or_state"
            | "bool_or_state_merge"
            | "bool_or_state_signed"
            | "bool_and_state"
            | "bool_and_state_merge"
            | "bool_and_state_signed"
            | "count_distinct_state"
            | "count_distinct_state_merge"
            | "count_distinct_state_signed"
            | "approx_count_distinct_state"
            | "approx_count_distinct_state_merge"
            | "approx_count_distinct_state_signed"
    )
}

fn approx_top_k_output_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item",
        DataType::Struct(
            vec![
                Arc::new(arrow::datatypes::Field::new("item", item_type, true)),
                Arc::new(arrow::datatypes::Field::new("count", DataType::Int64, true)),
            ]
            .into(),
        ),
        true,
    )))
}

fn null_map_output_type() -> DataType {
    DataType::Map(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("key", DataType::Null, true)),
                    Arc::new(arrow::datatypes::Field::new("value", DataType::Null, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn list_output_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item", item_type, true,
    )))
}

/// Apply the canonical DISTINCT aggregate name mangling to `name`: a distinct
/// `count` / `sum` / `array_agg` maps to its multi-distinct/`_distinct` variant
/// (`count` -> `multi_distinct_count`, `sum` -> `multi_distinct_sum`, `array_agg`
/// -> `array_agg_distinct`); every other name is returned lowercased unchanged.
///
/// This is the single source of truth for the DISTINCT-mangling table, feeding
/// both the planner-typed aggregate adapters (`sql::planner::physical`) and the
/// Backend-owned native decoder. It is pure
/// (`&str` in, `String` out) so this module stays a protobuf-free, planner-free
/// leaf next to [`infer_agg_function_types`].
pub fn mangle_distinct_aggregate_name(name: &str, distinct: bool) -> String {
    let name = name.to_ascii_lowercase();
    if !distinct {
        return name;
    }
    match name.as_str() {
        "count" => "multi_distinct_count".to_string(),
        "sum" => "multi_distinct_sum".to_string(),
        "array_agg" => "array_agg_distinct".to_string(),
        _ => name,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields};

    use super::{infer_agg_function_types, mangle_distinct_aggregate_name};

    #[test]
    fn distinct_name_mangling_is_the_single_source_of_truth() {
        // Non-distinct names are lowercased unchanged.
        assert_eq!(mangle_distinct_aggregate_name("count", false), "count");
        assert_eq!(mangle_distinct_aggregate_name("COUNT", false), "count");
        // The distinct-mangling table.
        assert_eq!(
            mangle_distinct_aggregate_name("COUNT", true),
            "multi_distinct_count"
        );
        assert_eq!(
            mangle_distinct_aggregate_name("sum", true),
            "multi_distinct_sum"
        );
        assert_eq!(
            mangle_distinct_aggregate_name("array_agg", true),
            "array_agg_distinct"
        );
        // A distinct function with no mangling rule keeps its lowercased name.
        assert_eq!(mangle_distinct_aggregate_name("MAX", true), "max");
    }

    #[test]
    fn infers_core_numeric_aggregate_types() {
        assert_eq!(
            infer_agg_function_types("count", &[], false).unwrap(),
            (DataType::Int64, Some(DataType::Int64))
        );
        assert_eq!(
            infer_agg_function_types("sum", &[DataType::Int32], false).unwrap(),
            (DataType::Int64, Some(DataType::Int64))
        );
        assert_eq!(
            infer_agg_function_types("avg", &[DataType::Float64], false).unwrap(),
            (DataType::Float64, Some(DataType::Utf8))
        );
    }

    #[test]
    fn infers_decimal_and_distinct_sum_contracts() {
        let input = DataType::Decimal128(20, 2);
        let sum = crate::canonical_agg_decimal_type("sum", &input).unwrap();
        let distinct = crate::canonical_agg_decimal_type("multi_distinct_sum", &input).unwrap();
        assert_eq!(
            infer_agg_function_types("sum", std::slice::from_ref(&input), false).unwrap(),
            (sum.clone(), Some(sum))
        );
        assert_eq!(
            infer_agg_function_types("multi_distinct_sum", &[input], true).unwrap(),
            (distinct, Some(DataType::Binary))
        );
    }

    #[test]
    fn infers_state_collection_and_rejects_unknown_aggregates() {
        assert_eq!(
            infer_agg_function_types("sum_state_merge", &[DataType::Int64], false).unwrap(),
            (DataType::Binary, Some(DataType::Binary))
        );
        let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(
            infer_agg_function_types("array_agg", &[DataType::Utf8], false).unwrap(),
            (list.clone(), Some(list))
        );
        assert_eq!(
            infer_agg_function_types("unknown_zero_arg", &[], false).unwrap_err(),
            "unknown aggregate function `unknown_zero_arg`"
        );
    }

    #[test]
    fn preserves_nested_collection_type_structure() {
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                    Arc::new(Field::new("value", DataType::Int32, true)),
                ])),
                false,
            )),
            false,
        );
        assert_eq!(
            infer_agg_function_types("map_agg", &[DataType::Utf8, DataType::Int32], false,)
                .unwrap(),
            (map.clone(), Some(map))
        );

        let top_k = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("item", DataType::Utf8, true)),
                Arc::new(Field::new("count", DataType::Int64, true)),
            ])),
            true,
        )));
        assert_eq!(
            infer_agg_function_types("approx_top_k", &[DataType::Utf8], false).unwrap(),
            (top_k, Some(DataType::Binary))
        );

        let output = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let intermediate = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new(
                "c0",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            )),
            Arc::new(Field::new(
                "c1",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            )),
        ]));
        assert_eq!(
            infer_agg_function_types("array_agg", &[DataType::Utf8, DataType::Int64], false,)
                .unwrap(),
            (output, Some(intermediate))
        );
    }
}
