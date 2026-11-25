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

mod largeint_text;

mod date_float;

mod float_date;

mod decimal_float;

mod decimal_float32;

mod float_decimal128;

mod decimal_largeint;
mod float_bool_largeint;

use std::{sync::Arc, time::Duration};

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionIntrinsicRowError, FunctionKind, KernelEvaluationControl, KernelFailure,
    PreparedPureKernel, PureCallPreparation, RowDataError, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection,
    builtin::{
        catalogue::builtin_engine_function_catalog, value_conversion::VALUE_CONVERSION_NAME,
    },
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameters, ValueLogicalType,
};

struct LargeIntConversionControl;
impl PureCompileControl for LargeIntConversionControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for LargeIntConversionControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric value conversion never waits")
    }
}

fn evaluate_to_largeint(
    values: &ArrayRef,
    selection: Selection<'_>,
    policy: DecimalOverflowPolicy,
) -> (
    Vec<Option<i128>>,
    Box<[RowDataError]>,
    FunctionIntrinsicRowError,
) {
    let catalog = builtin_engine_function_catalog();
    let arguments = [FunctionArgument::Value {
        value_type: FunctionValueType::from_legacy_declared_projection(
            values.data_type().clone(),
            true,
        ),
        constant: None,
    }];
    let target = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: Some(&target),
    };
    let control = LargeIntConversionControl;
    let bound = catalog
        .resolve_bound_trusted(
            VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request,
            &control,
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        region: EvaluationRegionId::new(2),
        demand: EvaluationDemand::Value,
    };
    let input = CallEffectInput {
        context: context.into(),
        argument_uses: CallArgumentUses::SelectedChannels(&[Some(ExpressionUseId::new(3))]),
        function_id: &bound.function_id,
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &SemanticParameters::default(),
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Region(context.region),
    };
    let prepared = catalog
        .prepare_fresh_selected(
            input,
            Arc::clone(&selected),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &control,
        )
        .unwrap();
    let PreparedPureKernel::Scalar(prepared) = prepared.into_prepared() else {
        panic!("numeric value conversion must prepare a scalar kernel");
    };
    assert_eq!(prepared.contract().result_type(), &target);
    let effects = prepared.contract().effects().own_row_error;
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let args = [EvaluatedArgument::Column(values)];
    let (_, values, errors) = instance
        .evaluate(selection, &args, &control)
        .unwrap()
        .into_parts();
    let array =
        novarocks_functions::largeint::as_fixed_size_binary_array(&values, "LARGEINT").unwrap();
    let values = array
        .iter()
        .map(|value| value.map(|bytes| i128::from_be_bytes(bytes.try_into().unwrap())))
        .collect();
    (values, errors, effects)
}

mod standard_conversion {
    use super::*;
    use arrow_array::{Array, Decimal128Array, Float64Array, Int8Array, Int32Array, StringArray};
    use novarocks_failure_contract::kernel_reason::{
        INVALID_CAST_ARGUMENT, INVALID_FUNCTION_ARGUMENT, NUMERIC_OUT_OF_RANGE,
    };
    use novarocks_functions::{DataFailureReason, PreparedCastRecipe};
    use novarocks_type_contract::{
        CharacterKind, CharacterType, TrinoCastMode, ValueKind, ValueLayout, ValueTypeRef,
        ValueTypeStorageLifetime,
    };

    fn physical(ty: DataType, nullable: bool) -> FunctionValueType {
        FunctionValueType::from_legacy_declared_projection(ty, nullable)
    }

    fn text(kind: CharacterKind, len: u32, nullable: bool) -> FunctionValueType {
        let storage = ValueTypeStorageLifetime::pure();
        let kind = ValueKind::BoundedCharacter(CharacterType::new(kind, len).unwrap());
        FunctionValueType::try_from_value(
            ValueTypeRef::try_new(kind, nullable, storage.clone()).unwrap(),
            ValueLayout::try_new(&DataType::Utf8, storage).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn casts_keep_original_error_categories_and_try_preserves_source_errors() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            "unused",
            " +17 ",
            "bad",
            "2147483648",
        ]));
        let source = physical(DataType::Utf8, false);
        let target = physical(DataType::Int32, false);
        let control = LargeIntConversionControl;
        let selection = Selection::try_sparse(4, &[1, 2, 3]).unwrap();
        let explicit =
            PreparedCastRecipe::try_new_trino(&source, &target, TrinoCastMode::Explicit, &control)
                .unwrap();
        let output = explicit
            .evaluate_selected(EvaluatedArgument::Column(&input), selection, &[], &control)
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(17), None, None],
        );
        assert_eq!(
            output
                .errors()
                .iter()
                .map(|e| (e.selected_ordinal(), e.reason()))
                .collect::<Vec<_>>(),
            vec![
                (1, DataFailureReason::Domain(INVALID_CAST_ARGUMENT)),
                (2, DataFailureReason::Domain(INVALID_CAST_ARGUMENT)),
            ],
        );

        let source_rows = novarocks_functions::SelectedValues::try_new(
            selection,
            &DataType::Utf8,
            Arc::new(StringArray::from(vec![Some("17"), None, Some("bad")])),
            Box::new([RowDataError::with_reason(
                1,
                DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
                "original source row error",
            )]),
        )
        .unwrap();
        let guarded = PreparedCastRecipe::try_new_trino(
            &source,
            &target.with_nullable(true).unwrap(),
            TrinoCastMode::Try,
            &control,
        )
        .unwrap();
        let output = guarded
            .evaluate_selected(
                EvaluatedArgument::SelectedColumn(&source_rows),
                selection,
                source_rows.errors(),
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(17), None, None],
        );
        assert_eq!(output.errors(), source_rows.errors());
    }

    #[test]
    fn casts_round_numeric_values_before_range_check() {
        let control = LargeIntConversionControl;
        let decimal = physical(DataType::Decimal128(4, 1), false);
        let tiny = physical(DataType::Int8, false);
        let input: ArrayRef = Arc::new(
            Decimal128Array::from(vec![15, -15, 1274, 1275, -1285])
                .with_precision_and_scale(4, 1)
                .unwrap(),
        );
        let recipe =
            PreparedCastRecipe::try_new_trino(&decimal, &tiny, TrinoCastMode::Explicit, &control)
                .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(5),
                &[],
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), Some(-2), Some(127), None, None],
        );
        assert!(
            output
                .errors()
                .iter()
                .all(|e| e.reason() == DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE))
        );

        let input: ArrayRef = Arc::new(Float64Array::from(vec![-1.5, f64::NAN, f64::INFINITY]));
        let recipe = PreparedCastRecipe::try_new_trino(
            &physical(DataType::Float64, false),
            &tiny,
            TrinoCastMode::Explicit,
            &control,
        )
        .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(3),
                &[],
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-2), None, None],
        );
        assert_eq!(
            output.errors()[0].reason(),
            DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
        );
        assert_eq!(
            output.errors()[1].reason(),
            DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE)
        );
    }

    #[test]
    fn casts_use_unicode_character_width_and_checked_assignment() {
        let control = LargeIntConversionControl;
        let source = text(CharacterKind::Varchar, 4, false);
        let target = text(CharacterKind::Varchar, 2, false);
        let input: ArrayRef = Arc::new(StringArray::from(vec!["甲乙x", "甲乙 ", "ab "]));
        for (mode, expected, failures) in [
            (
                TrinoCastMode::Explicit,
                vec![Some("甲乙"), Some("甲乙"), Some("ab")],
                0,
            ),
            (
                TrinoCastMode::StoreAssignment,
                vec![None, Some("甲乙"), Some("ab")],
                1,
            ),
        ] {
            let recipe =
                PreparedCastRecipe::try_new_trino(&source, &target, mode, &control).unwrap();
            let output = recipe
                .evaluate_selected(
                    EvaluatedArgument::Column(&input),
                    Selection::all(3),
                    &[],
                    &control,
                )
                .unwrap();
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected,
            );
            assert_eq!(output.errors().len(), failures);
        }

        let source = physical(DataType::Int32, false);
        let input: ArrayRef = Arc::new(Int32Array::from(vec![12, 123]));
        let recipe =
            PreparedCastRecipe::try_new_trino(&source, &target, TrinoCastMode::Explicit, &control)
                .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(2),
                &[],
                &control,
            )
            .unwrap();
        assert_eq!(
            output.errors()[0].reason(),
            DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
        );
    }
    #[test]
    fn casts_apply_the_same_leaf_to_each_array_and_keep_nulls_within_good_rows() {
        use arrow_array::ListArray;
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::Field;

        let input: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0_i32, 2, 4, 4].into()),
            Arc::new(StringArray::from(vec![
                Some("17"),
                None,
                Some("18"),
                Some("bad"),
            ])),
            None,
        ));
        let source = physical(input.data_type().clone(), false);
        let target = physical(
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        );
        let control = LargeIntConversionControl;
        for (mode, errors) in [(TrinoCastMode::Explicit, 1), (TrinoCastMode::Try, 0)] {
            let recipe =
                PreparedCastRecipe::try_new_trino(&source, &target, mode, &control).unwrap();
            let output = recipe
                .evaluate_selected(
                    EvaluatedArgument::Column(&input),
                    Selection::all(3),
                    &[],
                    &control,
                )
                .unwrap();
            let array = output
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(output.errors().len(), errors);
            assert_eq!(
                array
                    .value(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(17), None],
            );
            assert!(array.is_null(1));
            assert!(array.value(2).is_empty());
        }
    }

    #[test]
    fn casts_row_fields_by_position_and_journals_only_reachable_rows() {
        use arrow_array::StructArray;
        use arrow_buffer::NullBuffer;
        use arrow_schema::{Field, Fields};

        let source_fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("raw", DataType::Utf8, true),
        ]
        .into();
        let target_fields: Fields = vec![
            Field::new("raw", DataType::Int32, false),
            Field::new("id", DataType::Int32, true),
        ]
        .into();
        let source = physical(DataType::Struct(source_fields.clone()), true);
        let target = physical(DataType::Struct(target_fields.clone()), true);
        let input: ArrayRef = Arc::new(
            StructArray::try_new(
                source_fields,
                vec![
                    Arc::new(Int32Array::from(vec![5, 7, 8, 9, 11, 13])),
                    Arc::new(StringArray::from(vec![
                        Some("unselected"),
                        Some("8"),
                        Some("hidden"),
                        Some("bad"),
                        None,
                        Some("unselected"),
                    ])),
                ],
                Some(NullBuffer::from(vec![true, true, false, true, true, true])),
            )
            .unwrap(),
        );
        let control = LargeIntConversionControl;
        let selection = Selection::try_sparse(6, &[1, 2, 3, 4]).unwrap();
        for (mode, error_count) in [(TrinoCastMode::Explicit, 1), (TrinoCastMode::Try, 0)] {
            let recipe =
                PreparedCastRecipe::try_new_trino(&source, &target, mode, &control).unwrap();
            let out = recipe
                .evaluate_selected(EvaluatedArgument::Column(&input), selection, &[], &control)
                .unwrap();
            assert_eq!(out.errors().len(), error_count);
            if let Some(error) = out.errors().first() {
                assert_eq!(error.selected_ordinal(), 2);
                assert_eq!(
                    error.reason(),
                    DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
                );
            }
            let row = out.values().as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(row.fields(), &target_fields);
            assert!(row.is_valid(0));
            assert!(row.is_null(1));
            assert!(row.is_null(2));
            assert!(row.is_valid(3));
            let first = row.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!((first.value(0), first.value(3)), (7, 11));
            let second = row.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(second.value(0), 8);
            assert!(second.is_null(3));
        }

        let compact = novarocks_functions::SelectedValues::try_new(
            selection,
            source.data_type(),
            input.slice(1, 4),
            Box::new([RowDataError::with_reason(
                1,
                DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
                "original ROW source error",
            )]),
        )
        .unwrap();
        let try_cast =
            PreparedCastRecipe::try_new_trino(&source, &target, TrinoCastMode::Try, &control)
                .unwrap();
        let output = try_cast
            .evaluate_selected(
                EvaluatedArgument::SelectedColumn(&compact),
                selection,
                compact.errors(),
                &control,
            )
            .unwrap();
        assert_eq!(output.errors(), compact.errors());

        let wrong_arity = physical(
            DataType::Struct(vec![Field::new("raw", DataType::Int32, true)].into()),
            true,
        );
        assert_eq!(
            PreparedCastRecipe::try_new_trino(&source, &wrong_arity, TrinoCastMode::Try, &control),
            Err(novarocks_functions::CastPrepareError::Illegal)
        );
    }

    #[test]
    fn casts_timestamp_array_through_the_original_temporal_text_formatter() {
        use arrow_array::{ListArray, TimestampMicrosecondArray};
        use arrow_buffer::{NullBuffer, OffsetBuffer};
        use novarocks_type_contract::{
            TemporalPrecision, TemporalType, list_value_type_with_construction,
        };

        let temporal = TemporalType::Timestamp(TemporalPrecision::MICROSECONDS);
        let storage = ValueTypeStorageLifetime::pure();
        let item = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(ValueKind::Temporal(temporal), true, storage.clone()).unwrap(),
            ValueLayout::try_new(&temporal.carrier(), storage.clone()).unwrap(),
        )
        .unwrap();
        let source = list_value_type_with_construction::<CompileControlError>(
            &item,
            true,
            storage.clone(),
            None,
        )
        .unwrap();
        let target = list_value_type_with_construction::<CompileControlError>(
            &physical(DataType::Utf8, true),
            true,
            storage,
            None,
        )
        .unwrap();
        let DataType::List(field) = source.data_type() else {
            unreachable!();
        };
        let input: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(field),
                OffsetBuffer::new(vec![0_i32, 3, 3, 4].into()),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(0),
                    None,
                    Some(-1),
                    Some(1),
                ])),
                Some(NullBuffer::from(vec![true, true, false])),
            )
            .unwrap(),
        );
        let control = LargeIntConversionControl;
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let recipe =
                PreparedCastRecipe::try_new_trino(&source, &target, mode, &control).unwrap();
            let result = recipe
                .evaluate_selected(
                    EvaluatedArgument::Column(&input),
                    Selection::all(3),
                    &[],
                    &control,
                )
                .unwrap();
            assert!(result.errors().is_empty());
            let lists = result
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let values = lists.value(0);
            let text = values.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                text.iter().collect::<Vec<_>>(),
                vec![
                    Some("1970-01-01 00:00:00.000000"),
                    None,
                    Some("1969-12-31 23:59:59.999999"),
                ],
            );
            assert!(lists.value(1).is_empty());
            assert!(lists.is_null(2));
        }
        assert_eq!(
            PreparedCastRecipe::try_new_trino(&source, &target, TrinoCastMode::Implicit, &control),
            Err(novarocks_functions::CastPrepareError::Illegal)
        );
    }

    #[test]
    fn casts_map_rejects_converted_duplicates_and_nulls_only_the_failed_try_row() {
        use arrow_array::{Int64Array, MapArray, StructArray};
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::Field;

        let entry_field = |key_type, value_type| {
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", key_type, false)),
                        Arc::new(Field::new("value", value_type, true)),
                    ]
                    .into(),
                ),
                false,
            ))
        };
        let entries = entry_field(DataType::Int64, DataType::Utf8);
        let DataType::Struct(fields) = entries.data_type() else {
            unreachable!();
        };
        let input: ArrayRef = Arc::new(MapArray::new(
            Arc::clone(&entries),
            OffsetBuffer::new(vec![0_i32, 2, 3, 3].into()),
            StructArray::new(
                fields.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![
                        9_007_199_254_740_992,
                        9_007_199_254_740_993,
                        1,
                    ])),
                    Arc::new(StringArray::from(vec!["1", "2", "3"])),
                ],
                None,
            ),
            None,
            false,
        ));
        let source = physical(input.data_type().clone(), false);
        let target = physical(
            DataType::Map(entry_field(DataType::Float64, DataType::Int32), false),
            true,
        );
        let control = LargeIntConversionControl;
        for (mode, errors) in [(TrinoCastMode::Explicit, 1), (TrinoCastMode::Try, 0)] {
            let recipe =
                PreparedCastRecipe::try_new_trino(&source, &target, mode, &control).unwrap();
            let output = recipe
                .evaluate_selected(
                    EvaluatedArgument::Column(&input),
                    Selection::all(3),
                    &[],
                    &control,
                )
                .unwrap();
            let map = output.values().as_any().downcast_ref::<MapArray>().unwrap();
            assert_eq!(output.errors().len(), errors);
            if let Some(error) = output.errors().first() {
                assert_eq!(
                    error.reason(),
                    DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
                );
            }
            assert!(map.is_null(0));
            assert_eq!(map.value_length(1), 1);
            assert!(map.is_valid(2));
            assert_eq!(map.value_length(2), 0);
            assert_eq!(
                map.values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                3
            );
        }
    }

    #[test]
    fn casts_canonicalize_char_and_comparison_orders_with_space_fill() {
        use novarocks_functions::PreparedComparisonRecipe;
        use novarocks_type_contract::ComparisonOperator;

        let control = LargeIntConversionControl;
        let varchar = text(CharacterKind::Varchar, 3, false);
        let character = text(CharacterKind::Char, 4, false);
        let input: ArrayRef = Arc::new(StringArray::from(vec!["a ", "a\t", "é "]));
        let recipe = PreparedCastRecipe::try_new_trino(
            &varchar,
            &character,
            TrinoCastMode::Explicit,
            &control,
        )
        .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(3),
                &[],
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("a\t"), Some("é")],
        );
        let recipe = PreparedComparisonRecipe::try_new(
            ComparisonOperator::Gt,
            &character,
            &character,
            &control,
        )
        .unwrap();
        assert_eq!(
            recipe
                .compare_rows(
                    EvaluatedArgument::Column(output.values()),
                    0,
                    0,
                    EvaluatedArgument::Column(output.values()),
                    1,
                    1,
                    &control,
                )
                .unwrap(),
            Some(true),
        );
    }
    #[test]
    fn casts_accept_trino_microsecond_temporal_text_and_round_before_materialization() {
        use arrow_array::{
            Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
        };
        use arrow_schema::TimeUnit::{Microsecond, Nanosecond};

        let control = LargeIntConversionControl;
        let source = physical(DataType::Utf8, false);
        let timestamp = physical(DataType::Timestamp(Microsecond, None), false);
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            "1970-1-1",
            "1970-01-01 00:00:00.0000005",
            "1970-01-01T00:00:00",
            "1970-01-01 01:00:00 Asia/Shanghai",
            "1970-01-01 01:00:00 Bad/Zone",
        ]));
        let recipe = PreparedCastRecipe::try_new_trino(
            &source,
            &timestamp,
            TrinoCastMode::Explicit,
            &control,
        )
        .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(5),
                &[],
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), None, Some(3_600_000_000), None],
        );
        assert_eq!(output.errors().len(), 2);
        assert!(
            output
                .errors()
                .iter()
                .all(|e| e.reason() == DataFailureReason::Domain(INVALID_CAST_ARGUMENT))
        );

        let input: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            -1501, -1500, -501, -500, 500,
        ]));
        let recipe = PreparedCastRecipe::try_new_trino(
            &physical(DataType::Timestamp(Nanosecond, None), false),
            &timestamp,
            TrinoCastMode::Explicit,
            &control,
        )
        .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(5),
                &[],
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-2), Some(-1), Some(-1), Some(0), Some(1)],
        );

        let input: ArrayRef = Arc::new(StringArray::from(vec![
            "1:02",
            "23:59:59.9999995",
            "2020-01-01 01:01:01",
            "24:00",
        ]));
        let time = physical(DataType::Time64(Microsecond), false);
        let recipe =
            PreparedCastRecipe::try_new_trino(&source, &time, TrinoCastMode::Explicit, &control)
                .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(4),
                &[],
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(3_720_000_000), Some(0), None, None],
        );
        assert_eq!(output.errors().len(), 2);
    }
    #[test]
    fn casts_varchar_bytes_and_distinguishes_illegal_conversion_from_capability_gap() {
        use arrow_array::BinaryArray;
        use novarocks_functions::CastPrepareError;

        let control = LargeIntConversionControl;
        let varchar = text(CharacterKind::Varchar, 2, false);
        let binary = physical(DataType::Binary, false);
        let input: ArrayRef = Arc::new(StringArray::from(vec!["甲乙", "é"]));
        let recipe =
            PreparedCastRecipe::try_new_trino(&varchar, &binary, TrinoCastMode::Explicit, &control)
                .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(2),
                &[],
                &control,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("甲乙".as_bytes()), Some("é".as_bytes())],
        );
        assert!(matches!(
            PreparedCastRecipe::try_new_trino(&binary, &varchar, TrinoCastMode::Explicit, &control),
            Err(CastPrepareError::Illegal),
        ));
        let character = text(CharacterKind::Char, 3, false);
        let recipe = PreparedCastRecipe::try_new_trino(
            &character,
            &binary,
            TrinoCastMode::Explicit,
            &control,
        )
        .unwrap();
        let output = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                Selection::all(2),
                &[],
                &control,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("甲乙 ".as_bytes()), Some("é  ".as_bytes())],
        );
        assert!(matches!(
            PreparedCastRecipe::try_new_trino(
                &character,
                &physical(DataType::Time64(arrow_schema::TimeUnit::Microsecond), false),
                TrinoCastMode::Explicit,
                &control,
            ),
            Err(CastPrepareError::Illegal),
        ));
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let array = physical(
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
        );
        assert!(matches!(
            PreparedCastRecipe::try_new_trino(&json, &array, TrinoCastMode::Explicit, &control),
            Err(CastPrepareError::Unsupported),
        ));
    }
    #[test]
    fn casts_match_trino_float_text_for_subnormals_midpoints_and_special_values() {
        use arrow_array::Float32Array;

        let control = LargeIntConversionControl;
        let doubles: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::from_bits(1),
            f64::from_bits(2),
            f64::from_bits(20),
            f64::MIN_POSITIVE,
            1e23,
            1e20,
            12345.678912345678,
            0.125,
            -0.0,
            0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]));
        let reals: ArrayRef = Arc::new(Float32Array::from(vec![
            f32::from_bits(1),
            f32::from_bits(7),
            f32::MIN_POSITIVE,
            10.123_457,
            1e20,
            12345645.0,
            -1.2345675,
            -0.0,
            0.0,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ]));
        for (input, expected) in [
            (
                doubles,
                vec![
                    "4.9E-324",
                    "1.0E-323",
                    "1.0E-322",
                    "2.2250738585072014E-308",
                    "9.999999999999999E22",
                    "1.0E20",
                    "1.2345678912345678E4",
                    "1.25E-1",
                    "-0E0",
                    "0E0",
                    "NaN",
                    "Infinity",
                    "-Infinity",
                ],
            ),
            (
                reals,
                vec![
                    "1.4E-45",
                    "9.8E-45",
                    "1.175494E-38",
                    "1.012346E1",
                    "1.0E20",
                    "1.234565E7",
                    "-1.234568E0",
                    "-0E0",
                    "0E0",
                    "NaN",
                    "Infinity",
                    "-Infinity",
                ],
            ),
        ] {
            let recipe = PreparedCastRecipe::try_new_trino(
                &physical(input.data_type().clone(), false),
                &physical(DataType::Utf8, false),
                TrinoCastMode::Explicit,
                &control,
            )
            .unwrap();
            let output = recipe
                .evaluate_selected(
                    EvaluatedArgument::Column(&input),
                    Selection::all(input.len()),
                    &[],
                    &control,
                )
                .unwrap();
            assert!(output.errors().is_empty());
            let rendered = output
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                rendered.iter().collect::<Vec<_>>(),
                expected.into_iter().map(Some).collect::<Vec<_>>()
            );
        }
    }
}
