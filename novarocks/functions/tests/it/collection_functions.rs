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

use std::{sync::Arc, time::Duration};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, MapArray, StringArray, types,
};
use arrow_schema::DataType;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionKind, KernelEvaluationControl, KernelFailure, PreparedPureKernel, PureCallPreparation,
    RowDataError, ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
    builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameters,
};

struct Control;

impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("collection kernels must not wait");
    }
}

#[derive(Debug)]
struct Output {
    values: ArrayRef,
    errors: Box<[RowDataError]>,
}

impl Output {
    fn values(&self) -> &ArrayRef {
        &self.values
    }

    fn errors(&self) -> &[RowDataError] {
        &self.errors
    }
}

fn invoke(name: &str, columns: &[ArrayRef], rows: usize) -> Result<Output, KernelFailure> {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let arguments = columns
        .iter()
        .map(|column| FunctionArgument::Value {
            value_type: FunctionValueType::new(column.data_type().clone(), true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let evaluated = columns
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    invoke_bound(&catalog, bound, request, Selection::all(rows), &evaluated)
}

fn invoke_bound(
    catalog: &novarocks_functions::EngineFunctionCatalog,
    bound: novarocks_functions::ResolvedFunctionBinding,
    request: FunctionBindingRequest<'_>,
    selection: Selection<'_>,
    evaluated: &[EvaluatedArgument<'_>],
) -> Result<Output, KernelFailure> {
    let selected = Arc::new(bound.selected);
    let uses = (1..=request.arguments.len() as u32)
        .map(|index| Some(ExpressionUseId::new(index)))
        .collect::<Vec<_>>();
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        region: EvaluationRegionId::new(0),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                context: context.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&uses),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Unconditional,
            },
            Arc::clone(&selected),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &Control,
        )
        .unwrap()
        .into_prepared();
    let PreparedPureKernel::Scalar(prepared) = prepared else {
        panic!("expected scalar kernel");
    };
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let (_, values, errors) = instance
        .evaluate(selection, evaluated, &Control)?
        .into_parts();
    Ok(Output { values, errors })
}

fn evaluate(name: &str, columns: &[ArrayRef], rows: usize) -> Output {
    invoke(name, columns, rows).unwrap()
}

#[test]
fn row_constructor_keeps_sparse_selection_broadcast_and_child_nulls() {
    use arrow_array::StructArray;
    use arrow_schema::Field;
    let id: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let nullable: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(40)]));
    let result_type = FunctionValueType::new(
        DataType::Struct(
            vec![
                Field::new("ID", DataType::Int32, false),
                Field::new("", DataType::Int32, true),
            ]
            .into(),
        ),
        false,
    );
    let arguments = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, true),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: Some(&result_type),
    };
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let bound = catalog
        .resolve_bound_trusted("row", FunctionKind::Scalar, request, &Control)
        .unwrap();
    let evaluated = [
        EvaluatedArgument::Scalar(&id),
        EvaluatedArgument::Column(&nullable),
    ];
    let selection = Selection::try_sparse(4, &[1, 3]).unwrap();
    let output = invoke_bound(&catalog, bound, request, selection, &evaluated).unwrap();
    assert!(output.errors().is_empty());
    let row = output
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(row.null_count(), 0);
    assert_eq!(row.data_type(), result_type.data_type());
    assert_eq!(
        row.column(0).to_data(),
        Int32Array::from(vec![7, 7]).to_data()
    );
    assert_eq!(
        row.column(1).to_data(),
        Int32Array::from(vec![None, Some(40)]).to_data()
    );
}

#[test]
fn map_reports_invalid_pairs_and_preserves_null_values_and_input_order() {
    let keys: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Float64Type, _, _>(
        [
            Some(vec![Some(f64::NAN), Some(-0.0)]),
            Some(vec![Some(f64::NAN), Some(-f64::NAN)]),
            Some(vec![Some(-0.0), Some(0.0)]),
            Some(vec![None, Some(1.0)]),
            Some(vec![Some(1.0)]),
            None,
            Some(vec![]),
        ],
    ));
    let values: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>([
        Some(vec![Some(10), None]),
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(1), Some(2)]),
        Some(vec![]),
        Some(vec![Some(1)]),
        Some(vec![]),
    ]));
    let result = evaluate("map", &[keys, values], 7);
    assert_eq!(result.errors().len(), 4);
    let map = result.values().as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 2, 2, 2, 2, 2, 2, 2]);
    assert!(map.is_null(5));
    assert!(map.is_valid(6));
    assert_eq!(map.keys().data_type(), &DataType::Float64);
    let values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), [Some(10), None]);

    let keys: ArrayRef = Arc::new(Float64Array::from(vec![
        f64::NAN,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    ]));
    let selected = evaluate("__map_element_at", &[result.values().clone(), keys], 7);
    assert!(selected.values().is_null(0));
}

#[test]
fn map_empty_and_both_cardinality_overloads_have_bigint_results() {
    let empty = evaluate("map", &[], 2);
    assert!(empty.errors().is_empty());
    let actual = empty.values().as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(actual.keys().data_type(), &DataType::Null);
    assert_eq!(actual.values().data_type(), &DataType::Null);
    assert_eq!(actual.value_offsets(), &[0, 0, 0]);
    let count = evaluate("cardinality", &[empty.values().clone()], 2);
    let counts = count
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.iter().collect::<Vec<_>>(), [Some(0), Some(0)]);

    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>([
        Some(vec![None, Some(2)]),
        None,
    ]));
    let count = evaluate("cardinality", &[list], 2);
    let counts = count
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.iter().collect::<Vec<_>>(), [Some(2), None]);
}

#[test]
fn concat_binds_list_prepend_append_and_varchar_by_their_selected_arguments() {
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>([
        Some(vec![Some(1)]),
        Some(vec![None]),
        None,
    ]));
    let item: ArrayRef = Arc::new(Int32Array::from(vec![Some(9), None, Some(10)]));
    for (source, expected) in [
        (
            vec![Arc::clone(&list), Arc::clone(&item)],
            [Some(1), Some(9)],
        ),
        (vec![item, Arc::clone(&list)], [Some(9), Some(1)]),
    ] {
        let result = evaluate("concat", &source, 3);
        let result = result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(result.is_null(1));
        assert!(result.is_null(2));
        let items = result.value(0);
        let items = items.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(items.iter().collect::<Vec<_>>(), expected);
    }

    let result = evaluate("concat", &[list.clone(), list], 3);
    let result = result
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(result.value_length(0), 2);
    assert!(result.is_valid(1));
    assert!(result.is_null(2));

    let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
    let result = evaluate("concat", &[strings.clone(), strings], 2);
    let result = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(result.iter().collect::<Vec<_>>(), [Some("aa"), None]);
}

#[test]
fn collection_functions_concat_preflights_the_shared_utf8_child_layout() {
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::Field;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};

    // One independently allocated 32 MiB child buffer supplies all 64 arrays.
    // Its logical output is one byte beyond the Utf8 format without allocating it.
    let width = 1 << 20;
    let slots = 32;
    let child_offsets = (0..=slots).map(|index| width * index).collect::<Vec<_>>();
    let child = StringArray::try_new(
        OffsetBuffer::new(child_offsets.into()),
        vec![b'x'; (width * slots) as usize].into(),
        None,
    )
    .unwrap();
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0, slots].into()),
        Arc::new(child),
        None,
    ));

    let failure = invoke("concat", &vec![list; 64], 1)
        .unwrap_err()
        .failure_fact();
    assert_eq!(failure.class(), FailureClass::Representation);
    assert_eq!(failure.reason().code(), 259);
    let shape = failure.representation().unwrap();
    assert_eq!(shape.boundary, RepresentationBoundary::SingleOutput);
    assert_eq!(shape.layout.code.get(), 44);
    assert_eq!(shape.required, Some(2_147_483_648));
    assert_eq!(shape.extent_limit, Some(2_147_483_647));
    assert_eq!(shape.representable_prefix, None);
}

#[test]
fn element_at_is_one_based_and_keeps_array_and_map_overloads_distinct() {
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>(
        (0..4).map(|_| Some(vec![Some(10), Some(20)])),
    ));
    let index: ArrayRef = Arc::new(Int64Array::from(vec![1, -1, i64::MIN, 0]));
    let result = evaluate("element_at", &[list, index], 4);
    let result_values = result
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(
        result_values.iter().collect::<Vec<_>>(),
        [Some(10), Some(20), None, None]
    );
    assert_eq!(result.errors().len(), 1);
}

#[test]
fn map_entries_projects_anonymous_fields_and_concat_keeps_last_value() {
    use arrow_array::StructArray;
    use novarocks_type_contract::ValueKind;

    let list = |items: Vec<Option<i32>>| -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>([
            Some(items),
        ]))
    };
    let a = evaluate(
        "map",
        &[list(vec![Some(1), Some(2)]), list(vec![Some(10), Some(20)])],
        1,
    );
    let b = evaluate(
        "map",
        &[list(vec![Some(2), Some(3)]), list(vec![None, Some(30)])],
        1,
    );
    let merged = evaluate("map_concat", &[a.values().clone(), b.values().clone()], 1);
    let key: ArrayRef = Arc::new(Int32Array::from(vec![2]));
    let found = evaluate("element_at", &[merged.values().clone(), key], 1);
    assert_eq!(
        found.values().to_data(),
        Int32Array::from(vec![None]).to_data()
    );

    let output = evaluate("map_entries", &[merged.values().clone()], 1);
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let declared = FunctionValueType::new(list.data_type().clone(), true);
    let item = declared.child_at(0).unwrap();
    let ValueKind::Struct(fields) = item.value_type().kind() else {
        panic!("ROW item")
    };
    assert_eq!(
        fields
            .iter()
            .map(|field| field.row_name())
            .collect::<Vec<_>>(),
        vec![None, None]
    );
    assert!(!fields[0].value_type().nullable());
    let merged = merged.values().as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(entries.column(0).to_data(), merged.keys().to_data());
    assert_eq!(entries.column(1).to_data(), merged.values().to_data());
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let mut pairs: Vec<_> = keys.values().iter().copied().zip(values.iter()).collect();
    pairs.sort_unstable_by_key(|pair| pair.0);
    assert_eq!(pairs, vec![(1, Some(10)), (2, None), (3, Some(30))]);
}

#[test]
fn split_to_map_accounts_for_bad_pairs_as_original_row_errors() {
    use novarocks_failure_contract::{FailureProjection, kernel_reason::INVALID_FUNCTION_ARGUMENT};

    let string: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a:1|b:2|"),
        Some("a:1|a:2"),
        Some(""),
        Some("x:1:2"),
        None,
    ]));
    let delimiter: ArrayRef = Arc::new(StringArray::from(vec!["|"; 5]));
    let pair: ArrayRef = Arc::new(StringArray::from(vec![":"; 5]));
    let out = evaluate("split_to_map", &[string, delimiter, pair], 5);
    let map = out.values().as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 2, 2, 2, 2, 2]);
    assert!(map.is_null(1));
    assert!(map.is_valid(2));
    assert!(map.is_null(3));
    assert!(map.is_null(4));
    assert_eq!(
        out.errors()
            .iter()
            .map(|error| (error.selected_ordinal(), error.failure_fact().reason()))
            .collect::<Vec<_>>(),
        vec![
            (1, INVALID_FUNCTION_ARGUMENT),
            (3, INVALID_FUNCTION_ARGUMENT)
        ],
    );
}

#[test]
fn map_lookup_uses_actual_composite_comparisons_after_null_candidates() {
    use arrow_array::StructArray;
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::Field;
    use novarocks_failure_contract::{FailureProjection, kernel_reason::MAP_KEY_INDETERMINATE};

    let key = |rows: Vec<Vec<Option<i32>>>| -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>(
            rows.into_iter().map(Some),
        ))
    };
    // This carrier models a valid map_agg output with one nested NULL key.
    let keys = key(vec![
        vec![Some(1), None],
        vec![Some(1), Some(2)],
        vec![Some(2), Some(9)],
    ]);
    let fields = vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", DataType::Int32, true),
    ]
    .into();
    let entries = StructArray::try_new(
        fields,
        vec![keys, Arc::new(Int32Array::from(vec![7, 8, 9]))],
        None,
    )
    .unwrap();
    let maps: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2, 3, 3, 3].into()),
            entries,
            None,
            false,
        )
        .unwrap(),
    );
    let probes = key(vec![
        vec![Some(1), Some(2)],
        vec![None, Some(1)],
        vec![Some(1), None],
        vec![Some(1), Some(2)],
    ]);
    let output = evaluate("element_at", &[maps, probes], 4);
    assert!(output.errors().is_empty());
    assert_eq!(
        output.values().to_data(),
        Int32Array::from(vec![Some(8), None, None, None]).to_data()
    );

    let keys = key(vec![vec![Some(1), None]]);
    let fields = vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", DataType::Int32, true),
    ]
    .into();
    let entries = StructArray::try_new(
        fields,
        vec![keys, Arc::new(Int32Array::from(vec![7]))],
        None,
    )
    .unwrap();
    let map: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        )
        .unwrap(),
    );
    let result = evaluate("element_at", &[map, key(vec![vec![Some(1), Some(2)]])], 1);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(
        result.errors()[0].failure_fact().reason(),
        MAP_KEY_INDETERMINATE
    );
}
