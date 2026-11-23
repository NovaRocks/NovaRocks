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

use super::*;
use arrow_array::{Int64Array, StringArray};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{collections::HashMap, sync::Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn catalog() -> EngineFunctionCatalog {
    builtin::catalogue::build_builtin_engine_function_catalog().unwrap()
}
fn value(ty: DataType, nullable: bool) -> FunctionArgument {
    full_value(FunctionValueType::from_legacy_declared_projection(
        ty, nullable,
    ))
}
fn full_value(value_type: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type,
        constant: None,
    }
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}
fn bound(
    catalog: &EngineFunctionCatalog,
    name: &str,
    kind: FunctionKind,
    arguments: &[FunctionArgument],
) -> ResolvedFunctionBinding {
    catalog
        .resolve_bound_user(name, kind, request(arguments), &Control::default())
        .unwrap()
}
fn exact(
    catalog: &EngineFunctionCatalog,
    bound: &ResolvedFunctionBinding,
    arguments: &[FunctionArgument],
    control: &Control,
) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
    catalog.select_exact_overload_observed(
        &bound.function_id,
        bound.kind,
        &bound.selected.overload,
        request(arguments),
        control,
    )
}
fn canonical(
    catalog: &EngineFunctionCatalog,
    name: &str,
    kind: FunctionKind,
    initial: &[FunctionArgument],
    late: &[FunctionArgument],
    abi: PureKernelAbi,
) -> Arc<FunctionBindingSelection> {
    let original = bound(catalog, name, kind, initial);
    if initial
        .iter()
        .map(FunctionArgument::argument_type)
        .collect::<Vec<_>>()
        != late
            .iter()
            .map(FunctionArgument::argument_type)
            .collect::<Vec<_>>()
    {
        assert!(
            catalog
                .validate_frozen_selection(
                    &original.function_id,
                    kind,
                    &original.selected,
                    request(late),
                    &Control::default()
                )
                .is_err(),
            "old selected types must not be silently retagged: {name}"
        );
    }
    let selected = exact(catalog, &original, late, &Control::default()).unwrap();
    let authored = bound(catalog, name, kind, late);
    assert_eq!(
        selected.as_ref(),
        &authored.selected,
        "same original installed author: {name}"
    );
    assert_eq!(selected.overload, original.selected.overload);
    catalog
        .validate_frozen_selection(
            &original.function_id,
            kind,
            &selected,
            request(late),
            &Control::default(),
        )
        .unwrap();
    let declaration = catalog
        .pure_overload_declaration_observed(
            &original.function_id,
            kind,
            &selected.overload,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(declaration.implementation().abi, abi);
    selected
}
fn scalar_carrier(selected: &FunctionBindingSelection) -> &DataType {
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar selected result")
    };
    result.data_type()
}
fn list(nullable: bool) -> FunctionArgument {
    value(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        nullable,
    )
}
fn map(nullable: bool) -> FunctionArgument {
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ]
            .into(),
        ),
        false,
    );
    value(DataType::Map(Arc::new(entries), true), nullable)
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 32,
        max_logical_elements: 128,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 << 20,
        max_library_validation_bytes: 4 << 20,
    }
}
fn ordinal_i64() -> ConstantValue {
    let ty = FunctionValueType::new(DataType::Int64, false);
    ConstantPool::try_new(
        Arc::new(Field::new("original_count", DataType::Int64, false)),
        ty,
        Int64Array::from(vec![901, 3, 777]).to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap()
    .value(1)
    .unwrap()
}
fn constant(value: &ConstantValue) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: value.value_type().clone(),
        constant: Some(value.clone()),
    }
}
fn prefixes(
    call: impl Fn(&Control) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError>,
    success: bool,
) {
    let baseline = Control::default();
    let original = call(&baseline);
    assert_eq!(original.is_ok(), success);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    if success {
        assert!(trace.len() >= 2);
        assert!(trace.iter().any(|units| *units > 0));
    } else {
        // A semantic refusal can precede the first flush. Do not manufacture
        // an outer footer; an armed later callback must not replace the cause.
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((trace.len(), cause)),
            };
            assert_eq!(call(&control).unwrap_err(), *original.as_ref().unwrap_err());
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }
    for stop in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(call(&control), Err(FunctionBindingError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn installed_exact_scalar_owners_reauthor_nullable_numeric_text_calendar_and_collection() {
    let catalog = catalog();
    for (name, input, carrier) in [
        ("abs", DataType::Int64, DataType::FixedSizeBinary(16)),
        ("upper", DataType::Utf8, DataType::Utf8),
        ("year", DataType::Date32, DataType::Int64),
    ] {
        let selected = canonical(
            &catalog,
            name,
            FunctionKind::Scalar,
            &[value(input.clone(), false)],
            &[value(input, true)],
            PureKernelAbi::ScalarV1,
        );
        assert_eq!(scalar_carrier(&selected), &carrier);
        if name == "abs" {
            assert_eq!(
                selected.result_type,
                FunctionResultType::Scalar(
                    FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        true,
                        ValueLogicalType::LargeInt,
                    )
                    .unwrap(),
                ),
            );
        }
    }
    for (initial, late) in [(list(false), list(true)), (map(false), map(true))] {
        let selected = canonical(
            &catalog,
            "cardinality",
            FunctionKind::Scalar,
            &[initial],
            std::slice::from_ref(&late),
            PureKernelAbi::ScalarV1,
        );
        assert_eq!(selected.argument_types.as_ref(), &[late.argument_type()]);
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, true))
        );
    }
}

#[test]
fn installed_exact_round_and_truncate_reuse_original_precision_constant_and_overload() {
    let catalog = catalog();
    let digits = ordinal_i64();
    for name in ["round", "truncate"] {
        let initial = [value(DataType::Float64, false), constant(&digits)];
        let late = [value(DataType::Float64, true), constant(&digits)];
        let selected = canonical(
            &catalog,
            name,
            FunctionKind::Scalar,
            &initial,
            &late,
            PureKernelAbi::ScalarV1,
        );
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Float64, true))
        );
        assert_eq!(
            selected.argument_types.as_ref(),
            &[late[0].argument_type(), late[1].argument_type()]
        );
        let FunctionArgument::Value {
            constant: Some(retained),
            ..
        } = &late[1]
        else {
            unreachable!()
        };
        assert_eq!(retained.ordinal(), 1);
        assert_eq!(retained.try_i64().unwrap(), Some(3));
        assert_eq!(
            retained.pool().backing_identity(),
            digits.pool().backing_identity()
        );
        assert!(Arc::ptr_eq(
            retained.pool().field_ref(),
            digits.pool().field_ref()
        ));
    }
}

#[test]
fn installed_exact_count_min_max_keep_actual_aggregate_state_and_full_signature() {
    let catalog = catalog();
    let star = canonical(
        &catalog,
        "count",
        FunctionKind::Aggregate,
        &[],
        &[],
        PureKernelAbi::AggregateWindowV1,
    );
    assert!(star.argument_types.is_empty());
    assert_eq!(
        star.result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false))
    );
    for name in ["count", "min", "max"] {
        let initial = [value(DataType::Int64, false)];
        let late = [value(DataType::Int64, true)];
        let original = bound(&catalog, name, FunctionKind::Aggregate, &initial);
        let selected = canonical(
            &catalog,
            name,
            FunctionKind::Aggregate,
            &initial,
            &late,
            PureKernelAbi::AggregateWindowV1,
        );
        assert_eq!(selected.argument_types.as_ref(), &[late[0].argument_type()]);
        assert_eq!(scalar_carrier(&selected), &DataType::Int64);
        let aggregate = selected.aggregate.as_ref().unwrap();
        assert_eq!(
            aggregate.state_format,
            original.selected.aggregate.as_ref().unwrap().state_format
        );
        assert_eq!(
            aggregate.intermediate_type.data_type().clone(),
            DataType::Int64
        );
    }
    // This is selected binding/state-format authoring, not phase preparation,
    // a COUNT Window adapter, DISTINCT, or a SUM overflow decision.
}

#[test]
fn installed_exact_window_owners_keep_ranking_offset_value_and_ntile_declarations() {
    let catalog = catalog();
    for (name, carrier) in [
        ("row_number", DataType::Int64),
        ("rank", DataType::Int64),
        ("dense_rank", DataType::Int64),
        ("cume_dist", DataType::Float64),
        ("percent_rank", DataType::Float64),
    ] {
        let selected = canonical(
            &catalog,
            name,
            FunctionKind::Window,
            &[],
            &[],
            PureKernelAbi::WindowV1,
        );
        assert_eq!(scalar_carrier(&selected), &carrier);
    }
    let count = ordinal_i64();
    for name in ["lead", "lag"] {
        let initial = [value(DataType::Utf8, false), constant(&count)];
        let late = [value(DataType::Utf8, true), constant(&count)];
        let selected = canonical(
            &catalog,
            name,
            FunctionKind::Window,
            &initial,
            &late,
            PureKernelAbi::WindowV1,
        );
        assert_eq!(scalar_carrier(&selected), &DataType::Utf8);
        assert_eq!(
            selected.argument_types.as_ref(),
            &[late[0].argument_type(), late[1].argument_type()]
        );
    }
    for name in ["first_value", "last_value"] {
        let selected = canonical(
            &catalog,
            name,
            FunctionKind::Window,
            &[value(DataType::Int64, false)],
            &[value(DataType::Int64, true)],
            PureKernelAbi::WindowV1,
        );
        assert_eq!(scalar_carrier(&selected), &DataType::Int64);
    }
    let selected = canonical(
        &catalog,
        "ntile",
        FunctionKind::Window,
        &[constant(&count)],
        &[constant(&count)],
        PureKernelAbi::WindowV1,
    );
    assert_eq!(scalar_carrier(&selected), &DataType::Int64);
}

#[test]
fn installed_exact_unnest_preserves_whole_relation_and_nested_full_type() {
    let catalog = catalog();
    let original = [list(false)];
    let field = Arc::new(
        Field::new("provider_child", DataType::Utf8, false).with_metadata(HashMap::from([(
            "provider.source".into(),
            "retained-é".into(),
        )])),
    );
    let element = DataType::Struct(vec![field.clone()].into());
    let late = [value(
        DataType::List(Arc::new(Field::new("item", element.clone(), true))),
        true,
    )];
    // Changing a generic child's carrier is lawful at the SAME declared
    // UNNEST overload; the original selected type is consequently stale.
    let selected = canonical(
        &catalog,
        "unnest",
        FunctionKind::Table,
        &original,
        &late,
        PureKernelAbi::TableV1,
    );
    assert_eq!(selected.argument_types.as_ref(), &[late[0].argument_type()]);
    let FunctionResultType::Relation(results) = &selected.result_type else {
        panic!("whole selected Relation")
    };
    assert_eq!(
        results.as_ref(),
        &[FunctionValueType::from_legacy_declared_projection(
            element, true
        )]
    );
    let DataType::Struct(fields) = results[0].data_type() else {
        unreachable!()
    };
    assert_eq!(fields[0].name(), field.name());
    assert_eq!(fields[0].data_type(), field.data_type());
    assert!(fields[0].metadata().is_empty());
    assert!(selected.aggregate.is_none());
}

#[test]
fn installed_exact_some_none_request_keeps_original_nonzero_field_and_backing() {
    let catalog = catalog();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let field = Arc::new(
        Field::new("original_utf8", DataType::Utf8, false).with_metadata(HashMap::from([(
            "provider.unknown".into(),
            "full-field".into(),
        )])),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        StringArray::from(vec!["hidden", "ΟΣİß", "tail"]).to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let cv = pool.value(1).unwrap();
    let initial = [full_value(ty.clone())];
    let original = bound(&catalog, "upper", FunctionKind::Scalar, &initial);
    let some = [constant(&cv)];
    let selected = exact(&catalog, &original, &some, &Control::default()).unwrap();
    assert_eq!(
        selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(ty)]
    );
    let FunctionArgument::Value {
        constant: Some(retained),
        ..
    } = &some[0]
    else {
        unreachable!()
    };
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.try_utf8().unwrap(), Some("ΟΣİß"));
    assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
    assert!(matches!(
        initial[0],
        FunctionArgument::Value { constant: None, .. }
    ));
    catalog
        .validate_frozen_selection(
            &original.function_id,
            original.kind,
            &selected,
            request(&some),
            &Control::default(),
        )
        .unwrap();
}

#[test]
fn installed_exact_unknown_and_other_declared_overload_refuse_without_fallback() {
    let catalog = catalog();
    let args = [list(true)];
    let original = bound(&catalog, "cardinality", FunctionKind::Scalar, &args);
    let other = bound(&catalog, "cardinality", FunctionKind::Scalar, &[map(true)]);
    assert_ne!(original.selected.overload, other.selected.overload);
    let unknown = FunctionOverloadId::try_new("fixture/not-declared/v1").unwrap();
    let unknown_function = FunctionId::try_new("fixture/no-installed-owner/v1").unwrap();
    for (identity, kind, overload) in [
        (
            &unknown_function,
            FunctionKind::Scalar,
            &original.selected.overload,
        ),
        (
            &original.function_id,
            FunctionKind::Window,
            &original.selected.overload,
        ),
        (&original.function_id, FunctionKind::Scalar, &unknown),
        (
            &original.function_id,
            FunctionKind::Scalar,
            &other.selected.overload,
        ),
    ] {
        prefixes(
            |control| {
                catalog.select_exact_overload_observed(
                    identity,
                    kind,
                    overload,
                    request(&args),
                    control,
                )
            },
            false,
        );
    }
    let source = ordinal_i64();
    let wrong = [FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Utf8, false),
        constant: Some(source),
    }];
    let upper = bound(
        &catalog,
        "upper",
        FunctionKind::Scalar,
        &[value(DataType::Utf8, false)],
    );
    prefixes(|control| exact(&catalog, &upper, &wrong, control), false);
}

#[test]
fn installed_exact_actual_small_prefixes_and_wide_collection_quantum_preserve_first_cause() {
    let catalog = catalog();
    let count = ordinal_i64();
    let cases = vec![
        (
            "abs",
            FunctionKind::Scalar,
            vec![value(DataType::Int64, true)],
        ),
        (
            "upper",
            FunctionKind::Scalar,
            vec![value(DataType::Utf8, true)],
        ),
        (
            "year",
            FunctionKind::Scalar,
            vec![value(DataType::Date32, true)],
        ),
        ("cardinality", FunctionKind::Scalar, vec![list(true)]),
        (
            "round",
            FunctionKind::Scalar,
            vec![value(DataType::Float64, true), constant(&count)],
        ),
        (
            "truncate",
            FunctionKind::Scalar,
            vec![value(DataType::Float64, true), constant(&count)],
        ),
        ("count", FunctionKind::Aggregate, vec![]),
        (
            "min",
            FunctionKind::Aggregate,
            vec![value(DataType::Int64, true)],
        ),
        (
            "max",
            FunctionKind::Aggregate,
            vec![value(DataType::Int64, true)],
        ),
        ("row_number", FunctionKind::Window, vec![]),
        (
            "lead",
            FunctionKind::Window,
            vec![value(DataType::Utf8, true), constant(&count)],
        ),
        (
            "first_value",
            FunctionKind::Window,
            vec![value(DataType::Int64, true)],
        ),
        ("ntile", FunctionKind::Window, vec![constant(&count)]),
        ("unnest", FunctionKind::Table, vec![list(true)]),
    ];
    for (name, kind, args) in cases {
        let original = bound(&catalog, name, kind, &args);
        prefixes(|control| exact(&catalog, &original, &args, control), true);
        let impossible = FunctionValueType::new(DataType::Boolean, true);
        prefixes(
            |control| {
                catalog.select_exact_overload_observed(
                    &original.function_id,
                    kind,
                    &original.selected.overload,
                    FunctionBindingRequest {
                        expected_result_type: Some(&impossible),
                        ..request(&args)
                    },
                    control,
                )
            },
            false,
        );
    }
    let original = bound(&catalog, "cardinality", FunctionKind::Scalar, &[list(true)]);
    let fields = (0..320)
        .map(|index| Arc::new(Field::new(format!("actual_{index}"), DataType::Int64, true)))
        .collect::<Vec<_>>();
    let wide = [value(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(fields.into()),
            true,
        ))),
        true,
    )];
    let baseline = Control::default();
    let selected = exact(&catalog, &original, &wide, &baseline).unwrap();
    assert_eq!(selected.argument_types.as_ref(), &[wide[0].argument_type()]);
    let trace = baseline.trace.lock().unwrap().clone();
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("actual admitted request type walk quantum");
    for stop in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(exact(&catalog, &original, &wide, &control), Err(FunctionBindingError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    // These are binding/control observations, not nested value copying,
    // effect refinement, lifecycle execution, or a host allocation grant.
}
