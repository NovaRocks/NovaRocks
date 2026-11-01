// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::super::collection_cardinality_owner::{
    effects, operation, owner_for_test, prepared_for_test_with_control,
    prepared_for_test_with_policy,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionArgumentType,
    FunctionBindingRequest, FunctionBindingResolver, FunctionResultType,
    FunctionSpecializationFailure, FunctionValueType, PureFunctionMetadataOwner,
    PureScalarImplementation, ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
    specialize_frozen_scalar, specialize_scalar,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, LargeListArray, ListArray, MapArray,
    StringArray, StructArray, builder::FixedSizeBinaryBuilder, types::Int8Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, FunctionVolatility, PureCompileControl, SemanticParameters,
    ValueLogicalType,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("cardinality never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}

fn policy() -> ConstantPolicy {
    // Finite fixture admission of already allocated real Arrow arrays, not a MEM grant.
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}
fn target() -> FunctionValueType {
    FunctionValueType::new(DataType::Int32, true)
}
fn ty(array: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), nullable)
}
fn instance(source: &FunctionValueType) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            "cardinality",
            std::slice::from_ref(source),
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap(),
    )
    .unwrap()
}
fn output(result: &SelectedValues<'_>) -> Vec<Option<i32>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn pool(array: &ArrayRef, nullable: bool) -> ConstantPool {
    let source = ty(array, nullable);
    pool_with_type(array, source)
}
fn pool_with_type(array: &ArrayRef, source: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(
            source
                .try_to_field("original-collection")
                .unwrap()
                .with_data_type(array.data_type().clone())
                .with_metadata(std::collections::HashMap::<String, String>::from([(
                    "source-note".into(),
                    "original pool and selected ordinal".into(),
                )])),
        ),
        source,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}
fn list(nullable: bool) -> ArrayRef {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(7),
        None,
        Some(8),
        None,
        Some(9),
    ]));
    let raw: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(
                Field::new("authored-item", DataType::Int32, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "unknown-child-fact".into(),
                        "preserved".into(),
                    )]),
                ),
            ),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 3, 5, 6])),
            values,
            nullable.then(|| NullBuffer::from(vec![true, true, false, true])),
        )
        .unwrap(),
    );
    crate::test_value_ingress::array_for_binding(
        &raw,
        &FunctionValueType::new(raw.data_type().clone(), nullable),
    )
    .unwrap()
}
fn map(nullable: bool, sorted: bool) -> ArrayRef {
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(20), None, None, Some(50)]));
    let fields = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", DataType::Int32, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "value-fact".into(),
                "NULL values still count".into(),
            )]),
        )),
    ]
    .into();
    let entries = StructArray::new(fields, vec![keys, values], None);
    let raw: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(
                Field::new("actual-entries", entries.data_type().clone(), false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "entry-fact".into(),
                        "original".into(),
                    )]),
                ),
            ),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 4, 5])),
            entries,
            nullable.then(|| NullBuffer::from(vec![true, true, false, true])),
            sorted,
        )
        .unwrap(),
    );
    crate::test_value_ingress::array_for_binding(
        &raw,
        &FunctionValueType::new(raw.data_type().clone(), nullable),
    )
    .unwrap()
}
fn fixed(values: &[i128]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        builder.append_value(value.to_be_bytes()).unwrap();
    }
    Arc::new(builder.finish())
}
fn rich_list(dict_id: i64, ordered: bool) -> ArrayRef {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let large = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), None, Some(0), Some(1)]),
            Arc::new(StringArray::from(vec!["unused-or-used-a", "b", "unused-c"])),
        )
        .unwrap(),
    );
    // IDs/order are authored Arrow Field facts ignored by ordinary DataType Eq.
    #[allow(deprecated)]
    let dictionary_field = Arc::new(Field::new_dict(
        "dictionary",
        dictionary.data_type().clone(),
        true,
        dict_id,
        ordered,
    ));
    let json_field = json.try_to_field("json-child").unwrap();
    let mut metadata = json_field.metadata().clone();
    metadata.insert("unknown-json-fact".to_owned(), "kept".to_owned());
    let fields = vec![
        Arc::new(json_field.with_metadata(metadata)),
        Arc::new(large.try_to_field("large-child").unwrap()),
        Arc::new(uuid.try_to_field("uuid-child").unwrap()),
        dictionary_field,
    ]
    .into();
    let values: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(StringArray::from(vec![
                Some("{}"),
                None,
                Some("[]"),
                Some("null"),
            ])),
            fixed(&[-1, 0, 1, i128::MAX]),
            fixed(&[0, 1, 2, 3]),
            dictionary,
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    ));
    Arc::new(
        ListArray::try_new(
            Arc::new(
                Field::new("named-struct-item", values.data_type().clone(), true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "unknown-struct-fact".into(),
                        "preserved".into(),
                    )]),
                ),
            ),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 4])),
            values,
            None,
        )
        .unwrap(),
    )
}

#[test]
fn cardinality_two_actual_records_keep_selected_fresh_frozen_complete_sources_and_effects() {
    let owner = owner_for_test("cardinality");
    assert_eq!(operation("cardinality"), Some(()));
    for other in ["CARDINALITY", "array_length", "array_size", "map_size"] {
        assert!(operation(other).is_none());
    }
    assert_eq!(owner.binding_declaration().overloads().len(), 2);
    assert_eq!(owner.implementation_declarations().len(), 2);
    assert_eq!(
        owner.binding_declaration().function_id().as_str(),
        "builtin.scalar/cardinality/v1"
    );
    for overload in owner.binding_declaration().overloads() {
        assert_eq!(overload.effects.as_ref(), Some(&effects()));
    }
    for array in [rich_list(17, true), map(true, true)] {
        let source = ty(&array, true);
        for decimal in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let arguments = [FunctionArgument::Value {
                value_type: source.clone(),
                constant: None,
            }];
            let request = FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            };
            let selected = Arc::new(
                owner
                    .resolve(request, crate::binding_test_control())
                    .unwrap(),
            );
            assert!(
                matches!(&selected.argument_types[0], FunctionArgumentType::Value(actual) if novarocks_type_contract::arrow_data_types_exact(actual.data_type(), source.data_type()) && actual.nullable() == source.nullable() && actual.logical_type() == source.logical_type())
            );
            assert_eq!(selected.result_type, FunctionResultType::Scalar(target()));
            let FunctionArgumentType::Value(selected_source) = &selected.argument_types[0] else {
                panic!("cardinality requires a value source")
            };
            match (source.data_type(), selected_source.data_type()) {
                (DataType::List(original), DataType::List(selected_field))
                | (DataType::Map(original, _), DataType::Map(selected_field, _)) => {
                    assert!(
                        Arc::ptr_eq(original, selected_field),
                        "already-canonical source must preserve its original nested Field Arc"
                    );
                }
                _ => panic!("cardinality source carrier changed"),
            }

            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(41),
                domain: EvaluationDomainId::new(7),
                demand: EvaluationDemand::Value,
            };
            let uses = [Some(ExpressionUseId::new(42))];
            let parameters = SemanticParameters::try_new([]).unwrap();
            let input = crate::CallEffectInput {
                context,
                argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
                function_id: owner.binding_declaration().function_id(),
                kind: crate::FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: decimal,
                proof_scope: CallProofScope::Domain(context.domain),
            };
            let fresh = specialize_scalar(
                &owner,
                input,
                selected.clone(),
                ScopedExpressionEffects::pure_value(context),
                crate::binding_test_control(),
            )
            .unwrap();
            let canonical = fresh.prepared().contract().clone();
            let direct = owner
                .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
                .unwrap();
            assert!(Arc::ptr_eq(direct.contract(), &canonical));
            let frozen = specialize_frozen_scalar(
                &owner,
                input,
                selected.clone(),
                canonical.effects(),
                ScopedExpressionEffects::pure_value(context),
                crate::binding_test_control(),
            )
            .unwrap();
            assert!(std::ptr::eq(
                frozen.prepared().contract().selected(),
                selected.as_ref()
            ));
            assert_eq!(canonical.decimal_overflow_policy(), decimal);
            assert_eq!(
                canonical.effects().value_stability,
                FunctionVolatility::Immutable
            );
            assert_eq!(
                canonical.effects().own_row_error,
                crate::FunctionIntrinsicRowError::NoRowError
            );
            assert_eq!(
                canonical.effects().null_behavior,
                FunctionNullBehavior::Strict
            );
            assert_eq!(
                canonical.effects().argument_control,
                novarocks_type_contract::ArgumentControl::Eager
            );
            assert_eq!(
                canonical.effects().instance_state,
                FunctionInstanceState::None
            );
            assert!(canonical.effects().observable_effects.is_empty());
            let mut bad = canonical.effects().clone();
            bad.null_behavior = FunctionNullBehavior::CalledOnNull;
            assert!(
                specialize_frozen_scalar(
                    &owner,
                    input,
                    selected.clone(),
                    &bad,
                    ScopedExpressionEffects::pure_value(context),
                    crate::binding_test_control()
                )
                .is_err()
            );
            let mut stale = input;
            stale.decimal_overflow_policy = if decimal == DecimalOverflowPolicy::OutputNull {
                DecimalOverflowPolicy::ReportError
            } else {
                DecimalOverflowPolicy::OutputNull
            };
            assert!(
                owner
                    .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                    .is_err()
            );
            let mut wrong_kind = input;
            wrong_kind.kind = crate::FunctionKind::Aggregate;
            assert!(
                owner
                    .prepare_scalar(wrong_kind, canonical.clone(), crate::binding_test_control())
                    .is_err()
            );
            let mut wrong_context = input;
            wrong_context.context.domain = EvaluationDomainId::new(99);
            assert!(
                owner
                    .prepare_scalar(
                        wrong_context,
                        canonical.clone(),
                        crate::binding_test_control()
                    )
                    .is_err()
            );
            let child_context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(42),
                ..context
            };
            let child_effects = ScopedExpressionEffects::pure_value(context)
                .join_same_domain(ScopedExpressionEffects::primitive(
                    child_context,
                    novarocks_type_contract::ExpressionEffects {
                        may_raise_row_error: true,
                        ..novarocks_type_contract::ExpressionEffects::PURE_VALUE
                    },
                ))
                .unwrap();
            let inherited = specialize_scalar(
                &owner,
                input,
                selected.clone(),
                child_effects,
                crate::binding_test_control(),
            )
            .unwrap();
            assert!(
                inherited
                    .effects()
                    .for_use(context)
                    .unwrap()
                    .may_raise_row_error
            );
            let mut forged = (*selected).clone();
            forged.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Int32, false));
            assert!(
                owner
                    .validate_selected(&forged, request, crate::binding_test_control())
                    .is_err()
            );
        }
    }
}

#[test]
fn cardinality_hand_counts_keep_empty_null_children_and_nested_payloads_untouched() {
    for nullable in [false, true] {
        for (array, expected) in [
            (
                list(nullable),
                vec![
                    Some(3),
                    Some(0),
                    if nullable { None } else { Some(2) },
                    Some(1),
                ],
            ),
            (
                map(nullable, false),
                vec![
                    Some(2),
                    Some(0),
                    if nullable { None } else { Some(2) },
                    Some(1),
                ],
            ),
            (
                map(nullable, true),
                vec![
                    Some(2),
                    Some(0),
                    if nullable { None } else { Some(2) },
                    Some(1),
                ],
            ),
        ] {
            let source = ty(&array, nullable);
            for decimal in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let prepared = prepared_for_test_with_policy(
                    "cardinality",
                    std::slice::from_ref(&source),
                    decimal,
                )
                .unwrap();
                assert_eq!(prepared.contract().decimal_overflow_policy(), decimal);
                let args = [EvaluatedArgument::Column(&array)];
                let result = ScalarEvaluationInstance::instantiate(prepared)
                    .unwrap()
                    .evaluate(Selection::all(4), &args, &Control::default())
                    .unwrap();
                assert_eq!(output(&result), expected);
            }
        }
    }
    let rich = rich_list(17, true);
    let rich_type = ty(&rich, false);
    // This fixture deliberately owns Dictionary decoding before entering a
    // Plain kernel. Raw encoded children are not an installed capability.
    let decoded = arrow_cast::cast(rich.as_ref(), rich_type.data_type()).unwrap();
    let rich = crate::test_value_ingress::array_for_binding(&decoded, &rich_type).unwrap();
    let original = rich.to_data();
    assert_eq!(
        output(
            &instance(&rich_type)
                .evaluate(
                    Selection::all(3),
                    &[EvaluatedArgument::Column(&rich)],
                    &Control::default()
                )
                .unwrap()
        ),
        vec![Some(2), Some(0), Some(2)]
    );
    assert_eq!(rich.to_data(), original);
    // Full ConstantValue validation is the actual nested grammar author.
    let checked = pool_with_type(&rich, rich_type);
    assert!(novarocks_type_contract::arrow_data_types_exact(
        checked.value_type().data_type(),
        rich.data_type()
    ));
    assert_eq!(checked.value(2).unwrap().ordinal(), 2);
}

#[test]
fn cardinality_slice_sparse_compact_scalar_and_nonzero_pool_addresses_preserve_source() {
    for array in [list(true), map(true, false)] {
        let source = ty(&array, true);
        let sliced = array.slice(1, 3);
        let rows = [0, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let compact_values = arrow_select::take::take(
            array.as_ref(),
            &arrow_array::UInt32Array::from(vec![1, 3]),
            None,
        )
        .unwrap();
        let compact =
            SelectedValues::try_new(selection, array.data_type(), compact_values, Box::default())
                .unwrap();
        for argument in [
            EvaluatedArgument::Column(&sliced),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            assert_eq!(
                output(
                    &instance(&source)
                        .evaluate(selection, &[argument], &Control::default())
                        .unwrap()
                ),
                vec![Some(0), Some(1)]
            );
        }
        let checked = pool(&array, true);
        let selected = checked.value(3).unwrap();
        let scalar = array.slice(3, 1);
        for argument in [
            EvaluatedArgument::Constant(&selected),
            EvaluatedArgument::Scalar(&scalar),
        ] {
            assert_eq!(
                output(
                    &instance(&source)
                        .evaluate(selection, &[argument], &Control::default())
                        .unwrap()
                ),
                vec![Some(1), Some(1)]
            );
        }
        assert_eq!(selected.ordinal(), 3);
        assert!(Arc::ptr_eq(selected.pool().array(), checked.array()));
        assert!(Arc::ptr_eq(
            selected.pool().field_ref(),
            checked.field_ref()
        ));
        let null = checked.value(2).unwrap();
        assert_eq!(
            output(
                &instance(&source)
                    .evaluate(
                        selection,
                        &[EvaluatedArgument::Constant(&null)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![None, None]
        );
        let original_rows = [0, 3];
        let original_selection = Selection::try_sparse(4, &original_rows).unwrap();
        assert_eq!(
            output(
                &instance(&source)
                    .evaluate(
                        original_selection,
                        &[EvaluatedArgument::Column(&array)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![
                Some(if matches!(array.data_type(), DataType::Map(_, _)) {
                    2
                } else {
                    3
                }),
                Some(1)
            ]
        );
    }
}

#[test]
fn cardinality_complete_dictionary_field_identity_and_unknown_metadata_are_not_arrow_eq() {
    let source_array = rich_list(17, true);
    let foreign_header = rich_list(18, false);
    assert_eq!(source_array.data_type(), foreign_header.data_type());
    assert!(!novarocks_type_contract::arrow_data_types_exact(
        source_array.data_type(),
        foreign_header.data_type()
    ));
    let source = ty(&source_array, false);
    assert!(source.same_value_domain(&ty(&foreign_header, false)));
    assert!(matches!(
        instance(&source).evaluate(
            Selection::all(3),
            &[EvaluatedArgument::Column(&foreign_header)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let checked = pool(&foreign_header, false);
    let selected = checked.value(2).unwrap();
    assert!(matches!(
        instance(&source).evaluate(
            Selection::all(3),
            &[EvaluatedArgument::Constant(&selected)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let plain = list(false);
    let DataType::List(item) = plain.data_type() else {
        unreachable!()
    };
    for changed_item in [
        Field::new(
            "misleading-item-name",
            item.data_type().clone(),
            item.is_nullable(),
        )
        .with_metadata(item.metadata().clone()),
        Field::new(item.name(), item.data_type().clone(), item.is_nullable()),
    ] {
        let changed = FunctionValueType::from_legacy_declared_projection(
            DataType::List(Arc::new(changed_item)),
            false,
        );
        assert!(
            instance(&changed)
                .evaluate(
                    Selection::all(4),
                    &[EvaluatedArgument::Column(&plain)],
                    &Control::default()
                )
                .is_ok(),
            "anonymous role names and source annotations do not author identity"
        );
    }
    let DataType::List(item) = source_array.data_type() else {
        unreachable!()
    };
    let DataType::Struct(fields) = item.data_type() else {
        unreachable!()
    };
    // Only the unknown outer item metadata changes. All descendants, including
    // the dictionary ID/order and nominal tags, remain the same borrowed Fields.
    let changed_item_metadata = item
        .as_ref()
        .clone()
        .with_metadata(arrow_schema::Metadata::default());
    let lost_unknown = FunctionValueType::from_legacy_declared_projection(
        DataType::List(Arc::new(changed_item_metadata)),
        false,
    );
    assert_eq!(lost_unknown, source);
    assert!(matches!(
        instance(&lost_unknown).evaluate(
            Selection::all(3),
            &[EvaluatedArgument::Column(&source_array)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));

    let mut changed_fields = fields.to_vec();
    let mut metadata = changed_fields[0].metadata().clone();
    metadata.remove(novarocks_type_contract::NR_LOGICAL_TYPE_KEY);
    changed_fields[0] = Arc::new(changed_fields[0].as_ref().clone().with_metadata(metadata));
    let changed_item = item
        .as_ref()
        .clone()
        .with_data_type(DataType::Struct(changed_fields.into()));
    let lost_json = FunctionValueType::from_legacy_declared_projection(
        DataType::List(Arc::new(changed_item)),
        false,
    );
    assert!(!lost_json.same_semantics(&source));
    assert!(matches!(
        instance(&lost_json).evaluate(
            Selection::all(3),
            &[EvaluatedArgument::Column(&source_array)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let wrong_sorted = map(false, false);
    let sorted_source = ty(&map(false, true), false);
    assert!(matches!(
        instance(&sorted_source).evaluate(
            Selection::all(4),
            &[EvaluatedArgument::Column(&wrong_sorted)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn cardinality_large_list_is_a_binding_coercion_not_a_raw_prepared_profile() {
    let array = list(false);
    let item = match array.data_type() {
        DataType::List(field) => field.clone(),
        _ => unreachable!(),
    };
    let raw = FunctionValueType::from_legacy_declared_projection(DataType::LargeList(item), false);
    let owner = owner_for_test("cardinality");
    let arguments = [FunctionArgument::Value {
        value_type: raw.clone(),
        constant: None,
    }];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let selected = owner
        .resolve(request, crate::binding_test_control())
        .unwrap();
    assert!(
        matches!(&selected.argument_types[0], FunctionArgumentType::Value(actual) if matches!(actual.data_type(), DataType::List(_)))
    );
    assert!(
        prepared_for_test_with_policy("cardinality", &[raw], DecimalOverflowPolicy::OutputNull)
            .is_err()
    );
    for raw in [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Null, true),
    ] {
        assert!(
            prepared_for_test_with_policy("cardinality", &[raw], DecimalOverflowPolicy::OutputNull)
                .is_err()
        );
    }
    for arity in [0, 2] {
        assert!(
            prepared_for_test_with_policy(
                "cardinality",
                &vec![ty(&array, false); arity],
                DecimalOverflowPolicy::OutputNull
            )
            .is_err()
        );
    }
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let large: ArrayRef = Arc::new(
        LargeListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 3])),
            values,
            None,
        )
        .unwrap(),
    );
    assert!(matches!(
        instance(&ty(&array, true)).evaluate(
            Selection::all(1),
            &[EvaluatedArgument::Column(&large)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn cardinality_required_child_errors_shapes_and_nonnull_promise_are_not_erased() {
    let array = list(true);
    let source = ty(&array, true);
    let null = array.slice(2, 1);
    let selected = SelectedValues::try_new(
        Selection::all(1),
        array.data_type(),
        null.clone(),
        Box::from([crate::RowDataError::new(
            0,
            "required collection child failed",
        )]),
    )
    .unwrap();
    let short = array.slice(0, 0);
    for bad in [
        EvaluatedArgument::SelectedColumn(&selected),
        EvaluatedArgument::Column(&short),
    ] {
        let mut kernel = instance(&source);
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &[bad], &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
        let after = Control::default();
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(1),
                    &[EvaluatedArgument::Column(&null)],
                    &after
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
    assert!(matches!(
        instance(&ty(&array, false)).evaluate(
            Selection::all(1),
            &[EvaluatedArgument::Column(&null)],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let empty = Selection::try_sparse(4, &[]).unwrap();
    assert!(
        output(
            &instance(&source)
                .evaluate(
                    empty,
                    &[EvaluatedArgument::Column(&array)],
                    &Control::default()
                )
                .unwrap()
        )
        .is_empty()
    );
}

#[derive(Debug)]
struct ForeignCollection(ArrayRef);
// SAFETY: Every Arrow layout/buffer method delegates to the real immutable
// collection Array. Only Any identity differs to exercise the concrete-class gate.
unsafe impl Array for ForeignCollection {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_data(&self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn into_data(self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        self.0.slice(offset, length)
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn offset(&self) -> usize {
        self.0.offset()
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        self.0.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.0.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}

#[test]
fn cardinality_every_compile_callback_keeps_three_causes_and_ordinary_tail() {
    let array = list(true);
    for sources in [
        vec![ty(&array, true)],
        vec![],
        vec![FunctionValueType::new(DataType::Int32, true)],
    ] {
        let good = CompileControl::default();
        let success = sources.len() == 1 && matches!(sources[0].data_type(), DataType::List(_));
        assert_eq!(
            prepared_for_test_with_control(
                "cardinality",
                &sources,
                DecimalOverflowPolicy::OutputNull,
                &good
            )
            .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let error = prepared_for_test_with_control(
                    "cardinality",
                    &sources,
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .unwrap();
                let actual = match error {
                    FunctionSpecializationFailure::Control(e) => Some(e),
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        Some(CompileControlError::DeadlineExceeded)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        Some(CompileControlError::ResourceExhausted)
                    }
                    _ => None,
                };
                assert_eq!(actual, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn cardinality_every_small_runtime_callback_keeps_seven_causes_and_failed_latch() {
    for array in [list(true), map(true, false)] {
        let good_row = array.slice(0, 1);
        let null = array.slice(2, 1);
        let wrong: ArrayRef = Arc::new(ForeignCollection(null.clone()));
        let source = ty(&array, true);
        for (input, success) in [(&good_row, true), (&null, true), (&wrong, false)] {
            let args = [EvaluatedArgument::Column(input)];
            let good = Control::default();
            let result = instance(&source).evaluate(Selection::all(1), &args, &good);
            assert_eq!(result.is_ok(), success);
            if !success {
                assert!(matches!(result, Err(KernelFailure::Internal(_))));
            }
            let trace = good.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            for at in 0..trace.len() {
                for cause in causes() {
                    let control = Control {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    let mut kernel = instance(&source);
                    assert_eq!(
                        kernel
                            .evaluate(Selection::all(1), &args, &control)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                    let after = Control::default();
                    assert_eq!(
                        kernel
                            .evaluate(Selection::all(1), &args, &after)
                            .unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.trace.lock().unwrap().is_empty());
                }
            }
        }
    }
}

#[test]
fn cardinality_actual_offset_and_null_loops_reach_quantum_with_sampled_first_causes() {
    for root_null in [false, true] {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![None; 480]));
        let offsets = (0..=320)
            .map(|i| i / 2 * 3 + if i % 2 == 0 { 0 } else { 1 })
            .collect::<Vec<_>>();
        let array: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("all-null-child", DataType::Int32, true)),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                root_null.then(|| NullBuffer::from(vec![false; 320])),
            )
            .unwrap(),
        );
        let source = ty(&array, root_null);
        let array = crate::test_value_ingress::array_for_binding(&array, &source).unwrap();
        let args = [EvaluatedArgument::Column(&array)];
        let good = Control::default();
        let result = instance(&source)
            .evaluate(Selection::all(320), &args, &good)
            .unwrap();
        assert_eq!(
            output(&result),
            (0..320)
                .map(|i| if root_null {
                    None
                } else {
                    Some(if i % 2 == 0 { 1 } else { 2 })
                })
                .collect::<Vec<_>>()
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        // Sample actual entry, completed-work quanta and tail. No child values
        // are decoded; this does not claim cooperation inside opaque Arrow.
        for (at, units) in trace.iter().enumerate() {
            if at != 0 && at + 1 != trace.len() && *units != 256 {
                continue;
            }
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance(&source);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(320), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(320), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn cardinality_output_layout_rejects_unrepresentable_i32_and_validity_before_allocation() {
    use super::super::collection_offset_count::output_capacity;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert!(output_capacity(0).is_ok());
    assert!(output_capacity(320).is_ok());
    for rows in [
        usize::MAX,
        isize::MAX as usize / std::mem::size_of::<i32>() + 1,
    ] {
        assert_eq!(
            output_capacity(rows).unwrap_err().failure_fact().class(),
            FailureClass::Representation
        );
    }
}
