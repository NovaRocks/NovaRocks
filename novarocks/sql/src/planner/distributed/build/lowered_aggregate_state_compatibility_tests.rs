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

use super::lowered_partial_aggregate_canonical_tests::{chain, entry};
use super::*;
use crate::planner::distributed::build::physical_aggregate_requests::author_physical_aggregate_merge_request_observed;

fn repeated_chain(name: &str, ty: ValueType) -> PhysicalPlanNode {
    let mut input = column(
        7,
        "original_nonnullable",
        ty.data_type().clone(),
        ty.nullable(),
    );
    input.value_type = ty;
    // The source channel exists even for nested carriers without a fabricated
    // runtime literal; these tests verify the actual structural source owner.
    let child = values(vec![input.clone()], vec![]);
    chain(name, vec![reference(&input)], vec![], repeat(child, &input))
}

fn assert_actual_root_widening(owner: &SqlAuthoredPhysicalPlan, original: &ValueType) {
    let partial = entry(owner, true);
    let final_entry = entry(owner, false);
    let canonical = partial.canonical().unwrap();
    let contract =
        novarocks_type_contract::AggregateStateArgumentContract::ValueRootNullabilityIndependent;
    assert_eq!(partial.source().binding.state_argument_contract, contract);
    assert_eq!(
        final_entry.source().binding.state_argument_contract,
        contract
    );
    assert_eq!(
        canonical
            .selected()
            .aggregate
            .as_ref()
            .unwrap()
            .state_argument_contract,
        contract
    );
    assert_eq!(
        final_entry
            .captured()
            .binding()
            .selected
            .aggregate
            .as_ref()
            .unwrap()
            .state_argument_contract,
        contract
    );
    let (captured, cv) = value(&partial.captured().request().arguments[0]);
    assert_eq!(captured, original);
    assert!(cv.is_none());
    let mut actual = original.clone();
    actual = actual.with_nullable(true).unwrap();
    assert_eq!(value(&canonical.request().arguments[0]).0, &actual);
    assert!(value(&canonical.request().arguments[0]).1.is_none());
    assert_eq!(
        value(&final_entry.captured().request().arguments[0]).0,
        original
    );
    let final_canonical = final_entry.canonical().unwrap();
    assert!(final_canonical.belongs_to(final_entry.captured()));
    assert!(!Arc::ptr_eq(final_canonical, canonical));
    assert_eq!(
        final_canonical.selected().argument_types.as_ref(),
        final_entry
            .source()
            .binding
            .function
            .argument_types
            .as_ref()
    );
    assert_eq!(value(&final_canonical.request().arguments[0]).0, &actual);
    assert!(value(&final_canonical.request().arguments[0]).1.is_none());
    assert_eq!(
        canonical.selected().argument_types.as_ref(),
        partial.source().binding.function.argument_types.as_ref()
    );
    assert_eq!(
        final_entry
            .captured()
            .binding()
            .selected
            .argument_types
            .as_ref(),
        &[novarocks_functions::FunctionArgumentType::Value(
            original.clone()
        )]
    );
    assert!(canonical.belongs_to(partial.captured()));
    assert!(
        partial
            .captured()
            .logical_identity()
            .same_revision(final_entry.captured().logical_identity())
    );
    let AggregatePhase::Partial { sequence } = partial.phase() else {
        unreachable!()
    };
    assert_eq!(final_entry.phase(), AggregatePhase::Final { sequence });
    assert_eq!(
        partial.source().binding.intermediate_type,
        final_entry.source().binding.intermediate_type
    );
    assert_eq!(
        partial.source().binding.function.result_type,
        final_entry.source().binding.function.result_type
    );
    assert_eq!(
        partial.source().binding.state_format,
        final_entry.source().binding.state_format
    );
    assert_eq!(partial.source().binding.logical_argument_count, 1);
    assert_eq!(final_entry.source().binding.logical_argument_count, 1);
    let expression = partial
        .fragment()
        .expressions()
        .get(partial.source().arguments[0])
        .unwrap();
    assert_eq!(expression.ty, actual);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let merge = author_physical_aggregate_merge_request_observed(&final_entry, &mut work).unwrap();
    assert!(Arc::ptr_eq(merge.selected(), final_canonical.selected()));
    assert!(std::ptr::eq(
        merge.request().arguments,
        final_canonical.request().arguments
    ));
    assert!(std::ptr::eq(merge.source(), final_entry.source()));
    assert_eq!(value(&merge.request().arguments[0]).0, &actual);
    assert!(value(&merge.request().arguments[0]).1.is_none());
    assert_eq!(merge.request().logical_argument_count, 1);
    let mut producers = 0;
    let mut streams = 0;
    final_entry
        .state_inputs_observed(&mut work)
        .unwrap()
        .visit_observed(
            &mut work,
            |producer, endpoint, work| {
                producers += 1;
                assert_eq!(producer.site(), partial.site());
                assert_eq!(endpoint.fragment, partial.fragment().id());
                assert_eq!(endpoint.node, partial.node().id);
                assert_eq!(endpoint.value, partial.source().output);
                assert!(std::ptr::eq(producer.captured(), partial.captured()));
                assert!(Arc::ptr_eq(
                    producer.canonical().unwrap().selected(),
                    canonical.selected()
                ));
                assert_eq!(
                    value(&producer.canonical().unwrap().request().arguments[0]).0,
                    &actual
                );
                assert_eq!(
                    value(&producer.captured().request().arguments[0]).0,
                    original
                );
                work.step()?;
                Ok(())
            },
            |_, kind, links, work| {
                assert!(matches!(
                    kind,
                    crate::planner::distributed::build::lowered_draft::AggregateStateTransport::Stream(_)
                ));
                assert_eq!(links.len(), 1);
                streams += 1;
                work.step()?;
                Ok(())
            },
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!((producers, streams), (1, 1));
}

#[test]
fn state_compat_count_and_min_repeat_keep_actual_partial_nullable_and_final_original_request() {
    for name in ["count", "min"] {
        let original = ValueType::new(DataType::Int64, false);
        let source = repeated_chain(name, original.clone());
        let owner = authored(&source, &Control::default()).unwrap();
        assert_actual_root_widening(&owner, &original);
        let partial = entry(&owner, true);
        let final_entry = entry(&owner, false);
        let expected = ValueType::new(DataType::Int64, name == "min");
        assert_eq!(partial.source().binding.intermediate_type, expected);
        assert_eq!(final_entry.source().binding.function.result_type, expected);
    }
}

fn nested_type(child_nullable: bool, origin: &str) -> ValueType {
    ValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new(
                    if origin == "foreign" {
                        "different_child"
                    } else {
                        "中国"
                    },
                    DataType::Utf8,
                    child_nullable,
                )
                .with_metadata(std::collections::HashMap::<String, String>::from(
                    [("provider-origin".into(), origin.into())],
                )),
            )]
            .into(),
        ),
        false,
    )
}

#[test]
fn state_compat_min_preserves_nested_metadata_and_nominal_json_beyond_root_nullable() {
    let json = ValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    for original in [nested_type(true, "original"), json] {
        let source = repeated_chain("min", original.clone());
        let owner = authored(&source, &Control::default()).unwrap();
        assert_actual_root_widening(&owner, &original);
        let final_entry = entry(&owner, false);
        let mut state = original.clone();
        state = state.with_nullable(true).unwrap();
        assert_eq!(final_entry.source().binding.intermediate_type, state);
        assert_eq!(final_entry.source().binding.function.result_type, state);
    }
}

fn replace_final_logical_source(plan: &mut PhysicalPlanNode, ty: ValueType) {
    let mut input = column(
        7,
        "independent_final_logical_source",
        ty.data_type().clone(),
        ty.nullable(),
    );
    input.value_type = ty;
    let replacement = aggregate(
        "min",
        vec![reference(&input)],
        vec![],
        values(vec![input], vec![]),
    );
    let PhysicalPlanKind::HashAggregate(replacement) = replacement.kind else {
        unreachable!()
    };
    let PhysicalPlanKind::HashAggregate(spec) = &mut plan.kind else {
        unreachable!()
    };
    // This is a separately resolved genuine static source, deliberately
    // inconsistent with the retained Partial. It cannot lend that producer.
    spec.aggregates[0].source = replacement.aggregates[0].source.clone();
}

#[test]
fn state_compat_nested_drift_and_nominal_drift_still_refuse_original_state_domain() {
    for final_type in [nested_type(false, "original"), nested_type(true, "foreign")] {
        let mut source = repeated_chain("min", nested_type(true, "original"));
        replace_final_logical_source(&mut source, final_type);
        assert!(matches!(
            authored(&source, &Control::default()),
            Err(ContractLoweringError::InvalidAggregate {
                detail: "state-consuming aggregate reads a state of another type"
            })
        ));
    }
    let mut source = repeated_chain("min", ValueType::new(DataType::Utf8, false));
    replace_final_logical_source(
        &mut source,
        ValueType::try_with_logical_type(
            DataType::Utf8,
            false,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap(),
    );
    let Err(ContractLoweringError::Validation(errors)) = authored(&source, &Control::default())
    else {
        panic!("same Utf8 storage cannot lend a different nominal state")
    };
    assert!(
        errors.errors().iter().any(|error| error.message()
            == "aggregate state input differs from its bound intermediate type")
    );
}

fn mutate_final_binding(
    plan: &mut PhysicalPlanNode,
    change: impl FnOnce(&mut novarocks_functions::ResolvedFunctionBinding),
) {
    let PhysicalPlanKind::HashAggregate(spec) = &mut plan.kind else {
        unreachable!()
    };
    let original = &spec.aggregates[0].source;
    let mut resolved = original.binding().resolved().clone();
    change(&mut resolved);
    let binding = SqlFunctionBinding::new(resolved, original.binding().decimal_overflow_policy());
    // Deliberate malformed metadata from a clone of the actual installed
    // binding. This does not create a new implementation or bypass its gate.
    spec.aggregates[0].source = AggregateArgumentSource::logical_update(
        original.arguments().to_vec(),
        original.order_by().to_vec(),
        binding,
    );
}
fn require_sequence_refusal(source: &PhysicalPlanNode) {
    assert!(
        matches!(
            authored(source, &Control::default()),
            Err(ContractLoweringError::InvalidAggregate {
                detail: "aggregate original binding differs from its installed logical request"
            })
        ),
        "actual mismatched state/result metadata must refuse before reselection"
    );
}

#[test]
fn state_compat_stateformat_result_logical_count_phase_and_orphan_remain_exact() {
    let good = || chain("count", vec![], vec![], values(vec![], vec![vec![]]));
    let mut format = good();
    mutate_final_binding(&mut format, |binding| {
        binding.selected.aggregate.as_mut().unwrap().state_format =
            novarocks_type_contract::AggregateStateFormatId::try_new("test.count/foreign-state-v1")
                .unwrap()
    });
    require_sequence_refusal(&format);

    let mut contract = good();
    mutate_final_binding(&mut contract, |binding| {
        binding
            .selected
            .aggregate
            .as_mut()
            .unwrap()
            .state_argument_contract =
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature;
    });
    require_sequence_refusal(&contract);

    let mut result = good();
    mutate_final_binding(&mut result, |binding| {
        binding.selected.result_type =
            FunctionResultType::Scalar(ValueType::new(DataType::Utf8, true))
    });
    let PhysicalPlanKind::HashAggregate(spec) = &mut result.kind else {
        unreachable!()
    };
    let output = column(92, "final_result", DataType::Utf8, true);
    spec.aggregates[0].result_type =
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true);
    spec.output_layout = AggregateOutputLayout::new(vec![], vec![output.clone()]);
    spec.output_columns = vec![output.clone()];
    result.output_columns = vec![output];
    require_sequence_refusal(&result);

    let mut logical_count = good();
    mutate_final_binding(&mut logical_count, |binding| {
        binding.logical_argument_count = 1
    });
    assert!(matches!(
        authored(&logical_count, &Control::default()),
        Err(ContractLoweringError::InvalidAggregate {
            detail: "binding logical/ORDER BY arity differs from the call"
        })
    ));

    let mut phase = good();
    let local = &mut phase.children[0].children[0];
    let PhysicalPlanKind::HashAggregate(spec) = &mut local.kind else {
        unreachable!()
    };
    spec.is_merge[0] = true;
    assert!(matches!(
        authored(&phase, &Control::default()),
        Err(ContractLoweringError::InvalidAggregate {
            detail: "local aggregate consumes an intermediate state"
        })
    ));

    let mut extra = good();
    let local = &mut extra.children[0].children[0];
    let PhysicalPlanKind::HashAggregate(spec) = &mut local.kind else {
        unreachable!()
    };
    spec.aggregates.push(spec.aggregates[0].clone());
    spec.is_merge.push(false);
    let mut output = spec.output_columns[0].clone();
    output.column_id = ColumnId(93);
    spec.aggregates[1].output_column_id = output.column_id;
    spec.output_layout.aggregate_columns.push(output.clone());
    spec.output_columns.push(output.clone());
    local.output_columns.push(output.clone());
    // Carry both actual state outputs through the genuine Gather so the
    // oracle reaches the Local/Global sequence arity gate, not an unrelated
    // stale exchange layout rejection.
    let gather = &mut extra.children[0];
    let PhysicalPlanKind::Redistribute(spec) = &mut gather.kind else {
        unreachable!()
    };
    spec.output_columns.push(output.clone());
    gather.output_columns.push(output);
    let extra_result = authored(&extra, &Control::default());
    assert!(
        matches!(
            extra_result,
            Err(ContractLoweringError::InvalidAggregate {
                detail: "Local and Global aggregate call arities differ"
            })
        ),
        "actual extra contribution refusal: {extra_result:?}"
    );

    let orphan = good().children[0].children[0].clone();
    assert!(matches!(
        authored(&orphan, &Control::default()),
        Err(ContractLoweringError::MissingPlannerFact {
            node: "HashAggregate",
            fact: "the exact downstream final sequence for a Local producer"
        })
    ));
}

#[test]
fn state_compat_actual_repeat_lowerer_and_stateformat_refusal_keep_every_original_control_prefix() {
    let good = repeated_chain("count", ValueType::new(DataType::Int64, false));
    let mut bad = good.clone();
    mutate_final_binding(&mut bad, |binding| {
        binding.selected.aggregate.as_mut().unwrap().state_format =
            novarocks_type_contract::AggregateStateFormatId::try_new("test.count/foreign-state-v1")
                .unwrap()
    });
    for (positive, source) in [(true, &good), (false, &bad)] {
        let baseline = Control::default();
        let result = authored(source, &baseline);
        if positive {
            assert_actual_root_widening(&result.unwrap(), &ValueType::new(DataType::Int64, false));
        } else {
            assert!(
                matches!(
                    result,
                    Err(ContractLoweringError::InvalidAggregate {
                        detail: "aggregate original binding differs from its installed logical request"
                    })
                ),
                "exact original state format refuses before current-signature publication"
            );
        }
        let expected = baseline.trace.into_inner().unwrap();
        assert!(!expected.is_empty());
        for at in 0..expected.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Default::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(authored(source, &control), Err(ContractLoweringError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
            }
        }
    }
}
