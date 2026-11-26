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

use super::values_lowering_tests::{build, checked_pool, compile_options, providers};
use super::*;
use crate::FragmentCompileError;
use crate::{compile_fragment, topn::lower_topn};
use arrow_array::{ArrayRef, Int64Array, StringArray, StructArray};
use novarocks_local_program::{LocalSortMode, ProgramNodeId, ProgramNodeKind};
use novarocks_physical_plan::NodeKind;
use novarocks_physical_plan::{
    NullOrdering, SortDirection, SortExpr, SortMode, TopNPhase, TopNSequenceId,
};
use novarocks_type_contract::ValueLogicalType;

fn compile(
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    compile_fragment(providers(package), &functions(), compile_options(), control)
}
fn builder_from(base: &FragmentPackage) -> FragmentBuilder {
    let mut builder = FragmentBuilder::new(base.fragment().id());
    for value in base.fragment().values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for node in base.fragment().nodes().values() {
        builder.insert_node_unchecked(node.clone()).unwrap();
    }
    builder
}
fn complete(
    mut input: FragmentPackageInput,
    builder: FragmentBuilder,
    root: NodeId,
) -> Result<Arc<FragmentPackage>, Box<dyn std::error::Error>> {
    let fragment = builder.finish_fixture(
        root,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;
    let output = fragment.nodes()[&root].output.clone();
    let fields = output
        .columns
        .iter()
        .enumerate()
        .map(|(index, &value)| ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: format!("result-{index}").into(),
            alias: Some(format!("original-{index}").into()),
            value,
            ty: fragment.values()[&value].ty.clone(),
        })
        .collect();
    input.result = Some(ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output,
        fields,
    });
    input.fragment = fragment;
    Ok(Arc::new(FragmentPackage::try_new(
        input,
        package_admission(),
        &Control::good(),
    )?))
}
fn package(
    source: &ConstantPool,
    keys: usize,
    tail: bool,
    limit: u64,
    offset: u64,
) -> Arc<FragmentPackage> {
    phased(source, keys, tail, limit, offset, TopNPhase::Single)
}
/// The same ordinary TopN package with its frozen phase replaced. A partial or
/// final phase without its pair is a checked single-fragment package; only
/// whole-plan validation pairs a sequence.
fn phased(
    source: &ConstantPool,
    keys: usize,
    tail: bool,
    limit: u64,
    offset: u64,
    phase: TopNPhase,
) -> Arc<FragmentPackage> {
    let base = super::sort_lowering_tests::package(source, keys, tail, SortMode::Global, true);
    let mut input = (*base).clone().into_input();
    let root = input.fragment.root();
    let mut builder = FragmentBuilder::new(input.fragment.id());
    for value in input.fragment.values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for node in input.fragment.nodes().values() {
        let mut node = node.clone();
        if node.id == NodeId::new(0) {
            let NodeKind::TopN {
                limit: old_limit,
                offset: old_offset,
                phase: old_phase,
                ..
            } = &mut node.kind
            else {
                panic!("actual ordinary TopN")
            };
            *old_limit = limit;
            *old_offset = offset;
            *old_phase = phase;
        }
        builder.insert_node_unchecked(node).unwrap();
    }
    // All roots are re-authored against this actual fragment, never borrowed from a changed body.
    input.result = None;
    complete(input, builder, root).unwrap()
}
fn component(
    package: &FragmentPackage,
    control: &dyn PureCompileControl,
    missing: bool,
) -> Result<(ProgramNodeKind, novarocks_local_program::StaticLayout), FragmentCompileError> {
    let program = compile(Arc::new(package.clone()), &Control::good()).unwrap();
    let layout = program.graph().nodes()[0].output_layout();
    let physical = &package.fragment().nodes()[&NodeId::new(0)];
    let NodeKind::TopN { order_by, .. } = &physical.kind else {
        panic!("TopN")
    };
    let ProgramNodeKind::Sort {
        order_by: local, ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("compiled TopN")
    };
    let mut ids = order_by
        .iter()
        .zip(local)
        .map(|(source, local)| (source.expr, local.expr))
        .collect::<BTreeMap<_, _>>();
    if missing {
        ids.clear();
    }
    lower_topn(
        &package.fragment().nodes()[&NodeId::new(0)],
        ProgramNodeId::new(0),
        layout,
        &ids,
        control,
    )
}
fn is_control(error: &FragmentCompileError, cause: CompileControlError) -> bool {
    matches!(error,FragmentCompileError::Control(actual) if *actual==cause)
}

#[test]
fn single_topn_public_compile_preserves_ordered_keys_root_occurrences_and_exact_source_layout() {
    let package = package(&integer_pool(), 3, false, 2, 1);
    let program = compile(package.clone(), &Control::good()).unwrap();
    let ProgramNodeKind::Sort {
        input,
        order_by,
        mode: LocalSortMode::RowsTopN { limit, offset },
    } = program.graph().nodes()[1].kind()
    else {
        panic!("actual local TopN sort")
    };
    assert_eq!(input.index(), 0);
    assert_eq!(*limit, 2);
    assert_eq!(*offset, 1);
    assert_eq!(
        order_by
            .iter()
            .map(|k| (k.asc, k.nulls_first))
            .collect::<Vec<_>>(),
        [(false, true), (true, false), (false, true)]
    );
    assert!(
        order_by
            .iter()
            .all(|k| root_definition(&program, k.expr)
                == root_definition(&program, order_by[0].expr))
    );
    assert_eq!(
        program.graph().nodes()[0].output_layout().slots(),
        program.graph().nodes()[1].output_layout().slots()
    );
    let roles = order_by
        .iter()
        .map(|key| key.expr.root.get())
        .collect::<Vec<_>>();
    assert_eq!(roles, [0, 1, 2]);
    let physical = package.fragment().nodes()[&NodeId::new(0)]
        .expressions
        .as_ref()
        .unwrap()
        .roots()
        .len();
    assert_eq!(physical, 3);
    let ProgramNodeKind::Values { values } = program.graph().nodes()[0].kind() else {
        panic!("actual values")
    };
    let column = values
        .batch()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(column.values().as_ref(), &[-7, 42]);
    assert_eq!(
        program.graph().nodes()[0].physical_sources()[0].get(),
        u32::MAX
    );
}

#[test]
fn single_topn_zero_and_host_representable_max_rows_preserve_values_without_buffer_defaults() {
    for (limit, offset) in [(0, 0), (0, 1), (2, 1), (u64::MAX, 0)] {
        let source = integer_pool();
        let package = package(&source, 1, false, limit, offset);
        let result = compile(package, &Control::good());
        if usize::try_from(limit).is_err() {
            assert!(matches!(
                result,
                Err(FragmentCompileError::Invalid(
                    "TopN limit exceeds host range"
                ))
            ));
            continue;
        }
        let program = result.unwrap();
        let ProgramNodeKind::Sort {
            mode:
                LocalSortMode::RowsTopN {
                    limit: actual,
                    offset: actual_offset,
                },
            ..
        } = program.graph().nodes()[1].kind()
        else {
            panic!("TopN")
        };
        assert_eq!(*actual, usize::try_from(limit).unwrap());
        assert_eq!(*actual_offset, usize::try_from(offset).unwrap());
    }
    // A legal host conversion never replaces the original u64 sum validator.
    let base =
        super::sort_lowering_tests::package(&integer_pool(), 1, false, SortMode::Global, true);
    let mut input = (*base).clone().into_input();
    let root = input.fragment.root();
    let mut builder = FragmentBuilder::new(input.fragment.id());
    for value in input.fragment.values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for node in input.fragment.nodes().values() {
        let mut node = node.clone();
        if let NodeKind::TopN { limit, offset, .. } = &mut node.kind {
            *limit = u64::MAX;
            *offset = 1;
        }
        builder.insert_node_unchecked(node).unwrap();
    }
    input.result = None;
    let error = complete(input, builder, root).unwrap_err();
    assert!(error.to_string().contains("TopN limit and offset overflow"));
}

#[test]
fn topn_transparent_tail_preserves_original_ordering_and_passthrough_result_channels() {
    let package = package(&integer_pool(), 2, true, 2, 1);
    let program = compile(package, &Control::good()).unwrap();
    assert_eq!(program.graph().nodes().len(), 5);
    let source = program.graph().nodes()[1].output_layout().slots();
    assert_eq!(source, program.graph().nodes()[2].output_layout().slots());
    let ProgramNodeKind::Limit { limit, offset, .. } = program.graph().nodes()[4].kind() else {
        panic!("transparent limit")
    };
    assert_eq!(*limit, Some(1));
    assert_eq!(*offset, 0);
    assert!(matches!(
        program.graph().nodes()[3].kind(),
        ProgramNodeKind::Project { .. }
    ));
}

#[test]
fn topn_duplicate_passthrough_occurrences_keep_independent_slots_and_nested_field_metadata() {
    let scalar = integer_pool();
    let field = Arc::new(
        Field::new("nested-original", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider.annotation".into(),
                "source-text".into(),
            )]),
        ),
    );
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(StringArray::from(vec![
            Some("ignored"),
            None,
            Some("selected"),
        ]))],
        None,
    ));
    let nested = checked_pool(nested, false, ValueLogicalType::Physical);
    let base = build(
        &[scalar, nested],
        &[vec![(0, 2), (1, 2)], vec![(0, 1), (1, 1)]],
        &[0, 1],
        false,
    );
    let mut builder = builder_from(&base);
    let input = NodeId::new(u32::MAX);
    let columns = &base.fragment().nodes()[&input].output.columns;
    let project = NodeId::new(7);
    let expression = builder
        .add_root(
            project,
            base.fragment().values()[&columns[0]].ty.clone(),
            ExprKind::Value(columns[0]),
            RootScope::Port(0),
            EvaluationDemand::Value,
        )
        .unwrap();
    let nested_expr = builder
        .add_root(
            project,
            base.fragment().values()[&columns[1]].ty.clone(),
            ExprKind::Value(columns[1]),
            RootScope::Port(0),
            EvaluationDemand::Value,
        )
        .unwrap();
    let repeated = builder.repeat_root(project, expression);
    builder
        .add_project(
            project,
            input,
            Box::from([
                (expression, columns[0]),
                (repeated, columns[0]),
                (nested_expr, columns[1]),
            ]),
            Box::from([columns[0], columns[0], columns[1]]),
        )
        .unwrap();
    let topn = NodeId::new(0);
    let key = builder
        .add_root(
            topn,
            base.fragment().values()[&columns[0]].ty.clone(),
            ExprKind::Value(columns[0]),
            RootScope::Port(0),
            EvaluationDemand::Value,
        )
        .unwrap();
    builder
        .add_top_n(
            topn,
            project,
            Box::from([SortExpr {
                expr: key,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            2,
            0,
            TopNPhase::Single,
        )
        .unwrap();
    let package = complete((*base).clone().into_input(), builder, topn).unwrap();
    let program = compile(package.clone(), &Control::good()).unwrap();
    let child = program.graph().nodes()[1].output_layout();
    let top = program.graph().nodes()[2].output_layout();
    assert_eq!(child.slots(), top.slots());
    assert_eq!(top.slots().len(), 3);
    assert_ne!(top.slots()[0], top.slots()[1]);
    let channel = program.channels();
    // Complete nested types come from the existing channel owner, not a raw carrier retag.
    let output = channel
        .channel_type(novarocks_local_program::ProgramChannelSite::Layout {
            node: ProgramNodeId::new(2),
            role: novarocks_local_program::ProgramChannelLayoutRole::NodeOutput,
            ordinal: 2,
        })
        .unwrap();
    let observation = Control::good();
    assert!(
        output
            .exactly_equals_observed::<crate::expressions::ExpressionLoweringError>(
                &package.fragment().values()[&columns[1]].ty,
                || observation
                    .checkpoint(CompilePhase::Validate, 1)
                    .map_err(crate::expressions::ExpressionLoweringError::Control)
            )
            .unwrap()
    );
}

#[test]
fn actual_closed_partial_final_topn_package_lowers_both_phases_and_keeps_original_control_prefixes()
{
    let base =
        super::sort_lowering_tests::package(&integer_pool(), 1, false, SortMode::Global, true);
    let mut input = (*base).clone().into_input();
    let mut builder = FragmentBuilder::new(input.fragment.id());
    for value in input.fragment.values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for node in input.fragment.nodes().values() {
        let mut node = node.clone();
        if let NodeKind::TopN {
            phase,
            limit,
            offset,
            ..
        } = &mut node.kind
        {
            *phase = TopNPhase::Partial {
                sequence: TopNSequenceId::new(u32::MAX),
            };
            *limit = 3;
            *offset = 0;
        }
        builder.insert_node_unchecked(node).unwrap();
    }
    let final_node = NodeId::new(7);
    let column = input.fragment.nodes()[&NodeId::new(0)].output.columns[0];
    let expr = builder
        .add_root(
            final_node,
            input.fragment.values()[&column].ty.clone(),
            ExprKind::Value(column),
            RootScope::Port(0),
            EvaluationDemand::Value,
        )
        .unwrap();
    builder
        .add_top_n(
            final_node,
            NodeId::new(0),
            Box::from([SortExpr {
                expr,
                direction: SortDirection::Descending,
                null_ordering: NullOrdering::First,
            }]),
            2,
            1,
            TopNPhase::Final {
                sequence: TopNSequenceId::new(u32::MAX),
            },
        )
        .unwrap();
    input.result = None;
    let package = complete(input, builder, final_node).unwrap();
    let baseline = Control::good();
    let program = compile(package.clone(), &baseline).unwrap();
    // Partial keeps the top `final.limit + final.offset` rows without an
    // offset; Final applies the frozen window over the partial's rows.
    let windows = program
        .graph()
        .nodes()
        .iter()
        .filter_map(|node| match node.kind() {
            ProgramNodeKind::Sort {
                input,
                order_by,
                mode: LocalSortMode::RowsTopN { limit, offset },
            } => Some((
                input.index(),
                Some(*limit),
                *offset,
                order_by
                    .iter()
                    .map(|key| (key.asc, key.nulls_first))
                    .collect::<Vec<_>>(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        windows,
        [
            (0, Some(3), 0, vec![(false, true)]),
            (1, Some(2), 1, vec![(false, true)])
        ]
    );
    assert_eq!(program.graph().root().index(), 2);
    let orders = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .flat_map(|(node, definition)| match definition.kind() {
            ProgramNodeKind::Sort { order_by, .. } => order_by
                .iter()
                .enumerate()
                .map(|(ordinal, key)| {
                    assert!(
                        program
                            .unit(key.expr.unit)
                            .unwrap()
                            .root(key.expr.root)
                            .is_some()
                    );
                    (node, ordinal as u32)
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect::<Vec<_>>();
    assert_eq!(orders, [(1, 0), (2, 0)]);
    let trace = baseline.trace();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause.clone());
            assert!(is_control(
                &compile(package.clone(), &control).unwrap_err(),
                cause
            ));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
}

#[test]
fn topn_actual_public_and_shared_key_component_preserve_all_small_control_prefixes_and_ordinary_tail()
 {
    let package = package(&integer_pool(), 2, false, 2, 1);
    for missing in [false, true] {
        let baseline = Control::good();
        let result = component(&package, &baseline, missing);
        assert_eq!(result.is_ok(), !missing);
        let trace = baseline.trace();
        assert!(trace.last().is_some_and(|(_, units)| *units > 0));
        for at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control::refusing(at, cause.clone());
                assert!(is_control(
                    &component(&package, &control, missing).unwrap_err(),
                    cause
                ));
                assert_eq!(control.trace(), trace[..at]);
            }
        }
    }
    let baseline = Control::good();
    compile(package.clone(), &baseline).unwrap();
    let trace = baseline.trace();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause.clone());
            assert!(is_control(
                &compile(package.clone(), &control).unwrap_err(),
                cause
            ));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
}

#[test]
fn topn_320_order_occurrences_cross_real_quantum_and_sample_entry_boundary_tail_causes() {
    let package = package(&integer_pool(), 320, false, 3, 1);
    let baseline = Control::good();
    let program = compile(package.clone(), &baseline).unwrap();
    let ProgramNodeKind::Sort { order_by, .. } = program.graph().nodes()[1].kind() else {
        panic!("TopN")
    };
    assert_eq!(order_by.len(), 320);
    assert!(order_by[319].asc);
    assert!(!order_by[319].nulls_first);
    let trace = baseline.trace();
    let quantum = trace
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("real completed work quantum")
        + 1;
    for at in [1, quantum, trace.len()] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause.clone());
            assert!(is_control(
                &compile(package.clone(), &control).unwrap_err(),
                cause
            ));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
}

/// The lowered local TopN fields of program node 1, the TopN over Values.
fn lowered_window(
    program: &novarocks_local_program::LocalProgram,
) -> (Option<usize>, usize, Vec<(bool, bool)>) {
    let ProgramNodeKind::Sort {
        input,
        order_by,
        mode: LocalSortMode::RowsTopN { limit, offset },
    } = program.graph().nodes()[1].kind()
    else {
        panic!("actual local TopN sort")
    };
    assert_eq!(input.index(), 0);
    let keys = order_by.iter().map(|k| (k.asc, k.nulls_first)).collect();
    (Some(*limit), *offset, keys)
}

#[test]
fn partial_and_final_topn_lower_to_the_ordinary_window_with_no_phase_state() {
    let sequence = TopNSequenceId::new(7);
    // Partial: the frozen `final.limit + final.offset` row budget, offset 0.
    let partial = compile(
        phased(
            &integer_pool(),
            2,
            false,
            3,
            0,
            TopNPhase::Partial { sequence },
        ),
        &Control::good(),
    )
    .unwrap();
    let (limit, offset, keys) = lowered_window(&partial);
    assert_eq!((limit, offset), (Some(3), 0));
    assert_eq!(keys, [(false, true), (true, false)]);
    assert_eq!(
        partial.graph().nodes()[0].output_layout().slots(),
        partial.graph().nodes()[1].output_layout().slots()
    );
    // Final: the frozen window over its gathered Singleton input.
    let fin = compile(
        phased(
            &integer_pool(),
            2,
            false,
            2,
            1,
            TopNPhase::Final { sequence },
        ),
        &Control::good(),
    )
    .unwrap();
    let (limit, offset, keys) = lowered_window(&fin);
    assert_eq!((limit, offset), (Some(2), 1));
    assert_eq!(keys, [(false, true), (true, false)]);
    // Every phase lowers to exactly the Single owner fields of its window.
    for (limit, offset, phase) in [
        (3, 0, TopNPhase::Partial { sequence }),
        (2, 1, TopNPhase::Final { sequence }),
        (0, 0, TopNPhase::Partial { sequence }),
    ] {
        let single = compile(
            package(&integer_pool(), 2, false, limit, offset),
            &Control::good(),
        )
        .unwrap();
        let phased = compile(
            phased(&integer_pool(), 2, false, limit, offset, phase),
            &Control::good(),
        )
        .unwrap();
        let roots = |program: &novarocks_local_program::LocalProgram| {
            program
                .graph()
                .nodes()
                .iter()
                .enumerate()
                .flat_map(|(node, definition)| match definition.kind() {
                    ProgramNodeKind::Sort { order_by, .. } => order_by
                        .iter()
                        .map(|key| (node, key.expr))
                        .collect::<Vec<_>>(),
                    _ => Vec::new(),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(roots(&single), roots(&phased));
        let single = lowered_window(&single);
        let phased = lowered_window(&phased);
        assert_eq!(single, phased);
    }
}

#[test]
fn partial_and_final_topn_keep_one_sort_order_root_per_occurrence() {
    for phase in [
        TopNPhase::Partial {
            sequence: TopNSequenceId::new(1),
        },
        TopNPhase::Final {
            sequence: TopNSequenceId::new(1),
        },
    ] {
        let offset = u64::from(matches!(phase, TopNPhase::Final { .. }));
        let program = compile(
            phased(&integer_pool(), 3, false, 2, offset, phase),
            &Control::good(),
        )
        .unwrap();
        let ProgramNodeKind::Sort { order_by, .. } = program.graph().nodes()[1].kind() else {
            panic!("Sort")
        };
        let roles = order_by
            .iter()
            .map(|key| key.expr.root.get())
            .collect::<Vec<_>>();
        assert_eq!(roles, [0, 1, 2]);
        let uses = order_by
            .iter()
            .map(|key| root_use(&program, key.expr))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(uses.len(), 3);
    }
}

#[test]
fn topn_owner_refuses_a_partial_offset_and_a_grouped_state_reduction() {
    let package = package(&integer_pool(), 1, false, 2, 1);
    let program = compile(package.clone(), &Control::good()).unwrap();
    let layout = program.graph().nodes()[0].output_layout();
    let original = &package.fragment().nodes()[&NodeId::new(0)];
    let NodeKind::TopN { order_by, .. } = &original.kind else {
        panic!("actual ordinary TopN")
    };
    let ProgramNodeKind::Sort {
        order_by: local, ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("Sort")
    };
    let ids = order_by
        .iter()
        .zip(local)
        .map(|(source, local)| (source.expr, local.expr))
        .collect::<BTreeMap<_, _>>();
    let sequence = TopNSequenceId::new(3);
    // A partial offset would drop rows the final window needs.
    let mut node = original.clone();
    let NodeKind::TopN { phase, offset, .. } = &mut node.kind else {
        unreachable!()
    };
    *phase = TopNPhase::Partial { sequence };
    *offset = 1;
    assert!(matches!(
        lower_topn(&node, ProgramNodeId::new(0), layout, &ids, &Control::good()),
        Err(FragmentCompileError::Invalid(
            "partial TopN carries an offset before global completion"
        ))
    ));
    // GroupedStates has a separate lowering owner; it cannot enter row-count lowering.
    for phase in [TopNPhase::Partial { sequence }, TopNPhase::Single] {
        let mut node = original.clone();
        let NodeKind::TopN {
            phase: actual,
            offset,
            reduction,
            ..
        } = &mut node.kind
        else {
            unreachable!()
        };
        *actual = phase;
        *offset = 0;
        *reduction = novarocks_physical_plan::TopNReduction::GroupedStates {
            group_by: Box::default(),
            calls: Box::default(),
            comparator: novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
        };
        assert!(matches!(
            lower_topn(&node, ProgramNodeId::new(0), layout, &ids, &Control::good()),
            Err(FragmentCompileError::Invalid(
                "row-count TopN physical kind differs"
            ))
        ));
    }
}

/// `SELECT v0, v1 FROM lake ORDER BY v0 DESC NULLS FIRST LIMIT 2 OFFSET 1`
/// as the planner splits it: a runtime-split provider scan pruned by a
/// Partial TopN, gathered, and finished by its Final TopN. The whole plan is
/// validated, so the sequence pairing is a checked fact, and each fragment
/// compiles on its own.
mod scan_split {
    use super::super::UnitFixture;
    use crate::{
        FragmentCompileError, LocalCompileOptions, compile_fragment, validate_fragment_providers,
    };
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_connector_contract::*;
    use novarocks_expression_contract::{ExprKind, RootOrdinal, RootScope};
    use novarocks_local_program::{
        KernelAbiVersion, LocalProgram, LocalSortMode, ProgramNodeKind, StaticSinkProgram,
    };
    use novarocks_physical_plan::*;
    use novarocks_type_contract::{
        CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
    };
    use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

    const PRODUCER: FragmentId = FragmentId::new(1);
    const CONSUMER: FragmentId = FragmentId::new(2);
    const EDGE: EdgeId = EdgeId::new(5);
    const SCAN: NodeId = NodeId::new(10);
    const PARTIAL: NodeId = NodeId::new(11);
    const RECEIVER: NodeId = NodeId::new(20);
    const FINAL: NodeId = NodeId::new(21);
    const SEQUENCE: TopNSequenceId = TopNSequenceId::new(4);

    struct Good;
    impl PureCompileControl for Good {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }

    fn int64() -> ValueType {
        ValueType::new(DataType::Int64, false)
    }
    fn binding() -> ConnectorReadBinding {
        let instance = ConnectorInstanceId::parse("lake").unwrap();
        ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("alpha").unwrap(),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
        )
    }
    fn payload(
        binding: &ConnectorReadBinding,
        category: ConnectorCodecCategory,
        value: &'static [u8],
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(value),
        )
    }
    fn public_schema() -> Schema {
        Schema::new(vec![
            Field::new("v0", DataType::Int64, false),
            Field::new("v1", DataType::Int64, false),
        ])
    }

    /// Canonicalizes private bytes only; the public facts are borrowed.
    struct Port;
    impl ConnectorReadProgramCompiler for Port {
        type Error = ConnectorError;
        fn compile_private(
            &self,
            input: &FrozenConnectorRead,
            control: &dyn PureCompileControl,
        ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>>
        {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
            let original = input.scan().recipe();
            work.step()?;
            work.finish()?;
            ConnectorReadRelationRecipeDraft::try_new(
                original.binding().clone(),
                original.relation().clone(),
                original.columns().to_vec(),
            )
            .map_err(|error| {
                PureProviderCompileError::Provider(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    error.to_string(),
                ))
            })
        }
    }
    fn providers() -> PureProviderProgramCatalog<ConnectorError> {
        let provider = ConnectorProviderId::parse("alpha").unwrap();
        PureProviderProgramCatalog::try_new(
            &[PureProviderManifestEntry::new(
                provider.clone(),
                true,
                false,
            )],
            vec![PureProviderProgramDefinition::new(
                provider,
                Some(Arc::new(Port)
                    as Arc<
                        dyn ConnectorReadProgramCompiler<Error = ConnectorError>,
                    >),
                None,
            )],
            &Good,
        )
        .unwrap()
    }

    fn descending(expr: RootOrdinal) -> Box<[SortExpr]> {
        Box::from([SortExpr {
            expr,
            direction: SortDirection::Descending,
            null_ordering: NullOrdering::First,
        }])
    }

    fn packages(partial_limit: u64) -> Result<BTreeMap<FragmentId, FragmentPackage>, String> {
        let binding = binding();
        let relation = ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            payload(&binding, ConnectorCodecCategory::ReadTable, b"table"),
            payload(&binding, ConnectorCodecCategory::ReadView, b"view"),
        );
        let columns = [b"c0" as &'static [u8], b"c1"].map(|bytes| ProviderColumnReference {
            column_payload: payload(&binding, ConnectorCodecCategory::ReadColumn, bytes),
        });
        let mut producer = FragmentBuilder::new(PRODUCER);
        let provider = columns
            .iter()
            .map(|column| {
                producer
                    .add_value(
                        int64(),
                        ValueOrigin::ProviderField {
                            scan_node: SCAN,
                            field: column.clone(),
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let schema = columns
            .iter()
            .map(|column| RelationField {
                column: column.clone(),
                ty: int64(),
            })
            .collect::<Box<[_]>>();
        producer
            .add_scan(
                SCAN,
                NodeKind::Scan {
                    occurrence: ProviderReadOccurrenceId::new(0),
                    relation: Box::new(Relation::Data(DataRelation {
                        read: ProviderReadReference {
                            binding: binding.clone(),
                            input_version: ExactInputVersion::try_new([9]).unwrap(),
                            relation: relation.clone(),
                        },

                        selection_digest: [7; 32],
                        schema,
                        predicate_guarantees: Box::default(),
                        // Runtime splits land on any instance and driver.
                        provided_properties: PhysicalProperties {
                            distribution: Distribution::Unconstrained,
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ordering: Box::default(),
                        },
                    })),
                    provider_outputs: columns
                        .iter()
                        .cloned()
                        .zip(provider.iter().copied())
                        .collect(),
                    residuals: Box::default(),
                    derived_values: Box::default(),
                },
                provider.clone().into_boxed_slice(),
            )
            .unwrap();
        let key = producer
            .add_root(
                PARTIAL,
                int64(),
                ExprKind::Value(provider[0]),
                RootScope::Port(0),
                EvaluationDemand::Value,
            )
            .unwrap();
        producer
            .add_top_n(
                PARTIAL,
                SCAN,
                descending(key),
                partial_limit,
                0,
                TopNPhase::Partial { sequence: SEQUENCE },
            )
            .map_err(|error| error.to_string())?;
        let dop = PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        };
        let producer = producer
            .finish_fixture(
                PARTIAL,
                FragmentSink::Stream { edge: EDGE },
                dop,
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .map_err(|error| error.to_string())?;

        let mut consumer = FragmentBuilder::new(CONSUMER);
        let imports = provider
            .iter()
            .map(|&source| {
                let imported = consumer
                    .add_value(
                        int64(),
                        ValueOrigin::ExchangeImport {
                            edge: EDGE,
                            source_value: source,
                        },
                    )
                    .unwrap();
                (source, imported)
            })
            .collect::<Vec<_>>();
        let received = imports
            .iter()
            .map(|(_, imported)| *imported)
            .collect::<Box<[_]>>();
        consumer
            .add_exchange_source(
                RECEIVER,
                EDGE,
                imports.clone().into_boxed_slice(),
                received.clone(),
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            )
            .unwrap();
        let key = consumer
            .add_root(
                FINAL,
                int64(),
                ExprKind::Value(received[0]),
                RootScope::Port(0),
                EvaluationDemand::Value,
            )
            .unwrap();
        consumer
            .add_top_n(
                FINAL,
                RECEIVER,
                descending(key),
                2,
                1,
                TopNPhase::Final { sequence: SEQUENCE },
            )
            .map_err(|error| error.to_string())?;
        let consumer = consumer
            .finish_fixture(
                FINAL,
                FragmentSink::Result,
                dop,
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .map_err(|error| error.to_string())?;

        let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
        plan.add_fragment(producer).unwrap();
        plan.add_fragment(consumer).unwrap();
        plan.add_edge(Edge {
            role: novarocks_physical_plan::OutputRole::Relational,
            delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
            id: EDGE,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                port: novarocks_physical_plan::FragmentOutputPort::Root,
                fragment: PRODUCER,
                projection: provider.clone().into_boxed_slice(),
            },
            destination: EdgeDestination {
                fragment: CONSUMER,
                node: RECEIVER,
                receive_mapping: imports.into_boxed_slice(),
            },
            // Gather: the edge carries every partial row to one Final.
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
                routing_only: Box::default(),
            },
        })
        .unwrap();
        plan.set_result_port(ResultPort {
            scalar_schema: None,
            fragment: CONSUMER,
            output: OutputPort {
                node: FINAL,
                columns: received.clone(),
            },
            fields: ["v0", "v1"]
                .iter()
                .zip(received.iter())
                .map(|(name, value)| ResultField {
                    domain: novarocks_physical_plan::ResultValueDomain::Plain,
                    name: (*name).into(),
                    alias: None,
                    value: *value,
                    ty: int64(),
                })
                .collect(),
        })
        .unwrap();
        let plan = plan.finish().map_err(|error| format!("{error:?}"))?;

        let draft = ConnectorReadRelationRecipeDraft::try_new(
            binding,
            relation,
            columns
                .iter()
                .map(|column| column.column_payload.clone())
                .collect(),
        )
        .unwrap();
        let scan = FrozenConnectorScan::try_new(
            draft,
            ["v0", "v1"]
                .into_iter()
                .map(|name| StaticScanAssignment::new(Arc::from(name), ConnectorValueType::BigInt))
                .collect(),
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![],
        )
        .unwrap();
        let source = ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new([9]).unwrap(),
            [7; 32],
            ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![])
                .unwrap(),
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            vec![],
        )
        .unwrap();
        let public = crate::scan_lowering_tests::typed_public_schema(source, None, public_schema());
        let scans = BTreeMap::from([(
            ProviderReadOccurrenceId::new(0),
            FrozenConnectorRead::try_new(scan, public).unwrap(),
        )]);
        let mut pruning = BTreeMap::new();
        let mut admissions = BTreeMap::new();
        for &id in plan.fragments().keys() {
            pruning.insert(
                id,
                FrozenFragmentPruning::try_new(id, vec![], &Good).unwrap(),
            );
            admissions.insert(id, super::package_admission());
        }
        extract_fragment_packages(
            &plan,
            &scans,
            &BTreeMap::new(),
            &pruning,
            &admissions,
            &Good,
        )
        .map_err(|error| format!("{error:?}"))
    }

    fn compile(
        package: FragmentPackage,
        root_sink_dop: Option<usize>,
    ) -> Result<LocalProgram, FragmentCompileError> {
        let validated = validate_fragment_providers(Arc::new(package), &providers(), &Good)
            .expect("pure provider validation");
        compile_fragment(
            validated,
            &super::functions(),
            LocalCompileOptions {
                pipeline_dop: NonZeroUsize::new(1).unwrap(),
                root_sink_dop: root_sink_dop.map(|dop| NonZeroUsize::new(dop).unwrap()),
                kernel_abi: KernelAbiVersion::CURRENT,
                constants: super::policy(),
            },
            &Good,
        )
    }

    /// The single local TopN of a compiled split half: input, limit, offset.
    fn window(program: &LocalProgram) -> (usize, Option<usize>, usize, bool, bool) {
        let windows = program
            .graph()
            .nodes()
            .iter()
            .filter_map(|node| match node.kind() {
                ProgramNodeKind::Sort {
                    input,
                    order_by,
                    mode: LocalSortMode::RowsTopN { limit, offset },
                } if order_by.len() == 1 => Some((
                    input.index(),
                    Some(*limit),
                    *offset,
                    order_by[0].asc,
                    order_by[0].nulls_first,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(windows.len(), 1, "one local TopN per split half");
        windows[0]
    }

    #[test]
    fn scan_rooted_partial_topn_and_its_gathered_final_compile_as_the_checked_split() {
        let mut packages = packages(3).expect("the split sequence validates");
        let producer = compile(packages.remove(&PRODUCER).unwrap(), None)
            .expect("a runtime-split scan feeds its partial TopN");
        assert_eq!(producer.graph().nodes().len(), 2);
        assert!(matches!(
            producer.graph().nodes()[0].kind(),
            ProgramNodeKind::Scan { .. }
        ));
        // Partial: the top `limit + offset` rows of each instance, no offset.
        assert_eq!(window(&producer), (0, Some(3), 0, false, true));
        assert_eq!(producer.graph().root().index(), 1);
        assert!(matches!(
            producer.graph().sink(),
            Some(StaticSinkProgram::DataStream { .. })
        ));

        let consumer = compile(packages.remove(&CONSUMER).unwrap(), Some(1))
            .expect("the gathered final TopN compiles");
        assert!(matches!(
            consumer.graph().nodes()[0].kind(),
            ProgramNodeKind::ExchangeSource { .. }
        ));
        // Final: the frozen window over every gathered partial row.
        assert_eq!(window(&consumer), (0, Some(2), 1, false, true));
        // The Final publishes the receiver's layout unchanged, so the
        // receiver carries the result labels the frontend declared.
        assert_eq!(
            consumer.graph().nodes()[0]
                .output_layout()
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["v0", "v1"]
        );
        assert!(matches!(
            consumer.graph().sink(),
            Some(StaticSinkProgram::Result)
        ));
    }

    #[test]
    fn a_partial_budget_short_of_the_final_window_never_reaches_the_compiler() {
        // The physical sequence trace, not the compiler, owns the pairing.
        let error = packages(2).expect_err("partial limit must equal limit + offset");
        assert!(
            error.contains("TopN partial paths do not reduce exactly into their matching final"),
            "{error}"
        );
    }
}

#[test]
fn grouped_topn_refuses_invalid_phase_offset_and_empty_keys_before_construction() {
    let package = package(&integer_pool(), 1, false, 2, 0);
    let original = &package.fragment().nodes()[&NodeId::new(0)];
    let NodeKind::TopN { order_by, .. } = &original.kind else {
        panic!("TopN")
    };
    let expr = order_by[0].expr;
    let ExprKind::Value(value) = package
        .fragment()
        .root_expression(NodeId::new(0), expr)
        .unwrap()
        .kind
    else {
        panic!("direct key")
    };
    let sequence = TopNSequenceId::new(3);
    for (phase, offset, empty) in [
        (TopNPhase::Single, 0, false),
        (TopNPhase::Partial { sequence }, 1, false),
        (TopNPhase::Partial { sequence }, 0, true),
    ] {
        let mut node = original.clone();
        let NodeKind::TopN {
            phase: actual,
            offset: actual_offset,
            reduction,
            ..
        } = &mut node.kind
        else {
            unreachable!()
        };
        *actual = phase;
        *actual_offset = offset;
        *reduction = novarocks_physical_plan::TopNReduction::GroupedStates {
            group_by: if empty {
                Box::default()
            } else {
                Box::from([(expr, value)])
            },
            calls: Box::default(),
            comparator: novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
        };
        assert!(matches!(
            crate::topn::admit_grouped_topn_shape(&node),
            Err(FragmentCompileError::Invalid(
                "GroupedTopN requires complete keys and a partial zero-offset input"
            ))
        ));
        let mut request = |_| -> Result<(), std::convert::Infallible> {
            panic!("reject the invalid contract before constructing values")
        };
        let mut opaque = |_| -> Result<(), std::convert::Infallible> {
            panic!("reject the invalid contract before constructing values")
        };
        let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut opaque,
        );
        let error = crate::topn::lower_grouped_topn_with_creation(
            &package,
            &node,
            ProgramNodeId::new(0),
            &BTreeMap::new(),
            &[],
            &Control::good(),
            &mut creation,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            crate::FragmentCreationError::Compile(FragmentCompileError::Invalid(
                "GroupedTopN requires complete keys and a partial zero-offset input"
            ))
        ));
    }
}

#[test]
fn grouped_topn_rejects_a_key_outside_the_partition_domain() {
    let pool = checked_pool(
        Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_iter(
                [b"12345678", b"abcdefgh", b"87654321"].into_iter(),
            )
            .unwrap(),
        ),
        false,
        ValueLogicalType::Physical,
    );
    // The original package is a valid row-count TopN. Only the private
    // grouped lowering author receives this standalone contract input.
    let package = package(&pool, 1, false, 2, 0);
    let mut node = package.fragment().nodes()[&NodeId::new(0)].clone();
    let NodeKind::TopN {
        phase,
        reduction,
        order_by,
        ..
    } = &mut node.kind
    else {
        unreachable!()
    };
    let expr = order_by[0].expr;
    let definition = package
        .fragment()
        .root_expression(NodeId::new(0), expr)
        .unwrap();
    let ExprKind::Value(value) = definition.kind else {
        unreachable!()
    };
    let comparator = novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1;
    assert!(
        !novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1
            .supports_partition_value_type(&definition.ty)
    );
    assert!(!novarocks_physical_plan::grouped_state_topn_key_supported(
        comparator,
        &definition.ty
    ));
    *phase = TopNPhase::Partial {
        sequence: TopNSequenceId::new(3),
    };
    *reduction = novarocks_physical_plan::TopNReduction::GroupedStates {
        group_by: Box::from([(expr, value)]),
        calls: Box::default(),
        comparator,
    };
    let mut request =
        |_| -> Result<(), std::convert::Infallible> { panic!("reject before value construction") };
    let mut opaque =
        |_| -> Result<(), std::convert::Infallible> { panic!("reject before value construction") };
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut opaque,
    );
    let error = crate::topn::lower_grouped_topn_with_creation(
        &package,
        &node,
        ProgramNodeId::new(0),
        &BTreeMap::new(),
        &[novarocks_types::SlotId(0)],
        &Control::good(),
        &mut creation,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        crate::FragmentCreationError::Compile(FragmentCompileError::Invalid(
            "GroupedTopN key is not an exact supported direct value"
        ))
    ));
}
