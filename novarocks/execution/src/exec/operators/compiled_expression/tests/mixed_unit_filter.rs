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

use super::super::*;
use crate::exec::expr::test_support::{HarnessControl, constant_policy};
use arrow::datatypes::{DataType, Schema};
use novarocks_expression::{
    CompiledExprSet, EvaluationEnvironment, ScopeColumn, ScopeLayout, ScopeLayouts,
    compile_expr_set_pure,
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_local_program::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
    ValueTypeStorageLifetime,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

fn predicate_unit(layout: &StaticLayout, ty: &FunctionValueType) -> Arc<CompiledExprSet> {
    let control = HarnessControl;
    let value = ValueId::new(17);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
    });
    let definition = draft.add(None, ty.clone(), ExprKind::Value(value)).unwrap();
    let root = draft
        .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    assert_eq!(root, RootOrdinal::new(0));
    let unit = draft
        .seal(
            vec![ExprPort {
                values: Box::from([(value, ty.clone())]),
            }],
            &mut work,
        )
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let scope = ScopeLayout::try_new(
        layout.schema().clone(),
        Arc::from([ty.clone()]),
        Arc::from([ScopeColumn {
            value,
            ordinal: 0,
            ty: ty.clone(),
        }]),
        ValueTypeStorageLifetime::pure(),
        &mut work,
    )
    .unwrap();
    // This real checked unit contains only a declared input, no call or constant.
    let constants = novarocks_physical_plan::ConstantPools::empty();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let catalog = novarocks_functions::testkit::rng_subset();
    let compiled = compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[scope],
            all_ports: None,
        },
        constant_policy(),
        &catalog,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    compiled
}

#[test]
fn filter_factory_preserves_actual_roots_from_distinct_unit_table_entries() {
    let control = HarnessControl;
    let ty = FunctionValueType::new(DataType::Boolean, false);
    let layout = StaticLayout::try_new_typed_for_compile(
        Arc::new(Schema::new(vec![ty.try_to_field("predicate").unwrap()])),
        Arc::from([novarocks_types::SlotId::new(17)]),
        vec![(ty.typed_binding().clone(), None)],
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &control,
    )
    .unwrap();
    let input = RecordBatch::try_new(
        layout.schema().clone(),
        vec![Arc::new(BooleanArray::from(vec![true, false, true]))],
    )
    .unwrap();
    // Exercise both the same scope pointer and equal independently authored scopes.
    for reuse_actual_arc in [true, false] {
        let first = predicate_unit(&layout, &ty);
        let second = if reuse_actual_arc {
            Arc::clone(&first)
        } else {
            predicate_unit(&layout, &ty)
        };
        assert_eq!(Arc::ptr_eq(&first, &second), reuse_actual_arc);
        let units = vec![first, second];
        let predicates =
            [0, 1].map(|index| ProgramRootRef::new(ProgramUnitId::new(index), RootOrdinal::new(0)));
        let values = StaticValues::try_new(input.clone(), layout.clone()).unwrap();
        let nodes = vec![
            ProgramNode::new_local(
                ProgramNodeId::new(0),
                vec![DiagnosticSourceNodeId::new(0)],
                ProgramNodeKind::Values { values },
                layout.clone(),
            ),
            ProgramNode::new_local(
                ProgramNodeId::new(1),
                vec![DiagnosticSourceNodeId::new(1)],
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicates: Box::from(predicates),
                },
                layout.clone(),
            ),
        ];
        let graph = LocalProgramGraph::try_new_with_sink(
            nodes,
            ProgramNodeId::new(1),
            &units,
            CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                layout.identity().unwrap(),
                KernelAbiVersion::CURRENT,
            ),
            BindingRequirements::try_new(vec![BindingRequirement::ResultSink {
                layout: layout.clone(),
            }])
            .unwrap(),
            Some(StaticSinkProgram::Result),
        )
        .unwrap();
        let channels = [0, 1]
            .map(|node| {
                (
                    ProgramChannelSite::Layout {
                        node: ProgramNodeId::new(node),
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal: 0,
                    },
                    ty.clone(),
                )
            })
            .to_vec();
        let operators = [0, 1]
            .map(|index| LocalOperatorProvenance {
                id: LocalOperatorId::new(index),
                lowered_nodes: Box::from([ProgramNodeId::new(index as usize)]),
                sources: Box::from([DiagnosticSourceNodeId::new(index)]),
                origin: LocalOperatorOrigin::Direct,
                cost_owner: LocalOperatorId::new(index),
                metrics: OperatorMetricAggregation {
                    cpu_time: MetricAggregation::Sum,
                    wall_time: MetricAggregation::Maximum,
                    peak_retained_bytes: MetricAggregation::Maximum,
                },
            })
            .to_vec();
        let program = Arc::new(
            LocalProgram::try_new(
                graph,
                units,
                vec![],
                channels,
                operators,
                &BTreeSet::from([
                    DiagnosticSourceNodeId::new(0),
                    DiagnosticSourceNodeId::new(1),
                ]),
                CompiledProgramFacts::default(),
                &control,
            )
            .unwrap(),
        );
        let ProgramNodeKind::Filter {
            predicates: actual, ..
        } = program.graph().nodes()[1].kind()
        else {
            panic!("the final author must retain the actual Filter node")
        };
        assert_eq!(actual.as_ref(), &predicates);
        let error = Arc::new(RuntimeErrorState::default());
        let factory = CompiledFilterProcessorFactory::try_new(
            program,
            ProgramNodeId::new(1),
            Arc::clone(&error),
        )
        .expect("the checked Filter roots may use distinct unit table entries");
        let mut body = factory.create_body();
        let truth = body
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                &RuntimeKernelControl::new(error),
            )
            .unwrap();
        assert_eq!(
            truth.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(true)]
        );
    }
}
