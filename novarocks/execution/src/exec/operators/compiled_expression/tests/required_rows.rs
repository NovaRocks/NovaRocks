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

//! Required row promotion and typed occurrence projection through the real operator helper.
use arrow::{
    array::{Array, Decimal128Array},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledExpressionInstance,
    testkit::row_errors::{Control, Program, Shape, batch, instance, program},
};
use std::sync::Arc;

// This operator caller owns a real Values -> Project graph and publishes the
// same actual unit table through LocalProgram's original final author.
fn operator_program(source: &Program) -> novarocks_local_program::LocalProgram {
    use novarocks_local_program::*;
    use novarocks_type_contract::{FunctionArgumentType, ValueTypeStorageLifetime};
    use std::{collections::BTreeSet, num::NonZeroUsize};
    let root = source.unit.root(source.roots[0]).unwrap();
    let scope = source.unit.scope(root.scope).unwrap();
    let input = StaticLayout::try_new_typed_for_compile(
        scope.schema().clone(),
        Arc::from([
            novarocks_types::SlotId::new(333),
            novarocks_types::SlotId::new(901),
        ]),
        scope
            .ordinal_types()
            .iter()
            .map(|ty| (ty.typed_binding().clone(), None))
            .collect(),
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    )
    .unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(
            input.schema().clone(),
            scope
                .ordinal_types()
                .iter()
                .map(|ty| arrow::array::new_null_array(ty.data_type(), 0))
                .collect(),
        )
        .unwrap(),
        input.clone(),
    )
    .unwrap();
    let FunctionArgumentType::Value(output_type) = &source.unit.types()[root.definition.index()]
    else {
        panic!("the actual Project root is scalar")
    };
    let output = StaticLayout::try_new_typed_for_compile(
        Arc::new(arrow::datatypes::Schema::new(vec![
            output_type.try_to_field("result").unwrap(),
        ])),
        Arc::from([novarocks_types::SlotId::new(77)]),
        vec![(output_type.typed_binding().clone(), None)],
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    )
    .unwrap();
    let site = ProgramRootRef {
        unit: ProgramUnitId::new(0),
        root: source.roots[0],
    };
    let nodes = vec![
        ProgramNode::new_local(
            ProgramNodeId::new(0),
            vec![DiagnosticSourceNodeId::new(17)],
            ProgramNodeKind::Values { values },
            input,
        ),
        ProgramNode::new_local(
            ProgramNodeId::new(1),
            vec![DiagnosticSourceNodeId::new(2)],
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![site],
                expr_slot_ids: vec![novarocks_types::SlotId::new(77)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            output.clone(),
        ),
    ];
    let graph = LocalProgramGraph::try_new_with_sink(
        nodes,
        ProgramNodeId::new(1),
        std::slice::from_ref(&source.unit),
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            output.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![BindingRequirement::ResultSink { layout: output }])
            .unwrap(),
        Some(StaticSinkProgram::Result),
    )
    .unwrap();
    let channels = [
        (0, 0, scope.ordinal_types()[0].clone()),
        (0, 1, scope.ordinal_types()[1].clone()),
        (1, 0, output_type.clone()),
    ]
    .into_iter()
    .map(|(node, ordinal, ty)| {
        (
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(node),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal,
            },
            ty,
        )
    })
    .collect();
    let operators = [(0, 17), (1, 2)]
        .into_iter()
        .map(|(index, source)| LocalOperatorProvenance {
            id: LocalOperatorId::new(index),
            lowered_nodes: Box::from([ProgramNodeId::new(index as usize)]),
            sources: Box::from([DiagnosticSourceNodeId::new(source)]),
            origin: LocalOperatorOrigin::Direct,
            cost_owner: LocalOperatorId::new(index),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        })
        .collect();
    LocalProgram::try_new(
        graph,
        vec![source.unit.clone()],
        vec![],
        channels,
        operators,
        &BTreeSet::from([
            DiagnosticSourceNodeId::new(17),
            DiagnosticSourceNodeId::new(2),
        ]),
        CompiledProgramFacts::default(),
        &Control,
    )
    .unwrap()
}

// The operator helper behind ChangeEventExpand assignments evaluates only the
// selected rows: an overflow on an unselected row raises nothing, and a
// selected row's overflow is a required error reported at its batch row.
#[test]
fn operator_selected_evaluation_skips_unselected_row_errors_and_reports_batch_rows() {
    use crate::exec::operators::compiled_expression::evaluate_selected;
    let program = program(Shape::Single);
    let max38 = 10_i128.pow(38) - 1;
    let input = batch(
        &program,
        vec![Some(15), Some(max38), Some(25)],
        vec![None; 3],
    );
    let local = operator_program(&program);
    let node = &local.graph().nodes()[local.graph().root().index()];
    let novarocks_local_program::ProgramNodeKind::Project { exprs, .. } = node.kind() else {
        panic!("the actual caller is a Project")
    };
    let site = exprs[0];
    let actual_node = node.local_id().unwrap();
    let unit = local.unit(site.unit).unwrap();
    assert!(Arc::ptr_eq(unit, &program.unit));
    assert_eq!(site.root, program.roots[0]);
    let mut evaluator =
        CompiledExpressionInstance::try_new(unit.clone(), site.root, &Control).unwrap();
    let control =
        crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::default());
    let values = evaluate_selected(&mut evaluator, site, &input, &[0, 2], &control)
        .map_err(|cause| cause.at_expression_node(actual_node))
        .unwrap();
    let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!((values.value(0), values.value(1)), (20, 30));
    let original = evaluate_selected(&mut instance(&program, 0), site, &input, &[1, 2], &control)
        .expect_err("the selected overflow row is required");
    let error = original.clone().at_expression_node(actual_node);
    assert!(error.same_occurrence(&original));
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(&original),
    );
    assert_eq!(error.expression_node(), Some(actual_node));
    let crate::runtime::task::execution_failure::ExecutionFailureCause::RequiredRow(required) =
        error.cause()
    else {
        panic!("the selected ROUND overflow must retain its typed row error")
    };
    assert_eq!(required.root(), site.root);
    assert_eq!(required.batch_row(), 1);
    assert_eq!(required.error().selected_ordinal(), 0);
    assert!(required.error().message().contains("overflow"));
    assert!(std::ptr::eq(
        std::error::Error::source(required)
            .unwrap()
            .downcast_ref::<novarocks_functions::RowDataError>()
            .unwrap(),
        required.error(),
    ));
    let message = error.to_string();
    assert!(
        message.contains("failed at batch row 1 (selected ordinal 0)"),
        "{message}"
    );
    assert!(message.contains("overflow"), "{message}");
    let unordered = evaluate_selected(&mut instance(&program, 0), site, &input, &[2, 0], &control)
        .map_err(|cause| cause.at_expression_node(actual_node))
        .expect_err("rows must be strictly increasing");
    assert_eq!(unordered.expression_node(), None);
    assert!(unordered.to_string().contains("not ordered"), "{unordered}");
}
