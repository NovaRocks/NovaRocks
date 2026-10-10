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
//! Actual SQL-authored Window input roots at the runtime funding boundary.
use super::filter_conjunction_actual_sql_compiler_tests::compiler_results;
use super::ndv_filter_actual_sql_source_tests::sql_source_with_columns;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use crate::{
    exec::{
        chunk::{Chunk, ChunkSchema},
        operators::compiled_window::CompiledWindowProcessorFactory,
        pipeline::operator_factory::OperatorFactory,
    },
    runtime::{
        fragment::ExecutionFailureCause, mem_tracker::MemTracker, query_memory::QueryMemoryBinding,
        scalar_memory::RuntimeScalarMemoryRefusal,
    },
};
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_local_program::{ProgramNodeId, ProgramNodeKind};
use novarocks_memory::{AccountKind, ExternalRef};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::sync::Arc;

#[test]
fn runtime_scalar_memory_actual_sql_window_target_uses_same_binding() {
    let source = sql_source_with_columns(
        "SELECT first_value(bit_shift_left(v,c)) OVER () AS r FROM fixture.ndv_null_contract",
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        &[("v", DataType::Int64), ("c", DataType::Int64)],
    );
    let mut found = None;
    for program in compiler_results(&source, &installed_builtin_owner_catalogue()).into_values() {
        let program = Arc::new(program.unwrap());
        for (index, node) in program.graph().nodes().iter().enumerate() {
            if matches!(node.kind(), ProgramNodeKind::Analytic { .. }) {
                assert!(
                    found
                        .replace((program.clone(), ProgramNodeId::new(index)))
                        .is_none()
                );
            }
        }
    }
    let (program, node) = found.expect("actual SQL has one Analytic");
    let ProgramNodeKind::Analytic { input, .. } = program.graph().nodes()[node.index()].kind()
    else {
        unreachable!()
    };
    let wanted = novarocks_local_program::ProgramExpressionRootSite::Node {
        node,
        role: novarocks_local_program::ProgramNodeExpressionRole::WindowInput {
            call: 0,
            argument: 0,
        },
    };
    assert!(program.checked().channels().expressions().resolved_calls().calls().values().any(|call| {
        matches!(call.state_template(), novarocks_local_program::ProgramStateTemplate::Scalar { scope, kernel } if scope.root == wanted && kernel.invocation_resource_profile().is_some())
    }), "the actual Window input owns an installed covered shift");
    let layout = program.graph().nodes()[input.index()].output_layout();
    let batch = RecordBatch::try_new(
        layout.schema().clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None, Some(3)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = ChunkSchema::from_compiled_layout(layout).unwrap();
    for funded in [false, true] {
        let runtime = crate::runtime::execution_runtime::test_execution_runtime();
        let execution =
            QueryExecutionId::new(QueryId::new(37, 303), AttemptId::new(1).unwrap()).unwrap();
        let account = runtime
            .memory_authority()
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let binding =
            QueryMemoryBinding::try_new(execution, runtime.memory_authority().clone(), account)
                .unwrap();
        let task = novarocks_execution_contract::TaskIdentity::new(
            execution,
            novarocks_types::identity::StageId::new(1).unwrap(),
            novarocks_types::identity::TaskId::new(1).unwrap(),
            novarocks_types::identity::BackendProcessId::new_v7(),
        );
        let state = crate::runtime::fragment::runtime_state::build_runtime_state(
            crate::runtime::fragment::runtime_state::RuntimeStateInputs {
                query_options: None,
                query_id: Some(execution.query_id()),
                fragment_instance_id: None,
                backend_num: None,
                mem_tracker: None,
                runtime_filter_session: None,
                execution_runtime: Some(runtime),
                query_memory: funded.then_some(binding),
                task_identity: Some(task),
            },
        )
        .unwrap();
        let factory =
            CompiledWindowProcessorFactory::try_new(program.clone(), node, state.error_state())
                .unwrap();
        let mut operator = factory.create(1, 0);
        operator.set_mem_tracker(MemTracker::new_root("window scalar funding fixture"));
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        let processor = operator.as_processor_mut().unwrap();
        let result = processor
            .push_chunk(
                &state,
                Chunk::try_new_with_chunk_schema(batch.clone(), schema.clone()).unwrap(),
            )
            .and_then(|()| processor.set_finishing(&state));
        if funded {
            result.unwrap();
            let mut values = Vec::new();
            while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
                values.extend(
                    chunk
                        .columns()
                        .last()
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter(),
                );
            }
            assert_eq!(values, vec![Some(14); 3]);
        } else {
            let error = result.expect_err("the actual Window argument invokes a covered scalar");
            assert_eq!(
                error.cause(),
                &ExecutionFailureCause::RuntimeScalarMemory(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                )
            );
            assert_eq!(
                processor.pull_chunk(&state).unwrap_err().cause(),
                error.cause()
            );
        }
    }
}

/// Actual SQL supplies the installed scalar and its original checked tree.
/// The RF fixture separately authors a new complete checked occurrence.
pub(crate) fn scan_shift_base() -> (
    Arc<novarocks_local_program::LocalProgram>,
    novarocks_functions::PureEngineFunctionCatalog,
) {
    let source = sql_source_with_columns(
        "SELECT bit_shift_left(v,c) AS r FROM fixture.ndv_null_contract",
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        &[("v", DataType::Int64), ("c", DataType::Int64)],
    );
    let functions = installed_builtin_owner_catalogue();
    let mut found = None;
    for program in compiler_results(&source, &functions).into_values() {
        let program = program.unwrap();
        if !program.scan_inputs().is_empty() {
            assert!(found.replace(Arc::new(program)).is_none());
        }
    }
    (found.unwrap(), functions)
}
