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

use crate::exec::chunk::Chunk;
use crate::exec::expr::fixtures::{RandArgument, program};
use crate::exec::operators::{ResultSinkBuffer, ResultSinkFactory};
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::{Float64Array, Int64Array};
use std::sync::Arc;
use std::time::Duration;

fn run(program: Arc<novarocks_local_program::LocalProgram>) -> Vec<Chunk> {
    let state = Arc::new(RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled tests",
        )),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    let output = ResultSinkBuffer::new();
    let prepared = prepare_compiled_program_pipeline_execution(
        program,
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        crate::exec::pipeline::binding::ExchangeBindings::default(),
        None,
        1,
        state,
        Arc::new(crate::runtime::task::io::NoopTaskEventSink),
    )
    .expect("compiled program prepares drivers");
    prepared.start().join().expect("compiled program runs");
    output.take_chunks()
}

// Values -> Project(literals) -> Filter -> Project(RAND(bound), bound, bound) ->
// Limit -> Result, compiled by local-compiler and executed only through
// compiled roots. The RAND oracle is independent of the evaluator.
#[test]
fn compiled_program_runs_through_compiled_roots_to_result_rows() {
    for mode in [RandArgument::Input, RandArgument::DirectConstant] {
        let chunks = run(program(mode, false));
        let rows: usize = chunks.iter().map(Chunk::len).sum();
        assert_eq!(rows, 1);
        let batch = chunks.iter().find(|c| c.len() == 1).unwrap().record_batch();
        let sample = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bounded RAND result is Int64");
        assert_eq!(sample.value(0), 0);
        for column in 1..3 {
            let bound = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("bound passthrough is Int64");
            assert_eq!(bound.value(0), 1);
        }
    }
}

// A shared volatile definition has two independently activated uses. A
// cancellation within one frame must not poison its sibling's RNG instance.
#[test]
fn twin_roots_of_one_definition_keep_independent_instances_in_the_pipeline() {
    use crate::exec::expr::fixtures::{input_chunk, input_project};
    use crate::exec::operators::compiled_expression::{RuntimeKernelControl, instances};
    use crate::runtime::runtime_state::RuntimeErrorState;
    use novarocks_expression::{
        EvaluationInput, ExprEvaluationFailure, testkit::PassThroughCallHost,
    };
    use novarocks_functions::{
        KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
        Selection,
    };

    struct Cancel;
    impl KernelEvaluationControl for Cancel {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Err(KernelFailure::Cancelled)
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("RAND cannot wait")
        }
    }

    let program = program(RandArgument::Unbounded, true);
    let (_, sites) = input_project(&program);
    let left = program
        .unit(sites[0].unit)
        .unwrap()
        .root(sites[0].root)
        .unwrap();
    let right = program
        .unit(sites[1].unit)
        .unwrap()
        .root(sites[1].root)
        .unwrap();
    assert_eq!(sites[0].unit, sites[1].unit);
    assert_eq!(left.definition, right.definition);
    assert_ne!(left.use_id, right.use_id);

    let mut frames = None;
    let control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    instances(&mut frames, &program, &sites[..2], &control).unwrap();
    let mut frames = frames.unwrap();
    let input = input_chunk(&program, vec![Some(1)]);
    let data = EvaluationInput::Batch {
        batch: input.record_batch(),
        source: None,
    };
    let selection = Selection::all(1);
    let invocation = ScalarInvocationScope::new(selection, selection);
    let activation = ScalarInvocationActivation::Activated;
    frames[0]
        .evaluate(
            data,
            invocation,
            activation,
            &control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(matches!(
        frames[0].evaluate(
            data,
            invocation,
            activation,
            &Cancel,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(KernelFailure::Cancelled))
    ));
    assert!(matches!(
        frames[0].evaluate(
            data,
            invocation,
            activation,
            &control,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    let sibling = frames[1]
        .evaluate(
            data,
            invocation,
            activation,
            &control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let values = sibling
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((0.0..1.0).contains(&values.value(0)));

    let chunks = run(program);
    let batch = chunks.iter().find(|c| c.len() == 1).unwrap().record_batch();
    for column in 0..2 {
        let sample = batch
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("unbounded RAND result is Float64");
        assert!((0.0..1.0).contains(&sample.value(0)), "root {column}");
    }
}

// The filter factory reads all predicate references from its actual Filter
// or Scan node. Other node families are refused while building the pipeline.
#[test]
fn compiled_filter_factory_accepts_only_an_owned_predicate_site() {
    use crate::exec::operators::compiled_expression::CompiledFilterProcessorFactory;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use novarocks_local_program::{ProgramNodeId, ProgramNodeKind};

    let program = program(RandArgument::Input, false);
    let filter = ProgramNodeId::new(2);
    let project = ProgramNodeId::new(3);
    assert!(matches!(
        program.graph().nodes()[filter.index()].kind(),
        ProgramNodeKind::Filter { .. }
    ));
    let error = Arc::new(RuntimeErrorState::default());
    assert!(
        CompiledFilterProcessorFactory::try_new(Arc::clone(&program), filter, Arc::clone(&error))
            .is_ok()
    );
    // All three references are actual nodes in this fixture: Project, Values,
    // and the root Limit. A caller supplies no expression-role selector.
    for refused in [project, ProgramNodeId::new(0), program.graph().root()] {
        let failure = match CompiledFilterProcessorFactory::try_new(
            Arc::clone(&program),
            refused,
            Arc::clone(&error),
        ) {
            Ok(_) => panic!("{refused:?} is not a predicate-owning node"),
            Err(failure) => failure,
        };
        assert!(
            failure
                .to_string()
                .contains("compiled filter requires its actual Filter or Scan node"),
            "{failure}"
        );
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&failure).class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
    }
}

mod table_function;
