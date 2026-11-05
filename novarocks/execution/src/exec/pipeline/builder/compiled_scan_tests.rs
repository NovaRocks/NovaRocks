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

//! A compiled provider Scan runs from its Task's bound scan operation: one
//! driver polls the scan stream, the residual is a compiled filter over the
//! scan's own output, and the projection publishes SQL names. Results are
//! compared with an oracle computed from the scripted rows.

use std::sync::Arc;
use std::time::Duration;

use novarocks_local_program::LocalProgram;

use crate::exec::chunk::Chunk;
use crate::exec::node::scan::ScanOp;
use crate::exec::operators::{ResultSinkBuffer, ResultSinkFactory};
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::executor::{
    PreparedPipelineExecution, prepare_compiled_program_pipeline_execution_with_profiler,
};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::ExecutionResult;
use crate::runtime::task::io::NoopTaskEventSink;
use crate::runtime::task::scan::compiled_fixture::{
    FixtureScanOp, SCAN_NODE, rows, scan_chunk, scan_program,
};

/// The scripted `(v0, v1)` rows, across a nonempty, an empty and a second
/// nonempty chunk.
const INPUT: [(i64, i64); 5] = [(1, 10), (8, 80), (9, 90), (7, 70), (20, 200)];

fn input(program: &LocalProgram) -> Vec<Chunk> {
    let column = |range: std::ops::Range<usize>, pick: fn(&(i64, i64)) -> i64| {
        INPUT[range].iter().map(pick).collect::<Vec<_>>()
    };
    vec![
        scan_chunk(
            program,
            &column(0..3, |row| row.0),
            &column(0..3, |row| row.1),
        ),
        scan_chunk(program, &[], &[]),
        scan_chunk(
            program,
            &column(3..5, |row| row.0),
            &column(3..5, |row| row.1),
        ),
    ]
}

/// `SELECT v1 AS b, v0 AS a FROM t [WHERE v0 > 7]`, row by row.
fn oracle(residual: bool) -> Vec<(i64, i64)> {
    INPUT
        .iter()
        .filter(|(v0, _)| !residual || *v0 > 7)
        .map(|(v0, v1)| (*v1, *v0))
        .collect()
}

fn bound(op: &Arc<FixtureScanOp>) -> ScanBindings {
    let mut bindings = ScanBindings::default();
    bindings.insert(SCAN_NODE, Arc::clone(op) as Arc<dyn ScanOp>);
    bindings
}

fn prepare(
    program: &Arc<LocalProgram>,
    bindings: ScanBindings,
    output: &ResultSinkBuffer,
) -> ExecutionResult<PreparedPipelineExecution> {
    let state = Arc::new(RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled scan tests",
        )),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    prepare_compiled_program_pipeline_execution_with_profiler(
        Arc::clone(program),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        bindings,
        crate::runtime::task::CompiledWriterBindings::default(),
        None,
        None,
        dop,
        state,
        Arc::new(NoopTaskEventSink),
    )
}

fn refusal(program: &Arc<LocalProgram>, bindings: ScanBindings) -> String {
    match prepare(program, bindings, &ResultSinkBuffer::new()) {
        Ok(_) => panic!("compiled scan preparation must be refused"),
        Err(error) => error.to_string(),
    }
}

// The residual decides every row after the one scan driver, also behind the
// handoff to more drivers, and the projection reorders under SQL names.
#[test]
fn a_compiled_scan_runs_its_residual_and_projection_against_the_oracle() {
    for dop in [1, 2] {
        for residual in [true, false] {
            let program = scan_program(dop, residual);
            let op = FixtureScanOp::new(input(&program), false);
            let output = ResultSinkBuffer::new();
            let prepared = prepare(&program, bound(&op), &output)
                .unwrap_or_else(|error| panic!("dop {dop} residual {residual}: {error}"));
            prepared.start().join().expect("the compiled scan runs");
            let mut actual = rows(&output.take_chunks());
            let mut expected = oracle(residual);
            if dop > 1 {
                // The handoff fans rows out to several drivers.
                actual.sort_unstable();
                expected.sort_unstable();
            }
            assert_eq!(actual, expected, "dop {dop} residual {residual}");
            assert_eq!(op.claims(), 1, "one driver owns the scan stream");
            assert_eq!(op.terminations(), 0, "a finished scan is not aborted");
        }
    }
}

#[test]
fn compiled_scan_bindings_cover_exactly_the_compiled_scans() {
    let program = scan_program(1, true);
    let op = FixtureScanOp::new(Vec::new(), false);
    let missing = refusal(&program, ScanBindings::default());
    assert!(
        missing.contains("missing scan binding for compiled scan node 10"),
        "{missing}"
    );

    let mut misplaced = ScanBindings::default();
    misplaced.insert(SCAN_NODE + 1, Arc::clone(&op) as Arc<dyn ScanOp>);
    let misplaced = refusal(&program, misplaced);
    assert!(
        misplaced.contains("missing scan binding for compiled scan node 10"),
        "{misplaced}"
    );

    let mut extra = bound(&op);
    extra.insert(99, Arc::clone(&op) as Arc<dyn ScanOp>);
    let extra = refusal(&program, extra);
    assert!(
        extra.contains("scan binding for node 99 has no compiled scan"),
        "{extra}"
    );
    assert_eq!(op.claims(), 0, "a refused preparation claims no stream");
}

// An abort reaches a reader whose driver is parked on its stream: the
// prepared execution retains the bound scan's terminal hook.
#[test]
fn an_aborted_compiled_scan_reaches_its_parked_reader() {
    let program = scan_program(1, true);
    let op = FixtureScanOp::new(vec![scan_chunk(&program, &[8], &[80])], true);
    let output = ResultSinkBuffer::new();
    let running = prepare(&program, bound(&op), &output)
        .expect("compiled scan prepares")
        .start();
    let cancellation: crate::runtime::task::ExecutionFailure =
        crate::runtime::task::TaskCancelDetail::new("aborted by the test").into();
    running.cancel(cancellation.clone());
    assert!(
        running
            .conclusion()
            .unwrap()
            .unwrap_err()
            .same_occurrence(&cancellation)
    );
    let error = running
        .join()
        .expect_err("an aborted scan fails its fragment");
    assert!(
        op.terminations() >= 1,
        "actual join observes the scan's terminal cleanup, independently of cancel acceptance"
    );
    assert!(error.to_string().contains("aborted by the test"), "{error}");
}
