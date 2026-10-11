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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, Int64Array};
use novarocks_types::{QueryId, UniqueId};

use super::*;
use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::tests::{SEED_42_FIRST, SeedMode, program};
use crate::exec::fragment::program::FragmentContractVersion;
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::node::scan::{BoundScanRanges, ScanSource};
use crate::runtime::fragment::instance::{
    BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
    FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
};
use crate::runtime::fragment::io::{
    FragmentIoError, FragmentResultSession, FragmentResultWriter, ResultAbort,
    ResultWriteAdmission, ResultWriteCredit, ResultWriteSpec,
};
use crate::runtime::fragment::scan::compiled_fixture::{
    FixtureScanOp, FixtureScanSource, SCAN_NODE, rows, scan_chunk, scan_program,
};
use crate::runtime::fragment::{ExecutionFailureCause, ExecutionResult};
use crate::runtime::observable::Observable;
use crate::runtime::query_options::QueryOptions;

#[derive(Default)]
struct CollectingSession {
    chunks: Mutex<Vec<Chunk>>,
    finished: Mutex<bool>,
}

impl FragmentResultSession for CollectingSession {
    fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError> {
        Ok(chunk.logical_bytes())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError> {
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            |_| {},
        )))
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    fn write_with_credit(
        &self,
        chunk: Chunk,
        _credit: ResultWriteCredit,
    ) -> Result<(), FragmentIoError> {
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        *self.finished.lock().unwrap() = true;
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}

struct CollectingWriter {
    session: Arc<CollectingSession>,
}

impl FragmentResultWriter for CollectingWriter {
    fn open(
        &self,
        _spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        Ok(self.session.clone())
    }
}

fn refused(result: Result<DormantFragmentHandle, FragmentLaunchError>) -> FragmentLaunchError {
    match result {
        Ok(_) => panic!("compiled preparation must be refused"),
        Err(error) => error,
    }
}

fn instance(
    finst_id: UniqueId,
    pipeline_dop: usize,
    exchange_inputs: ExchangeInputAssignments,
) -> FragmentInstanceSpec {
    FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        QueryId::new(finst_id.high() - 2, finst_id.low() - 2),
        FragmentInstanceId::new(finst_id),
        ScanAssignments::default(),
        exchange_inputs,
        FragmentSinkAssignment::None,
        FragmentRuntimeOptions::new(QueryOptions::default(), false),
        NonZeroUsize::new(pipeline_dop).expect("nonzero DOP"),
        BackendNum::try_new(1).expect("backend number"),
    )
}

// The Task-facing entry prepares a compiled program into the ordinary dormant
// handle: its result rows reach the opened result session, computed only by
// compiled roots.
#[test]
fn compiled_submission_prepares_and_runs_into_the_result_session() {
    let program = program(SeedMode::Input, false);
    let submission = CompiledFragmentSubmission::try_new(
        program,
        CompiledScanSources::new(),
        instance(
            UniqueId::new(201, 202),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect("compiled submission");
    assert_eq!(submission.sink_kind(), FragmentSinkKind::Result);
    let session = Arc::new(CollectingSession::default());
    let mut context = FragmentPrepareContext::default();
    context.result_writer = Arc::new(CollectingWriter {
        session: Arc::clone(&session),
    });
    let handle = prepare_compiled_fragment(submission, context).expect("compiled prepare");
    assert!(matches!(
        handle.start().join().outcome(),
        FragmentOutcome::Succeeded
    ));
    let chunks = session.chunks.lock().unwrap();
    let rows: usize = chunks.iter().map(Chunk::len).sum();
    assert_eq!(rows, 1);
    let batch = &chunks.iter().find(|chunk| chunk.len() == 1).unwrap().batch;
    let sample = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("RAND result is Float64");
    assert_eq!(sample.value(0).to_bits(), SEED_42_FIRST);
    let seed = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("seed passthrough is Int64");
    assert_eq!(seed.value(0), 42);
}

#[test]
fn compiled_submission_refuses_a_foreign_dop_and_unaddressed_inputs() {
    let program = program(SeedMode::Input, false);
    let error = CompiledFragmentSubmission::try_new(
        Arc::clone(&program),
        CompiledScanSources::new(),
        instance(
            UniqueId::new(203, 204),
            2,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect_err("compiled DOP is a frozen profile fact");
    assert!(error.to_string().contains("pipeline DOP"), "{error}");

    // An exchange assignment for a receiver the program does not have is
    // refused at registration, and nothing stays registered.
    let stray = ExchangeInputAssignments::new(std::collections::BTreeMap::from([(
        crate::exec::fragment::program::FragmentNodeId::new(7),
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    )]));
    let submission = CompiledFragmentSubmission::try_new(
        program,
        CompiledScanSources::new(),
        instance(UniqueId::new(205, 206), 1, stray),
    )
    .expect("submission facts are consistent");
    let error = refused(prepare_compiled_fragment(
        submission,
        FragmentPrepareContext::default(),
    ));
    assert!(
        error
            .to_string()
            .contains("has no compiled exchange source"),
        "{error}"
    );
}

#[test]
fn a_host_root_sink_width_must_equal_the_compiled_width() {
    let program = program(SeedMode::Input, false);
    let submission = CompiledFragmentSubmission::try_new(
        program,
        CompiledScanSources::new(),
        instance(
            UniqueId::new(207, 208),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect("compiled submission");
    let mut context = FragmentPrepareContext::default();
    context.root_sink_dop = Some(4);
    let error = refused(prepare_compiled_fragment(submission, context));
    assert!(error.to_string().contains("root sink width"), "{error}");
}

fn scan_instance(finst_id: UniqueId, assignments: ScanAssignments) -> FragmentInstanceSpec {
    FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        QueryId::new(finst_id.high() - 2, finst_id.low() - 2),
        FragmentInstanceId::new(finst_id),
        assignments,
        ExchangeInputAssignments::default(),
        FragmentSinkAssignment::None,
        FragmentRuntimeOptions::new(QueryOptions::default(), false),
        NonZeroUsize::new(1).expect("nonzero DOP"),
        BackendNum::try_new(1).expect("backend number"),
    )
}

fn scan_assignments(nodes: &[i32], ranges: BoundScanRanges) -> ScanAssignments {
    ScanAssignments::try_new(
        nodes
            .iter()
            .map(|node| (FragmentNodeId::new(*node), ranges.clone()))
            .collect(),
    )
    .expect("scan assignments")
}

fn scan_sources(program: &LocalProgram, op: &Arc<FixtureScanOp>) -> CompiledScanSources {
    let scan = *program
        .scan_inputs()
        .keys()
        .next()
        .expect("the fixture has one scan");
    BTreeMap::from([(
        scan,
        Arc::new(FixtureScanSource(Arc::clone(op))) as Arc<dyn ScanSource>,
    )])
}

fn submission_refusal(
    program: Arc<LocalProgram>,
    scans: CompiledScanSources,
    instance: FragmentInstanceSpec,
) -> String {
    CompiledFragmentSubmission::try_new(program, scans, instance)
        .expect_err("the compiled submission must be refused")
        .to_string()
}

// The Task's scan source is bound to the instance's empty range binding of
// the physical scan node, and its rows reach the opened result session
// through the compiled residual and projection.
#[test]
fn a_compiled_scan_submission_binds_its_task_source_and_runs_into_the_result_session() {
    let program = scan_program(1, true);
    let op = FixtureScanOp::new(
        vec![
            scan_chunk(&program, &[1, 8], &[10, 80]),
            scan_chunk(&program, &[20], &[200]),
        ],
        false,
    );
    let submission = CompiledFragmentSubmission::try_new(
        Arc::clone(&program),
        scan_sources(&program, &op),
        scan_instance(
            UniqueId::new(211, 212),
            scan_assignments(&[SCAN_NODE], BoundScanRanges::None),
        ),
    )
    .expect("compiled scan submission");
    assert_eq!(submission.sink_kind(), FragmentSinkKind::Result);
    let session = Arc::new(CollectingSession::default());
    let context = FragmentPrepareContext {
        result_writer: Arc::new(CollectingWriter {
            session: Arc::clone(&session),
        }),
        ..FragmentPrepareContext::default()
    };
    let handle = prepare_compiled_fragment(submission, context).expect("compiled prepare");
    assert!(matches!(
        handle.start().join().outcome(),
        FragmentOutcome::Succeeded
    ));
    assert_eq!(
        rows(&session.chunks.lock().unwrap()),
        vec![(80, 8), (200, 20)]
    );
    assert_eq!(op.claims(), 1, "one driver owns the scan stream");
}

#[test]
fn a_compiled_scan_submission_refuses_unbound_and_unaddressed_scans() {
    let scanned = scan_program(1, true);
    let op = FixtureScanOp::new(Vec::new(), false);
    let assigned = || scan_assignments(&[SCAN_NODE], BoundScanRanges::None);

    let unbound = submission_refusal(
        Arc::clone(&scanned),
        CompiledScanSources::new(),
        scan_instance(UniqueId::new(213, 214), assigned()),
    );
    assert!(unbound.contains("has no Task scan source"), "{unbound}");

    let unassigned = submission_refusal(
        Arc::clone(&scanned),
        scan_sources(&scanned, &op),
        scan_instance(UniqueId::new(215, 216), ScanAssignments::default()),
    );
    assert!(
        unassigned.contains("has no instance scan assignment"),
        "{unassigned}"
    );

    let stray = submission_refusal(
        Arc::clone(&scanned),
        scan_sources(&scanned, &op),
        scan_instance(
            UniqueId::new(217, 218),
            scan_assignments(&[SCAN_NODE, SCAN_NODE + 2], BoundScanRanges::None),
        ),
    );
    assert!(stray.contains("names no compiled scan"), "{stray}");

    // A program without a scan takes no scan source.
    let scanless = submission_refusal(
        program(SeedMode::Input, false),
        scan_sources(&scanned, &op),
        scan_instance(UniqueId::new(219, 220), ScanAssignments::default()),
    );
    assert!(
        scanless.contains("which is not a compiled scan"),
        "{scanless}"
    );

    // The source checks its range variant when preparation binds it, and the
    // refusal rolls back everything acquired before it.
    let submission = CompiledFragmentSubmission::try_new(
        Arc::clone(&scanned),
        scan_sources(&scanned, &op),
        scan_instance(
            UniqueId::new(221, 222),
            scan_assignments(
                &[SCAN_NODE],
                BoundScanRanges::SchemaSelection { should_scan: true },
            ),
        ),
    )
    .expect("the range variant is the source's to check");
    let error = refused(prepare_compiled_fragment(
        submission,
        FragmentPrepareContext::default(),
    ));
    assert!(error.to_string().contains("bind failed"), "{error}");
    assert_eq!(op.claims(), 0, "no refused submission claims the stream");
}

struct RecordingProjectSchemaHost<'a> {
    program: &'a Arc<LocalProgram>,
    sites: Vec<crate::runtime::preparation_metadata::ProjectSchemaSite>,
    body_calls: usize,
    refusal: Option<(
        usize,
        crate::runtime::preparation_metadata::PreparationMetadataFailure,
    )>,
}
impl crate::runtime::preparation_metadata::CompiledSchemaMetadataScope
    for RecordingProjectSchemaHost<'_>
{
    fn materialize<B>(
        &mut self,
        program: &Arc<LocalProgram>,
        site: crate::runtime::preparation_metadata::ProjectSchemaSite,
        layout: &novarocks_local_program::StaticLayout,
        body: B,
    ) -> ExecutionResult<crate::exec::chunk::ChunkSchemaRef>
    where
        B: FnOnce() -> Result<crate::exec::chunk::ChunkSchemaRef, String>,
    {
        use crate::runtime::preparation_metadata::ProjectSchemaSite;
        assert!(Arc::ptr_eq(program, self.program));
        let node = match site {
            ProjectSchemaSite::Project(id) => {
                let node = &program.graph().nodes()[id.index()];
                assert!(matches!(
                    node.kind(),
                    novarocks_local_program::ProgramNodeKind::Project { .. }
                ));
                node
            }
            ProjectSchemaSite::FinalResult => {
                &program.graph().nodes()[program.graph().root().index()]
            }
        };
        assert!(std::ptr::eq(layout, node.output_layout()));
        self.sites.push(site);
        if let Some((at, cause)) = &self.refusal
            && self.sites.len() == *at
        {
            return Err(cause.clone().into());
        }
        self.body_calls += 1;
        body().map_err(Into::into)
    }
}

#[test]
fn project_metadata_borrow_reaches_nested_projects_before_running() {
    use crate::runtime::preparation_metadata::ProjectSchemaSite;
    let program = program(SeedMode::Input, false);
    let submission = CompiledFragmentSubmission::try_new(
        Arc::clone(&program),
        CompiledScanSources::new(),
        instance(
            UniqueId::new(231, 232),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .unwrap();
    let session = Arc::new(CollectingSession::default());
    let context = FragmentPrepareContext {
        result_writer: Arc::new(CollectingWriter {
            session: Arc::clone(&session),
        }),
        ..FragmentPrepareContext::default()
    };
    let mut host = RecordingProjectSchemaHost {
        program: &program,
        sites: Vec::new(),
        body_calls: 0,
        refusal: None,
    };
    let handle =
        prepare_compiled_fragment_with_metadata_host(submission, context, &mut host).unwrap();
    let expected: Vec<_> = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter(|(_, node)| {
            matches!(
                node.kind(),
                novarocks_local_program::ProgramNodeKind::Project { .. }
            )
        })
        .map(|(index, _)| {
            ProjectSchemaSite::Project(novarocks_local_program::ProgramNodeId::new(index))
        })
        .collect();
    assert_eq!(host.sites.len(), expected.len());
    for site in expected {
        assert_eq!(
            host.sites.iter().filter(|&&actual| actual == site).count(),
            1
        );
    }
    assert_eq!(host.body_calls, host.sites.len());
    assert!(session.chunks.lock().unwrap().is_empty());
    // A caller-local host can be destroyed before any driver is scheduled.
    drop(host);
    assert!(matches!(
        handle.start().join().outcome(),
        FragmentOutcome::Succeeded
    ));
    let chunks = session.chunks.lock().unwrap();
    assert_eq!(chunks.iter().map(Chunk::len).sum::<usize>(), 1);
    let batch = &chunks.iter().find(|chunk| chunk.len() == 1).unwrap().batch;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        SEED_42_FIRST
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
}

#[test]
fn project_metadata_refusal_keeps_nominal_cause_through_the_original_prepare_chain() {
    use crate::runtime::{
        preparation_memory::{PreparationMemoryRefusal, PreparationMemoryStop},
        preparation_metadata::PreparationMetadataFailure,
    };
    use novarocks_execution_contract::task_execution::status::AbortCause;
    let causes = [
        PreparationMetadataFailure::Host(PreparationMemoryRefusal::Stopped(
            PreparationMemoryStop::Abort(AbortCause::LeaseExpired),
        )),
        PreparationMetadataFailure::Host(PreparationMemoryRefusal::Capacity(
            novarocks_memory::CapacityError::Invalid {
                detail: "original host capacity refusal",
            },
        )),
        PreparationMetadataFailure::Request(
            novarocks_type_contract::MetadataRequestError::SourceModel("original source refusal"),
        ),
    ];
    for cause in causes {
        for at in [1, 2] {
            let program = program(SeedMode::Input, false);
            let submission = CompiledFragmentSubmission::try_new(
                Arc::clone(&program),
                CompiledScanSources::new(),
                instance(
                    UniqueId::new(233, 234),
                    1,
                    ExchangeInputAssignments::default(),
                ),
            )
            .unwrap();
            let session = Arc::new(CollectingSession::default());
            let context = FragmentPrepareContext {
                result_writer: Arc::new(CollectingWriter {
                    session: Arc::clone(&session),
                }),
                ..FragmentPrepareContext::default()
            };
            let mut host = RecordingProjectSchemaHost {
                program: &program,
                sites: Vec::new(),
                body_calls: 0,
                refusal: Some((at, cause.clone())),
            };
            let error = refused(prepare_compiled_fragment_with_metadata_host(
                submission, context, &mut host,
            ));
            assert_eq!(error.stage(), FragmentLaunchStage::BuildPipelines);
            assert_eq!(error.kind(), FragmentLaunchErrorKind::PipelineBuild);
            assert_eq!(
                error.cause().cause(),
                &ExecutionFailureCause::PreparationMetadata(cause.clone())
            );
            assert_eq!(host.sites.len(), at);
            assert_eq!(host.body_calls, at - 1);
            assert!(session.chunks.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn project_metadata_borrow_reaches_the_actual_final_result_boundary() {
    use crate::runtime::preparation_metadata::{PreparationMetadataFailure, ProjectSchemaSite};
    for project in [None, Some(false), Some(true)] {
        for refuse_final in [false, true] {
            let program = crate::exec::pipeline::builder::compiled_root_result_fixture(project);
            let submission = CompiledFragmentSubmission::try_new(
                Arc::clone(&program),
                CompiledScanSources::new(),
                instance(
                    UniqueId::new(235, 236),
                    1,
                    ExchangeInputAssignments::default(),
                ),
            )
            .unwrap();
            let context = super::super::compiled_root_result_context_fixture();
            let cause = PreparationMetadataFailure::Request(
                novarocks_type_contract::MetadataRequestError::Arithmetic,
            );
            let expected = 1 + usize::from(project.is_some());
            let mut host = RecordingProjectSchemaHost {
                program: &program,
                sites: Vec::new(),
                body_calls: 0,
                refusal: refuse_final.then(|| (expected, cause.clone())),
            };
            let result =
                prepare_compiled_fragment_with_metadata_host(submission, context, &mut host);
            assert_eq!(host.sites.len(), expected);
            assert_eq!(host.sites.last(), Some(&ProjectSchemaSite::FinalResult));
            assert_eq!(host.body_calls, expected - usize::from(refuse_final));
            if refuse_final {
                assert_eq!(
                    refused(result).cause().cause(),
                    &ExecutionFailureCause::PreparationMetadata(cause)
                );
            } else {
                // Original RootResult production is already covered by its own
                // oracle; this probe ends before scheduling its producer.
                let dormant = result.unwrap();
                drop(host);
                drop(dormant);
            }
        }
    }
}
