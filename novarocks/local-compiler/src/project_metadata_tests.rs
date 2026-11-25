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

//! Pure callback contracts; these do not claim a real memory Grant.
use super::*;
use crate::{
    ProjectMetadataFailure, ProjectMetadataOutput, ProjectMetadataScope, ProjectOutputRequestFacts,
};
use novarocks_local_program::LocalProgram;
use novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Refusal;
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("explicit Project host refusal")
    }
}
impl std::error::Error for Refusal {}
impl novarocks_failure_contract::FailureProjection for Refusal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(
                domain::LOCAL_COMPILER,
                std::num::NonZeroU16::new(1000).unwrap(),
            ),
        )
    }
}

struct Host<'a> {
    calls: usize,
    requests: Option<ProjectOutputRequestFacts>,
    stop: Option<&'a AtomicBool>,
    reject: bool,
    body_failure: bool,
}
impl ProjectMetadataScope for Host<'_> {
    type HostError = Refusal;
    fn materialize<C, B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<Refusal, C>>
    where
        C: From<FragmentCompileError>,
        B: FnOnce() -> Result<ProjectMetadataOutput, C>,
    {
        self.calls += 1;
        self.requests = Some(*facts);
        if self.reject {
            if let Some(stop) = self.stop {
                stop.store(true, Ordering::SeqCst);
            }
            return Err(ProjectMetadataFailure::Host(Refusal));
        }
        let output = body().map_err(ProjectMetadataFailure::Body)?;
        if self.body_failure {
            drop(output);
            if let Some(stop) = self.stop {
                stop.store(true, Ordering::SeqCst);
            }
            return Err(ProjectMetadataFailure::Body(
                FragmentCompileError::Invalid("original body error").into(),
            ));
        }
        Ok(output)
    }
}
struct FooterControl<'a> {
    stop: &'a AtomicBool,
    stopped_checks: AtomicUsize,
}
impl PureCompileControl for FooterControl<'_> {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        if self.stop.load(Ordering::SeqCst) {
            self.stopped_checks.fetch_add(1, Ordering::SeqCst);
            Err(CompileControlError::Cancelled)
        } else {
            Ok(())
        }
    }
}
fn positive_package(functions: &PureEngineFunctionCatalog, count: usize) -> Arc<FragmentPackage> {
    let source = Arc::try_unwrap(package(functions, count)).unwrap();
    Arc::new(source.with_original_metadata_namespace(
        MaterializedFieldNamespace::from_original_loans(Arc::from([])),
    ))
}
#[test]
fn project_metadata_hosted_original_output_matches_direct_names_slots_and_types() {
    let functions = rng_subset();
    let source = positive_package(&functions, 3);
    let direct = compile_fragment(
        providers(Arc::clone(&source)),
        &functions,
        options(1),
        &FixtureControl,
    )
    .unwrap();
    let mut host = Host {
        calls: 0,
        requests: None,
        stop: None,
        reject: false,
        body_failure: false,
    };
    let hosted = compile_fragment_with_project_metadata_host(
        providers(source),
        &functions,
        options(1),
        &FixtureControl,
        &mut host,
    )
    .unwrap();
    assert_eq!(host.calls, 1);
    let facts = host.requests.unwrap();
    assert_eq!(facts.node, NodeId::new(41));
    // Typed projection shares immutable child bindings instead of cloning
    // retired recursive metadata tables; its actual scaffolds remain covered.
    assert!(facts.requests.allocation_request_bytes_upper_bound > 0);
    assert!(facts.requests.allocation_requests_upper_bound >= 3);
    assert_eq!(direct.graph().nodes().len(), hosted.graph().nodes().len());
    for (left, right) in direct.graph().nodes().iter().zip(hosted.graph().nodes()) {
        assert_eq!(left.output_layout().slots(), right.output_layout().slots());
        assert!(novarocks_type_contract::arrow_schemas_exact(
            left.output_layout().schema(),
            right.output_layout().schema()
        ));
    }
    let project = hosted
        .graph()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), ProgramNodeKind::Project { .. }))
        .unwrap();
    let names: Vec<_> = project
        .output_layout()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["sample_0", "sample_1", "sample_2"]);
}
#[test]
fn project_metadata_plain_direct_source_does_not_compute_or_call_host() {
    let functions = rng_subset();
    let mut host = Host {
        calls: 0,
        requests: None,
        stop: None,
        reject: true,
        body_failure: false,
    };
    compile_fragment_with_project_metadata_host(
        providers(package(&functions, 2)),
        &functions,
        options(1),
        &FixtureControl,
        &mut host,
    )
    .unwrap();
    assert_eq!(host.calls, 0);
    assert!(host.requests.is_none());
}
#[test]
fn project_metadata_nominal_host_refusal_bypasses_later_compiler_footer() {
    let functions = rng_subset();
    let stop = AtomicBool::new(false);
    let control = FooterControl {
        stop: &stop,
        stopped_checks: AtomicUsize::new(0),
    };
    let mut host = Host {
        calls: 0,
        requests: None,
        stop: Some(&stop),
        reject: true,
        body_failure: false,
    };
    let result = compile_fragment_with_project_metadata_host(
        providers(positive_package(&functions, 1)),
        &functions,
        options(1),
        &control,
        &mut host,
    );
    let Err(FragmentCompileError::ProjectMetadataHost { error }) = result else {
        panic!("nominal host refusal must remain primary")
    };
    assert!(error.downcast_ref::<Refusal>().is_some());
    assert_eq!(host.calls, 1);
    assert_eq!(control.stopped_checks.load(Ordering::SeqCst), 0);
}
#[test]
fn project_metadata_original_body_failure_retains_original_compiler_footer_priority() {
    let functions = rng_subset();
    let stop = AtomicBool::new(false);
    let control = FooterControl {
        stop: &stop,
        stopped_checks: AtomicUsize::new(0),
    };
    let mut host = Host {
        calls: 0,
        requests: None,
        stop: Some(&stop),
        reject: false,
        body_failure: true,
    };
    let result = compile_fragment_with_project_metadata_host(
        providers(positive_package(&functions, 1)),
        &functions,
        options(1),
        &control,
        &mut host,
    );
    assert!(matches!(
        result,
        Err(FragmentCompileError::Control(
            CompileControlError::Cancelled
        ))
    ));
    assert_eq!(host.calls, 1);
    assert!(control.stopped_checks.load(Ordering::SeqCst) > 0);
}

#[derive(Default)]
struct RecordingHost(Vec<ProjectOutputRequestFacts>);
impl ProjectMetadataScope for RecordingHost {
    type HostError = Refusal;
    fn materialize<C, B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<Refusal, C>>
    where
        C: From<FragmentCompileError>,
        B: FnOnce() -> Result<ProjectMetadataOutput, C>,
    {
        self.0.push(*facts);
        body().map_err(ProjectMetadataFailure::Body)
    }
}
fn positive_case(
    functions: &PureEngineFunctionCatalog,
    count: usize,
    case: PackageCase,
) -> Arc<FragmentPackage> {
    let source = Arc::try_unwrap(package_with_case(functions, count, case)).unwrap();
    Arc::new(source.with_original_metadata_namespace(
        MaterializedFieldNamespace::from_original_loans(Arc::from([])),
    ))
}
fn assert_graph_outputs_match(left: &LocalProgram, right: &LocalProgram) {
    assert_eq!(left.graph().nodes().len(), right.graph().nodes().len());
    for (left, right) in left.graph().nodes().iter().zip(right.graph().nodes()) {
        assert_eq!(left.output_layout().slots(), right.output_layout().slots());
        assert!(novarocks_type_contract::arrow_schemas_exact(
            left.output_layout().schema(),
            right.output_layout().schema()
        ));
        if let ProgramNodeKind::Project {
            exprs: left_exprs,
            expr_slot_ids: left_slots,
            ..
        } = left.kind()
        {
            let ProgramNodeKind::Project {
                exprs: right_exprs,
                expr_slot_ids: right_slots,
                ..
            } = right.kind()
            else {
                panic!("same physical Project")
            };
            assert_eq!(left_exprs, right_exprs);
            assert_eq!(left_slots, right_slots);
        }
    }
}
#[test]
fn project_metadata_empty_and_wide_hosted_outputs_keep_original_direct_contract() {
    let functions = rng_subset();
    for (count, case) in [(0, PackageCase::EmptyProject), (320, PackageCase::Ordinary)] {
        let source = positive_case(&functions, count, case);
        let direct = compile_fragment(
            providers(source.clone()),
            &functions,
            options(1),
            &FixtureControl,
        )
        .unwrap();
        let mut host = RecordingHost::default();
        let hosted = compile_fragment_with_project_metadata_host(
            providers(source.clone()),
            &functions,
            options(1),
            &FixtureControl,
            &mut host,
        )
        .unwrap();
        assert_graph_outputs_match(&direct, &hosted);
        assert_eq!(host.0.len(), 1);
        // Empty Arc headers remain real nonzero requests even with zero roots.
        assert!(host.0[0].requests.allocation_requests_upper_bound > 0);
        assert!(host.0[0].requests.allocation_request_bytes_upper_bound > 0);
        let mut reject = Host {
            calls: 0,
            requests: None,
            stop: None,
            reject: true,
            body_failure: false,
        };
        assert!(matches!(
            compile_fragment_with_project_metadata_host(
                providers(source),
                &functions,
                options(1),
                &FixtureControl,
                &mut reject
            ),
            Err(FragmentCompileError::ProjectMetadataHost { .. })
        ));
        assert_eq!(reject.calls, 1);
    }
}
#[test]
fn project_metadata_non_result_labels_keep_original_local_format_and_final_labels() {
    let functions = rng_subset();
    let source = positive_case(&functions, 3, PackageCase::NestedProject);
    let direct = compile_fragment(
        providers(source.clone()),
        &functions,
        options(1),
        &FixtureControl,
    )
    .unwrap();
    let mut host = RecordingHost::default();
    let hosted = compile_fragment_with_project_metadata_host(
        providers(source),
        &functions,
        options(1),
        &FixtureControl,
        &mut host,
    )
    .unwrap();
    assert_graph_outputs_match(&direct, &hosted);
    assert_eq!(
        host.0
            .iter()
            .map(|facts| facts.node.get())
            .collect::<Vec<_>>(),
        [41, 42]
    );
    let names: Vec<Vec<_>> = hosted
        .graph()
        .nodes()
        .iter()
        .filter(|node| matches!(node.kind(), ProgramNodeKind::Project { .. }))
        .map(|node| {
            node.output_layout()
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        })
        .collect();
    assert_eq!(
        names,
        [
            vec!["local_2_0", "local_2_1", "local_2_2"],
            vec!["sample_0"]
        ]
    );
}
#[test]
fn project_metadata_repeated_produced_value_keeps_original_pre_scope_refusal() {
    let functions = rng_subset();
    let source = positive_case(&functions, 2, PackageCase::DuplicateProducedValue);
    let mut host = RecordingHost::default();
    for hosted in [false, true] {
        let result = if hosted {
            compile_fragment_with_project_metadata_host(
                providers(source.clone()),
                &functions,
                options(1),
                &FixtureControl,
                &mut host,
            )
        } else {
            compile_fragment(
                providers(source.clone()),
                &functions,
                options(1),
                &FixtureControl,
            )
        };
        assert!(matches!(
            result,
            Err(FragmentCompileError::Invalid(
                "independent project roots share a produced value"
            ))
        ));
    }
    assert!(host.0.is_empty());
}
fn recipe(
    value: &FunctionValueType,
    count: usize,
) -> novarocks_type_contract::CompleteMetadataRequestFacts {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        &FixtureControl,
        CompilePhase::LowerProgram,
    )
    .unwrap();
    let facts = crate::project_metadata::project_output_requests(
        NodeId::new(41),
        0,
        count,
        std::iter::repeat_n((value, Some("same")), count).map(Ok),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    facts.requests
}
#[test]
fn project_metadata_recipe_counts_typed_occurrences_without_external_metadata_or_dictionary_boxes()
{
    let physical = FunctionValueType::new(DataType::Utf8, true);
    let physical_binary = FunctionValueType::new(DataType::LargeBinary, true);
    let dictionary = FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    );
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let child = |metadata: std::collections::HashMap<String, String>| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(
                    arrow_schema::Field::new("child", DataType::Int64, true)
                        .with_metadata(metadata),
                )]
                .into(),
            ),
            true,
        )
    };
    let plain_child = child(Default::default());
    let external_child = child(std::collections::HashMap::from([(
        "source-key".into(),
        "source-value".into(),
    )]));
    for count in [0, 1, 3, 128] {
        let physical = recipe(&physical, count);
        assert_eq!(
            recipe(&dictionary, count),
            physical,
            "runtime dictionary encoding does not recreate logical layout boxes"
        );
        assert_eq!(
            recipe(&json, count),
            recipe(&physical_binary, count),
            "complete nominal binding is shared without an execution metadata map"
        );
        assert_eq!(
            recipe(&plain_child, count),
            recipe(&external_child, count),
            "external metadata belongs to the source owner, not the execution projection"
        );
        if count > 0 {
            assert!(physical.allocation_requests_upper_bound > 0);
        }
    }
    assert!(
        recipe(&physical, 128).allocation_request_bytes_upper_bound
            > recipe(&physical, 3).allocation_request_bytes_upper_bound
    );
}
