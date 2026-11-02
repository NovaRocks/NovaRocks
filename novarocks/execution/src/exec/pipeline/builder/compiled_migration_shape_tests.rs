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

//! Independent shape and per-call phase assertions after construction-tree removal.

use std::sync::Arc;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::PureKernelAbi;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::*;
use novarocks_types::UniqueId;

use super::aggregate_fixture::{
    CallSpec, add_aggregate, aggregate_catalog, bind, finish, packages, result_sink, try_run,
};
use super::family_fixture::{FixtureControl, constant_policy, int64, int64_rows};
use super::{PipelineGraph, PipelinePlan, build_compiled_pipeline_graph};
use crate::exec::node::scan::ScanOp;
use crate::exec::operators::ResultSinkHandle;
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::dependency::DependencyManager;
use crate::runtime::fragment::CompiledWriterBindings;
use crate::runtime::fragment::scan::compiled_fixture::{FixtureScanOp, SCAN_NODE, scan_program};
use crate::runtime::runtime_state::RuntimeErrorState;

fn scan_graph(dop: usize, root_dop: Option<i32>) -> PipelineGraph {
    let program = scan_program(dop, false);
    let mut scans = ScanBindings::default();
    scans.insert(
        SCAN_NODE,
        FixtureScanOp::new(Vec::new(), false) as Arc<dyn ScanOp>,
    );
    build_compiled_pipeline_graph(
        &program,
        ExchangeBindings::default(),
        scans,
        CompiledWriterBindings::default(),
        None,
        DependencyManager::new(),
        dop as i32,
        root_dop,
        crate::runtime::execution_runtime::test_execution_function_set(),
        Arc::new(RuntimeErrorState::default()),
    )
    .expect("the checked provider scan builds")
}

fn begins(pipeline: &PipelinePlan, prefix: &str) -> bool {
    pipeline
        .factories
        .first()
        .is_some_and(|factory| factory.name().starts_with(prefix))
}
fn ends(pipeline: &PipelinePlan, prefix: &str) -> bool {
    pipeline
        .factories
        .last()
        .is_some_and(|factory| factory.name().starts_with(prefix))
}

#[test]
fn compiled_single_scan_stream_hands_off_from_one_producer_to_the_frozen_dop() {
    let graph = scan_graph(4, None);
    assert_eq!(graph.pipelines.len(), 2);
    let producers = graph
        .pipelines
        .iter()
        .filter(|p| ends(p, "LOCAL_EXCHANGE_SINK"))
        .collect::<Vec<_>>();
    let consumers = graph
        .pipelines
        .iter()
        .filter(|p| begins(p, "LOCAL_EXCHANGE_SOURCE"))
        .collect::<Vec<_>>();
    assert_eq!(producers.len(), 1);
    assert_eq!(consumers.len(), 1);
    assert_eq!(
        producers[0].dop, 1,
        "one driver owns the physical scan stream"
    );
    assert!(
        !producers[0].needs_sink,
        "the handoff already terminates its producer"
    );
    assert_eq!(
        consumers[0].dop, 4,
        "the downstream pipeline keeps the frozen width"
    );
    assert_eq!(consumers[0].id, graph.root_id);
    assert!(consumers[0].needs_sink);
}

#[test]
fn compiled_single_driver_scan_adds_no_local_handoff() {
    let graph = scan_graph(1, None);
    assert_eq!(graph.pipelines.len(), 1);
    let root = &graph.pipelines[0];
    assert_eq!(root.id, graph.root_id);
    assert_eq!(root.dop, 1);
    assert!(root.needs_sink);
    assert!(
        root.factories
            .iter()
            .all(|f| !f.name().starts_with("LOCAL_EXCHANGE_"))
    );
}

#[test]
fn compiled_scan_root_override_gathers_the_parallel_consumers_to_one_terminal_driver() {
    let graph = scan_graph(4, Some(1));
    assert_eq!(
        graph.pipelines.len(),
        3,
        "scan, parallel handoff and final gather"
    );
    let root = graph
        .pipelines
        .iter()
        .find(|p| p.id == graph.root_id)
        .unwrap();
    assert_eq!(root.dop, 1);
    assert!(root.needs_sink);
    assert!(begins(root, "LOCAL_EXCHANGE_SOURCE"));
    let handoff = graph.pipelines.iter().find(|p| p.dop == 4).unwrap();
    assert!(begins(handoff, "LOCAL_EXCHANGE_SOURCE"));
    assert!(ends(handoff, "LOCAL_EXCHANGE_SINK"));
    assert!(!handoff.needs_sink);
    let scan = graph
        .pipelines
        .iter()
        .find(|p| p.id != root.id && p.id != handoff.id)
        .unwrap();
    assert_eq!(scan.dop, 1);
    assert!(ends(scan, "LOCAL_EXCHANGE_SINK"));
    assert!(!scan.needs_sink);
}

fn mixed_phase_program() -> Arc<LocalProgram> {
    let catalog = aggregate_catalog(&[
        ("count", PureKernelAbi::AggregateWindowV1),
        ("sum", PureKernelAbi::AggregateWindowV1),
    ]);
    let logical = int64(false);
    let sum = bind(&catalog, "sum", std::slice::from_ref(&logical));
    let count = bind(&catalog, "count", &[sum.intermediate_type()]);
    let sequence = AggregateSequenceId::new(71);
    let fragment = FragmentId::new(71);
    let mut builder = FragmentBuilder::new(fragment);
    let mut inputs = Vec::new();
    let mut mappings = Vec::new();
    // Each producer emits a real exact SUM state. The UnionAll preserves
    // both state identities, so the final call has a complete checked trace.
    for (ordinal, value) in [30_i64, 5].into_iter().enumerate() {
        let source = builder.reserve_node_id().unwrap();
        let columns = super::aggregate_fixture::values(
            &mut builder,
            source,
            std::slice::from_ref(&logical),
            &[vec![LiteralValue::Int64(value)]],
        );
        let (partial, output) = add_aggregate(
            &mut builder,
            source,
            &[],
            &[CallSpec {
                bound: &sum,
                phase: AggregatePhase::Partial { sequence },
                id: AggregateCallId::new(ordinal as u32),
                arguments: columns,
                distinct: false,
            }],
            AggregateGrouping::Partial,
        );
        inputs.push(partial);
        mappings.push(output.into_boxed_slice());
    }
    let union = builder.reserve_node_id().unwrap();
    let state = builder
        .add_value(
            sum.intermediate_type(),
            ValueOrigin::NodeOutput {
                node: union,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_row_consuming(
            union,
            inputs.into_boxed_slice(),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::from([state]),
            NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: mappings.into_boxed_slice(),
            },
        )
        .unwrap();
    let (root, columns) = add_aggregate(
        &mut builder,
        union,
        &[],
        &[
            CallSpec {
                bound: &count,
                phase: AggregatePhase::Single,
                id: AggregateCallId::new(2),
                arguments: vec![state],
                distinct: false,
            },
            CallSpec {
                bound: &sum,
                phase: AggregatePhase::Final { sequence },
                id: AggregateCallId::new(3),
                arguments: vec![state],
                distinct: false,
            },
        ],
        AggregateGrouping::Complete,
    );
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([71; 16]).unwrap());
    plan.add_fragment(finish(builder, root, FragmentSink::Result, 1))
        .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment,
        output: OutputPort {
            node: root,
            columns: columns.clone().into_boxed_slice(),
        },
        fields: columns
            .into_iter()
            .zip([count.result_type(), sum.result_type()])
            .enumerate()
            .map(|(ordinal, (value, ty))| ResultField {
                domain: crate::test_result_domain::result_value_domain(&ty),
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value,
                ty,
            })
            .collect(),
    })
    .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .expect("the complete partial/union/final trace is valid");
    let package = packages(&plan, &catalog).remove(&fragment).unwrap();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(Arc::new(package), &providers, &FixtureControl)
        .expect("provider-free package admission");
    Arc::new(
        compile_fragment(
            validated,
            &catalog,
            LocalCompileOptions {
                pipeline_dop: std::num::NonZeroUsize::MIN,
                root_sink_dop: Some(std::num::NonZeroUsize::MIN),
                kernel_abi: KernelAbiVersion::CURRENT,
                constants: constant_policy(),
            },
            &FixtureControl,
        )
        .expect("the typed compiler admits two finalizing calls with distinct input phases"),
    )
}

#[test]
fn compiled_mixed_update_and_merge_calls_keep_their_own_phase_and_exact_result() {
    let program = mixed_phase_program();
    let ProgramNodeKind::Aggregate {
        functions,
        need_finalize,
        input_is_intermediate,
        ..
    } = program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("the root is the authored mixed aggregate")
    };
    assert!(*need_finalize);
    assert!(
        !*input_is_intermediate,
        "the mixed node cannot classify every call as merging"
    );
    assert_eq!(functions.len(), 2);
    assert_eq!(functions[0].name.as_ref(), "builtin.aggregate/count/v1");
    assert!(
        !functions[0].input_is_intermediate,
        "COUNT updates from its logical argument"
    );
    assert_eq!(functions[1].name.as_ref(), "builtin.aggregate/sum/v1");
    assert!(
        functions[1].input_is_intermediate,
        "SUM reads the exact state of its original logical INT64 binding"
    );
    let output = ResultSinkHandle::new();
    let instance = UniqueId::new(71, 72);
    try_run(
        &program,
        result_sink(&program, instance, &output),
        ExchangeBindings::default(),
        instance,
    )
    .unwrap();
    assert_eq!(
        int64_rows(&output.take_chunks()),
        vec![vec![Some(2), Some(35)]]
    );
}
