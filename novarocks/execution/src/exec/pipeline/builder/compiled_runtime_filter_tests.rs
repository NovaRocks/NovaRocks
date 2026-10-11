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

//! Compiled runtime filters: a BlockingSnapshot membership consumer at a
//! compiled scan source and membership producers at a compiled hash join's
//! build keys.
//!
//! The local compiler does not author runtime-filter facts yet, so every
//! program here is a compiled fixture whose checked chain is rebuilt over the
//! same graph with runtime-filter sites, their binding requirements and their
//! key roots added. The Task's runtime-filter session is a test double that
//! records what the operators ask of it.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;
use novarocks_local_program::{
    BindingRequirement, BindingRequirements, CompiledProgramFacts, FilterConsumerActivation,
    FilterConsumerAtExpr, FilterLateApplyGranularity, FilterNullOrder, FilterNullSemantics,
    FilterOrderKey, FilterProducerAtExpr, FilterProducerKind, FilterReduction, FilterSortDirection,
    ImmutableExpressions, LocalOperatorId, LocalOperatorOrigin, LocalOperatorProvenance,
    LocalProgram, LocalProgramGraph, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramControlFlow, ProgramExprId, ProgramExpressionArena, ProgramExpressionRootSite,
    ProgramExpressionUse, ProgramLexicalBindings, ProgramLexicalSource, ProgramNode,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramResolvedCalls,
    ProgramRootControlBindings, ProgramRootUseBinding, ProgramSlotBinding, ProgramTypedChannels,
    ProgramTypedExpressions, ProgramUseRef, StaticExprKind, StaticExprNode, StaticFilterConsumer,
    StaticFilterContract, StaticFilterProducer,
};
use novarocks_physical_plan::{JoinKind, JoinSide};
use novarocks_spi::connector::ConnectorScalarValue;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, EvaluationDemand, ExpressionEffectContext, ExpressionUseId,
    FunctionValueType, PureCompileControl,
};

use super::aggregate_fixture::result_sink;
use super::family_fixture::int64_rows;
use super::join_tests::{
    Family, LEFT_ROWS, Overflow, RIGHT_ROWS, Rows, SINGLE, SINGLE_FINST, Spec, compile, oracle,
    overflow_plan, packages, single_plan, sorted,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
use crate::exec::node::scan::{ScanNode, ScanOp};
use crate::exec::operators::scan::StreamScanSourceFactory;
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::executor::{
    PreparedPipelineExecution, prepare_compiled_program_pipeline_execution,
    prepare_compiled_program_pipeline_execution_with_profiler,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::ExecutionResult;
use crate::runtime::fragment::io::{FragmentEvent, FragmentEventSink, NoopFragmentEventSink};
use crate::runtime::fragment::scan::compiled_fixture::{
    FixtureScanOp, SCAN_NODE, rows, scan_chunk, scan_program,
};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter as execution;
use novarocks_types::SlotId;

struct NoControl;
impl PureCompileControl for NoControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

/// One added key root: its site, and the existing leaf root whose control,
/// definition and lexical source it shares, unless `definition` and `source`
/// name its own definition and the slot of its own input port.
struct AddedRoot {
    site: ProgramExpressionRootSite,
    like: ProgramExpressionRootSite,
    definition: Option<ProgramExprId>,
    source: Option<ProgramLexicalSource>,
}

/// What a rebuilt program adds to the compiled one.
#[derive(Default)]
struct Additions {
    /// A separately prepared, complete scalar occurrence over the SAME Scan port.
    scalar_root: Option<(
        ProgramExpressionRootSite,
        ProgramExpressionRootSite,
        novarocks_functions::PureEngineFunctionCatalog,
    )>,
    /// A new root node, when an added node is the new root.
    root: Option<ProgramNodeId>,
    /// Definitions appended to the main expression arena, with their types.
    definitions: Vec<(StaticExprNode, FunctionValueType)>,
    requirements: Vec<BindingRequirement>,
    roots: Vec<AddedRoot>,
    channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    operators: Vec<LocalOperatorProvenance>,
}

/// `program` with its checked chain rebuilt over `nodes` and `additions`:
/// extra requirements, one eager key use per added root, the channels and
/// operators of added nodes. Every other checked fact, provider address and
/// provenance is carried over as is.
fn rebuild(
    program: &LocalProgram,
    nodes: Vec<ProgramNode>,
    additions: Additions,
) -> Result<Arc<LocalProgram>, String> {
    let Additions {
        scalar_root,
        root,
        definitions: added_definitions,
        requirements,
        roots,
        channels: added_channels,
        operators: added_operators,
    } = additions;
    let control = NoControl;
    let graph = program.graph();
    let mut entries = graph.requirements().entries().to_vec();
    entries.extend(requirements);
    let main = graph.expressions();
    let arena = if added_definitions.is_empty() {
        Arc::clone(main)
    } else {
        Arc::new(
            ImmutableExpressions::try_new(
                main.nodes()
                    .iter()
                    .cloned()
                    .chain(added_definitions.iter().map(|(node, _)| node.clone()))
                    .collect(),
                main.allow_throw_exception(),
                main.query_global_dicts().clone(),
                main.session_time_zone().map(Arc::from),
            )
            .map_err(|error| format!("{error:?}"))?,
        )
    };
    let rebuilt = LocalProgramGraph::try_new_with_sink(
        nodes,
        root.unwrap_or(graph.root()),
        Arc::clone(&arena),
        graph.profile(),
        BindingRequirements::try_new(entries).map_err(|error| format!("{error:?}"))?,
        graph.sink().cloned(),
    )
    .map_err(|error| error.to_string())?;
    let checked = program.checked();
    let channels = checked.channels();
    let expressions = channels.expressions();
    let calls = expressions.resolved_calls();
    let snapshot = calls.snapshot();
    let mut flows = snapshot
        .flows()
        .iter()
        .map(|(arena, flow)| {
            (
                *arena,
                (
                    flow.domains().values().cloned().collect::<Vec<_>>(),
                    flow.uses().values().cloned().collect::<Vec<_>>(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut bindings = snapshot
        .bindings()
        .iter()
        .map(|(site, use_id)| ProgramRootUseBinding {
            site: *site,
            use_id: *use_id,
        })
        .collect::<Vec<_>>();
    let mut slots = checked
        .slots()
        .iter()
        .map(|(occurrence, source)| ProgramSlotBinding {
            occurrence: *occurrence,
            source: *source,
        })
        .collect::<Vec<_>>();
    for root in &roots {
        let arena = root.site.arena();
        let like_use = *snapshot
            .bindings()
            .get(&root.like)
            .ok_or("the template root is bound")?;
        let like = snapshot.flows()[&arena].uses()[&like_use].clone();
        if !like.arguments.is_empty() {
            return Err("a key root template must be a leaf".to_string());
        }
        let (_, uses) = flows.get_mut(&arena).ok_or("the root arena has a flow")?;
        let next = uses
            .iter()
            .map(|existing| existing.context.use_id.get())
            .max()
            .map_or(0, |max| max + 1);
        let use_id = ExpressionUseId::new(next);
        uses.push(ProgramExpressionUse {
            context: ExpressionEffectContext {
                use_id,
                domain: like.context.domain,
                demand: EvaluationDemand::Value,
            },
            definition: root.definition.unwrap_or(like.definition),
            control: like.control,
            arguments: Box::default(),
        });
        bindings.push(ProgramRootUseBinding {
            site: root.site,
            use_id,
        });
        let source = match root.source {
            Some(source) => source,
            None => *checked
                .slots()
                .get(&ProgramUseRef {
                    arena,
                    use_id: like_use,
                })
                .ok_or("the template root reads a slot")?,
        };
        slots.push(ProgramSlotBinding {
            occurrence: ProgramUseRef { arena, use_id },
            source,
        });
    }
    let mut prepared_calls = calls
        .calls()
        .iter()
        .map(|(site, call)| (*site, call.specialization().clone()))
        .collect::<Vec<_>>();
    if let Some((new_site, like_site, functions)) = scalar_root {
        use novarocks_functions::{
            CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingRequest,
            FunctionKind, PureCallPreparation, ScopedExpressionEffects,
        };
        let scope = new_site.arena();
        assert_eq!(scope, like_site.arena());
        let old_use = snapshot.bindings()[&like_site];
        let original = &snapshot.flows()[&scope].uses()[&old_use];
        assert!(matches!(
            arena.nodes()[original.definition.index()].kind(),
            StaticExprKind::BoundCall { .. }
        ));
        let old_call =
            &calls.calls()[&novarocks_local_program::ProgramCallSite::Expression(ProgramUseRef {
                arena: scope,
                use_id: old_use,
            })];
        let contract = old_call.call_contract();
        let (_, uses) = flows.get_mut(&scope).unwrap();
        let mut next = uses.iter().map(|v| v.context.use_id.get()).max().unwrap() + 1;
        let mut arguments = Vec::new();
        for old_child in &original.arguments {
            let old = &snapshot.flows()[&scope].uses()[old_child];
            assert!(
                old.arguments.is_empty(),
                "this fixture requires two original slot children"
            );
            assert!(matches!(
                arena.nodes()[old.definition.index()].kind(),
                StaticExprKind::SlotId(_)
            ));
            let use_id = ExpressionUseId::new(next);
            next += 1;
            uses.push(ProgramExpressionUse {
                context: ExpressionEffectContext {
                    use_id,
                    ..old.context
                },
                definition: old.definition,
                control: old.control,
                arguments: Box::default(),
            });
            // Project and the Scan key both read this exact Scan NodeOutput port.
            let source = *checked
                .slots()
                .get(&ProgramUseRef {
                    arena: scope,
                    use_id: *old_child,
                })
                .unwrap();
            assert!(matches!(
                source,
                ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ..
                })
            ));
            slots.push(ProgramSlotBinding {
                occurrence: ProgramUseRef {
                    arena: scope,
                    use_id,
                },
                source,
            });
            arguments.push(use_id);
        }
        assert_eq!(arguments.len(), 2);
        let use_id = ExpressionUseId::new(next);
        let context = ExpressionEffectContext {
            use_id,
            ..original.context
        };
        uses.push(ProgramExpressionUse {
            context,
            definition: original.definition,
            control: original.control,
            arguments: arguments.clone().into_boxed_slice(),
        });
        bindings.push(ProgramRootUseBinding {
            site: new_site,
            use_id,
        });
        let selected = Arc::clone(contract.selected_owner());
        let request = selected
            .argument_types
            .iter()
            .map(|ty| match ty {
                novarocks_type_contract::FunctionArgumentType::Value(value_type) => {
                    FunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant: None,
                    }
                }
                _ => panic!("the original shift has two value channels"),
            })
            .collect::<Vec<_>>();
        let argument_uses = arguments.iter().copied().map(Some).collect::<Vec<_>>();
        let token = functions
            .prepare_frozen(
                CallEffectInput {
                    context,
                    argument_uses: CallArgumentUses::SelectedChannels(&argument_uses),
                    function_id: contract.function_id(),
                    kind: FunctionKind::Scalar,
                    selected: &selected,
                    request: FunctionBindingRequest {
                        arguments: &request,
                        logical_argument_count: request.len(),
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters: contract.parameters(),
                    decimal_overflow_policy: contract.decimal_overflow_policy(),
                    proof_scope: contract.effects().proof_scope,
                },
                selected.clone(),
                contract.effects(),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::primitive(
                        context,
                        novarocks_type_contract::ExpressionEffects::PURE_VALUE,
                    ),
                },
                &control,
            )
            .unwrap();
        let novarocks_functions::PreparedPureKernel::Scalar(kernel) = token.prepared() else {
            panic!("actual ScalarV1 source")
        };
        assert!(kernel.invocation_resource_profile().is_some());
        prepared_calls.push((
            novarocks_local_program::ProgramCallSite::Expression(ProgramUseRef {
                arena: scope,
                use_id,
            }),
            token,
        ));
    }
    let main_definitions = arena.nodes().len();
    let flows = flows
        .into_iter()
        .map(|(arena, (domains, uses))| {
            let definitions = if arena == ProgramExpressionArena::Main {
                main_definitions
            } else {
                snapshot.roots().arenas()[&arena].nodes().len()
            };
            ProgramControlFlow::try_new(domains, uses, definitions, &control)
                .map(|flow| (arena, flow))
                .map_err(|error| format!("{error:?}"))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let snapshot = ProgramRootControlBindings::try_new(rebuilt, flows, bindings, &control)
        .map_err(|error| format!("{error:?}"))?;
    let calls = ProgramResolvedCalls::try_new(snapshot, prepared_calls, &control)
        .map_err(|error| error.to_string())?;
    let typed = ProgramTypedExpressions::try_new(
        calls,
        expressions
            .types()
            .iter()
            .map(|(arena, types)| {
                let mut types = types.to_vec();
                if *arena == ProgramExpressionArena::Main {
                    types.extend(added_definitions.iter().map(|(_, value)| {
                        novarocks_type_contract::FunctionArgumentType::Value(value.clone())
                    }));
                }
                (*arena, types)
            })
            .collect(),
        &control,
    )
    .map_err(|error| error.to_string())?;
    let typed_channels = ProgramTypedChannels::try_new(
        typed,
        channels
            .channels()
            .iter()
            .map(|(site, value)| (*site, value.clone()))
            .chain(added_channels)
            .collect(),
        &control,
    )
    .map_err(|error| error.to_string())?;
    let lexical = ProgramLexicalBindings::try_new(
        typed_channels,
        checked.lambdas().values().cloned().collect(),
        slots,
        &control,
    )
    .map_err(|error| error.to_string())?;
    let operators = program
        .provenance()
        .operators()
        .values()
        .cloned()
        .chain(added_operators)
        .collect::<Vec<_>>();
    let allowed = operators
        .iter()
        .flat_map(|operator| operator.sources.iter().copied())
        .chain(
            graph
                .nodes()
                .iter()
                .flat_map(|node| node.physical_sources().iter().copied()),
        )
        .collect::<BTreeSet<_>>();
    LocalProgram::try_new(
        lexical,
        operators,
        &allowed,
        CompiledProgramFacts {
            writes: program.write_recipes().clone(),
            exchange_inputs: program.exchange_inputs().clone(),
            scan_inputs: program.scan_inputs().clone(),
            aggregates: program.aggregates().clone(),
        },
        &control,
    )
    .map(Arc::new)
    .map_err(|error| error.to_string())
}

/// `program`'s nodes with the node at `id` replaced by `kind`.
fn with_kind(program: &LocalProgram, id: ProgramNodeId, kind: ProgramNodeKind) -> Vec<ProgramNode> {
    program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let kind = if index == id.index() {
                kind.clone()
            } else {
                node.kind().clone()
            };
            ProgramNode::new_local(
                node.local_id()
                    .expect("a compiled node has a local identity"),
                node.physical_sources().to_vec(),
                kind,
                node.output_layout().clone(),
            )
        })
        .collect()
}

fn site(node: ProgramNodeId, role: ProgramNodeExpressionRole) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node { node, role }
}

fn requirement(binding_id: u32) -> BindingRequirement {
    BindingRequirement::RuntimeFilter {
        binding_id: i32::try_from(binding_id).unwrap(),
    }
}

fn membership(data_type: &DataType, null_semantics: FilterNullSemantics) -> StaticFilterContract {
    StaticFilterContract::membership(data_type, null_semantics).expect("membership contract")
}

fn blocking_consumer(binding_id: u32) -> StaticFilterConsumer {
    StaticFilterConsumer::try_new(
        binding_id,
        binding_id,
        FilterConsumerActivation::BlockingSnapshot,
        membership(&DataType::Int64, FilterNullSemantics::NeverMatches),
        FilterReduction::SetUnion,
    )
    .expect("blocking membership consumer")
}

fn membership_producer(
    binding_id: u32,
    null_semantics: FilterNullSemantics,
) -> StaticFilterProducer {
    StaticFilterProducer::try_new(
        binding_id,
        binding_id,
        FilterProducerKind::Membership,
        membership(&DataType::Int64, null_semantics),
        FilterReduction::SetUnion,
    )
    .expect("membership producer")
}

// ---------------------------------------------------------------------------
// Scan consumers.

/// The scan fixture's project publishes `b = v1` then `a = v0`.
const V1_OUTPUT: u32 = 0;
const V0_OUTPUT: u32 = 1;

/// The compiled scan fixture (`SELECT v1 AS b, v0 AS a FROM t`) whose scan
/// consumes `consumers`: each is keyed by the scan column the project
/// publishes at the paired output ordinal.
fn scan_with_consumers(
    dop: usize,
    consumers: Vec<(StaticFilterConsumer, u32)>,
) -> Result<Arc<LocalProgram>, String> {
    let base = scan_program(dop, false);
    let scan = *base.scan_inputs().keys().next().expect("one scan");
    let project = base.graph().root();
    let snapshot = base
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let ProgramNodeKind::Scan {
        source,
        residuals,
        limit,
        ..
    } = base.graph().nodes()[scan.index()].kind()
    else {
        panic!("the fixture scan is a Scan");
    };
    let mut runtime_filters = Vec::new();
    let mut roots = Vec::new();
    let mut requirements = Vec::new();
    for (binding, (consumer, output)) in consumers.into_iter().enumerate() {
        let like = site(
            project,
            ProgramNodeExpressionRole::ProjectOutput { expression: output },
        );
        let definition = snapshot.roots().sites()[&like].definition;
        requirements.push(requirement(consumer.binding_id()));
        runtime_filters.push(FilterConsumerAtExpr {
            expr_id: definition,
            consumer,
        });
        roots.push(AddedRoot {
            site: site(
                scan,
                ProgramNodeExpressionRole::RuntimeFilter {
                    binding: u32::try_from(binding).unwrap(),
                },
            ),
            like,
            definition: None,
            source: None,
        });
    }
    let kind = ProgramNodeKind::Scan {
        source: source.clone(),
        runtime_filters,
        residuals: residuals.clone(),
        limit: *limit,
    };
    rebuild(
        &base,
        with_kind(&base, scan, kind),
        Additions {
            requirements,
            roots,
            ..Additions::default()
        },
    )
}

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

/// `(b, a)` = `(v1, v0)` of every scripted row `keep` accepts.
fn expected(keep: impl Fn(i64, i64) -> bool) -> Vec<(i64, i64)> {
    let mut rows = INPUT
        .iter()
        .filter(|(v0, v1)| keep(*v0, *v1))
        .map(|(v0, v1)| (*v1, *v0))
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

fn sorted_rows(chunks: &[Chunk]) -> Vec<(i64, i64)> {
    let mut rows = rows(chunks);
    rows.sort_unstable();
    rows
}

/// An Int64 membership artifact accepting exactly `accepted`.
struct Int64Membership {
    accepted: BTreeSet<i64>,
}

impl execution::RuntimeFilterArtifactQuery for Int64Membership {
    fn data_type(&self) -> &DataType {
        &DataType::Int64
    }

    fn matches_null(&self) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        Ok(false)
    }

    fn has_non_null_matches(&self) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        Ok(!self.accepted.is_empty())
    }

    fn non_null_value_may_match(
        &self,
        value: execution::RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        match value {
            execution::RuntimeFilterScalarRef::Int64(value) => Ok(self.accepted.contains(&value)),
            _ => Err(execution::RuntimeFilterArtifactQueryError::ContractViolation),
        }
    }

    fn non_null_range_may_match(
        &self,
        _: &ConnectorScalarValue,
        _: &ConnectorScalarValue,
    ) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        Ok(true)
    }
}

fn accepting(binding_id: u32, values: &[i64]) -> execution::SnapshotAcquireOutcome {
    execution::SnapshotAcquireOutcome::Published(Arc::new(execution::RuntimeFilterSnapshot::new(
        execution::RuntimeFilterBindingId::new(binding_id),
        execution::LogicalVersion::FIRST,
        [0; 32],
        Arc::new(Int64Membership {
            accepted: values.iter().copied().collect(),
        }),
    )))
}

/// A blocking subscription whose outcome the test publishes, keeping what
/// the consumer records.
struct ControlledSubscription {
    outcome: Mutex<Option<execution::SnapshotAcquireOutcome>>,
    published: Arc<Observable>,
    records: Mutex<Vec<&'static str>>,
}

impl ControlledSubscription {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(None),
            published: Arc::new(Observable::new()),
            records: Mutex::new(Vec::new()),
        })
    }

    fn publish(&self, outcome: execution::SnapshotAcquireOutcome) {
        *self.outcome.lock().expect("outcome lock") = Some(outcome);
        self.published.notify_observers();
    }

    fn records(&self) -> Vec<&'static str> {
        self.records.lock().expect("records lock").clone()
    }
}

impl execution::BlockingSnapshotSubscription for ControlledSubscription {
    fn try_outcome(&self) -> Option<execution::SnapshotAcquireOutcome> {
        self.outcome.lock().expect("outcome lock").clone()
    }

    fn outcome_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.published)
    }

    fn record_consumer_outcome(&self, outcome: &execution::SnapshotAcquireOutcome) {
        self.records
            .lock()
            .expect("records lock")
            .push(match outcome {
                execution::SnapshotAcquireOutcome::Published(_) => "published",
                execution::SnapshotAcquireOutcome::Unsupported(_) => "unsupported",
                execution::SnapshotAcquireOutcome::Unavailable(_) => "unavailable",
                execution::SnapshotAcquireOutcome::Cancelled => "cancelled",
                execution::SnapshotAcquireOutcome::TimedOut => "timed_out",
            });
    }

    fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
        match &*self.outcome.lock().expect("outcome lock") {
            Some(execution::SnapshotAcquireOutcome::Published(snapshot)) => {
                Some(Arc::clone(snapshot))
            }
            _ => None,
        }
    }
}

/// What a producer was asked to do, in order.
#[derive(Debug, Eq, PartialEq)]
enum ProducerEvent {
    Submit {
        partition: u32,
        sequence: u64,
        contribution: execution::RuntimeFilterContribution,
    },
    Close {
        partition: u32,
        sequence: u64,
    },
    Fail(execution::RuntimeFilterProducerFailure),
}

#[derive(Default)]
struct RecordingProducer {
    events: Mutex<Vec<ProducerEvent>>,
}

impl execution::RuntimeFilterProducer for RecordingProducer {
    fn max_contribution_bytes(&self) -> usize {
        1 << 20
    }

    fn submit(
        &self,
        partition: execution::PartitionId,
        sequence: execution::ProducerSequence,
        contribution: execution::RuntimeFilterContribution,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        self.events
            .lock()
            .expect("producer events")
            .push(ProducerEvent::Submit {
                partition: partition.get(),
                sequence: sequence.get(),
                contribution,
            });
        Ok(execution::RuntimeFilterSubmitOutcome::Applied)
    }

    fn close_partition(
        &self,
        partition: execution::PartitionId,
        sequence: execution::ProducerSequence,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        self.events
            .lock()
            .expect("producer events")
            .push(ProducerEvent::Close {
                partition: partition.get(),
                sequence: sequence.get(),
            });
        Ok(execution::RuntimeFilterSubmitOutcome::Completed)
    }

    fn fail(
        &self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        self.events
            .lock()
            .expect("producer events")
            .push(ProducerEvent::Fail(reason));
        Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop)
    }
}

/// A Task runtime-filter session double: blocking subscriptions and
/// recording producers by binding, and every request it was asked.
#[derive(Default)]
struct FakeSession {
    subscriptions: BTreeMap<u32, Arc<ControlledSubscription>>,
    producers: BTreeMap<u32, Arc<RecordingProducer>>,
    subscribed: Mutex<Vec<execution::RuntimeFilterConsumerContract>>,
    opened: Mutex<Vec<execution::RuntimeFilterProducerOpenRequest>>,
}

impl FakeSession {
    fn consumers(subscriptions: &[(u32, &Arc<ControlledSubscription>)]) -> Arc<Self> {
        Arc::new(Self {
            subscriptions: subscriptions
                .iter()
                .map(|(binding, subscription)| (*binding, Arc::clone(subscription)))
                .collect(),
            ..Self::default()
        })
    }

    fn producers(producers: &[(u32, &Arc<RecordingProducer>)]) -> Arc<Self> {
        Arc::new(Self {
            producers: producers
                .iter()
                .map(|(binding, producer)| (*binding, Arc::clone(producer)))
                .collect(),
            ..Self::default()
        })
    }
}

impl execution::RuntimeFilterSession for FakeSession {
    fn open_producer(
        &self,
        request: execution::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterProducerHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        let producer = self
            .producers
            .get(&request.contract().binding_id().get())
            .cloned()
            .ok_or_else(|| {
                execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "fake session has no such producer",
                )
            })?;
        self.opened.lock().expect("opened").push(request);
        Ok(execution::RuntimeFilterBindOutcome::Bound(producer))
    }

    fn subscribe(
        &self,
        request: execution::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        let subscription = self
            .subscriptions
            .get(&request.contract().binding_id().get())
            .cloned()
            .ok_or_else(|| {
                execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "fake session has no such subscription",
                )
            })?;
        self.subscribed
            .lock()
            .expect("subscribed")
            .push(request.contract().clone());
        Ok(execution::RuntimeFilterBindOutcome::Bound(
            execution::RuntimeFilterSubscriptionHandle::Blocking(subscription),
        ))
    }

    fn open_final_domain_completion(
        &self,
        _: execution::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterFinalDomainCompletionHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        Err(execution::RuntimeFilterContractViolation::new(
            execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
            "fake session has no final-domain completion",
        ))
    }
}

/// Every runtime-filter row effect the fragment reported.
#[derive(Default)]
struct RecordingEvents {
    effects: Mutex<Vec<execution::RuntimeFilterRowEffect>>,
}

impl FragmentEventSink for RecordingEvents {
    fn record(&self, event: FragmentEvent) {
        if let FragmentEvent::RuntimeFilterRowEffect(effect) = event {
            self.effects.lock().expect("effects").push(effect);
        }
    }
}

/// A runtime state whose general runtime-filter wait is `wait`, scan wait
/// `scan_wait`, bound to `session`.
fn state(
    session: Option<Arc<FakeSession>>,
    wait: Duration,
    scan_wait: Option<Duration>,
) -> Arc<RuntimeState> {
    Arc::new(
        RuntimeState::new(
            Some(QueryOptions {
                runtime_filter_wait_timeout_ms: Some(i32::try_from(wait.as_millis()).unwrap()),
                runtime_filter_scan_wait_time_ms: scan_wait
                    .map(|wait| i64::try_from(wait.as_millis()).unwrap()),
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            None,
            Some(crate::runtime::execution_runtime::test_execution_runtime()),
        )
        .with_runtime_filter_session(
            session.map(|session| session as execution::RuntimeFilterSessionRef),
        ),
    )
}

fn bound(op: &Arc<FixtureScanOp>) -> ScanBindings {
    let mut bindings = ScanBindings::default();
    bindings.insert(SCAN_NODE, Arc::clone(op) as Arc<dyn ScanOp>);
    bindings
}

fn prepare_scan(
    program: &Arc<LocalProgram>,
    op: &Arc<FixtureScanOp>,
    output: &ResultSinkHandle,
    state: Arc<RuntimeState>,
    events: Arc<dyn FragmentEventSink>,
) -> ExecutionResult<PreparedPipelineExecution> {
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    prepare_compiled_program_pipeline_execution_with_profiler(
        Arc::clone(program),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        bound(op),
        crate::runtime::fragment::CompiledWriterBindings::default(),
        None,
        None,
        dop,
        state,
        events,
    )
}

fn scan_refusal(program: &Arc<LocalProgram>, session: Option<Arc<FakeSession>>) -> String {
    let op = FixtureScanOp::new(Vec::new(), false);
    match prepare_scan(
        program,
        &op,
        &ResultSinkHandle::new(),
        state(session, Duration::from_secs(1), None),
        Arc::new(NoopFragmentEventSink),
    ) {
        Ok(_) => panic!("compiled runtime-filter preparation must be refused"),
        Err(error) => {
            assert_eq!(op.claims(), 0, "a refused preparation claims no stream");
            error.to_string()
        }
    }
}

// The gate holds the scan's first read until the filter is published; the
// published membership then filters every row the scan reads, before the
// handoff to the downstream drivers and the downstream projection.
#[test]
fn a_compiled_scan_holds_its_first_read_until_its_filter_arrives_then_filters_rows() {
    for dop in [1, 2] {
        let program = scan_with_consumers(dop, vec![(blocking_consumer(7), V0_OUTPUT)])
            .unwrap_or_else(|error| panic!("dop {dop}: the scan consumer program builds: {error}"));
        let op = FixtureScanOp::new(input(&program), false);
        let subscription = ControlledSubscription::new();
        let session = FakeSession::consumers(&[(7, &subscription)]);
        let events = Arc::new(RecordingEvents::default());
        let output = ResultSinkHandle::new();
        let running = prepare_scan(
            &program,
            &op,
            &output,
            state(Some(Arc::clone(&session)), Duration::from_secs(60), None),
            Arc::clone(&events) as Arc<dyn FragmentEventSink>,
        )
        .unwrap_or_else(|error| panic!("dop {dop}: the scan prepares: {error}"))
        .start();
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(
            op.claims(),
            0,
            "dop {dop}: the pending filter holds the scan's first read"
        );
        assert!(subscription.records().is_empty());

        subscription.publish(accepting(7, &[8, 20]));
        running.join().expect("the compiled scan runs");
        assert_eq!(
            sorted_rows(&output.take_chunks()),
            expected(|v0, _| [8, 20].contains(&v0)),
            "dop {dop}"
        );
        assert_eq!(op.claims(), 1, "dop {dop}: one driver owns the scan stream");
        assert_eq!(subscription.records(), vec!["published"], "dop {dop}");
        let subscribed = session.subscribed.lock().unwrap();
        assert_eq!(subscribed.len(), 1, "dop {dop}: one subscription per scan");
        assert_eq!(subscribed[0].binding_id().get(), 7);
        assert_eq!(
            subscribed[0].activation(),
            execution::ConsumerActivation::BlockingSnapshot
        );
        // One evaluated effect per chunk the scan reads, the empty one
        // included, exactly as the v1 consumer reports them.
        let effects = events.effects.lock().unwrap();
        let effects = effects
            .iter()
            .map(|effect| {
                (
                    effect.binding_id().get(),
                    effect.input_rows(),
                    effect.output_rows(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(effects, vec![(7, 3, 1), (7, 0, 0), (7, 2, 1)], "dop {dop}");
    }
}

// Several blocking consumers of one scan share its one gate, settle in
// binding order, and each keys its own column through its own root.
#[test]
fn several_consumers_of_one_compiled_scan_share_its_gate_and_key_their_own_roots() {
    let program = scan_with_consumers(
        1,
        vec![
            (blocking_consumer(7), V0_OUTPUT),
            (blocking_consumer(8), V1_OUTPUT),
        ],
    )
    .expect("the two-consumer program builds");
    let op = FixtureScanOp::new(input(&program), false);
    let first = ControlledSubscription::new();
    let second = ControlledSubscription::new();
    let session = FakeSession::consumers(&[(7, &first), (8, &second)]);
    let output = ResultSinkHandle::new();
    let running = prepare_scan(
        &program,
        &op,
        &output,
        state(Some(session), Duration::from_secs(60), None),
        Arc::new(NoopFragmentEventSink),
    )
    .expect("the scan prepares")
    .start();
    second.publish(accepting(8, &[10, 90, 70, 200]));
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(op.claims(), 0, "the earlier binding still holds the gate");
    first.publish(accepting(7, &[1, 8, 9, 20]));
    running.join().expect("the compiled scan runs");
    assert_eq!(
        sorted_rows(&output.take_chunks()),
        expected(|v0, v1| [1, 8, 9, 20].contains(&v0) && [10, 90, 70, 200].contains(&v1))
    );
    assert_eq!(first.records(), vec!["published"]);
    assert_eq!(second.records(), vec!["published"]);
}

// A filter that is not published within the wait passes the scan's rows
// through, recorded once as timed out, exactly as the v1 consumer does.
#[test]
fn an_expired_compiled_scan_wait_passes_rows_through_as_timed_out_once() {
    let program = scan_with_consumers(1, vec![(blocking_consumer(7), V0_OUTPUT)])
        .expect("the scan consumer program builds");
    let op = FixtureScanOp::new(input(&program), false);
    let subscription = ControlledSubscription::new();
    let session = FakeSession::consumers(&[(7, &subscription)]);
    let events = Arc::new(RecordingEvents::default());
    let output = ResultSinkHandle::new();
    let started = Instant::now();
    prepare_scan(
        &program,
        &op,
        &output,
        state(Some(session), Duration::from_millis(50), None),
        Arc::clone(&events) as Arc<dyn FragmentEventSink>,
    )
    .expect("the scan prepares")
    .start()
    .join()
    .expect("the timed-out scan still runs");
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert_eq!(sorted_rows(&output.take_chunks()), expected(|_, _| true));
    assert_eq!(subscription.records(), vec!["timed_out"]);
    // A late publication changes nothing: the binding already settled.
    subscription.publish(accepting(7, &[8]));
    assert_eq!(subscription.records(), vec!["timed_out"]);
    assert!(
        events.effects.lock().unwrap().is_empty(),
        "a pass-through binding evaluates no row"
    );
}

/// The gate deadline a scan source sets on its first read, bounded by the
/// instants just before and after that read.
fn first_read_wait(
    factory: &StreamScanSourceFactory,
    state: &RuntimeState,
) -> (Duration, Duration) {
    let mut operator = factory.create(1, 0);
    operator.prepare().expect("prepare scan");
    operator
        .bind_runtime_state(state)
        .expect("bind scan runtime state");
    let before = Instant::now();
    let pulled = operator
        .as_processor_mut()
        .expect("a scan is a processor")
        .pull_chunk(state)
        .expect("a pending gate is not an error");
    let after = Instant::now();
    assert!(pulled.is_none(), "the pending gate holds the first read");
    let deadline = operator
        .as_processor_ref()
        .expect("a scan is a processor")
        .source_block_deadline()
        .expect("a pending gate bounds the wait")
        .at();
    (deadline - after, deadline - before)
}

// The compiled scan source and the v1 scan source take their gate wait from
// the same runtime state by the same rule, with the scan-specific and the
// general wait both configured.
#[test]
fn a_compiled_scan_gate_waits_exactly_as_long_as_the_v1_scan_gate() {
    let program = scan_with_consumers(1, vec![(blocking_consumer(7), V0_OUTPUT)])
        .expect("the scan consumer program builds");
    for (wait, scan_wait) in [
        (Duration::from_secs(600), Some(Duration::from_secs(5))),
        (Duration::from_secs(5), Some(Duration::from_secs(600))),
        (Duration::from_secs(300), None),
    ] {
        let subscription = ControlledSubscription::new();
        let session = FakeSession::consumers(&[(7, &subscription)]);
        let state = state(Some(session), wait, scan_wait);
        let op = FixtureScanOp::new(Vec::new(), true);

        let mut arena = ExprArena::default();
        let key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let contract = execution::RuntimeFilterConsumerContract::membership_blocking(
            execution::RuntimeFilterBindingId::new(7),
            execution::RuntimeFilterChannelId::new(7),
            execution::RuntimeFilterExecutionContract::Membership(
                execution::RuntimeFilterMembershipSchema::new(
                    &DataType::Int64,
                    execution::RuntimeFilterNullSemantics::NeverMatches,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let scan = ScanNode::new_for_test(Arc::clone(&op) as Arc<dyn ScanOp>)
            .with_node_id(SCAN_NODE)
            .with_runtime_filter_consumers(vec![RuntimeFilterConsumerBinding::new(key, contract)]);
        let v1 = StreamScanSourceFactory::new_native(
            scan,
            Arc::clone(&op) as Arc<dyn ScanOp>,
            Arc::new(arena),
        )
        .expect("v1 scan source");

        let consumers =
            crate::exec::operators::runtime_filter::CompiledRuntimeFilterConsumers::try_new(
                "Scan",
                Arc::clone(&program),
                *program.scan_inputs().keys().next().unwrap(),
                vec![super::runtime_filter_consumer_contract(&blocking_consumer(7)).unwrap()],
                state.error_state(),
            )
            .expect("compiled consumers");
        let compiled = StreamScanSourceFactory::new_compiled(
            SCAN_NODE,
            Arc::clone(&op) as Arc<dyn ScanOp>,
            Some(Arc::new(consumers)),
        );

        let (v1_low, v1_high) = first_read_wait(&v1, &state);
        let (compiled_low, compiled_high) = first_read_wait(&compiled, &state);
        assert!(
            compiled_low <= v1_high && v1_low <= compiled_high,
            "wait {wait:?} scan wait {scan_wait:?}: compiled [{compiled_low:?}, {compiled_high:?}] v1 [{v1_low:?}, {v1_high:?}]"
        );
        assert_eq!(op.claims(), 0, "a pending gate claims no stream");
    }
}

#[test]
fn compiled_scan_runtime_filter_shapes_outside_m1_are_refused_by_name() {
    let subscription = ControlledSubscription::new();
    let session = || Some(FakeSession::consumers(&[(7, &subscription)]));

    let ordered = StaticFilterConsumer::try_new(
        7,
        7,
        FilterConsumerActivation::NonBlockingLive {
            late_apply: FilterLateApplyGranularity::Batch,
        },
        StaticFilterContract::Ordered {
            keys: Arc::from([FilterOrderKey {
                data_type: DataType::Int64,
                direction: FilterSortDirection::Ascending,
                null_order: FilterNullOrder::First,
            }]),
            comparator_digest: [1; 32],
            contract_digest: [2; 32],
        },
        FilterReduction::TightenOrderedBound,
    )
    .unwrap();
    let program = scan_with_consumers(1, vec![(ordered, V0_OUTPUT)]).unwrap();
    let message = scan_refusal(&program, session());
    assert!(
        message.contains(
            "compiled scan at local node 0 runtime-filter binding_id=7 with an ordered-domain contract is not executable yet"
        ),
        "{message}"
    );

    let live = StaticFilterConsumer::try_new(
        7,
        7,
        FilterConsumerActivation::NonBlockingLive {
            late_apply: FilterLateApplyGranularity::Batch,
        },
        membership(&DataType::Int64, FilterNullSemantics::NeverMatches),
        FilterReduction::SetUnion,
    )
    .unwrap();
    let program = scan_with_consumers(1, vec![(live, V0_OUTPUT)]).unwrap();
    let message = scan_refusal(&program, session());
    assert!(
        message.contains("runtime-filter binding_id=7 with NonBlockingLive Batch activation is not executable yet"),
        "{message}"
    );

    // A key root whose static type is not the membership key type.
    let mistyped = StaticFilterConsumer::try_new(
        7,
        7,
        FilterConsumerActivation::BlockingSnapshot,
        membership(&DataType::Int32, FilterNullSemantics::NeverMatches),
        FilterReduction::SetUnion,
    )
    .unwrap();
    let program = scan_with_consumers(1, vec![(mistyped, V0_OUTPUT)]).unwrap();
    let message = scan_refusal(&program, session());
    assert!(
        message.contains("key root at local node 0 has type Int64, not its membership type Int32"),
        "{message}"
    );

    // A membership digest that is not the canonical schema's.
    let forged = StaticFilterConsumer::try_new(
        7,
        7,
        FilterConsumerActivation::BlockingSnapshot,
        StaticFilterContract::Membership {
            data_type: DataType::Int64,
            null_semantics: FilterNullSemantics::NeverMatches,
            digest: [9; 32],
        },
        FilterReduction::SetUnion,
    )
    .unwrap();
    let program = scan_with_consumers(1, vec![(forged, V0_OUTPUT)]).unwrap();
    let message = scan_refusal(&program, session());
    assert!(
        message.contains("local program membership filter digest mismatch"),
        "{message}"
    );
    assert!(subscription.records().is_empty());
}

#[test]
fn compiled_runtime_filter_bindings_must_match_the_requirements_and_a_session() {
    let program = scan_with_consumers(1, vec![(blocking_consumer(7), V0_OUTPUT)]).unwrap();
    let message = scan_refusal(&program, None);
    assert!(
        message.contains(
            "compiled runtime-filter binding_id=7 at local node 0 requires an execution runtime-filter session"
        ),
        "{message}"
    );

    // A requirement without a program site.
    let base = scan_program(1, false);
    let extra = rebuild(
        &base,
        with_kind(
            &base,
            base.graph().root(),
            base.graph().nodes()[base.graph().root().index()]
                .kind()
                .clone(),
        ),
        Additions {
            requirements: vec![requirement(9)],
            ..Additions::default()
        },
    )
    .expect("a program with an extra requirement still compiles");
    let subscription = ControlledSubscription::new();
    let message = scan_refusal(&extra, Some(FakeSession::consumers(&[(7, &subscription)])));
    assert!(
        message.contains(
            "compiled runtime-filter binding requirement binding_id=9 has no program site"
        ),
        "{message}"
    );

    // A program site without its requirement cannot even be compiled.
    let scan = *base.scan_inputs().keys().next().unwrap();
    let ProgramNodeKind::Scan { source, .. } = base.graph().nodes()[scan.index()].kind() else {
        unreachable!()
    };
    let definition = base
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot()
        .roots()
        .sites()[&site(
        base.graph().root(),
        ProgramNodeExpressionRole::ProjectOutput {
            expression: V0_OUTPUT,
        },
    )]
        .definition;
    let missing = rebuild(
        &base,
        with_kind(
            &base,
            scan,
            ProgramNodeKind::Scan {
                source: source.clone(),
                runtime_filters: vec![FilterConsumerAtExpr {
                    expr_id: definition,
                    consumer: blocking_consumer(7),
                }],
                residuals: Vec::new(),
                limit: None,
            },
        ),
        Additions {
            roots: vec![AddedRoot {
                site: site(
                    scan,
                    ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
                ),
                like: site(
                    base.graph().root(),
                    ProgramNodeExpressionRole::ProjectOutput {
                        expression: V0_OUTPUT,
                    },
                ),
                definition: None,
                source: None,
            }],
            ..Additions::default()
        },
    );
    assert!(
        matches!(&missing, Err(error) if error.contains("InvalidRequirement")),
        "{:?}",
        missing.map(|_| ())
    );
}

// ---------------------------------------------------------------------------
// Hash join producers.

fn hash_spec(null_safe: bool) -> Spec {
    Spec {
        family: Family::Hash {
            build_side: JoinSide::Right,
            null_safe,
        },
        ..Spec::hash(JoinKind::Inner, false)
    }
}

fn join_node(program: &LocalProgram) -> ProgramNodeId {
    let index = program
        .graph()
        .nodes()
        .iter()
        .position(|node| matches!(node.kind(), ProgramNodeKind::Join { .. }))
        .expect("one compiled hash join");
    ProgramNodeId::new(index)
}

/// `program` whose hash join produces `producers`, each over the build key
/// at its ordinal.
fn join_with_producers(
    program: &LocalProgram,
    producers: Vec<(usize, StaticFilterProducer)>,
) -> Result<Arc<LocalProgram>, String> {
    let join = join_node(program);
    let mut kind = program.graph().nodes()[join.index()].kind().clone();
    let ProgramNodeKind::Join {
        build_keys,
        runtime_filters,
        ..
    } = &mut kind
    else {
        unreachable!()
    };
    let mut requirements = Vec::new();
    for (key_ordinal, producer) in producers {
        requirements.push(requirement(producer.binding_id()));
        runtime_filters.push(FilterProducerAtExpr {
            expr_id: build_keys[key_ordinal],
            key_ordinal,
            producer,
        });
    }
    rebuild(
        program,
        with_kind(program, join, kind),
        Additions {
            requirements,
            ..Additions::default()
        },
    )
}

fn single_join(null_safe: bool) -> Arc<LocalProgram> {
    let mut packages = packages(&single_plan(hash_spec(null_safe), &LEFT_ROWS, &RIGHT_ROWS));
    compile(packages.remove(&SINGLE).unwrap(), 1, true)
}

fn run_join(program: &Arc<LocalProgram>, session: Arc<FakeSession>) -> ExecutionResult<Rows> {
    let output = ResultSinkHandle::new();
    let sink: Box<dyn OperatorFactory> = result_sink(program, SINGLE_FINST, &output);
    prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        ExchangeBindings::default(),
        Some((SINGLE_FINST.high(), SINGLE_FINST.low())),
        1,
        state(Some(session), Duration::from_secs(1), None),
        Arc::new(NoopFragmentEventSink),
    )?
    .start()
    .join()?;
    Ok(sorted(int64_rows(&output.take_chunks())))
}

/// The union of every membership contribution `events` submitted, and the
/// partition and sequence it closed with.
fn published_membership(
    events: &[ProducerEvent],
    digest: [u8; 32],
) -> (BTreeSet<i64>, bool, Option<(u32, u64)>) {
    let mut values = BTreeSet::new();
    let mut contains_null = false;
    let mut close = None;
    for (ordinal, event) in events.iter().enumerate() {
        match event {
            ProducerEvent::Submit {
                partition,
                sequence,
                contribution,
            } => {
                assert_eq!(*partition, 0, "the one build driver is partition 0");
                assert_eq!(*sequence, u64::try_from(ordinal).unwrap());
                assert_eq!(contribution.contract_digest(), digest);
                let decoded = execution::contribution::decode_contribution(
                    contribution.canonical_bytes(),
                    &contribution.contract_digest(),
                    execution::contribution::ContributionCodecExpectation::membership(
                        &DataType::Int64,
                        digest,
                    ),
                    usize::MAX,
                )
                .expect("a canonical membership contribution");
                let execution::contribution::RuntimeFilterContribution::Membership(domain) =
                    decoded
                else {
                    panic!("a membership producer submits membership: {decoded:?}");
                };
                let execution::contribution::MembershipValues::Int64(submitted) = domain.values()
                else {
                    panic!("an Int64 key submits Int64 values");
                };
                values.extend(submitted.iter().copied());
                contains_null |= domain.contains_null();
            }
            ProducerEvent::Close {
                partition,
                sequence,
            } => {
                assert!(close.is_none(), "the partition closes once");
                close = Some((*partition, *sequence));
            }
            ProducerEvent::Fail(reason) => panic!("a completed build fails nothing: {reason:?}"),
        }
    }
    (values, contains_null, close)
}

// The build driver publishes exactly the membership of its evaluated build
// keys: NULL keys included as a NULL fact, for whichever NULL semantics the
// key's equality fixes, then closes its one partition after the build.
#[test]
fn a_compiled_hash_join_publishes_the_exact_membership_of_its_build_keys() {
    let build_keys = RIGHT_ROWS
        .iter()
        .filter_map(|(key, _)| *key)
        .collect::<BTreeSet<_>>();
    for (null_safe, null_semantics) in [
        (false, FilterNullSemantics::NeverMatches),
        (true, FilterNullSemantics::NullSafeEqual),
    ] {
        let base = single_join(null_safe);
        let producer = membership_producer(11, null_semantics);
        let StaticFilterContract::Membership { digest, .. } = producer.contract().clone() else {
            unreachable!()
        };
        let program = join_with_producers(&base, vec![(0, producer)])
            .expect("the join producer program builds");
        let recording = Arc::new(RecordingProducer::default());
        let session = FakeSession::producers(&[(11, &recording)]);
        let rows = run_join(&program, Arc::clone(&session))
            .unwrap_or_else(|error| panic!("null-safe {null_safe}: the join runs: {error}"));
        assert_eq!(
            rows,
            oracle(hash_spec(null_safe), &LEFT_ROWS, &RIGHT_ROWS),
            "null-safe {null_safe}: a producer does not change the join"
        );
        let events = std::mem::take(&mut *recording.events.lock().unwrap());
        let (values, contains_null, close) = published_membership(&events, digest);
        assert_eq!(values, build_keys, "null-safe {null_safe}");
        assert!(
            contains_null,
            "null-safe {null_safe}: the build has NULL keys"
        );
        let submits = u64::try_from(events.len() - 1).unwrap();
        assert_eq!(close, Some((0, submits)), "null-safe {null_safe}");
        let opened = session.opened.lock().unwrap();
        assert_eq!(opened.len(), 1, "null-safe {null_safe}");
        assert_eq!(
            opened[0].local_partition_count(),
            1,
            "the one build driver is the producers' only partition"
        );
        assert_eq!(opened[0].contract().binding_id().get(), 11);
        assert_eq!(
            opened[0].contract().kind(),
            execution::RuntimeFilterProducerKind::Membership
        );
    }
}

// A build key row error fails the producer instead of closing it: no
// contribution and no close is ever published for the failed build.
#[test]
fn a_failed_compiled_build_fails_its_runtime_filter_producer() {
    let mut packages = packages(&overflow_plan(Overflow::BuildKey));
    let base = compile(packages.remove(&SINGLE).unwrap(), 1, true);
    let program = join_with_producers(
        &base,
        vec![(
            0,
            membership_producer(11, FilterNullSemantics::NeverMatches),
        )],
    )
    .expect("the overflowing join producer program builds");
    let recording = Arc::new(RecordingProducer::default());
    let session = FakeSession::producers(&[(11, &recording)]);
    let error = run_join(&program, session).expect_err("the overflowing build fails");
    assert!(error.to_string().contains("Arithmetic overflow"), "{error}");
    assert_eq!(
        *recording.events.lock().unwrap(),
        vec![ProducerEvent::Fail(
            execution::RuntimeFilterProducerFailure::ExecutionFailed
        )]
    );
}

#[test]
fn compiled_hash_join_producer_shapes_outside_m1_are_refused_by_name() {
    let base = single_join(false);
    let recording = Arc::new(RecordingProducer::default());

    // The producer's NULL semantics must be the ones its key equality fixes.
    let program = join_with_producers(
        &base,
        vec![(
            0,
            membership_producer(11, FilterNullSemantics::NullSafeEqual),
        )],
    )
    .unwrap();
    let message = run_join(&program, FakeSession::producers(&[(11, &recording)]))
        .expect_err("a mismatched producer is refused")
        .to_string();
    assert!(
        message.contains("membership schema does not match build key ordinal 0"),
        "{message}"
    );

    let program = join_with_producers(
        &base,
        vec![(
            0,
            StaticFilterProducer::try_new(
                11,
                11,
                FilterProducerKind::FinalDomain,
                membership(&DataType::Int64, FilterNullSemantics::NeverMatches),
                FilterReduction::SetUnion,
            )
            .unwrap(),
        )],
    )
    .unwrap();
    let message = run_join(&program, FakeSession::producers(&[(11, &recording)]))
        .expect_err("a final-domain producer is refused")
        .to_string();
    assert!(
        message.contains(
            "runtime-filter binding_id=11 with a FinalDomain producer is not executable yet"
        ),
        "{message}"
    );

    let program = join_with_producers(
        &base,
        vec![(
            0,
            StaticFilterProducer::try_new(
                11,
                11,
                FilterProducerKind::TopKSummary,
                StaticFilterContract::Ordered {
                    keys: Arc::from([FilterOrderKey {
                        data_type: DataType::Int64,
                        direction: FilterSortDirection::Ascending,
                        null_order: FilterNullOrder::First,
                    }]),
                    comparator_digest: [1; 32],
                    contract_digest: [2; 32],
                },
                FilterReduction::MergeTopKSummary {
                    k: NonZeroU32::new(3).unwrap(),
                },
            )
            .unwrap(),
        )],
    )
    .unwrap();
    let message = run_join(&program, FakeSession::producers(&[(11, &recording)]))
        .expect_err("a top-k producer is refused")
        .to_string();
    assert!(
        message.contains(
            "runtime-filter binding_id=11 with a TopKSummary producer is not executable yet"
        ),
        "{message}"
    );
    assert!(recording.events.lock().unwrap().is_empty());
}

// Producers and consumers each count as one binding site; a binding bound
// twice in one program is refused before any driver is built.
#[test]
fn one_runtime_filter_binding_has_exactly_one_program_site() {
    let base = single_join(false);
    let recording = Arc::new(RecordingProducer::default());
    let join = join_node(&base);
    let mut kind = base.graph().nodes()[join.index()].kind().clone();
    let ProgramNodeKind::Join {
        build_keys,
        runtime_filters,
        ..
    } = &mut kind
    else {
        unreachable!()
    };
    for _ in 0..2 {
        runtime_filters.push(FilterProducerAtExpr {
            expr_id: build_keys[0],
            key_ordinal: 0,
            producer: membership_producer(11, FilterNullSemantics::NeverMatches),
        });
    }
    let program = rebuild(
        &base,
        with_kind(&base, join, kind),
        Additions {
            requirements: vec![requirement(11)],
            ..Additions::default()
        },
    )
    .expect("the graph accepts a covered binding");
    let message = run_join(&program, FakeSession::producers(&[(11, &recording)]))
        .expect_err("a binding with two sites is refused")
        .to_string();
    assert!(
        message.contains(&format!(
            "compiled runtime-filter binding_id=11 has a site at local node {} and at local node {}",
            join.index(),
            join.index()
        )),
        "{message}"
    );
}

/// `program` whose result is a runtime-filter consumer node over its hash
/// join, keyed by the join's probe key as the join outputs it. The local
/// program places a join probe-key consumer in this node kind.
fn join_under_consumer_node(program: &LocalProgram) -> Result<Arc<LocalProgram>, String> {
    let graph = program.graph();
    let join = join_node(program);
    assert_eq!(join, graph.root(), "the single join is the fragment result");
    let consumer = ProgramNodeId::new(graph.nodes().len());
    // The probe key's leaf use lends its control; the key itself is a new
    // definition of the probe key column as the join outputs it.
    let like = site(join, ProgramNodeExpressionRole::JoinProbeKey { key: 0 });
    let output = graph.nodes()[join.index()].output_layout();
    let ordinal = 0;
    let key_type = program
        .checked()
        .channels()
        .channel_type(ProgramChannelSite::Layout {
            node: join,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal,
        })
        .expect("the join output is typed")
        .clone();
    let definition = ProgramExprId::new(graph.expressions().nodes().len());
    let sources = graph.nodes()[join.index()].physical_sources().to_vec();
    let mut nodes = with_kind(program, join, graph.nodes()[join.index()].kind().clone());
    nodes.push(ProgramNode::new_local(
        consumer,
        sources.clone(),
        ProgramNodeKind::RuntimeFilterConsumer {
            input: join,
            bindings: vec![FilterConsumerAtExpr {
                expr_id: definition,
                consumer: blocking_consumer(7),
            }],
        },
        output.clone(),
    ));
    let channels = program
        .checked()
        .channels()
        .channels()
        .iter()
        .filter_map(|(channel, value)| match channel {
            ProgramChannelSite::Layout {
                node,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal,
            } if *node == join => Some((
                ProgramChannelSite::Layout {
                    node: consumer,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: *ordinal,
                },
                value.clone(),
            )),
            _ => None,
        })
        .collect();
    let owner = program
        .provenance()
        .operators()
        .values()
        .find(|operator| operator.lowered_nodes.contains(&join))
        .expect("the join has an operator");
    let id = LocalOperatorId::new(
        program
            .provenance()
            .operators()
            .keys()
            .map(|id| id.get())
            .max()
            .unwrap()
            + 1,
    );
    rebuild(
        program,
        nodes,
        Additions {
            scalar_root: None,
            root: Some(consumer),
            definitions: vec![(
                StaticExprNode::new(
                    StaticExprKind::SlotId(output.slots()[0]),
                    key_type.data_type.clone(),
                    None,
                ),
                key_type,
            )],
            requirements: vec![requirement(7)],
            roots: vec![AddedRoot {
                site: site(
                    consumer,
                    ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
                ),
                like,
                definition: Some(definition),
                source: Some(ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                    node: join,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal,
                })),
            }],
            channels,
            operators: vec![LocalOperatorProvenance {
                id,
                lowered_nodes: Box::from([consumer]),
                sources: sources.into_boxed_slice(),
                origin: LocalOperatorOrigin::Split { piece: 1_000 },
                cost_owner: id,
                metrics: owner.metrics,
            }],
        },
    )
}

// A runtime-filter consumer node, which carries a join's probe-key consumer,
// is refused by name before any driver subscribes.
#[test]
fn a_compiled_join_probe_key_consumer_node_is_refused_by_name() {
    let base = single_join(false);
    let program = join_under_consumer_node(&base).expect("the consumer-node program builds");
    let consumer = program.graph().root();
    assert!(matches!(
        program.graph().nodes()[consumer.index()].kind(),
        ProgramNodeKind::RuntimeFilterConsumer { .. }
    ));
    let subscription = ControlledSubscription::new();
    let session = FakeSession::consumers(&[(7, &subscription)]);
    let message = run_join(&program, Arc::clone(&session))
        .expect_err("a probe-key consumer node is refused")
        .to_string();
    assert!(
        message.contains(&format!(
            "compiled join probe-key runtime-filter consumer at local node {} is not executable yet",
            consumer.index()
        )),
        "{message}"
    );
    assert!(session.subscribed.lock().unwrap().is_empty());
    assert!(subscription.records().is_empty());
}

// A join-owned consumer retains its key definition but receives one fresh
// actual use and an exact JoinLeft lexical source, independent of probe uses.
fn join_with_probe_consumers(
    program: &LocalProgram,
    consumers: Vec<StaticFilterConsumer>,
) -> Result<Arc<LocalProgram>, String> {
    let join = join_node(program);
    let mut kind = program.graph().nodes()[join.index()].kind().clone();
    let ProgramNodeKind::Join {
        probe_keys,
        runtime_filter_consumers,
        ..
    } = &mut kind
    else {
        unreachable!()
    };
    let definition = probe_keys[0];
    let like = site(join, ProgramNodeExpressionRole::JoinProbeKey { key: 0 });
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let like_use = snapshot.bindings()[&like];
    let source = program.checked().slots()[&ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: like_use,
    }];
    let mut additions = Additions::default();
    for (binding, consumer) in consumers.into_iter().enumerate() {
        additions
            .requirements
            .push(requirement(consumer.binding_id()));
        runtime_filter_consumers.push(novarocks_local_program::FilterConsumerAtJoinKey {
            expr_id: definition,
            key_ordinal: 0,
            consumer,
        });
        additions.roots.push(AddedRoot {
            site: site(
                join,
                ProgramNodeExpressionRole::RuntimeFilter {
                    binding: binding.try_into().unwrap(),
                },
            ),
            like,
            definition: Some(definition),
            source: Some(source),
        });
    }
    rebuild(program, with_kind(program, join, kind), additions)
}

#[test]
fn join_probe_owned_consumer_actual_root_and_runtime_membership_empty_unavailable() {
    let base = single_join(false);
    let program = join_with_probe_consumers(&base, vec![blocking_consumer(7)]).unwrap();
    let join = join_node(&program);
    let rf = site(
        join,
        ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
    );
    let probe = site(join, ProgramNodeExpressionRole::JoinProbeKey { key: 0 });
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    assert_eq!(
        snapshot.roots().sites()[&rf].definition,
        snapshot.roots().sites()[&probe].definition
    );
    assert_ne!(snapshot.bindings()[&rf], snapshot.bindings()[&probe]);
    let use_id = snapshot.bindings()[&rf];
    assert!(
        matches!(program.checked().slots()[&ProgramUseRef { arena: ProgramExpressionArena::Main, use_id }],
        ProgramLexicalSource::Input(ProgramChannelSite::Layout { node, role: ProgramChannelLayoutRole::JoinLeft, .. }) if node == join)
    );
    for values in [vec![1, 2, 3, 4, 6], vec![1], vec![]] {
        let subscription = ControlledSubscription::new();
        subscription.publish(accepting(7, &values));
        let filtered = LEFT_ROWS
            .iter()
            .copied()
            .filter(|row| row.0.is_some_and(|v| values.contains(&v)))
            .collect::<Vec<_>>();
        assert_eq!(
            run_join(&program, FakeSession::consumers(&[(7, &subscription)])).unwrap(),
            sorted(oracle(hash_spec(false), &filtered, &RIGHT_ROWS))
        );
        assert_eq!(subscription.records(), vec!["published"]);
    }
    let subscription = ControlledSubscription::new();
    subscription.publish(execution::SnapshotAcquireOutcome::Cancelled);
    assert_eq!(
        run_join(&program, FakeSession::consumers(&[(7, &subscription)])).unwrap(),
        sorted(oracle(hash_spec(false), &LEFT_ROWS, &RIGHT_ROWS))
    );
    assert_eq!(subscription.records(), vec!["cancelled"]);
}

#[test]
fn join_probe_owned_shared_processor_gate_starts_at_input_and_has_exact_observable() {
    use crate::exec::operators::runtime_filter::{
        CompiledRuntimeFilterConsumers, NativeRuntimeFilterProcessorFactory,
    };
    use crate::runtime::runtime_state::RuntimeErrorState;
    let base = single_join(false);
    let program = join_with_probe_consumers(&base, vec![blocking_consumer(7)]).unwrap();
    let join = join_node(&program);
    let contract =
        super::super::local::runtime_filter_consumer_contract(&blocking_consumer(7)).unwrap();
    let consumers = Arc::new(
        CompiledRuntimeFilterConsumers::try_new(
            "Join",
            program.clone(),
            join,
            vec![contract],
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap(),
    );
    let subscription = ControlledSubscription::new();
    let runtime = state(
        Some(FakeSession::consumers(&[(7, &subscription)])),
        Duration::from_secs(60),
        None,
    );
    let factory = NativeRuntimeFilterProcessorFactory::new_compiled(7, consumers.clone());
    let mut operator = factory.create(1, 0);
    operator.activate(&runtime).unwrap();
    let ProgramNodeKind::Join { left_layout, .. } = program.graph().nodes()[join.index()].kind()
    else {
        unreachable!()
    };
    let arrays = vec![
        Arc::new(arrow::array::Int64Array::from(vec![Some(1), None, Some(2)]))
            as arrow::array::ArrayRef,
        Arc::new(arrow::array::Int64Array::from(vec![
            Some(10),
            Some(40),
            Some(20),
        ])) as arrow::array::ArrayRef,
    ];
    let batch =
        arrow::record_batch::RecordBatch::try_new(left_layout.schema().clone(), arrays).unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(
        batch,
        crate::exec::chunk::ChunkSchema::from_compiled_layout(left_layout).unwrap(),
    )
    .unwrap();
    assert!(operator.as_processor_ref().unwrap().need_input());
    assert!(
        operator
            .as_processor_ref()
            .unwrap()
            .sink_block_deadline()
            .is_none()
    );
    assert!(
        !operator
            .as_processor_ref()
            .unwrap()
            .can_accept_input(&chunk)
            .unwrap()
    );
    assert!(!operator.as_processor_ref().unwrap().need_input());
    assert!(
        operator
            .as_processor_ref()
            .unwrap()
            .sink_block_deadline()
            .is_some()
    );
    assert!(Arc::ptr_eq(
        &operator
            .as_processor_ref()
            .unwrap()
            .sink_observable()
            .unwrap(),
        &consumers.state().gate_observable()
    ));
    subscription.publish(accepting(7, &[2]));
    assert!(
        operator
            .as_processor_ref()
            .unwrap()
            .can_accept_input(&chunk)
            .unwrap()
    );
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&runtime, chunk)
        .unwrap();
    let output = operator
        .as_processor_mut()
        .unwrap()
        .pull_chunk(&runtime)
        .unwrap()
        .unwrap();
    assert_eq!(int64_rows(&[output]), vec![vec![Some(2), Some(20)]]);
    operator
        .as_processor_mut()
        .unwrap()
        .set_finishing(&runtime)
        .unwrap();
    assert!(operator.is_finished());
    assert_eq!(subscription.records(), vec!["published"]);
}

#[test]
fn join_probe_owned_root_frame_every_actual_callback_keeps_seven_causes_and_latch() {
    use crate::exec::expr::compiled_program::CompiledExpressionInstance;
    use novarocks_functions::{
        KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection,
    };
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            calls.push(units);
            if let Some((stop, cause)) = &self.refusal {
                if at == *stop {
                    return Err(cause.clone());
                }
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("a key root never waits")
        }
    }
    let program =
        join_with_probe_consumers(&single_join(false), vec![blocking_consumer(7)]).unwrap();
    let join = join_node(&program);
    let root = site(
        join,
        ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
    );
    let ProgramNodeKind::Join { left_layout, .. } = program.graph().nodes()[join.index()].kind()
    else {
        unreachable!()
    };
    let batch = arrow::record_batch::RecordBatch::try_new(
        left_layout.schema().clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![Some(1), None, Some(2)]))
                as arrow::array::ArrayRef,
            Arc::new(arrow::array::Int64Array::from(vec![
                Some(10),
                Some(40),
                Some(20),
            ])) as arrow::array::ArrayRef,
        ],
    )
    .unwrap();
    let make = || {
        let construction = Control {
            calls: Mutex::new(vec![]),
            refusal: None,
        };
        CompiledExpressionInstance::try_new(program.clone(), root, &construction).unwrap()
    };
    let baseline = Control {
        calls: Mutex::new(vec![]),
        refusal: None,
    };
    let mut instance = make();
    instance
        .evaluate(&batch, Selection::all(3), &baseline)
        .unwrap();
    let trace = baseline.calls.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original")),
            KernelFailure::Internal(KernelDiagnostic::new("original")),
            KernelFailure::Operational(KernelDiagnostic::new("original")),
            KernelFailure::InstanceFailed,
        ] {
            let control = Control {
                calls: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut instance = make();
            assert_eq!(
                instance
                    .evaluate(&batch, Selection::all(3), &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.calls.lock().unwrap(), trace[..=at]);
            let before = control.calls.lock().unwrap().clone();
            assert_eq!(
                instance
                    .evaluate(&batch, Selection::all(3), &control)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(*control.calls.lock().unwrap(), before);
        }
    }
}

#[test]
fn join_probe_owned_null_safe_key_retains_null_matches_with_original_evaluator() {
    struct NullMembership(Int64Membership);
    impl execution::RuntimeFilterArtifactQuery for NullMembership {
        fn data_type(&self) -> &DataType {
            self.0.data_type()
        }
        fn matches_null(&self) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
            Ok(true)
        }
        fn has_non_null_matches(&self) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
            self.0.has_non_null_matches()
        }
        fn non_null_value_may_match(
            &self,
            value: execution::RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
            self.0.non_null_value_may_match(value)
        }
        fn non_null_range_may_match(
            &self,
            lo: &ConnectorScalarValue,
            hi: &ConnectorScalarValue,
        ) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
            self.0.non_null_range_may_match(lo, hi)
        }
    }
    let consumer = StaticFilterConsumer::try_new(
        7,
        7,
        FilterConsumerActivation::BlockingSnapshot,
        membership(&DataType::Int64, FilterNullSemantics::NullSafeEqual),
        FilterReduction::SetUnion,
    )
    .unwrap();
    let program = join_with_probe_consumers(&single_join(true), vec![consumer]).unwrap();
    let subscription = ControlledSubscription::new();
    subscription.publish(execution::SnapshotAcquireOutcome::Published(Arc::new(
        execution::RuntimeFilterSnapshot::new(
            execution::RuntimeFilterBindingId::new(7),
            execution::LogicalVersion::FIRST,
            [0; 32],
            Arc::new(NullMembership(Int64Membership {
                accepted: BTreeSet::from([1, 2, 3, 4, 6]),
            })),
        ),
    )));
    assert_eq!(
        run_join(&program, FakeSession::consumers(&[(7, &subscription)])).unwrap(),
        sorted(oracle(hash_spec(true), &LEFT_ROWS, &RIGHT_ROWS))
    );
    assert_eq!(subscription.records(), vec!["published"]);
}

#[test]
fn join_probe_owned_contract_rejects_bad_ordinal_and_unsafe_local_join_direction() {
    let base = single_join(false);
    let program = join_with_probe_consumers(&base, vec![blocking_consumer(7)]).unwrap();
    let join = join_node(&program);
    for unsafe_kind in [
        novarocks_local_program::JoinType::LeftOuter,
        novarocks_local_program::JoinType::LeftAnti,
        novarocks_local_program::JoinType::NullAwareLeftAnti,
    ] {
        let mut kind = program.graph().nodes()[join.index()].kind().clone();
        let ProgramNodeKind::Join { join_type, .. } = &mut kind else {
            unreachable!()
        };
        *join_type = unsafe_kind;
        assert!(
            rebuild(
                &program,
                with_kind(&program, join, kind),
                Additions::default()
            )
            .is_err()
        );
    }
    let mut kind = program.graph().nodes()[join.index()].kind().clone();
    let ProgramNodeKind::Join {
        runtime_filter_consumers,
        ..
    } = &mut kind
    else {
        unreachable!()
    };
    runtime_filter_consumers[0].key_ordinal = usize::MAX;
    assert!(
        rebuild(
            &program,
            with_kind(&program, join, kind),
            Additions::default()
        )
        .is_err()
    );
}

#[test]
fn runtime_scalar_memory_actual_scan_driver_bind_funds_original_shift_key() {
    use crate::runtime::{
        fragment::ExecutionFailureCause, query_memory::QueryMemoryBinding,
        scalar_memory::RuntimeScalarMemoryRefusal,
    };
    use novarocks_memory::{AccountKind, ExternalRef};
    use novarocks_types::{
        QueryId,
        identity::{AttemptId, QueryExecutionId},
    };
    let (base, functions) =
        crate::exec::expr::runtime_scalar_memory_actual_sql_tests::scan_shift_base();
    let scan = *base.scan_inputs().keys().next().unwrap();
    let like = site(
        base.graph().root(),
        ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    );
    let definition = base
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot()
        .roots()
        .sites()[&like]
        .definition;
    let consumer = blocking_consumer(7);
    let ProgramNodeKind::Scan {
        source,
        residuals,
        limit,
        ..
    } = base.graph().nodes()[scan.index()].kind()
    else {
        panic!("actual Scan")
    };
    let program = rebuild(
        &base,
        with_kind(
            &base,
            scan,
            ProgramNodeKind::Scan {
                source: source.clone(),
                residuals: residuals.clone(),
                limit: *limit,
                runtime_filters: vec![FilterConsumerAtExpr {
                    expr_id: definition,
                    consumer: consumer.clone(),
                }],
            },
        ),
        Additions {
            requirements: vec![requirement(consumer.binding_id())],
            scalar_root: Some((
                site(
                    scan,
                    ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
                ),
                like,
                functions,
            )),
            ..Default::default()
        },
    )
    .unwrap();
    for funded in [false, true] {
        let subscription = ControlledSubscription::new();
        subscription.publish(accepting(7, &[14]));
        let state = state(
            Some(FakeSession::consumers(&[(7, &subscription)])),
            Duration::from_secs(5),
            None,
        );
        let authority = Arc::clone(state.execution_runtime().unwrap().memory_authority());
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let binding = QueryMemoryBinding::try_new(
            QueryExecutionId::new(QueryId::new(37, 304), AttemptId::new(1).unwrap()).unwrap(),
            authority,
            account,
        )
        .unwrap();
        let task = novarocks_execution_contract::TaskIdentity::new(
            binding.execution(),
            novarocks_types::identity::StageId::new(1).unwrap(),
            novarocks_types::identity::TaskId::new(1).unwrap(),
            novarocks_types::identity::BackendProcessId::new_v7(),
        );
        binding
            .validate_task(task, state.execution_runtime().unwrap().memory_authority())
            .unwrap();
        let state = state
            .as_ref()
            .clone()
            .with_query_memory(funded.then_some(binding));
        let layout = program.graph().nodes()[scan.index()].output_layout();
        let batch = arrow::record_batch::RecordBatch::try_new(
            layout.schema().clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![7, 3])) as arrow::array::ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(
            batch,
            crate::exec::chunk::ChunkSchema::from_compiled_layout(layout).unwrap(),
        )
        .unwrap();
        let op = FixtureScanOp::new(vec![chunk], true);
        let consumers =
            crate::exec::operators::runtime_filter::CompiledRuntimeFilterConsumers::try_new(
                "Scan",
                program.clone(),
                scan,
                vec![super::runtime_filter_consumer_contract(&consumer).unwrap()],
                state.error_state(),
            )
            .unwrap();
        let factory = StreamScanSourceFactory::new_compiled(
            SCAN_NODE,
            op.clone() as Arc<dyn ScanOp>,
            Some(Arc::new(consumers)),
        );
        let mut operator = factory.create(1, 0);
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        let result = operator.as_processor_mut().unwrap().pull_chunk(&state);
        if funded {
            let chunk = result
                .unwrap()
                .expect("real published membership delivers one row");
            assert_eq!(chunk.len(), 1);
            assert_eq!(
                chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(0),
                7
            );
        } else {
            assert_eq!(
                result.unwrap_err().cause(),
                &ExecutionFailureCause::RuntimeScalarMemory(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                )
            );
        }
        assert_eq!(
            op.claims(),
            1,
            "the real driver claimed its source exactly once"
        );
        operator.close().unwrap();
    }
}
