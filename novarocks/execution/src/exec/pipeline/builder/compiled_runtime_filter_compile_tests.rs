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

//! Compiled runtime filters authored by local-compiler and run end to end.
//!
//! The fragment is `SELECT v0, v1 FROM t JOIN (VALUES (8), (20), (99)) b ON
//! v0 = c0` on one instance. The physical plan carries one complete-once
//! membership filter: the join's build key produces it and the probe scan's
//! `v0` consumes it as a blocking scan-source filter. It is authored with the
//! real physical builders, extracted as a checked package whose cuts number
//! both endpoints, provider-validated and compiled by local-compiler, then
//! prepared by the compiled pipeline builder.
//!
//! The Task's runtime-filter session is a loopback double: it collects what
//! the join's producer submits and, when the test relays it, publishes exactly
//! that membership to the scan's subscription.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_connector_contract::*;
use novarocks_functions::ConstantPolicy;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    BindingRequirement, FilterConsumerActivation, KernelAbiVersion, LocalProgram, ProgramNodeKind,
};
use novarocks_physical_plan::*;
use novarocks_spi::connector::ConnectorScalarValue;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, PureCompileControl, control_argument_semantics,
};

use crate::exec::node::scan::ScanOp;
use crate::exec::operators::ResultSinkFactory;
use crate::exec::operators::ResultSinkHandle;
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution_with_profiler;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::{FragmentEvent, FragmentEventSink};
use crate::runtime::fragment::scan::compiled_fixture::{
    FixtureScanOp, SCAN_NODE, rows, scan_chunk,
};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter as execution;

const FRAGMENT: FragmentId = FragmentId::new(1);
const SCAN: NodeId = NodeId::new(10);
const JOIN: NodeId = NodeId::new(13);
const VALUES: NodeId = NodeId::new(30);
const FILTER: RuntimeFilterId = RuntimeFilterId::new(31);
const EQUALITY: RuntimeFilterEqualityWitnessId = RuntimeFilterEqualityWitnessId::new(41);
const WITNESS: RuntimeFilterWitnessId = RuntimeFilterWitnessId::new(51);
/// The fragment's slice of the plan numbering: the producer, then the
/// consumer.
const PRODUCER_BINDING: u32 = 1;
const CONSUMER_BINDING: u32 = 2;
/// The build keys the join's producer publishes.
const BUILD_KEYS: [i64; 3] = [8, 20, 99];
/// The scripted `(v0, v1)` rows, across a nonempty, an empty and a second
/// nonempty chunk.
const INPUT: [(i64, i64); 5] = [(1, 10), (8, 80), (9, 90), (7, 70), (20, 200)];

struct FixtureControl;
impl PureCompileControl for FixtureControl {
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

fn payload(category: ConnectorCodecCategory, value: &'static [u8]) -> ConnectorEncodedPayload {
    let binding = binding();
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

fn relation_payload() -> ConnectorReadRelationPayload {
    ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        payload(ConnectorCodecCategory::ReadTable, b"table"),
        payload(ConnectorCodecCategory::ReadView, b"view"),
    )
}

fn columns() -> [ProviderColumnReference; 2] {
    [b"c0" as &'static [u8], b"c1"].map(|bytes| ProviderColumnReference {
        column_payload: payload(ConnectorCodecCategory::ReadColumn, bytes),
    })
}

fn public_schema() -> Schema {
    Schema::new_with_metadata(
        ["v0", "v1"]
            .into_iter()
            .enumerate()
            .map(|(id, name)| {
                Field::new(name, DataType::Int64, false).with_metadata(HashMap::from([(
                    "provider.field-id".into(),
                    (id + 1).to_string(),
                )]))
            })
            .collect::<Vec<_>>(),
        HashMap::from([("provider.schema".into(), "generation-1".into())]),
    )
}

fn singleton() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

/// The join fragment, its one runtime filter and the frozen read of its scan.
fn plan() -> (PhysicalPlan, FrozenConnectorRead) {
    let columns = columns();
    let mut builder = FragmentBuilder::new(FRAGMENT);
    let scan = columns.clone().map(|field| {
        builder
            .add_value(
                int64(),
                ValueOrigin::ProviderField {
                    scan_node: SCAN,
                    field,
                },
            )
            .unwrap()
    });
    builder
        .add_scan(
            SCAN,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(Relation::Data(DataRelation {
                    read: ProviderReadReference {
                        binding: binding(),
                        input_version: ExactInputVersion::try_new([9]).unwrap(),
                        relation: relation_payload(),
                    },

                    selection_digest: [7; 32],
                    schema: columns
                        .iter()
                        .map(|column| RelationField {
                            column: column.clone(),
                            ty: int64(),
                        })
                        .collect(),
                    predicate_guarantees: Box::default(),
                    provided_properties: singleton(),
                })),
                read_budget: ScanReadBudget {
                    max_batch_rows: 100,
                    max_batch_bytes: 4096,
                },
                provider_outputs: columns.iter().cloned().zip(scan).collect(),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
            Box::from(scan),
        )
        .unwrap();
    let build = builder
        .add_value(
            int64(),
            ValueOrigin::NodeOutput {
                node: VALUES,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let cells = BUILD_KEYS
        .map(|key| {
            Box::from([builder
                .add_expression(VALUES, int64(), ExprKind::Literal(LiteralValue::Int64(key)))
                .unwrap()])
        })
        .into_iter()
        .collect();
    builder
        .add_values(VALUES, cells, Box::from([build]))
        .unwrap();
    let key = JoinKey {
        left: builder
            .add_expression(JOIN, int64(), ExprKind::Value(scan[0]))
            .unwrap(),
        right: builder
            .add_expression(JOIN, int64(), ExprKind::Value(build))
            .unwrap(),
        null_safe: false,
    };
    builder
        .add_join(
            JOIN,
            [SCAN, VALUES],
            Box::from([singleton(), singleton()]),
            Box::from(scan),
            Distribution::Singleton,
            NodeKind::HashJoin {
                kind: JoinKind::Inner,
                keys: Box::from([key]),
                build_side: JoinSide::Right,
                distribution: JoinDistribution::Singleton,
                residual: None,
                null_extended: Box::default(),
            },
        )
        .unwrap();
    builder.attach_runtime_filter(FILTER).unwrap();
    let fragment = builder
        .finish_definition(
            JOIN,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 4,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FRAGMENT,
        output: OutputPort {
            node: JOIN,
            columns: Box::from(scan),
        },
        fields: scan
            .iter()
            .zip(["a", "b"])
            .map(|(value, label)| ResultField {
                domain: crate::test_result_domain::result_value_domain(&int64()),
                name: label.into(),
                alias: None,
                value: *value,
                ty: int64(),
            })
            .collect(),
    })
    .unwrap();
    // A singleton join has one build, so one producer witness covers it.
    let coverage = RuntimeFilterCoverage {
        nodes: Box::from([RuntimeFilterCoverageNode::Witness(WITNESS)]),
        root: 0,
    };
    plan.add_runtime_filter(RuntimeFilter {
        id: FILTER,
        kind: RuntimeFilterKind::InList,
        domain: RuntimeFilterDomain::Membership {
            ty: int64(),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::SetUnion,
        availability_coverage: coverage.clone(),
        terminal_coverage: coverage,
        equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
            id: EQUALITY,
            fragment: FRAGMENT,
            join: JOIN,
            key_ordinal: 0,
            domain_side: JoinSide::Right,
        }]),
        producers: Box::from([RuntimeFilterProducer {
            witness: WITNESS,
            endpoint: RuntimeFilterEndpoint {
                fragment: FRAGMENT,
                node: JOIN,
                values: Box::from([build]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::ValueDomainDelta,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::ProducerClosed,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality: EQUALITY },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: FRAGMENT,
                node: SCAN,
                values: Box::from([scan[0]]),
            },
            apply_point: RuntimeFilterApplyPoint::ScanSource,
            capabilities: Box::from([
                RuntimeFilterArtifactCapability::Membership,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ]),
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::ScanField {
                equality: EQUALITY,
                lineage: Box::default(),
            },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1 << 20,
            max_artifact_bytes: 1 << 20,
            deadline_ms: 60_000,
            max_retries: 1,
        },
    })
    .unwrap();
    let plan = plan
        .finish()
        .unwrap_or_else(|error| panic!("the runtime-filter join plan validates: {error:?}"));

    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding(),
        relation_payload(),
        columns
            .iter()
            .map(|column| column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    // The scan's frozen dynamic filter names the filter its consumer reads.
    let scan = FrozenConnectorScan::try_new(
        draft,
        ["v0", "v1"]
            .into_iter()
            .map(|name| StaticScanAssignment::new(Arc::from(name), ConnectorValueType::BigInt))
            .collect(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![StaticScanDynamicFilter::new(FILTER.get(), Arc::from("v0"))],
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
    )
    .unwrap();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new([9]).unwrap(),
        [7; 32],
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Singleton, vec![]).unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        vec![],
    )
    .unwrap();
    let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
    let schema = Arc::new(Schema::new(vec![
        Field::new("v0", DataType::Int64, false),
        Field::new("v1", DataType::Int64, false),
    ]));
    let execution = ConnectorReadExecutionSchema::try_new(
        schema.clone(),
        vec![int64(), int64()].into_boxed_slice(),
        storage.clone(),
    )
    .unwrap();
    let original_source = public_schema();
    let shape = ConnectorSourceSchemaShape::try_new(schema, storage.clone()).unwrap();
    let source_fields = [0_u32, 1]
        .into_iter()
        .map(|ordinal| {
            ConnectorSourceFieldFacts::try_new(
                vec![ordinal].into_boxed_slice(),
                Some((ordinal + 1) as i32),
                None,
                HashMap::from([("provider.field-id".into(), (ordinal + 1).to_string())]),
                &[],
                storage.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let source_schema = ConnectorSourceSchemaFacts::try_new(
        shape,
        std::collections::HashMap::from(original_source.metadata()),
        source_fields,
        storage.clone(),
    )
    .unwrap();
    let public =
        ConnectorReadPublicFacts::try_new_typed(source, None, execution, source_schema, storage)
            .unwrap();
    (plan, FrozenConnectorRead::try_new(scan, public).unwrap())
}

/// One complete eager use tree per physical root site in one root domain.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    struct Author<'a> {
        fragment: &'a Fragment,
        next: u32,
        uses: Vec<ExpressionInvocation<ExprId>>,
    }
    impl Author<'_> {
        fn visit(&mut self, expr: ExprId, demand: EvaluationDemand) -> ExpressionUseId {
            let id = ExpressionUseId::new(self.next);
            self.next += 1;
            let args = match &self.fragment.expressions().get(expr).unwrap().kind {
                ExprKind::Binary { left, right, .. } => vec![*left, *right],
                ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
                other => panic!("the join fixture has no {other:?}"),
            };
            let arguments = args
                .iter()
                .enumerate()
                .map(|(ordinal, &child)| {
                    let (child_demand, guard) = control_argument_semantics(
                        ControlShape::Eager,
                        args.len(),
                        ordinal,
                        demand,
                    )
                    .unwrap();
                    assert!(guard.is_none());
                    self.visit(child, child_demand)
                })
                .collect::<Box<[_]>>();
            self.uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain: EvaluationDomainId::new(0),
                    demand,
                },
                definition: expr,
                control: ControlShape::Eager,
                arguments,
            });
            id
        }
    }
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let mut author = Author {
        fragment,
        next: 0,
        uses: vec![],
    };
    let bindings = roots
        .sites()
        .iter()
        .map(|(&site, root)| (site, author.visit(root.expr, root.demand)))
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        author.uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap()
}

/// Keeps the complete frozen read, private bytes included; the public facts
/// are borrowed unchanged.
struct Port;
impl ConnectorReadProgramCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        work.step()?;
        work.finish()?;
        Ok(input.scan().recipe().clone())
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
        &FixtureControl,
    )
    .unwrap()
}

/// Extract the one package and compile it for `dop` drivers with local-compiler.
fn compile(dop: usize) -> Arc<LocalProgram> {
    let (plan, read) = plan();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let root_uses = root_uses(fragment);
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &FixtureControl).unwrap(),
        );
        uses.insert(id, root_uses);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
        );
        // Conservative retained-source invoice and independent projection
        // ceilings for this small fixture only; not a production default.
        admissions.insert(
            id,
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 64 * 1024 * 1024,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 16 * 1024 * 1024,
                    max_coexisting_bytes: 256 * 1024 * 1024,
                    max_projection_work: 16 * 1024 * 1024,
                },
            },
        );
    }
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::from([(ProviderReadOccurrenceId::new(0), read)]),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("the runtime-filter package extracts: {error:?}"));
    let package = Arc::new(packages.remove(&FRAGMENT).unwrap());
    let validated = validate_fragment_providers(package, &providers(), &FixtureControl).unwrap();
    let functions = crate::exec::expr::compiled_program::tests::rng_subset();
    let options = LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
        // The result is published by one root sink driver.
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: Duration::from_secs(120),
        // Explicit fixture admission; these values are not production defaults.
        constants: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
    };
    Arc::new(
        compile_fragment(validated, &functions, options, &FixtureControl)
            .unwrap_or_else(|error| panic!("the runtime-filter fragment compiles: {error}")),
    )
}

// ---------------------------------------------------------------------------
// The loopback runtime-filter session.

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

/// The scan's blocking subscription: pending until the session publishes.
struct Subscription {
    outcome: Mutex<Option<execution::SnapshotAcquireOutcome>>,
    published: Arc<Observable>,
    records: Mutex<Vec<&'static str>>,
}

impl execution::BlockingSnapshotSubscription for Subscription {
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

/// The join's producer: the decoded membership it submitted, and whether
/// its one partition closed.
#[derive(Default)]
struct Producer {
    values: Mutex<BTreeSet<i64>>,
    submits: Mutex<u64>,
    closed: Mutex<Option<(u32, u64)>>,
    failed: Mutex<Vec<execution::RuntimeFilterProducerFailure>>,
}

impl execution::RuntimeFilterProducer for Producer {
    fn max_contribution_bytes(&self) -> usize {
        1 << 20
    }

    fn submit(
        &self,
        _: execution::PartitionId,
        _: execution::ProducerSequence,
        contribution: execution::RuntimeFilterContribution,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        let digest = contribution.contract_digest();
        let decoded = execution::contribution::decode_contribution(
            contribution.canonical_bytes(),
            &digest,
            execution::contribution::ContributionCodecExpectation::membership(
                &DataType::Int64,
                digest,
            ),
            usize::MAX,
        )
        .expect("a canonical membership contribution");
        let execution::contribution::RuntimeFilterContribution::Membership(domain) = decoded else {
            panic!("a membership producer submits membership: {decoded:?}");
        };
        let execution::contribution::MembershipValues::Int64(submitted) = domain.values() else {
            panic!("an Int64 build key submits Int64 values");
        };
        self.values
            .lock()
            .expect("values")
            .extend(submitted.iter().copied());
        *self.submits.lock().expect("submits") += 1;
        Ok(execution::RuntimeFilterSubmitOutcome::Applied)
    }

    fn close_partition(
        &self,
        partition: execution::PartitionId,
        sequence: execution::ProducerSequence,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        *self.closed.lock().expect("closed") = Some((partition.get(), sequence.get()));
        Ok(execution::RuntimeFilterSubmitOutcome::Completed)
    }

    fn fail(
        &self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        self.failed.lock().expect("failed").push(reason);
        Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop)
    }
}

/// Binds exactly the program's two bindings: the join's producer and the
/// scan's subscription, and records every contract it was asked for.
struct LoopbackSession {
    producer: Arc<Producer>,
    subscription: Arc<Subscription>,
    opened: Mutex<Vec<execution::RuntimeFilterProducerContract>>,
    subscribed: Mutex<Vec<execution::RuntimeFilterConsumerContract>>,
}

impl LoopbackSession {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            producer: Arc::default(),
            subscription: Arc::new(Subscription {
                outcome: Mutex::new(None),
                published: Arc::new(Observable::new()),
                records: Mutex::new(Vec::new()),
            }),
            opened: Mutex::new(Vec::new()),
            subscribed: Mutex::new(Vec::new()),
        })
    }

    /// Wait until the producer closed its partition.
    fn await_close(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.producer.closed.lock().expect("closed").is_none() {
            assert!(
                Instant::now() < deadline,
                "the build never closed its producer"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Publish to the consumer exactly the membership the producer submitted.
    fn publish(&self) -> BTreeSet<i64> {
        let accepted = self.producer.values.lock().expect("values").clone();
        let snapshot = execution::RuntimeFilterSnapshot::new(
            execution::RuntimeFilterBindingId::new(CONSUMER_BINDING),
            execution::LogicalVersion::FIRST,
            [0; 32],
            Arc::new(Int64Membership {
                accepted: accepted.clone(),
            }),
        );
        *self.subscription.outcome.lock().expect("outcome") = Some(
            execution::SnapshotAcquireOutcome::Published(Arc::new(snapshot)),
        );
        self.subscription.published.notify_observers();
        accepted
    }
}

fn unauthorized(detail: &'static str) -> execution::RuntimeFilterContractViolation {
    execution::RuntimeFilterContractViolation::new(
        execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
        detail,
    )
}

impl execution::RuntimeFilterSession for LoopbackSession {
    fn open_producer(
        &self,
        request: execution::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterProducerHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        if request.contract().binding_id().get() != PRODUCER_BINDING {
            return Err(unauthorized("the loopback session has no such producer"));
        }
        self.opened
            .lock()
            .expect("opened")
            .push(request.contract().clone());
        Ok(execution::RuntimeFilterBindOutcome::Bound(
            Arc::clone(&self.producer) as execution::RuntimeFilterProducerHandle,
        ))
    }

    fn subscribe(
        &self,
        request: execution::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        if request.contract().binding_id().get() != CONSUMER_BINDING {
            return Err(unauthorized(
                "the loopback session has no such subscription",
            ));
        }
        self.subscribed
            .lock()
            .expect("subscribed")
            .push(request.contract().clone());
        Ok(execution::RuntimeFilterBindOutcome::Bound(
            execution::RuntimeFilterSubscriptionHandle::Blocking(
                Arc::clone(&self.subscription) as Arc<dyn execution::BlockingSnapshotSubscription>
            ),
        ))
    }

    fn open_final_domain_completion(
        &self,
        _: execution::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterFinalDomainCompletionHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        Err(unauthorized(
            "the loopback session has no final-domain completion",
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

fn state(session: Arc<LoopbackSession>) -> Arc<RuntimeState> {
    Arc::new(
        RuntimeState::for_local_execution(
            Some(QueryOptions {
                runtime_filter_wait_timeout_ms: Some(60_000),
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            Some(crate::runtime::mem_tracker::MemTracker::new_root(
                "compiled runtime filter compile tests",
            )),
            Some(crate::runtime::execution_runtime::test_execution_runtime()),
        )
        .with_runtime_filter_session(Some(session as execution::RuntimeFilterSessionRef)),
    )
}

fn input(program: &LocalProgram) -> Vec<crate::exec::chunk::Chunk> {
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

// The compiled program the local compiler authored carries one producer site
// on the join and one blocking consumer site on the scan, each with its one
// requirement. Prepared with a loopback session, the join's build publishes
// the membership of its build keys; the scan holds its first read until that
// membership is published, then filters every row it reads by it.
#[test]
fn a_compiled_join_membership_gates_and_filters_its_compiled_probe_scan() {
    for dop in [1, 2] {
        let program = compile(dop);
        let graph = program.graph();
        let producers = graph
            .nodes()
            .iter()
            .filter_map(|node| match node.kind() {
                ProgramNodeKind::Join {
                    runtime_filters, ..
                } => Some(runtime_filters.len()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(producers, vec![1], "dop {dop}: one join producer site");
        let required = graph
            .requirements()
            .entries()
            .iter()
            .filter_map(|requirement| match requirement {
                BindingRequirement::RuntimeFilter { binding_id } => Some(*binding_id),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            required,
            BTreeSet::from([PRODUCER_BINDING as i32, CONSUMER_BINDING as i32])
        );

        let op = FixtureScanOp::new(input(&program), false);
        let mut bindings = ScanBindings::default();
        bindings.insert(SCAN_NODE, Arc::clone(&op) as Arc<dyn ScanOp>);
        let session = LoopbackSession::new();
        let events = Arc::new(RecordingEvents::default());
        let output = ResultSinkHandle::new();
        let running = prepare_compiled_program_pipeline_execution_with_profiler(
            Arc::clone(&program),
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(output.clone())),
            ExchangeBindings::default(),
            bindings,
            crate::runtime::fragment::CompiledWriterBindings::default(),
            None,
            None,
            i32::try_from(dop).unwrap(),
            state(Arc::clone(&session)),
            Arc::clone(&events) as Arc<dyn FragmentEventSink>,
        )
        .unwrap_or_else(|error| panic!("dop {dop}: the compiled join prepares: {error}"))
        .start();

        // The build completes and closes its producer while the probe scan
        // still holds its first read for the unpublished filter.
        session.await_close();
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            op.claims(),
            0,
            "dop {dop}: the pending filter holds the scan's first read"
        );
        assert!(session.subscription.records.lock().unwrap().is_empty());

        let published = session.publish();
        running.join().expect("the compiled join runs");
        assert_eq!(
            published,
            BTreeSet::from(BUILD_KEYS),
            "dop {dop}: the producer submitted exactly its build keys"
        );
        let submits = *session.producer.submits.lock().unwrap();
        assert!(submits > 0, "dop {dop}: the build submitted its keys");
        assert_eq!(
            *session.producer.closed.lock().unwrap(),
            Some((0, submits)),
            "dop {dop}: the one build partition closes after its submissions"
        );
        assert!(session.producer.failed.lock().unwrap().is_empty());

        let mut actual = rows(&output.take_chunks());
        actual.sort_unstable();
        assert_eq!(actual, vec![(8, 80), (20, 200)], "dop {dop}");
        assert_eq!(op.claims(), 1, "dop {dop}: one driver owns the scan stream");
        assert_eq!(
            *session.subscription.records.lock().unwrap(),
            vec!["published"],
            "dop {dop}"
        );

        // The scan itself filtered its rows by the published membership, one
        // evaluated effect per chunk it read, the empty one included.
        let effects = events
            .effects
            .lock()
            .unwrap()
            .iter()
            .map(|effect| {
                (
                    effect.binding_id().get(),
                    effect.input_rows(),
                    effect.output_rows(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            effects,
            vec![
                (CONSUMER_BINDING, 3, 1),
                (CONSUMER_BINDING, 0, 0),
                (CONSUMER_BINDING, 2, 1)
            ],
            "dop {dop}"
        );

        // Both sites bound the contracts the compiler froze.
        let opened = session.opened.lock().unwrap();
        assert_eq!(opened.len(), 1, "dop {dop}");
        assert_eq!(opened[0].binding_id().get(), PRODUCER_BINDING);
        assert_eq!(opened[0].channel_id().get(), FILTER.get());
        assert_eq!(
            opened[0].kind(),
            execution::RuntimeFilterProducerKind::Membership
        );
        let subscribed = session.subscribed.lock().unwrap();
        assert_eq!(subscribed.len(), 1, "dop {dop}");
        assert_eq!(subscribed[0].binding_id().get(), CONSUMER_BINDING);
        assert_eq!(subscribed[0].channel_id().get(), FILTER.get());
        assert_eq!(
            subscribed[0].activation(),
            execution::ConsumerActivation::BlockingSnapshot
        );
    }
}

// The scan consumer the compiler lowered is the BlockingSnapshot membership
// shape the compiled scan source executes.
#[test]
fn the_compiled_scan_consumer_is_a_blocking_membership_site() {
    let program = compile(1);
    let consumers = program
        .graph()
        .nodes()
        .iter()
        .filter_map(|node| match node.kind() {
            ProgramNodeKind::Scan {
                runtime_filters, ..
            } => Some(runtime_filters),
            _ => None,
        })
        .flatten()
        .map(|site| {
            (
                site.consumer.binding_id(),
                site.consumer.channel_id(),
                site.consumer.activation(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        consumers,
        vec![(
            CONSUMER_BINDING,
            FILTER.get(),
            FilterConsumerActivation::BlockingSnapshot
        )]
    );
}
