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

//! Task binding of a compiled program's provider scans.
//!
//! A compiled Scan carries its whole frozen read as one provider-validated
//! recipe: the binding, relation and column payloads, the ordered
//! assignments, the predicate responsibilities and the public facts. This
//! binder turns each recipe into the Task's typed scan source. It reads no
//! generated DTO and seals nothing again: the relation and columns are
//! decoded from the recipe's own payloads by the installed read execution of
//! exactly the recipe's binding, and the source's output is the compiled
//! layout, which is the provider's public schema.
//!
//! Splits stay runtime work. The source polls the Task's split queue of its
//! physical scan node, and that node is registered with the Task's read
//! context so split delivery decodes against the same installed execution.
//!
//! A scan with runtime-filter consumers is bound too. Its frozen dynamic
//! filters, keyed by runtime-filter id, must correspond one to one to its
//! scan-source consumer bindings through the package's binding table. This
//! milestone prunes nothing at the connector: the compiled scan source
//! applies the consumers to rows, and the connector is handed the truthful
//! complete-all filter, which never narrows a read.

use novarocks_execution::runtime::task::ExecutionFailure;
use novarocks_execution::runtime::task::connector_failure::{
    ConnectorCallFailure, ConnectorOperation,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use novarocks_connector_contract::{
    ConnectorReadDistribution, ConnectorReadProgramRecipe, ConnectorReadRelationKind,
    FrozenConnectorRead, FrozenConnectorScan, ScanColumnId,
};
use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_execution::exec::node::scan::ScanSource;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution::runtime::task::scan::CompiledScanSources;
use novarocks_execution::runtime_filter::{
    RuntimeFilterConsumerContract, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind, RuntimeFilterSessionRef,
};
use novarocks_local_program::{FilterConsumerAtExpr, LocalProgram, ProgramNodeKind, StaticLayout};
use novarocks_spi::connector::read_stack::runtime::ConnectorReadAssignment;
use novarocks_spi::connector::read_stack::{
    Assignment, CompleteAllDynamicFilter, ConnectorDataCacheOptions,
    ConnectorPageSourceProviderOptions, ConnectorReadColumnHandle, ConnectorReadDynamicFilter,
    ConnectorSourceOperations,
};
use novarocks_spi::connector::{
    ConnectorRangeScope, ConnectorRequestContext, ConnectorStopView,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_types::UniqueId;
use novarocks_worker::typed_connector_runtime::TypedConnectorScanSource;
use novarocks_worker::typed_scan_filter::TypedScanLiveDynamicFilterFactory;
use novarocks_worker::{TypedConnectorReadDescriptor, TypedScanRuntime};

use crate::compiled_runtime_filter::{
    CompiledRuntimeFilterEndpoints, validate_scan_dynamic_filters,
};

/// The Task facts every compiled scan of one task binds with.
pub(crate) struct CompiledScanTaskFacts<'a> {
    pub(crate) runtime: &'a TypedScanRuntime,
    #[cfg(novarocks_dev_controls)]
    pub(crate) observation_controls:
        Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    pub(crate) task_key: UniqueId,
    pub(crate) query_options: &'a QueryOptions,
    pub(crate) stop: ConnectorStopView,
    /// The runtime-filter bindings the task's package numbers.
    pub(crate) runtime_filters: &'a CompiledRuntimeFilterEndpoints,
}

/// Why a compiled scan could not be bound to its Task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledScanBindingError {
    scan_node: Option<i32>,
    source: ExecutionFailure,
}
impl CompiledScanBindingError {
    fn at_failure(scan_node: i32, source: ExecutionFailure) -> Self {
        Self {
            scan_node: Some(scan_node),
            source,
        }
    }
    fn at_domain<E>(scan_node: i32, source: E) -> Self
    where
        E: std::error::Error
            + novarocks_execution_contract::failure::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        Self::at_failure(scan_node, ExecutionFailure::from_domain(source))
    }
    fn program_failure(source: ExecutionFailure) -> Self {
        Self {
            scan_node: None,
            source,
        }
    }
    pub const fn scan_node(&self) -> Option<i32> {
        self.scan_node
    }
    pub fn detail(&self) -> &str {
        self.source.detail()
    }
}
impl fmt::Display for CompiledScanBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.scan_node {
            Some(node) => write!(f, "compiled scan node {node}: {}", self.source),
            None => self.source.fmt(f),
        }
    }
}
impl std::error::Error for CompiledScanBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for CompiledScanBindingError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        self.source.failure_fact()
    }
}

#[derive(Clone, Copy, Debug)]
enum ScanOrdinalFact {
    EnforcedPredicate,
    UnenforcedPredicate,
    Distribution,
    Ordering,
}
impl fmt::Display for ScanOrdinalFact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EnforcedPredicate => "enforced predicate",
            Self::UnenforcedPredicate => "unenforced predicate",
            Self::Distribution => "distribution key",
            Self::Ordering => "ordering key",
        })
    }
}
#[derive(Debug)]
enum ScanBindingContractFailure {
    MissingNode(usize),
    DisplayRange {
        node: u32,
        source: std::num::TryFromIntError,
    },
    WrongNode(usize),
    MissingRecipe,
    UnsupportedRelation(ConnectorReadRelationKind),
    NegativeNode {
        node: i32,
        source: std::num::TryFromIntError,
    },
    LayoutArity {
        slots: usize,
        assignments: usize,
    },
    WrongBinding,
    WrongRelation {
        decoded: ConnectorReadRelationKind,
        expected: ConnectorReadRelationKind,
    },
    MissingOrdinal {
        fact: ScanOrdinalFact,
        ordinal: usize,
    },
    NonFirstOrdinal {
        fact: ScanOrdinalFact,
        ordinal: usize,
        first: usize,
    },
    Observation(novarocks_execution_contract::failure::BoundedDetail),
}
impl fmt::Display for ScanBindingContractFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode(node) => {
                write!(f, "compiled scan address names absent local node {node}")
            }
            Self::DisplayRange { node, .. } => write!(f, "compiled scan node {node} exceeds i32"),
            Self::WrongNode(node) => write!(f, "local node {node} is not a Scan"),
            Self::MissingRecipe => f.write_str("the Scan carries no compiled provider read"),
            Self::UnsupportedRelation(kind) => write!(
                f,
                "provider relation {kind:?} is not read by a compiled scan"
            ),
            Self::NegativeNode { node, .. } => write!(f, "compiled scan node {node} is negative"),
            Self::LayoutArity { slots, assignments } => write!(
                f,
                "the compiled layout has {slots} slots for {assignments} assignments"
            ),
            Self::WrongBinding => {
                f.write_str("compiled scan binding does not match the installed read execution")
            }
            Self::WrongRelation { decoded, expected } => write!(
                f,
                "provider decoded relation {decoded:?} from a frozen {expected:?} relation"
            ),
            Self::MissingOrdinal { fact, ordinal } => {
                write!(f, "{fact} names absent scan ordinal {ordinal}")
            }
            Self::NonFirstOrdinal {
                fact,
                ordinal,
                first,
            } => write!(
                f,
                "{fact} is stated at ordinal {ordinal} of a provider column first assigned at ordinal {first}"
            ),
            Self::Observation(detail) => write!(f, "compiled scan observation refused: {detail}"),
        }
    }
}
impl std::error::Error for ScanBindingContractFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DisplayRange { source, .. } | Self::NegativeNode { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for ScanBindingContractFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let code = match self {
            Self::MissingNode(_) => 9700,
            Self::DisplayRange { .. } => 9701,
            Self::WrongNode(_) => 9702,
            Self::MissingRecipe => 9703,
            Self::UnsupportedRelation(_) => 9704,
            Self::NegativeNode { .. } => 9706,
            Self::LayoutArity { .. } => 9707,
            Self::WrongBinding => 9708,
            Self::WrongRelation { .. } => 9709,
            Self::MissingOrdinal { .. } => 9710,
            Self::NonFirstOrdinal { .. } => 9711,
            Self::Observation(_) => 9712,
        };
        let class = match self {
            Self::UnsupportedRelation(_) => FailureClass::Dependency,
            Self::Observation(_) => FailureClass::Unexpected,
            _ => FailureClass::Invariant,
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// Bind every compiled Scan of `program` to the Task: one typed scan source
/// per Scan node, keyed by its local node. Every refusal precedes the first
/// read-execution registration of its scan.
pub(crate) fn bind_compiled_scans(
    program: &LocalProgram,
    task: &CompiledScanTaskFacts<'_>,
) -> Result<CompiledScanSources, CompiledScanBindingError> {
    let nodes = program.graph().nodes();
    let mut sources = CompiledScanSources::new();
    for (id, input) in program.scan_inputs() {
        let node = nodes.get(id.index()).ok_or_else(|| {
            CompiledScanBindingError::program_failure(ExecutionFailure::from_domain(
                ScanBindingContractFailure::MissingNode(id.index()),
            ))
        })?;
        let scan_node = i32::try_from(input.scan_node).map_err(|source| {
            CompiledScanBindingError::program_failure(ExecutionFailure::from_domain(
                ScanBindingContractFailure::DisplayRange {
                    node: input.scan_node,
                    source,
                },
            ))
        })?;
        let ProgramNodeKind::Scan {
            source,
            runtime_filters,
            ..
        } = node.kind()
        else {
            return Err(CompiledScanBindingError::at_domain(
                scan_node,
                ScanBindingContractFailure::WrongNode(id.index()),
            ));
        };
        let recipe = source.compiled().ok_or_else(|| {
            CompiledScanBindingError::at_domain(
                scan_node,
                ScanBindingContractFailure::MissingRecipe,
            )
        })?;
        let bound = bind_compiled_scan(
            scan_node,
            recipe,
            runtime_filters,
            node.output_layout(),
            task,
        )?;
        sources.insert(*id, bound);
    }
    Ok(sources)
}

/// Bind one compiled provider read, whose scan's runtime-filter consumer
/// sites are `consumers`, as the typed scan source of `scan_node`.
pub(crate) fn bind_compiled_scan(
    scan_node: i32,
    recipe: &ConnectorReadProgramRecipe,
    consumers: &[FilterConsumerAtExpr],
    layout: &StaticLayout,
    task: &CompiledScanTaskFacts<'_>,
) -> Result<Arc<dyn ScanSource>, CompiledScanBindingError> {
    let frozen: &FrozenConnectorRead = recipe.frozen();
    let scan = frozen.scan();
    let draft = scan.recipe();

    // Everything the recipe and this backend must agree on is decided from
    // the recipe alone, before any installed provider is consulted, so a read
    // this slice cannot run is refused on its own terms.
    match draft.relation().kind() {
        ConnectorReadRelationKind::Table
        | ConnectorReadRelationKind::ChangeWindow
        | ConnectorReadRelationKind::SystemTable
        | ConnectorReadRelationKind::TableExecute => {}
        kind @ (ConnectorReadRelationKind::TableFunction
        | ConnectorReadRelationKind::MergeTable) => {
            return Err(CompiledScanBindingError::at_domain(
                scan_node,
                ScanBindingContractFailure::UnsupportedRelation(kind),
            ));
        }
    }
    // Each frozen dynamic filter names its consumer binding through the
    // package's table; the source applies the consumers, the connector
    // prunes nothing.
    let physical_node = u32::try_from(scan_node).map_err(|source| {
        CompiledScanBindingError::at_domain(
            scan_node,
            ScanBindingContractFailure::NegativeNode {
                node: scan_node,
                source,
            },
        )
    })?;
    validate_scan_dynamic_filters(
        physical_node,
        scan.dynamic_filters(),
        consumers,
        task.runtime_filters,
    )
    .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    // `slot_ids[i]` names page channel `i`, and a channel exists for each
    // assignment, so the layout is exactly one slot per assignment.
    if layout.slots().len() != scan.assignments().len() {
        return Err(CompiledScanBindingError::at_domain(
            scan_node,
            ScanBindingContractFailure::LayoutArity {
                slots: layout.slots().len(),
                assignments: scan.assignments().len(),
            },
        ));
    }
    let output_schema = ChunkSchema::from_compiled_layout(layout)
        .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;

    let execution = task
        .runtime
        .catalog_read_execution(draft.binding().catalog_handle())
        .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    if draft.binding() != execution.binding() {
        return Err(CompiledScanBindingError::at_domain(
            scan_node,
            ScanBindingContractFailure::WrongBinding,
        ));
    }
    let decoder = execution.decoder();
    let relation = decoder
        .decode_relation_payload(draft.relation())
        .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    if relation.kind() != draft.relation().kind() {
        return Err(CompiledScanBindingError::at_domain(
            scan_node,
            ScanBindingContractFailure::WrongRelation {
                decoded: relation.kind(),
                expected: draft.relation().kind(),
            },
        ));
    }
    let columns = draft
        .columns()
        .iter()
        .map(|payload| decoder.decode_column_payload(payload))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    require_first_ordinal_facts(frozen, &columns)
        .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    let assignments = scan
        .assignments()
        .iter()
        .zip(columns)
        .map(|(assignment, column)| {
            Assignment::try_new(assignment.variable(), column, assignment.value_type())
        })
        .collect::<Result<Vec<ConnectorReadAssignment>, _>>()
        .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    let dynamic_filter = complete_all_dynamic_filter(scan, &assignments);
    let descriptor =
        TypedConnectorReadDescriptor::new(relation.table().clone(), assignments, dynamic_filter);

    let execution_id = task.runtime.execution_id();
    let range_scope = ConnectorRangeScope::try_new(
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get(),
        task.task_key.high(),
        task.task_key.low(),
        scan_node,
    )
    .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    let (_, query_expire) = novarocks_execution::runtime::query_options::query_expire_durations(
        Some(task.query_options),
    );
    // One execution source per Task and scan node: its requests' I/O is
    // scheduled under this scope and admitted to these operations.
    let request = ConnectorRequestContext::try_new(
        std::time::Instant::now() + query_expire,
        task.stop.clone(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|request| {
        request
            .with_storage_resolver(task.runtime.storage_resolver())
            .with_execution_source(range_scope, ConnectorSourceOperations::new())
    })
    .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    let reader_policy = reader_policy(task.query_options)
        .map_err(|source| CompiledScanBindingError::at_failure(scan_node, source))?;

    task.runtime
        .register_read_execution(scan_node, execution.clone())
        .map_err(|source| CompiledScanBindingError::at_domain(scan_node, source))?;
    // The provider is opened only when the source is bound, after the Task's
    // admission installed its fragment tracker.
    let provider_factory = execution.provider_factory();
    let runtime = task.runtime.clone();
    let provider_request = request.clone();
    let page_source_provider = Arc::new(move || {
        let resources =
            runtime.admitted_connector_resources_for_bound_task(runtime.execution_id())?;
        provider_factory
            .create_page_source_provider(&provider_request, resources, reader_policy)
            .map_err(|error| {
                ExecutionFailure::from(ConnectorCallFailure::new(
                    ConnectorOperation::OpenReader,
                    error,
                ))
            })
    });
    Ok(Arc::new(TypedConnectorScanSource::new_deferred(
        descriptor,
        page_source_provider,
        task.runtime.session(),
        request,
        task.runtime.queues(),
        scan_node,
        layout.slots().to_vec(),
        output_schema,
        task.runtime.runtime_filter(),
        Arc::new(NoLiveDynamicFilter),
        #[cfg(novarocks_dev_controls)]
        task.observation_controls
            .as_deref()
            .map(|controls| {
                controls.observation_for(
                    novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker
                        .as_str(),
                    task.runtime.execution_id(),
                )
            })
            .transpose()
            .map_err(|detail| {
                CompiledScanBindingError::at_domain(
                    scan_node,
                    ScanBindingContractFailure::Observation(
                        novarocks_execution_contract::failure::BoundedDetail::truncating(&detail),
                    ),
                )
            })?
            .flatten(),
        task.runtime.stream_host().clone(),
    )))
}

/// A provider column assigned at several ordinals is one column, and the
/// frozen read states each fact about it once, at the first ordinal that
/// assigns it; every later ordinal reads the same values. A predicate domain
/// or a read property addressed to a later ordinal of a decoded column is
/// therefore not that statement: it is refused, never merged onto the column.
fn require_first_ordinal_facts(
    frozen: &FrozenConnectorRead,
    columns: &[ConnectorReadColumnHandle],
) -> Result<(), ScanBindingContractFailure> {
    let mut first = BTreeMap::<&ConnectorReadColumnHandle, ScanColumnId>::new();
    for (ordinal, column) in columns.iter().enumerate() {
        first.entry(column).or_insert(ScanColumnId::new(ordinal));
    }
    let check =
        |fact: ScanOrdinalFact, ordinal: ScanColumnId| -> Result<(), ScanBindingContractFailure> {
            let column =
                columns
                    .get(ordinal.index())
                    .ok_or(ScanBindingContractFailure::MissingOrdinal {
                        fact,
                        ordinal: ordinal.index(),
                    })?;
            let canonical = first[column];
            if canonical != ordinal {
                return Err(ScanBindingContractFailure::NonFirstOrdinal {
                    fact,
                    ordinal: ordinal.index(),
                    first: canonical.index(),
                });
            }
            Ok(())
        };
    let scan = frozen.scan();
    for (fact, domain) in [
        (
            ScanOrdinalFact::EnforcedPredicate,
            scan.enforced_predicate(),
        ),
        (
            ScanOrdinalFact::UnenforcedPredicate,
            scan.unenforced_predicate(),
        ),
    ] {
        if let Some(domains) = domain.domains() {
            for ordinal in domains.keys() {
                check(fact, *ordinal)?;
            }
        }
    }
    let properties = frozen.public_facts().source().properties();
    match properties.distribution() {
        ConnectorReadDistribution::Unconstrained
        | ConnectorReadDistribution::Singleton
        | ConnectorReadDistribution::RoundRobin => {}
        ConnectorReadDistribution::Hash { keys, .. }
        | ConnectorReadDistribution::BucketShuffle { keys, .. } => {
            for key in keys.iter() {
                check(ScanOrdinalFact::Distribution, *key)?;
            }
        }
    }
    for key in properties.ordering() {
        check(ScanOrdinalFact::Ordering, *key.column())?;
    }
    Ok(())
}

/// The scan's dynamic filter as its frozen contract states it, for a scan
/// whose connector receives no live feedback: complete from the start, over
/// the columns its dynamic-filter variables assign, and constraining none of
/// them. No dynamic filter is complete-all. A compiled scan's runtime filters
/// are applied to its rows by the compiled source, never by this filter.
fn complete_all_dynamic_filter(
    scan: &FrozenConnectorScan,
    assignments: &[ConnectorReadAssignment],
) -> Arc<ConnectorReadDynamicFilter> {
    let filtered = scan
        .dynamic_filters()
        .iter()
        .map(|filter| filter.variable())
        .collect::<BTreeSet<_>>();
    let covered = assignments
        .iter()
        .filter(|assignment| filtered.contains(assignment.variable()))
        .map(|assignment| assignment.column().clone())
        .collect();
    Arc::new(CompleteAllDynamicFilter::new(covered))
}

/// A compiled scan's connector subscribes to no live runtime filter: its
/// consumers run in the compiled scan source, which records no connector
/// consumer contract, so a build request is a contract violation. Connector
/// pruning keyed by binding identity is a later milestone.
struct NoLiveDynamicFilter;

impl TypedScanLiveDynamicFilterFactory for NoLiveDynamicFilter {
    fn build(
        &self,
        _session: Option<&RuntimeFilterSessionRef>,
        _contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
    ) -> Result<Arc<ConnectorReadDynamicFilter>, RuntimeFilterContractViolation> {
        Err(RuntimeFilterContractViolation::new(
            RuntimeFilterContractViolationKind::ContractMismatch,
            "a compiled scan subscribes to no live runtime filter",
        ))
    }
}

/// The reader policy a Task's query options give its connector page sources.
fn reader_policy(
    options: &QueryOptions,
) -> Result<ConnectorPageSourceProviderOptions, ExecutionFailure> {
    let cache = novarocks_execution::runtime::cache::ExecutionCacheOptions::from_query_options(
        Some(options),
    )?;
    Ok(ConnectorPageSourceProviderOptions {
        enable_parquet_reader_page_index: options.enable_parquet_reader_page_index(),
        data_cache: ConnectorDataCacheOptions {
            enable_scan_datacache: cache.enable_scan_datacache,
            enable_populate_datacache: cache.enable_populate_datacache,
            enable_datacache_async_populate_mode: cache.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: cache.enable_datacache_io_adaptor,
            enable_cache_select: cache.enable_cache_select,
            datacache_evict_probability: cache.datacache_evict_probability,
            datacache_priority: cache.datacache_priority,
            datacache_ttl_seconds: cache.datacache_ttl_seconds,
            datacache_sharing_work_period: cache.datacache_sharing_work_period,
        },
    })
}

#[cfg(test)]
#[path = "compiled_scan_binding_tests.rs"]
pub(crate) mod tests;
