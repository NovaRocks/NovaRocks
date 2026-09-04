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

use crate::sql_error_codes::SqlErrorPhase;
use crate::suite_manifest::SuiteManifest;
pub use novarocks_failpoint::QueryLifecycleFaultKind;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLifecycleFaultDirective {
    pub kind: QueryLifecycleFaultKind,
    pub be_index: usize,
}

/// A runner-owned BE process fault released only after the frontend reaches a
/// stable lifecycle barrier. The phase trigger is deliberately separate from
/// the BE action: the frontend owns the phase marker while the harness owns
/// process lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KillBeAtLifecyclePhaseDirective {
    pub be_index: usize,
    pub phase: QueryLifecyclePhase,
}

/// A runner-owned BE process kill released once a runner-owned BE log shows a
/// new matching line.
///
/// Protocol-neutral by construction: the case names the execution point it
/// wants the process to disappear at, rather than a coordinator phase whose
/// marker belongs to one protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillBeAfterBeLogDirective {
    pub be_index: usize,
    pub pattern: String,
}

/// Structured fault assertions deliberately name a result category rather
/// than matching a human-readable diagnostic. T7/T9 provide the actual
/// snapshot producer; T4 owns this stable runner contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryLifecycleErrorSource {
    BackendAttestation,
    FrontendLiveness,
    NoOutcome,
}

impl QueryLifecycleErrorSource {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "backend-attestation" => Some(Self::BackendAttestation),
            "frontend-liveness" => Some(Self::FrontendLiveness),
            "no-outcome" => Some(Self::NoOutcome),
            _ => None,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BackendAttestation => "backend-attestation",
            Self::FrontendLiveness => "frontend-liveness",
            Self::NoOutcome => "no-outcome",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParticipantOutcomeExpectation {
    Proof,
    Attestation { reason: String },
    NoOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLifecycleStructuredAssertion {
    pub error_source: Option<QueryLifecycleErrorSource>,
    pub participant_outcome: Option<ParticipantOutcomeExpectation>,
    pub telemetry_unavailable: Vec<QueryLifecycleTelemetryUnavailableExpectation>,
    pub metric_deltas: Vec<QueryLifecycleMetricDeltaExpectation>,
    /// Runtime Filter facts are asserted from the typed query-terminal
    /// projection supplied by the cluster harness, never from profile text.
    pub runtime_filter_availability: Option<RuntimeFilterAvailabilityExpectation>,
    pub runtime_filter_details: Vec<RuntimeFilterDetailExpectation>,
    pub runtime_filter_totals_at_least: Vec<RuntimeFilterTotalAtLeastExpectation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterAvailabilityExpectation {
    Available,
}

impl RuntimeFilterAvailabilityExpectation {
    pub fn parse(value: &str) -> Option<Self> {
        (value == "available").then_some(Self::Available)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterDetailExpectation {
    CompletedChannel,
    AcceptedProducer,
    SentAckedTransport,
    DeliveredConsumer,
    DeliveredAppliedConsumer,
}

impl RuntimeFilterDetailExpectation {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "completed-channel" => Some(Self::CompletedChannel),
            "accepted-producer" => Some(Self::AcceptedProducer),
            "sent-acked-transport" => Some(Self::SentAckedTransport),
            "delivered-consumer" => Some(Self::DeliveredConsumer),
            "delivered-applied-consumer" => Some(Self::DeliveredAppliedConsumer),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CompletedChannel => "completed-channel",
            Self::AcceptedProducer => "accepted-producer",
            Self::SentAckedTransport => "sent-acked-transport",
            Self::DeliveredConsumer => "delivered-consumer",
            Self::DeliveredAppliedConsumer => "delivered-applied-consumer",
        }
    }

    pub const fn valid_names() -> &'static str {
        "completed-channel, accepted-producer, sent-acked-transport, delivered-consumer, delivered-applied-consumer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterTotalMetric {
    ChannelCount,
    ChannelCompletedCount,
    ProducerStreamCount,
    ProducerAcceptedCount,
    TransportRouteCount,
    TransportSentCount,
    TransportAckedCount,
    ConsumerCount,
    ConsumerRowEvaluations,
    ConsumerInputRows,
    ConsumerOutputRows,
    ConsumerScanEvaluated,
    ConsumerScanPruned,
}

impl RuntimeFilterTotalMetric {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "channel_count" => Some(Self::ChannelCount),
            "channel_completed_count" => Some(Self::ChannelCompletedCount),
            "producer_stream_count" => Some(Self::ProducerStreamCount),
            "producer_accepted_count" => Some(Self::ProducerAcceptedCount),
            "transport_route_count" => Some(Self::TransportRouteCount),
            "transport_sent_count" => Some(Self::TransportSentCount),
            "transport_acked_count" => Some(Self::TransportAckedCount),
            "consumer_count" => Some(Self::ConsumerCount),
            "consumer_row_evaluations" => Some(Self::ConsumerRowEvaluations),
            "consumer_input_rows" => Some(Self::ConsumerInputRows),
            "consumer_output_rows" => Some(Self::ConsumerOutputRows),
            "consumer_scan_evaluated" => Some(Self::ConsumerScanEvaluated),
            "consumer_scan_pruned" => Some(Self::ConsumerScanPruned),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ChannelCount => "channel_count",
            Self::ChannelCompletedCount => "channel_completed_count",
            Self::ProducerStreamCount => "producer_stream_count",
            Self::ProducerAcceptedCount => "producer_accepted_count",
            Self::TransportRouteCount => "transport_route_count",
            Self::TransportSentCount => "transport_sent_count",
            Self::TransportAckedCount => "transport_acked_count",
            Self::ConsumerCount => "consumer_count",
            Self::ConsumerRowEvaluations => "consumer_row_evaluations",
            Self::ConsumerInputRows => "consumer_input_rows",
            Self::ConsumerOutputRows => "consumer_output_rows",
            Self::ConsumerScanEvaluated => "consumer_scan_evaluated",
            Self::ConsumerScanPruned => "consumer_scan_pruned",
        }
    }

    pub const fn valid_names() -> &'static str {
        "channel_count, channel_completed_count, producer_stream_count, producer_accepted_count, transport_route_count, transport_sent_count, transport_acked_count, consumer_count, consumer_row_evaluations, consumer_input_rows, consumer_output_rows, consumer_scan_evaluated, consumer_scan_pruned"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFilterTotalAtLeastExpectation {
    pub metric: RuntimeFilterTotalMetric,
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLifecycleMetricDeltaExpectation {
    pub metric: String,
    pub delta: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLifecycleTelemetryUnavailableExpectation {
    pub scope: String,
    pub stage: String,
    pub code: String,
}

#[derive(Debug, Clone)]
pub struct SuiteConfig {
    pub name: String,
    pub sql_dir: PathBuf,
    pub result_dir: Option<PathBuf>,
    pub sql_glob: String,
    pub default_catalog: String,
    pub default_db: String,
    pub auto_case_db: bool,
    pub verify_default: bool,
    pub init_sql: Option<PathBuf>,
    pub cleanup_sql: Option<PathBuf>,
    pub manifest: SuiteManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImvStatelessLevel {
    Baseline,
    Package,
    Provenance,
    Full,
}

impl ImvStatelessLevel {
    /// Render as the string form expected by the server-side
    /// `novarocks_imv_stateless_rebuild` procedure's `level` argument, and
    /// returned (case-insensitively) as its `AvailableLevel` result column.
    pub fn as_sql(&self) -> &'static str {
        match self {
            ImvStatelessLevel::Baseline => "baseline",
            ImvStatelessLevel::Package => "package",
            ImvStatelessLevel::Provenance => "provenance",
            ImvStatelessLevel::Full => "full",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImvStatelessDirective {
    pub mv: String,
    pub level: ImvStatelessLevel,
    /// Catalog that hosts the `system.novarocks_imv_stateless_rebuild`
    /// procedure and the target MV. Defaults to `ice` when unset, so
    /// REST-catalog cases can omit it; per-case hadoop catalogs (e.g.
    /// `mv_ice_${uuid0}`) must set it explicitly.
    pub catalog: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct QueryMeta {
    pub order_sensitive: Option<bool>,
    pub float_epsilon: Option<f64>,
    pub db: Option<String>,
    pub expect_error: Option<String>,
    pub expect_error_code: Option<String>,
    pub expect_sql_code: Option<String>,
    pub expect_sql_phase: Option<SqlErrorPhase>,
    pub expect_error_at: Option<SqlErrorLocation>,
    pub expect_error_tier: Option<SqlErrorTier>,
    /// Declarative NovaRocks-only syntax label. It is consumed only by the
    /// extension-manifest listing path and never changes execution behavior.
    pub nova_extension: Option<String>,
    pub result_contains: Vec<String>,
    pub result_contains_any: Vec<String>,
    pub result_not_contains: Vec<String>,
    pub explain_contains: Vec<String>,
    pub explain_not_contains: Vec<String>,
    pub normalize_explain_timing: bool,
    pub tags: Vec<String>,
    pub skip_result_check: bool,
    pub retry_count: Option<usize>,
    pub retry_interval_ms: Option<u64>,
    pub kill_be_index: Option<usize>,
    pub network_partition_be: Option<usize>,
    pub heartbeat_delay_ms: Option<u64>,
    pub restart_be_delay_ms: Option<u64>,
    /// Restart the runner-owned frontend after this statement has succeeded.
    /// This is intentionally a post-statement control-plane failure, not a
    /// query-lifecycle injection, so durable background work can be recovered.
    pub restart_fe_after_step: bool,
    /// One runner-owned, frontend-only connector-cleanup fault token.
    pub cleanup_fault: Option<String>,
    /// One bounded runner-owned fault for the next matching standard Iceberg
    /// REST publication request. The SQL case never names an operation id.
    pub publication_catalog_fault: Option<PublicationCatalogFaultDirective>,
    pub drop_next_init_ack_be_index: Option<usize>,
    pub stop_query_control_heartbeat_be_index: Option<usize>,
    pub kill_fe_after_control_ready_count: Option<usize>,
    /// Kill and restart FE after an MV lake publication is known committed but
    /// before the Accelerator projector can CAS its local projection.
    pub kill_fe_after_mv_known_committed_before_projector_cas: bool,
    /// Replace one BE process after its task-protocol `EstablishQueryContext`
    /// has been applied.
    pub restart_be_after_establish_context_index: Option<usize>,
    /// Execute KILL QUERY from a separate client after this query's Nth ControlReady.
    pub kill_query_after_control_ready_count: Option<usize>,
    /// Execute KILL QUERY after a new matching line is observed in a runner-owned BE log.
    pub kill_query_after_be_log_contains: Option<String>,
    /// Kill one BE after a new matching line is observed in a runner-owned BE
    /// log.
    pub kill_be_after_be_log_contains: Option<KillBeAfterBeLogDirective>,
    /// Kill and restart FE after a new matching line is observed in a
    /// runner-owned BE log.
    ///
    /// Protocol-neutral for the same reason as
    /// `kill_query_after_be_log_contains`: the case names the point it wants
    /// the coordinator to die at, instead of a phase whose marker belongs to
    /// one protocol.
    pub kill_fe_after_be_log_contains: Option<String>,
    /// Fail the local StageFragments build at this one-based fragment ordinal.
    pub fail_stage_prepare_ordinal: Option<usize>,
    pub drop_next_stage_ack_be_index: Option<usize>,
    pub drop_next_start_ack_be_index: Option<usize>,
    pub suppress_start_ack_be_index: Option<usize>,
    /// Store the immutable terminal snapshot but deliberately omit the stream
    /// ACK for this participant, requiring BE unary fallback delivery.
    pub drop_next_terminal_ack_be_index: Option<usize>,
    /// Close one BE's control stream immediately before TerminalSnapshot so
    /// the immutable payload can only reach FE through unary fallback.
    pub drop_terminal_snapshot_stream_be_index: Option<usize>,
    /// Inject a second valid-but-different participant snapshot at FE ingress
    /// before ACK, proving same-identity conflicts fail closed.
    pub terminal_snapshot_conflict_be_index: Option<usize>,
    /// One RFO-8R2 owner-local lifecycle fault. The first field names the
    /// stable arm and the second scopes it to one runner-owned BE.
    pub query_lifecycle_fault: Option<QueryLifecycleFaultDirective>,
    /// All explicitly configured RFO-8R2 owner-local lifecycle faults. The
    /// legacy scalar remains for existing programmatic callers and for the
    /// primary BE-log token, while parsed SQL directives populate this list.
    pub query_lifecycle_faults: Vec<QueryLifecycleFaultDirective>,
    /// Typed outcome/source assertion for RFO-8R2 resilience cases. It is
    /// intentionally separate from `expect_error` and log assertions.
    pub query_lifecycle_structured_assertion: Option<QueryLifecycleStructuredAssertion>,
    pub kill_query_at_lifecycle_phase: Option<QueryLifecyclePhase>,
    /// Kill one BE only after FE has retained an immutable participant outcome
    /// for the requested lifecycle phase.
    pub kill_be_at_lifecycle_phase: Option<KillBeAtLifecyclePhaseDirective>,
    pub stop_query_control_heartbeat_after_stage_be_index: Option<usize>,
    pub hold_start_until_early_ingress: bool,
    pub query_control_fragment_backend_limit: Option<usize>,
    /// Before this step, resolve a REST-catalog table and write one real,
    /// deliberately unreferenced MinIO object below its data directory.
    pub iceberg_orphan_fixture: Option<String>,
    /// After this step, assert that the fixture object for `iceberg_orphan_fixture`
    /// is absent. This keeps the object-store assertion outside SQL result text.
    pub iceberg_orphan_fixture_absent: bool,
    /// After the step SQL executes, poll `SHOW ALTER TABLE COLUMN` until FINISHED.
    /// Value is the table name.
    pub wait_alter_column: Option<String>,
    /// After the step SQL executes, poll `SHOW ALTER TABLE ROLLUP` until FINISHED.
    /// Value is the table name.
    pub wait_alter_rollup: Option<String>,
    /// After the step SQL executes, poll `SHOW ALTER TABLE OPTIMIZE` until FINISHED.
    /// Value is the table name.
    pub wait_alter_optimize: Option<String>,
    /// After the step SQL executes (verify mode), assert the named MV's
    /// incremental contents equal a full recompute derived by running the MV's
    /// SelectText (from `SHOW MATERIALIZED VIEWS`) directly against its base
    /// tables. Value is the MV name (qualified by the step/case db like
    /// wait_alter_*).
    pub imv_equivalence_check: Option<String>,
    /// After the step SQL executes (verify mode), assert that the named MV
    /// can be rebuilt statelessly at the requested fidelity level (default
    /// `Package`) — i.e. its lake-native metadata is sufficient to reproduce
    /// current contents without relying on in-process incremental state.
    pub imv_stateless_rebuild: Option<ImvStatelessDirective>,
    /// Test-only destructive Accelerator wipe followed immediately by a
    /// runner-owned cold FE restart. This is intentionally distinct from the
    /// in-process `full` stateless rebuild check.
    pub imv_accelerator_wipe_restart: Option<ImvStatelessDirective>,
    /// Require a substring to occur in at least one runner-owned BE log.
    pub be_log_contains: Vec<String>,
    /// Reject a substring if it occurs in any runner-owned BE log after this step began.
    pub be_log_not_contains: Vec<String>,
    /// Require the total non-overlapping substring count across all BE logs.
    pub be_log_count_at_least: Vec<(String, usize)>,
    /// Require a substring to appear in at least this many distinct BE logs.
    pub be_log_be_count_at_least: Vec<(String, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlErrorLocation {
    pub line: usize,
    pub column: usize,
}

impl SqlErrorLocation {
    pub fn parse(value: &str) -> Option<Self> {
        let (line, column) = value.split_once(':')?;
        let line = line.parse::<usize>().ok()?;
        let column = column.parse::<usize>().ok()?;
        (line > 0 && column > 0).then_some(Self { line, column })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorTier {
    Drift,
    Target,
}

impl SqlErrorTier {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "drift" => Some(Self::Drift),
            "target" => Some(Self::Target),
            _ => None,
        }
    }
}

pub use novarocks_cluster_harness::QueryLifecyclePhase;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationCatalogAction {
    StageCreate,
    TableCommit,
    NamespaceList,
    TableLoad,
}

impl PublicationCatalogAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StageCreate => "stage-create",
            Self::TableCommit => "table-commit",
            Self::NamespaceList => "namespace-list",
            Self::TableLoad => "table-load",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationCatalogFault {
    BeforeDispatch,
    AfterCommitBeforeResponse,
    /// Hold the downstream-successful response until the runner has killed
    /// the frontend while the issuing statement is still in flight.
    AfterCommitHoldForFrontendKill,
    /// Fail exactly one standard namespace enumeration read.
    IncompleteDiscovery,
    /// Return malformed bytes for exactly one standard table package read.
    CorruptPackage,
}

impl PublicationCatalogFault {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeDispatch => "before-dispatch",
            Self::AfterCommitBeforeResponse => "after-commit-before-response",
            Self::AfterCommitHoldForFrontendKill => "after-commit-hold-for-frontend-kill",
            Self::IncompleteDiscovery => "incomplete-discovery",
            Self::CorruptPackage => "corrupt-package",
        }
    }

    pub const fn requires_inflight_frontend_kill(self) -> bool {
        matches!(self, Self::AfterCommitHoldForFrontendKill)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationCatalogFaultDirective {
    pub action: PublicationCatalogAction,
    pub fault: PublicationCatalogFault,
}

impl QueryMeta {
    pub fn sql_error_tier(&self) -> SqlErrorTier {
        self.expect_error_tier.unwrap_or(SqlErrorTier::Drift)
    }

    pub fn has_error_expectation(&self) -> bool {
        self.expect_error.is_some()
            || self.expect_error_code.is_some()
            || self.has_sql_error_assertion()
    }

    pub fn has_sql_error_assertion(&self) -> bool {
        self.expect_sql_code.is_some()
            || self.expect_sql_phase.is_some()
            || self.expect_error_at.is_some()
    }

    pub fn has_be_log_directives(&self) -> bool {
        !self.be_log_contains.is_empty()
            || !self.be_log_not_contains.is_empty()
            || !self.be_log_count_at_least.is_empty()
            || !self.be_log_be_count_at_least.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct SqlStep {
    pub query_number: usize,
    pub sql: String,
    pub meta: QueryMeta,
}

#[derive(Debug, Clone)]
pub struct SqlCase {
    pub source_file: PathBuf,
    pub case_id: String,
    pub steps: Vec<SqlStep>,
    /// Resolved per-case database names detected from `${case_db}` / `${case_db_N}` placeholders.
    /// Index 0 is the primary (`${case_db}`), subsequent entries are `${case_db_2}`, etc.
    /// Empty when the case does not use per-case database isolation.
    pub case_dbs: Vec<String>,
    /// When true, this case must run sequentially (not in parallel with other cases).
    /// Set by file-level `@sequential = true` metadata or a legacy `-- name: ... @sequential` tag.
    pub sequential: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub mysql: String,
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: Option<String>,
    pub catalog: Option<String>,
    pub db: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryExecution {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub text_output: String,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultSet {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct SuiteHook {
    pub path: PathBuf,
    pub sql: String,
    pub catalog: Option<String>,
    pub db: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct RunnerConfig {
    pub path: Option<PathBuf>,
    pub values: HashMap<String, String>,
    pub cluster: HashMap<String, String>,
}
