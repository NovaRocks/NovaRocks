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

use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use novarocks_failpoint::{
    QueryLifecycleFaultKind, arm_path as lifecycle_arm_path, cleanup_trigger_path,
    mv_known_committed_before_projector_cas_marker_path,
    mv_known_committed_before_projector_cas_trigger_path, parse_cleanup_fault_directive,
    parse_runner_rfo_kind,
};
use novarocks_native_trust::{
    AutomaticTlsMaterial, DeploymentId, NativeCallerSubject, NativeEndpointConnector,
    NativeTlsMaterial, NativeTransportMode, NativeTrust, PemTransportMaterial,
    ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use novarocks_test_support::{ManagedProcess, ReadyMarker, ReservedTcpPort};
use novarocks_types::NativeEndpoint;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    PKCS_ED25519,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use toml::Value;

const LIFECYCLE_CONVERGENCE_DEBUG_PATH: &str = "/debug/query-lifecycle/latest";
const SYSTEM_NATIVE_TRUST_DEPLOYMENT_ID: &str = "novarocks-system-tests";
const SYSTEM_NATIVE_TRUST_SECRET_ENV: &str = "NOVAROCKS_SYSTEM_NATIVE_TRUST_SECRET";

#[derive(serde::Deserialize)]
struct LifecycleConvergenceWireSnapshot {
    execution_id: String,
    query_process_namespace: String,
    query_local_sequence: u64,
    query_attempt_id: u64,
    error_source: Option<String>,
    participant_outcomes: Vec<LifecycleParticipantOutcomeWire>,
    telemetry_unavailable: Vec<LifecycleTelemetryUnavailableWire>,
    runtime_filter: RuntimeFilterTerminalRollupWire,
    metrics: BTreeMap<String, i64>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum LifecycleParticipantOutcomeWire {
    Proof,
    Attestation { reason: String },
    NoOutcome,
}

#[derive(serde::Deserialize)]
struct LifecycleTelemetryUnavailableWire {
    scope: String,
    stage: String,
    code: String,
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
enum RuntimeFilterTerminalRollupWire {
    Available {
        participants: Vec<RuntimeFilterParticipantTerminalWire>,
        totals: RuntimeFilterTerminalTotalsWire,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(serde::Deserialize)]
struct RuntimeFilterParticipantTerminalWire {
    participant: RuntimeFilterParticipantWire,
    telemetry: RuntimeFilterParticipantTelemetryWire,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterParticipantWire {
    process_id: String,
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum RuntimeFilterParticipantTelemetryWire {
    Available {
        channels: Vec<RuntimeFilterChannelWire>,
        producer_streams: Vec<RuntimeFilterProducerStreamWire>,
        transport_routes: Vec<RuntimeFilterTransportRouteWire>,
        consumers: Vec<RuntimeFilterConsumerWire>,
    },
    Unavailable {
        stage: String,
        code: String,
    },
}

#[derive(serde::Deserialize)]
struct RuntimeFilterChannelWire {
    channel_binding_id: u32,
    channel_id: u32,
    install_state: String,
    terminal_state: String,
    latest_published_logical_version: Option<u64>,
    published_count: u64,
    completed_count: u64,
    unavailable_count: u64,
    cancelled_count: u64,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterProducerStreamWire {
    channel_binding_id: u32,
    channel_id: u32,
    producer_fragment_instance_id: Option<RuntimeFilterUniqueIdWire>,
    partition_id: u32,
    latest_accepted_sequence: Option<u64>,
    accepted_count: u64,
    duplicate_count: u64,
    stale_count: u64,
    conflict_count: u64,
    resource_limit_count: u64,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterTransportRouteWire {
    channel_binding_id: u32,
    channel_id: u32,
    route_edge_id: u64,
    sent_count: u64,
    sent_bytes: u64,
    retried_count: u64,
    retried_bytes: u64,
    acked_count: u64,
    acked_bytes: u64,
    fail_open_count: u64,
    fail_open_bytes: u64,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterConsumerWire {
    channel_binding_id: u32,
    channel_id: u32,
    consumer_binding_id: u32,
    fragment_instance_id: Option<RuntimeFilterUniqueIdWire>,
    latest_delivered_logical_version: Option<u64>,
    latest_applied_logical_version: Option<u64>,
    subscription_terminal: String,
    row_evaluations: u64,
    input_rows: u64,
    output_rows: u64,
    scan_evaluated: u64,
    scan_kept: u64,
    scan_pruned: u64,
    scan_not_evaluated: u64,
    scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedWire,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterUniqueIdWire {
    high: i64,
    low: i64,
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
enum RuntimeFilterTerminalTotalsWire {
    Available {
        channels: RuntimeFilterChannelTotalsWire,
        producer_streams: RuntimeFilterProducerStreamTotalsWire,
        transport_routes: RuntimeFilterTransportRouteTotalsWire,
        consumers: RuntimeFilterConsumerTotalsWire,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(serde::Deserialize)]
struct RuntimeFilterChannelTotalsWire {
    count: u64,
    published_count: u64,
    completed_count: u64,
    unavailable_count: u64,
    cancelled_count: u64,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterProducerStreamTotalsWire {
    count: u64,
    accepted_count: u64,
    duplicate_count: u64,
    stale_count: u64,
    conflict_count: u64,
    resource_limit_count: u64,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterTransportRouteTotalsWire {
    count: u64,
    sent_count: u64,
    sent_bytes: u64,
    retried_count: u64,
    retried_bytes: u64,
    acked_count: u64,
    acked_bytes: u64,
    fail_open_count: u64,
    fail_open_bytes: u64,
}

#[derive(serde::Deserialize)]
struct RuntimeFilterConsumerTotalsWire {
    count: u64,
    row_evaluations: u64,
    input_rows: u64,
    output_rows: u64,
    scan_evaluated: u64,
    scan_kept: u64,
    scan_pruned: u64,
    scan_not_evaluated: u64,
    scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedWire,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
struct RuntimeFilterScanNotEvaluatedWire {
    unit_facts_missing: u64,
    column_facts_missing: u64,
    data_type_unsupported: u64,
    predicate_capability_unsupported: u64,
    resource_unavailable: u64,
    snapshot_unavailable: u64,
    snapshot_timed_out: u64,
    snapshot_not_published: u64,
}

fn query_lifecycle_structured_snapshot_from_fe(
    port: u16,
) -> Result<Option<QueryLifecycleStructuredSnapshot>> {
    let response = reqwest::blocking::Client::builder()
        .timeout(TOPOLOGY_MYSQL_IO_TIMEOUT_CAP)
        .build()
        .context("build FE lifecycle snapshot client")?
        .get(format!(
            "http://127.0.0.1:{port}{LIFECYCLE_CONVERGENCE_DEBUG_PATH}"
        ))
        .send()
        .context("request FE lifecycle snapshot")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        bail!(
            "FE lifecycle snapshot returned non-success status: {}",
            response.status()
        );
    }
    let wire: LifecycleConvergenceWireSnapshot = response
        .json()
        .context("decode FE lifecycle snapshot JSON")?;
    decode_query_lifecycle_structured_snapshot(wire)
}

fn decode_query_lifecycle_structured_snapshot(
    wire: LifecycleConvergenceWireSnapshot,
) -> Result<Option<QueryLifecycleStructuredSnapshot>> {
    let process_namespace = decode_query_process_namespace(&wire.query_process_namespace)?;
    ensure!(
        wire.query_local_sequence > 0,
        "FE lifecycle snapshot query_local_sequence must be nonzero"
    );
    ensure!(
        wire.query_attempt_id > 0,
        "FE lifecycle snapshot query_attempt_id must be nonzero"
    );
    let error_source = match wire.error_source.as_deref() {
        None => None,
        Some("backend-attestation") => Some(QueryLifecycleErrorSource::BackendAttestation),
        Some("frontend-liveness") => Some(QueryLifecycleErrorSource::FrontendLiveness),
        Some("no-outcome") => Some(QueryLifecycleErrorSource::NoOutcome),
        Some(source) => bail!("unknown FE lifecycle snapshot error source {source:?}"),
    };
    let participant_outcomes = wire
        .participant_outcomes
        .into_iter()
        .map(|outcome| match outcome {
            LifecycleParticipantOutcomeWire::Proof => ParticipantTerminalOutcomeKind::Proof,
            LifecycleParticipantOutcomeWire::Attestation { reason } => {
                ParticipantTerminalOutcomeKind::Attestation { reason }
            }
            LifecycleParticipantOutcomeWire::NoOutcome => ParticipantTerminalOutcomeKind::NoOutcome,
        })
        .collect();
    let telemetry_unavailable = wire
        .telemetry_unavailable
        .into_iter()
        .map(|telemetry| QueryLifecycleTelemetryUnavailable {
            scope: telemetry.scope,
            stage: telemetry.stage,
            code: telemetry.code,
        })
        .collect();
    Ok(Some(QueryLifecycleStructuredSnapshot {
        execution_id: Some(wire.execution_id),
        process_namespace,
        local_sequence: wire.query_local_sequence,
        attempt_id: wire.query_attempt_id,
        error_source,
        participant_outcomes,
        telemetry_unavailable,
        runtime_filter: decode_runtime_filter_terminal_rollup(wire.runtime_filter)?,
        metrics: wire.metrics,
    }))
}

fn decode_query_process_namespace(value: &str) -> Result<u64> {
    let digits = value
        .strip_prefix("0x")
        .context("FE lifecycle snapshot query_process_namespace must start with 0x")?;
    ensure!(
        digits.len() == 16,
        "FE lifecycle snapshot query_process_namespace must contain exactly 16 hexadecimal digits"
    );
    u64::from_str_radix(digits, 16)
        .with_context(|| format!("decode FE lifecycle snapshot query_process_namespace {value:?}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterProcessRole {
    Fe,
    Be,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BePorts {
    pub http: u16,
    pub grpc: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossProcessRuntime {
    pub be: Vec<BePorts>,
    pub fe_http_port: u16,
    pub fe_grpc_port: u16,
    pub fe_mysql_port: u16,
}

/// Sanitized response captured from the FE management listener during a
/// lifecycle scenario. The harness exposes it read-only; it has no drain or
/// administration mutation endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendManagementResponse {
    pub status: u16,
    pub body: String,
}

/// Native transport profile owned by the system-test harness.
///
/// The profile selects the server configuration and the companion raw probe
/// connector. It is intentionally not a product configuration type: the
/// harness still renders the normal `[native_trust]` startup shape and starts
/// the same independent FE/BE processes as every other system scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeTrustFixtureMode {
    Plaintext,
    Automatic,
    Pem,
}

impl NativeTrustFixtureMode {
    fn transport_mode(self) -> NativeTransportMode {
        match self {
            Self::Plaintext => NativeTransportMode::Disabled,
            Self::Automatic => NativeTransportMode::Automatic,
            Self::Pem => NativeTransportMode::Pem,
        }
    }

    fn config_mode(self) -> &'static str {
        match self {
            Self::Plaintext => "disabled",
            Self::Automatic => "automatic",
            Self::Pem => "pem",
        }
    }
}

/// Per-cluster Native trust input with no secret-bearing public field.
///
/// Generated configs retain only an exact `${ENV:...}` reference. The harness
/// supplies its per-run test secret directly to child processes, never
/// to the generated TOML or test action log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTrustFixture {
    mode: NativeTrustFixtureMode,
    advertise_host: String,
}

impl Default for NativeTrustFixture {
    fn default() -> Self {
        Self::plaintext_ip()
    }
}

impl NativeTrustFixture {
    pub fn plaintext_ip() -> Self {
        Self {
            mode: NativeTrustFixtureMode::Plaintext,
            advertise_host: "127.0.0.1".to_string(),
        }
    }

    pub fn automatic_dns() -> Self {
        Self {
            mode: NativeTrustFixtureMode::Automatic,
            advertise_host: "localhost".to_string(),
        }
    }

    pub fn pem_ip() -> Self {
        Self {
            mode: NativeTrustFixtureMode::Pem,
            advertise_host: "127.0.0.1".to_string(),
        }
    }

    pub const fn mode(&self) -> NativeTrustFixtureMode {
        self.mode
    }

    pub fn advertise_host(&self) -> &str {
        &self.advertise_host
    }

    fn probe_trust(&self, shared_secret: &str) -> Result<NativeTrust> {
        let deployment_id = DeploymentId::parse(SYSTEM_NATIVE_TRUST_DEPLOYMENT_ID)
            .map_err(anyhow::Error::msg)
            .context("construct system Native trust probe deployment id")?;
        let secret = ValidatedSharedSecret::new(SecretValue::new(shared_secret))
            .map_err(anyhow::Error::msg)
            .context("construct system Native trust probe secret")?;
        let subject = NativeCallerSubject::parse("system-test-probe@native")
            .map_err(anyhow::Error::msg)
            .context("construct system Native trust probe subject")?;
        Ok(NativeTrust::new(
            deployment_id,
            secret,
            subject,
            self.mode.transport_mode(),
        ))
    }
}

#[derive(Debug, Clone)]
struct NativeTrustPemPaths {
    certificate_chain: PathBuf,
    private_key: PathBuf,
    trust_roots: PathBuf,
}

#[derive(Debug, Clone)]
struct PreparedNativeTrustFixture {
    fixture: NativeTrustFixture,
    shared_secret: String,
    pem_paths: NativeTrustPemPaths,
}

impl PreparedNativeTrustFixture {
    fn prepare(fixture: NativeTrustFixture, runtime_dir: &Path) -> Result<Self> {
        let native_trust_dir = runtime_dir.join("native-trust-material");
        fs::create_dir_all(&native_trust_dir).with_context(|| {
            format!(
                "create system Native trust fixture directory {}",
                native_trust_dir.display()
            )
        })?;
        let pem_paths =
            write_native_trust_pem_fixture(&native_trust_dir, fixture.advertise_host())?;
        Ok(Self {
            fixture,
            shared_secret: format!("system-native-trust-{}", next_fragment_failure_token(0)),
            pem_paths,
        })
    }

    fn apply_config(&self, root: &mut toml::map::Map<String, Value>) {
        let native_trust = table_mut(root, "native_trust");
        native_trust.insert(
            "deployment_id".to_string(),
            Value::String(SYSTEM_NATIVE_TRUST_DEPLOYMENT_ID.to_string()),
        );
        native_trust.insert(
            "shared_secret".to_string(),
            Value::String(format!("${{ENV:{SYSTEM_NATIVE_TRUST_SECRET_ENV}}}")),
        );
        let transport = table_mut(native_trust, "transport");
        transport.insert(
            "mode".to_string(),
            Value::String(self.fixture.mode.config_mode().to_string()),
        );
        if self.fixture.mode == NativeTrustFixtureMode::Pem {
            transport.insert(
                "certificate_chain_path".to_string(),
                Value::String(
                    self.pem_paths
                        .certificate_chain
                        .to_string_lossy()
                        .into_owned(),
                ),
            );
            transport.insert(
                "private_key_path".to_string(),
                Value::String(self.pem_paths.private_key.to_string_lossy().into_owned()),
            );
            transport.insert(
                "trust_roots_path".to_string(),
                Value::String(self.pem_paths.trust_roots.to_string_lossy().into_owned()),
            );
        } else {
            transport.remove("certificate_chain_path");
            transport.remove("private_key_path");
            transport.remove("trust_roots_path");
        }
    }

    fn probe_connector(
        &self,
        endpoint: NativeEndpoint,
        mode: NativeTrustFixtureMode,
    ) -> Result<NativeEndpointConnector> {
        match mode {
            NativeTrustFixtureMode::Plaintext => Ok(NativeEndpointConnector::plaintext(endpoint)),
            NativeTrustFixtureMode::Automatic => {
                let material =
                    AutomaticTlsMaterial::for_endpoint(self.probe_trust()?, endpoint.clone())
                        .map_err(anyhow::Error::msg)
                        .context("construct automatic Native trust probe material")?;
                NativeEndpointConnector::automatic(endpoint, &material)
                    .map_err(anyhow::Error::msg)
                    .context("construct automatic Native trust probe connector")
            }
            NativeTrustFixtureMode::Pem => {
                let material = self.probe_pem_material()?;
                Ok(NativeEndpointConnector::pem(endpoint, &material))
            }
        }
    }

    fn probe_pem_material(&self) -> Result<NativeTlsMaterial> {
        let certificate_chain = fs::read(&self.pem_paths.certificate_chain).with_context(|| {
            format!(
                "read system Native trust probe certificate {}",
                self.pem_paths.certificate_chain.display()
            )
        })?;
        let private_key = fs::read(&self.pem_paths.private_key).with_context(|| {
            format!(
                "read system Native trust probe private key {}",
                self.pem_paths.private_key.display()
            )
        })?;
        let trust_roots = fs::read(&self.pem_paths.trust_roots).with_context(|| {
            format!(
                "read system Native trust probe roots {}",
                self.pem_paths.trust_roots.display()
            )
        })?;
        PemTransportMaterial::new(certificate_chain, private_key, trust_roots)
            .and_then(|material| material.tls_material())
            .map_err(anyhow::Error::msg)
            .context("parse system Native trust probe PEM material")
    }

    fn probe_trust(&self) -> Result<NativeTrust> {
        self.fixture.probe_trust(&self.shared_secret)
    }

    fn cleanup_sensitive_material(&self) {
        if let Some(directory) = self.pem_paths.certificate_chain.parent() {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

fn write_native_trust_pem_fixture(
    directory: &Path,
    advertise_host: &str,
) -> Result<NativeTrustPemPaths> {
    let ca_key = KeyPair::generate_for(&PKCS_ED25519)
        .context("generate system Native trust fixture CA key")?;
    let mut ca_parameters = CertificateParams::new(Vec::<String>::new())
        .context("construct system Native trust fixture CA parameters")?;
    ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_parameters.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = ca_parameters
        .self_signed(&ca_key)
        .context("self-sign system Native trust fixture CA")?;

    let leaf_key = KeyPair::generate_for(&PKCS_ED25519)
        .context("generate system Native trust fixture leaf key")?;
    let mut leaf_parameters = CertificateParams::new(vec![advertise_host.to_string()])
        .context("construct system Native trust fixture leaf parameters")?;
    leaf_parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf = leaf_parameters
        .signed_by(&leaf_key, &ca, &ca_key)
        .context("sign system Native trust fixture leaf")?;

    let certificate_chain = directory.join("leaf.pem");
    let private_key = directory.join("leaf-key.pem");
    let trust_roots = directory.join("roots.pem");
    fs::write(&certificate_chain, leaf.pem()).with_context(|| {
        format!(
            "write system Native trust fixture certificate {}",
            certificate_chain.display()
        )
    })?;
    fs::write(&private_key, leaf_key.serialize_pem()).with_context(|| {
        format!(
            "write system Native trust fixture private key {}",
            private_key.display()
        )
    })?;
    fs::write(&trust_roots, ca.pem()).with_context(|| {
        format!(
            "write system Native trust fixture roots {}",
            trust_roots.display()
        )
    })?;
    Ok(NativeTrustPemPaths {
        certificate_chain,
        private_key,
        trust_roots,
    })
}

/// Explicit child-only environment configured by a harness consumer.
///
/// The harness treats these values as opaque launch inputs. They are applied
/// after its own invariant process environment, and the per-BE map overrides
/// the common BE map for a single backend index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrossProcessChildEnvironment {
    pub fe: BTreeMap<String, String>,
    pub be: BTreeMap<String, String>,
    pub be_by_index: BTreeMap<usize, BTreeMap<String, String>>,
}

/// Consumer-provided TOML fragments applied after role and port rendering.
///
/// Harness-owned role, endpoint, topology and StateStore keys are rejected;
/// the harness reapplies those values after the fragment is merged.
#[derive(Debug, Clone, Default)]
pub struct CrossProcessConfigOverlay {
    pub fe: Option<String>,
    pub be: Option<String>,
}

/// Typed inputs for one ephemeral 1FE+NBE cross-process cluster.
///
/// Consumers resolve environment and runner-specific configuration before
/// constructing this value. The harness owns only the distributed runtime.
#[derive(Debug, Clone)]
pub struct CrossProcessClusterOptions {
    pub binary: PathBuf,
    pub base_config_path: PathBuf,
    pub runtime_root: PathBuf,
    pub cluster_size: usize,
    pub query_lifecycle_faults_enabled: bool,
    pub cleanup_faults_enabled: bool,
    pub startup_timeout: Duration,
    pub child_environment: CrossProcessChildEnvironment,
    pub config_overlay: CrossProcessConfigOverlay,
    /// Harness-owned Native trust profile. `Default` is authenticated h2c on
    /// the loopback IP reference, never an unauthenticated transport.
    pub native_trust_fixture: NativeTrustFixture,
}

/// Lifecycle boundaries accepted by the distributed fault controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLifecyclePhase {
    Staging,
    Staged,
    Starting,
    Running,
    TerminalRetained,
}

/// Structured lifecycle facts exposed by a runner-owned cross-process
/// cluster. T4 owns this test boundary; T7/T9 supply query-scoped values once
/// their outcome and convergence contracts are implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLifecycleErrorSource {
    BackendAttestation,
    FrontendLiveness,
    NoOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParticipantTerminalOutcomeKind {
    Proof,
    Attestation { reason: String },
    NoOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLifecycleTelemetryUnavailable {
    pub scope: String,
    pub stage: String,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLifecycleStructuredSnapshot {
    /// The immutable execution identity used to correlate all values below.
    pub execution_id: Option<String>,
    /// Typed process attribution derived from the immutable query identity.
    pub process_namespace: u64,
    pub local_sequence: u64,
    pub attempt_id: u64,
    pub error_source: Option<QueryLifecycleErrorSource>,
    pub participant_outcomes: Vec<ParticipantTerminalOutcomeKind>,
    pub telemetry_unavailable: Vec<QueryLifecycleTelemetryUnavailable>,
    /// The FE-normalized Runtime Filter terminal read model. This is a
    /// query-scoped immutable projection, never a process counter or log
    /// rendering.
    pub runtime_filter: RuntimeFilterTerminalRollup,
    pub metrics: BTreeMap<String, i64>,
}

/// Runtime Filter telemetry availability for a completed query.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum RuntimeFilterTerminalRollup {
    Available {
        participants: Vec<RuntimeFilterParticipantTerminalTelemetry>,
        totals: RuntimeFilterTerminalTotalsTelemetry,
    },
    Unavailable {
        reason: RuntimeFilterTerminalRollupUnavailable,
    },
}

/// A query-level reason that terminal Runtime Filter facts are unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterTerminalRollupUnavailable {
    TerminalOutcomesIncomplete,
    NegativeAttestation,
}

/// One participant identity prefixes every detail in its terminal telemetry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RuntimeFilterTerminalParticipant {
    pub process_id: String,
}

/// One participant's complete Runtime Filter terminal telemetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterParticipantTerminalTelemetry {
    pub participant: RuntimeFilterTerminalParticipant,
    pub telemetry: RuntimeFilterParticipantTerminalTelemetryValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeFilterParticipantTerminalTelemetryValue {
    Available(RuntimeFilterParticipantTerminalDetails),
    Unavailable(RuntimeFilterTerminalUnavailable),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterTerminalUnavailable {
    pub stage: String,
    pub code: String,
}

/// The four owner-local detail sections retained for one participant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterParticipantTerminalDetails {
    pub channels: Vec<RuntimeFilterChannelTerminalDetail>,
    pub producer_streams: Vec<RuntimeFilterProducerStreamTerminalDetail>,
    pub transport_routes: Vec<RuntimeFilterTransportRouteTerminalDetail>,
    pub consumers: Vec<RuntimeFilterConsumerTerminalDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterChannelTerminalDetail {
    pub channel_binding_id: u32,
    pub channel_id: u32,
    pub install_state: RuntimeFilterChannelInstallState,
    pub terminal_state: RuntimeFilterChannelTerminalState,
    pub latest_published_logical_version: Option<u64>,
    pub published_count: u64,
    pub completed_count: u64,
    pub unavailable_count: u64,
    pub cancelled_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterChannelInstallState {
    Installed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterChannelTerminalState {
    Open,
    Completed,
    Unavailable,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterProducerStreamTerminalDetail {
    pub channel_binding_id: u32,
    pub channel_id: u32,
    pub producer_fragment_instance_id: Option<RuntimeFilterUniqueId>,
    pub partition_id: u32,
    pub latest_accepted_sequence: Option<u64>,
    pub accepted_count: u64,
    pub duplicate_count: u64,
    pub stale_count: u64,
    pub conflict_count: u64,
    pub resource_limit_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterTransportRouteTerminalDetail {
    pub channel_binding_id: u32,
    pub channel_id: u32,
    pub route_edge_id: u64,
    pub sent_count: u64,
    pub sent_bytes: u64,
    pub retried_count: u64,
    pub retried_bytes: u64,
    pub acked_count: u64,
    pub acked_bytes: u64,
    pub fail_open_count: u64,
    pub fail_open_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterConsumerTerminalDetail {
    pub channel_binding_id: u32,
    pub channel_id: u32,
    pub consumer_binding_id: u32,
    pub fragment_instance_id: Option<RuntimeFilterUniqueId>,
    pub latest_delivered_logical_version: Option<u64>,
    pub latest_applied_logical_version: Option<u64>,
    pub subscription_terminal: RuntimeFilterSubscriptionTerminal,
    pub row_evaluations: u64,
    pub input_rows: u64,
    pub output_rows: u64,
    pub scan_evaluated: u64,
    pub scan_kept: u64,
    pub scan_pruned: u64,
    pub scan_not_evaluated: u64,
    pub scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedCounters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterSubscriptionTerminal {
    Pending,
    Acquired,
    TimedOut,
    Unavailable,
    Unsupported,
    Cancelled,
    Completed,
    CompletedWithoutArtifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFilterUniqueId {
    pub high: i64,
    pub low: i64,
}

/// Checked totals from the FE read model. A caller must explicitly match
/// `Unavailable`; a partial sum is never exposed as a query total.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum RuntimeFilterTerminalTotalsTelemetry {
    Available(RuntimeFilterTerminalTotals),
    Unavailable(RuntimeFilterTerminalTotalsUnavailable),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterTerminalTotalsUnavailable {
    ParticipantTelemetryUnavailable,
    CounterOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterTerminalTotals {
    pub channels: RuntimeFilterChannelTotals,
    pub producer_streams: RuntimeFilterProducerStreamTotals,
    pub transport_routes: RuntimeFilterTransportRouteTotals,
    pub consumers: RuntimeFilterConsumerTotals,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFilterChannelTotals {
    pub count: u64,
    pub published_count: u64,
    pub completed_count: u64,
    pub unavailable_count: u64,
    pub cancelled_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFilterProducerStreamTotals {
    pub count: u64,
    pub accepted_count: u64,
    pub duplicate_count: u64,
    pub stale_count: u64,
    pub conflict_count: u64,
    pub resource_limit_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFilterTransportRouteTotals {
    pub count: u64,
    pub sent_count: u64,
    pub sent_bytes: u64,
    pub retried_count: u64,
    pub retried_bytes: u64,
    pub acked_count: u64,
    pub acked_bytes: u64,
    pub fail_open_count: u64,
    pub fail_open_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFilterConsumerTotals {
    pub count: u64,
    pub row_evaluations: u64,
    pub input_rows: u64,
    pub output_rows: u64,
    pub scan_evaluated: u64,
    pub scan_kept: u64,
    pub scan_pruned: u64,
    pub scan_not_evaluated: u64,
    pub scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedCounters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFilterScanNotEvaluatedCounters {
    pub unit_facts_missing: u64,
    pub column_facts_missing: u64,
    pub data_type_unsupported: u64,
    pub predicate_capability_unsupported: u64,
    pub resource_unavailable: u64,
    pub snapshot_unavailable: u64,
    pub snapshot_timed_out: u64,
    pub snapshot_not_published: u64,
}

fn decode_runtime_filter_terminal_rollup(
    wire: RuntimeFilterTerminalRollupWire,
) -> Result<RuntimeFilterTerminalRollup> {
    match wire {
        RuntimeFilterTerminalRollupWire::Available {
            participants,
            totals,
        } => Ok(RuntimeFilterTerminalRollup::Available {
            participants: participants
                .into_iter()
                .map(decode_runtime_filter_participant)
                .collect::<Result<Vec<_>>>()?,
            totals: decode_runtime_filter_totals(totals)?,
        }),
        RuntimeFilterTerminalRollupWire::Unavailable { reason } => {
            let reason = match reason.as_str() {
                "terminal-outcomes-incomplete" => {
                    RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete
                }
                "negative-attestation" => {
                    RuntimeFilterTerminalRollupUnavailable::NegativeAttestation
                }
                _ => bail!("unknown runtime-filter rollup unavailable reason {reason:?}"),
            };
            Ok(RuntimeFilterTerminalRollup::Unavailable { reason })
        }
    }
}

fn decode_runtime_filter_participant(
    wire: RuntimeFilterParticipantTerminalWire,
) -> Result<RuntimeFilterParticipantTerminalTelemetry> {
    let participant = RuntimeFilterTerminalParticipant {
        process_id: wire.participant.process_id,
    };
    let telemetry = match wire.telemetry {
        RuntimeFilterParticipantTelemetryWire::Available {
            channels,
            producer_streams,
            transport_routes,
            consumers,
        } => RuntimeFilterParticipantTerminalTelemetryValue::Available(
            RuntimeFilterParticipantTerminalDetails {
                channels: channels
                    .into_iter()
                    .map(decode_runtime_filter_channel)
                    .collect::<Result<Vec<_>>>()?,
                producer_streams: producer_streams
                    .into_iter()
                    .map(|stream| {
                        Ok(RuntimeFilterProducerStreamTerminalDetail {
                            channel_binding_id: stream.channel_binding_id,
                            channel_id: stream.channel_id,
                            producer_fragment_instance_id: stream
                                .producer_fragment_instance_id
                                .map(runtime_filter_unique_id)
                                .transpose()?,
                            partition_id: stream.partition_id,
                            latest_accepted_sequence: stream.latest_accepted_sequence,
                            accepted_count: stream.accepted_count,
                            duplicate_count: stream.duplicate_count,
                            stale_count: stream.stale_count,
                            conflict_count: stream.conflict_count,
                            resource_limit_count: stream.resource_limit_count,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                transport_routes: transport_routes
                    .into_iter()
                    .map(|route| RuntimeFilterTransportRouteTerminalDetail {
                        channel_binding_id: route.channel_binding_id,
                        channel_id: route.channel_id,
                        route_edge_id: route.route_edge_id,
                        sent_count: route.sent_count,
                        sent_bytes: route.sent_bytes,
                        retried_count: route.retried_count,
                        retried_bytes: route.retried_bytes,
                        acked_count: route.acked_count,
                        acked_bytes: route.acked_bytes,
                        fail_open_count: route.fail_open_count,
                        fail_open_bytes: route.fail_open_bytes,
                    })
                    .collect(),
                consumers: consumers
                    .into_iter()
                    .map(decode_runtime_filter_consumer)
                    .collect::<Result<Vec<_>>>()?,
            },
        ),
        RuntimeFilterParticipantTelemetryWire::Unavailable { stage, code } => {
            RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(
                RuntimeFilterTerminalUnavailable { stage, code },
            )
        }
    };
    Ok(RuntimeFilterParticipantTerminalTelemetry {
        participant,
        telemetry,
    })
}

fn decode_runtime_filter_channel(
    wire: RuntimeFilterChannelWire,
) -> Result<RuntimeFilterChannelTerminalDetail> {
    Ok(RuntimeFilterChannelTerminalDetail {
        channel_binding_id: wire.channel_binding_id,
        channel_id: wire.channel_id,
        install_state: match wire.install_state.as_str() {
            "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_INSTALL_STATE_V1_INSTALLED" => {
                RuntimeFilterChannelInstallState::Installed
            }
            _ => bail!(
                "unknown runtime-filter channel install state {:?}",
                wire.install_state
            ),
        },
        terminal_state: match wire.terminal_state.as_str() {
            "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_TERMINAL_STATE_V1_OPEN" => {
                RuntimeFilterChannelTerminalState::Open
            }
            "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_TERMINAL_STATE_V1_COMPLETED" => {
                RuntimeFilterChannelTerminalState::Completed
            }
            "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_TERMINAL_STATE_V1_UNAVAILABLE" => {
                RuntimeFilterChannelTerminalState::Unavailable
            }
            "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_TERMINAL_STATE_V1_CANCELLED" => {
                RuntimeFilterChannelTerminalState::Cancelled
            }
            _ => bail!(
                "unknown runtime-filter channel terminal state {:?}",
                wire.terminal_state
            ),
        },
        latest_published_logical_version: wire.latest_published_logical_version,
        published_count: wire.published_count,
        completed_count: wire.completed_count,
        unavailable_count: wire.unavailable_count,
        cancelled_count: wire.cancelled_count,
    })
}

fn decode_runtime_filter_consumer(
    wire: RuntimeFilterConsumerWire,
) -> Result<RuntimeFilterConsumerTerminalDetail> {
    let subscription_terminal = match wire.subscription_terminal.as_str() {
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_PENDING" => {
            RuntimeFilterSubscriptionTerminal::Pending
        }
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_ACQUIRED" => {
            RuntimeFilterSubscriptionTerminal::Acquired
        }
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_TIMED_OUT" => {
            RuntimeFilterSubscriptionTerminal::TimedOut
        }
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_UNAVAILABLE" => {
            RuntimeFilterSubscriptionTerminal::Unavailable
        }
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_UNSUPPORTED" => {
            RuntimeFilterSubscriptionTerminal::Unsupported
        }
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_CANCELLED" => {
            RuntimeFilterSubscriptionTerminal::Cancelled
        }
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_COMPLETED" => {
            RuntimeFilterSubscriptionTerminal::Completed
        }
        "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_COMPLETED_WITHOUT_ARTIFACT" => {
            RuntimeFilterSubscriptionTerminal::CompletedWithoutArtifact
        }
        _ => bail!(
            "unknown runtime-filter subscription terminal state {:?}",
            wire.subscription_terminal
        ),
    };
    Ok(RuntimeFilterConsumerTerminalDetail {
        channel_binding_id: wire.channel_binding_id,
        channel_id: wire.channel_id,
        consumer_binding_id: wire.consumer_binding_id,
        fragment_instance_id: wire
            .fragment_instance_id
            .map(runtime_filter_unique_id)
            .transpose()?,
        latest_delivered_logical_version: wire.latest_delivered_logical_version,
        latest_applied_logical_version: wire.latest_applied_logical_version,
        subscription_terminal,
        row_evaluations: wire.row_evaluations,
        input_rows: wire.input_rows,
        output_rows: wire.output_rows,
        scan_evaluated: wire.scan_evaluated,
        scan_kept: wire.scan_kept,
        scan_pruned: wire.scan_pruned,
        scan_not_evaluated: wire.scan_not_evaluated,
        scan_not_evaluated_reasons: runtime_filter_scan_not_evaluated(
            wire.scan_not_evaluated_reasons,
        ),
    })
}

fn runtime_filter_unique_id(wire: RuntimeFilterUniqueIdWire) -> Result<RuntimeFilterUniqueId> {
    Ok(RuntimeFilterUniqueId {
        high: wire.high,
        low: wire.low,
    })
}

fn decode_runtime_filter_totals(
    wire: RuntimeFilterTerminalTotalsWire,
) -> Result<RuntimeFilterTerminalTotalsTelemetry> {
    match wire {
        RuntimeFilterTerminalTotalsWire::Available {
            channels,
            producer_streams,
            transport_routes,
            consumers,
        } => Ok(RuntimeFilterTerminalTotalsTelemetry::Available(
            RuntimeFilterTerminalTotals {
                channels: RuntimeFilterChannelTotals {
                    count: channels.count,
                    published_count: channels.published_count,
                    completed_count: channels.completed_count,
                    unavailable_count: channels.unavailable_count,
                    cancelled_count: channels.cancelled_count,
                },
                producer_streams: RuntimeFilterProducerStreamTotals {
                    count: producer_streams.count,
                    accepted_count: producer_streams.accepted_count,
                    duplicate_count: producer_streams.duplicate_count,
                    stale_count: producer_streams.stale_count,
                    conflict_count: producer_streams.conflict_count,
                    resource_limit_count: producer_streams.resource_limit_count,
                },
                transport_routes: RuntimeFilterTransportRouteTotals {
                    count: transport_routes.count,
                    sent_count: transport_routes.sent_count,
                    sent_bytes: transport_routes.sent_bytes,
                    retried_count: transport_routes.retried_count,
                    retried_bytes: transport_routes.retried_bytes,
                    acked_count: transport_routes.acked_count,
                    acked_bytes: transport_routes.acked_bytes,
                    fail_open_count: transport_routes.fail_open_count,
                    fail_open_bytes: transport_routes.fail_open_bytes,
                },
                consumers: RuntimeFilterConsumerTotals {
                    count: consumers.count,
                    row_evaluations: consumers.row_evaluations,
                    input_rows: consumers.input_rows,
                    output_rows: consumers.output_rows,
                    scan_evaluated: consumers.scan_evaluated,
                    scan_kept: consumers.scan_kept,
                    scan_pruned: consumers.scan_pruned,
                    scan_not_evaluated: consumers.scan_not_evaluated,
                    scan_not_evaluated_reasons: runtime_filter_scan_not_evaluated(
                        consumers.scan_not_evaluated_reasons,
                    ),
                },
            },
        )),
        RuntimeFilterTerminalTotalsWire::Unavailable { reason } => {
            let reason = match reason.as_str() {
                "participant-telemetry-unavailable" => {
                    RuntimeFilterTerminalTotalsUnavailable::ParticipantTelemetryUnavailable
                }
                "counter-overflow" => RuntimeFilterTerminalTotalsUnavailable::CounterOverflow,
                _ => bail!("unknown runtime-filter totals unavailable reason {reason:?}"),
            };
            Ok(RuntimeFilterTerminalTotalsTelemetry::Unavailable(reason))
        }
    }
}

fn runtime_filter_scan_not_evaluated(
    wire: RuntimeFilterScanNotEvaluatedWire,
) -> RuntimeFilterScanNotEvaluatedCounters {
    RuntimeFilterScanNotEvaluatedCounters {
        unit_facts_missing: wire.unit_facts_missing,
        column_facts_missing: wire.column_facts_missing,
        data_type_unsupported: wire.data_type_unsupported,
        predicate_capability_unsupported: wire.predicate_capability_unsupported,
        resource_unavailable: wire.resource_unavailable,
        snapshot_unavailable: wire.snapshot_unavailable,
        snapshot_timed_out: wire.snapshot_timed_out,
        snapshot_not_published: wire.snapshot_not_published,
    }
}

impl QueryLifecyclePhase {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "staging" => Some(Self::Staging),
            "staged" => Some(Self::Staged),
            "starting" => Some(Self::Starting),
            "running" => Some(Self::Running),
            "terminal-retained" => Some(Self::TerminalRetained),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Staging => "staging",
            Self::Staged => "staged",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::TerminalRetained => "terminal-retained",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BackendTopologyRow {
    process_id: String,
    grpc_port: u16,
    state: String,
    alive: bool,
    scheduled_fragments: u64,
    build_identity: String,
    status_detail: String,
}

impl BackendTopologyRow {
    fn is_eligible_live(&self) -> bool {
        self.state == "Live"
            && self.alive
            && !self.build_identity.is_empty()
            && self.status_detail.is_empty()
    }
}

fn parse_frontend_show_backends_values(values: &[String]) -> Result<BackendTopologyRow> {
    let process_id = values
        .first()
        .context("SHOW BACKENDS row missing ProcessId")?
        .clone();
    let endpoint = values
        .get(1)
        .context("SHOW BACKENDS row missing Endpoint")?;
    let (_, port) = endpoint
        .rsplit_once(':')
        .context("SHOW BACKENDS Endpoint must contain a port")?;
    let grpc_port = port
        .parse::<u16>()
        .context("parse SHOW BACKENDS endpoint port")?;
    let alive = values
        .get(7)
        .context("SHOW BACKENDS row missing Eligible")?
        .parse::<bool>()
        .context("parse SHOW BACKENDS Eligible")?;
    let state = values
        .get(12)
        .context("SHOW BACKENDS row missing DiagnosticStatus")?
        .clone();
    let scheduled_fragments = values
        .get(8)
        .context("SHOW BACKENDS row missing ScheduledFragments")?
        .parse::<u64>()
        .context("parse SHOW BACKENDS ScheduledFragments")?;
    let build_identity = values
        .get(11)
        .context("SHOW BACKENDS row missing BuildIdentity")?
        .clone();
    let status_detail = values
        .get(13)
        .context("SHOW BACKENDS row missing StatusDetail")?
        .clone();
    Ok(BackendTopologyRow {
        process_id,
        grpc_port,
        state,
        alive,
        scheduled_fragments,
        build_identity,
        status_detail,
    })
}

fn query_frontend_backend_topology(
    mysql_user: &str,
    host: &str,
    port: u16,
    io_timeout: Duration,
) -> Result<Vec<BackendTopologyRow>> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(mysql_user))
        // The synchronous mysql client maps macOS socket read/write timeouts
        // to EAGAIN while decoding a valid response. The enclosing topology
        // barrier owns the deadline; retain only a bounded connect timeout.
        .tcp_connect_timeout(Some(io_timeout));
    let mut conn = MysqlConn::new(builder)
        .with_context(|| format!("connect to cross-process FE MySQL at {host}:{port}"))?;
    let rows: Vec<mysql::Row> = conn
        .query("SHOW BACKENDS")
        .context("query SHOW BACKENDS from cross-process FE")?;
    rows.into_iter()
        .map(|row| {
            let values = (0..14)
                .map(|index| {
                    row.get::<String, usize>(index)
                        .with_context(|| format!("SHOW BACKENDS row missing column {index}"))
                })
                .collect::<Result<Vec<_>>>()?;
            parse_frontend_show_backends_values(&values)
        })
        .collect()
}

const BACKEND_TOPOLOGY_TIMEOUT_CAP: Duration = Duration::from_secs(120);
const TOPOLOGY_MYSQL_IO_TIMEOUT_CAP: Duration = Duration::from_secs(2);
const TOPOLOGY_MYSQL_IO_TIMEOUT_MIN: Duration = Duration::from_millis(1);
const RESOURCE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const LIFECYCLE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(25);
const QUERY_EXECUTION_RESOURCE_METRIC: &str = "novarocks_backend_query_execution_resources";
const QUERY_LIFECYCLE_TERMINAL_METRIC: &str = "novarocks_backend_query_lifecycle_terminal_total";
const FRONTEND_QUERY_LIFECYCLE_CONTROL_METRIC: &str =
    "novarocks_frontend_query_lifecycle_control_total";

const HEAVY_QUERY_EXECUTION_RESOURCES: [&str; 10] = [
    "stage_active_builders",
    "stage_encoded_bytes",
    "stage_dormant_workers",
    "fragment_controls_reserved",
    "fragment_controls_running",
    "native_query_contexts_active",
    "native_query_contexts_second_chance",
    "native_query_active_fragments",
    "native_runtime_filter_services",
    "connector_query_leases",
];

const QUERY_EXECUTION_RESOURCE_BINDING_LEASE: &str = "connector_binding_leases";
const TERMINAL_RETAINED_OUTCOME: &str = "terminal_retained";
const TERMINAL_RETAINED_BYTES_OUTCOME: &str = "terminal_retained_bytes";
const TERMINAL_RETAINED_CAPACITY_OUTCOME: &str = "terminal_retained_capacity";
const TERMINAL_MAX_RETAINED_BYTES_OUTCOME: &str = "terminal_max_retained_bytes";
const TERMINAL_FALLBACK_ACCEPTED_OUTCOME: &str = "terminal_fallback_accepted";

#[derive(Debug, Clone, PartialEq)]
pub struct BackendResourceSnapshot {
    pub index: usize,
    pub process_running: bool,
    pub resources: BTreeMap<String, f64>,
    pub terminal_retained: f64,
    pub terminal_retained_bytes: f64,
    pub terminal_retained_capacity: f64,
    pub terminal_max_retained_bytes: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryExecutionResourceSnapshot {
    pub fe_running: bool,
    pub frontend_control_ready: f64,
    pub backends: Vec<BackendResourceSnapshot>,
}

impl QueryExecutionResourceSnapshot {
    fn convergence_failure(
        &self,
        baseline: &Self,
        permits_terminal_retention: bool,
    ) -> Option<String> {
        if self.backends.len() != baseline.backends.len() {
            return Some(format!(
                "backend cardinality changed: before={} current={}",
                baseline.backends.len(),
                self.backends.len()
            ));
        }
        let mut deltas = Vec::new();
        for (before, current) in baseline.backends.iter().zip(&self.backends) {
            if before.index != current.index {
                return Some(format!(
                    "backend ordering changed: before BE[{}] current BE[{}]",
                    before.index, current.index
                ));
            }
            if !current.process_running {
                // A killed BE that has not restarted proves heavy-resource release by
                // process exit; do not misclassify that as a metrics scrape failure.
                continue;
            }
            for (resource, before_value) in &before.resources {
                let current_value = current.resources.get(resource).copied().unwrap_or(f64::NAN);
                if current_value != *before_value {
                    deltas.push(format!(
                        "BE[{}] {resource}: before={before_value} current={current_value} delta={}",
                        current.index,
                        current_value - before_value
                    ));
                }
            }
            if self.fe_running
                && !permits_terminal_retention
                && (current.terminal_retained > before.terminal_retained
                    || current.terminal_retained_bytes > before.terminal_retained_bytes)
            {
                deltas.push(format!(
                    "BE[{}] terminal retention grew above baseline: before=({}, {}) current=({}, {})",
                    current.index,
                    before.terminal_retained,
                    before.terminal_retained_bytes,
                    current.terminal_retained,
                    current.terminal_retained_bytes
                ));
            }
            if (!self.fe_running || permits_terminal_retention)
                && (current.terminal_retained > current.terminal_retained_capacity
                    || current.terminal_retained_bytes > current.terminal_max_retained_bytes)
            {
                deltas.push(format!(
                    "BE[{}] terminal retention exceeds published limit: retained=({}, {}) limits=({}, {})",
                    current.index,
                    current.terminal_retained,
                    current.terminal_retained_bytes,
                    current.terminal_retained_capacity,
                    current.terminal_max_retained_bytes
                ));
            }
        }
        (!deltas.is_empty()).then(|| deltas.join("; "))
    }
}

fn bounded_backend_topology_timeout(requested: Duration) -> Duration {
    requested.min(BACKEND_TOPOLOGY_TIMEOUT_CAP)
}

fn backend_topology_deadline(now: Instant, requested: Duration) -> Instant {
    now.checked_add(bounded_backend_topology_timeout(requested))
        .unwrap_or(now)
}

fn topology_mysql_io_timeout(remaining: Duration) -> Duration {
    remaining
        .min(TOPOLOGY_MYSQL_IO_TIMEOUT_CAP)
        .max(TOPOLOGY_MYSQL_IO_TIMEOUT_MIN)
}

fn remaining_until(deadline: Instant, operation: &str) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        bail!("30s query lifecycle fault deadline expired before {operation}");
    }
    Ok(remaining)
}

fn validate_live_backend_topology(
    expected_ports: &[u16],
    rows: &[BackendTopologyRow],
) -> Result<()> {
    let expected = expected_ports.len();
    let live_rows = rows
        .iter()
        .filter(|row| row.is_eligible_live())
        .collect::<Vec<_>>();
    let live = live_rows.len();
    let identities = live_rows
        .iter()
        .map(|row| row.build_identity.as_str())
        .collect::<BTreeSet<_>>();
    let mut configured_ports = expected_ports.to_vec();
    configured_ports.sort_unstable();
    let mut observed_ports = live_rows
        .iter()
        .map(|row| row.grpc_port)
        .collect::<Vec<_>>();
    observed_ports.sort_unstable();
    if live == expected
        && observed_ports == configured_ports
        && (expected == 0 || identities.len() == 1)
    {
        return Ok(());
    }

    let observed = rows
        .iter()
        .map(|row| {
            format!(
                "{}:{}:{}:{}:{}",
                row.grpc_port, row.state, row.alive, row.build_identity, row.status_detail
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    bail!(
        "SHOW BACKENDS topology is not ready: registered={} expected={}; live={} expected={}; identities={identities:?}; configured_ports={configured_ports:?} observed_ports={observed_ports:?}; rows=[{}]",
        rows.len(),
        expected,
        live,
        expected,
        observed
    )
}

fn wait_for_live_backend_topology_with<Q, S, H>(
    expected_ports: &[u16],
    timeout: Duration,
    mut process_health: H,
    mut query: Q,
    mut sleep: S,
) -> Result<Vec<BackendTopologyRow>>
where
    Q: FnMut(Duration) -> Result<Vec<BackendTopologyRow>>,
    S: FnMut(Duration),
    H: FnMut() -> Result<String>,
{
    let expected = expected_ports.len();
    let deadline = backend_topology_deadline(Instant::now(), timeout);
    loop {
        process_health()
            .context("cross-process FE/BE exited before SHOW BACKENDS topology became ready")?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let io_timeout = topology_mysql_io_timeout(remaining);
        let last_observation = match query(io_timeout) {
            Ok(rows) => match validate_live_backend_topology(expected_ports, &rows) {
                Ok(()) => return Ok(rows),
                Err(error) => error.to_string(),
            },
            Err(error) => format!("SHOW BACKENDS query failed: {error:#}"),
        };

        if Instant::now() >= deadline {
            let process_diagnostics = process_health()
                .context("cross-process FE/BE exited during the bounded SHOW BACKENDS query")?;
            bail!(
                "timed out waiting for SHOW BACKENDS {expected}/{expected} Live; last_observation={last_observation}; {}",
                process_diagnostics
            );
        }
        sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100)),
        );
    }
}

struct LiveBackendTopologyWait<'a> {
    mysql_user: &'a str,
    runtime: &'a CrossProcessRuntime,
    expected_ports: &'a [u16],
    fe_config_path: &'a Path,
    be_config_paths: &'a [PathBuf],
    timeout: Duration,
}

fn wait_for_live_backend_topology(
    wait: LiveBackendTopologyWait<'_>,
    fe_process: &mut ManagedProcess,
    be_processes: &mut [ManagedProcess],
) -> Result<()> {
    let expected = wait.expected_ports.len();
    let host = "127.0.0.1";
    let port = wait.runtime.fe_mysql_port;
    let rows = wait_for_live_backend_topology_with(
        wait.expected_ports,
        wait.timeout,
        || {
            process_runtime_diagnostics(
                fe_process,
                be_processes,
                wait.fe_config_path,
                wait.be_config_paths,
                wait.runtime,
            )
        },
        |io_timeout| query_frontend_backend_topology(wait.mysql_user, host, port, io_timeout),
        thread::sleep,
    )?;
    let diagnostics = process_runtime_diagnostics(
        fe_process,
        be_processes,
        wait.fe_config_path,
        wait.be_config_paths,
        wait.runtime,
    )?;
    let build_identities = rows
        .iter()
        .map(|row| row.build_identity.as_str())
        .collect::<BTreeSet<_>>();
    let status_details = rows
        .iter()
        .map(|row| row.status_detail.as_str())
        .collect::<BTreeSet<_>>();
    println!(
        "cross-process topology barrier PASS: SHOW BACKENDS {}/{} Live; build_identities={build_identities:?}; status_details={status_details:?}; {}",
        rows.len(),
        expected,
        diagnostics
    );
    Ok(())
}

fn scrape_prometheus_metrics(port: u16) -> Result<String> {
    let address = format!("127.0.0.1:{port}");
    let mut stream = TcpStream::connect_timeout(
        &address
            .parse()
            .with_context(|| format!("parse metrics address {address}"))?,
        TOPOLOGY_MYSQL_IO_TIMEOUT_CAP,
    )
    .with_context(|| format!("connect metrics endpoint {address}"))?;
    stream
        .set_read_timeout(Some(TOPOLOGY_MYSQL_IO_TIMEOUT_CAP))
        .context("set metrics read timeout")?;
    stream
        .set_write_timeout(Some(TOPOLOGY_MYSQL_IO_TIMEOUT_CAP))
        .context("set metrics write timeout")?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .context("request /metrics")?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("read /metrics response")?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .context("malformed /metrics HTTP response")?;
    if !headers.starts_with("HTTP/1.1 200") && !headers.starts_with("HTTP/1.0 200") {
        bail!(
            "/metrics returned non-success status: {}",
            headers.lines().next().unwrap_or("<missing status>")
        );
    }
    Ok(body.to_string())
}

fn get_frontend_management(
    port: u16,
    path: &str,
    timeout: Duration,
) -> Result<FrontendManagementResponse> {
    if !path.starts_with('/') || path.contains('\r') || path.contains('\n') {
        bail!("invalid frontend management path {path:?}");
    }
    let address = format!("127.0.0.1:{port}");
    let socket = address
        .parse()
        .with_context(|| format!("parse frontend management address {address}"))?;
    let mut stream = TcpStream::connect_timeout(&socket, timeout)
        .with_context(|| format!("connect FE management endpoint {address}{path}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .context("set FE management read timeout")?;
    stream
        .set_write_timeout(Some(timeout))
        .context("set FE management write timeout")?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .with_context(|| format!("request FE management endpoint {path}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .with_context(|| format!("read FE management endpoint {path}"))?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .context("malformed FE management HTTP response")?;
    let status = headers
        .split_whitespace()
        .nth(1)
        .context("missing FE management HTTP status")?
        .parse::<u16>()
        .context("parse FE management HTTP status")?;
    Ok(FrontendManagementResponse {
        status,
        body: body.to_string(),
    })
}

const FRONTEND_METRIC_FAMILIES: [&str; 14] = [
    "novarocks_fragment_scheduled_total",
    "novarocks_heartbeat_rtt_seconds",
    "novarocks_backend_registry_entries",
    "novarocks_backend_announce_lease_valid",
    "novarocks_backend_identity_verified",
    "novarocks_backend_reported_state",
    "novarocks_backend_compatibility",
    "novarocks_backend_endpoint_ownership",
    "novarocks_backend_eligible",
    "novarocks_backend_topology_revision",
    "novarocks_frontend_query_lifecycle_active_attempts",
    "novarocks_frontend_query_lifecycle_init_total",
    "novarocks_frontend_query_lifecycle_control_total",
    "novarocks_frontend_query_lifecycle_latency_micros",
];

const BACKEND_METRIC_FAMILIES: [&str; 5] = [
    "novarocks_backend_query_lifecycle_entries",
    "novarocks_backend_query_lifecycle_rejections",
    "novarocks_backend_query_lifecycle_terminations",
    "novarocks_backend_query_lifecycle_terminal_total",
    "novarocks_backend_query_execution_resources",
];

fn assert_contains_metric_families(body: &str, families: &[&str], endpoint: &str) -> Result<()> {
    for family in families {
        if !body.contains(family) {
            bail!("{endpoint} is missing required metric family {family}");
        }
    }
    Ok(())
}

fn assert_excludes_metric_families(body: &str, families: &[&str], endpoint: &str) -> Result<()> {
    for family in families {
        if body.contains(family) {
            bail!("{endpoint} unexpectedly exposes foreign metric family {family}");
        }
    }
    Ok(())
}

/// The harness owns the production-shaped role boundary: every 1FE+NBE start
/// must prove the process metrics listener did not leak the other role's
/// registered metric families before a query is admitted.
fn assert_role_scoped_metrics(runtime: &CrossProcessRuntime) -> Result<()> {
    let frontend = scrape_prometheus_metrics(runtime.fe_http_port)
        .context("scrape cross-process FE /metrics")?;
    assert_contains_metric_families(
        &frontend,
        &FRONTEND_METRIC_FAMILIES,
        "cross-process FE /metrics",
    )?;
    assert_excludes_metric_families(
        &frontend,
        &BACKEND_METRIC_FAMILIES,
        "cross-process FE /metrics",
    )?;

    for (index, be) in runtime.be.iter().enumerate() {
        let backend = scrape_prometheus_metrics(be.http)
            .with_context(|| format!("scrape cross-process BE[{index}] /metrics"))?;
        let endpoint = format!("cross-process BE[{index}] /metrics");
        assert_contains_metric_families(&backend, &BACKEND_METRIC_FAMILIES, &endpoint)?;
        assert_excludes_metric_families(&backend, &FRONTEND_METRIC_FAMILIES, &endpoint)?;
    }
    println!(
        "cross-process role-scoped metrics barrier PASS: FE={} BE={}",
        runtime.fe_http_port,
        runtime.be.len()
    );
    Ok(())
}

fn prometheus_labeled_gauge(
    body: &str,
    metric: &str,
    label_name: &str,
    label_value: &str,
) -> Result<f64> {
    let label = format!("{label_name}=\"{label_value}\"");
    let mut values = body
        .lines()
        .filter(|line| line.starts_with(metric))
        .filter(|line| line.contains(&label))
        .filter_map(|line| line.split_whitespace().nth(1))
        .map(|value| value.parse::<f64>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parse {metric} label {label}"))?;
    match values.len() {
        1 => Ok(values.remove(0)),
        0 => bail!("missing required {metric}{{{label}}} gauge in BE /metrics"),
        count => bail!("ambiguous {metric}{{{label}}} gauge in BE /metrics: {count} samples"),
    }
}

fn await_query_lifecycle_structured_snapshot_after<F>(
    before_execution_id: Option<&str>,
    deadline: Instant,
    mut snapshot: F,
) -> Result<QueryLifecycleStructuredSnapshot>
where
    F: FnMut() -> Result<Option<QueryLifecycleStructuredSnapshot>>,
{
    let mut latest_execution_id = None;
    let mut latest_error = None;
    loop {
        match snapshot() {
            Ok(Some(candidate)) => {
                let execution_id = candidate.execution_id.as_deref();
                latest_execution_id = candidate.execution_id.clone();
                if execution_id.is_some() && execution_id != before_execution_id {
                    return Ok(candidate);
                }
            }
            Ok(None) => {}
            Err(error) => latest_error = Some(format!("{error:#}")),
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for a query lifecycle snapshot newer than {before_execution_id:?}; latest_execution_id={latest_execution_id:?}; latest_error={latest_error:?}"
            );
        }
        thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(LIFECYCLE_CONVERGENCE_POLL_INTERVAL),
        );
    }
}

pub trait ServerHandle: Send {
    fn target_host(&self) -> Option<&str>;
    fn target_port(&self) -> Option<u16>;
    fn supports_fault_injection(&self) -> bool {
        false
    }
    fn arm_cleanup_fault(&mut self, kind: &str) -> Result<()> {
        bail!("connector cleanup fault is unsupported by this server mode (kind={kind})")
    }
    fn clear_cleanup_faults(&mut self) -> Result<()> {
        Ok(())
    }
    fn supports_query_execution_resource_oracle(&self) -> bool {
        false
    }
    fn query_execution_resource_snapshot(
        &mut self,
    ) -> Result<Option<QueryExecutionResourceSnapshot>> {
        Ok(None)
    }
    fn query_execution_resource_diagnostics(&self) -> String {
        "resource diagnostics unavailable for this server mode".to_string()
    }
    fn await_query_execution_resource_convergence(
        &mut self,
        baseline: &QueryExecutionResourceSnapshot,
        permits_terminal_retention: bool,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            let current = match self.query_execution_resource_snapshot() {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => {
                    let error = anyhow::anyhow!("query execution resource oracle is unavailable");
                    if Instant::now() >= deadline {
                        return Err(error.context(self.query_execution_resource_diagnostics()));
                    }
                    thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(RESOURCE_CONVERGENCE_POLL_INTERVAL),
                    );
                    continue;
                }
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(error.context(self.query_execution_resource_diagnostics()));
                    }
                    thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(RESOURCE_CONVERGENCE_POLL_INTERVAL),
                    );
                    continue;
                }
            };
            if let Some(failure) = current.convergence_failure(baseline, permits_terminal_retention)
            {
                if Instant::now() < deadline {
                    thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(RESOURCE_CONVERGENCE_POLL_INTERVAL),
                    );
                    continue;
                }
                bail!(
                    "query execution resources did not converge before deadline: {failure}; baseline={baseline:?}; current={current:?}; {}",
                    self.query_execution_resource_diagnostics()
                );
            }
            return Ok(());
        }
    }
    fn kill_be(&mut self, index: usize) -> Result<()> {
        bail!("BE kill is unsupported by this server mode (index={index})")
    }
    fn restart_be(&mut self, index: usize) -> Result<()> {
        bail!("BE restart is unsupported by this server mode (index={index})")
    }
    fn restart_be_until(&mut self, index: usize, _deadline: Instant) -> Result<()> {
        self.restart_be(index)
    }
    fn drain_be_until(&mut self, index: usize, _deadline: Instant) -> Result<()> {
        bail!("BE drain is unsupported by this server mode (index={index})")
    }
    fn kill_fe(&mut self) -> Result<()> {
        bail!("FE kill is unsupported by this server mode")
    }
    fn restart_fe(&mut self) -> Result<()> {
        bail!("FE restart is unsupported by this server mode")
    }
    fn restart_fe_until(&mut self, _deadline: Instant) -> Result<()> {
        self.restart_fe()
    }
    fn kill_query(&mut self, connection_id: u32) -> Result<()> {
        bail!("KILL QUERY is unsupported by this server mode (connection_id={connection_id})")
    }
    fn kill_query_until(&mut self, connection_id: u32, _deadline: Instant) -> Result<()> {
        self.kill_query(connection_id)
    }
    fn backend_process_id(&self, index: usize) -> Result<novarocks_types::BackendProcessId> {
        bail!("backend process identity is unsupported by this server mode (index={index})")
    }
    fn fe_log_count(&self, needle: &str) -> Result<usize> {
        bail!("FE log counting is unsupported by this server mode (pattern={needle:?})")
    }
    fn fe_log_contents(&self) -> Result<String> {
        bail!("FE log reading is unsupported by this server mode")
    }
    fn clear_query_lifecycle_faults(&mut self) -> Result<()> {
        Ok(())
    }
    /// Returns query-scoped outcome/source/metric facts for a structured SQL
    /// assertion. `None` means the selected server mode does not expose the
    /// RFO-8R2 contract yet; the runner rejects an assertion rather than
    /// falling back to diagnostic text.
    fn query_lifecycle_structured_snapshot(
        &mut self,
    ) -> Result<Option<QueryLifecycleStructuredSnapshot>> {
        Ok(None)
    }
    /// Wait for the retained terminal record created by the query that started
    /// after `before_execution_id`. The debug endpoint intentionally exposes
    /// only `latest`, so accepting the pre-query identity would associate a
    /// test with a different query's terminal facts.
    fn await_query_lifecycle_structured_snapshot_after(
        &mut self,
        before_execution_id: Option<&str>,
        deadline: Instant,
    ) -> Result<QueryLifecycleStructuredSnapshot> {
        await_query_lifecycle_structured_snapshot_after(before_execution_id, deadline, || {
            self.query_lifecycle_structured_snapshot()
        })
    }
    fn release_query_lifecycle_phase_fault(
        &mut self,
        phase: QueryLifecyclePhase,
        fe_crash: bool,
    ) -> Result<()> {
        bail!(
            "lifecycle phase fault release is unsupported by this server mode (phase={}, fe_crash={fe_crash})",
            phase.as_str()
        )
    }
    fn armed_query_lifecycle_fault_token(
        &self,
        index: usize,
        kind: &'static str,
    ) -> Result<Option<String>> {
        bail!(
            "query lifecycle fault token is unsupported by this server mode (index={index}, kind={kind})"
        )
    }
    fn arm_init_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("InitAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_query_control_heartbeat_stop(&mut self, index: usize) -> Result<()> {
        bail!("query-control heartbeat stop is unsupported by this server mode (index={index})")
    }
    fn arm_fe_crash_after_control_ready(&mut self, count: usize) -> Result<()> {
        bail!("FE crash is unsupported by this server mode (ready_count={count})")
    }
    fn arm_be_restart_after_init_ack(&mut self, index: usize) -> Result<()> {
        bail!("BE restart-after-InitAck is unsupported by this server mode (index={index})")
    }
    fn arm_stage_prepare_failure(&mut self, ordinal: usize) -> Result<()> {
        bail!("Stage prepare failure is unsupported by this server mode (ordinal={ordinal})")
    }
    fn arm_stage_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("StageAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_start_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("StartAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_start_ack_suppress(&mut self, index: usize) -> Result<()> {
        bail!("StartAck suppression is unsupported by this server mode (index={index})")
    }
    fn arm_terminal_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("TerminalAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_terminal_snapshot_stream_drop(&mut self, index: usize) -> Result<()> {
        bail!("TerminalSnapshot stream drop is unsupported by this server mode (index={index})")
    }
    fn arm_terminal_snapshot_conflict(&mut self, index: usize) -> Result<()> {
        bail!("TerminalSnapshot conflict is unsupported by this server mode (index={index})")
    }
    /// Arms one stable RFO-8R2 owner-local lifecycle fault. The harness owns
    /// token publication and cleanup; application code alone claims the arm.
    fn arm_query_lifecycle_fault(&mut self, index: usize, kind: &'static str) -> Result<()> {
        bail!(
            "query lifecycle fault is unsupported by this server mode (index={index}, kind={kind})"
        )
    }
    fn arm_kill_query_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        bail!(
            "KILL QUERY lifecycle phase fault is unsupported by this server mode (phase={})",
            phase.as_str()
        )
    }
    fn arm_fe_crash_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        bail!(
            "FE lifecycle phase fault is unsupported by this server mode (phase={})",
            phase.as_str()
        )
    }
    fn arm_mv_known_committed_before_projector_cas(&mut self) -> Result<()> {
        bail!("MV known-committed projector barrier is unsupported by this server mode")
    }
    /// Arms the existing FE-owned phase barrier for a runner-owned BE kill.
    /// The trigger is released by `release_be_kill_at_lifecycle_phase` after
    /// the harness has killed its selected BE process.
    fn arm_be_kill_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        bail!(
            "BE lifecycle phase kill is unsupported by this server mode (phase={})",
            phase.as_str()
        )
    }
    fn release_be_kill_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        bail!(
            "BE lifecycle phase kill release is unsupported by this server mode (phase={})",
            phase.as_str()
        )
    }
    fn arm_query_control_heartbeat_stop_after_stage(&mut self, index: usize) -> Result<()> {
        bail!(
            "query-control heartbeat stop-after-stage is unsupported by this server mode (index={index})"
        )
    }
    fn arm_hold_start_until_early_ingress(&mut self) -> Result<()> {
        bail!("Start hold until early ingress is unsupported by this server mode")
    }
    fn arm_query_control_fragment_backend_limit(&mut self, limit: usize) -> Result<()> {
        bail!(
            "query-control fragment backend limit is unsupported by this server mode (limit={limit})"
        )
    }
    fn be_count(&self) -> usize {
        0
    }
    fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
        bail!("scheduled fragment telemetry is unsupported by this server mode (index={index})")
    }

    fn arm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        bail!(
            "fragment executor failure injection is unsupported by this server mode (index={index})"
        )
    }
    fn release_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        bail!(
            "fragment executor failure release is unsupported by this server mode (index={index})"
        )
    }
    fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        bail!(
            "fragment executor failure cleanup is unsupported by this server mode (index={index})"
        )
    }
    fn armed_fragment_failure_token(&self, index: usize) -> Result<Option<String>> {
        bail!("fragment failure token is unsupported by this server mode (index={index})")
    }
    #[allow(dead_code)]
    fn assert_be_log(&self, index: usize, _needle: &str) -> Result<()> {
        bail!("BE log assertions are unsupported by this server mode (index={index})")
    }
    #[allow(dead_code)]
    fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
        bail!(
            "BE log counting is unsupported by this server mode (index={index}, pattern={needle:?})"
        )
    }
    #[allow(dead_code)]
    fn be_log_contents(&self, index: usize) -> Result<String> {
        bail!("BE log reading is unsupported by this server mode (index={index})")
    }
    fn be_current_log_contents(&self, index: usize) -> Result<String> {
        self.be_log_contents(index)
    }
    #[allow(dead_code)]
    fn residual_process_ids(&self) -> Vec<u32> {
        Vec::new()
    }
    fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

/// File name of the FE durable SQLite StateStore inside a launch runtime dir.
const FE_STATE_STORE_FILE_NAME: &str = "frontend-state.sqlite";

/// Every file that belongs to the FE durable SQLite StateStore, relative to
/// `FE_STATE_STORE_FILE_NAME`.
///
/// The store runs in WAL mode (`novarocks/state-store/sqlite`), so SQLite keeps
/// the write-ahead journal and the shared-memory index next to the main
/// database file. Destroying the store means destroying all three: a surviving
/// `-wal` would let a replacement store replay already-committed records.
const FE_STATE_STORE_FILE_SUFFIXES: [&str; 3] = ["", "-wal", "-shm"];

/// Render the per-process TOML config for cross-process mode.
///
/// `be_index` is used when `role == Be` to select which BE's ports to use.
/// It is ignored for `role == Fe`.
pub fn render_cross_process_config(
    base_config: &str,
    role: ClusterProcessRole,
    be_index: usize,
    runtime: &CrossProcessRuntime,
) -> Result<String> {
    let mut value = if base_config.trim().is_empty() {
        Value::Table(Default::default())
    } else {
        base_config
            .parse::<Value>()
            .context("parse standalone config for cross-process mode")?
    };
    let root = value
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("standalone config root must be a TOML table"))?;

    let server = table_mut(root, "server");
    server.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
    match role {
        ClusterProcessRole::Fe => {
            server.insert(
                "http_port".to_string(),
                Value::Integer(i64::from(runtime.fe_http_port)),
            );
            server.insert(
                "grpc_port".to_string(),
                Value::Integer(i64::from(runtime.fe_grpc_port)),
            );
        }
        ClusterProcessRole::Be => {
            let be = &runtime.be[be_index];
            server.insert("http_port".to_string(), Value::Integer(i64::from(be.http)));
            server.insert("grpc_port".to_string(), Value::Integer(i64::from(be.grpc)));
        }
    }

    match role {
        ClusterProcessRole::Fe => {
            let standalone_server = table_mut(root, "standalone_server");
            standalone_server.insert(
                "mysql_port".to_string(),
                Value::Integer(i64::from(runtime.fe_mysql_port)),
            );
        }
        ClusterProcessRole::Be => {
            if let Some(standalone_server) = root
                .get_mut("standalone_server")
                .and_then(Value::as_table_mut)
            {
                standalone_server.remove("mysql_port");
            }
        }
    }

    let cluster = table_mut(root, "cluster");
    match role {
        ClusterProcessRole::Fe => {
            cluster.insert("role".to_string(), Value::String("fe".to_string()));
            cluster.insert("heartbeat_interval_ms".to_string(), Value::Integer(500));
            cluster.insert("heartbeat_timeout_retries".to_string(), Value::Integer(2));
            cluster.remove("backends");
            cluster.remove("frontend_endpoint");
        }
        ClusterProcessRole::Be => {
            cluster.insert("role".to_string(), Value::String("be".to_string()));
            cluster.remove("backends");
            cluster.insert(
                "frontend_endpoint".to_string(),
                Value::String(format!("127.0.0.1:{}", runtime.fe_grpc_port)),
            );
        }
    }

    if role == ClusterProcessRole::Be {
        root.remove("state_store");
        root.remove("catalog_source");
    }

    toml::to_string(&value).context("serialize cross-process standalone config")
}

struct CrossProcessLaunchConfig<'a> {
    base_config: &'a str,
    source_config_dir: &'a Path,
    role: ClusterProcessRole,
    be_index: usize,
    runtime: &'a CrossProcessRuntime,
    runtime_dir: &'a Path,
    query_lifecycle_faults_enabled: bool,
    cleanup_faults_enabled: bool,
    overlay: Option<&'a str>,
    native_trust_fixture: &'a PreparedNativeTrustFixture,
}

fn render_cross_process_launch_config(config: CrossProcessLaunchConfig<'_>) -> Result<String> {
    let CrossProcessLaunchConfig {
        base_config,
        source_config_dir,
        role,
        be_index,
        runtime,
        runtime_dir,
        query_lifecycle_faults_enabled,
        cleanup_faults_enabled,
        overlay,
        native_trust_fixture,
    } = config;
    let rendered = render_cross_process_config(base_config, role, be_index, runtime)?;
    let mut value = rendered
        .parse::<Value>()
        .context("parse rendered cross-process launch config")?;
    let root = value
        .as_table_mut()
        .context("rendered cross-process launch config root must be a TOML table")?;
    if let Some(overlay) = overlay {
        merge_safe_config_overlay(root, overlay)?;
    }
    materialize_static_catalog_snapshot(root, runtime_dir, source_config_dir)?;
    native_trust_fixture.apply_config(root);
    let cluster = table_mut(root, "cluster");
    cluster.insert(
        "advertise_host".to_string(),
        Value::String(native_trust_fixture.fixture.advertise_host().to_string()),
    );
    if role == ClusterProcessRole::Be {
        let frontend_endpoint = NativeEndpoint::from_host_port(
            native_trust_fixture.fixture.advertise_host(),
            runtime.fe_grpc_port,
        )
        .map_err(anyhow::Error::msg)
        .context("render frontend endpoint with the exact native trust reference")?;
        cluster.insert(
            "frontend_endpoint".to_string(),
            Value::String(frontend_endpoint.to_string()),
        );
    }
    // `role = fe` persists backend membership in StateStore. Every ephemeral
    // SQL-test FE needs its own store so it cannot restore membership rows
    // whose dynamically allocated BE endpoints belong to another launch.
    if role == ClusterProcessRole::Fe {
        let state_store = root
            .get_mut("state_store")
            .and_then(Value::as_table_mut)
            .context("cross-process FE config requires [state_store]")?;
        state_store.insert(
            "path".to_string(),
            Value::String(
                runtime_dir
                    .join(FE_STATE_STORE_FILE_NAME)
                    .to_string_lossy()
                    .into_owned(),
            ),
        );
    }
    if query_lifecycle_faults_enabled {
        // The production terminal-retention contract remains 120s.  Runner
        // fault scenarios use a short, self-contained lease so a deliberately
        // crashed FE proves BE runtime release and bounded record reclamation
        // without turning the distributed suite into a two-minute sleep.
        let runtime_table = table_mut(root, "runtime");
        runtime_table.insert(
            "query_control_terminal_ack_timeout_ms".to_string(),
            Value::Integer(500),
        );
        runtime_table.insert(
            "query_control_terminal_fallback_rpc_timeout_ms".to_string(),
            Value::Integer(500),
        );
        runtime_table.insert(
            "query_control_terminal_fallback_initial_backoff_ms".to_string(),
            Value::Integer(50),
        );
        runtime_table.insert(
            "query_control_terminal_fallback_max_backoff_ms".to_string(),
            Value::Integer(100),
        );
        runtime_table.insert(
            "query_control_terminal_retention_ms".to_string(),
            Value::Integer(2_000),
        );
    }
    if cleanup_faults_enabled && role == ClusterProcessRole::Fe {
        let debug = table_mut(root, "debug");
        debug.insert(
            "cleanup_fault_dir".to_string(),
            Value::String(
                runtime_dir
                    .join("connector-cleanup-faults")
                    .to_string_lossy()
                    .into_owned(),
            ),
        );
    }
    toml::to_string(&value).context("serialize cross-process launch config")
}

/// Copy a caller-owned static snapshot into the per-launch runtime directory.
///
/// The generated FE config then uses a relative path, so Server resolves it
/// against that isolated config directory instead of the caller's CWD. Dynamic
/// source mode retains its StateStore path owned below by the harness.
fn materialize_static_catalog_snapshot(
    root: &mut toml::map::Map<String, Value>,
    runtime_dir: &Path,
    source_config_dir: &Path,
) -> Result<()> {
    let Some(source) = root.get_mut("catalog_source").and_then(Value::as_table_mut) else {
        return Ok(());
    };
    if source.get("mode").and_then(Value::as_str) != Some("static-file") {
        return Ok(());
    }
    let configured_path = source
        .get("static_file_path")
        .and_then(Value::as_str)
        .context("static-file catalog source requires static_file_path")?;
    let configured_path = Path::new(configured_path);
    let source_path = if configured_path.is_absolute() {
        configured_path.to_path_buf()
    } else {
        source_config_dir.join(configured_path)
    };
    let target_path = runtime_dir.join("catalogs.toml");
    fs::copy(&source_path, &target_path).with_context(|| {
        format!(
            "copy static catalog snapshot {} into isolated runtime directory {}",
            source_path.display(),
            runtime_dir.display()
        )
    })?;
    source.insert(
        "static_file_path".to_string(),
        Value::String("catalogs.toml".to_string()),
    );
    Ok(())
}

struct QueryLifecycleFaultFiles {
    root: PathBuf,
    be_count: usize,
}

impl QueryLifecycleFaultFiles {
    fn new(root: &Path, be_count: usize) -> Result<Self> {
        if be_count == 0 {
            bail!("query lifecycle fault scope requires at least one BE");
        }
        fs::create_dir_all(root)
            .with_context(|| format!("create query lifecycle fault scope {}", root.display()))?;
        Ok(Self {
            root: root.to_path_buf(),
            be_count,
        })
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn init_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::InitAckDrop)
    }

    fn heartbeat_stop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::HeartbeatStop)
    }

    fn restart_after_init_ack_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::RestartAfterInitAck)
    }

    fn stage_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::StageAckDrop)
    }

    fn start_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::StartAckDrop)
    }

    fn start_ack_suppress_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::StartAckSuppress)
    }

    fn terminal_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::TerminalAckDrop)
    }

    fn terminal_snapshot_stream_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::TerminalSnapshotStreamDrop)
    }

    fn terminal_snapshot_conflict_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::TerminalSnapshotConflict)
    }

    fn heartbeat_stop_after_stage_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, QueryLifecycleFaultKind::HeartbeatStopAfterStage)
    }

    fn rfo_8r2_fault_path(&self, index: usize, kind: &'static str) -> Result<PathBuf> {
        let kind = parse_runner_rfo_kind(kind).ok_or_else(|| {
            anyhow::anyhow!("unsupported RFO-8R2 query lifecycle fault kind {kind}")
        })?;
        self.be_path(index, kind)
    }

    fn fe_crash_path(&self) -> PathBuf {
        self.root.join("fe-crash-after-control-ready.trigger")
    }

    fn fragment_backend_limit_path(&self) -> PathBuf {
        self.root.join("fragment-backend-limit.trigger")
    }

    fn stage_prepare_failure_path(&self) -> PathBuf {
        self.root.join("stage-prepare-fail.trigger")
    }

    fn kill_query_at_phase_path(&self, phase: QueryLifecyclePhase) -> PathBuf {
        self.root
            .join(format!("kill-query-at-{}.trigger", phase.as_str()))
    }

    fn fe_crash_at_phase_path(&self, phase: QueryLifecyclePhase) -> PathBuf {
        self.root
            .join(format!("fe-crash-at-{}.trigger", phase.as_str()))
    }

    fn mv_known_committed_before_projector_cas_trigger_path(&self) -> PathBuf {
        mv_known_committed_before_projector_cas_trigger_path(&self.root)
    }

    fn mv_known_committed_before_projector_cas_marker_path(&self) -> PathBuf {
        mv_known_committed_before_projector_cas_marker_path(&self.root)
    }

    fn hold_start_until_early_ingress_path(&self) -> PathBuf {
        self.root.join("hold-start-until-early-ingress.trigger")
    }

    fn publish_init_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.init_ack_drop_path(index)?, index, None)
    }

    fn publish_heartbeat_stop(&self, index: usize) -> Result<String> {
        self.publish(self.heartbeat_stop_path(index)?, index, None)
    }

    fn publish_restart_after_init_ack(&self, index: usize) -> Result<String> {
        self.publish(self.restart_after_init_ack_path(index)?, index, None)
    }

    fn publish_stage_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.stage_ack_drop_path(index)?, index, None)
    }

    fn publish_start_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.start_ack_drop_path(index)?, index, None)
    }

    fn publish_start_ack_suppress(&self, index: usize) -> Result<String> {
        self.publish(self.start_ack_suppress_path(index)?, index, None)
    }

    fn publish_terminal_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.terminal_ack_drop_path(index)?, index, None)
    }

    fn publish_terminal_snapshot_stream_drop(&self, index: usize) -> Result<String> {
        self.publish(self.terminal_snapshot_stream_drop_path(index)?, index, None)
    }

    fn publish_terminal_snapshot_conflict(&self, index: usize) -> Result<String> {
        self.publish(self.terminal_snapshot_conflict_path(index)?, index, None)
    }

    fn publish_heartbeat_stop_after_stage(&self, index: usize) -> Result<String> {
        self.publish(self.heartbeat_stop_after_stage_path(index)?, index, None)
    }

    fn publish_rfo_8r2_fault(&self, index: usize, kind: &'static str) -> Result<String> {
        self.publish(self.rfo_8r2_fault_path(index, kind)?, index, None)
    }

    fn publish_fe_crash(&self, count: usize) -> Result<String> {
        self.publish(self.fe_crash_path(), self.be_count, Some(count))
    }

    fn publish_fragment_backend_limit(&self, limit: usize) -> Result<String> {
        self.publish(
            self.fragment_backend_limit_path(),
            self.be_count,
            Some(limit),
        )
    }

    fn publish_stage_prepare_failure(&self, ordinal: usize) -> Result<String> {
        self.publish_fields(
            self.stage_prepare_failure_path(),
            self.be_count,
            "ordinal",
            ordinal,
        )
    }

    fn publish_kill_query_at_phase(&self, phase: QueryLifecyclePhase) -> Result<String> {
        self.publish_fields(
            self.kill_query_at_phase_path(phase),
            self.be_count,
            "phase",
            phase.as_str(),
        )
    }

    fn publish_fe_crash_at_phase(&self, phase: QueryLifecyclePhase) -> Result<String> {
        self.publish_fields(
            self.fe_crash_at_phase_path(phase),
            self.be_count,
            "phase",
            phase.as_str(),
        )
    }

    fn publish_mv_known_committed_before_projector_cas(&self) -> Result<String> {
        let marker = self.mv_known_committed_before_projector_cas_marker_path();
        if marker.exists() {
            remove_fragment_failure_file(&marker).with_context(|| {
                format!(
                    "clear stale MV projector barrier marker {}",
                    marker.display()
                )
            })?;
        }
        self.publish_fields(
            self.mv_known_committed_before_projector_cas_trigger_path(),
            self.be_count,
            "phase",
            "known-committed-before-projector-cas",
        )
    }

    fn publish_hold_start_until_early_ingress(&self) -> Result<String> {
        self.publish_fields(
            self.hold_start_until_early_ingress_path(),
            self.be_count,
            "enabled",
            "true",
        )
    }

    fn publish(&self, path: PathBuf, identity: usize, value: Option<usize>) -> Result<String> {
        let token = next_fragment_failure_token(identity);
        let contents = match value {
            Some(value) => format!("{token}\n{value}\n"),
            None => format!("token={token}\nbackend_index={identity}\n"),
        };
        publish_query_lifecycle_fault_token(&path, &token, contents.as_bytes())?;
        Ok(token)
    }

    fn publish_fields(
        &self,
        path: PathBuf,
        identity: usize,
        field: &str,
        value: impl std::fmt::Display,
    ) -> Result<String> {
        let token = next_fragment_failure_token(identity);
        let contents = format!("token={token}\n{field}={value}\n");
        publish_query_lifecycle_fault_token(&path, &token, contents.as_bytes())?;
        Ok(token)
    }

    fn clear(&self) -> Result<()> {
        for entry in fs::read_dir(&self.root)
            .with_context(|| format!("read query lifecycle fault scope {}", self.root.display()))?
        {
            let path = entry?.path();
            if path.is_file() {
                remove_fragment_failure_file(&path).with_context(|| {
                    format!("remove query lifecycle fault trigger {}", path.display())
                })?;
            }
        }
        Ok(())
    }

    fn be_path(&self, index: usize, kind: QueryLifecycleFaultKind) -> Result<PathBuf> {
        if index >= self.be_count {
            bail!(
                "BE index {index} is out of bounds for query lifecycle fault scope with {} BE(s)",
                self.be_count
            );
        }
        Ok(lifecycle_arm_path(&self.root, index, kind))
    }
}

impl Drop for QueryLifecycleFaultFiles {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct CleanupFaultFiles {
    root: PathBuf,
}

impl CleanupFaultFiles {
    fn new(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("create connector cleanup fault scope {}", root.display()))?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn arm(&self, kind: &str) -> Result<()> {
        let kind = parse_cleanup_fault_directive(kind)
            .ok_or_else(|| anyhow::anyhow!("unsupported connector cleanup fault {kind}"))?;
        let path = cleanup_trigger_path(&self.root, kind);
        let token = next_fragment_failure_token(0);
        publish_query_lifecycle_fault_token(&path, &token, token.as_bytes())
            .with_context(|| format!("publish connector cleanup fault {}", kind.directive_name()))
    }

    fn clear(&self) -> Result<()> {
        match fs::remove_dir_all(&self.root) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "clear connector cleanup fault scope {}",
                        self.root.display()
                    )
                });
            }
        }
        fs::create_dir_all(&self.root).with_context(|| {
            format!(
                "recreate connector cleanup fault scope {}",
                self.root.display()
            )
        })
    }
}

impl Drop for CleanupFaultFiles {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub struct CrossProcessServerHandle {
    target_host: String,
    target_port: u16,
    mysql_user: String,
    be_grpc_ports: Vec<u16>,
    fragment_failure_trigger_paths: Vec<PathBuf>,
    fragment_failure_tokens: Vec<Option<String>>,
    query_lifecycle_fault_files: QueryLifecycleFaultFiles,
    query_lifecycle_fault_tokens: BTreeMap<(usize, &'static str), String>,
    query_lifecycle_faults_enabled: bool,
    cleanup_fault_files: Option<CleanupFaultFiles>,
    cleanup_faults_enabled: bool,
    runtime_dir: PathBuf,
    runtime: CrossProcessRuntime,
    native_trust_fixture: PreparedNativeTrustFixture,
    novarocks_bin: PathBuf,
    be_config_paths: Vec<PathBuf>,
    fe_config_path: PathBuf,
    be_processes: Vec<ManagedProcess>,
    fe_process: ManagedProcess,
    be_log_history: Vec<String>,
    fe_log_history: String,
    startup_timeout: Duration,
    fe_environment: BTreeMap<String, String>,
    be_environments: Vec<BTreeMap<String, String>>,
    retain_runtime_artifacts: bool,
}

struct RuntimeDirGuard {
    runtime_dir: Option<PathBuf>,
}

impl RuntimeDirGuard {
    fn new(runtime_dir: PathBuf) -> Self {
        Self {
            runtime_dir: Some(runtime_dir),
        }
    }

    fn path(&self) -> &Path {
        self.runtime_dir.as_deref().expect("runtime dir available")
    }

    fn into_path(mut self) -> PathBuf {
        self.runtime_dir.take().expect("runtime dir available")
    }
}

impl Drop for RuntimeDirGuard {
    fn drop(&mut self) {
        if let Some(runtime_dir) = self.runtime_dir.take() {
            let _ = fs::remove_dir_all(runtime_dir);
        }
    }
}

impl CrossProcessServerHandle {
    /// Launch one normal ephemeral cross-process cluster from resolved inputs.
    pub fn launch(options: CrossProcessClusterOptions) -> Result<Self> {
        let CrossProcessClusterOptions {
            binary: novarocks_bin,
            base_config_path,
            runtime_root,
            cluster_size,
            query_lifecycle_faults_enabled,
            cleanup_faults_enabled,
            startup_timeout,
            child_environment,
            config_overlay,
            native_trust_fixture,
        } = options;
        let mut fe_environment = child_environment.fe;
        let mut be_environments = resolve_be_environments(
            &child_environment.be,
            &child_environment.be_by_index,
            cluster_size,
        )?;
        let runtime_dir = RuntimeDirGuard::new(create_runtime_dir(&runtime_root)?);
        let native_trust_fixture =
            PreparedNativeTrustFixture::prepare(native_trust_fixture, runtime_dir.path())?;
        fe_environment.insert(
            SYSTEM_NATIVE_TRUST_SECRET_ENV.to_string(),
            native_trust_fixture.shared_secret.clone(),
        );
        for environment in &mut be_environments {
            environment.insert(
                SYSTEM_NATIVE_TRUST_SECRET_ENV.to_string(),
                native_trust_fixture.shared_secret.clone(),
            );
        }
        let reserved = ReservedRuntimePorts::new(cluster_size)?;
        let query_lifecycle_fault_files = QueryLifecycleFaultFiles::new(
            &runtime_dir.path().join("query-lifecycle-faults"),
            cluster_size,
        )?;
        let cleanup_fault_files = cleanup_faults_enabled
            .then(|| CleanupFaultFiles::new(&runtime_dir.path().join("connector-cleanup-faults")))
            .transpose()?;

        // Build runtime port record from reserved ports (before releasing any).
        let runtime = CrossProcessRuntime {
            be: reserved
                .be_ports
                .iter()
                .map(|bp| BePorts {
                    http: bp.http.port(),
                    grpc: bp.grpc.port(),
                })
                .collect(),
            fe_http_port: reserved.fe_http_port.port(),
            fe_grpc_port: reserved.fe_grpc_port.port(),
            fe_mysql_port: reserved.fe_mysql_port.port(),
        };

        let base_config = fs::read_to_string(&base_config_path).with_context(|| {
            format!(
                "read standalone config for cross-process mode: {}",
                base_config_path.display()
            )
        })?;
        let mysql_user = base_config
            .parse::<Value>()
            .ok()
            .and_then(|value| {
                value
                    .get("standalone_server")
                    .and_then(|server| server.get("user"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| "root".to_string());

        let render = |role: ClusterProcessRole, be_index: usize| -> Result<String> {
            render_cross_process_launch_config(CrossProcessLaunchConfig {
                base_config: &base_config,
                source_config_dir: base_config_path.parent().unwrap_or_else(|| Path::new(".")),
                role,
                be_index,
                runtime: &runtime,
                runtime_dir: runtime_dir.path(),
                query_lifecycle_faults_enabled,
                cleanup_faults_enabled,
                overlay: match role {
                    ClusterProcessRole::Fe => config_overlay.fe.as_deref(),
                    ClusterProcessRole::Be => config_overlay.be.as_deref(),
                },
                native_trust_fixture: &native_trust_fixture,
            })
        };

        // Write per-BE configs.
        let mut be_config_paths: Vec<PathBuf> = Vec::with_capacity(cluster_size);
        let fragment_failure_trigger_paths = (0..cluster_size)
            .map(|index| {
                runtime_dir
                    .path()
                    .join(format!("be_{index}.fragment_failure_trigger"))
            })
            .collect::<Vec<_>>();
        for i in 0..cluster_size {
            let be_config_path = runtime_dir.path().join(format!("be_{i}.toml"));
            fs::write(&be_config_path, render(ClusterProcessRole::Be, i)?)
                .with_context(|| format!("write {}", be_config_path.display()))?;
            be_config_paths.push(be_config_path);
        }

        // Write FE config.
        let fe_config_path = runtime_dir.path().join("fe.toml");
        fs::write(&fe_config_path, render(ClusterProcessRole::Fe, 0)?)
            .with_context(|| format!("write {}", fe_config_path.display()))?;

        // Start FE before BEs so every backend uses the same authenticated
        // self-registration ingress from its first announce attempt.
        let _ = reserved.fe_http_port.release();
        let _ = reserved.fe_grpc_port.release();
        let _ = reserved.fe_mysql_port.release();
        let mut fe_process = spawn_novarocks_process(ProcessLaunch {
            binary: &novarocks_bin,
            role: "fe",
            config_path: &fe_config_path,
            marker: "NOVAROCKS_READY mysql_port=",
            startup_timeout,
            log_path: runtime_dir.path().join("fe.log"),
            fragment_failure_trigger: None,
            query_lifecycle_fault_scope: query_lifecycle_faults_enabled
                .then_some((query_lifecycle_fault_files.root(), None)),
            cleanup_fault_dir: cleanup_fault_files.as_ref().map(CleanupFaultFiles::root),
            child_environment: &fe_environment,
        })?;
        println!(
            "started cross-process FE pid={} mysql_port={} config={}",
            fe_process.pid(),
            runtime.fe_mysql_port,
            fe_config_path.display()
        );

        // Spawn all BEs: release each BE's ports immediately before spawning it.
        let mut be_processes: Vec<ManagedProcess> = Vec::with_capacity(cluster_size);
        for (i, (reserved_be, be_config_path)) in reserved
            .be_ports
            .into_iter()
            .zip(be_config_paths.iter())
            .enumerate()
        {
            let grpc_port = reserved_be.grpc.port();
            let _ = reserved_be.http.release();
            let _ = reserved_be.grpc.release();
            let be_process = spawn_novarocks_process(ProcessLaunch {
                binary: &novarocks_bin,
                role: "be",
                config_path: be_config_path,
                marker: "NOVAROCKS_READY role=be",
                startup_timeout,
                log_path: runtime_dir.path().join(format!("be_{i}.log")),
                fragment_failure_trigger: Some(&fragment_failure_trigger_paths[i]),
                query_lifecycle_fault_scope: query_lifecycle_faults_enabled
                    .then_some((query_lifecycle_fault_files.root(), Some(i))),
                cleanup_fault_dir: None,
                child_environment: &be_environments[i],
            })?;
            println!(
                "started cross-process BE[{i}] pid={} grpc_port={} config={}",
                be_process.pid(),
                grpc_port,
                be_config_path.display()
            );
            be_processes.push(be_process);
        }

        wait_for_live_backend_topology(
            LiveBackendTopologyWait {
                mysql_user: &mysql_user,
                runtime: &runtime,
                expected_ports: &runtime.be.iter().map(|be| be.grpc).collect::<Vec<_>>(),
                fe_config_path: &fe_config_path,
                be_config_paths: &be_config_paths,
                timeout: startup_timeout,
            },
            &mut fe_process,
            &mut be_processes,
        )
        .context("cross-process backend topology barrier")?;
        assert_role_scoped_metrics(&runtime)
            .context("cross-process role-scoped metrics barrier")?;

        Ok(Self {
            target_host: "127.0.0.1".to_string(),
            target_port: runtime.fe_mysql_port,
            mysql_user,
            be_grpc_ports: runtime.be.iter().map(|be| be.grpc).collect(),
            fragment_failure_trigger_paths,
            fragment_failure_tokens: vec![None; cluster_size],
            query_lifecycle_fault_files,
            query_lifecycle_fault_tokens: BTreeMap::new(),
            query_lifecycle_faults_enabled,
            cleanup_fault_files,
            cleanup_faults_enabled,
            runtime_dir: runtime_dir.into_path(),
            runtime,
            native_trust_fixture,
            novarocks_bin,
            be_config_paths,
            fe_config_path,
            be_processes,
            fe_process,
            be_log_history: vec![String::new(); cluster_size],
            fe_log_history: String::new(),
            startup_timeout,
            fe_environment,
            be_environments,
            retain_runtime_artifacts: false,
        })
    }

    /// Frozen runtime ports and endpoints for this launched cluster.
    pub fn runtime(&self) -> &CrossProcessRuntime {
        &self.runtime
    }

    /// The selected harness-owned Native transport profile.
    pub fn native_trust_mode(&self) -> NativeTrustFixtureMode {
        self.native_trust_fixture.fixture.mode()
    }

    /// Build one endpoint using the exact advertised reference identity that
    /// the FE topology and TLS verifier used for this BE.
    pub fn native_be_endpoint(&self, index: usize) -> Result<NativeEndpoint> {
        let port = self
            .runtime
            .be
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("native BE index {index} is out of bounds"))?
            .grpc;
        NativeEndpoint::from_host_port(self.native_trust_fixture.fixture.advertise_host(), port)
            .map_err(anyhow::Error::msg)
            .context("construct harness Native BE endpoint")
    }

    /// Construct an authenticated test caller with the same deployment key as
    /// the launched cluster. The secret is never returned or serialized.
    pub fn native_probe_trust(&self) -> Result<NativeTrust> {
        self.native_trust_fixture.probe_trust()
    }

    /// Construct a raw-probe connector for an explicitly selected transport
    /// mode. Negative scenarios use a mode different from `native_trust_mode`
    /// to prove that the listener has no transport fallback.
    pub fn native_probe_connector(
        &self,
        endpoint: NativeEndpoint,
        mode: NativeTrustFixtureMode,
    ) -> Result<NativeEndpointConnector> {
        self.native_trust_fixture.probe_connector(endpoint, mode)
    }

    /// Read the BE-owned terminal fallback acceptance counter for one live
    /// cross-process backend. System scenarios use this only to prove that an
    /// intentionally unacknowledged terminal report reached the FE fallback
    /// endpoint; it does not alter lifecycle delivery.
    pub fn backend_terminal_fallback_accepted(&self, index: usize) -> Result<f64> {
        self.ensure_be_index(index)?;
        let metrics = scrape_prometheus_metrics(self.runtime.be[index].http)
            .with_context(|| format!("scrape cross-process BE[{index}] /metrics"))?;
        prometheus_labeled_gauge(
            &metrics,
            QUERY_LIFECYCLE_TERMINAL_METRIC,
            "outcome",
            TERMINAL_FALLBACK_ACCEPTED_OUTCOME,
        )
        .with_context(|| format!("read BE[{index}] terminal fallback accepted count"))
    }

    /// Directory containing generated process config and captured logs.
    pub fn runtime_dir(&self) -> &Path {
        &self.runtime_dir
    }

    /// Path of this launch's FE durable SQLite StateStore.
    ///
    /// `render_cross_process_launch_config` renders exactly this path into the
    /// generated FE config, and `merge_safe_config_overlay` rejects any overlay
    /// that touches `[state_store]`, so the launched FE cannot own a different
    /// durable store.
    pub fn fe_state_store_path(&self) -> PathBuf {
        self.runtime_dir.join(FE_STATE_STORE_FILE_NAME)
    }

    /// Restart the FE against a brand-new empty durable store, using this
    /// handle's startup timeout.
    pub fn wipe_fe_state_store_and_restart(&mut self) -> Result<()> {
        self.wipe_fe_state_store_and_restart_until(Instant::now() + self.startup_timeout)
    }

    /// Stop the FE, destroy its durable StateStore, then start the FE again and
    /// wait until it is ready, all before `deadline`.
    ///
    /// BE processes are deliberately left running, so a caller observes exactly
    /// one FE that lost every durable record while the lake and the live
    /// backends stayed where they were. Stop, start and readiness reuse the
    /// same `ServerHandle::kill_fe` / `ServerHandle::restart_fe_until` paths an
    /// ordinary FE restart uses; only the store destruction is new.
    ///
    /// The reused restart path ends in the `SHOW BACKENDS` topology barrier.
    /// Backends self-register, so the restarted FE rebuilds membership from
    /// their announcements rather than from anything the destroyed store held;
    /// the barrier therefore converges without the store contributing to it.
    pub fn wipe_fe_state_store_and_restart_until(&mut self, deadline: Instant) -> Result<()> {
        ServerHandle::kill_fe(self)
            .context("stop cross-process FE before destroying its durable state store")?;
        let removed = remove_fe_state_store(&self.runtime_dir, &self.fe_config_path)
            .context("destroy cross-process FE durable state store")?;
        println!(
            "destroyed cross-process FE durable state store {}: removed={:?}",
            self.fe_state_store_path().display(),
            removed
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
        );
        ServerHandle::restart_fe_until(self, deadline)
            .context("restart cross-process FE against an empty durable state store")
    }

    /// MySQL user parsed from the supplied base config.
    pub fn mysql_user(&self) -> &str {
        &self.mysql_user
    }

    /// Begins the real FE SIGTERM drain without waiting for process exit, so a
    /// system scenario can inspect management/readiness during the drain.
    #[cfg(unix)]
    pub fn begin_fe_drain(&mut self) -> Result<()> {
        self.fe_process
            .request_termination()
            .context("send SIGTERM to cross-process FE")
    }

    /// Waits for a prior `begin_fe_drain` request to finish successfully.
    #[cfg(unix)]
    pub fn wait_fe_exit_until(&mut self, deadline: Instant) -> Result<()> {
        self.fe_process
            .wait_for_successful_exit_until(deadline)
            .context("wait for cross-process FE graceful exit")
            .map(|_| ())
    }

    /// Reads one FE management endpoint with a caller-owned deadline.
    pub fn frontend_management_get(
        &self,
        path: &str,
        timeout: Duration,
    ) -> Result<FrontendManagementResponse> {
        get_frontend_management(self.runtime.fe_http_port, path, timeout)
    }

    /// Keep generated config and logs when the handle is dropped.
    pub fn retain_runtime_artifacts(&mut self) {
        self.retain_runtime_artifacts = true;
    }

    /// A bounded process and log diagnostic suitable for scenario failures.
    pub fn diagnostics(&self) -> String {
        self.query_execution_resource_diagnostics_impl()
    }

    /// Stop this cluster explicitly. Retained artifacts remain available.
    pub fn shutdown(&mut self) -> Result<()> {
        let mut failures = Vec::new();
        if let Err(error) = self.fe_process.stop() {
            failures.push(format!("stop cross-process FE: {error:#}"));
        }
        for (index, process) in self.be_processes.iter_mut().enumerate() {
            if let Err(error) = process.stop() {
                failures.push(format!("stop cross-process BE[{index}]: {error:#}"));
            }
        }
        if !self.retain_runtime_artifacts
            && let Err(error) = fs::remove_dir_all(&self.runtime_dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            failures.push(format!(
                "remove cross-process runtime {}: {error}",
                self.runtime_dir.display()
            ));
        }
        // A retained failure artifact must keep redacted configs and logs for
        // diagnosis, but never the generated private key or certificate PEM.
        self.native_trust_fixture.cleanup_sensitive_material();
        if failures.is_empty() {
            Ok(())
        } else {
            bail!(failures.join("; "))
        }
    }

    fn ensure_be_index(&self, index: usize) -> Result<()> {
        if index >= self.be_processes.len() {
            bail!(
                "BE index {} is out of bounds for cross-process cluster with {} BE(s)",
                index,
                self.be_processes.len()
            );
        }
        Ok(())
    }

    fn query_execution_resource_snapshot_impl(&mut self) -> Result<QueryExecutionResourceSnapshot> {
        let fe_running = self
            .fe_process
            .is_running()
            .context("inspect FE process state")?;
        let frontend_control_ready = if fe_running {
            let metrics = scrape_prometheus_metrics(self.runtime.fe_http_port)
                .context("scrape cross-process FE /metrics")?;
            prometheus_labeled_gauge(
                &metrics,
                FRONTEND_QUERY_LIFECYCLE_CONTROL_METRIC,
                "outcome",
                "control_ready",
            )
            .context("read FE query lifecycle control-ready count")?
        } else {
            0.0
        };
        let mut backends = Vec::with_capacity(self.be_processes.len());
        for (index, (process, ports)) in self
            .be_processes
            .iter()
            .zip(self.runtime.be.iter())
            .enumerate()
        {
            let process_running = process
                .is_running()
                .with_context(|| format!("inspect cross-process BE[{index}] state"))?;
            if !process_running {
                backends.push(BackendResourceSnapshot {
                    index,
                    process_running,
                    resources: BTreeMap::new(),
                    terminal_retained: 0.0,
                    terminal_retained_bytes: 0.0,
                    terminal_retained_capacity: 0.0,
                    terminal_max_retained_bytes: 0.0,
                });
                continue;
            }

            let metrics = scrape_prometheus_metrics(ports.http)
                .with_context(|| format!("scrape cross-process BE[{index}] /metrics"))?;
            let mut resources = BTreeMap::new();
            for resource in HEAVY_QUERY_EXECUTION_RESOURCES
                .into_iter()
                .chain(std::iter::once(QUERY_EXECUTION_RESOURCE_BINDING_LEASE))
            {
                let value = prometheus_labeled_gauge(
                    &metrics,
                    QUERY_EXECUTION_RESOURCE_METRIC,
                    "resource",
                    resource,
                )
                .with_context(|| format!("read BE[{index}] heavy resource {resource}"))?;
                resources.insert(resource.to_string(), value);
            }
            backends.push(BackendResourceSnapshot {
                index,
                process_running,
                resources,
                terminal_retained: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_RETAINED_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained count"))?,
                terminal_retained_bytes: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_RETAINED_BYTES_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained bytes"))?,
                terminal_retained_capacity: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_RETAINED_CAPACITY_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained capacity"))?,
                terminal_max_retained_bytes: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_MAX_RETAINED_BYTES_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained byte limit"))?,
            });
        }
        Ok(QueryExecutionResourceSnapshot {
            fe_running,
            frontend_control_ready,
            backends,
        })
    }

    fn query_execution_resource_diagnostics_impl(&self) -> String {
        let tail = |contents: Result<String>| match contents {
            Ok(contents) => contents
                .lines()
                .rev()
                .take(20)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\\n"),
            Err(error) => format!("<read failed: {error:#}>"),
        };
        let fe_state = self
            .fe_process
            .is_running()
            .map(|running| if running { "running" } else { "exited" })
            .unwrap_or("unknown");
        let be = self
            .be_processes
            .iter()
            .enumerate()
            .map(|(index, process)| {
                let state = process
                    .is_running()
                    .map(|running| if running { "running" } else { "exited" })
                    .unwrap_or("unknown");
                format!(
                    "BE[{index}]={state} log_tail={:?}",
                    tail(process.log_contents())
                )
            })
            .collect::<Vec<_>>();
        format!(
            "FE={fe_state} log_tail={:?}; {}",
            tail(self.fe_process.log_contents()),
            be.join("; ")
        )
    }
}

impl ServerHandle for CrossProcessServerHandle {
    fn target_host(&self) -> Option<&str> {
        Some(self.target_host.as_str())
    }

    fn target_port(&self) -> Option<u16> {
        Some(self.target_port)
    }

    fn supports_fault_injection(&self) -> bool {
        true
    }

    fn arm_cleanup_fault(&mut self, kind: &str) -> Result<()> {
        let files = self.cleanup_fault_files.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "connector cleanup fault scope was not enabled for this selected SQL case"
            )
        })?;
        files.arm(kind)?;
        println!(
            "armed connector cleanup fault {kind} root={}",
            files.root().display()
        );
        Ok(())
    }

    fn clear_cleanup_faults(&mut self) -> Result<()> {
        if let Some(files) = &self.cleanup_fault_files {
            files.clear()?;
        }
        Ok(())
    }

    fn supports_query_execution_resource_oracle(&self) -> bool {
        true
    }

    fn query_execution_resource_snapshot(
        &mut self,
    ) -> Result<Option<QueryExecutionResourceSnapshot>> {
        self.query_execution_resource_snapshot_impl().map(Some)
    }

    fn query_execution_resource_diagnostics(&self) -> String {
        self.query_execution_resource_diagnostics_impl()
    }

    fn query_lifecycle_structured_snapshot(
        &mut self,
    ) -> Result<Option<QueryLifecycleStructuredSnapshot>> {
        query_lifecycle_structured_snapshot_from_fe(self.runtime.fe_http_port)
    }

    fn be_count(&self) -> usize {
        self.be_processes.len()
    }

    fn arm_init_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_init_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "init-ack-drop"), token.clone());
        println!(
            "armed InitAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .init_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_query_control_heartbeat_stop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_heartbeat_stop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "heartbeat-stop"), token.clone());
        println!(
            "armed query-control heartbeat stop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .heartbeat_stop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_fe_crash_after_control_ready(&mut self, count: usize) -> Result<()> {
        if !(1..=self.be_processes.len()).contains(&count) {
            bail!(
                "FE crash ControlReady count {count} is outside 1..={}",
                self.be_processes.len()
            );
        }
        let token = self.query_lifecycle_fault_files.publish_fe_crash(count)?;
        println!(
            "armed FE crash after {count} ControlReady marker(s) token={token} trigger={}",
            self.query_lifecycle_fault_files.fe_crash_path().display()
        );
        Ok(())
    }

    fn arm_be_restart_after_init_ack(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_restart_after_init_ack(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "restart-after-init-ack"), token.clone());
        println!(
            "armed BE[{index}] restart after InitAck token={token} trigger={}",
            self.query_lifecycle_fault_files
                .restart_after_init_ack_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_stage_prepare_failure(&mut self, ordinal: usize) -> Result<()> {
        if ordinal == 0 {
            bail!("Stage prepare ordinal must be at least 1");
        }
        let token = self
            .query_lifecycle_fault_files
            .publish_stage_prepare_failure(ordinal)?;
        println!(
            "armed Stage prepare failure at ordinal={ordinal} token={token} trigger={}",
            self.query_lifecycle_fault_files
                .stage_prepare_failure_path()
                .display()
        );
        Ok(())
    }

    fn arm_stage_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_stage_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "stage-ack-drop"), token.clone());
        println!(
            "armed StageAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .stage_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_start_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_start_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "start-ack-drop"), token.clone());
        println!(
            "armed StartAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .start_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_start_ack_suppress(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_start_ack_suppress(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "start-ack-suppress"), token.clone());
        println!(
            "armed StartAck suppression for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .start_ack_suppress_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_terminal_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_terminal_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "terminal-ack-drop"), token.clone());
        println!(
            "armed TerminalAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .terminal_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_terminal_snapshot_stream_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_terminal_snapshot_stream_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "terminal-snapshot-stream-drop"), token.clone());
        println!(
            "armed TerminalSnapshot stream drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .terminal_snapshot_stream_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_terminal_snapshot_conflict(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_terminal_snapshot_conflict(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "terminal-snapshot-conflict"), token.clone());
        println!(
            "armed TerminalSnapshot conflict for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .terminal_snapshot_conflict_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_query_lifecycle_fault(&mut self, index: usize, kind: &'static str) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_rfo_8r2_fault(index, kind)?;
        self.query_lifecycle_fault_tokens
            .insert((index, kind), token.clone());
        println!(
            "armed RFO-8R2 query lifecycle fault kind={kind} for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .rfo_8r2_fault_path(index, kind)?
                .display()
        );
        Ok(())
    }

    fn arm_kill_query_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        let token = self
            .query_lifecycle_fault_files
            .publish_kill_query_at_phase(phase)?;
        println!(
            "armed KILL QUERY at lifecycle phase={} token={token} trigger={}",
            phase.as_str(),
            self.query_lifecycle_fault_files
                .kill_query_at_phase_path(phase)
                .display()
        );
        Ok(())
    }

    fn arm_fe_crash_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        let token = self
            .query_lifecycle_fault_files
            .publish_fe_crash_at_phase(phase)?;
        println!(
            "armed FE crash at lifecycle phase={} token={token} trigger={}",
            phase.as_str(),
            self.query_lifecycle_fault_files
                .fe_crash_at_phase_path(phase)
                .display()
        );
        Ok(())
    }

    fn arm_mv_known_committed_before_projector_cas(&mut self) -> Result<()> {
        let token = self
            .query_lifecycle_fault_files
            .publish_mv_known_committed_before_projector_cas()?;
        println!(
            "armed MV known-committed projector barrier token={token} trigger={}",
            self.query_lifecycle_fault_files
                .mv_known_committed_before_projector_cas_trigger_path()
                .display()
        );
        Ok(())
    }

    fn arm_be_kill_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        // `record_lifecycle_phase_marker_for_execution` is a shared FE-owned
        // barrier. Its kill-query trigger name describes the historical
        // consumer, not the runner action; this caller kills a BE after the
        // same immutable phase marker and releases that trigger itself.
        let token = self
            .query_lifecycle_fault_files
            .publish_kill_query_at_phase(phase)?;
        println!(
            "armed BE kill at lifecycle phase={} token={token} trigger={}",
            phase.as_str(),
            self.query_lifecycle_fault_files
                .kill_query_at_phase_path(phase)
                .display()
        );
        Ok(())
    }

    fn arm_query_control_heartbeat_stop_after_stage(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_heartbeat_stop_after_stage(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "heartbeat-stop-after-stage"), token.clone());
        println!(
            "armed query-control heartbeat stop after Stage for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .heartbeat_stop_after_stage_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_hold_start_until_early_ingress(&mut self) -> Result<()> {
        let token = self
            .query_lifecycle_fault_files
            .publish_hold_start_until_early_ingress()?;
        println!(
            "armed Start hold until early ingress token={token} trigger={}",
            self.query_lifecycle_fault_files
                .hold_start_until_early_ingress_path()
                .display()
        );
        Ok(())
    }

    fn release_query_lifecycle_phase_fault(
        &mut self,
        phase: QueryLifecyclePhase,
        fe_crash: bool,
    ) -> Result<()> {
        let path = if fe_crash {
            self.query_lifecycle_fault_files
                .fe_crash_at_phase_path(phase)
        } else {
            self.query_lifecycle_fault_files
                .kill_query_at_phase_path(phase)
        };
        remove_fragment_failure_file(&path).with_context(|| {
            format!(
                "release {} lifecycle phase fault {}",
                if fe_crash { "FE crash" } else { "KILL QUERY" },
                phase.as_str()
            )
        })
    }

    fn release_be_kill_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        let path = self
            .query_lifecycle_fault_files
            .kill_query_at_phase_path(phase);
        remove_fragment_failure_file(&path)
            .with_context(|| format!("release BE kill lifecycle phase barrier {}", phase.as_str()))
    }

    fn arm_query_control_fragment_backend_limit(&mut self, limit: usize) -> Result<()> {
        if !(1..=self.be_processes.len()).contains(&limit) {
            bail!(
                "query-control fragment backend limit {limit} is outside 1..={}",
                self.be_processes.len()
            );
        }
        let token = self
            .query_lifecycle_fault_files
            .publish_fragment_backend_limit(limit)?;
        println!(
            "armed query-control fragment backend limit={limit} token={token} trigger={}",
            self.query_lifecycle_fault_files
                .fragment_backend_limit_path()
                .display()
        );
        Ok(())
    }

    fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
        self.ensure_be_index(index)?;
        let grpc_port = self.be_grpc_ports[index];
        let rows = query_frontend_backend_topology(
            &self.mysql_user,
            &self.target_host,
            self.target_port,
            TOPOLOGY_MYSQL_IO_TIMEOUT_CAP,
        )?;
        rows.into_iter()
            .find(|row| row.grpc_port == grpc_port && row.is_eligible_live())
            .map(|row| row.scheduled_fragments)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "SHOW BACKENDS has no row for cross-process BE[{index}] grpc_port={grpc_port}"
                )
            })
    }

    fn backend_process_id(&self, index: usize) -> Result<novarocks_types::BackendProcessId> {
        self.ensure_be_index(index)?;
        let grpc_port = self.be_grpc_ports[index];
        let rows = query_frontend_backend_topology(
            &self.mysql_user,
            &self.target_host,
            self.target_port,
            TOPOLOGY_MYSQL_IO_TIMEOUT_CAP,
        )?;
        rows.iter()
            .find(|row| row.grpc_port == grpc_port && row.is_eligible_live())
            // During a deliberate drain, no live entry exists. The retained
            // diagnostic identity is only the old-value comparison for the
            // following replacement; it is never returned while a live entry
            // for this endpoint exists.
            .or_else(|| rows.iter().find(|row| row.grpc_port == grpc_port))
            .map(|row| row.process_id.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "SHOW BACKENDS has no row for cross-process BE[{index}] grpc_port={grpc_port}"
                )
            })?
            // The harness is where wire text becomes a typed fact: a caller
            // comparing two process identities must not be handed two strings
            // that merely look alike.
            .parse::<novarocks_types::BackendProcessId>()
            .with_context(|| {
                format!("SHOW BACKENDS process_id for BE[{index}] is not a backend process id")
            })
    }

    fn arm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        if self.fragment_failure_tokens[index].is_some() {
            bail!("cross-process BE[{index}] already has an armed fragment executor failure token");
        }
        let trigger_path = &self.fragment_failure_trigger_paths[index];
        remove_fragment_failure_file(&fragment_failure_release_path(trigger_path)).with_context(
            || {
                format!(
                    "clear stale fragment executor failure release for cross-process BE[{index}]"
                )
            },
        )?;
        let token = next_fragment_failure_token(index);
        publish_fragment_failure_token(trigger_path, &token).with_context(|| {
            format!(
                "arm fragment executor failure for cross-process BE[{index}] at {}",
                trigger_path.display()
            )
        })?;
        self.fragment_failure_tokens[index] = Some(token.clone());
        println!(
            "armed fragment executor failure for cross-process BE[{index}] trigger={} token={token}",
            trigger_path.display(),
        );
        Ok(())
    }

    fn release_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self.fragment_failure_tokens[index]
            .as_deref()
            .with_context(|| {
                format!("cross-process BE[{index}] has no armed fragment executor failure token")
            })?;
        let release_path =
            fragment_failure_release_path(&self.fragment_failure_trigger_paths[index]);
        publish_fragment_failure_token(&release_path, token).with_context(|| {
            format!(
                "release fragment executor failure for cross-process BE[{index}] at {}",
                release_path.display()
            )
        })?;
        println!(
            "released fragment executor failure for cross-process BE[{index}] release={} token={token}",
            release_path.display(),
        );
        Ok(())
    }

    fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let trigger_path = &self.fragment_failure_trigger_paths[index];
        remove_fragment_failure_file(trigger_path).with_context(|| {
            format!(
                "disarm fragment executor failure trigger for cross-process BE[{index}] at {}",
                trigger_path.display()
            )
        })?;
        let release_path = fragment_failure_release_path(trigger_path);
        remove_fragment_failure_file(&release_path).with_context(|| {
            format!(
                "disarm fragment executor failure release for cross-process BE[{index}] at {}",
                release_path.display()
            )
        })?;
        self.fragment_failure_tokens[index] = None;
        Ok(())
    }

    fn armed_fragment_failure_token(&self, index: usize) -> Result<Option<String>> {
        self.ensure_be_index(index)?;
        Ok(self.fragment_failure_tokens[index].clone())
    }

    fn assert_be_log(&self, index: usize, needle: &str) -> Result<()> {
        self.ensure_be_index(index)?;
        if self.be_log_history[index].contains(needle) {
            return Ok(());
        }
        self.be_processes[index].assert_log_contains(needle)
    }

    fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
        self.ensure_be_index(index)?;
        Ok(self.be_log_history[index].match_indices(needle).count()
            + self.be_processes[index].log_count(needle)?)
    }

    fn be_log_contents(&self, index: usize) -> Result<String> {
        self.ensure_be_index(index)?;
        let current = self.be_processes[index].log_contents()?;
        Ok(format!("{}{}", self.be_log_history[index], current))
    }

    fn be_current_log_contents(&self, index: usize) -> Result<String> {
        self.ensure_be_index(index)?;
        self.be_processes[index].log_contents()
    }

    fn fe_log_count(&self, needle: &str) -> Result<usize> {
        Ok(
            self.fe_log_history.match_indices(needle).count()
                + self.fe_process.log_count(needle)?,
        )
    }

    fn fe_log_contents(&self) -> Result<String> {
        let current = self.fe_process.log_contents()?;
        Ok(format!("{}{}", self.fe_log_history, current))
    }

    fn clear_query_lifecycle_faults(&mut self) -> Result<()> {
        self.query_lifecycle_fault_tokens.clear();
        self.query_lifecycle_fault_files.clear()
    }

    fn armed_query_lifecycle_fault_token(
        &self,
        index: usize,
        kind: &'static str,
    ) -> Result<Option<String>> {
        self.ensure_be_index(index)?;
        Ok(self
            .query_lifecycle_fault_tokens
            .get(&(index, kind))
            .cloned())
    }

    fn kill_be(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let be_process = self
            .be_processes
            .get_mut(index)
            .expect("BE index checked above");
        be_process
            .kill_now()
            .with_context(|| format!("kill cross-process BE[{index}]"))?;
        println!("killed cross-process BE[{index}]");
        Ok(())
    }

    fn restart_be(&mut self, index: usize) -> Result<()> {
        self.restart_be_until(index, Instant::now() + self.startup_timeout)
    }

    fn restart_be_until(&mut self, index: usize, deadline: Instant) -> Result<()> {
        self.ensure_be_index(index)?;
        let old_process_id = self.backend_process_id(index)?;
        let prior_log = self.be_processes[index]
            .log_contents()
            .with_context(|| format!("preserve cross-process BE[{index}] log before restart"))?;
        self.be_log_history[index].push_str(&prior_log);
        {
            let be_process = self
                .be_processes
                .get_mut(index)
                .expect("BE index checked above");
            be_process
                .kill_now()
                .with_context(|| format!("stop old cross-process BE[{index}] before restart"))?;
        }

        let config_path = self
            .be_config_paths
            .get(index)
            .ok_or_else(|| {
                anyhow::anyhow!("missing config path for cross-process BE[{index}] during restart")
            })?
            .clone();
        let marker = "NOVAROCKS_READY role=be";
        let mut command = build_novarocks_command(&self.novarocks_bin, "be", &config_path);
        command.env(
            "NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE",
            &self.fragment_failure_trigger_paths[index],
        );
        if self.query_lifecycle_faults_enabled {
            command
                .env(
                    novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV,
                    self.query_lifecycle_fault_files.root(),
                )
                .env(
                    "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX",
                    index.to_string(),
                );
        }
        apply_child_environment(&mut command, &self.be_environments[index]);
        let log_path = self.runtime_dir.join(format!("be_{index}.log"));
        let be_process = self
            .be_processes
            .get_mut(index)
            .expect("BE index checked above");
        be_process
            .restart(
                command,
                ReadyMarker::StdoutContains(marker.to_string()),
                remaining_until(deadline, "BE readiness")?,
                log_path,
            )
            .map_err(|error| map_novarocks_process_error(&self.novarocks_bin, "be", marker, error))
            .with_context(|| format!("restart cross-process BE[{index}]"))?;
        println!(
            "restarted cross-process BE[{index}] pid={} config={}",
            be_process.pid(),
            config_path.display()
        );
        let expected_ports = self.runtime.be.iter().map(|be| be.grpc).collect::<Vec<_>>();
        wait_for_live_backend_topology(
            LiveBackendTopologyWait {
                mysql_user: &self.mysql_user,
                runtime: &self.runtime,
                expected_ports: &expected_ports,
                fe_config_path: &self.fe_config_path,
                be_config_paths: &self.be_config_paths,
                timeout: remaining_until(deadline, "BE topology barrier")?,
            },
            &mut self.fe_process,
            &mut self.be_processes,
        )
        .context("cross-process backend topology barrier after BE restart")?;
        loop {
            let remaining = remaining_until(deadline, "BE process-identity barrier")?;
            let observed = query_frontend_backend_topology(
                &self.mysql_user,
                &self.target_host,
                self.target_port,
                topology_mysql_io_timeout(remaining),
            )
            .ok()
            .and_then(|rows| {
                rows.into_iter().find(|row| {
                    row.grpc_port == self.be_grpc_ports[index] && row.is_eligible_live()
                })
            });
            if observed
                .as_ref()
                .is_some_and(|row| row.alive && row.process_id != old_process_id.to_string())
            {
                println!(
                    "cross-process BE[{index}] process-identity barrier PASS: old_process_id={old_process_id} new_process_id={}",
                    observed.expect("observed row checked").process_id
                );
                break;
            }
            if Instant::now() >= deadline {
                let diagnostics = process_runtime_diagnostics(
                    &mut self.fe_process,
                    &mut self.be_processes,
                    &self.fe_config_path,
                    &self.be_config_paths,
                    &self.runtime,
                )?;
                bail!(
                    "timed out waiting for BE[{index}] process identity to change from {old_process_id}; observed={observed:?}; {diagnostics}"
                );
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100)),
            );
        }
        Ok(())
    }

    fn drain_be_until(&mut self, index: usize, deadline: Instant) -> Result<()> {
        self.ensure_be_index(index)?;
        self.be_processes[index]
            .stop()
            .with_context(|| format!("send SIGTERM to cross-process BE[{index}]"))?;
        let expected_eligible = self.be_processes.len().saturating_sub(1);
        loop {
            let rows = query_frontend_backend_topology(
                &self.mysql_user,
                &self.target_host,
                self.target_port,
                topology_mysql_io_timeout(remaining_until(deadline, "drain topology query")?),
            )?;
            if rows.iter().filter(|row| row.alive).count() == expected_eligible {
                println!(
                    "cross-process BE[{index}] drain barrier PASS: eligible_backends={expected_eligible}"
                );
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for BE[{index}] drain to reduce eligible backends to {expected_eligible}; rows={rows:?}"
                );
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100)),
            );
        }
    }

    fn kill_fe(&mut self) -> Result<()> {
        self.fe_process
            .kill_now()
            .context("kill cross-process FE")?;
        println!("killed cross-process FE");
        Ok(())
    }

    fn restart_fe(&mut self) -> Result<()> {
        self.restart_fe_until(Instant::now() + self.startup_timeout)
    }

    fn restart_fe_until(&mut self, deadline: Instant) -> Result<()> {
        let prior_log = self
            .fe_process
            .log_contents()
            .context("preserve cross-process FE log before restart")?;
        self.fe_log_history.push_str(&prior_log);
        let marker = "NOVAROCKS_READY mysql_port=";
        let mut command = build_novarocks_command(&self.novarocks_bin, "fe", &self.fe_config_path);
        if self.query_lifecycle_faults_enabled {
            command.env(
                novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV,
                self.query_lifecycle_fault_files.root(),
            );
        }
        if self.cleanup_faults_enabled {
            let files = self
                .cleanup_fault_files
                .as_ref()
                .expect("cleanup fault scope enabled");
            command.env(novarocks_failpoint::CLEANUP_FAULT_DIR_ENV, files.root());
        }
        apply_child_environment(&mut command, &self.fe_environment);
        self.fe_process
            .restart(
                command,
                ReadyMarker::StdoutContains(marker.to_string()),
                remaining_until(deadline, "FE readiness")?,
                self.runtime_dir.join("fe.log"),
            )
            .map_err(|error| map_novarocks_process_error(&self.novarocks_bin, "fe", marker, error))
            .context("restart cross-process FE")?;
        println!(
            "restarted cross-process FE pid={} config={}",
            self.fe_process.pid(),
            self.fe_config_path.display()
        );
        let expected_ports = self.runtime.be.iter().map(|be| be.grpc).collect::<Vec<_>>();
        wait_for_live_backend_topology(
            LiveBackendTopologyWait {
                mysql_user: &self.mysql_user,
                runtime: &self.runtime,
                expected_ports: &expected_ports,
                fe_config_path: &self.fe_config_path,
                be_config_paths: &self.be_config_paths,
                timeout: remaining_until(deadline, "FE topology barrier")?,
            },
            &mut self.fe_process,
            &mut self.be_processes,
        )
        .context("cross-process backend topology barrier after FE restart")?;
        Ok(())
    }

    fn kill_query(&mut self, connection_id: u32) -> Result<()> {
        self.kill_query_until(connection_id, Instant::now() + self.startup_timeout)
    }

    fn kill_query_until(&mut self, connection_id: u32, deadline: Instant) -> Result<()> {
        let io_timeout =
            topology_mysql_io_timeout(remaining_until(deadline, "KILL QUERY connect")?);
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some(self.target_host.clone()))
            .tcp_port(self.target_port)
            .prefer_socket(false)
            .user(Some(self.mysql_user.clone()))
            .tcp_connect_timeout(Some(io_timeout))
            .read_timeout(Some(io_timeout))
            .write_timeout(Some(io_timeout));
        let mut control = MysqlConn::new(builder).with_context(|| {
            format!(
                "connect KILL QUERY control session to {}:{}",
                self.target_host, self.target_port
            )
        })?;
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .with_context(|| format!("execute KILL QUERY {connection_id}"))?;
        println!("executed KILL QUERY {connection_id} through a separate control session");
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        Self::shutdown(self)
    }
}

impl Drop for CrossProcessServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn process_runtime_diagnostics(
    fe_process: &mut ManagedProcess,
    be_processes: &mut [ManagedProcess],
    fe_config_path: &Path,
    be_config_paths: &[PathBuf],
    runtime: &CrossProcessRuntime,
) -> Result<String> {
    if be_processes.len() != runtime.be.len() || be_config_paths.len() != runtime.be.len() {
        bail!(
            "cross-process diagnostic cardinality mismatch: processes={} configs={} endpoints={}",
            be_processes.len(),
            be_config_paths.len(),
            runtime.be.len()
        );
    }

    let mut diagnostics = Vec::with_capacity(be_processes.len() + 1);
    let mut exited = false;
    match fe_process.runtime_diagnostic(
        "FE",
        &format!("mysql://127.0.0.1:{}", runtime.fe_mysql_port),
        fe_config_path,
    ) {
        Ok(diagnostic) => diagnostics.push(diagnostic),
        Err(error) => {
            exited = true;
            diagnostics.push(format!("{error:#}"));
        }
    }
    for (index, ((process, config_path), ports)) in be_processes
        .iter_mut()
        .zip(be_config_paths.iter())
        .zip(runtime.be.iter())
        .enumerate()
    {
        match process.runtime_diagnostic(
            &format!("BE[{index}]"),
            &format!("grpc://127.0.0.1:{}", ports.grpc),
            config_path,
        ) {
            Ok(diagnostic) => diagnostics.push(diagnostic),
            Err(error) => {
                exited = true;
                diagnostics.push(format!("{error:#}"));
            }
        }
    }
    let diagnostics = diagnostics.join("; ");
    if exited {
        bail!("cross-process process exited: {diagnostics}");
    }
    Ok(diagnostics)
}

pub fn build_novarocks_command(binary: &Path, role: &str, config_path: &Path) -> Command {
    let mut command = Command::new(binary);
    command
        .arg("standalone")
        .arg("--role")
        .arg(role)
        .arg("--config")
        .arg(config_path)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD", "1")
        // Cross-process SQL fixtures own both process logs, so they enable the
        // bounded connector scan lifecycle markers used for structural
        // evidence. This is runner-owned and compiled out of release builds.
        .env("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

struct ProcessLaunch<'a> {
    binary: &'a Path,
    role: &'a str,
    config_path: &'a Path,
    marker: &'a str,
    startup_timeout: Duration,
    log_path: PathBuf,
    fragment_failure_trigger: Option<&'a Path>,
    query_lifecycle_fault_scope: Option<(&'a Path, Option<usize>)>,
    cleanup_fault_dir: Option<&'a Path>,
    child_environment: &'a BTreeMap<String, String>,
}

fn spawn_novarocks_process(launch: ProcessLaunch<'_>) -> Result<ManagedProcess> {
    let ProcessLaunch {
        binary,
        role,
        config_path,
        marker,
        startup_timeout,
        log_path,
        fragment_failure_trigger,
        query_lifecycle_fault_scope,
        cleanup_fault_dir,
        child_environment,
    } = launch;
    let mut command = build_novarocks_command(binary, role, config_path);
    if let Some(trigger_path) = fragment_failure_trigger {
        command.env(
            "NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE",
            trigger_path,
        );
    }
    if let Some((fault_dir, backend_index)) = query_lifecycle_fault_scope {
        command.env(
            novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV,
            fault_dir,
        );
        if let Some(backend_index) = backend_index {
            command.env(
                "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX",
                backend_index.to_string(),
            );
        }
    }
    if let Some(fault_dir) = cleanup_fault_dir {
        command.env(novarocks_failpoint::CLEANUP_FAULT_DIR_ENV, fault_dir);
    }
    apply_child_environment(&mut command, child_environment);
    let result = ManagedProcess::spawn(
        "novarocks".to_string(),
        command,
        ReadyMarker::StdoutContains(marker.to_string()),
        startup_timeout,
        log_path,
    );
    match result {
        Ok(process) => Ok(process),
        Err(error) => Err(map_novarocks_process_error(binary, role, marker, error)),
    }
}

fn apply_child_environment(command: &mut Command, environment: &BTreeMap<String, String>) {
    command.envs(environment);
}

fn merge_safe_config_overlay(
    root: &mut toml::map::Map<String, Value>,
    overlay: &str,
) -> Result<()> {
    let overlay = overlay
        .parse::<Value>()
        .context("parse cross-process config overlay")?;
    let overlay = overlay
        .as_table()
        .context("cross-process config overlay root must be a TOML table")?;
    for key in ["cluster", "state_store"] {
        if overlay.contains_key(key) {
            bail!("cross-process config overlay cannot modify [{key}]");
        }
    }
    if let Some(server) = overlay.get("server").and_then(Value::as_table) {
        for key in server.keys() {
            if !matches!(
                key.as_str(),
                "frontend_drain_timeout_ms" | "frontend_cleanup_timeout_ms"
            ) {
                bail!(
                    "cross-process config overlay cannot modify server.{key}; only frontend drain budgets are scenario-safe"
                );
            }
        }
    }
    if overlay
        .get("standalone_server")
        .and_then(Value::as_table)
        .is_some_and(|table| table.contains_key("mysql_port"))
    {
        bail!("cross-process config overlay cannot modify standalone_server.mysql_port");
    }
    merge_toml_table(root, overlay);
    Ok(())
}

fn merge_toml_table(
    target: &mut toml::map::Map<String, Value>,
    overlay: &toml::map::Map<String, Value>,
) {
    for (key, value) in overlay {
        match (target.get_mut(key), value) {
            (Some(Value::Table(target)), Value::Table(overlay)) => {
                merge_toml_table(target, overlay);
            }
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

fn resolve_be_environments(
    common: &BTreeMap<String, String>,
    overrides: &BTreeMap<usize, BTreeMap<String, String>>,
    cluster_size: usize,
) -> Result<Vec<BTreeMap<String, String>>> {
    for index in overrides.keys() {
        if *index >= cluster_size {
            bail!(
                "BE environment override index {index} is out of bounds for cross-process cluster with {cluster_size} BE(s)"
            );
        }
    }
    Ok((0..cluster_size)
        .map(|index| {
            let mut environment = common.clone();
            if let Some(index_overrides) = overrides.get(&index) {
                environment.extend(index_overrides.clone());
            }
            environment
        })
        .collect())
}

fn next_fragment_failure_token(index: usize) -> String {
    static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{index}-{nanos}-{sequence}", std::process::id())
}

fn fragment_failure_release_path(trigger_path: &Path) -> PathBuf {
    trigger_path.with_extension("release")
}

fn remove_fragment_failure_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn publish_fragment_failure_token(trigger_path: &Path, token: &str) -> Result<()> {
    let staging_path = trigger_path.with_extension(format!("arming-{token}"));
    let mut staging = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .with_context(|| {
            format!(
                "create fragment executor failure staging file {}",
                staging_path.display()
            )
        })?;
    if let Err(error) = staging.write_all(token.as_bytes()) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "write fragment executor failure token to staging file {}",
                staging_path.display()
            )
        });
    }
    drop(staging);

    if let Err(error) = fs::hard_link(&staging_path, trigger_path) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "publish fragment executor failure trigger {}",
                trigger_path.display()
            )
        });
    }
    let _ = fs::remove_file(staging_path);
    Ok(())
}

fn publish_query_lifecycle_fault_token(
    trigger_path: &Path,
    token: &str,
    contents: &[u8],
) -> Result<()> {
    let staging_path = trigger_path.with_extension(format!("arming-{token}"));
    let mut staging = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .with_context(|| {
            format!(
                "create query lifecycle fault staging file {}",
                staging_path.display()
            )
        })?;
    if let Err(error) = staging.write_all(contents) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "write query lifecycle fault token to staging file {}",
                staging_path.display()
            )
        });
    }
    drop(staging);
    if let Err(error) = fs::hard_link(&staging_path, trigger_path) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "publish query lifecycle fault trigger {}",
                trigger_path.display()
            )
        });
    }
    let _ = fs::remove_file(staging_path);
    Ok(())
}

fn map_novarocks_process_error(
    binary: &Path,
    role: &str,
    marker: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    if format!("{error:#}").starts_with("spawn novarocks;") {
        return error.context(format!("spawn novarocks {role} from {}", binary.display()));
    }
    managed_novarocks_startup_error(marker, error)
}

fn managed_novarocks_startup_error(marker: &str, error: anyhow::Error) -> anyhow::Error {
    let message = format!("{error:#}");
    anyhow::anyhow!(format_startup_failure(marker, &message, &message))
}

pub fn startup_timeout_from_env(raw: Option<&str>) -> Duration {
    let timeout_secs = raw
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(120);
    bounded_backend_topology_timeout(Duration::from_secs(timeout_secs))
}

struct ReservedBePorts {
    http: ReservedTcpPort,
    grpc: ReservedTcpPort,
}

struct ReservedRuntimePorts {
    be_ports: Vec<ReservedBePorts>,
    fe_http_port: ReservedTcpPort,
    fe_grpc_port: ReservedTcpPort,
    fe_mysql_port: ReservedTcpPort,
}

impl ReservedRuntimePorts {
    fn new(cluster_size: usize) -> Result<Self> {
        assert!(cluster_size >= 1, "cluster_size must be >= 1");
        let mut be_ports = Vec::with_capacity(cluster_size);
        for _ in 0..cluster_size {
            be_ports.push(ReservedBePorts {
                http: ReservedTcpPort::new()?,
                grpc: ReservedTcpPort::new()?,
            });
        }
        Ok(Self {
            be_ports,
            fe_http_port: ReservedTcpPort::new()?,
            fe_grpc_port: ReservedTcpPort::new()?,
            fe_mysql_port: ReservedTcpPort::new()?,
        })
    }
}

fn format_startup_failure(marker: &str, message: &str, stderr: &str) -> String {
    if is_bind_conflict(stderr) {
        format!(
            "{message}; probable port bind conflict while starting cross-process mode. Retry the run or inspect processes already using the reserved ports (readiness marker `{marker}`)."
        )
    } else {
        format!("{message} (readiness marker `{marker}`)")
    }
}

fn is_bind_conflict(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("address already in use")
        || stderr.contains("addrinuse")
        || stderr.contains("eaddrinuse")
        || stderr.contains("os error 48")
        || (stderr.contains("bind") && stderr.contains("in use"))
}

/// Destroy the FE durable SQLite StateStore of one launch runtime directory and
/// return the files that were actually removed.
///
/// Every candidate path is `runtime_dir` joined with a literal file name, so
/// this can never reach outside the launch it was handed. A file that is
/// already absent is already destroyed and is not an error.
fn remove_fe_state_store(runtime_dir: &Path, fe_config_path: &Path) -> Result<Vec<PathBuf>> {
    // A wipe that destroys nothing is indistinguishable from a wipe that
    // worked, so a caller asserting "the FE lost every durable record" would
    // pass while the records were still there. Only the SQLite provider keeps
    // its store in this directory; refuse rather than quietly no-op if the
    // launched FE owns its durable state somewhere this function cannot reach.
    let rendered = fs::read_to_string(fe_config_path)
        .with_context(|| format!("read {}", fe_config_path.display()))?;
    let parsed = rendered
        .parse::<Value>()
        .with_context(|| format!("parse {}", fe_config_path.display()))?;
    let provider = parsed
        .get("state_store")
        .and_then(|section| section.get("provider"))
        .and_then(Value::as_str);
    if provider != Some("sqlite") {
        bail!(
            "cannot destroy the FE durable state store: {} declares [state_store] provider {:?}, \
             but only \"sqlite\" keeps its store under the launch runtime directory",
            fe_config_path.display(),
            provider
        );
    }
    let mut removed = Vec::new();
    for suffix in FE_STATE_STORE_FILE_SUFFIXES {
        let path = runtime_dir.join(format!("{FE_STATE_STORE_FILE_NAME}{suffix}"));
        match fs::remove_file(&path) {
            Ok(()) => removed.push(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("remove FE durable state store file {}", path.display())
                });
            }
        }
    }
    Ok(removed)
}

fn create_runtime_dir(runtime_root: &Path) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = runtime_root.join(format!("{}_{}", std::process::id(), nanos));
    fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
    Ok(path)
}

fn table_mut<'a>(
    table: &'a mut toml::map::Map<String, Value>,
    key: &str,
) -> &'a mut toml::map::Map<String, Value> {
    if !matches!(table.get(key), Some(Value::Table(_))) {
        table.insert(key.to_string(), Value::Table(Default::default()));
    }
    table
        .get_mut(key)
        .and_then(Value::as_table_mut)
        .expect("table inserted")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fs;

    fn lifecycle_debug_json(execution_id: &str) -> serde_json::Value {
        let mut value = serde_json::json!({
            "execution_id": execution_id,
            "error_source": null,
            "participant_outcomes": [],
            "telemetry_unavailable": [],
            "runtime_filter": {
                "kind": "available",
                "participants": [{
                    "participant": { "process_id": "018f8bcb-0000-7000-8000-000000000001" },
                    "telemetry": {
                        "kind": "available",
                        "channels": [{
                            "channel_binding_id": 1,
                            "channel_id": 2,
                            "install_state": "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_INSTALL_STATE_V1_INSTALLED",
                            "terminal_state": "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_TERMINAL_STATE_V1_COMPLETED",
                            "latest_published_logical_version": 3,
                            "published_count": 4,
                            "completed_count": 1,
                            "unavailable_count": 0,
                            "cancelled_count": 0
                        }],
                        "producer_streams": [{
                            "channel_binding_id": 1,
                            "channel_id": 2,
                            "producer_fragment_instance_id": { "high": 9, "low": 10 },
                            "partition_id": 3,
                            "latest_accepted_sequence": 0,
                            "accepted_count": 1,
                            "duplicate_count": 2,
                            "stale_count": 0,
                            "conflict_count": 0,
                            "resource_limit_count": 0
                        }],
                        "transport_routes": [{
                            "channel_binding_id": 1,
                            "channel_id": 2,
                            "route_edge_id": 13,
                            "sent_count": 1,
                            "sent_bytes": 17,
                            "retried_count": 1,
                            "retried_bytes": 17,
                            "acked_count": 1,
                            "acked_bytes": 17,
                            "fail_open_count": 0,
                            "fail_open_bytes": 0
                        }],
                        "consumers": [{
                            "channel_binding_id": 1,
                            "channel_id": 2,
                            "consumer_binding_id": 5,
                            "fragment_instance_id": { "high": 19, "low": 20 },
                            "latest_delivered_logical_version": 3,
                            "latest_applied_logical_version": 3,
                            "subscription_terminal": "QUERY_TERMINAL_RUNTIME_FILTER_SUBSCRIPTION_TERMINAL_V1_COMPLETED",
                            "row_evaluations": 100,
                            "input_rows": 100,
                            "output_rows": 20,
                            "scan_evaluated": 10,
                            "scan_kept": 4,
                            "scan_pruned": 6,
                            "scan_not_evaluated": 0,
                            "scan_not_evaluated_reasons": {
                                "unit_facts_missing": 0,
                                "column_facts_missing": 0,
                                "data_type_unsupported": 0,
                                "predicate_capability_unsupported": 0,
                                "resource_unavailable": 0,
                                "snapshot_unavailable": 0,
                                "snapshot_timed_out": 0,
                                "snapshot_not_published": 0
                            }
                        }]
                    }
                }],
                "totals": {
                    "kind": "available",
                    "channels": { "count": 1, "published_count": 4, "completed_count": 1, "unavailable_count": 0, "cancelled_count": 0 },
                    "producer_streams": { "count": 1, "accepted_count": 1, "duplicate_count": 2, "stale_count": 0, "conflict_count": 0, "resource_limit_count": 0 },
                    "transport_routes": { "count": 1, "sent_count": 1, "sent_bytes": 17, "retried_count": 1, "retried_bytes": 17, "acked_count": 1, "acked_bytes": 17, "fail_open_count": 0, "fail_open_bytes": 0 },
                    "consumers": {
                        "count": 1, "row_evaluations": 100, "input_rows": 100, "output_rows": 20, "scan_evaluated": 10, "scan_kept": 4, "scan_pruned": 6, "scan_not_evaluated": 0,
                        "scan_not_evaluated_reasons": {
                            "unit_facts_missing": 0, "column_facts_missing": 0, "data_type_unsupported": 0, "predicate_capability_unsupported": 0,
                            "resource_unavailable": 0, "snapshot_unavailable": 0, "snapshot_timed_out": 0, "snapshot_not_published": 0
                        }
                    }
                }
            },
            "metrics": {}
        });
        value["query_process_namespace"] = serde_json::json!("0x000000000000000b");
        value["query_local_sequence"] = serde_json::json!(12);
        value["query_attempt_id"] = serde_json::json!(13);
        value
    }

    fn decode_lifecycle_debug_json(value: serde_json::Value) -> QueryLifecycleStructuredSnapshot {
        let wire = serde_json::from_value(value).expect("decode lifecycle debug wire");
        decode_query_lifecycle_structured_snapshot(wire)
            .expect("decode lifecycle debug snapshot")
            .expect("debug endpoint returns a snapshot")
    }

    #[test]
    fn lifecycle_debug_decode_preserves_prefixed_runtime_filter_details_and_totals() {
        let snapshot = decode_lifecycle_debug_json(lifecycle_debug_json("11:12:13"));
        let RuntimeFilterTerminalRollup::Available {
            participants,
            totals,
        } = snapshot.runtime_filter
        else {
            panic!("runtime-filter fixture must be available");
        };
        assert_eq!(snapshot.execution_id.as_deref(), Some("11:12:13"));
        assert_eq!(snapshot.process_namespace, 11);
        assert_eq!(snapshot.local_sequence, 12);
        assert_eq!(snapshot.attempt_id, 13);
        assert_eq!(participants.len(), 1);
        assert_eq!(
            participants[0].participant.process_id,
            "018f8bcb-0000-7000-8000-000000000001"
        );
        let RuntimeFilterParticipantTerminalTelemetryValue::Available(details) =
            &participants[0].telemetry
        else {
            panic!("participant telemetry must be available");
        };
        assert_eq!(
            details.channels[0].terminal_state,
            RuntimeFilterChannelTerminalState::Completed
        );
        assert_eq!(
            details.producer_streams[0].latest_accepted_sequence,
            Some(0)
        );
        assert_eq!(details.transport_routes[0].retried_count, 1);
        assert_eq!(
            details.consumers[0].subscription_terminal,
            RuntimeFilterSubscriptionTerminal::Completed
        );
        let RuntimeFilterTerminalTotalsTelemetry::Available(totals) = totals else {
            panic!("fixture totals must be available");
        };
        assert_eq!(totals.channels.completed_count, 1);
        assert_eq!(totals.producer_streams.duplicate_count, 2);
        assert_eq!(totals.transport_routes.acked_count, 1);
        assert_eq!(totals.consumers.scan_pruned, 6);
    }

    #[test]
    fn lifecycle_debug_decode_preserves_explicit_runtime_filter_unavailable_categories() {
        let mut value = lifecycle_debug_json("11:12:13");
        value["runtime_filter"] = serde_json::json!({
            "kind": "unavailable",
            "reason": "negative-attestation"
        });
        let snapshot = decode_lifecycle_debug_json(value);
        assert_eq!(
            snapshot.runtime_filter,
            RuntimeFilterTerminalRollup::Unavailable {
                reason: RuntimeFilterTerminalRollupUnavailable::NegativeAttestation
            }
        );

        let mut value = lifecycle_debug_json("11:12:13");
        value["runtime_filter"]["participants"][0]["telemetry"] = serde_json::json!({
            "kind": "unavailable",
            "stage": "terminal_capture",
            "code": "BUDGET_EXHAUSTED"
        });
        value["runtime_filter"]["totals"] = serde_json::json!({
            "kind": "unavailable",
            "reason": "participant-telemetry-unavailable"
        });
        let snapshot = decode_lifecycle_debug_json(value);
        let RuntimeFilterTerminalRollup::Available {
            participants,
            totals,
        } = snapshot.runtime_filter
        else {
            panic!("rollup remains available when participant truth is retained");
        };
        assert!(matches!(
            participants[0].telemetry,
            RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(RuntimeFilterTerminalUnavailable { ref stage, ref code })
                if stage == "terminal_capture" && code == "BUDGET_EXHAUSTED"
        ));
        assert_eq!(
            totals,
            RuntimeFilterTerminalTotalsTelemetry::Unavailable(
                RuntimeFilterTerminalTotalsUnavailable::ParticipantTelemetryUnavailable
            )
        );
    }

    #[test]
    fn lifecycle_debug_decode_rejects_unknown_runtime_filter_categories() {
        let mut unknown_rollup = lifecycle_debug_json("11:12:13");
        unknown_rollup["runtime_filter"]["kind"] = serde_json::json!("future-category");
        let wire = match serde_json::from_value::<LifecycleConvergenceWireSnapshot>(unknown_rollup)
        {
            Ok(_) => panic!("unknown tagged rollup category must fail wire decode"),
            Err(error) => error,
        };
        assert!(wire.to_string().contains("future-category"), "{wire}");

        let mut unknown_state = lifecycle_debug_json("11:12:13");
        unknown_state["runtime_filter"]["participants"][0]["telemetry"]["channels"][0]["terminal_state"] =
            serde_json::json!("future-terminal-state");
        let wire = serde_json::from_value(unknown_state).expect("known wire envelope");
        let error = decode_query_lifecycle_structured_snapshot(wire)
            .expect_err("unknown terminal state must fail typed decoding");
        assert!(
            format!("{error:#}").contains("future-terminal-state"),
            "{error:#}"
        );
    }

    #[test]
    fn lifecycle_debug_decode_rejects_missing_or_invalid_process_attribution() {
        let mut missing_namespace = lifecycle_debug_json("11:12:13");
        missing_namespace
            .as_object_mut()
            .expect("JSON object")
            .remove("query_process_namespace");
        assert!(
            serde_json::from_value::<LifecycleConvergenceWireSnapshot>(missing_namespace).is_err()
        );

        let mut invalid_namespace = lifecycle_debug_json("11:12:13");
        invalid_namespace["query_process_namespace"] = serde_json::json!("namespace=11");
        let wire = serde_json::from_value(invalid_namespace).expect("wire envelope");
        let error = decode_query_lifecycle_structured_snapshot(wire)
            .expect_err("non-hex namespace must fail closed");
        assert!(format!("{error:#}").contains("query_process_namespace"));

        let mut zero_sequence = lifecycle_debug_json("11:12:13");
        zero_sequence["query_local_sequence"] = serde_json::json!(0);
        let wire = serde_json::from_value(zero_sequence).expect("wire envelope");
        let error = decode_query_lifecycle_structured_snapshot(wire)
            .expect_err("zero local sequence must fail closed");
        assert!(format!("{error:#}").contains("query_local_sequence"));
    }

    #[test]
    fn lifecycle_snapshot_wait_rejects_old_identity_until_a_new_query_arrives() {
        let mut snapshots = VecDeque::from([
            Ok(Some(decode_lifecycle_debug_json(lifecycle_debug_json(
                "old",
            )))),
            Ok(None),
            Ok(Some(decode_lifecycle_debug_json(lifecycle_debug_json(
                "new",
            )))),
        ]);
        let snapshot = await_query_lifecycle_structured_snapshot_after(
            Some("old"),
            Instant::now() + Duration::from_secs(1),
            || snapshots.pop_front().expect("fixture snapshot"),
        )
        .expect("new execution identity must be returned");
        assert_eq!(snapshot.execution_id.as_deref(), Some("new"));
    }

    #[test]
    fn lifecycle_snapshot_wait_timeout_names_the_latest_stale_identity() {
        let snapshot = decode_lifecycle_debug_json(lifecycle_debug_json("old"));
        let error =
            await_query_lifecycle_structured_snapshot_after(Some("old"), Instant::now(), || {
                Ok(Some(snapshot.clone()))
            })
            .expect_err("the pre-query execution identity must not satisfy the wait");
        assert!(format!("{error:#}").contains("latest_execution_id=Some(\"old\")"));
    }

    fn backend_row(grpc_port: u16, state: &str, alive: bool) -> BackendTopologyRow {
        BackendTopologyRow {
            process_id: format!("01900000-0000-7000-8000-{grpc_port:012x}"),
            grpc_port,
            state: state.to_string(),
            alive,
            scheduled_fragments: 0,
            build_identity: "test-build-identity".to_string(),
            status_detail: String::new(),
        }
    }

    #[test]
    fn frontend_show_backends_parses_membership_diagnostics() {
        let row = parse_frontend_show_backends_values(&[
            "01900000-0000-7000-8000-000000000001".to_string(),
            "127.0.0.1:19070".to_string(),
            "true".to_string(),
            "true".to_string(),
            "Running".to_string(),
            "true".to_string(),
            "true".to_string(),
            "true".to_string(),
            "41".to_string(),
            "1000".to_string(),
            "1001".to_string(),
            "test-build-identity".to_string(),
            "Live".to_string(),
            "".to_string(),
        ])
        .expect("parse frontend SHOW BACKENDS row");

        assert_eq!(row.process_id, "01900000-0000-7000-8000-000000000001");
        assert_eq!(row.grpc_port, 19070);
        assert_eq!(row.state, "Live");
        assert!(row.alive);
        assert_eq!(row.scheduled_fragments, 41);
        assert_eq!(row.build_identity, "test-build-identity");
        assert!(row.status_detail.is_empty());
    }

    #[test]
    fn live_backend_topology_requires_exact_configured_count_and_all_live() {
        let expected = [19070, 19071];
        let ready = vec![
            backend_row(19070, "Live", true),
            backend_row(19071, "Live", true),
        ];
        validate_live_backend_topology(&expected, &ready).expect("2/2 Live should pass");

        let extra = vec![
            backend_row(19070, "Live", true),
            backend_row(19071, "Live", true),
            backend_row(19072, "Live", true),
        ];
        let err = validate_live_backend_topology(&expected, &extra)
            .expect_err("an extra registered backend must fail the exact topology");
        assert!(err.to_string().contains("registered=3 expected=2"), "{err}");

        let registering = vec![
            backend_row(19070, "Live", true),
            backend_row(19071, "Registering", false),
        ];
        let err = validate_live_backend_topology(&expected, &registering)
            .expect_err("a non-Live configured backend must fail readiness");
        assert!(err.to_string().contains("live=1 expected=2"), "{err}");
        assert!(err.to_string().contains("19071:Registering:false"), "{err}");

        let stale_replacement = vec![
            backend_row(19070, "Live", true),
            backend_row(19072, "Live", true),
        ];
        let err = validate_live_backend_topology(&expected, &stale_replacement)
            .expect_err("a stale Live backend must not replace a configured endpoint");
        assert!(
            err.to_string()
                .contains("configured_ports=[19070, 19071] observed_ports=[19070, 19072]"),
            "{err}"
        );

        let identity_split = vec![
            backend_row(19070, "Live", true),
            BackendTopologyRow {
                build_identity: "other-build-identity".to_string(),
                ..backend_row(19071, "Live", true)
            },
        ];
        let err = validate_live_backend_topology(&expected, &identity_split)
            .expect_err("mixed Native build identities must fail the barrier");
        assert!(
            err.to_string()
                .contains("identities={\"other-build-identity\", \"test-build-identity\"}"),
            "{err}"
        );

        let empty_identity = vec![
            backend_row(19070, "Live", true),
            BackendTopologyRow {
                build_identity: String::new(),
                ..backend_row(19071, "Live", true)
            },
        ];
        assert!(validate_live_backend_topology(&expected, &empty_identity).is_err());

        let detail_on_live = vec![
            backend_row(19070, "Live", true),
            BackendTopologyRow {
                status_detail: "unexpected admission failure".to_string(),
                ..backend_row(19071, "Live", true)
            },
        ];
        assert!(validate_live_backend_topology(&expected, &detail_on_live).is_err());

        let incompatible = vec![
            backend_row(19070, "Live", true),
            BackendTopologyRow {
                state: "Incompatible".to_string(),
                alive: false,
                status_detail: "native build identity mismatch".to_string(),
                ..backend_row(19071, "Live", true)
            },
        ];
        assert!(validate_live_backend_topology(&expected, &incompatible).is_err());

        let retained_replacement = vec![
            backend_row(19070, "Live", true),
            backend_row(19071, "Live", true),
            BackendTopologyRow {
                process_id: "01900000-0000-7000-8000-000000000099".to_string(),
                grpc_port: 19071,
                state: "Stale|Lost|Replaced".to_string(),
                alive: false,
                scheduled_fragments: 0,
                build_identity: "test-build-identity".to_string(),
                status_detail: "heartbeat expected backend process id does not match this backend"
                    .to_string(),
            },
        ];
        validate_live_backend_topology(&expected, &retained_replacement)
            .expect("a retained replaced process must not alter the live topology");
        let selected = retained_replacement
            .iter()
            .find(|row| row.grpc_port == 19071 && row.is_eligible_live())
            .expect("the replacement endpoint must select its current eligible process");
        assert_eq!(selected.process_id, "01900000-0000-7000-8000-000000004a7f");
    }

    #[test]
    fn empty_backend_topology_is_ready_only_when_show_backends_is_empty() {
        validate_live_backend_topology(&[], &[])
            .expect("an empty self-registration registry should be ready before any BE announces");

        let unexpected = vec![backend_row(19070, "Live", true)];
        let error = validate_live_backend_topology(&[], &unexpected)
            .expect_err("an empty topology expectation must reject an announced backend row");
        assert!(
            error.to_string().contains("registered=1 expected=0"),
            "{error}"
        );
    }

    #[test]
    fn backend_topology_barrier_retries_until_general_n_is_live() {
        let mut attempts = 0;
        let mut io_timeouts = Vec::new();
        let snapshot = wait_for_live_backend_topology_with(
            &[19070, 19071],
            Duration::from_secs(1),
            || Ok("fe=running be=[running,running]".to_string()),
            |io_timeout| {
                io_timeouts.push(io_timeout);
                attempts += 1;
                if attempts == 1 {
                    Ok(vec![
                        backend_row(19070, "Live", true),
                        backend_row(19071, "Registering", false),
                    ])
                } else {
                    Ok(vec![
                        backend_row(19070, "Live", true),
                        backend_row(19071, "Live", true),
                    ])
                }
            },
            |_| {},
        )
        .expect("barrier should retry until 2/2 Live");

        assert_eq!(attempts, 2);
        assert_eq!(snapshot.len(), 2);
        assert!(
            io_timeouts
                .iter()
                .all(|timeout| *timeout > Duration::ZERO && *timeout <= Duration::from_secs(2)),
            "unexpected per-attempt MySQL timeouts: {io_timeouts:?}"
        );
    }

    #[test]
    fn backend_topology_barrier_timeout_includes_pid_and_endpoint_diagnostics() {
        let err = wait_for_live_backend_topology_with(
            &[19070, 19071, 19072],
            Duration::ZERO,
            || Ok("fe_pid=11 be_pids=[21,22,23] fe_mysql=127.0.0.1:29030 be_grpc=[127.0.0.1:19070,127.0.0.1:19071,127.0.0.1:19072]".to_string()),
            |_| Ok(vec![backend_row(19070, "Live", true)]),
            |_| {},
        )
        .expect_err("incomplete topology must time out");

        let message = format!("{err:#}");
        assert!(
            message.contains("timed out waiting for SHOW BACKENDS 3/3 Live"),
            "{message}"
        );
        assert!(message.contains("registered=1 expected=3"), "{message}");
        assert!(message.contains("fe_pid=11"), "{message}");
        assert!(message.contains("be_pids=[21,22,23]"), "{message}");
        assert!(message.contains("fe_mysql=127.0.0.1:29030"), "{message}");
        assert!(message.contains("be_grpc=[127.0.0.1:19070"), "{message}");
    }

    #[test]
    fn backend_topology_barrier_fails_before_query_when_a_process_exits() {
        let mut queries = 0;
        let err = wait_for_live_backend_topology_with(
            &[19070],
            Duration::from_secs(30),
            || {
                bail!(
                    "FE exited status=exit status: 9 pid=11 endpoint=mysql://127.0.0.1:29030 config=/tmp/fe.toml stdout_tail=ready stderr_tail=fatal"
                )
            },
            |_| {
                queries += 1;
                Ok(vec![backend_row(19070, "Live", true)])
            },
            |_| {},
        )
        .expect_err("a dead FE must fail without waiting for the topology timeout");

        assert_eq!(queries, 0, "SHOW BACKENDS must not run after process exit");
        let message = format!("{err:#}");
        assert!(
            message.contains("FE exited status=exit status: 9"),
            "{message}"
        );
        assert!(message.contains("config=/tmp/fe.toml"), "{message}");
        assert!(message.contains("stderr_tail=fatal"), "{message}");
    }

    #[test]
    fn backend_topology_timeout_refreshes_process_health_after_query() {
        let mut health_checks = 0;
        let err = wait_for_live_backend_topology_with(
            &[19070],
            Duration::ZERO,
            || {
                health_checks += 1;
                if health_checks == 1 {
                    Ok("FE=running before query".to_string())
                } else {
                    bail!(
                        "FE exited post-query status=exit status: 7 pid=11 config=/tmp/fe.toml stderr_tail=post-query-fatal"
                    )
                }
            },
            |_| Ok(vec![backend_row(19070, "Registering", false)]),
            |_| {},
        )
        .expect_err("timeout must refresh process health after the bounded query");

        assert_eq!(
            health_checks, 2,
            "health must be sampled before and after query"
        );
        let message = format!("{err:#}");
        assert!(
            message.contains("FE exited post-query status=exit status: 7"),
            "{message}"
        );
        assert!(message.contains("post-query-fatal"), "{message}");
    }

    #[test]
    fn topology_timeouts_are_bounded_and_deadline_addition_cannot_panic() {
        assert_eq!(
            bounded_backend_topology_timeout(Duration::MAX),
            Duration::from_secs(120)
        );
        assert_eq!(
            topology_mysql_io_timeout(Duration::from_secs(30)),
            Duration::from_secs(2)
        );
        assert_eq!(
            topology_mysql_io_timeout(Duration::from_millis(250)),
            Duration::from_millis(250)
        );
        assert_eq!(
            topology_mysql_io_timeout(Duration::ZERO),
            Duration::from_millis(1)
        );
        let now = Instant::now();
        let deadline = backend_topology_deadline(now, Duration::MAX);
        assert!(deadline >= now);
        assert!(deadline.duration_since(now) <= Duration::from_secs(120));
    }

    #[test]
    fn query_lifecycle_fault_files_publish_isolated_tokens_and_clean_up_on_drop() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-query-lifecycle-fault-test-{}",
            next_fragment_failure_token(99)
        ));
        fs::create_dir_all(&root).expect("create temp root");
        let trigger_dir = root.join("query-lifecycle-faults");
        let paths = QueryLifecycleFaultFiles::new(&trigger_dir, 3)
            .expect("create query lifecycle fault paths");
        let init_token = paths
            .publish_init_ack_drop(1)
            .expect("publish init ack token");
        let heartbeat_token = paths
            .publish_heartbeat_stop(2)
            .expect("publish heartbeat stop token");
        let stage_ack_token = paths
            .publish_stage_ack_drop(0)
            .expect("publish stage ack token");
        let start_ack_token = paths
            .publish_start_ack_suppress(1)
            .expect("publish start ack token");
        let stage_prepare_token = paths
            .publish_stage_prepare_failure(2)
            .expect("publish stage prepare failure");
        let phase_token = paths
            .publish_kill_query_at_phase(QueryLifecyclePhase::Starting)
            .expect("publish phase fault");
        let hold_token = paths
            .publish_hold_start_until_early_ingress()
            .expect("publish early ingress hold");

        assert_ne!(init_token, heartbeat_token);
        assert_eq!(
            fs::read_to_string(paths.init_ack_drop_path(1).expect("init path"))
                .expect("read init token"),
            format!("token={init_token}\nbackend_index=1\n")
        );
        assert_eq!(
            fs::read_to_string(paths.heartbeat_stop_path(2).expect("heartbeat path"))
                .expect("read heartbeat token"),
            format!("token={heartbeat_token}\nbackend_index=2\n")
        );
        assert!(!paths.init_ack_drop_path(0).expect("init path 0").exists());
        assert!(
            !paths
                .heartbeat_stop_path(1)
                .expect("heartbeat path 1")
                .exists()
        );
        assert_eq!(
            fs::read_to_string(paths.stage_ack_drop_path(0).expect("stage ack path"))
                .expect("read stage ack token"),
            format!("token={stage_ack_token}\nbackend_index=0\n")
        );
        assert_eq!(
            fs::read_to_string(paths.start_ack_suppress_path(1).expect("start ack path"))
                .expect("read start ack token"),
            format!("token={start_ack_token}\nbackend_index=1\n")
        );
        assert_eq!(
            fs::read_to_string(paths.stage_prepare_failure_path())
                .expect("read stage prepare token"),
            format!("token={stage_prepare_token}\nordinal=2\n")
        );
        assert_eq!(
            fs::read_to_string(paths.kill_query_at_phase_path(QueryLifecyclePhase::Starting))
                .expect("read phase token"),
            format!("token={phase_token}\nphase=starting\n")
        );
        assert_eq!(
            fs::read_to_string(paths.hold_start_until_early_ingress_path())
                .expect("read hold token"),
            format!("token={hold_token}\nenabled=true\n")
        );

        let duplicate = paths
            .publish_init_ack_drop(1)
            .expect_err("an armed trigger must not be clobbered");
        assert!(
            format!("{duplicate:#}").contains("publish query lifecycle fault trigger"),
            "unexpected duplicate error: {duplicate:#}"
        );

        drop(paths);
        assert!(
            !trigger_dir.exists(),
            "dropping the runner-owned fault scope must remove every trigger"
        );
        fs::remove_dir(&root).expect("remove empty temp root");
    }

    #[test]
    fn rfo_8r2_fault_arm_uses_the_same_tokenized_scope_and_rejects_unknown_kinds() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-rfo-8r2-fault-test-{}",
            next_fragment_failure_token(99)
        ));
        fs::create_dir_all(&root).expect("create temp root");
        let trigger_dir = root.join("query-lifecycle-faults");
        let paths = QueryLifecycleFaultFiles::new(&trigger_dir, 3).expect("create fault paths");
        let token = paths
            .publish_rfo_8r2_fault(2, "terminal-outcome-suppress")
            .expect("publish RFO-8R2 arm");
        assert_eq!(
            fs::read_to_string(
                paths
                    .rfo_8r2_fault_path(2, "terminal-outcome-suppress")
                    .expect("fault path"),
            )
            .expect("read fault arm"),
            format!("token={token}\nbackend_index=2\n")
        );
        let error = paths
            .publish_rfo_8r2_fault(0, "not-a-rfo-8r2-fault")
            .expect_err("unknown fault kind must not publish a file");
        assert!(
            format!("{error:#}").contains("unsupported RFO-8R2 query lifecycle fault kind"),
            "unexpected error: {error:#}"
        );
        drop(paths);
        fs::remove_dir(&root).expect("remove empty temp root");
    }

    fn make_runtime_1be() -> CrossProcessRuntime {
        CrossProcessRuntime {
            be: vec![BePorts {
                http: 18080,
                grpc: 19070,
            }],
            fe_http_port: 28080,
            fe_grpc_port: 29070,
            fe_mysql_port: 29030,
        }
    }

    fn make_runtime_2be() -> CrossProcessRuntime {
        CrossProcessRuntime {
            be: vec![
                BePorts {
                    http: 18080,
                    grpc: 19070,
                },
                BePorts {
                    http: 18081,
                    grpc: 19071,
                },
            ],
            fe_http_port: 28080,
            fe_grpc_port: 29070,
            fe_mysql_port: 29030,
        }
    }

    fn rendered_native_trust_fixture() -> PreparedNativeTrustFixture {
        PreparedNativeTrustFixture {
            fixture: NativeTrustFixture::plaintext_ip(),
            shared_secret: "test-only-fixture-secret".to_string(),
            pem_paths: NativeTrustPemPaths {
                certificate_chain: PathBuf::from("/tmp/novarocks-test-leaf.pem"),
                private_key: PathBuf::from("/tmp/novarocks-test-leaf-key.pem"),
                trust_roots: PathBuf::from("/tmp/novarocks-test-roots.pem"),
            },
        }
    }

    static BASE_CONFIG: &str = r#"
[state_store]
provider = "sqlite"
cluster_id = "novarocks-sql-test-cross-process"
path = "tmp/novarocks-sql-test-state-store.sqlite"

[catalog_source]
mode = "dynamic-state-store"

[standalone_server]
mysql_port = 9030
user = "root"

[connector.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
enable_path_style_access = true
"#;

    #[test]
    fn render_cross_process_config_patches_fe_and_be_independently() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(
            fe_value["state_store"]["path"].as_str(),
            Some("tmp/novarocks-sql-test-state-store.sqlite")
        );
        assert!(
            fe_value.get("metadata").is_none(),
            "FE rendering must not create a legacy metadata store"
        );
        assert_eq!(
            fe_value["connector"]["object_store"]["endpoint"].as_str(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(fe_value["server"]["host"].as_str(), Some("127.0.0.1"));
        assert_eq!(fe_value["server"]["http_port"].as_integer(), Some(28080));
        assert_eq!(fe_value["server"]["grpc_port"].as_integer(), Some(29070));
        assert_eq!(
            fe_value["standalone_server"]["mysql_port"].as_integer(),
            Some(29030)
        );
        assert_eq!(fe_value["standalone_server"]["user"].as_str(), Some("root"));
        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert_eq!(
            fe_value["catalog_source"]["mode"].as_str(),
            Some("dynamic-state-store")
        );
        assert_eq!(
            fe_value["cluster"]["heartbeat_interval_ms"].as_integer(),
            Some(500)
        );
        assert_eq!(
            fe_value["cluster"]["heartbeat_timeout_retries"].as_integer(),
            Some(2)
        );
        assert!(fe_value["cluster"].get("backends").is_none());

        assert!(
            be_value.get("state_store").is_none(),
            "BE rendering must not own frontend StateStore"
        );
        assert!(
            be_value.get("catalog_source").is_none(),
            "BE rendering must not retain the FE catalog source"
        );
        assert!(
            be_value.get("metadata").is_none(),
            "BE rendering must not create a legacy metadata store"
        );
        assert_eq!(
            be_value["connector"]["object_store"]["endpoint"].as_str(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(be_value["server"]["host"].as_str(), Some("127.0.0.1"));
        assert_eq!(be_value["server"]["http_port"].as_integer(), Some(18080));
        assert_eq!(be_value["server"]["grpc_port"].as_integer(), Some(19070));
        assert_eq!(be_value["standalone_server"]["user"].as_str(), Some("root"));
        assert!(
            be_value
                .get("standalone_server")
                .and_then(|value| value.get("mysql_port"))
                .is_none()
        );
        assert_eq!(be_value["cluster"]["role"].as_str(), Some("be"));
        assert_eq!(
            be_value["cluster"]["frontend_endpoint"].as_str(),
            Some("127.0.0.1:29070")
        );
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("backends"))
                .is_none()
        );
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_interval_ms"))
                .is_none()
        );
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_timeout_retries"))
                .is_none()
        );
    }

    #[test]
    fn native_trust_fixture_renders_env_secret_and_exact_advertise_reference() {
        let runtime = make_runtime_1be();
        let mut root =
            render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
                .expect("render base FE config")
                .parse::<Value>()
                .expect("parse rendered config");
        let root = root.as_table_mut().expect("config root table");
        let fixture = PreparedNativeTrustFixture {
            fixture: NativeTrustFixture::automatic_dns(),
            shared_secret: "test-only-fixture-secret".to_string(),
            pem_paths: NativeTrustPemPaths {
                certificate_chain: PathBuf::from("/not-retained/leaf.pem"),
                private_key: PathBuf::from("/not-retained/leaf-key.pem"),
                trust_roots: PathBuf::from("/not-retained/roots.pem"),
            },
        };
        fixture.apply_config(root);
        let cluster = table_mut(root, "cluster");
        cluster.insert(
            "advertise_host".to_string(),
            Value::String(fixture.fixture.advertise_host().to_string()),
        );
        let rendered = toml::to_string(root).expect("serialize rendered fixture config");
        assert!(rendered.contains("${ENV:NOVAROCKS_SYSTEM_NATIVE_TRUST_SECRET}"));
        assert_eq!(
            root["native_trust"]["transport"]["mode"].as_str(),
            Some("automatic")
        );
        assert_eq!(
            root["cluster"]["advertise_host"].as_str(),
            Some("localhost")
        );
        assert!(
            root["native_trust"]["transport"]
                .get("private_key_path")
                .is_none(),
            "automatic profile must not emit PEM paths"
        );

        let be = render_cross_process_launch_config(CrossProcessLaunchConfig {
            base_config: BASE_CONFIG,
            source_config_dir: Path::new("/tmp"),
            role: ClusterProcessRole::Be,
            be_index: 0,
            runtime: &runtime,
            runtime_dir: Path::new("/tmp/novarocks-native-trust-reference"),
            query_lifecycle_faults_enabled: false,
            cleanup_faults_enabled: false,
            overlay: None,
            native_trust_fixture: &fixture,
        })
        .expect("render automatic BE config");
        let be: Value = be.parse().expect("parse automatic BE config");
        assert_eq!(
            be["cluster"]["frontend_endpoint"].as_str(),
            Some("localhost:29070"),
            "BE announce must use the same DNS reference as the FE automatic TLS listener"
        );
    }

    #[test]
    fn render_cross_process_config_does_not_add_runtime_selector() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert!(
            fe_value.get("runtime").is_none(),
            "FE config must not add a runtime selector"
        );
        assert!(
            be_value.get("runtime").is_none(),
            "BE config must not add a runtime selector"
        );
    }

    #[test]
    fn render_cross_process_config_preserves_retired_base_runtime_key() {
        let runtime = make_runtime_1be();
        let retired_key = ["plan", "wire", "format"].join("_");
        let base_config = format!("{}\n[runtime]\n{retired_key} = \"thrift\"\n", BASE_CONFIG);

        let fe = render_cross_process_config(&base_config, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(&base_config, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(
            fe_value["runtime"]
                .get(&retired_key)
                .and_then(Value::as_str),
            Some("thrift"),
            "renderer must leave retired base keys for the product loader to reject"
        );
        assert_eq!(
            be_value["runtime"]
                .get(&retired_key)
                .and_then(Value::as_str),
            Some("thrift"),
            "renderer must leave retired base keys for the product loader to reject"
        );
    }

    #[test]
    fn ordinary_cross_process_launches_do_not_share_persisted_backend_rows() {
        let runtime = make_runtime_1be();
        let native_trust_fixture = rendered_native_trust_fixture();
        let first_runtime = Path::new("/tmp/novarocks-cross-process-run-a");
        let second_runtime = Path::new("/tmp/novarocks-cross-process-run-b");

        let first = render_cross_process_launch_config(CrossProcessLaunchConfig {
            base_config: BASE_CONFIG,
            source_config_dir: Path::new("/tmp"),
            role: ClusterProcessRole::Fe,
            be_index: 0,
            runtime: &runtime,
            runtime_dir: first_runtime,
            query_lifecycle_faults_enabled: false,
            cleanup_faults_enabled: false,
            overlay: None,
            native_trust_fixture: &native_trust_fixture,
        })
        .unwrap()
        .parse::<Value>()
        .unwrap();
        let second = render_cross_process_launch_config(CrossProcessLaunchConfig {
            base_config: BASE_CONFIG,
            source_config_dir: Path::new("/tmp"),
            role: ClusterProcessRole::Fe,
            be_index: 0,
            runtime: &runtime,
            runtime_dir: second_runtime,
            query_lifecycle_faults_enabled: false,
            cleanup_faults_enabled: false,
            overlay: None,
            native_trust_fixture: &native_trust_fixture,
        })
        .unwrap()
        .parse::<Value>()
        .unwrap();

        assert_eq!(
            first["state_store"]["path"].as_str(),
            first_runtime.join("frontend-state.sqlite").to_str()
        );
        assert_eq!(
            second["state_store"]["path"].as_str(),
            second_runtime.join("frontend-state.sqlite").to_str()
        );
        assert!(first.get("metadata").is_none());
        assert!(second.get("metadata").is_none());
        assert_ne!(
            first["state_store"]["path"], second["state_store"]["path"],
            "ephemeral clusters must not restore stale backend rows from another launch"
        );
        assert!(
            first.get("debug").is_none(),
            "ordinary cross-process config must not render debug knobs"
        );
        assert!(
            second.get("debug").is_none(),
            "ordinary cross-process config must not render debug knobs"
        );
    }

    #[test]
    fn static_catalog_snapshot_is_copied_to_the_isolated_fe_runtime() {
        let runtime = make_runtime_1be();
        let root = std::env::temp_dir().join(format!(
            "novarocks-static-catalog-render-{}",
            next_fragment_failure_token(99)
        ));
        let source = root.join("source");
        let output = root.join("runtime");
        fs::create_dir_all(&source).expect("create static source directory");
        fs::create_dir_all(&output).expect("create isolated runtime directory");
        let snapshot = source.join("catalogs.toml");
        fs::write(&snapshot, "format_version = 1\ncatalogs = []\n").expect("write static snapshot");
        let base_config = r#"
[state_store]
provider = "sqlite"
path = "frontend-state.sqlite"
cluster_id = "static-harness"
deployment_owner = "fe-1"

[catalog_source]
mode = "static-file"
static_file_path = "catalogs.toml"
"#;
        let native_trust_fixture = rendered_native_trust_fixture();
        let rendered = render_cross_process_launch_config(CrossProcessLaunchConfig {
            base_config,
            source_config_dir: &source,
            role: ClusterProcessRole::Fe,
            be_index: 0,
            runtime: &runtime,
            runtime_dir: &output,
            query_lifecycle_faults_enabled: false,
            cleanup_faults_enabled: false,
            overlay: None,
            native_trust_fixture: &native_trust_fixture,
        })
        .expect("render static FE config");
        let rendered: Value = rendered.parse().expect("parse static FE config");
        assert_eq!(
            rendered["catalog_source"]["static_file_path"].as_str(),
            Some("catalogs.toml")
        );
        assert_eq!(
            fs::read_to_string(output.join("catalogs.toml")).expect("read copied snapshot"),
            "format_version = 1\ncatalogs = []\n"
        );
        fs::remove_dir_all(&root).expect("remove static catalog render root");
    }

    #[test]
    fn render_cross_process_config_empty_base_patches_fe_heartbeat_only() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config("", ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config("", ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert_eq!(
            fe_value["cluster"]["heartbeat_interval_ms"].as_integer(),
            Some(500)
        );
        assert_eq!(
            fe_value["cluster"]["heartbeat_timeout_retries"].as_integer(),
            Some(2)
        );
        assert!(fe_value["cluster"].get("backends").is_none());

        assert_eq!(be_value["cluster"]["role"].as_str(), Some("be"));
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_interval_ms"))
                .is_none()
        );
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_timeout_retries"))
                .is_none()
        );
    }

    #[test]
    fn render_cross_process_config_2be_fe_has_no_backend_seeds() {
        let runtime = make_runtime_2be();

        let fe = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let fe_value: toml::Value = fe.parse().expect("parse fe toml");

        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert_eq!(
            fe_value["cluster"]["heartbeat_interval_ms"].as_integer(),
            Some(500)
        );
        assert_eq!(
            fe_value["cluster"]["heartbeat_timeout_retries"].as_integer(),
            Some(2)
        );
        assert!(fe_value["cluster"].get("backends").is_none());
    }

    #[test]
    fn render_cross_process_config_2be_each_be_has_own_ports() {
        let runtime = make_runtime_2be();

        let be0 = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be0 config");
        let be1 = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 1, &runtime)
            .expect("render be1 config");

        let be0_value: toml::Value = be0.parse().expect("parse be0 toml");
        let be1_value: toml::Value = be1.parse().expect("parse be1 toml");

        // BE[0]
        assert_eq!(be0_value["cluster"]["role"].as_str(), Some("be"));
        assert_eq!(
            be0_value["cluster"]["frontend_endpoint"].as_str(),
            Some("127.0.0.1:29070")
        );
        assert!(
            be0_value
                .get("cluster")
                .and_then(|c| c.get("backends"))
                .is_none()
        );
        assert_eq!(be0_value["server"]["http_port"].as_integer(), Some(18080));
        assert_eq!(be0_value["server"]["grpc_port"].as_integer(), Some(19070));

        // BE[1]
        assert_eq!(be1_value["cluster"]["role"].as_str(), Some("be"));
        assert_eq!(
            be1_value["cluster"]["frontend_endpoint"].as_str(),
            Some("127.0.0.1:29070")
        );
        assert!(
            be1_value
                .get("cluster")
                .and_then(|c| c.get("backends"))
                .is_none()
        );
        assert_eq!(be1_value["server"]["http_port"].as_integer(), Some(18081));
        assert_eq!(be1_value["server"]["grpc_port"].as_integer(), Some(19071));

        // Ports must differ between the two BEs.
        assert_ne!(
            be0_value["server"]["http_port"].as_integer(),
            be1_value["server"]["http_port"].as_integer()
        );
        assert_ne!(
            be0_value["server"]["grpc_port"].as_integer(),
            be1_value["server"]["grpc_port"].as_integer()
        );
    }

    #[test]
    fn reserved_runtime_ports_new_2_yields_two_distinct_be_port_pairs() {
        let reserved = ReservedRuntimePorts::new(2).expect("reserve 2 BE port pairs");
        assert_eq!(reserved.be_ports.len(), 2);
        let http0 = reserved.be_ports[0].http.port();
        let grpc0 = reserved.be_ports[0].grpc.port();
        let http1 = reserved.be_ports[1].http.port();
        let grpc1 = reserved.be_ports[1].grpc.port();
        // All four ports must be distinct.
        let ports = [http0, grpc0, http1, grpc1];
        for i in 0..ports.len() {
            for j in (i + 1)..ports.len() {
                assert_ne!(
                    ports[i], ports[j],
                    "BE port pair ports must all be distinct: {:?}",
                    ports
                );
            }
        }
    }

    #[test]
    fn remove_fe_state_store_destroys_sidecars_keeps_other_artifacts_and_tolerates_absence() {
        let repo_root = std::env::current_dir().expect("current dir");
        let runtime_root = repo_root.join("tests/cluster-harness/.test-runtime");
        fs::create_dir_all(&runtime_root).expect("create runtime root");
        let dir = runtime_root.join(format!(
            "remove_fe_state_store_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("create wipe test dir");

        let store = dir.join(FE_STATE_STORE_FILE_NAME);
        let wal = dir.join(format!("{FE_STATE_STORE_FILE_NAME}-wal"));
        let shm = dir.join(format!("{FE_STATE_STORE_FILE_NAME}-shm"));
        // The wipe must not touch anything else the launch keeps in the same
        // runtime directory: the lake warehouse and the process logs live here.
        let unrelated = dir.join("fe.log");
        for path in [&store, &wal, &shm, &unrelated] {
            fs::write(path, b"x").expect("create fixture file");
        }
        let fe_config = dir.join("fe.toml");
        fs::write(&fe_config, "[state_store]\nprovider = \"sqlite\"\n")
            .expect("write sqlite FE config");

        let removed = remove_fe_state_store(&dir, &fe_config).expect("remove FE state store");
        assert_eq!(
            removed,
            vec![store.clone(), wal.clone(), shm.clone()],
            "the main database and both WAL sidecars must be destroyed"
        );
        assert!(!store.exists(), "main database must be gone");
        assert!(!wal.exists(), "WAL journal must be gone");
        assert!(!shm.exists(), "shared-memory index must be gone");
        assert!(
            unrelated.exists(),
            "wiping the durable store must not remove unrelated runtime artifacts"
        );

        let removed_again =
            remove_fe_state_store(&dir, &fe_config).expect("absent store is not an error");
        assert!(
            removed_again.is_empty(),
            "a second wipe removes nothing and still succeeds: {removed_again:?}"
        );

        // A provider that keeps its store outside this directory must fail
        // loudly: a silent no-op would let a caller assert "the FE lost every
        // durable record" while every record was still readable.
        let remote_config = dir.join("fe-mysql.toml");
        fs::write(&remote_config, "[state_store]\nprovider = \"mysql\"\n")
            .expect("write mysql FE config");
        let refusal = remove_fe_state_store(&dir, &remote_config)
            .expect_err("a non-sqlite durable store must refuse the wipe");
        assert!(
            format!("{refusal:#}").contains("only \"sqlite\""),
            "refusal must name the supported provider: {refusal:#}"
        );

        fs::remove_dir_all(&dir).expect("cleanup wipe test dir");
    }

    #[test]
    fn runtime_dir_guard_removes_directory_on_drop_and_keeps_it_when_disarmed() {
        let repo_root = std::env::current_dir().expect("current dir");
        let runtime_root = repo_root.join("tests/cluster-harness/.test-runtime");
        fs::create_dir_all(&runtime_root).expect("create runtime root");

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let dir = runtime_root.join(format!(
            "runtime_dir_guard_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&dir).expect("create runtime dir");

        {
            let guard = RuntimeDirGuard::new(dir.clone());
            drop(guard);
        }
        assert!(!dir.exists(), "runtime dir should be removed on drop");

        fs::create_dir_all(&dir).expect("recreate runtime dir");
        let guard = RuntimeDirGuard::new(dir.clone());
        let dir = guard.into_path();
        assert!(
            dir.exists(),
            "disarmed runtime dir should remain for caller cleanup"
        );

        fs::remove_dir_all(&dir).expect("cleanup runtime dir");
    }

    #[test]
    fn fragment_failure_token_publish_is_complete_and_does_not_clobber_an_existing_arm() {
        let repo_root = std::env::current_dir().expect("current dir");
        let runtime_root = repo_root.join("tests/sql/.runtime/unit");
        fs::create_dir_all(&runtime_root).expect("create runtime root");
        let dir = runtime_root.join(format!(
            "fragment_failure_publish_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("create fragment failure test dir");
        let trigger = dir.join("be_1.fragment_failure_trigger");

        publish_fragment_failure_token(&trigger, "step-token-17")
            .expect("publish complete fragment failure token");
        assert_eq!(
            fs::read_to_string(&trigger).expect("read published trigger"),
            "step-token-17"
        );

        let error = publish_fragment_failure_token(&trigger, "replacement-token")
            .expect_err("a second arm must not replace the active trigger");
        assert!(
            format!("{error:#}").contains("publish fragment executor failure trigger"),
            "{error:#}"
        );
        assert_eq!(
            fs::read_to_string(&trigger).expect("read original trigger"),
            "step-token-17"
        );
        fs::remove_dir_all(dir).expect("cleanup fragment failure test dir");
    }

    #[test]
    fn prometheus_labeled_gauge_requires_one_exact_sample() {
        let metrics = concat!(
            "novarocks_backend_query_execution_resources{resource=\"stage_active_builders\"} 7\n",
            "novarocks_backend_query_execution_resources{resource=\"stage_encoded_bytes\"} 11\n",
            "novarocks_backend_query_execution_resources{resource=\"native_query_active_fragments\"} 3\n",
            "novarocks_frontend_query_lifecycle_control_total{outcome=\"control_ready\"} 13\n",
        );
        assert_eq!(
            prometheus_labeled_gauge(
                metrics,
                QUERY_EXECUTION_RESOURCE_METRIC,
                "resource",
                "stage_active_builders"
            )
            .expect("read exact resource sample"),
            7.0
        );
        assert_eq!(
            prometheus_labeled_gauge(
                metrics,
                QUERY_EXECUTION_RESOURCE_METRIC,
                "resource",
                "native_query_active_fragments"
            )
            .expect("read exact native fragment activity sample"),
            3.0
        );
        assert_eq!(
            prometheus_labeled_gauge(
                metrics,
                FRONTEND_QUERY_LIFECYCLE_CONTROL_METRIC,
                "outcome",
                "control_ready"
            )
            .expect("read exact frontend control-ready sample"),
            13.0
        );
        assert!(
            prometheus_labeled_gauge(
                metrics,
                QUERY_EXECUTION_RESOURCE_METRIC,
                "resource",
                "native_query_contexts_active"
            )
            .is_err()
        );
    }

    #[test]
    fn role_scoped_metrics_reject_foreign_role_families() {
        let frontend = FRONTEND_METRIC_FAMILIES.join("\n");
        assert_contains_metric_families(&frontend, &FRONTEND_METRIC_FAMILIES, "FE")
            .expect("frontend body includes every frontend family");
        assert_excludes_metric_families(&frontend, &BACKEND_METRIC_FAMILIES, "FE")
            .expect("frontend body excludes backend families");

        let backend = BACKEND_METRIC_FAMILIES.join("\n");
        assert_contains_metric_families(&backend, &BACKEND_METRIC_FAMILIES, "BE")
            .expect("backend body includes every backend family");
        assert_excludes_metric_families(&backend, &FRONTEND_METRIC_FAMILIES, "BE")
            .expect("backend body excludes frontend families");
        let error = assert_excludes_metric_families(
            &format!("{backend}\n{}", FRONTEND_METRIC_FAMILIES[0]),
            &FRONTEND_METRIC_FAMILIES,
            "BE",
        )
        .expect_err("backend leak must fail the role boundary");
        assert!(
            error.to_string().contains(FRONTEND_METRIC_FAMILIES[0]),
            "{error:#}"
        );
    }

    #[test]
    fn child_environment_applies_common_be_values_and_index_overrides() {
        let common = BTreeMap::from([
            ("COMMON".to_string(), "yes".to_string()),
            ("OVERRIDE".to_string(), "common".to_string()),
        ]);
        let overrides = BTreeMap::from([(
            1,
            BTreeMap::from([("OVERRIDE".to_string(), "be-1".to_string())]),
        )]);
        let environments =
            resolve_be_environments(&common, &overrides, 3).expect("resolve BE environments");
        assert_eq!(environments.len(), 3);
        assert_eq!(environments[0]["OVERRIDE"], "common");
        assert_eq!(environments[1]["OVERRIDE"], "be-1");
        assert_eq!(environments[2]["COMMON"], "yes");
    }

    #[test]
    fn child_environment_rejects_out_of_range_be_override() {
        let overrides = BTreeMap::from([(
            2,
            BTreeMap::from([("MARKER".to_string(), "value".to_string())]),
        )]);
        let error = resolve_be_environments(&BTreeMap::new(), &overrides, 2)
            .expect_err("out of range override must fail");
        assert!(format!("{error:#}").contains("out of bounds"));
    }

    #[test]
    fn config_overlay_merges_allowed_tables_and_rejects_harness_owned_values() {
        let mut config: Value = BASE_CONFIG.parse().expect("parse base config");
        let root = config.as_table_mut().expect("config root");
        merge_safe_config_overlay(
            root,
            "[standalone_server]\nmv_refresh_scheduler_enabled = true\n[runtime]\nexchange_wait_ms = 42\n",
        )
        .expect("merge safe overlay");
        assert_eq!(
            root["standalone_server"]["mv_refresh_scheduler_enabled"].as_bool(),
            Some(true)
        );
        assert_eq!(root["runtime"]["exchange_wait_ms"].as_integer(), Some(42));
        assert!(merge_safe_config_overlay(root, "[cluster]\nrole = 'be'\n").is_err());
        assert!(merge_safe_config_overlay(root, "[server]\ngrpc_port = 1\n").is_err());
        merge_safe_config_overlay(
            root,
            "[server]\nfrontend_drain_timeout_ms = 500\nfrontend_cleanup_timeout_ms = 2000\n",
        )
        .expect("merge scenario-safe frontend drain budgets");
        assert_eq!(
            root["server"]["frontend_drain_timeout_ms"].as_integer(),
            Some(500)
        );
        assert!(merge_safe_config_overlay(root, "[standalone_server]\nmysql_port = 1\n").is_err());
    }

    #[test]
    fn resource_convergence_allows_a_killed_backend_but_not_a_live_leak() {
        let baseline = QueryExecutionResourceSnapshot {
            fe_running: true,
            frontend_control_ready: 0.0,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        let exited = QueryExecutionResourceSnapshot {
            fe_running: true,
            frontend_control_ready: 0.0,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: false,
                resources: BTreeMap::new(),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 0.0,
                terminal_max_retained_bytes: 0.0,
            }],
        };
        assert!(exited.convergence_failure(&baseline, false).is_none());

        let leaked = QueryExecutionResourceSnapshot {
            fe_running: true,
            frontend_control_ready: 0.0,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 1.0)]),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        assert!(
            leaked
                .convergence_failure(&baseline, false)
                .expect("live leak must be reported")
                .contains("native_query_contexts_active")
        );
    }

    #[test]
    fn resource_convergence_allows_bounded_terminal_retention_after_frontend_crash() {
        let baseline = QueryExecutionResourceSnapshot {
            fe_running: true,
            frontend_control_ready: 0.0,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        let retained = QueryExecutionResourceSnapshot {
            fe_running: true,
            frontend_control_ready: 0.0,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 2.0,
                terminal_retained_bytes: 512.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };

        assert!(retained.convergence_failure(&baseline, true).is_none());
        assert!(retained.convergence_failure(&baseline, false).is_some());
    }

    #[test]
    fn resource_convergence_allows_existing_terminal_retention_to_expire() {
        let baseline = QueryExecutionResourceSnapshot {
            fe_running: true,
            frontend_control_ready: 0.0,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 2.0,
                terminal_retained_bytes: 512.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        let expired = QueryExecutionResourceSnapshot {
            fe_running: true,
            frontend_control_ready: 0.0,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 1.0,
                terminal_retained_bytes: 256.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };

        assert!(expired.convergence_failure(&baseline, false).is_none());
    }
}
