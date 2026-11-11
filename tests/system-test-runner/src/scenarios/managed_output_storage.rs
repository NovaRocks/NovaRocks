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

//! Conditional creation against the live shared MinIO. The fixture owns a
//! private scratch range and the proxy records real HTTP outcomes before
//! independently checking publication, object content and ownership.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_cluster_harness::fixture_run::RunBudget;
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use novarocks_cluster_harness::s3_write_fault::{
    AckTarget, PauseAfter, S3Operation, S3WriteEvent, S3WriteFault,
};
use novarocks_fs::{
    FsAccessHandle, FsAccessResolver, ObjectStoreAccessContext, ObjectStoreConfig,
    ObjectStoreProviderPool, ObjectStoreProviderPoolOptions, SecretValue,
};
use novarocks_spi::connector::{StaticCredentialReference, StorageAccessDomainId};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::actors::sql::{self, SqlSession, StatementFailure};
use crate::scenario::{
    Scenario, ScenarioContext, ScenarioDeclaration, ScenarioHeavyReason, ScenarioLaunchConfig,
};

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::iceberg_delete_applicability::{
    CATALOG, create_catalog_sql_from_parts, delete_launch_config,
};
use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use super::writer_statement_actor::{WriterActor, combine};

const RETRIES: usize = 3;
const RETRY_MIN_MS: u64 = 20;
const RETRY_MAX_MS: u64 = 100;
const REQUEST_TIMEOUT_MS: u64 = 20_000;
const IO_TIMEOUT_MS: u64 = 10_000;
const PART: usize = 5 * 1024 * 1024;
const SOURCE: &str = "managed_objects.managed_input";
const FAILED_CTAS: &str = "managed_objects.failed_ctas";
const CANCELED_CTAS: &str = "managed_objects.canceled_ctas";
const CTAS_FAULT: &str = "connector-write-root-read-after-prefix-failure";
const CTAS_HOLD: &str = "connector-write-append-hold";
const MARKER_ID: &str = "novarocks-create-id";
const MARKER_IDENTITY: &str = "novarocks-create-identity";
const OWN: &str = "d56eff61-8e8c-41bc-9abe-1a1ce7000001";
const FOREIGN: &str = "d56eff61-8e8c-41bc-9abe-1a1ce7000002";
const READBACK_FAULTS: [ReadbackFault; 3] = [
    ReadbackFault::ForeignSameBytes,
    ReadbackFault::OwnedWrongDigest,
    ReadbackFault::MissingAfterRetry,
];

#[derive(Clone, Copy, Debug)]
enum ReadbackFault {
    ForeignSameBytes,
    OwnedWrongDigest,
    MissingAfterRetry,
}

impl ReadbackFault {
    fn sql(self) -> String {
        let id = match self {
            Self::ForeignSameBytes => 101,
            Self::OwnedWrongDigest => 102,
            Self::MissingAfterRetry => 103,
        };
        format!("INSERT INTO {CATALOG}.{SOURCE} VALUES ({id})")
    }

    fn pause(self) -> PauseAfter {
        match self {
            Self::MissingAfterRetry => PauseAfter::RetryPreconditionFailure,
            _ => PauseAfter::SuccessfulCreate,
        }
    }

    fn head_status(self) -> u16 {
        match self {
            Self::MissingAfterRetry => 404,
            _ => 200,
        }
    }
}

struct Fixture {
    root: PathBuf,
    rest: IsolatedIcebergRestFixture,
    proxy: S3WriteFault,
}

#[derive(Default)]
pub(super) struct ManagedOutputStorage {
    fixture: Mutex<Option<Fixture>>,
    observations: SqlIntentOwner,
}

impl Scenario for ManagedOutputStorage {
    fn name(&self) -> &'static str {
        "connector/managed-output-storage-conformance"
    }

    fn declaration(&self) -> ScenarioDeclaration {
        let mut declaration =
            super::declarations::for_module("managed_output_storage", self.name(), true, false);
        declaration.basis = Some(
            "Fresh native 1FE+3BE, a private REST catalog and an exclusive scratch range on the ready shared MinIO publication. Input and cleanup belong to this invocation.",
        );
        declaration.heavy = Some(ScenarioHeavyReason {
            reason: "fixture",
            detail: "requires a ready shared MinIO publication, isolated REST scratch, and native 1FE+3BE",
        });
        declaration
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config_with_fixture_run(
        &self,
        root: &Path,
        budget: &RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        budget.check_work()?;
        let rest = IsolatedIcebergRestFixture::start_with_shared_minio(root, budget)
            .context("start managed output storage scratch")?;
        let config = delete_launch_config(&rest, false);
        let proxy = S3WriteFault::start(
            &rest.endpoints().minio_endpoint,
            Duration::from_millis(REQUEST_TIMEOUT_MS),
        )?;
        let identity = rest.runtime_identity();
        let mut slot = self.fixture.lock().expect("managed output fixture lock");
        ensure!(slot.is_none(), "managed output fixture launched twice");
        *slot = Some(Fixture {
            root: root.to_owned(),
            rest,
            proxy,
        });
        std::fs::write(
            root.join("managed-output-runtime.json"),
            serde_json::to_vec_pretty(&json!({
                "fixture":identity?,
                "retry_max_times":RETRIES,
                "retry_min_delay_ms":RETRY_MIN_MS,
                "retry_max_delay_ms":RETRY_MAX_MS,
                "request_timeout_ms":REQUEST_TIMEOUT_MS,
                "io_timeout_ms":IO_TIMEOUT_MS,
            }))?,
        )?;
        Ok(config)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let observed = || {
            (0..cluster_size)
                .flat_map(|index| {
                    use novarocks_failpoint::observation_marker::ObservationAction;
                    [
                        (index, ObservationAction::TaskProtocol.as_str(), None),
                        (index, ObservationAction::WriterMarker.as_str(), None),
                    ]
                })
                .collect()
        };
        let specs = [(FAILED_CTAS, CTAS_FAULT), (CANCELED_CTAS, CTAS_HOLD)]
            .into_iter()
            .map(|(table, fault)| StatementSpec {
                sql: ctas_sql(table),
                faults: (0..cluster_size).map(|index| (index, fault)).collect(),
                observations: observed(),
            })
            .chain(READBACK_FAULTS.map(|fault| StatementSpec {
                sql: fault.sql(),
                faults: Vec::new(),
                observations: observed(),
            }))
            .collect();
        Ok(Some(self.observations.freeze(
            specs,
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut slot = self.fixture.lock().expect("managed output fixture lock");
        let fixture = slot.as_mut().context("managed output fixture is missing")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        context.action("probe conditional single and multipart creation on real MinIO");
        runtime.block_on(probe_storage(fixture))?;
        context.action("exercise native INSERT, CTAS and DV materialization on the same store");
        native_writes(fixture, context, &self.observations, &runtime)
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .expect("managed output fixture lock")
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        let exit = fixture.proxy.shutdown();
        let report = (|| -> Result<()> {
            let report = match &exit {
                Ok(exit) => serde_json::to_value(exit)?,
                Err(error) => {
                    json!({"exit_error":error.to_string(),"events":fixture.proxy.events()})
                }
            };
            std::fs::write(
                fixture.root.join("managed-output-http-exit.json"),
                serde_json::to_vec_pretty(&report)?,
            )?;
            Ok(())
        })();
        let proxy = combine(exit.map(|_| ()), report);

        let storage = fixture.rest.shutdown();
        combine(proxy, storage)
    }
}

fn config(fixture: &Fixture, endpoint: &str) -> ObjectStoreConfig {
    let identity = fixture.rest.static_s3_identity();
    ObjectStoreConfig {
        endpoint: endpoint.to_owned(),
        access_key_id: SecretValue::new(identity.access_key_id),
        access_key_secret: SecretValue::new(identity.secret_access_key),
        session_token: None,
        enable_path_style_access: Some(true),
        region: Some("us-east-1".to_owned()),
        retry_max_times: Some(RETRIES),
        retry_min_delay_ms: Some(RETRY_MIN_MS),
        retry_max_delay_ms: Some(RETRY_MAX_MS),
        timeout_ms: Some(REQUEST_TIMEOUT_MS),
        io_timeout_ms: Some(IO_TIMEOUT_MS),
    }
}

fn access(
    cfg: &ObjectStoreConfig,
    pool: &ObjectStoreProviderPool,
    uri: &str,
) -> Result<FsAccessHandle> {
    let domain = StorageAccessDomainId::from_bytes(Sha256::digest(cfg.endpoint.as_bytes()).into());
    Ok(FsAccessResolver::new().resolve_location(
        domain,
        uri,
        Some(ObjectStoreAccessContext::for_static(
            cfg.endpoint_config(),
            StaticCredentialReference::try_new("managed-output-fixture", "v1")?,
            cfg.secret_material(),
            pool,
        )),
    )?)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn marker(id: &str, object_uri: &str) -> [(String, String); 2] {
    [
        (MARKER_ID.into(), id.into()),
        (MARKER_IDENTITY.into(), sha256(object_uri.as_bytes())),
    ]
}

async fn verify_readback(
    oracle: &FsAccessHandle,
    expected: &[(String, String); 2],
    payload: &[u8],
) -> Result<()> {
    let op = oracle.operator();
    let relative = oracle.paths()[0].operator_relative_path();
    let metadata = op.stat(relative).await?;
    ensure!(
        metadata.content_length() == payload.len() as u64,
        "independent storage readback length differs"
    );
    let actual = metadata
        .user_metadata()
        .context("independent object marker is absent")?;
    ensure!(
        expected
            .iter()
            .all(|(name, value)| actual.get(name) == Some(value)),
        "independent storage readback marker differs"
    );
    let bytes = op.read(relative).await?.to_bytes();
    ensure!(
        bytes.as_ref() == payload && sha256(&bytes) == sha256(payload),
        "independent storage readback content differs"
    );
    Ok(())
}

fn upload_exit(event: &S3WriteEvent, upload: &str) -> bool {
    event.upload_id_sha256.as_deref() == Some(upload)
        && event.upstream_response_complete
        && !event.downstream_response_dropped
        && matches!(
            (event.operation, event.status, event.xml_code.as_deref()),
            (S3Operation::Abort, Some(204), None)
                | (
                    S3Operation::Abort | S3Operation::Complete,
                    Some(404),
                    Some("NoSuchUpload")
                )
        )
}

fn verify_multipart(
    events: &[S3WriteEvent],
    condition_status: Option<u16>,
    lost_ack: bool,
) -> Result<()> {
    let uploads = events
        .iter()
        .filter(|event| event.operation == S3Operation::Initiate)
        .collect::<Vec<_>>();
    ensure!(
        uploads.len() == 1
            && uploads[0].upstream_response_complete
            && uploads[0]
                .status
                .is_some_and(|status| (200..300).contains(&status)),
        "expected one real successful MPU initiation: {events:?}"
    );
    let upload = uploads[0]
        .upload_id_sha256
        .as_ref()
        .context("real MPU initiation omitted upload identity")?;
    for number in [1, 2] {
        ensure!(
            events
                .iter()
                .any(|event| event.operation == S3Operation::Part
                    && event.part_number == Some(number)
                    && event.upload_id_sha256.as_ref() == Some(upload)
                    && event
                        .status
                        .is_some_and(|status| (200..300).contains(&status))
                    && event.upstream_response_complete),
            "missing successful MPU part {number}: {events:?}"
        );
    }
    let completes = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.operation == S3Operation::Complete)
        .collect::<Vec<_>>();
    ensure!(
        !completes.is_empty()
            && completes
                .iter()
                .all(|(_, event)| event.upload_id_sha256.as_ref() == Some(upload)
                    && event.condition_if_none_match_star
                    && event.upstream_response_complete),
        "conditional Complete must address the actual upload: {events:?}"
    );
    if let Some(status) = condition_status {
        ensure!(
            completes.len() == 1
                && completes[0].1.status == Some(status)
                && completes[0].1.xml_code.as_deref() == Some("PreconditionFailed")
                && !completes[0].1.downstream_response_dropped,
            "foreign MPU completion did not receive actual 412: {events:?}"
        );
        ensure!(
            events
                .iter()
                .any(|event| event.operation == S3Operation::Abort
                    && event.upload_id_sha256.as_ref() == Some(upload)
                    && event.upstream_response_complete
                    && event.status == Some(204)),
            "the original failed MPU did not settle its abort: {events:?}"
        );
    }
    if lost_ack {
        let [(_, original), (retry_index, retry), ..] = completes.as_slice() else {
            anyhow::bail!(
                "production RetryLayer did not repeat the actual completed MPU: {events:?}"
            );
        };
        ensure!(
            original
                .status
                .is_some_and(|status| (200..300).contains(&status))
                && original.downstream_response_dropped
                && retry.attempt == original.attempt + 1
                && !retry.downstream_response_dropped,
            "production RetryLayer did not repeat the actual completed MPU: {events:?}"
        );
        ensure!(
            events[*retry_index..]
                .iter()
                .any(|event| upload_exit(event, upload)),
            "lost Complete ACK left no confirmed exit for the same upload: {events:?}"
        );
    }
    Ok(())
}

fn verify_single(events: &[S3WriteEvent], lost_ack: bool) -> Result<()> {
    ensure!(
        !events.iter().any(|event| matches!(
            event.operation,
            S3Operation::Initiate | S3Operation::Part | S3Operation::Complete
        )),
        "single conditional PUT unexpectedly used multipart"
    );
    let puts = events
        .iter()
        .filter(|event| event.operation == S3Operation::Put && event.condition_if_none_match_star)
        .collect::<Vec<_>>();
    if lost_ack {
        ensure!(
            puts.len() == 2
                && puts[0]
                    .status
                    .is_some_and(|status| (200..300).contains(&status))
                && puts[0].upstream_response_complete
                && puts[0].downstream_response_dropped
                && puts[1].attempt == puts[0].attempt + 1,
            "production RetryLayer did not repeat the actual completed PUT: {events:?}"
        );
    } else {
        ensure!(
            puts.len() == 1,
            "expected one colliding conditional PUT: {events:?}"
        );
    }
    let refused = puts.last().context("conditional PUT absent")?;
    ensure!(
        refused.status == Some(412)
            && refused.xml_code.as_deref() == Some("PreconditionFailed")
            && refused.upstream_response_complete
            && !refused.downstream_response_dropped,
        "conditional PUT did not observe actual MinIO 412: {events:?}"
    );
    Ok(())
}

async fn probe_storage(fixture: &Fixture) -> Result<()> {
    let endpoints = fixture.rest.endpoints();
    let through_proxy = config(fixture, fixture.proxy.endpoint());
    let direct = config(fixture, &endpoints.minio_endpoint);
    let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())?;
    let scratch = format!(
        "{}/managed-output-probes",
        endpoints.rest_warehouse.trim_end_matches('/')
    );
    let first = Bytes::from(vec![0x71; PART]);
    let second = Bytes::from(vec![0x9e; PART]);
    let mut outcomes = Vec::new();
    for (name, multipart, lost_ack) in [
        ("foreign-single", false, false),
        ("foreign-multipart", true, false),
        ("lost-single", false, true),
        ("lost-multipart", true, true),
    ] {
        let uri = format!("{scratch}/{name}");
        let object_path = uri.replacen("s3://", "/", 1);
        let signed = access(&through_proxy, &pool, &uri)?;
        let oracle = access(&direct, &pool, &uri)?;
        let (op, upload_witness) = signed.conditional_object_writer_operator()?;
        let path = signed.paths()[0].operator_relative_path();
        let mut payload = b"same fixture payload".to_vec();
        if multipart {
            payload = [first.as_ref(), second.as_ref()].concat();
        }
        let own = marker(OWN, &uri);
        let foreign = marker(FOREIGN, &uri);
        let start = fixture.proxy.events().len();
        if lost_ack {
            fixture.proxy.arm_ack_loss(
                if multipart {
                    S3Operation::Complete
                } else {
                    S3Operation::Put
                },
                AckTarget::Object(object_path.clone()),
            )?;
        } else {
            oracle
                .operator()
                .write_with(
                    oracle.paths()[0].operator_relative_path(),
                    Bytes::copy_from_slice(&payload),
                )
                .user_metadata(foreign.clone())
                .await?;
        }
        let (close_kind, abort_kind) = if multipart {
            let mut writer = op
                .writer_with(path)
                .if_not_exists(true)
                .chunk(PART)
                .concurrent(1)
                .user_metadata(own.clone())
                .await?;
            let close = async {
                writer.write(first.clone()).await?;
                writer.write(second.clone()).await?;
                writer.close().await
            }
            .await;
            let abort = if close.is_err() {
                writer
                    .abort()
                    .await
                    .err()
                    .map(|error| format!("{:?}", error.kind()))
            } else {
                None
            };
            (
                close.err().map(|error| format!("{:?}", error.kind())),
                abort,
            )
        } else {
            let close = op
                .write_with(path, Bytes::copy_from_slice(&payload))
                .if_not_exists(true)
                .user_metadata(own.clone())
                .await
                .err()
                .map(|error| format!("{:?}", error.kind()));
            (close, None)
        };
        let events = fixture.proxy.events()[start..]
            .iter()
            .filter(|event| event.object_path == object_path)
            .cloned()
            .collect::<Vec<_>>();
        if multipart {
            verify_multipart(&events, (!lost_ack).then_some(412), lost_ack)?;
        } else {
            verify_single(&events, lost_ack)?;
        }
        if !lost_ack || !multipart {
            ensure!(
                close_kind.is_some(),
                "raw conditional writer accepted a real MinIO 412"
            );
        }
        upload_witness
            .check_exit()
            .context("production FS upload witness did not settle")?;
        verify_readback(&oracle, if lost_ack { &own } else { &foreign }, &payload).await?;
        outcomes.push(
            json!({"case":name,"sha256":sha256(&payload),"length":payload.len(),
            "sdk_close_error_kind":close_kind,"sdk_abort_error_kind":abort_kind,
            "fs_upload_exit_confirmed":true,"object_marker_owned":lost_ack,"http":events}),
        );
        std::fs::write(
            fixture.root.join("managed-output-fs-probes.json"),
            serde_json::to_vec_pretty(&outcomes)?,
        )?;
    }
    Ok(())
}

fn ctas_sql(table: &str) -> String {
    format!("CREATE TABLE {CATALOG}.{table} AS SELECT id FROM {CATALOG}.{SOURCE}")
}

fn require_absent_table(control: &mut SqlSession, name: &str) -> Result<()> {
    let tables = control.execute(format!("SHOW TABLES FROM {CATALOG}.managed_objects"))?;
    ensure!(
        tables.is_result_set() && tables.columns.len() == 1,
        "SHOW TABLES did not return its original result shape"
    );
    for row in tables.rows {
        let [sql::TrinoValue::Text(table)] = row.as_slice() else {
            anyhow::bail!("unexpected SHOW TABLES result carrier");
        };
        ensure!(table != name, "CTAS unexpectedly published {name}");
    }
    Ok(())
}

fn staged_ctas_objects(fixture: &Fixture, start: usize) -> Vec<String> {
    fixture.proxy.events()[start..]
        .iter()
        .filter(|event| {
            event.operation == S3Operation::Put
                && event.condition_if_none_match_star
                && event
                    .status
                    .is_some_and(|status| (200..300).contains(&status))
                && event.upstream_response_complete
                && event.object_path.ends_with(".parquet")
        })
        .map(|event| event.object_path.clone())
        .collect()
}

fn cancel_ctas(context: &mut ScenarioContext, observations: &SqlIntentOwner) -> Result<()> {
    let selected = observations.take(1)?;
    let actor = WriterActor::start(context, &ctas_sql(CANCELED_CTAS), selected.clone(), None)?;
    let outcome = (|| {
        loop {
            context.remaining("wait for the original CTAS append hold")?;
            for &(index, kind) in &selected.faults {
                if context.handle().selected_query_lifecycle_fault_hit(
                    index,
                    kind,
                    &selected.token,
                )? {
                    return actor.cancel(selected.deadline);
                }
            }
            ensure!(
                actor.done.try_recv().is_err(),
                "CTAS exited before the selected append hold"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    })();
    let mut scopes_closed = Ok(());
    for &(index, kind) in &selected.faults {
        let close =
            context
                .handle()
                .end_selected_query_lifecycle_fault(index, kind, &selected.token);
        scopes_closed = combine(scopes_closed, close.map_err(Into::into));
    }
    let terminal = combine(outcome, scopes_closed).and_then(|()| {
        actor
            .done
            .recv_timeout(context.remaining("observe canceled CTAS terminal")?)
            .context("canceled CTAS has no actual SQL terminal")
    });
    let outcome = actor.settle(context, terminal)?;
    ensure!(
        matches!(outcome, Err(StatementFailure::Trino(_))),
        "native canceled CTAS did not reach a public SQL terminal"
    );
    Ok(())
}

fn live_paths(control: &mut SqlSession, table: &str) -> Result<Vec<String>> {
    let result = control.execute(format!(
        "SELECT file_path FROM {table}$files WHERE content=0 ORDER BY file_path"
    ))?;
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [sql::TrinoValue::Text(path)] => Ok(path.clone()),
            _ => anyhow::bail!("unexpected data file metadata carrier"),
        })
        .collect()
}

fn object_uri(path: &str) -> String {
    format!("s3://{}", path.trim_start_matches('/'))
}

fn object_digest(
    fixture: &Fixture,
    runtime: &tokio::runtime::Runtime,
    uri: &str,
) -> Result<String> {
    runtime.block_on(async {
        let cfg = config(fixture, &fixture.rest.endpoints().minio_endpoint);
        let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())?;
        let access = access(&cfg, &pool, uri)?;
        let bytes = access
            .operator()
            .read(access.paths()[0].operator_relative_path())
            .await?
            .to_bytes();
        Ok(sha256(&bytes))
    })
}

async fn alter_fixture_object(
    fixture: &Fixture,
    path: &str,
    fault: ReadbackFault,
) -> Result<Option<String>> {
    let cfg = config(fixture, &fixture.rest.endpoints().minio_endpoint);
    let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())?;
    let access = access(&cfg, &pool, &object_uri(path))?;
    let op = access.operator();
    let relative = access.paths()[0].operator_relative_path();
    let metadata = op.stat(relative).await?;
    let original_marker = metadata
        .user_metadata()
        .context("native create marker absent")?;
    ensure!(
        original_marker.contains_key(MARKER_ID) && original_marker.contains_key(MARKER_IDENTITY),
        "native write did not use an exact create marker"
    );
    match fault {
        ReadbackFault::ForeignSameBytes => {
            let data = op.read(relative).await?.to_bytes();
            op.write(relative, data.clone()).await?;
            ensure!(
                op.read(relative).await?.to_bytes() == data,
                "fixture did not preserve the foreign object's bytes"
            );
            return Ok(Some(sha256(&data)));
        }
        ReadbackFault::OwnedWrongDigest => {
            let mut data = op.read(relative).await?.to_vec();
            *data.first_mut().context("native staged parquet is empty")? ^= 1;
            op.write_with(relative, data)
                .user_metadata(original_marker.clone())
                .await?;
        }
        ReadbackFault::MissingAfterRetry => op.delete(relative).await?,
    }
    Ok(None)
}

fn run_readback_fault(
    fixture: &Fixture,
    context: &mut ScenarioContext,
    observations: &SqlIntentOwner,
    runtime: &tokio::runtime::Runtime,
    prefix: &str,
    index: usize,
    fault: ReadbackFault,
) -> Result<(String, Option<String>)> {
    let pause = fixture.proxy.arm_ack_loss_with_pause(
        S3Operation::Put,
        AckTarget::NextObjectUnder(prefix.to_owned()),
        fault.pause(),
    )?;
    let actor = WriterActor::start(context, &fault.sql(), observations.take(index + 2)?, None)?;
    let operation = (|| -> Result<_> {
        let path = pause.wait_for_object(
            context
                .remaining("await the exact MinIO write pause")?
                .min(Duration::from_millis(IO_TIMEOUT_MS)),
        )?;
        ensure!(
            path.starts_with(prefix),
            "managed output fault escaped the private target"
        );
        let original_hash = runtime.block_on(alter_fixture_object(fixture, &path, fault))?;
        Ok((path, original_hash))
    })();
    drop(pause);
    let (path, terminal) = match operation {
        Ok(path) => (
            Some(path),
            context
                .remaining("observe readback refusal")
                .and_then(|remaining| {
                    actor
                        .done
                        .recv_timeout(remaining)
                        .context("readback fault has no actual SQL terminal")
                }),
        ),
        Err(error) => (None, Err(error)),
    };
    let outcome = actor.settle(context, terminal)?;
    ensure!(
        matches!(outcome, Err(StatementFailure::Trino(_))),
        "native managed create with {fault:?} did not fail through the SQL owner"
    );
    path.context("readback fault did not observe the exact object path")
}

fn check_readback_exit(
    fixture: &Fixture,
    runtime: &tokio::runtime::Runtime,
    start: usize,
    path: &str,
    original_hash: Option<&str>,
    fault: ReadbackFault,
) -> Result<serde_json::Value> {
    let events = fixture.proxy.events()[start..]
        .iter()
        .filter(|event| event.object_path == path)
        .cloned()
        .collect::<Vec<_>>();
    verify_single(&events, true)?;
    ensure!(
        events
            .iter()
            .any(|event| event.operation == S3Operation::Head
                && event.status == Some(fault.head_status())
                && event.upstream_response_complete),
        "native write did not observe the actual readback status for {fault:?}: {events:?}"
    );
    let cleanup = events
        .iter()
        .filter(|event| event.operation == S3Operation::Delete)
        .collect::<Vec<_>>();
    let uri = object_uri(path);
    match fault {
        ReadbackFault::ForeignSameBytes => {
            ensure!(
                cleanup.is_empty() && fixture.rest.object_exists(&uri)?,
                "native cleanup deleted or tried to own the foreign object"
            );
            runtime.block_on(async {
                let cfg = config(fixture, &fixture.rest.endpoints().minio_endpoint);
                let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())?;
                let access = access(&cfg, &pool, &uri)?;
                let op = access.operator();
                let path = access.paths()[0].operator_relative_path();
                let metadata = op.stat(path).await?;
                ensure!(
                    metadata
                        .user_metadata()
                        .is_none_or(|map| !map.contains_key(MARKER_ID)),
                    "foreign replacement unexpectedly regained the native ownership marker"
                );
                let actual = op.read(path).await?.to_bytes();
                ensure!(
                    original_hash == Some(sha256(&actual).as_str()),
                    "native failure path changed the foreign object's bytes"
                );
                Ok::<_, anyhow::Error>(())
            })?;
        }
        ReadbackFault::OwnedWrongDigest => ensure!(
            events
                .iter()
                .any(|event| event.operation == S3Operation::Get
                    && event
                        .status
                        .is_some_and(|status| matches!(status, 200 | 206)))
                && cleanup
                    .iter()
                    .any(|event| event.status == Some(204) && event.upstream_response_complete)
                && !fixture.rest.object_exists(&uri)?,
            "native owner failed to read back and retire its known-owned mismatching output",
        ),
        ReadbackFault::MissingAfterRetry => ensure!(
            cleanup.is_empty() && !fixture.rest.object_exists(&uri)?,
            "native cleanup tried to remove an unproven target after actual HEAD 404"
        ),
    }
    Ok(json!({"fault":format!("{fault:?}"),"http":events}))
}

fn native_writes(
    fixture: &Fixture,
    context: &mut ScenarioContext,
    observations: &SqlIntentOwner,
    runtime: &tokio::runtime::Runtime,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let mut sql = SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    let endpoints = fixture.rest.endpoints();
    let catalog = create_catalog_sql_from_parts(
        &endpoints.rest_uri,
        &endpoints.rest_warehouse,
        fixture.proxy.endpoint(),
    );
    let retry_properties = format!(
        r#", "aws.s3.max_retries"="{RETRIES}", "aws.s3.retry_min_delay_ms"="{RETRY_MIN_MS}", "aws.s3.retry_max_delay_ms"="{RETRY_MAX_MS}", "aws.s3.request_timeout_ms"="{REQUEST_TIMEOUT_MS}", "aws.s3.io_timeout_ms"="{IO_TIMEOUT_MS}")"#
    );
    sql.execute(format!(
        "{}{}",
        catalog
            .strip_suffix(')')
            .context("REST catalog SQL layout changed")?,
        retry_properties
    ))?;
    sql.execute(format!("CREATE DATABASE {CATALOG}.managed_objects"))?;
    sql.execute("SET pipeline_dop=1")?;
    let table = format!("{CATALOG}.{SOURCE}");
    sql.execute(format!("CREATE TABLE {table} (id BIGINT) TBLPROPERTIES (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\",\"novarocks.update.mode\"=\"merge-on-read\")"))?;
    sql::expect_update_count(&sql.execute(format!("INSERT INTO {table} VALUES (1)"))?, 1)?;
    let seed = live_paths(&mut sql, &table)?;
    let [first] = seed.as_slice() else {
        anyhow::bail!("native seed did not write exactly one file")
    };
    let (dir, _) = first
        .rsplit_once('/')
        .context("native seed file has no directory")?;
    let prefix = format!("{}/", dir.replacen("s3://", "/", 1));
    let start = fixture.proxy.events().len();
    fixture
        .proxy
        .arm_ack_loss(S3Operation::Put, AckTarget::NextObjectUnder(prefix.clone()))?;
    sql::expect_update_count(
        &sql.execute(format!("INSERT INTO {table} VALUES (2),(3),(4)"))?,
        3,
    )?;
    ensure!(
        sql::integer_column(&sql.execute(format!("SELECT id FROM {table} ORDER BY id"))?)?
            == [1, 2, 3, 4],
        "native lost-ACK INSERT changed the committed row bag"
    );
    let events = fixture.proxy.events();
    let dropped = events[start..]
        .iter()
        .filter(|event| event.downstream_response_dropped)
        .collect::<Vec<_>>();
    let [dropped] = dropped.as_slice() else {
        anyhow::bail!("expected one actual lost native INSERT ACK")
    };
    let path = &dropped.object_path;
    verify_single(
        &events[start..]
            .iter()
            .filter(|event| &event.object_path == path)
            .cloned()
            .collect::<Vec<_>>(),
        true,
    )?;
    let location = format!("s3://{}", path.trim_start_matches('/'));
    ensure!(
        live_paths(&mut sql, &table)?.contains(&location)
            && fixture.rest.object_exists(&location)?,
        "lost-ACK native INSERT object is absent from publication or storage"
    );

    let landed = sql.execute(format!(
        "SELECT _file FROM {table} WHERE id BETWEEN 2 AND 4 GROUP BY _file"
    ))?;
    ensure!(
        landed.rows == [vec![sql::TrinoValue::Text(location)]],
        "the selected native INSERT rows must share the one original lost-ACK file"
    );

    let mut readback_faults = Vec::new();
    for (index, fault) in READBACK_FAULTS.into_iter().enumerate() {
        let start = fixture.proxy.events().len();
        let (path, original_hash) = run_readback_fault(
            fixture,
            context,
            observations,
            runtime,
            &prefix,
            index,
            fault,
        )?;
        await_resource_convergence(context, &baseline, "managed write readback refusal")?;
        readback_faults.push(check_readback_exit(
            fixture,
            runtime,
            start,
            &path,
            original_hash.as_deref(),
            fault,
        )?);
        ensure!(
            sql::integer_column(&sql.execute(format!("SELECT id FROM {table} ORDER BY id"))?)?
                == [1, 2, 3, 4],
            "a refused managed creation changed the committed row bag"
        );
        std::fs::write(
            fixture.root.join("managed-output-readback-faults.json"),
            serde_json::to_vec_pretty(&readback_faults)?,
        )?;
    }

    let before_ctas = fixture.proxy.events().len();
    let selected = observations.take(0)?;
    let actor = WriterActor::start(context, &ctas_sql(FAILED_CTAS), selected.clone(), None)?;
    let terminal = context
        .remaining("observe failed CTAS outcome")
        .and_then(|remaining| {
            actor
                .done
                .recv_timeout(remaining)
                .context("failed CTAS has no actual SQL terminal")
        });
    let outcome = actor.settle(context, terminal)?;
    ensure!(
        matches!(outcome, Err(StatementFailure::Trino(_))),
        "native faulted CTAS did not fail through the SQL owner"
    );
    let fault_hit =
        (0..context.handle().be_count()).try_fold(false, |hit, index| -> Result<bool> {
            Ok(context.handle().selected_query_lifecycle_fault_hit(
                index,
                CTAS_FAULT,
                &selected.token,
            )? || hit)
        })?;
    ensure!(
        fault_hit,
        "failed CTAS did not reach the original root fault"
    );
    await_resource_convergence(context, &baseline, "failed managed CTAS")?;
    let sealed = staged_ctas_objects(fixture, before_ctas)
        .into_iter()
        .map(|path| -> Result<_> {
            let uri = object_uri(&path);
            let digest = object_digest(fixture, runtime, &uri)
                .context("root-prefix failure must retain the sealed CTAS object")?;
            Ok((uri, digest))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        !sealed.is_empty(),
        "faulted CTAS did not create an actual staged object"
    );
    require_absent_table(&mut sql, "failed_ctas")?;

    let before_cancel = fixture.proxy.events().len();
    cancel_ctas(context, observations)?;
    await_resource_convergence(context, &baseline, "canceled managed CTAS")?;
    let canceled_staged = staged_ctas_objects(fixture, before_cancel);
    for path in &canceled_staged {
        ensure!(
            !fixture.rest.object_exists(&object_uri(path))?,
            "canceled pre-seal CTAS left a staged managed object"
        );
    }
    require_absent_table(&mut sql, "canceled_ctas")?;

    let before_dv = fixture.proxy.events().len();
    sql::expect_update_count(&sql.execute(format!("DELETE FROM {table} WHERE id=2"))?, 1)?;
    sql::expect_update_count(&sql.execute(format!("DELETE FROM {table} WHERE id=3"))?, 1)?;
    ensure!(
        sql::integer_column(&sql.execute(format!("SELECT id FROM {table} ORDER BY id"))?)?
            == [1, 4],
        "native DV replacement changed the committed row bag"
    );
    let vectors = sql::integer_column(&sql.execute(format!(
        "SELECT record_count FROM {table}$files WHERE content=1 AND file_format='PUFFIN'"
    ))?)?;
    ensure!(
        vectors == [2],
        "native DV did not publish exactly one current vector with both positions: {vectors:?}"
    );
    let events = fixture.proxy.events();
    let dv_puts = events[before_dv..]
        .iter()
        .filter(|event| {
            event.operation == S3Operation::Put
                && event.condition_if_none_match_star
                && event.object_path.ends_with(".puffin")
                && event
                    .status
                    .is_some_and(|status| (200..300).contains(&status))
                && event.upstream_response_complete
        })
        .collect::<Vec<_>>();
    ensure!(
        dv_puts.len() == 2,
        "each materialization must create its own real conditional Puffin object"
    );
    sql::expect_update_count(&sql.execute(format!("INSERT INTO {table} VALUES (5)"))?, 1)?;
    ensure!(
        sql::integer_column(&sql.execute(format!("SELECT id FROM {table} ORDER BY id"))?)?
            == [1, 4, 5],
        "native follow-up INSERT did not preserve prior committed rows"
    );
    await_resource_convergence(context, &baseline, "managed output native writes")?;
    for (uri, expected) in &sealed {
        ensure!(
            object_digest(fixture, runtime, uri)? == *expected,
            "sealed CTAS bytes changed before original fixture cleanup"
        );
    }

    std::fs::write(
        fixture.root.join("managed-output-native.json"),
        serde_json::to_vec_pretty(
            &json!({"insert_lost_ack":dropped,"ctas_sealed_objects":sealed,"canceled_ctas_cleaned_objects":canceled_staged,"dv_conditional_puts":dv_puts,"final_rows":[1,4,5]}),
        )?,
    )?;
    Ok(())
}
