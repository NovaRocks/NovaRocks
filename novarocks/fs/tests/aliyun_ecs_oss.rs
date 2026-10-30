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

//! Opt-in real ECS/OSS coverage of the original consumer-side signing path.
//! NOVA_OSS_TEST_CONFIG names one absolute, non-secret JSON file containing
//! exactly kind, region, bucket, role_name and root_prefix. No environment
//! credential chain, static material, alternate signer or SDK cache is used.

use std::collections::BTreeSet;
use std::io::Read;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use futures::task::{ArcWake, AtomicWaker};
use novarocks_fs::{
    AliyunEcsRamRoleConfig, AuthorityCapabilityPath, FileCancellation, FileErrorKind, FileIdentity,
    FileReadRange, FsAccessHandle, FsAccessResolver, ObjectStoreAccessContext,
    ObjectStoreEndpointConfig, ObjectStoreProviderPool, ObjectStoreProviderPoolOptions,
    RefreshPolicy, StorageAuthorityId, StorageAuthorityRegistry, TokioFileIoRuntime,
    TokioRefreshExecutor,
};
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
    CatalogNonSecretProperty, CatalogStorageAccessDomainInput, CatalogVersion, ConnectorError,
    ConnectorErrorKind, ConnectorInstanceId, ConnectorIoAdmissionPort, ConnectorIoCleanupFuture,
    ConnectorIoProgressSource, ConnectorIoRequest, ConnectorIoRequestSource,
    ConnectorPayloadRetentionGuard, ConnectorProviderId, ConnectorRequestContext,
    ConnectorStopOwner, CredentialConsumerRole, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, NodeLocalCredentialReference, SharedDependency,
    StorageCredentialScopePrefix,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const PAYLOAD: &[u8] = b"NovaRocks real ECS -> OSS known bytes\n\x00\x01\x7f\x80\xfe\xff";
const MULTIPART_COMPLETE: &str = "multipart-complete.bin";
const MULTIPART_ABORT: &str = "multipart-abort.bin";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    kind: String,
    region: String,
    bucket: String,
    role_name: String,
    root_prefix: String,
}

fn configuration() -> Result<Config, String> {
    let path = PathBuf::from(
        std::env::var_os("NOVA_OSS_TEST_CONFIG")
            .ok_or("NOVA_OSS_TEST_CONFIG must name an explicit configuration file")?,
    );
    if !path.is_absolute() {
        return Err("NOVA_OSS_TEST_CONFIG must be absolute".into());
    }
    let file = std::fs::File::open(path).map_err(|_| "open explicit OSS test configuration")?;
    if !file
        .metadata()
        .map_err(|_| "stat OSS configuration")?
        .is_file()
    {
        return Err("OSS test configuration must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(16_385)
        .read_to_end(&mut bytes)
        .map_err(|_| "read explicit OSS test configuration")?;
    if bytes.len() > 16_384 {
        return Err("OSS test configuration exceeds its byte bound".into());
    }
    let config: Config = serde_json::from_slice(&bytes)
        .map_err(|_| "OSS test configuration must have exactly five non-secret fields")?;
    if config.kind != "aliyun-oss"
        || config.region.is_empty()
        || !config
            .region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || config.root_prefix.is_empty()
        || config.root_prefix.starts_with('/')
        || !config.root_prefix.ends_with('/')
    {
        return Err("invalid explicit OSS test configuration".into());
    }
    Ok(config)
}

fn unsupported() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "the real OSS probe owns only its finite network I/O phase",
    )
}

struct WakeDriver(Arc<AtomicWaker>);
impl ArcWake for WakeDriver {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.wake();
    }
}

/// A test-owned finite I/O observation cell, not a production WC owner. It
/// drives the original authority progress while the real request is pending;
/// neither a detached task nor a replacement credential future is installed.
struct ProbeIoCell {
    allowed: BTreeSet<String>,
    observed: Mutex<BTreeSet<String>>,
    progress: Mutex<Vec<Arc<dyn ConnectorIoProgressSource>>>,
    progress_failed: AtomicUsize,
    requests: AtomicUsize,
    wake: Arc<AtomicWaker>,
    exit_waker: Waker,
}

impl ProbeIoCell {
    fn new(endpoint: &str) -> Arc<Self> {
        let wake = Arc::new(AtomicWaker::new());
        Arc::new(Self {
            allowed: BTreeSet::from([endpoint.to_owned(), "http://100.100.100.200".into()]),
            observed: Mutex::new(BTreeSet::new()),
            progress: Mutex::new(Vec::new()),
            progress_failed: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            exit_waker: futures::task::waker(Arc::new(WakeDriver(Arc::clone(&wake)))),
            wake,
        })
    }

    fn include(&self, dependency: &SharedDependency) -> Result<(), ConnectorError> {
        let SharedDependency::Network(origin) = dependency else {
            return Err(unsupported());
        };
        if !self.allowed.contains(origin.as_str()) {
            return Err(unsupported());
        }
        self.observed
            .lock()
            .unwrap()
            .insert(origin.as_str().to_owned());
        Ok(())
    }

    fn poll_progress(&self, cx: &mut Context<'_>) {
        self.wake.register(cx.waker());
        let sources = self.progress.lock().unwrap().clone();
        for source in sources {
            if matches!(source.poll_original_progress(cx), Poll::Ready(Err(_))) {
                self.progress_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.progress
            .lock()
            .unwrap()
            .retain(|source| !source.actual_exit_observed());
    }

    async fn drive(&self) {
        std::future::poll_fn(|cx| {
            self.poll_progress(cx);
            Poll::<()>::Pending
        })
        .await
    }
}

impl ConnectorIoRequestSource for ProbeIoCell {
    fn exit_waker(&self) -> Waker {
        self.exit_waker.clone()
    }
    fn reserve_stream(&self) -> Result<(), ConnectorError> {
        Err(unsupported())
    }
    fn stream_closed(&self) {
        self.wake.wake();
    }
    fn install_stream_cleanup_in(
        &self,
        _: &mut Option<ConnectorIoCleanupFuture>,
    ) -> Result<(), ConnectorError> {
        Err(unsupported())
    }
    fn stream_custody_lost(&self) {
        self.progress_failed.fetch_add(1, Ordering::Relaxed);
        self.wake.wake();
    }
    fn record_cleanup_failure(
        &self,
        original: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Box<dyn std::any::Any + Send>> {
        self.progress_failed.fetch_add(1, Ordering::Relaxed);
        Err(original)
    }
    fn attach_original_progress(
        &self,
        source: Arc<dyn ConnectorIoProgressSource>,
    ) -> Result<(), ConnectorError> {
        let mut progress = self.progress.lock().unwrap();
        if !progress.iter().any(|old| Arc::ptr_eq(old, &source)) {
            progress.push(source);
        }
        drop(progress);
        self.wake.wake();
        Ok(())
    }
    fn poll_admitted(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        Poll::Ready(Ok(()))
    }
    fn poll_include_dependency(
        &self,
        dependency: &SharedDependency,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), ConnectorError>> {
        Poll::Ready(self.include(dependency))
    }
}

struct ProbeIoPort(Arc<ProbeIoCell>);
impl ConnectorIoAdmissionPort for ProbeIoPort {
    fn request_owned(&self, _: NonZeroU64) -> Result<ConnectorIoRequest, ConnectorError> {
        Err(unsupported())
    }
    fn request(&self, dependency: SharedDependency) -> Result<ConnectorIoRequest, ConnectorError> {
        self.0.include(&dependency)?;
        if self.0.requests.fetch_add(1, Ordering::Relaxed) >= 32 {
            return Err(unsupported());
        }
        Ok(ConnectorIoRequest::from_original(
            Arc::clone(&self.0) as Arc<dyn ConnectorIoRequestSource>
        ))
    }
    fn publication_obligation_started(&self) {}
    fn retain_payload(&self) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        Err(unsupported())
    }
}

async fn probe(
    access: &FsAccessHandle,
    uri: &str,
    directory: &str,
    cancellation: &FileCancellation,
    work_deadline: Instant,
    deadline: Instant,
) -> Result<(), String> {
    let file = access
        .bind(0, FileIdentity::new(uri, PAYLOAD.len() as u64, None))
        .map_err(|_| "bind the exact probe object")?;
    let operator = access.operator();
    let key = file.operator_relative_path();
    let mut confirmed_written = false;
    let mut put_dispatched = false;
    let work = async {
        match file.stat(cancellation).await {
            Err(error) if error.kind() == FileErrorKind::NotFound => {}
            Err(error) => return Err(format!("preflight FileIO stat failed ({:?})", error.kind())),
            Ok(_) => return Err("nonce object already exists; refusing to write".into()),
        }
        put_dispatched = true;
        operator
            .write(key, PAYLOAD)
            .await
            .map_err(|error| format!("OSS put failed ({:?})", error.kind()))?;
        confirmed_written = true;
        let actual = file
            .read(FileReadRange::WholeFile, cancellation)
            .await
            .map_err(|error| format!("FileIO read failed ({:?})", error.kind()))?;
        if actual.as_ref() != PAYLOAD {
            return Err("read bytes differ from the independent fixed oracle".into());
        }
        let range = file
            .read(
                FileReadRange::Bounded {
                    offset: 3,
                    length: 17,
                },
                cancellation,
            )
            .await
            .map_err(|error| format!("FileIO range read failed ({:?})", error.kind()))?;
        if range.as_ref() != &PAYLOAD[3..20] {
            return Err("range bytes differ from the independent fixed oracle".into());
        }
        let size = file
            .stat(cancellation)
            .await
            .map_err(|error| format!("FileIO stat failed ({:?})", error.kind()))?;
        if size != PAYLOAD.len() as u64 {
            return Err("stat size differs from the fixed oracle".into());
        }
        let mut entries = access
            .list_location(directory, false, cancellation)
            .await
            .map_err(|error| format!("FileIO list failed ({:?})", error.kind()))?;
        let entry = entries
            .try_next()
            .await
            .map_err(|_| "read FileIO list entry")?
            .ok_or("written probe is absent from its nonce listing")?;
        let (listed_uri, listed_size, is_dir) = entry.into_parts();
        if listed_uri != uri
            || listed_size != PAYLOAD.len() as u64
            || is_dir
            || entries
                .try_next()
                .await
                .map_err(|_| "finish nonce listing")?
                .is_some()
        {
            return Err("nonce listing differs from the one known object".into());
        }
        Ok::<(), String>(())
    };
    let work = tokio::time::timeout_at(tokio::time::Instant::from_std(work_deadline), work)
        .await
        .unwrap_or_else(|_| Err("probe work deadline elapsed".into()));
    // An unknown PUT is retained. Never infer ownership from a listing, and
    // never recursively delete a prefix or an object the test did not write.
    if confirmed_written {
        let cleanup = async {
            operator
                .delete(key)
                .await
                .map_err(|error| format!("exact probe delete failed ({:?})", error.kind()))?;
            match file.stat(cancellation).await {
                Err(error) if error.kind() == FileErrorKind::NotFound => Ok(()),
                _ => Err("deleted probe was not confirmed absent".to_owned()),
            }
        };
        let cleanup = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), cleanup)
            .await
            .unwrap_or_else(|_| Err("exact probe cleanup deadline elapsed".into()));
        if let Err(cleanup) = cleanup {
            return Err(format!(
                "{}; {cleanup}; retained probe URI: {uri}",
                work.err().unwrap_or_else(|| "oracle checks passed".into())
            ));
        }
    } else if put_dispatched {
        return Err(format!(
            "{}; unconfirmed PUT retained at probe URI: {uri}",
            work.err().unwrap_or_else(|| "PUT was not confirmed".into())
        ));
    }
    work
}

// OpenDAL 0.58.1 S3 advertises a 5 MiB multipart minimum. With an
// explicit exact chunk, WriteGenerator keeps its current chunk buffered and
// MultipartWriter keeps one previous chunk. Two full writes + close therefore
// initiate, upload two parts, and complete. A third nonempty write flushes the
// second chunk into MultipartWriter and awaits initiate + first part before
// abort. concurrent(1) executes each part inline, with no detached part task.
// See opendal-core types/context/write.rs and raw/oio/write/multipart_write.rs;
// opendal-service-s3 writer.rs requires HTTP 204 from abort_part.
async fn multipart_probe(
    operator: &opendal::Operator,
    access: &FsAccessHandle,
    directory: &str,
    io: &ProbeIoCell,
    cancellation: &FileCancellation,
    work_deadline: Instant,
    deadline: Instant,
    abort: bool,
) -> Result<(), String> {
    const PART: usize = 5 * 1024 * 1024;
    let uri = format!(
        "{directory}{}",
        if abort {
            MULTIPART_ABORT
        } else {
            MULTIPART_COMPLETE
        }
    );
    let file = access
        .bind_location(&uri, FileIdentity::new(&uri, (2 * PART) as u64, None))
        .map_err(|_| "bind the exact multipart probe")?;
    let key = file.operator_relative_path();
    let mut writer: Option<opendal::Writer> = None;
    let mut attempted = false;
    let mut completed = false;
    let mut aborted = false;
    eprintln!("Real ECS/OSS multipart probe URI: {uri}");
    let work = async {
        match file.stat(cancellation).await {
            Err(error) if error.kind() == FileErrorKind::NotFound => {}
            Err(error) => return Err(format!("multipart preflight failed ({:?})", error.kind())),
            Ok(_) => return Err("multipart nonce already exists; refusing to write".into()),
        }
        let before = io.requests.load(Ordering::Relaxed);
        writer = Some(
            operator
                .writer_with(key)
                .chunk(PART)
                .concurrent(1)
                .await
                .map_err(|error| format!("create multipart writer failed ({:?})", error.kind()))?,
        );
        let writer = writer.as_mut().expect("writer just installed");
        attempted = true;
        writer
            .write(bytes::Bytes::from(vec![0x36; PART]))
            .await
            .map_err(|error| format!("write first multipart chunk failed ({:?})", error.kind()))?;
        writer
            .write(bytes::Bytes::from(vec![0xa9; PART]))
            .await
            .map_err(|error| format!("write second multipart chunk failed ({:?})", error.kind()))?;
        if abort {
            writer
                .write(bytes::Bytes::from_static(b"pending-tail-never-committed"))
                .await
                .map_err(|error| format!("trigger multipart part failed ({:?})", error.kind()))?;
            let uploaded = io.requests.load(Ordering::Relaxed);
            if uploaded.saturating_sub(before) < 2 {
                return Err("multipart initiate and first uploaded part were not observed".into());
            }
            writer
                .abort()
                .await
                .map_err(|error| format!("real multipart abort failed ({:?})", error.kind()))?;
            if io.requests.load(Ordering::Relaxed) <= uploaded {
                return Err("multipart abort performed no HTTP request".into());
            }
            aborted = true;
            match file.stat(cancellation).await {
                Err(error) if error.kind() == FileErrorKind::NotFound => {}
                _ => return Err("aborted multipart object was not confirmed absent".into()),
            }
            eprintln!(
                "Real multipart abort confirmed: initiate + uploaded part + abort HTTP; {uri}"
            );
        } else {
            writer
                .close()
                .await
                .map_err(|error| format!("complete multipart write failed ({:?})", error.kind()))?;
            completed = true;
            if io.requests.load(Ordering::Relaxed).saturating_sub(before) < 4 {
                return Err(
                    "multipart initiate, two uploaded parts and complete were not observed".into(),
                );
            }
            let actual = file
                .read(FileReadRange::WholeFile, cancellation)
                .await
                .map_err(|error| format!("read completed multipart failed ({:?})", error.kind()))?;
            if actual.len() != 2 * PART
                || actual[..PART].iter().any(|byte| *byte != 0x36)
                || actual[PART..].iter().any(|byte| *byte != 0xa9)
            {
                return Err(
                    "completed multipart differs from the independent two-part byte oracle".into(),
                );
            }
            if file
                .stat(cancellation)
                .await
                .map_err(|_| "stat completed multipart")?
                != (2 * PART) as u64
            {
                return Err("completed multipart size differs from its fixed oracle".into());
            }
            eprintln!(
                "Real multipart completion confirmed: two distinct 5 MiB parts, exact byte oracle; {uri}"
            );
        }
        Ok::<(), String>(())
    };
    let work = tokio::time::timeout_at(tokio::time::Instant::from_std(work_deadline), work)
        .await
        .unwrap_or_else(|_| Err("multipart work deadline elapsed".into()));
    let cleanup = async {
        if completed {
            // Only a successful complete ACK grants this exact object's delete.
            operator.delete(key).await.map_err(|error| {
                format!("delete completed multipart failed ({:?})", error.kind())
            })?;
            match file.stat(cancellation).await {
                Err(error) if error.kind() == FileErrorKind::NotFound => {}
                _ => return Err("completed multipart delete was not confirmed absent".into()),
            }
        } else if attempted && !aborted {
            // Keep the original writer/upload identity through cleanup, even
            // after timeout. Unknown initiate/complete outcomes stay failures.
            let before = io.requests.load(Ordering::Relaxed);
            writer
                .as_mut()
                .expect("attempted writer remains owned")
                .abort()
                .await
                .map_err(|error| format!("settle failed multipart abort ({:?})", error.kind()))?;
            if io.requests.load(Ordering::Relaxed) <= before {
                return Err("failed multipart had no confirmed upload identity to abort".into());
            }
            match file.stat(cancellation).await {
                Err(error) if error.kind() == FileErrorKind::NotFound => {}
                _ => return Err("failed multipart object remains or is unobserved".into()),
            }
        }
        Ok::<(), String>(())
    };
    let cleanup = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), cleanup)
        .await
        .unwrap_or_else(|_| Err("multipart cleanup deadline elapsed".into()));
    match (work, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (work, cleanup) => Err(format!(
            "{}; {}; exact multipart probe URI retained for diagnosis: {uri}",
            work.err()
                .unwrap_or_else(|| "multipart oracle passed".into()),
            cleanup
                .err()
                .unwrap_or_else(|| "owned multipart cleanup confirmed".into())
        )),
    }
}

#[test]
#[ignore = "requires an ECS RAM role and explicit NOVA_OSS_TEST_CONFIG; uses only exact nonce probe objects"]
fn real_ecs_authority_signs_oss_fileio_roundtrip() -> Result<(), String> {
    let config = configuration()?;
    let root = format!("s3://{}/{}", config.bucket, config.root_prefix);
    let provider = AliyunEcsRamRoleConfig::try_new(&config.role_name, vec![root.clone()])
        .map_err(|_| "invalid explicit ECS role or storage scope")?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before Unix epoch")?
        .as_nanos();
    let directory = format!("{root}g19-rust-ecs-oss/{}-{stamp}/", std::process::id());
    let uri = format!("{directory}oracle.bin");
    provider
        .scope_for_locations(&[
            uri.clone(),
            format!("{directory}{MULTIPART_COMPLETE}"),
            format!("{directory}{MULTIPART_ABORT}"),
        ])
        .map_err(|_| "a nonce probe leaves the explicit configured scope")?;
    // The deployment definition is validated for all three exact targets, but
    // this test authority can sign only inside its own nonce directory.
    let scope = StorageCredentialScopePrefix::try_from_normalized(&directory)
        .map_err(|_| "invalid exact nonce directory scope")?;
    let endpoint = format!("https://s3.oss-{}-internal.aliyuncs.com", config.region);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| "construct explicit test I/O runtime")?;
    let handle = runtime.handle().clone();
    // Definition changes get another generation; credential rotation does not.
    let mut definition_hash = Sha256::new();
    for field in [
        &config.kind,
        &config.region,
        &config.bucket,
        &config.role_name,
        &config.root_prefix,
    ] {
        definition_hash.update((field.len() as u64).to_be_bytes());
        definition_hash.update(field.as_bytes());
    }
    let definition_hash: [u8; 32] = definition_hash.finalize().into();
    let generation = format!(
        "v-{}",
        definition_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let reference = NodeLocalCredentialReference::try_new("ecs-oss-live-probe", &generation)
        .map_err(|_| "construct node-local reference")?;
    let catalog = ConnectorInstanceId::parse("ecs-oss-live-probe").map_err(|_| "probe catalog")?;
    let domain = CatalogStorageAccessDomainInput::try_new(
        ConnectorProviderId::parse("iceberg").map_err(|_| "probe provider identity")?,
        catalog.clone(),
        1,
        vec![
            CatalogNonSecretProperty::try_new("aws.s3.endpoint", &endpoint)
                .map_err(|_| "probe endpoint fact")?,
        ],
        CatalogCredentialBinding::try_new(
            CatalogCredentialPurpose::ObjectStoreData,
            CredentialConsumerRole::Backend,
            CatalogCredentialMode::NodeLocal(reference.clone()),
        )
        .map_err(|_| "probe credential binding")?,
        vec![],
    )
    .map_err(|_| "derive probe access domain")?
    .derive_access_domain();
    let registry = StorageAuthorityRegistry::with_default_options(
        TokioRefreshExecutor::shared(handle.clone()),
        RefreshPolicy::default(),
    );
    let id = StorageAuthorityId::new(
        CatalogHandle::new(catalog, CatalogVersion::from_bytes(definition_hash)),
        scope,
        AuthorityCapabilityPath::NodeLocalProvider {
            purpose: CatalogCredentialPurpose::ObjectStoreData,
            principal: reference,
            provider: provider.clone(),
        },
    );
    let authority = registry.authority(&id, Instant::now(), || {
        (
            provider.material_source(Arc::new(TokioFileIoRuntime::new(handle.clone()))),
            None,
        )
    });
    let again = registry.authority(&id, Instant::now(), || {
        panic!("exact authority must be reused")
    });
    if !Arc::ptr_eq(&authority, &again) {
        return Err("registry replaced the original authority".into());
    }
    let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())
        .map_err(|_| "construct explicit object-store provider pool")?;
    let io = ProbeIoCell::new(&endpoint);
    let io_port: Arc<dyn ConnectorIoAdmissionPort> = Arc::new(ProbeIoPort(Arc::clone(&io)));
    let stop = ConnectorStopOwner::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    let request = ConnectorRequestContext::try_new(
        deadline,
        stop.view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|_| "construct original FileIO request context")?
    .with_io_admission(Arc::clone(&io_port));
    let cancellation = FileCancellation::from_connector_request(&request);
    let access = FsAccessResolver::new()
        .resolve_location(
            domain,
            &uri,
            Some(ObjectStoreAccessContext::for_authority(
                ObjectStoreEndpointConfig {
                    endpoint: endpoint.clone(),
                    enable_path_style_access: Some(false),
                    region: Some(config.region),
                    retry_max_times: Some(0),
                    retry_min_delay_ms: None,
                    retry_max_delay_ms: None,
                    timeout_ms: Some(20_000),
                    io_timeout_ms: Some(20_000),
                },
                Arc::clone(&authority),
                &pool,
            )),
        )
        .map_err(|_| "resolve original authorized FileIO handle")?
        .with_request_context(request);
    eprintln!("Real ECS/OSS probe URI: {uri}");
    let result = runtime.block_on(async {
        let operator = access.operator();
        let operation = Box::pin(async {
            let work_deadline = deadline - Duration::from_secs(20);
            probe(
                &access,
                &uri,
                &directory,
                &cancellation,
                work_deadline,
                deadline,
            )
            .await?;
            multipart_probe(
                &operator,
                &access,
                &directory,
                &io,
                &cancellation,
                work_deadline,
                deadline,
                false,
            )
            .await?;
            multipart_probe(
                &operator,
                &access,
                &directory,
                &io,
                &cancellation,
                work_deadline,
                deadline,
                true,
            )
            .await
        });
        match futures::future::select(operation, Box::pin(io.drive())).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(_) => {
                unreachable!("original progress driver remains pending")
            }
        }
    });
    if let Err(error) = result {
        return Err(format!(
            "{error}; authority metrics: {:?}; observed service origins: {:?}",
            authority.metrics(),
            io.observed.lock().unwrap()
        ));
    }
    if *io.observed.lock().unwrap() != io.allowed {
        return Err("FileIO did not observe both exact OSS and IMDS service origins".into());
    }
    if io.progress_failed.load(Ordering::Relaxed) != 0 {
        return Err("original authority progress reported a failure".into());
    }
    let metrics = authority.metrics();
    if metrics.refreshes_applied < 1 || metrics.cache_hits < 1 {
        return Err("the original authority did not acquire and reuse ECS material".into());
    }
    Ok(())
}
