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

mod rest;

use super::*;
use crate::catalog_control::add_files::tests::fixtures::SourceHttpFixture;
use crate::catalog_control::data_mutation::tests::exact_provider_with_table;
use crate::iceberg::spec::{
    DataFile, FormatVersion, NestedField, PrimitiveType, TableProperties, Type,
};
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{
    ConnectorAddFilesRecursion, ConnectorPayloadByteBudget, ConnectorPayloadRetentionGuard,
    ConnectorRequestInitiation, ConnectorStopOwner, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct Budget {
    cap: usize,
    held: Arc<AtomicUsize>,
}

impl Budget {
    fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap,
            held: Arc::default(),
        })
    }
}

struct Receipt(Arc<AtomicUsize>, usize);

impl Drop for Receipt {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::SeqCst);
    }
}

impl ConnectorPayloadByteBudget for Budget {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes).filter(|total| *total <= self.cap)
            })
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "original statement payload budget exhausted",
                )
            })?;
        Ok(ConnectorPayloadRetentionGuard::new(Receipt(
            Arc::clone(&self.held),
            bytes,
        )))
    }
}

fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(120),
        stop.view(),
        64 * 1024,
        256 * 1024,
    )
    .unwrap()
    .with_initiation(ConnectorRequestInitiation::Statement)
}

fn read_table(provider: &IcebergMetadata) -> crate::iceberg::table::Table {
    provider
        .runtime()
        .control_state()
        .invalidate_table_cache("db", "t");
    provider
        .runtime()
        .load_table_for_request("db", "t", &table_context())
        .unwrap()
        .into_table()
}

fn request(
    adapter: &IcebergDataMutationAdapter,
    provider: &IcebergMetadata,
    location: impl Into<Arc<str>>,
    context: ConnectorRequestContext,
    budget: &Arc<Budget>,
) -> ConnectorDataMutationExecuteRequest {
    let table = provider
        .load_table(
            ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            },
            &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            ),
        )
        .unwrap();
    let plan = adapter
        .plan_mutation(
            ConnectorDataMutationPlanningRequest::try_new(
                ConnectorMutationOperationId::new(),
                adapter.binding_key().clone(),
                ConnectorDataMutationOperation::register_existing_files(
                    table.table,
                    Some(location.into()),
                    Some(ConnectorAddFilesFormat::Parquet),
                    ConnectorAddFilesRecursion::Fail,
                ),
                context.clone(),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(plan.summary(), ConnectorDataMutationPlanSummary::default());
    ConnectorDataMutationExecuteRequest::try_new(plan, context)
        .unwrap()
        .with_registration_byte_author(ConnectorPayloadRetentionGuard::from_byte_budget(
            Arc::clone(budget),
        ))
        .unwrap()
}

fn external_file(
    path: &Path,
    values: Vec<Option<i64>>,
    embedded_id: Option<i32>,
    properties: Option<WriterProperties>,
) -> PathBuf {
    let mut field = Field::new("value", DataType::Int64, true);
    if let Some(id) = embedded_id {
        field = field.with_metadata(HashMap::from([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )]));
    }
    let schema = Arc::new(ArrowSchema::new(vec![field]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values)) as ArrayRef],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, properties).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path.to_path_buf()
}

fn execute(
    provider: &Arc<IcebergMetadata>,
    location: impl Into<Arc<str>>,
    budget: &Arc<Budget>,
) -> ExternalMutationOutcome<ConnectorDataMutationReceipt> {
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(provider)).unwrap();
    let req = request(
        &adapter,
        provider,
        location,
        context(&ConnectorStopOwner::new()),
        budget,
    );
    adapter.execute(req).unwrap()
}

fn assert_failure(
    outcome: ExternalMutationOutcome<ConnectorDataMutationReceipt>,
    reason: novarocks_failure_contract::FailureReason,
    detail: &str,
) {
    let ExternalMutationOutcome::KnownUncommitted {
        failure,
        cleanup: ExternalMutationFinalization::Complete,
    } = outcome
    else {
        panic!("expected known-uncommitted registration, got {outcome:?}");
    };
    assert_eq!(failure.failure_fact().reason(), reason, "{failure}");
    assert_eq!(failure.connector_source().unwrap().message(), detail);
}

fn assert_committed(
    outcome: ExternalMutationOutcome<ConnectorDataMutationReceipt>,
    count: u32,
    rows: u64,
    bytes: u64,
) -> ConnectorDataMutationReceipt {
    let ExternalMutationOutcome::KnownCommitted {
        receipt,
        effect: ExternalMutationEffect::Applied,
        finalization: ExternalMutationFinalization::Complete,
        ..
    } = outcome
    else {
        panic!("expected committed registration, got {outcome:?}");
    };
    assert_eq!(
        receipt.summary(),
        ConnectorDataMutationPlanSummary::new(count, rows, bytes)
    );
    receipt
}

fn live_files(
    runtime: &tokio::runtime::Runtime,
    table: &crate::iceberg::table::Table,
) -> Vec<DataFile> {
    runtime.block_on(async {
        use crate::commit::manifest_sources::SnapshotSources;
        let source = table.metadata();
        let snapshot = source.current_snapshot().unwrap();
        let sources = SnapshotSources::load(source, snapshot, table.file_io(), || Ok(()))
            .await
            .unwrap();
        let mut files = Vec::new();
        sources
            .fold(
                table.file_io(),
                || Ok(()),
                |descriptor| descriptor.content == crate::iceberg::spec::ManifestContentType::Data,
                |entry| {
                    if entry.raw.is_alive() {
                        files.push(entry.raw.data_file().clone());
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
        files
    })
}

#[test]
fn registration_publishes_real_footer_metrics_and_preserves_external_data() {
    for version in [FormatVersion::V1, FormatVersion::V2] {
        let (runtime, _warehouse, provider) = exact_provider_with_table(
            version,
            NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)),
            vec![],
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let file = external_file(
            &dir.path().join("data.file"),
            vec![Some(7), None, Some(23)],
            Some(1),
            None,
        );
        let original = std::fs::read(&file).unwrap();
        let location = format!("file://{}", file.display());
        let budget = Budget::new(8 * 1024 * 1024);
        assert_committed(
            execute(&provider, location.clone(), &budget),
            1,
            3,
            original.len() as u64,
        );
        assert_eq!(budget.held.load(Ordering::SeqCst), 0);
        let table = read_table(&provider);
        assert_eq!(table.metadata().format_version(), version);
        let [registered]: [DataFile; 1] = live_files(&runtime, &table).try_into().unwrap();
        assert_eq!(registered.file_path(), location);
        assert_eq!(registered.null_value_counts()[&1], 1);
        assert_eq!(registered.value_counts()[&1], 3);
        assert_eq!(
            registered.lower_bounds()[&1],
            crate::iceberg::spec::Datum::long(7)
        );
        assert_eq!(
            registered.upper_bounds()[&1],
            crate::iceberg::spec::Datum::long(23)
        );
        assert_eq!(registered.record_count(), 3);
        assert_failure(
            execute(&provider, location.clone(), &budget),
            connector_reason::ICEBERG_COMMIT_ERROR,
            &format!("Failed to add files: File already exists: {location}"),
        );
        assert_eq!(budget.held.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read(&file).unwrap(), original);
        assert_eq!(
            read_table(&provider).metadata().current_snapshot_id(),
            table.metadata().current_snapshot_id()
        );
    }
}

#[test]
fn registration_mapping_and_empty_append_publish_through_hadoop() {
    let (runtime, _warehouse, provider) = exact_provider_with_table(
        FormatVersion::V2,
        NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)),
        vec![],
        None,
    );
    let dir = tempfile::tempdir().unwrap();
    let budget = Budget::new(8 * 1024 * 1024);
    assert_committed(
        execute(
            &provider,
            format!("file://{}", dir.path().display()),
            &budget,
        ),
        0,
        0,
        0,
    );
    let empty = read_table(&provider);
    assert!(live_files(&runtime, &empty).is_empty());
    let property = TableProperties::PROPERTY_DEFAULT_NAME_MAPPING;
    assert!(!empty.metadata().properties().contains_key(property));

    let file = external_file(
        &dir.path().join("without-id.parquet"),
        vec![Some(7)],
        None,
        None,
    );
    assert_committed(
        execute(&provider, format!("file://{}", file.display()), &budget),
        1,
        1,
        std::fs::metadata(&file).unwrap().len(),
    );
    let bound = read_table(&provider);
    assert_eq!(
        bound
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id(),
        empty.metadata().current_snapshot_id()
    );
    let mapping =
        crate::schema_mapping::parse_name_mapping(&bound.metadata().properties()[property])
            .unwrap();
    assert_eq!(
        crate::schema_mapping::mapped_path_segments(&mapping, ["value"].into_iter()).unwrap(),
        Some(1)
    );
    assert_eq!(
        live_files(&runtime, &bound)[0].lower_bounds()[&1],
        crate::iceberg::spec::Datum::long(7)
    );
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
}

#[test]
fn bad_footer_precedes_required_column_and_budget_refuses_without_publication() {
    let (_runtime, _warehouse, provider) = exact_provider_with_table(
        FormatVersion::V2,
        NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)),
        vec![],
        None,
    );
    let dir = tempfile::tempdir().unwrap();
    let nullable = external_file(
        &dir.path().join("a.parquet"),
        vec![Some(1), None],
        Some(1),
        None,
    );
    let invalid = dir.path().join("z.not-parquet");
    std::fs::write(&invalid, b"not a Parquet file").unwrap();
    let generous = Budget::new(8 * 1024 * 1024);
    assert_failure(
        execute(
            &provider,
            format!("file://{}", dir.path().display()),
            &generous,
        ),
        connector_reason::ICEBERG_COMMIT_ERROR,
        &format!(
            "Failed to add files: Failed to read file footer: file://{}",
            invalid.display()
        ),
    );
    assert_failure(
        execute(
            &provider,
            format!("file://{}", nullable.display()),
            &generous,
        ),
        connector_reason::NOT_NULL_VIOLATION,
        "NULL value not allowed for NOT NULL column: value",
    );
    assert_eq!(generous.held.load(Ordering::SeqCst), 0);

    let tight = Budget::new(1);
    let outcome = execute(&provider, format!("file://{}", nullable.display()), &tight);
    assert!(
        matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, .. }
        if failure.kind() == ConnectorMutationFailureKind::ResourceExhausted)
    );
    assert_eq!(tight.held.load(Ordering::SeqCst), 0);
    assert!(
        read_table(&provider)
            .metadata()
            .current_snapshot()
            .is_none()
    );
    assert!(invalid.exists() && nullable.exists());
}

fn large_footer_properties() -> WriterProperties {
    let padding = parquet::file::metadata::KeyValue {
        key: "fixture_padding".into(),
        value: Some("x".repeat(9 * 1024 * 1024)),
    };
    WriterProperties::builder()
        .set_key_value_metadata(Some(vec![padding]))
        .build()
}

#[test]
fn registration_accepts_more_than_the_retired_file_and_footer_extents() {
    let (runtime, _warehouse, provider) = exact_provider_with_table(
        FormatVersion::V2,
        NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)),
        vec![],
        None,
    );
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("candidates");
    std::fs::create_dir(&source).unwrap();
    let small = external_file(
        &dir.path().join("small.parquet"),
        vec![Some(7)],
        Some(1),
        None,
    );
    let big = external_file(
        &dir.path().join("big.parquet"),
        vec![Some(8)],
        Some(1),
        Some(large_footer_properties()),
    );
    for n in 0..4097 {
        std::fs::hard_link(&small, source.join(format!("small-{n:05}"))).unwrap();
    }
    for n in 0..8 {
        std::fs::hard_link(&big, source.join(format!("large-{n}"))).unwrap();
    }
    let bytes =
        4097 * std::fs::metadata(small).unwrap().len() + 8 * std::fs::metadata(big).unwrap().len();
    let budget = Budget::new(64 * 1024 * 1024);
    assert_committed(
        execute(&provider, format!("file://{}", source.display()), &budget),
        4105,
        4105,
        bytes,
    );
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
    assert_eq!(live_files(&runtime, &read_table(&provider)).len(), 4105);
    assert_eq!(std::fs::read_dir(source).unwrap().count(), 4105);
}

#[test]
fn cancellation_during_source_stat_keeps_the_original_until_actual_exit() {
    let dir = tempfile::tempdir().unwrap();
    let local = write_external_parquet(dir.path(), vec![7, 8]);
    let source = SourceHttpFixture::new(std::fs::read(local).unwrap());
    let target = rest::RestTarget::with_s3(source.config());
    let provider = Arc::clone(&target.provider);
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
    let stop = ConnectorStopOwner::new();
    let budget = Budget::new(8 * 1024 * 1024);
    let req = request(
        &adapter,
        &provider,
        "s3://source-bucket/incoming/a.parquet",
        context(&stop),
        &budget,
    );
    source.arm_stat();
    std::thread::scope(|scope| {
        let (sent, observed) = std::sync::mpsc::channel();
        let running = scope.spawn(move || sent.send(adapter.execute(req)).unwrap());
        source.wait_for_tail();
        stop.request_stop();
        let before_release = observed.recv_timeout(Duration::from_millis(100));
        source.release_tail();
        assert!(matches!(
            before_release,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        running.join().unwrap();
        let outcome = observed.recv().unwrap().unwrap();
        assert!(
            matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, .. }
            if failure.kind() == ConnectorMutationFailureKind::Cancelled)
        );
    });
    let requests = source.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "HEAD"
                && request.target.ends_with("/incoming/a.parquet"))
            .count(),
        1
    );
    assert!(requests.iter().all(|request| request.range.is_none()));
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
    assert!(
        read_table(&provider)
            .metadata()
            .current_snapshot()
            .is_none()
    );
}

#[test]
fn registration_uses_the_planned_version_gate_during_a_concurrent_upgrade() {
    let (runtime, _warehouse, provider) = exact_provider_with_empty_table();
    let dir = tempfile::tempdir().unwrap();
    let input = write_external_parquet(dir.path(), vec![1, 3]);
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
    let budget = Budget::new(8 * 1024 * 1024);
    let source = format!("file://{}", input.display());
    let req = request(
        &adapter,
        &provider,
        source,
        context(&ConnectorStopOwner::new()),
        &budget,
    );
    let table = read_table(&provider);
    let version = table
        .metadata()
        .clone()
        .into_builder(table.metadata_location().map(str::to_owned))
        .upgrade_format_version(FormatVersion::V3)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    install_competing_metadata(&table, &version);
    assert_committed(
        adapter.execute(req).unwrap(),
        1,
        2,
        std::fs::metadata(input).unwrap().len(),
    );
    let updated = read_table(&provider);
    assert_eq!(updated.metadata().format_version(), FormatVersion::V3);
    assert_eq!(live_files(&runtime, &updated)[0].record_count(), 2);
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
}

fn install_competing_metadata(table: &crate::iceberg::table::Table, metadata: &TableMetadata) {
    use std::io::Write as _;
    let path = table
        .metadata_location()
        .expect("competing Hadoop metadata predecessor");
    let path = Path::new(path.strip_prefix("file://").unwrap_or(path));
    assert_eq!(path.file_name().unwrap(), "v1.metadata.json");
    let directory = path.parent().unwrap();
    let mut next = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join("v2.metadata.json"))
        .unwrap();
    next.write_all(&serde_json::to_vec(metadata).unwrap())
        .unwrap();
    next.sync_all().unwrap();
    std::fs::write(directory.join("version-hint.text"), "2\n").unwrap();
}

struct FooterDrainGate {
    ready: std::sync::mpsc::Sender<()>,
    permit_exit: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl novarocks_fs::FileIoSpawner for FooterDrainGate {
    fn spawn(
        &self,
        task: novarocks_fs::FileIoFuture,
    ) -> novarocks_fs::FileResult<novarocks_fs::FileIoJob> {
        let permit = self.permit_exit.lock().unwrap().take();
        let ready = self.ready.clone();
        Ok(novarocks_fs::FileIoJob::new(tokio::spawn(async move {
            task.await;
            if let Some(permit) = permit {
                ready.send(()).unwrap();
                let _ = permit.await;
            }
        })))
    }
}

#[test]
fn cancellation_of_an_issued_footer_waits_for_its_original_stream_exit() {
    run_footer_stop(false);
}

#[test]
fn deadline_during_an_issued_footer_waits_for_its_original_stream_exit() {
    run_footer_stop(true);
}

fn run_footer_stop(use_deadline: bool) {
    use std::sync::mpsc;
    let dir = tempfile::tempdir().unwrap();
    let original = external_file(
        &dir.path().join("large.parquet"),
        vec![Some(7)],
        Some(1),
        Some(large_footer_properties()),
    );
    let source = SourceHttpFixture::new(std::fs::read(&original).unwrap());
    let (permit_exit, permit) = tokio::sync::oneshot::channel();
    let (ready, result_ready) = mpsc::channel();
    let spawner = Arc::new(FooterDrainGate {
        ready,
        permit_exit: Mutex::new(Some(permit)),
    });
    let target = rest::RestTarget::with_s3_spawner(source.config(), spawner);
    let provider = Arc::clone(&target.provider);
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
    let budget = Budget::new(8 * 1024 * 1024);
    let owner = ConnectorStopOwner::new();
    let mut req = request(
        &adapter,
        &provider,
        source.source(),
        context(&owner),
        &budget,
    );
    let stopped = if use_deadline {
        req.context = req
            .context
            .with_earlier_deadline(Instant::now() + Duration::from_secs(3));
        ConnectorMutationFailureKind::DeadlineExceeded
    } else {
        ConnectorMutationFailureKind::Cancelled
    };
    source.arm_tail();
    std::thread::scope(|scope| {
        let (sent, observed) = mpsc::channel();
        let running = scope.spawn(move || sent.send(adapter.execute(req)).unwrap());
        source.wait_for_tail();
        if !use_deadline {
            owner.request_stop();
        }
        result_ready.recv_timeout(Duration::from_secs(10)).unwrap();
        source.release_tail();
        let before_release = observed.recv_timeout(Duration::from_millis(100));
        assert!(budget.held.load(Ordering::SeqCst) > 0);
        permit_exit.send(()).unwrap();
        assert!(matches!(
            before_release,
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        let outcome = observed
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        running.join().unwrap();
        assert!(
            matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, .. }
            if failure.kind() == stopped)
        );
    });
    let requests = source.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.range.as_deref() == Some(&source.tail_range()))
            .count(),
        1
    );
    assert!(
        requests
            .iter()
            .all(|request| !request.target.ends_with("/incoming/b.parquet"))
    );
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
    assert_eq!(source.bytes(), std::fs::read(&original).unwrap());
    assert!(
        read_table(&provider)
            .metadata()
            .current_snapshot()
            .is_none()
    );
}

struct PropertyCompetition {
    inner: Arc<CatalogPublisher>,
    first_install: Mutex<Option<(crate::iceberg::table::Table, TableMetadata)>>,
    checked_rounds: Mutex<Vec<crate::commit::model::BaseIdentity>>,
}

#[async_trait]
impl PublicationPreparer for PropertyCompetition {
    async fn load_target(
        &self,
        attempt: &IcebergCommitAttempt,
    ) -> crate::iceberg::Result<StagingBase> {
        self.inner.load_target(attempt).await
    }

    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        self.inner.preflight_recovery(request, operation)
    }

    fn evidence_policy(&self) -> CatalogEvidencePolicy {
        self.inner.evidence_policy()
    }

    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        context: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        self.checked_rounds
            .lock()
            .unwrap()
            .push(request.base().clone());
        let prepared = Arc::clone(&self.inner)
            .prepare_dispatch(request, descriptor, generation, context)
            .await?;
        let race = self.first_install.lock().unwrap().take();
        if let Some((table, metadata)) = race {
            // The original Hadoop adapter has checked B and frozen v2, but
            // its conditional install has yet to run. The competing engine
            // wins that version with a property-only metadata document.
            install_competing_metadata(&table, &metadata);
        }
        Ok(prepared)
    }
}

#[derive(Clone, Copy, Debug)]
enum MappingRace {
    Remove,
    Change,
    IncompatibleFirstMapping,
    CompatibleFirstMapping,
    PreviouslyUnbound,
    MissingSpec,
    NoMapping,
    Exhausted,
}

#[test]
fn registration_checks_mapping_after_a_hadoop_property_only_install_wins() {
    let property = TableProperties::PROPERTY_DEFAULT_NAME_MAPPING;
    let original_mapping = r#"[{"field-id":1,"names":["value"]}]"#;
    let incompatible = r#"[{"field-id":2,"names":["value"]}]"#;
    let compatible = r#"[{"field-id":1,"names":["value","parallel_alias"]}]"#;
    for race in [
        MappingRace::Remove,
        MappingRace::Change,
        MappingRace::IncompatibleFirstMapping,
        MappingRace::CompatibleFirstMapping,
        MappingRace::PreviouslyUnbound,
        MappingRace::MissingSpec,
        MappingRace::NoMapping,
        MappingRace::Exhausted,
    ] {
        let mut properties = vec![
            ("commit.retry.min-wait-ms".into(), "0".into()),
            ("commit.retry.max-wait-ms".into(), "0".into()),
            (
                "commit.retry.num-retries".into(),
                if matches!(race, MappingRace::Exhausted) {
                    "0"
                } else {
                    "2"
                }
                .into(),
            ),
        ];
        match race {
            MappingRace::Remove | MappingRace::Change => {
                properties.push((property.into(), original_mapping.into()));
            }
            MappingRace::PreviouslyUnbound => {
                properties.push((
                    property.into(),
                    r#"[{"field-id":1,"names":["legacy_alias"]}]"#.into(),
                ));
            }
            _ => {}
        }
        let (runtime, _warehouse, provider) = exact_provider_with_table(
            FormatVersion::V2,
            NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)),
            properties,
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let file = external_file(
            &dir.path().join("legacy.parquet"),
            vec![Some(11)],
            None,
            None,
        );
        let original = std::fs::read(&file).unwrap();
        let location = format!("file://{}", file.display());
        let budget = Budget::new(8 * 1024 * 1024);
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
        let context = context(&ConnectorStopOwner::new());
        let request = request(
            &adapter,
            &provider,
            location.clone(),
            context.clone(),
            &budget,
        );
        let table = read_table(&provider);
        let source = table.metadata();
        let connector = provider.runtime();
        let resources = connector.resources();
        let binding = resources.planning_binding().for_request(context.clone());
        let (files, mapping) = runtime
            .block_on(
                RegistrationPreparation {
                    table: &table,
                    location: &location,
                    recursion: ConnectorAddFilesRecursion::Fail,
                    catalog: connector.novarocks_catalog().as_ref(),
                    binding: &binding,
                    context: &context,
                    byte_author: request.registration_byte_author().unwrap(),
                }
                .collect(),
            )
            .unwrap();
        let operation = IcebergCommitOperation::new(
            OperationToken::from_mutation(request.plan.operation_id()),
            source.location(),
            resources.planning_binding().clone(),
            context.clone(),
            resources.catalog_runtime().clone(),
            OperationLimits::default(),
        )
        .unwrap();
        let paths = operation.install_registered_new_files(1, files).unwrap();
        let mut recovery = MutationRecoveryTemplate::new(
            adapter.descriptor.clone(),
            adapter.key.clone(),
            request.plan.clone(),
            MutationRecoverySource {
                target: DataFinalizationFacts {
                    namespace: "db".into(),
                    table: "t".into(),
                    table_uuid: source.uuid().to_string(),
                },
                target_ref: "main".into(),
                base_snapshot_id: None,
                base_sequence_number: None,
            },
        );
        recovery.summary = ConnectorDataMutationPlanSummary::new(1, 1, original.len() as u64);
        let intent =
            freeze_registration_intent(&table, &operation, None, &recovery.marker()).unwrap();
        let policy = RetryPolicy::from_properties(source.properties()).unwrap();
        let journal = PublicationJournal::<DataPublication>::for_runtime(
            operation.token(),
            resources.catalog_runtime(),
        );
        let backend = RegisteredIcebergDataMutationBackend::new(Arc::clone(&provider));
        let direct = backend.catalog_publisher(
            &journal,
            &recovery,
            &intent,
            &operation,
            CatalogOperation::RegisterFiles,
        );
        let mut next = source
            .clone()
            .into_builder(table.metadata_location().map(str::to_owned));
        next = next
            .set_properties(HashMap::from([("concurrent".into(), "kept".into())]))
            .unwrap();
        let expected_mapping = match race {
            MappingRace::Change | MappingRace::IncompatibleFirstMapping => Some(incompatible),
            MappingRace::CompatibleFirstMapping => Some(compatible),
            MappingRace::PreviouslyUnbound => Some(original_mapping),
            MappingRace::Remove
            | MappingRace::MissingSpec
            | MappingRace::NoMapping
            | MappingRace::Exhausted => None,
        };
        next = if let Some(raw) = expected_mapping {
            next.set_properties(HashMap::from([(property.into(), raw.into())]))
                .unwrap()
        } else {
            next.remove_properties(&[property.into()]).unwrap()
        };
        if matches!(race, MappingRace::NoMapping) {
            let schema = crate::iceberg::spec::Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "added", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap();
            next = next.add_current_schema(schema).unwrap();
        }
        if matches!(race, MappingRace::MissingSpec) {
            use crate::iceberg::spec::{Transform, UnboundPartitionSpec};
            let spec = UnboundPartitionSpec::builder()
                .add_partition_field(1, "value_partition", Transform::Identity)
                .unwrap()
                .build();
            next = next
                .add_default_partition_spec(spec)
                .unwrap()
                .remove_partition_specs(&[source.default_partition_spec_id()])
                .unwrap();
        }
        let external = next.build().unwrap().metadata;
        assert_eq!(external.current_snapshot_id(), source.current_snapshot_id());
        let race_publisher = Arc::new(PropertyCompetition {
            inner: direct,
            checked_rounds: Mutex::default(),
            first_install: Mutex::new(Some((table.clone(), external))),
        });
        let publisher = journal.observe_prepared(race_publisher.clone());
        let checks = RegistrationChecks {
            table_uuid: source.uuid(),
            partition_spec_id: source.default_partition_spec_id(),
            paths: &paths,
            mapping: mapping.as_ref(),
            last_verified_main: Mutex::new(None),
        };
        let append = CompactFastAppendPreparer(operation.clone());
        let report = runtime.block_on(attempt::run(
            &operation,
            &intent,
            &PreparedChange::default(),
            &[&checks, &append],
            &publisher,
            policy,
        ));
        drop(paths);
        drop(mapping);
        operation.release_registered_paths();
        let outcome = project_for_operation(journal.project(report), REGISTER_EXISTING_FILES_KIND);
        let current = read_table(&provider);
        assert_eq!(
            &current.metadata().properties()["concurrent"],
            "kept",
            "{race:?}"
        );
        let checked = race_publisher.checked_rounds.lock().unwrap();
        assert!(matches!(
            &checked[0],
            crate::commit::model::BaseIdentity::Existing { parent: None, .. }
        ));
        let actual_mapping = current
            .metadata()
            .properties()
            .get(property)
            .map(String::as_str);
        match race {
            MappingRace::CompatibleFirstMapping | MappingRace::NoMapping => {
                assert_eq!(checked.len(), 2, "{race:?}");
                assert!(matches!(
                    &checked[1],
                    crate::commit::model::BaseIdentity::Existing { parent: None, .. }
                ));
                assert_committed(outcome, 1, 1, original.len() as u64);
                let files = live_files(&runtime, &current);
                assert_eq!(files.len(), 1);
                assert_eq!(
                    files[0].lower_bounds()[&1],
                    crate::iceberg::spec::Datum::long(11)
                );
                if let Some(expected) = expected_mapping {
                    assert_eq!(actual_mapping, Some(expected));
                } else {
                    let mapping =
                        crate::schema_mapping::parse_name_mapping(actual_mapping.unwrap()).unwrap();
                    assert_eq!(
                        crate::schema_mapping::mapped_path_segments(
                            &mapping,
                            ["added"].into_iter()
                        )
                        .unwrap(),
                        Some(2)
                    );
                }
            }
            MappingRace::Exhausted => {
                assert_eq!(checked.len(), 1);
                let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
                    panic!("expected genuine Hadoop competition exhaustion, got {outcome:?}");
                };
                assert_eq!(
                    failure.failure_fact().reason(),
                    connector_reason::COMMIT_TRANSACTION_CONFLICT
                );
                assert!(current.metadata().current_snapshot().is_none());
                assert_eq!(actual_mapping, expected_mapping);
            }
            MappingRace::Remove => {
                assert_eq!(checked.len(), 1);
                assert_failure(
                    outcome,
                    connector_reason::ICEBERG_COMMIT_ERROR,
                    "Failed to add files: schema.name-mapping.default was removed after file binding",
                );
                assert!(current.metadata().current_snapshot().is_none());
                assert_eq!(actual_mapping, expected_mapping);
            }
            MappingRace::MissingSpec => {
                assert_eq!(checked.len(), 1);
                assert_failure(
                    outcome,
                    connector_reason::ICEBERG_COMMIT_ERROR,
                    "Failed to add files: Registered partition spec disappeared",
                );
                assert!(current.metadata().current_snapshot().is_none());
                assert_eq!(actual_mapping, expected_mapping);
            }
            MappingRace::Change
            | MappingRace::IncompatibleFirstMapping
            | MappingRace::PreviouslyUnbound => {
                assert_eq!(checked.len(), 1);
                assert_failure(
                    outcome,
                    connector_reason::ICEBERG_COMMIT_ERROR,
                    "Failed to add files: schema.name-mapping.default changed a registered field binding",
                );
                assert!(current.metadata().current_snapshot().is_none());
                assert_eq!(actual_mapping, expected_mapping);
            }
        }
        assert_eq!(budget.held.load(Ordering::SeqCst), 0, "{race:?}");
        assert_eq!(std::fs::read(&file).unwrap(), original);
    }
}

#[test]
fn registration_refuses_a_rebound_table_before_list_or_footer_admission() {
    let (_runtime, _warehouse, provider) = exact_provider_with_empty_table();
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
    let budget = Budget::new(8 * 1024 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let missing = format!("file://{}/absent", dir.path().display());
    let req = request(
        &adapter,
        &provider,
        missing,
        context(&ConnectorStopOwner::new()),
        &budget,
    );
    let original = read_table(&provider);
    let replacement = original
        .metadata()
        .clone()
        .into_builder(original.metadata_location().map(str::to_owned))
        .assign_uuid(uuid::Uuid::now_v7())
        .build()
        .unwrap()
        .metadata;
    let metadata = original.metadata_location().unwrap();
    std::fs::write(
        metadata.strip_prefix("file://").unwrap_or(metadata),
        serde_json::to_vec(&replacement).unwrap(),
    )
    .unwrap();
    assert_eq!(read_table(&provider).metadata().uuid(), replacement.uuid());
    assert_failure(
        adapter.execute(req).unwrap(),
        connector_reason::ICEBERG_COMMIT_ERROR,
        "Failed to add files: Target table was replaced",
    );
    let current = read_table(&provider);
    assert_eq!(current.metadata().uuid(), replacement.uuid());
    assert!(current.metadata().current_snapshot().is_none());
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
}

#[test]
fn a_known_leaf_id_outside_its_bound_ancestry_does_not_produce_pruning_metrics() {
    use arrow::array::StructArray;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    let (runtime, _warehouse, provider) = exact_provider_with_empty_table();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("unbound-nested.parquet");
    let child = Arc::new(
        Field::new("value", DataType::Int64, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            "1".into(),
        )])),
    );
    let fields = arrow::datatypes::Fields::from(vec![child]);
    let outer = Field::new("unrelated", DataType::Struct(fields.clone()), true).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.into(), "2".into())]),
    );
    let schema = Arc::new(ArrowSchema::new(vec![outer]));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int64Array::from(vec![7, 23]))],
            None,
        ))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&file).unwrap(), schema, None).unwrap();
    writer.write(&input).unwrap();
    writer.close().unwrap();

    let budget = Budget::new(8 * 1024 * 1024);
    assert_committed(
        execute(&provider, format!("file://{}", file.display()), &budget),
        1,
        2,
        std::fs::metadata(&file).unwrap().len(),
    );
    let files = live_files(&runtime, &read_table(&provider));
    assert!(!files[0].null_value_counts().contains_key(&1));
    assert!(!files[0].lower_bounds().contains_key(&1));
    assert!(!files[0].upper_bounds().contains_key(&1));
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
}
