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

//! Native wire fixtures and focused tests for Worker typed scan execution.

/// Wire fixtures shared by this module's tests and the typed scan decoder's.
///
/// They live here because the carrier they build is this module's input; the
/// decoder tests assert what the decoder does with exactly that carrier.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use novarocks_proto_codec::connector_common::encode_connector_payload_message;
    use novarocks_proto_codec::connector_read::{ConnectorReadDecoder, encode_value_type};
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::ConnectorExecutionReadBinding;
    use novarocks_spi::connector::read_stack::ConnectorValueType;
    use novarocks_worker::read_attempt::TypedReadAttemptContext;

    pub fn encoded_payload(
        category: novarocks_spi::connector::ConnectorCodecCategory,
        value: impl Into<bytes::Bytes>,
    ) -> novarocks_proto_models::connector_common::ConnectorEncodedPayload {
        encode_connector_payload_message(&novarocks_spi::connector::ConnectorEncodedPayload::new(
            novarocks_spi::connector::ConnectorEnvelopeHeader::new(
                novarocks_spi::connector::ConnectorProviderId::parse("fixture")
                    .expect("provider id"),
                novarocks_spi::connector::CatalogHandle::new(
                    novarocks_spi::connector::ConnectorInstanceId::try_from_canonical("test.typed")
                        .expect("instance id"),
                    novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
                ),
                category,
                novarocks_spi::connector::ConnectorCodecRevision::try_new(1)
                    .expect("codec revision"),
            ),
            value.into(),
        ))
    }

    pub fn unconstrained() -> dto::TupleDomain {
        dto::TupleDomain {
            none: false,
            column_domains: Vec::new(),
        }
    }

    pub fn column_handle(field_id: i32) -> dto::ColumnHandle {
        dto::ColumnHandle {
            provider_payload: Some(encoded_payload(
                novarocks_spi::connector::ConnectorCodecCategory::ReadColumn,
                bytes::Bytes::from(format!("column-{field_id}")),
            )),
        }
    }

    pub fn catalog_table_handle() -> dto::CatalogTableHandle {
        dto::CatalogTableHandle {
            catalog_handle: Some(novarocks_proto_models::catalog::CatalogHandle {
                catalog_name: "test.typed".to_owned(),
                version: vec![1; 32],
            }),
            transaction: Some(dto::ConnectorTransactionHandle {
                provider_payload: Some(encoded_payload(
                    novarocks_spi::connector::ConnectorCodecCategory::ReadView,
                    bytes::Bytes::from_static(b"transaction"),
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::Table(
                dto::ConnectorTableHandle {
                    provider_payload: Some(encoded_payload(
                        novarocks_spi::connector::ConnectorCodecCategory::ReadTable,
                        bytes::Bytes::from_static(b"table"),
                    )),
                },
            )),
        }
    }

    #[derive(Clone, Debug)]
    struct FixtureTable;
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct FixtureColumn(i32);
    #[derive(Clone, Debug)]
    struct FixtureSplit;

    impl novarocks_spi::connector::read_stack::ColumnHandle for FixtureColumn {}
    impl novarocks_spi::connector::read_stack::ConnectorSplit for FixtureSplit {
        fn retained_size_in_bytes(&self) -> u64 {
            64
        }
    }

    struct FixtureRuntime {
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        catalog_handle: novarocks_spi::connector::CatalogHandle,
    }

    impl FixtureRuntime {
        fn new() -> Self {
            let descriptor = novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("fixture")
                    .expect("provider id"),
                instance_id: novarocks_spi::connector::ConnectorInstanceId::try_from_canonical(
                    "test.typed",
                )
                .expect("instance id"),
            };
            Self {
                catalog_handle: novarocks_spi::connector::CatalogHandle::new(
                    descriptor.instance_id.clone(),
                    novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
                ),
                descriptor,
            }
        }
    }

    impl novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime for FixtureRuntime {
        type Table = FixtureTable;
        type Column = FixtureColumn;
        type Transaction = ();
        type Split = FixtureSplit;

        fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
            &self.catalog_handle
        }
        fn transaction(&self) -> Self::Transaction {}
    }

    #[derive(Clone)]
    struct FixtureCodec {
        adapter: novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter<FixtureRuntime>,
    }

    fn fixture_codec() -> FixtureCodec {
        FixtureCodec {
            adapter: novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter::new(
                std::sync::Arc::new(FixtureRuntime::new()),
            ),
        }
    }

    pub fn installed_read_execution() -> ConnectorExecutionReadBinding {
        ConnectorExecutionReadBinding::new(
            std::sync::Arc::new(FixtureFactory),
            std::sync::Arc::new(fixture_codec()),
        )
    }

    impl novarocks_spi::connector::ConnectorReadWireDecoder for FixtureCodec {
        fn owner(&self) -> &str {
            "fixture"
        }
        fn decode_relation_payload(
            &self,
            _relation: &novarocks_spi::connector::ConnectorReadRelationPayload,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadRelation,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            let table = self.adapter.wrap_table(FixtureTable);
            self.adapter
                .relation(
                    novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table,
                    table,
                )
                .map_err(|error| {
                    novarocks_spi::connector::ConnectorCodecError::new(
                        novarocks_spi::connector::ConnectorFieldPath::root("table"),
                        novarocks_spi::connector::ConnectorCodecErrorKind::InvalidValue,
                        error.to_string(),
                    )
                })
        }
        fn decode_column_payload(
            &self,
            _: &novarocks_spi::connector::ConnectorEncodedPayload,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(self.adapter.wrap_column(FixtureColumn(1)))
        }
        fn decode_transaction_payload(
            &self,
            _: &novarocks_spi::connector::ConnectorEncodedPayload,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(self.adapter.wrap_transaction(()))
        }
        fn decode_split_payload(
            &self,
            _: &novarocks_spi::connector::ConnectorReadSplitPayload,
            _: &novarocks_spi::connector::read_stack::ConnectorReadSplitFacts,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadSplit,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(self.adapter.wrap_split(FixtureSplit))
        }
    }

    struct InertPageProvider;
    impl novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider for InertPageProvider {
        fn create_page_source(
            &self,
            _: &novarocks_spi::connector::read_stack::ConnectorSession,
            _: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _: u64,
            _: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _: &std::sync::Arc<novarocks_spi::connector::read_stack::ConnectorReadDynamicFilter>,
        ) -> Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorPageSource>,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "fixture page provider is never read",
            ))
        }
    }
    struct InertSystemProvider;
    impl novarocks_spi::connector::read_stack::ConnectorReadSystemTableProvider
        for InertSystemProvider
    {
        fn create_system_page_source(
            &self,
            _: &novarocks_spi::connector::read_stack::ConnectorSession,
            _: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
        ) -> Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorPageSource>,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "fixture system provider is never read",
            ))
        }
    }
    struct FixtureFactory;
    impl novarocks_spi::connector::read_stack::ConnectorReadProviderFactory for FixtureFactory {
        fn create_page_source_provider(
            &self,
            _: &novarocks_spi::connector::ConnectorRequestContext,
            _: novarocks_spi::connector::read_stack::ConnectorPageSourceProviderOptions,
        ) -> Result<
            std::sync::Arc<
                dyn novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider,
            >,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(std::sync::Arc::new(InertPageProvider))
        }
        fn create_system_table_provider(
            &self,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<
            std::sync::Arc<
                dyn novarocks_spi::connector::read_stack::ConnectorReadSystemTableProvider,
            >,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(std::sync::Arc::new(InertSystemProvider))
        }
    }

    pub fn decoded_scan() -> novarocks_proto_codec::connector_read::DecodedConnectorReadScan {
        let raw = novarocks_proto_codec::connector_read::ConnectorTableScanSource::parse(
            scan_source_proto(),
            novarocks_proto_codec::FieldPath::root("scan"),
        )
        .expect("scan");
        novarocks_proto_codec::connector_read::DecodedConnectorReadScan::decode(
            &fixture_codec(),
            &raw,
        )
        .expect("decoded scan")
    }

    pub fn decoded_scheduled_split(
        plan_node_id: i32,
        sequence_id: u64,
    ) -> novarocks_proto_codec::connector_read::DecodedScheduledReadSplit {
        let raw = novarocks_proto_codec::connector_read::ScheduledSplit::parse(
            split_proto(plan_node_id, sequence_id),
            novarocks_proto_codec::FieldPath::root("split"),
        )
        .expect("split");
        fixture_codec()
            .decode_scheduled_split(&raw)
            .expect("decoded split")
    }

    /// The runtime bundle a typed decode needs, wired to the same binding
    /// generation `catalog_table_handle` names.
    pub fn typed_scan_runtime() -> novarocks_worker::TypedScanRuntime {
        struct NoVendedStorageResolver;

        impl novarocks_spi::connector::ConnectorStorageResolver for NoVendedStorageResolver {
            fn resolve_vended_s3(
                &self,
                _: &novarocks_spi::connector::StorageAccessRequest,
            ) -> Result<
                novarocks_spi::connector::ResolvedVendedS3Access,
                novarocks_spi::connector::ConnectorError,
            > {
                Err(novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "test fixture has no vended storage lease",
                ))
            }
        }

        use novarocks_types::{AttemptId, QueryId};
        let catalog_handle = novarocks_spi::connector::CatalogHandle::new(
            novarocks_spi::connector::ConnectorInstanceId::try_from_canonical("test.typed")
                .expect("canonical instance id"),
            novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
        );
        let execution = installed_read_execution();
        let execution_id = novarocks_proto_codec::lifecycle::QueryExecutionId::new(
            QueryId::new(1, 2),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let queues = novarocks_execution::connector::SplitQueueRegistry::new().open_attempt(
            novarocks_execution::connector::TaskAttemptKey::new(
                execution_id,
                novarocks_types::UniqueId::new(9, 1),
            ),
            novarocks_execution::connector::SplitQueueConfig::default(),
        );
        let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
            "q1",
            "novarocks",
            "UTC",
            "en_US",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .expect("session");
        novarocks_worker::TypedScanRuntime::new(
            execution_id,
            std::sync::Arc::new(move |handle| {
                if handle == &catalog_handle {
                    Ok(execution.clone())
                } else {
                    Err("no query-leased test catalog runtime".to_owned())
                }
            }),
            std::sync::Arc::new(|_| Err("no query-leased test writer runtime".to_owned())),
            queues,
            session,
            std::sync::Arc::new(|| Ok(None)),
            std::sync::Arc::new(TypedReadAttemptContext::new()),
            std::sync::Arc::new(NoVendedStorageResolver),
        )
    }

    pub fn scan_source_proto() -> dto::ConnectorTableScanSource {
        dto::ConnectorTableScanSource {
            table: Some(catalog_table_handle()),
            assignments: vec![dto::ScanAssignment {
                variable: "v0".to_owned(),
                column: Some(column_handle(1)),
                value_type: Some(encode_value_type(ConnectorValueType::BigInt)),
            }],
            enforced_predicate: Some(unconstrained()),
            unenforced_predicate: Some(unconstrained()),
            remaining_expression: None,
            dynamic_filters: Vec::new(),
            max_batch_rows: 1024,
            max_batch_bytes: 1 << 20,
            work_source: dto::ScanWorkSource::RuntimeSplits as i32,
        }
    }

    pub fn split_proto(plan_node_id: i32, sequence_id: u64) -> dto::ScheduledSplit {
        dto::ScheduledSplit {
            sequence_id,
            plan_node_id,
            split: Some(dto::ConnectorSplit {
                split_weight_raw: 100,
                remotely_accessible: true,
                addresses: Vec::new(),
                affinity_key: None,
                retained_size_in_bytes: 64,
                category: Some(dto::connector_split::Category::Data(dto::DataSplit {
                    provider_payload: Some(encoded_payload(
                        novarocks_spi::connector::ConnectorCodecCategory::ReadSplit,
                        bytes::Bytes::from(format!("split-{sequence_id}")),
                    )),
                })),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime};

    use novarocks_execution::connector::TaskAttemptSplitQueues;
    use novarocks_execution::exec::node::scan::{
        BoundScanRanges, IncrementalScanRange, ScanMorsel, ScanOp, ScanSource,
    };
    use novarocks_execution::runtime::profile::RuntimeProfile;
    use novarocks_spi::connector::ConnectorRequestContext;
    use novarocks_spi::connector::read_stack::{
        CompleteAllDynamicFilter, ConnectorSession, PageSourceFileMetrics,
    };
    use novarocks_worker::typed_page_source::flush_page_source_file_metrics;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{ArrayRef, Int64Array};
    use novarocks_execution::connector::{SplitQueueConfig, SplitQueueRegistry, TaskAttemptKey};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::connector_read::ConnectorTableScanSource;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::{
        ConnectorPageSource, ConnectorReadDynamicFilter, ConnectorReadPageSourceProvider,
        ConnectorReadSystemTableProvider, PageSourceMetrics, SourcePage,
    };
    use novarocks_spi::connector::{ConnectorCancellation, ConnectorError, ConnectorErrorKind};
    use novarocks_types::SlotId;
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId, UniqueId};
    use novarocks_worker::RuntimeFilterSessionResolver;
    use novarocks_worker::TypedConnectorReadDescriptor;
    use novarocks_worker::read_attempt::ReceivedReadSplit;
    use novarocks_worker::typed_connector_runtime::{
        TypedConnectorScanSource, TypedConnectorSystemTableScanSource,
    };
    use novarocks_worker::typed_scan_filter::TypedScanLiveDynamicFilterFactory;

    use super::*;

    fn scan_source() -> ConnectorTableScanSource {
        ConnectorTableScanSource::parse(
            test_support::scan_source_proto(),
            FieldPath::root("typed_connector_read"),
        )
        .expect("valid typed scan source")
    }

    const NODE: i32 = 7;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct AlwaysCancelled;

    impl ConnectorCancellation for AlwaysCancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    /// A page source scripted turn by turn. A `None` entry is an idle turn, not
    /// termination: only running out of script finishes it.
    struct ScriptedPageSource {
        pages: Vec<Option<SourcePage>>,
        cursor: usize,
        finished: bool,
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorPageSource for ScriptedPageSource {
        fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
            if self.cursor >= self.pages.len() {
                self.finished = true;
                return Ok(None);
            }
            let page = self.pages[self.cursor].take();
            self.cursor += 1;
            Ok(page)
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[test]
    fn typed_page_source_file_metrics_are_projected_as_deltas() {
        let profile = RuntimeProfile::new("typed-file-read");
        let mut last = PageSourceFileMetrics::default();
        let first = PageSourceFileMetrics {
            bytes_read: 25,
            page_index_attempts: 2,
            page_index_rows_considered: 16,
            page_index_rows_pruned: 12,
            ..Default::default()
        };
        flush_page_source_file_metrics(Some(&profile), &mut last, first);
        // Re-observing one cumulative snapshot must not count it twice.
        flush_page_source_file_metrics(Some(&profile), &mut last, first);
        flush_page_source_file_metrics(
            Some(&profile),
            &mut last,
            PageSourceFileMetrics {
                bytes_read: 40,
                page_index_attempts: 3,
                page_index_rows_considered: 24,
                page_index_rows_pruned: 18,
                ..Default::default()
            },
        );

        assert_eq!(profile.counter_value("ConnectorFileBytesRead"), Some(40));
        assert_eq!(
            profile.counter_value("ConnectorFilePageIndexAttempts"),
            Some(3)
        );
        assert_eq!(
            profile.counter_value("ConnectorFilePageIndexRowsConsidered"),
            Some(24)
        );
        assert_eq!(
            profile.counter_value("ConnectorFilePageIndexRowsPruned"),
            Some(18)
        );
    }

    /// A page-source provider that hands out one scripted source per split.
    struct ScriptedProvider {
        script: Mutex<Vec<Vec<Option<SourcePage>>>>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        fail_open: bool,
    }

    impl ScriptedProvider {
        fn new(script: Vec<Vec<Option<SourcePage>>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
                fail_open: false,
            })
        }
    }

    impl ConnectorReadPageSourceProvider for ScriptedProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            if self.fail_open {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "scripted open failure",
                ));
            }
            self.opens.fetch_add(1, Ordering::AcqRel);
            let mut script = self.script.lock().expect("script lock");
            let pages = if script.is_empty() {
                Vec::new()
            } else {
                script.remove(0)
            };
            Ok(Box::new(ScriptedPageSource {
                pages,
                cursor: 0,
                finished: false,
                closes: Arc::clone(&self.closes),
            }))
        }
    }

    /// Records how many columns the dynamic filter it was handed covers.
    struct FilterRecordingProvider {
        observed: Arc<Mutex<Option<usize>>>,
    }

    impl ConnectorReadPageSourceProvider for FilterRecordingProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            *self.observed.lock().expect("observed lock") =
                Some(dynamic_filter.columns_covered().len());
            Ok(Box::new(ScriptedPageSource {
                pages: Vec::new(),
                cursor: 0,
                finished: false,
                closes: Arc::new(AtomicUsize::new(0)),
            }))
        }
    }

    /// A scan whose attempt installed no runtime filter.
    fn no_runtime_filter() -> RuntimeFilterSessionResolver {
        Arc::new(|| Ok(None))
    }

    fn descriptor() -> TypedConnectorReadDescriptor {
        let wire_scan = scan_source();
        let decoded_scan = test_support::decoded_scan();
        TypedConnectorReadDescriptor::new(
            decoded_scan.relation().table().clone(),
            decoded_scan.assignments().to_vec(),
            crate::runtime_filter_typed_scan::complete_all_scan_dynamic_filter(
                &wire_scan,
                &decoded_scan,
            ),
        )
    }

    fn live_dynamic_filter_factory() -> Arc<dyn TypedScanLiveDynamicFilterFactory> {
        crate::runtime_filter_typed_scan::typed_scan_live_dynamic_filter_factory(
            scan_source(),
            test_support::decoded_scan(),
        )
    }

    fn int_page(values: Vec<i64>) -> SourcePage {
        let positions = values.len();
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        SourcePage::try_new(positions, vec![column]).expect("valid page")
    }

    fn scheduled_split(sequence_id: u64) -> ReceivedReadSplit {
        let (evidence, split) =
            test_support::decoded_scheduled_split(NODE, sequence_id).into_parts();
        ReceivedReadSplit::new(evidence.sequence_id(), evidence.plan_node_id(), split)
    }

    fn attempt_queues() -> Arc<TaskAttemptSplitQueues<ReceivedReadSplit>> {
        let registry = SplitQueueRegistry::new();
        registry.open_attempt(
            TaskAttemptKey::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("attempt"))
                    .expect("execution id"),
                UniqueId::new(3, 4),
            ),
            SplitQueueConfig::default(),
        )
    }

    fn request(cancellation: Arc<dyn ConnectorCancellation>) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            cancellation,
            1 << 20,
            1 << 22,
        )
        .expect("request context")
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q-1", "test", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn source_with(
        provider: Arc<ScriptedProvider>,
        queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
        cancellation: Arc<dyn ConnectorCancellation>,
    ) -> TypedConnectorScanSource {
        TypedConnectorScanSource::new(
            descriptor(),
            provider,
            session(),
            request(cancellation),
            queues,
            NODE,
            vec![SlotId::new(1)],
            no_runtime_filter(),
            live_dynamic_filter_factory(),
            false,
        )
    }

    fn bind(source: &TypedConnectorScanSource) -> Arc<dyn ScanOp> {
        source
            .bind(BoundScanRanges::None)
            .expect("bind typed connector scan")
    }

    #[test]
    fn typed_scan_starts_with_zero_splits_and_does_not_end_the_stream() {
        let queues = attempt_queues();
        let source = source_with(
            ScriptedProvider::new(Vec::new()),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);

        // Exactly one morsel, before any split exists: it is the driver that
        // will drain the queue. Reporting none would leave the splits enqueued
        // and unread, and the query would return zero rows while reporting
        // success everywhere.
        let morsels = op.build_morsels().expect("build morsels");
        assert_eq!(morsels.morsels.len(), 1);
        // The morsel set is final; it is the queue that grows.
        assert!(!morsels.has_more);
        assert!(!op.supports_incremental_scan_ranges());

        // The terminal marker alone is a clean, empty end of stream.
        queues
            .queue(NODE)
            .offer_splits(NODE, Vec::new(), true)
            .expect("terminal marker");
        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive an empty typed scan");
        assert!(rows.is_empty());
    }

    /// A system relation resolved to one backend has no split at all, so its
    /// scan must do its whole job in one morsel. A source that waited on a
    /// split queue here would park forever.
    struct ScriptedSystemTables {
        script: Mutex<Vec<Option<SourcePage>>>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorReadSystemTableProvider for ScriptedSystemTables {
        fn create_system_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            self.opens.fetch_add(1, Ordering::AcqRel);
            let pages = std::mem::take(&mut *self.script.lock().expect("script lock"));
            Ok(Box::new(ScriptedPageSource {
                pages,
                cursor: 0,
                finished: false,
                closes: Arc::clone(&self.closes),
            }))
        }
    }

    fn system_table_source(
        provider: Arc<ScriptedSystemTables>,
    ) -> TypedConnectorSystemTableScanSource {
        TypedConnectorSystemTableScanSource::new(
            descriptor(),
            provider,
            session(),
            request(Arc::new(NeverCancelled)),
            NODE,
            vec![SlotId::new(1)],
            false,
        )
    }

    #[test]
    fn a_system_relation_scan_reads_its_metadata_file_without_any_split() {
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(ScriptedSystemTables {
            script: Mutex::new(vec![Some(int_page(vec![7, 8]))]),
            opens: Arc::clone(&opens),
            closes: Arc::clone(&closes),
        });
        let source = system_table_source(provider);
        let op = source
            .bind(BoundScanRanges::None)
            .expect("a system relation binds with no range");

        // One unit of work, known before execution: nothing can add more.
        let morsels = op.build_morsels().expect("build morsels");
        assert_eq!(morsels.morsels.len(), 1);
        assert!(!morsels.has_more);

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drain the metadata file");
        assert_eq!(
            rows.iter()
                .map(|chunk| chunk.batch.num_rows())
                .sum::<usize>(),
            2
        );
        assert_eq!(opens.load(Ordering::Acquire), 1, "opened exactly once");
        assert_eq!(closes.load(Ordering::Acquire), 1, "closed exactly once");
    }

    /// Its work unit is the relation itself, so a morsel that names a physical
    /// range belongs to some other scan and must not be silently accepted.
    #[test]
    fn a_system_relation_scan_refuses_a_morsel_it_does_not_own() {
        let provider = Arc::new(ScriptedSystemTables {
            script: Mutex::new(Vec::new()),
            opens: Arc::new(AtomicUsize::new(0)),
            closes: Arc::new(AtomicUsize::new(0)),
        });
        let source = system_table_source(provider);
        let op = source.bind(BoundScanRanges::None).expect("bind");
        let outcome = op.execute_iter(
            ScanMorsel::FileRange {
                path: "s3://bucket/f.parquet".to_string(),
                offset: 0,
                length: 1,
                file_len: 1,
                scan_range_id: 0,
                external_datacache: None,
            },
            None,
            None,
        );
        let error = match outcome {
            Ok(_) => panic!("a file-range morsel is not this scan's work unit"),
            Err(error) => error,
        };
        assert!(
            error.contains("file-range morsel"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn typed_scan_reads_a_split_that_arrives_after_the_driver_started() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1, 2, 3]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);

        let mut iter = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver");
        // The driver parks on an empty queue; the split arrives only now.
        let offering = {
            let queues = Arc::clone(&queues);
            std::thread::spawn(move || {
                let queue = queues.queue(NODE);
                queue
                    .offer_splits(NODE, vec![scheduled_split(1)], true)
                    .expect("late split");
            })
        };

        let chunk = iter
            .next()
            .expect("the late split produces a chunk")
            .expect("chunk");
        assert_eq!(chunk.len(), 3);
        assert!(iter.next().is_none());
        offering.join().expect("offering thread");
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
        // One split, one page source, closed when the split finished.
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_scan_treats_an_idle_page_as_not_end_of_stream() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![
            None,
            Some(int_page(vec![10])),
            None,
            Some(int_page(vec![20, 30])),
        ]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive across idle turns");
        // Both pages arrive: the idle turns between them ended nothing.
        assert_eq!(
            rows.iter()
                .map(novarocks_execution::exec::chunk::Chunk::len)
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn typed_scan_reads_every_queued_split_before_exhaustion_ends_it() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![
            vec![Some(int_page(vec![1]))],
            vec![Some(int_page(vec![2, 3]))],
        ]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1), scheduled_split(2)], true)
            .expect("two splits");

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive both splits");
        assert_eq!(rows.len(), 2);
        assert_eq!(provider.opens.load(Ordering::Acquire), 2);
        assert_eq!(provider.closes.load(Ordering::Acquire), 2);
        assert!(queues.queue(NODE).is_exhausted());
    }

    #[test]
    fn typed_scan_terminate_closes_the_page_source_and_the_queue_exactly_once() {
        let queues = attempt_queues();
        let provider =
            ScriptedProvider::new(vec![vec![Some(int_page(vec![1])), Some(int_page(vec![2]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        let queue = queues.queue(NODE);
        let closes = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&closes);
        queue.observable().add_observer(Arc::new(move || {
            observed.fetch_add(1, Ordering::AcqRel);
        }));
        queue
            .offer_splits(NODE, vec![scheduled_split(1)], false)
            .expect("one split");
        let woken_by_offer = closes.load(Ordering::Acquire);

        let mut iter = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver");
        iter.next()
            .expect("first page")
            .expect("first page is a chunk");
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);

        op.terminate().expect("terminate");
        op.terminate().expect("terminate is idempotent");
        op.terminate().expect("terminate is idempotent");
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
        assert_eq!(closes.load(Ordering::Acquire), woken_by_offer + 1);
        assert!(queue.is_closed());

        // After terminal the driver ends without another provider call.
        assert!(iter.next().is_none());
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_scan_after_terminate_opens_no_new_page_source() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");
        op.terminate().expect("terminate before any read");

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("a terminated scan yields nothing");
        assert!(rows.is_empty());
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_scan_fails_fast_on_a_cancelled_attempt() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(AlwaysCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let error = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect_err("a cancelled attempt must not read");
        assert!(error.contains("cancelled"), "unexpected error: {error}");
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_scan_rejects_a_morsel_it_does_not_own() {
        let source = source_with(
            ScriptedProvider::new(Vec::new()),
            attempt_queues(),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        assert!(
            op.execute_iter(ScanMorsel::ConnectorScanUnit { index: 0 }, None, None,)
                .is_err()
        );
        assert!(
            op.build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
                .is_err()
        );
    }

    #[test]
    fn typed_scan_hands_the_substituted_dynamic_filter_to_the_provider() {
        let queues = attempt_queues();
        let observed = Arc::new(Mutex::new(None));
        let provider = Arc::new(FilterRecordingProvider {
            observed: Arc::clone(&observed),
        });
        // The seam: a backend-driven filter replaces the default one, and the
        // provider is handed exactly what was substituted.
        let covered = BTreeSet::from([test_support::decoded_scan().assignments()[0]
            .column()
            .clone()]);
        let source = TypedConnectorScanSource::new(
            descriptor(),
            provider,
            session(),
            request(Arc::new(NeverCancelled)),
            Arc::clone(&queues),
            NODE,
            vec![SlotId::new(1)],
            no_runtime_filter(),
            live_dynamic_filter_factory(),
            false,
        )
        .with_backend_dynamic_filter(Arc::new(CompleteAllDynamicFilter::new(covered)));
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let _ = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive the scan");
        assert_eq!(
            *observed.lock().expect("observed lock"),
            Some(1),
            "the provider must see the substituted filter's covered columns"
        );
    }

    #[test]
    fn typed_scan_dynamic_filter_covers_only_the_scans_bound_columns() {
        let mut proto = test_support::scan_source_proto();
        proto.dynamic_filters = vec![dto::DynamicFilterBinding {
            filter_id: 3,
            variable: "v0".to_owned(),
        }];
        let scan = ConnectorTableScanSource::parse(proto, FieldPath::root("scan"))
            .expect("valid typed scan source");
        let filter = crate::runtime_filter_typed_scan::complete_all_scan_dynamic_filter(
            &scan,
            &test_support::decoded_scan(),
        );
        assert_eq!(filter.columns_covered().len(), 1);
        // Truthful and unconstrained: never blocked, never awaitable.
        assert!(filter.current_predicate().is_all());
        assert!(filter.is_complete());
        assert!(!filter.is_awaitable());
        assert!(!filter.is_blocked());

        // A scan with no binding covers nothing at all.
        assert!(
            crate::runtime_filter_typed_scan::complete_all_scan_dynamic_filter(
                &scan_source(),
                &test_support::decoded_scan(),
            )
            .columns_covered()
            .is_empty()
        );
    }
}
