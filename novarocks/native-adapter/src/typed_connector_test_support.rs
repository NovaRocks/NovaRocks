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
#[cfg(any(test, novarocks_dev_controls))]
pub mod test_support {
    use novarocks_proto_codec::connector_common::encode_connector_payload_message;
    use novarocks_proto_codec::connector_read::{ConnectorReadDecoder, encode_value_type};
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::ConnectorExecutionReadBinding;
    use novarocks_spi::connector::read_stack::ConnectorValueType;
    use novarocks_worker::read_attempt::TaskReadBindingRegistry;

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
        installed_read_execution_with_factory_version([1; 32])
    }

    pub fn installed_read_execution_with_factory_version(
        version: [u8; 32],
    ) -> ConnectorExecutionReadBinding {
        installed_read_execution_with_provider(version, None)
    }

    pub fn installed_read_execution_with_provider(
        version: [u8; 32],
        provider: Option<
            std::sync::Arc<
                dyn novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider,
            >,
        >,
    ) -> ConnectorExecutionReadBinding {
        let codec = fixture_codec();
        let binding = codec.adapter.binding();
        ConnectorExecutionReadBinding::new(
            std::sync::Arc::new(FixtureFactory {
                provider,
                binding: novarocks_spi::connector::read_stack::ConnectorReadBinding::new(
                    binding.descriptor().clone(),
                    novarocks_spi::connector::CatalogHandle::new(
                        binding.catalog_handle().catalog_name().clone(),
                        novarocks_spi::connector::CatalogVersion::from_bytes(version),
                    ),
                ),
            }),
            std::sync::Arc::new(codec.clone()),
            std::sync::Arc::new(codec),
        )
    }

    impl novarocks_connector_contract::ConnectorReadRelationRecipeCompiler for FixtureCodec {
        type Error = novarocks_spi::connector::ConnectorCodecError;

        fn compile_private(
            &self,
            draft: &novarocks_connector_contract::ConnectorReadRelationRecipeDraft,
        ) -> Result<novarocks_connector_contract::ConnectorReadRelationRecipeDraft, Self::Error>
        {
            // This test fixture has no provider-private bytes. Product bindings
            // use their provider's strict pure codec instead.
            Ok(draft.clone())
        }

        fn compile_split_private(
            &self,
            _binding: &novarocks_connector_contract::ConnectorReadBinding,
            draft: &novarocks_connector_contract::ConnectorReadRecipeSplitDraft,
        ) -> Result<novarocks_connector_contract::ConnectorReadRecipeSplitDraft, Self::Error>
        {
            Ok(draft.clone())
        }
    }

    impl novarocks_spi::connector::ConnectorReadWireDecoder for FixtureCodec {
        fn owner(&self) -> &str {
            "fixture"
        }
        fn decode_relation_payload(
            &self,
            relation: &novarocks_spi::connector::ConnectorReadRelationPayload,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadRelation,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            let table = self.adapter.wrap_table(FixtureTable);
            self.adapter
                .relation(relation.kind(), table)
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
        fn create_page_stream(
            &self,
            _: &novarocks_spi::connector::read_stack::ConnectorSession,
            _: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _: u64,
            _: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _: &std::sync::Arc<novarocks_spi::connector::read_stack::ConnectorReadDynamicFilter>,
            _: &novarocks_spi::connector::read_stack::ConnectorPollBudget,
        ) -> Result<
            novarocks_spi::connector::read_stack::OwnedConnectorPageStream,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "fixture page provider is never read",
            ))
        }
    }
    struct FixtureFactory {
        binding: novarocks_spi::connector::read_stack::ConnectorReadBinding,
        provider: Option<
            std::sync::Arc<
                dyn novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider,
            >,
        >,
    }
    impl novarocks_spi::connector::read_stack::ConnectorAdmittedReadProviderFactory for FixtureFactory {
        fn binding(&self) -> &novarocks_spi::connector::read_stack::ConnectorReadBinding {
            &self.binding
        }

        fn create_page_source_provider(
            &self,
            _: &novarocks_spi::connector::ConnectorRequestContext,
            _: novarocks_spi::connector::ConnectorExecutionResources,
            _: novarocks_spi::connector::read_stack::ConnectorPageSourceProviderOptions,
        ) -> Result<
            std::sync::Arc<
                dyn novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider,
            >,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(self
                .provider
                .clone()
                .unwrap_or_else(|| std::sync::Arc::new(InertPageProvider)))
        }
    }

    /// A real opened stream which parks until its owning Task closes it.
    #[derive(Default)]
    pub struct PendingReadProbe {
        pub opens: std::sync::atomic::AtomicUsize,
        pub closes: std::sync::atomic::AtomicUsize,
    }
    struct PendingFixtureProvider(std::sync::Arc<PendingReadProbe>);
    struct PendingFixtureStream(std::sync::Arc<PendingReadProbe>, Option<std::task::Waker>);
    impl tokio_stream::Stream for PendingFixtureStream {
        type Item = Result<
            novarocks_spi::connector::read_stack::SourcePage,
            novarocks_spi::connector::ConnectorError,
        >;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.get_mut().1 = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    }
    impl novarocks_spi::connector::read_stack::ConnectorPageStream for PendingFixtureStream {
        fn metrics(&self) -> novarocks_spi::connector::read_stack::PageSourceMetrics {
            Default::default()
        }
        fn memory_usage_bytes(&self) -> u64 {
            0
        }
        fn close(
            self: std::pin::Pin<Box<Self>>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<(), novarocks_spi::connector::ConnectorError>,
                    > + Send
                    + 'static,
            >,
        > {
            self.0
                .closes
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Box::pin(async { Ok(()) })
        }
    }
    impl novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider
        for PendingFixtureProvider
    {
        fn create_page_stream(
            &self,
            _: &novarocks_spi::connector::read_stack::ConnectorSession,
            _: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _: u64,
            _: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _: &std::sync::Arc<novarocks_spi::connector::read_stack::ConnectorReadDynamicFilter>,
            _: &novarocks_spi::connector::read_stack::ConnectorPollBudget,
        ) -> Result<
            novarocks_spi::connector::read_stack::OwnedConnectorPageStream,
            novarocks_spi::connector::ConnectorError,
        > {
            self.0
                .opens
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Ok(Box::pin(PendingFixtureStream(self.0.clone(), None)))
        }
    }
    pub fn installed_pending_read_execution(
        probe: std::sync::Arc<PendingReadProbe>,
    ) -> ConnectorExecutionReadBinding {
        installed_read_execution_with_provider(
            [1; 32],
            Some(std::sync::Arc::new(PendingFixtureProvider(probe))),
        )
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
            novarocks_execution::connector::SplitQueueKey::new(
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
                    Err(
                        novarocks_worker::CatalogExecutionAccessError::LeaseUnavailable {
                            catalog: handle.clone(),
                        },
                    )
                }
            }),
            std::sync::Arc::new(|handle| {
                Err(
                    novarocks_worker::CatalogExecutionAccessError::WriteUnavailable {
                        catalog: handle.clone(),
                    },
                )
            }),
            queues,
            session,
            std::sync::Arc::new(|| Ok(None)),
            std::sync::Arc::new(TaskReadBindingRegistry::new()),
            std::sync::Arc::new(NoVendedStorageResolver),
            novarocks_worker::ScanStreamHost::new(
                novarocks_worker::ScanPreparationConfig::default(),
                crate::backend_test_support::test_scan_stream_runtime(),
            ),
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
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime};
    use tokio_stream::Stream;

    use novarocks_execution::connector::SplitQueues;
    use novarocks_execution::exec::node::scan::{BoundScanRanges, ScanOp, ScanSource};
    use novarocks_execution::runtime::profile::RuntimeProfile;
    use novarocks_spi::connector::ConnectorRequestContext;
    use novarocks_spi::connector::read_stack::{
        CompleteAllDynamicFilter, ConnectorSession, PageSourceFileMetrics,
    };
    use novarocks_worker::typed_page_source::flush_page_source_file_metrics;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{ArrayRef, Int64Array};
    use novarocks_execution::connector::{SplitQueueConfig, SplitQueueKey, SplitQueueRegistry};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::connector_read::ConnectorReadDecoder;
    use novarocks_proto_codec::connector_read::ConnectorTableScanSource;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::{
        ConnectorPageStream, ConnectorPollBudget, ConnectorPreparationControl,
        ConnectorPreparationProgress, ConnectorPreparationStart, ConnectorPreparedPageSource,
        ConnectorReadDynamicFilter, ConnectorReadPageSourceProvider, ConnectorSourceOperations,
        OwnedConnectorPageStream, PageSourceMetrics, SourcePage,
    };
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorStopOwner, ConnectorStopView,
    };
    use novarocks_types::SlotId;
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId, UniqueId};
    use novarocks_worker::RuntimeFilterSessionResolver;
    use novarocks_worker::TypedConnectorReadDescriptor;
    use novarocks_worker::read_attempt::ReceivedReadSplit;
    use novarocks_worker::typed_connector_runtime::TypedConnectorScanSource;
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

    #[derive(Clone, Copy)]
    enum PreparationMode {
        Supported,
        Unsupported,
        FailOn(u64),
    }

    #[derive(Clone)]
    struct PreparationProbeProvider {
        mode: PreparationMode,
        events: Arc<Mutex<Vec<String>>>,
        closes: Arc<AtomicUsize>,
    }

    impl PreparationProbeProvider {
        fn new(mode: PreparationMode) -> Arc<Self> {
            Arc::new(Self {
                mode,
                events: Arc::new(Mutex::new(Vec::new())),
                closes: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn record(&self, event: impl Into<String>) {
            self.events
                .lock()
                .expect("preparation events")
                .push(event.into());
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().expect("preparation events").clone()
        }

        fn page_stream(&self, sequence_id: u64) -> OwnedConnectorPageStream {
            let pages = if sequence_id == 1 {
                vec![Some(int_page(vec![1])), Some(int_page(vec![11]))]
            } else {
                vec![Some(int_page(vec![sequence_id as i64]))]
            };
            Box::pin(ScriptedPageStream {
                pages: pages.into(),
                closes: Arc::clone(&self.closes),
                close_error: None,
            })
        }
    }

    struct ProbePreparationControl {
        retained: AtomicUsize,
        changed: tokio::sync::Notify,
    }

    impl ConnectorPreparationControl for ProbePreparationControl {
        fn request_pause(&self) {}
        fn request_resume(&self) {}
        fn request_reclaim(&self) {
            self.retained.store(0, Ordering::Release);
            self.changed.notify_waiters();
        }
        fn request_stop(&self) {
            self.retained.store(0, Ordering::Release);
            self.changed.notify_waiters();
        }
        fn retained_input_bytes(&self) -> u64 {
            self.retained.load(Ordering::Acquire) as u64
        }
        fn is_drained(&self) -> bool {
            self.retained.load(Ordering::Acquire) == 0
        }
        fn wait_drained(
            &self,
        ) -> Pin<
            Box<
                dyn Future<Output = novarocks_spi::connector::read_stack::ConnectorPreparationExit>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move {
                loop {
                    let changed = self.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if self.is_drained() {
                        return novarocks_spi::connector::read_stack::ConnectorPreparationExit::Exited;
                    }
                    changed.await;
                }
            })
        }
    }

    struct ProbePreparedSource {
        provider: PreparationProbeProvider,
        sequence_id: u64,
        control: Arc<ProbePreparationControl>,
    }

    impl ProbePreparedSource {
        fn page_stream(&self) -> OwnedConnectorPageStream {
            self.provider.page_stream(self.sequence_id)
        }
    }

    impl ConnectorPreparedPageSource for ProbePreparedSource {
        fn advance(
            &mut self,
            remaining_input_bytes: u64,
        ) -> Result<ConnectorPreparationProgress, ConnectorError> {
            self.provider
                .record(format!("advance:{}", self.sequence_id));
            if remaining_input_bytes == 0 {
                return Ok(ConnectorPreparationProgress::Deferred);
            }
            Ok(ConnectorPreparationProgress::Ready)
        }

        fn retained_input_bytes(&self) -> u64 {
            self.control.retained_input_bytes()
        }

        fn control(&self) -> Arc<dyn ConnectorPreparationControl> {
            self.control.clone()
        }

        fn promote(
            self: Box<Self>,
            _dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
            _budget: &ConnectorPollBudget,
        ) -> Result<OwnedConnectorPageStream, ConnectorError> {
            self.provider
                .record(format!("promote:{}", self.sequence_id));
            self.control.retained.store(0, Ordering::Release);
            self.control.changed.notify_waiters();
            Ok(self.page_stream())
        }
    }

    impl ConnectorReadPageSourceProvider for PreparationProbeProvider {
        fn create_page_stream(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            scheduled_split_sequence_id: u64,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
            _budget: &ConnectorPollBudget,
        ) -> Result<OwnedConnectorPageStream, ConnectorError> {
            self.record(format!("open:{scheduled_split_sequence_id}"));
            Ok(self.page_stream(scheduled_split_sequence_id))
        }

        fn prepare_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            scheduled_split_sequence_id: u64,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        ) -> Result<ConnectorPreparationStart, ConnectorError> {
            self.record(format!("prepare:{scheduled_split_sequence_id}"));
            match self.mode {
                PreparationMode::Unsupported => Ok(ConnectorPreparationStart::Unsupported),
                PreparationMode::FailOn(id) if id == scheduled_split_sequence_id => {
                    Err(ConnectorError::new(
                        ConnectorErrorKind::Unavailable,
                        "scripted future preparation failure",
                    ))
                }
                PreparationMode::Supported | PreparationMode::FailOn(_) => Ok(
                    ConnectorPreparationStart::Prepared(Box::new(ProbePreparedSource {
                        provider: self.clone(),
                        sequence_id: scheduled_split_sequence_id,
                        control: Arc::new(ProbePreparationControl {
                            retained: AtomicUsize::new(1),
                            changed: tokio::sync::Notify::new(),
                        }),
                    })),
                ),
            }
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

    fn attempt_queues() -> Arc<SplitQueues<ReceivedReadSplit>> {
        let registry = SplitQueueRegistry::new();
        registry.open_attempt(
            SplitQueueKey::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("attempt"))
                    .expect("execution id"),
                UniqueId::new(3, 4),
            ),
            SplitQueueConfig::default(),
        )
    }

    fn request(cancellation: ConnectorStopView) -> ConnectorRequestContext {
        execution_request(cancellation, ConnectorSourceOperations::new())
    }

    fn execution_request(
        cancellation: ConnectorStopView,
        operations: ConnectorSourceOperations,
    ) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            cancellation,
            1 << 20,
            1 << 22,
        )
        .expect("request context")
        // This is an execution fixture, with its exact NODE and real Task
        // operation ledger. Production never supplies a default source.
        .with_execution_source(
            novarocks_spi::connector::ConnectorRangeScope::try_new(1, 2, 1, 3, 4, NODE)
                .expect("fixture execution scope"),
            operations,
        )
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q-1", "test", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn bind(source: &TypedConnectorScanSource) -> Arc<dyn ScanOp> {
        source
            .bind(BoundScanRanges::None)
            .expect("bind typed connector scan")
    }

    fn chunk_values(chunk: &novarocks_execution::exec::chunk::Chunk) -> Vec<i64> {
        chunk
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer fixture column")
            .values()
            .to_vec()
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

    // ---------------------------------------------------- the driver's stream

    type CloseFuture = Pin<Box<dyn Future<Output = Result<(), ConnectorError>> + Send + 'static>>;

    /// A page stream scripted poll by poll. `None` is one poll that wakes its
    /// task and returns `Pending`, which is not end of stream.
    struct ScriptedPageStream {
        pages: std::collections::VecDeque<Option<SourcePage>>,
        closes: Arc<AtomicUsize>,
        close_error: Option<ConnectorError>,
    }

    impl tokio_stream::Stream for ScriptedPageStream {
        type Item = Result<SourcePage, ConnectorError>;

        fn poll_next(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.get_mut();
            match this.pages.pop_front() {
                None => std::task::Poll::Ready(None),
                Some(Some(page)) => std::task::Poll::Ready(Some(Ok(page))),
                Some(None) => {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            }
        }
    }

    impl ConnectorPageStream for ScriptedPageStream {
        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(self: Pin<Box<Self>>) -> CloseFuture {
            self.closes.fetch_add(1, Ordering::AcqRel);
            let result = self.close_error.clone().map_or(Ok(()), Err);
            Box::pin(async move { result })
        }
    }

    /// Hands out one scripted page stream per split, and records what each
    /// open was given.
    struct ScriptedStreamProvider {
        script: Mutex<std::collections::VecDeque<Vec<Option<SourcePage>>>>,
        opens: AtomicUsize,
        closes: Arc<AtomicUsize>,
        budgets: Mutex<Vec<ConnectorPollBudget>>,
        filter_columns: Mutex<Option<usize>>,
        close_error: Mutex<Option<ConnectorError>>,
    }

    impl ScriptedStreamProvider {
        fn new(script: Vec<Vec<Option<SourcePage>>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script.into()),
                opens: AtomicUsize::new(0),
                closes: Arc::new(AtomicUsize::new(0)),
                budgets: Mutex::new(Vec::new()),
                filter_columns: Mutex::new(None),
                close_error: Mutex::new(None),
            })
        }
    }

    impl ConnectorReadPageSourceProvider for ScriptedStreamProvider {
        fn create_page_stream(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
            budget: &ConnectorPollBudget,
        ) -> Result<OwnedConnectorPageStream, ConnectorError> {
            self.opens.fetch_add(1, Ordering::AcqRel);
            self.budgets.lock().expect("budgets").push(budget.clone());
            *self.filter_columns.lock().expect("filter") =
                Some(dynamic_filter.columns_covered().len());
            let pages = self
                .script
                .lock()
                .expect("script")
                .pop_front()
                .unwrap_or_default();
            Ok(Box::pin(ScriptedPageStream {
                pages: pages.into(),
                closes: Arc::clone(&self.closes),
                close_error: self.close_error.lock().unwrap().take(),
            }))
        }
    }

    /// Counts the wakes a stream's driver receives.
    #[derive(Default)]
    struct CountingWake(AtomicUsize);

    impl std::task::Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn actual_stream_test_factory()
    -> std::sync::Arc<dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory>
    {
        novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),novarocks_execution::runtime::mem_tracker::MemTracker::new_root("typed provider stream fixture creator"),std::sync::Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default())).unwrap()
    }
    // The test driver retains the same independent observation as production.
    // The wrapper owns no second source receiver or artificial stop signal.
    #[derive(Default)]
    struct TestSourceReports {
        runtime: novarocks_execution::runtime::runtime_state::RuntimeErrorState,
        cleanup: Mutex<Vec<novarocks_execution::runtime::task::ExecutionFailure>>,
    }
    impl novarocks_execution::runtime::task::ExecutionFailureReporter for TestSourceReports {
        fn report_failure(&self, cause: novarocks_execution::runtime::task::ExecutionFailure) {
            self.runtime.set_error(cause);
        }
        fn report_cleanup(&self, cause: &novarocks_execution::runtime::task::ExecutionFailure) {
            self.cleanup.lock().unwrap().push(cause.clone());
            self.runtime.report_cleanup(cause);
        }
        fn report_unresolved_exit(
            &self,
            cause: &novarocks_execution::runtime::task::ExecutionFailure,
        ) {
            self.runtime.report_unresolved_exit(cause);
        }
    }
    type ObservedTestFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
    struct ObservedTestStream {
        stream: novarocks_execution::exec::node::scan::ScanOutputStream,
        exit: Option<ObservedTestFuture<novarocks_execution::exec::node::scan::ScanExitProof>>,
        proof: Option<novarocks_execution::exec::node::scan::ScanExitProof>,
        failures: Arc<TestSourceReports>,
    }
    type ObservedTestOutput = std::pin::Pin<Box<ObservedTestStream>>;
    impl tokio_stream::Stream for ObservedTestStream {
        type Item = novarocks_execution::runtime::task::ExecutionResult<
            novarocks_execution::exec::chunk::Chunk,
        >;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if let Some(exit) = &mut this.exit
                && let std::task::Poll::Ready(proof) = exit.as_mut().poll(cx)
            {
                this.proof = Some(proof);
                this.exit = None;
            }
            this.stream.as_mut().poll_next(cx)
        }
    }
    fn claim(op: &Arc<dyn ScanOp>, budget: &ConnectorPollBudget) -> ObservedTestOutput {
        let mut stream = op
            .stream_source()
            .claim(budget.clone(), None, actual_stream_test_factory())
            .expect("claim the scan's stream");
        let failures = Arc::new(TestSourceReports::default());
        let observation = stream
            .as_mut()
            .take_exit_observation(failures.clone())
            .expect("claim the original source observer before reading");
        Box::pin(ObservedTestStream {
            stream,
            exit: Some(observation.actual_exit),
            proof: None,
            failures,
        })
    }
    fn close_observed(
        stream: ObservedTestOutput,
    ) -> ObservedTestFuture<novarocks_execution::runtime::task::ExecutionResult<()>> {
        let this = *std::pin::Pin::into_inner(stream);
        let close = novarocks_execution::exec::node::scan::ScanChunkStream::close(
            this.stream,
            this.failures,
        );
        Box::pin(async move {
            let exit = async move {
                match (this.proof, this.exit) {
                    (Some(proof), _) => proof,
                    (None, Some(exit)) => exit.await,
                    (None, None) => panic!("the original test observer remains owned until proof"),
                }
            };
            let (result, proof) = tokio::join!(close, exit);
            assert_eq!(
                proof,
                novarocks_execution::exec::node::scan::ScanExitProof::Exited,
                "a clean fixture close requires actual source proof"
            );
            result
        })
    }

    /// Polls as a driver does, one refilled turn per poll, until the stream
    /// yields an item or ends.
    fn next_item(
        stream: &mut ObservedTestOutput,
        budget: &ConnectorPollBudget,
    ) -> Option<
        novarocks_execution::runtime::task::ExecutionResult<
            novarocks_execution::exec::chunk::Chunk,
        >,
    > {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        for _ in 0..1_000 {
            budget.refill(64);
            if let std::task::Poll::Ready(item) = stream.as_mut().poll_next(&mut cx) {
                return item;
            }
        }
        panic!("the stream made no progress on ready input");
    }

    fn drain_values(stream: &mut ObservedTestOutput, budget: &ConnectorPollBudget) -> Vec<i64> {
        let mut values = Vec::new();
        while let Some(chunk) = next_item(stream, budget) {
            values.extend(chunk_values(&chunk.expect("chunk")));
        }
        values
    }

    fn stream_source_with(
        provider: Arc<dyn ConnectorReadPageSourceProvider>,
        queues: Arc<SplitQueues<ReceivedReadSplit>>,
        request: ConnectorRequestContext,
    ) -> TypedConnectorScanSource {
        TypedConnectorScanSource::new(
            descriptor(),
            provider,
            session(),
            request,
            queues,
            NODE,
            vec![SlotId::new(1)],
            Arc::new({
                // This source is explicitly Plain INT64; its Arrow field is
                // presentation only and cannot author its value domain.
                let value = novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                );
                let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
                let slot = novarocks_execution::exec::chunk::ChunkSlotSchema::try_new_typed(
                    SlotId::new(1),
                    Arc::new(arrow::datatypes::Field::new(
                        "value",
                        arrow::datatypes::DataType::Int64,
                        true,
                    )),
                    value.typed_binding().clone(),
                    None,
                    storage.clone(),
                )
                .expect("complete test output slot");
                novarocks_execution::exec::chunk::ChunkSchema::try_new_typed(vec![slot], storage)
                    .expect("complete test output schema")
            }),
            no_runtime_filter(),
            live_dynamic_filter_factory(),
            #[cfg(novarocks_dev_controls)]
            None,
            novarocks_worker::ScanStreamHost::new(
                novarocks_worker::ScanPreparationConfig::default(),
                crate::backend_test_support::test_scan_stream_runtime(),
            ),
        )
    }

    /// A request carrying the Task source its operations are admitted to.
    fn request_with_source(operations: &ConnectorSourceOperations) -> ConnectorRequestContext {
        execution_request(ConnectorStopOwner::new().view(), operations.clone())
    }

    fn close_now(stream: ObservedTestOutput) {
        crate::backend_test_support::test_scan_stream_runtime()
            .block_on(close_observed(stream))
            .expect("close the scan's stream and its actual source");
    }

    #[test]
    fn one_work_unit_and_an_empty_sibling_use_the_same_split_read_execution() {
        let execution = test_support::installed_read_execution();
        let read_unit = || {
            let raw = novarocks_proto_codec::connector_read::ScheduledSplit::parse(
                test_support::split_proto(NODE, 1),
                novarocks_proto_codec::FieldPath::root("unit"),
            )
            .unwrap();
            let (evidence, split) = execution
                .decoder()
                .decode_scheduled_split(&raw)
                .unwrap()
                .into_parts();
            ReceivedReadSplit::new(evidence.sequence_id(), evidence.plan_node_id(), split)
        };
        let provider = ScriptedStreamProvider::new(vec![vec![Some(int_page(vec![2, 3]))]]);
        let mut combined = Vec::new();
        for with_unit in [true, false] {
            let queues = attempt_queues();
            queues
                .queue(NODE)
                .offer_splits(
                    NODE,
                    if with_unit { vec![read_unit()] } else { vec![] },
                    true,
                )
                .unwrap();
            let source = stream_source_with(
                provider.clone(),
                queues,
                request(ConnectorStopOwner::new().view()),
            );
            let op = bind(&source);
            let budget = ConnectorPollBudget::new();
            let mut stream = claim(&op, &budget);
            let values = drain_values(&mut stream, &budget);
            if with_unit {
                assert_eq!(values, vec![2, 3]);
            } else {
                assert!(values.is_empty(), "zero units end without opening a stream");
            }
            combined.extend(values);
            close_now(stream);
        }
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
        let single = ScriptedStreamProvider::new(vec![vec![Some(int_page(vec![2, 3]))]]);
        let queues = attempt_queues();
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![read_unit()], true)
            .unwrap();
        let source = stream_source_with(
            single.clone(),
            queues,
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        assert_eq!(combined, drain_values(&mut stream, &budget));
        close_now(stream);
        assert_eq!(single.opens.load(Ordering::Acquire), 1);
        assert_eq!(single.closes.load(Ordering::Acquire), 1);
    }

    #[test]
    fn the_scan_stream_parks_on_an_empty_queue_and_a_split_wakes_it() {
        let queues = attempt_queues();
        let provider = ScriptedStreamProvider::new(vec![vec![Some(int_page(vec![1, 2, 3]))]]);
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);

        let wake = Arc::new(CountingWake::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        let mut cx = std::task::Context::from_waker(&waker);
        budget.refill(64);
        assert!(
            stream.as_mut().poll_next(&mut cx).is_pending(),
            "no split yet is not end of stream"
        );
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("the split arrives");
        assert!(
            wake.0.load(Ordering::Acquire) >= 1,
            "the arriving split wakes the driver"
        );
        assert_eq!(drain_values(&mut stream, &budget), vec![1, 2, 3]);
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
        close_now(stream);
    }

    #[test]
    fn the_scan_stream_reads_every_queued_split_in_order_across_pending_pages() {
        let queues = attempt_queues();
        let provider = ScriptedStreamProvider::new(vec![
            vec![None, Some(int_page(vec![1])), None],
            vec![Some(int_page(vec![2, 3]))],
        ]);
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1), scheduled_split(2)], true)
            .expect("two splits");
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        assert_eq!(drain_values(&mut stream, &budget), vec![1, 2, 3]);
        assert_eq!(provider.opens.load(Ordering::Acquire), 2);
        assert_eq!(
            provider.closes.load(Ordering::Acquire),
            2,
            "each split's stream is closed once, when it ends"
        );
        assert!(queues.queue(NODE).is_exhausted());
        // Every split's stream spends the scan's own turn budget.
        budget.refill(5);
        for handed in provider.budgets.lock().expect("budgets").iter() {
            assert_eq!(handed.remaining(), 5);
        }
        close_now(stream);
    }

    #[test]
    fn a_run_of_splits_that_end_at_once_still_yields_the_driver_s_turn() {
        let queues = attempt_queues();
        let provider = ScriptedStreamProvider::new(Vec::new());
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, (1..=100).map(scheduled_split).collect(), true)
            .expect("a hundred empty splits");
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);

        let wake = Arc::new(CountingWake::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        let mut cx = std::task::Context::from_waker(&waker);
        budget.refill(64);
        assert!(
            stream.as_mut().poll_next(&mut cx).is_pending(),
            "a turn switches through at most its budget of splits"
        );
        assert_eq!(budget.exhaustions(), 1);
        assert_eq!(wake.0.load(Ordering::Acquire), 1, "the yield wakes itself");
        assert_eq!(provider.opens.load(Ordering::Acquire), 65);
        assert!(drain_values(&mut stream, &budget).is_empty());
        assert_eq!(provider.opens.load(Ordering::Acquire), 100);
        assert_eq!(provider.closes.load(Ordering::Acquire), 100);
        close_now(stream);
    }

    #[test]
    fn the_scan_stream_promotes_prepared_claims_in_their_queue_order() {
        let queues = attempt_queues();
        let provider = PreparationProbeProvider::new(PreparationMode::Supported);
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, (1..=4).map(scheduled_split).collect(), true)
            .expect("four splits and the terminal marker");
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);

        let first = next_item(&mut stream, &budget)
            .expect("current split")
            .expect("chunk");
        assert_eq!(chunk_values(&first), vec![1]);
        let events = provider.events();
        assert!(events.contains(&"open:1".to_owned()), "{events:?}");
        assert!(events.contains(&"prepare:2".to_owned()), "{events:?}");
        assert!(
            !events.iter().any(|event| event.starts_with("promote:")),
            "prepared claims wait for the current split: {events:?}"
        );

        let mut values = vec![1];
        values.extend(drain_values(&mut stream, &budget));
        assert_eq!(values, vec![1, 11, 2, 3, 4]);
        let promoted: Vec<_> = provider
            .events()
            .into_iter()
            .filter(|event| event.starts_with("promote:"))
            .collect();
        assert_eq!(promoted, vec!["promote:2", "promote:3", "promote:4"]);
        assert_eq!(provider.closes.load(Ordering::Acquire), 4);
        close_now(stream);
    }

    #[test]
    fn the_scan_stream_reports_a_saved_preparation_failure_only_at_its_turn() {
        let queues = attempt_queues();
        let provider = PreparationProbeProvider::new(PreparationMode::FailOn(3));
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, (1..=3).map(scheduled_split).collect(), true)
            .expect("three splits");
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        let mut values = Vec::new();
        for _ in 0..3 {
            values.extend(chunk_values(
                &next_item(&mut stream, &budget)
                    .expect("an earlier split's chunk")
                    .expect("chunk"),
            ));
        }
        assert_eq!(values, vec![1, 11, 2]);
        let error = next_item(&mut stream, &budget)
            .expect("the saved failure")
            .expect_err("split 3 fails");
        assert!(error.to_string().contains("sequence 3"), "{error}");
        assert!(
            error
                .to_string()
                .contains("scripted future preparation failure"),
            "{error}"
        );
        assert!(next_item(&mut stream, &budget).is_none());
        close_now(stream);
    }

    #[test]
    fn the_scan_stream_opens_directly_when_the_provider_prepares_nothing() {
        let queues = attempt_queues();
        let provider = PreparationProbeProvider::new(PreparationMode::Unsupported);
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, (1..=3).map(scheduled_split).collect(), true)
            .expect("three splits");
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        assert_eq!(drain_values(&mut stream, &budget), vec![1, 11, 2, 3]);
        let events = provider.events();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.starts_with("open:"))
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["open:1", "open:2", "open:3"]
        );
        assert!(!events.iter().any(|event| event.starts_with("promote:")));
        close_now(stream);
    }

    #[test]
    fn terminating_the_scan_stops_its_stream_and_seals_its_task_source() {
        let queues = attempt_queues();
        let operations = ConnectorSourceOperations::new();
        let provider = ScriptedStreamProvider::new(vec![vec![
            Some(int_page(vec![1])),
            Some(int_page(vec![2])),
        ]]);
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request_with_source(&operations),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], false)
            .expect("one split");
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        next_item(&mut stream, &budget)
            .expect("first page")
            .expect("chunk");

        op.terminate().expect("terminate");
        op.terminate().expect("terminate is idempotent");
        assert!(queues.queue(NODE).is_closed());
        assert!(
            operations.is_sealed(),
            "no operation of the scan starts after it"
        );
        close_now(stream);
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
        assert!(operations.is_exited());
    }

    #[test]
    fn closing_the_scan_stream_seals_its_source_and_waits_for_its_operations() {
        let queues = attempt_queues();
        let operations = ConnectorSourceOperations::new();
        let provider = ScriptedStreamProvider::new(Vec::new());
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request_with_source(&operations),
        );
        let op = bind(&source);
        let budget = ConnectorPollBudget::new();
        let stream = claim(&op, &budget);
        // An operation the scan admitted and that is still running.
        let running = operations.admit(Arc::new(|| {})).expect("admitted");

        let runtime = crate::backend_test_support::test_scan_stream_runtime();
        let mut closed = close_observed(stream);
        assert!(operations.is_sealed());
        assert!(queues.queue(NODE).is_closed());
        assert!(
            runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_millis(50), &mut closed).await
                })
                .is_err(),
            "the close waits for the running operation"
        );
        running.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
        runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), closed).await })
            .expect("the close resolves once the operation exited")
            .expect("a clean exit");
    }

    #[test]
    fn a_cancelled_attempt_opens_no_provider() {
        let provider = ScriptedStreamProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let owner = ConnectorStopOwner::new();
        owner.request_stop();
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            attempt_queues(),
            request(owner.view()),
        );
        let error = source
            .bind(BoundScanRanges::None)
            .err()
            .expect("a cancelled attempt must not open a provider");
        assert!(
            error.to_string().contains("cancelled"),
            "unexpected error: {error}"
        );
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
    }

    #[test]
    fn a_terminated_scan_opens_no_further_split() {
        let queues = attempt_queues();
        let provider = ScriptedStreamProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");
        op.terminate().expect("terminate");

        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        assert!(
            next_item(&mut stream, &budget).is_none(),
            "a terminated scan delivers nothing"
        );
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
        close_now(stream);
    }

    #[test]
    fn the_scan_stream_hands_the_substituted_dynamic_filter_to_the_provider() {
        let queues = attempt_queues();
        let provider = ScriptedStreamProvider::new(vec![Vec::new()]);
        let covered = BTreeSet::from([test_support::decoded_scan().assignments()[0]
            .column()
            .clone()]);
        let source = stream_source_with(
            Arc::clone(&provider) as Arc<dyn ConnectorReadPageSourceProvider>,
            Arc::clone(&queues),
            request(ConnectorStopOwner::new().view()),
        )
        .with_backend_dynamic_filter(Arc::new(CompleteAllDynamicFilter::new(covered)));
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        assert!(drain_values(&mut stream, &budget).is_empty());
        assert_eq!(*provider.filter_columns.lock().expect("filter"), Some(1));
        close_now(stream);
    }

    #[test]
    fn normal_split_cleanup_does_not_replace_required_work_or_end_later_split_data() {
        use novarocks_execution::runtime::task::{
            ExecutionFailureCause, connector_failure::ConnectorOperation,
        };
        let queues = attempt_queues();
        let provider = ScriptedStreamProvider::new(vec![
            vec![Some(int_page(vec![1]))],
            vec![Some(int_page(vec![2]))],
        ]);
        let cleanup = ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "the actual first reader close failed",
        );
        *provider.close_error.lock().unwrap() = Some(cleanup.clone());
        let source = stream_source_with(
            provider.clone(),
            queues.clone(),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1), scheduled_split(2)], true)
            .unwrap();
        let budget = ConnectorPollBudget::new();
        let mut stream = claim(&op, &budget);
        let reports = stream.failures.clone();
        assert_eq!(
            drain_values(&mut stream, &budget),
            vec![1, 2],
            "cleanup does not cancel required sibling reads"
        );
        assert_eq!(provider.closes.load(Ordering::Acquire), 2);
        assert!(
            reports.runtime.error().is_none(),
            "cleanup cannot accept an originating Task failure"
        );
        assert!(
            reports.runtime.local_exit_fault().is_none(),
            "an ordinary close error is not missing exit proof"
        );
        let recorded = reports.cleanup.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        let ExecutionFailureCause::Connector(actual) = recorded[0].cause() else {
            panic!("the original close author remains typed");
        };
        assert_eq!(actual.operation(), ConnectorOperation::CloseReader);
        assert_eq!(actual.cause(), &cleanup);
        close_now(stream);
        assert_eq!(
            reports.cleanup.lock().unwrap().len(),
            1,
            "terminal cleanup does not repeat the consumed split close"
        );
        assert!(reports.runtime.error().is_none());
    }

    #[test]
    fn a_scan_hands_out_its_stream_once() {
        let source = stream_source_with(
            ScriptedStreamProvider::new(Vec::new()) as Arc<dyn ConnectorReadPageSourceProvider>,
            attempt_queues(),
            request(ConnectorStopOwner::new().view()),
        );
        let op = bind(&source);
        let budget = ConnectorPollBudget::new();
        let stream = claim(&op, &budget);
        let refused = op
            .stream_source()
            .claim(budget.clone(), None, actual_stream_test_factory());
        assert!(
            refused.is_err(),
            "a second claim is refused, never a second reader"
        );
        close_now(stream);
    }
}
