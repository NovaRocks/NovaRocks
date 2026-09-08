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

//! The frontend-side StarRocks control generation.
//!
//! It resolves metadata and declares its backend execution binding. Its scan
//! planning capability exists only because the control binding requires one;
//! every entry point on it refuses. See the crate documentation for what the
//! read cut removed.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorProviderBinding, ConnectorProviderId, ConnectorRequestContext, ConnectorScan,
    ConnectorScanHandle, ConnectorScanPlanning, ConnectorSplitPlanningRequest,
    ConnectorSplitPlanningResult, ConnectorTableDefinitionFacts, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTablePlanningFacts,
    ConnectorTableRequest, ProviderBindingEpoch, StatisticsDataVersion,
};
use serde::Serialize;

use crate::codec::{Base64Bytes, CODEC_VERSION, encode_v1};
use crate::domain::{StarRocksConnectorConfig, StarRocksResolvedTable, invalid};
use crate::{STARROCKS_PROVIDER_ID, starrocks_read_unsupported};

/// The external StarRocks facts a control generation is composed over.
///
/// The implementation owns every remote call; the connector owns only how the
/// answers become NovaRocks catalog facts.
pub trait StarRocksMetadataSource: Send + Sync {
    fn namespace_exists(
        &self,
        namespace: &str,
        context: &ConnectorRequestContext,
    ) -> Result<bool, ConnectorError>;
    fn table_exists(
        &self,
        namespace: &str,
        table: &str,
        context: &ConnectorRequestContext,
    ) -> Result<bool, ConnectorError>;
    fn list_tables(
        &self,
        namespace: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, ConnectorError>;
    fn load_table(
        &self,
        namespace: &str,
        table: &str,
        context: &ConnectorRequestContext,
    ) -> Result<StarRocksResolvedTable, ConnectorError>;
}

pub struct StarRocksControlGeneration;

impl StarRocksControlGeneration {
    pub fn try_new(
        config: StarRocksConnectorConfig,
        metadata: Arc<dyn StarRocksMetadataSource>,
    ) -> Result<ConnectorControlBinding, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(STARROCKS_PROVIDER_ID)?,
            instance_id: config.instance_id.clone(),
        };
        let incarnation = ProviderBindingEpoch::new();
        let provider = Arc::new(Provider {
            descriptor: descriptor.clone(),
            incarnation,
            config,
            metadata,
        });
        ConnectorControlBinding::try_new(
            descriptor,
            incarnation,
            provider.clone(),
            provider.clone(),
            provider,
            None,
        )
    }
}

struct Provider {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    config: StarRocksConnectorConfig,
    metadata: Arc<dyn StarRocksMetadataSource>,
}

/// The provider-private payload of a StarRocks table handle.
///
/// It carries only the identity of one resolved table and the generation that
/// minted it. This is a metadata handle, not a read handle: nothing in this
/// crate decodes it, and no scan is ever planned from it.
#[derive(Serialize)]
struct TablePayload {
    version: u16,
    owner: String,
    incarnation: Base64Bytes,
    namespace: String,
    table: String,
    schema_version: Base64Bytes,
    data_version: Base64Bytes,
}

impl Provider {
    fn active(&self, context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "StarRocks connector request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "StarRocks connector request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn ensure_identity(&self, instance: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        if instance != &self.descriptor.instance_id {
            return Err(invalid(
                "StarRocks connector request belongs to another instance",
            ));
        }
        Ok(())
    }

    fn encode_table(
        &self,
        table: StarRocksResolvedTable,
        max: usize,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        let payload = TablePayload {
            version: CODEC_VERSION,
            owner: self.descriptor.instance_id.as_str().to_string(),
            incarnation: Base64Bytes(Bytes::copy_from_slice(&self.incarnation.to_bytes())),
            namespace: table.namespace.to_string(),
            table: table.table.to_string(),
            schema_version: Base64Bytes(table.schema_version.clone()),
            data_version: Base64Bytes(table.data_version.clone()),
        };
        let table_handle = ConnectorTableHandle::try_new(
            self.descriptor.instance_id.clone(),
            encode_v1(&payload, "table handle", max)?,
        )?;
        Ok(ConnectorTableMetadata {
            identity: ConnectorTableIdentity {
                instance_id: self.descriptor.instance_id.clone(),
                namespace: table.namespace,
                table: table.table,
            },
            schema: table.schema,
            planning_facts: ConnectorTablePlanningFacts::empty(),
            definition_facts: ConnectorTableDefinitionFacts::empty(),
            version: Some(table.schema_version),
            statistics_data_version: Some(StatisticsDataVersion::try_new(table.data_version)?),
            table: table_handle,
        })
    }
}

impl ConnectorMetadata for Provider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }
    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        self.active(&request.context)?;
        self.ensure_identity(&request.namespace.instance_id)?;
        self.metadata
            .namespace_exists(&request.namespace.namespace, &request.context)
    }
    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        self.active(&request.context)?;
        self.ensure_identity(&request.table.instance_id)?;
        self.metadata.table_exists(
            &request.table.namespace,
            &request.table.table,
            &request.context,
        )
    }
    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        self.active(&request.context)?;
        self.ensure_identity(&request.namespace.instance_id)?;
        self.metadata
            .list_tables(&request.namespace.namespace, &request.context)?
            .into_iter()
            .map(|table| {
                Ok(ConnectorTableIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    table: Arc::from(table),
                })
            })
            .collect()
    }
    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.active(&request.context)?;
        self.ensure_identity(&request.table.instance_id)?;
        self.encode_table(
            self.metadata.load_table(
                &request.table.namespace,
                &request.table.table,
                &request.context,
            )?,
            request.context.max_handle_payload_bytes(),
        )
    }
}

impl ConnectorScanPlanning for Provider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }

    /// Refuses before decoding the table handle or reaching the metadata
    /// source, so a StarRocks scan never freezes facts it cannot execute and
    /// no remote call is made on the way to a certain refusal.
    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(starrocks_read_unsupported())
    }

    /// Unreachable through a scan this provider began; it still refuses
    /// unconditionally so that no fabricated scan handle can reach a planner.
    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        Err(starrocks_read_unsupported())
    }
}

impl ConnectorExecutionDistribution for Provider {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        self.active(context)?;
        ConnectorProviderBinding::starrocks(
            self.descriptor.instance_id.as_str(),
            self.incarnation.to_bytes(),
            self.config.local_binding.as_str(),
        )
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("build StarRocks provider binding: {error}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::time::Duration;

    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorCancellation, ConnectorReadSelector, ConnectorScanSelection,
        ConnectorTableObjectCaptureRequest, ConnectorTableObjectId,
        ConnectorTableObjectRebindRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
    };

    use super::*;
    use crate::STARROCKS_READ_UNSUPPORTED;
    use crate::domain::StarRocksLocalBindingRef;
    use crate::remote_control::{
        StarRocksHttpRequest, StarRocksHttpTransport, StarRocksRemoteControlClient,
        StarRocksRemoteControlConfig, StarRocksRemoteMetadataSource,
    };

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(2),
            Arc::new(NeverCancelled),
            64 * 1024,
            128 * 1024,
        )
        .expect("request context")
    }

    /// Records every remote call the connector makes, so a test can prove that
    /// a refusal happened before the connector reached its cluster at all.
    #[derive(Default)]
    struct RecordingTransport {
        requests: Mutex<Vec<String>>,
    }

    impl RecordingTransport {
        fn request_count(&self) -> usize {
            self.requests.lock().expect("recorded requests").len()
        }
    }

    impl StarRocksHttpTransport for RecordingTransport {
        fn request(&self, request: StarRocksHttpRequest<'_>) -> Result<Bytes, ConnectorError> {
            self.requests
                .lock()
                .expect("recorded requests")
                .push(request.url.to_string());
            Ok(Bytes::from_static(match request.url.path() {
                path if path.ends_with("/databases") => br#"{"status":200,"databases":["db"]}"#,
                path if path.ends_with("/tables") => br#"{"status":200,"tables":["t"]}"#,
                _ => br#"{"status":200}"#,
            }))
        }
    }

    fn binding() -> (ConnectorControlBinding, Arc<RecordingTransport>) {
        let transport = Arc::new(RecordingTransport::default());
        let client = Arc::new(StarRocksRemoteControlClient::with_transport(
            StarRocksRemoteControlConfig::try_new(
                &["https://fe.example:8030".to_string()],
                "user",
                "password",
                Duration::from_secs(1),
                0,
            )
            .expect("remote control configuration"),
            Arc::clone(&transport) as Arc<dyn StarRocksHttpTransport>,
        ));
        let binding = StarRocksControlGeneration::try_new(
            StarRocksConnectorConfig::new(
                ConnectorInstanceId::parse("catalog.starrocks").expect("instance ID"),
                StarRocksLocalBindingRef::parse("default").expect("local binding"),
            ),
            Arc::new(StarRocksRemoteMetadataSource::new(client)),
        )
        .expect("StarRocks control binding");
        (binding, transport)
    }

    #[test]
    fn provider_binding_is_the_typed_starrocks_local_binding() {
        let (binding, _) = binding();
        let declaration = binding
            .provider_binding(&context())
            .expect("typed declaration");

        assert_eq!(declaration.provider_id().as_str(), "starrocks");
        assert_eq!(declaration.starrocks_local_binding(), Some("default"));
    }

    #[test]
    fn scan_planning_refuses_with_the_stable_message_before_any_remote_call() {
        let (binding, transport) = binding();
        // Both carriers are fabricated on purpose. This provider never mints a
        // scan handle, and the owner below belongs to another instance: if the
        // refusal were preceded by identity or payload validation, the kind
        // would not be Unsupported.
        let foreign = ConnectorInstanceId::parse("catalog.other").expect("foreign instance ID");
        let table = ConnectorTableHandle::try_new(
            foreign.clone(),
            Bytes::from_static(b"not a StarRocks table handle"),
        )
        .expect("fabricated table handle");
        let scan =
            ConnectorScanHandle::try_new(foreign, Bytes::from_static(b"not a StarRocks scan"))
                .expect("fabricated scan handle");

        let begin = binding
            .planning()
            .begin_scan(
                &table,
                ConnectorBeginScanRequest {
                    projection: vec![0],
                    static_predicates: Vec::new(),
                    selection: ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
                    purpose: novarocks_spi::connector::ConnectorReadPurpose::Query,
                    limit: None,
                    batch: ConnectorBatchBudget {
                        max_rows: NonZeroUsize::new(1).expect("batch rows"),
                        max_bytes: NonZeroUsize::new(1024).expect("batch bytes"),
                    },
                    context: context(),
                },
            )
            .expect_err("StarRocks must not begin a scan");
        assert_eq!(begin.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(begin.message(), STARROCKS_READ_UNSUPPORTED);

        let planned = binding
            .planning()
            .plan_splits(
                &scan,
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                    max_split_bytes: None,
                    context: context(),
                },
            )
            .expect_err("StarRocks must not plan splits");
        assert_eq!(planned.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(planned.message(), STARROCKS_READ_UNSUPPORTED);

        assert_eq!(transport.request_count(), 0);
    }

    #[test]
    fn metadata_still_resolves_namespaces_and_tables_through_the_remote_cluster() {
        let (binding, transport) = binding();

        let namespace = novarocks_spi::connector::ConnectorNamespaceIdentity {
            instance_id: binding.descriptor().instance_id.clone(),
            namespace: Arc::from("db"),
        };
        assert!(
            binding
                .metadata()
                .namespace_exists(ConnectorNamespaceRequest {
                    namespace: namespace.clone(),
                    context: context(),
                })
                .expect("namespace lookup")
        );
        assert_eq!(
            binding
                .metadata()
                .list_tables(ConnectorListTablesRequest {
                    namespace,
                    context: context(),
                })
                .expect("table listing")
                .into_iter()
                .map(|identity| identity.table.to_string())
                .collect::<Vec<_>>(),
            ["t"]
        );
        assert_eq!(transport.request_count(), 2);
    }

    #[test]
    fn physical_table_object_binding_is_explicitly_unsupported() {
        let (binding, _) = binding();
        let table = ConnectorTableIdentity {
            instance_id: binding.descriptor().instance_id.clone(),
            namespace: Arc::from("db"),
            table: Arc::from("t"),
        };

        let capture = match binding.metadata().capture_table_object_binding(
            ConnectorTableObjectCaptureRequest {
                table: table.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: context(),
            },
        ) {
            Ok(_) => panic!("StarRocks must not synthesize a physical table object ID"),
            Err(error) => error,
        };
        assert_eq!(capture.kind(), ConnectorErrorKind::Unsupported);
        assert!(!capture.is_table_object_binding_failure());
        assert_eq!(capture.table_object_binding_failure(), None);

        let rebind = match binding.metadata().rebind_table_object_binding(
            ConnectorTableObjectRebindRequest {
                table,
                expected_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"id"))
                    .expect("test object ID"),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: context(),
            },
        ) {
            Ok(_) => panic!("StarRocks must not rebind a physical table object ID"),
            Err(error) => error,
        };
        assert_eq!(rebind.kind(), ConnectorErrorKind::Unsupported);
        assert!(!rebind.is_table_object_binding_failure());
        assert_eq!(rebind.table_object_binding_failure(), None);
    }
}
