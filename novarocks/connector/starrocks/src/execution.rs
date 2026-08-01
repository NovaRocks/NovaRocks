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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorExecutionBinding,
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorExecutionInstaller,
    ConnectorOpenReaderRequest, ConnectorProviderId, ConnectorReadExecution,
    ConnectorRequestContext, ConnectorSplit,
};

use crate::STARROCKS_PROVIDER_ID;
use crate::codec::{encode_schema_ipc, schema_digest};
use crate::control::{
    StrategyPayload, decode_declaration, decode_split, direct_outer_facts,
    split_output_schema_digest, split_strategy, validate_split_generation,
};
use crate::direct::{StarRocksDirectSplit, decode_direct_split};
use crate::domain::{
    StarRocksLocalBindingRef, StarRocksRpcOpaquePayload, StarRocksRpcTransport,
    StarRocksSelectedStrategy, unavailable,
};

pub trait StarRocksRpcReaderFactory: Send + Sync {
    fn open_rpc_reader(
        &self,
        transport: StarRocksRpcTransport,
        payload: StarRocksRpcOpaquePayload,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError>;
}

pub trait StarRocksDirectReaderFactory: Send + Sync {
    fn open_direct_reader(
        &self,
        split: StarRocksDirectSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError>;
}

#[derive(Default)]
pub struct StarRocksLocalExecutionBinding {
    pub rpc: Option<Arc<dyn StarRocksRpcReaderFactory>>,
    pub direct: Option<Arc<dyn StarRocksDirectReaderFactory>>,
}

#[derive(Default)]
pub struct StarRocksExecutionBindings {
    entries: BTreeMap<StarRocksLocalBindingRef, StarRocksLocalExecutionBinding>,
}

impl StarRocksExecutionBindings {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(
        &mut self,
        binding: StarRocksLocalBindingRef,
        value: StarRocksLocalExecutionBinding,
    ) -> Option<StarRocksLocalExecutionBinding> {
        self.entries.insert(binding, value)
    }
    fn get(&self, binding: &StarRocksLocalBindingRef) -> Option<&StarRocksLocalExecutionBinding> {
        self.entries.get(binding)
    }
}

pub struct StarRocksExecutionInstaller {
    provider_id: ConnectorProviderId,
    bindings: StarRocksExecutionBindings,
}

impl StarRocksExecutionInstaller {
    pub fn new(bindings: StarRocksExecutionBindings) -> Self {
        Self {
            provider_id: ConnectorProviderId::parse(STARROCKS_PROVIDER_ID)
                .expect("valid StarRocks provider ID"),
            bindings,
        }
    }
}

impl ConnectorExecutionInstaller for StarRocksExecutionInstaller {
    fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    fn install(
        &self,
        declaration: &ConnectorExecutionDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionBinding, ConnectorError> {
        ensure_active(context)?;
        if declaration.descriptor().provider_id != self.provider_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "StarRocks installer received a declaration for another provider",
            ));
        }
        let payload = decode_declaration(declaration.payload())?;
        let local_binding = StarRocksLocalBindingRef::parse(payload.local_binding)?;
        let binding = self
            .bindings
            .get(&local_binding)
            .ok_or_else(|| unavailable("StarRocks local execution binding is unavailable"))?;
        let key = declaration.binding_key();
        ConnectorExecutionBinding::try_new(
            self.provider_id.clone(),
            key.clone(),
            Arc::new(CompositeReadExecution {
                key,
                rpc: binding.rpc.clone(),
                direct: binding.direct.clone(),
            }),
        )
    }
}

struct CompositeReadExecution {
    key: ConnectorExecutionBindingKey,
    rpc: Option<Arc<dyn StarRocksRpcReaderFactory>>,
    direct: Option<Arc<dyn StarRocksDirectReaderFactory>>,
}

impl ConnectorReadExecution for CompositeReadExecution {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        ensure_active(&request.context)?;
        if split.owner() != &self.key.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "StarRocks split belongs to another connector instance",
            ));
        }
        let split = decode_split(split.payload())?;
        validate_split_generation(&split, &self.key)?;
        let encoded_schema = encode_schema_ipc(request.expected_schema.as_ref())?;
        if schema_digest(&encoded_schema).as_slice() != split_output_schema_digest(&split) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks split output schema does not match the requested schema",
            ));
        }
        let direct_outer = direct_outer_facts(&split)?;
        match (split_strategy(&split), split.strategy_payload) {
            (StarRocksSelectedStrategy::Rpc { transport }, StrategyPayload::Rpc { payload }) => {
                self.rpc
                    .as_ref()
                    .ok_or_else(|| unavailable("StarRocks RPC reader factory is unavailable"))?
                    .open_rpc_reader(
                        transport,
                        StarRocksRpcOpaquePayload::new(payload.0)?,
                        request,
                    )
            }
            (
                StarRocksSelectedStrategy::SharedDataDirect,
                StrategyPayload::SharedDataDirect { payload },
            ) => {
                let direct = decode_direct_split(&payload.0, &direct_outer)?;
                self.direct
                    .as_ref()
                    .ok_or_else(|| {
                        unavailable("StarRocks shared-data direct reader factory is unavailable")
                    })?
                    .open_direct_reader(direct, request)
            }
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "StarRocks split tag does not match its frozen strategy",
            )),
        }
    }
}

fn ensure_active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorCancellation, ConnectorControlBinding, ConnectorInstanceId,
        ConnectorReadSelector, ConnectorSplitPlanningRequest, ConnectorTableIdentity,
        ConnectorTableRequest, ConnectorTableResolution,
    };

    use super::*;
    use crate::{
        StarRocksCapabilitySnapshot, StarRocksConnectorConfig, StarRocksControlGeneration,
        StarRocksDirectColumnBinding, StarRocksDirectLocation, StarRocksDirectLocationSource,
        StarRocksDirectMetadataLayout, StarRocksDirectSplitPlanner,
        StarRocksDirectTabletDescriptor, StarRocksDirectTabletPlanningSource,
        StarRocksMetadataSource, StarRocksReadPolicy, StarRocksResolvedTable,
        StarRocksRpcSplitPlanner, StarRocksSharedDataDirectPlanner, StarRocksSplitPlanningInput,
        StarRocksStorageBindingRef, StarRocksStrategySplit, StarRocksStrategySplitPayload,
        StarRocksTopology,
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
        .expect("context")
    }

    struct Source;
    impl StarRocksMetadataSource for Source {
        fn namespace_exists(
            &self,
            _: &str,
            _: &ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn table_exists(
            &self,
            _: &str,
            _: &str,
            _: &ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn list_tables(
            &self,
            _: &str,
            _: &ConnectorRequestContext,
        ) -> Result<Vec<String>, ConnectorError> {
            Ok(vec!["t".into()])
        }
        fn load_table(
            &self,
            namespace: &str,
            table: &str,
            _: &ConnectorRequestContext,
        ) -> Result<StarRocksResolvedTable, ConnectorError> {
            StarRocksResolvedTable::try_new(
                namespace,
                table,
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                StarRocksTopology::SharedNothing,
                Bytes::from_static(b"schema-v1"),
                Bytes::from_static(b"data-v1"),
                StarRocksCapabilitySnapshot {
                    api_contract_version: 1,
                    rpc_transports: [StarRocksRpcTransport::BrpcChunk].into(),
                    rpc_ready: true,
                    direct_contract_version: None,
                    direct_ready: false,
                },
            )
        }
    }

    struct RpcPlanner;
    impl StarRocksRpcSplitPlanner for RpcPlanner {
        fn plan_rpc_splits(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
            Ok(vec![StarRocksStrategySplit {
                split_id: Arc::from("rpc-1"),
                payload: StarRocksStrategySplitPayload::Rpc(
                    StarRocksRpcOpaquePayload::new(Bytes::from_static(b"query-token")).unwrap(),
                ),
                estimated_bytes: Some(8),
            }])
        }
    }
    struct DirectPlanner;
    impl StarRocksDirectSplitPlanner for DirectPlanner {
        fn plan_direct_splits(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "direct planner must not be called",
            ))
        }
    }

    struct ReadyDirectSource;
    impl StarRocksMetadataSource for ReadyDirectSource {
        fn namespace_exists(
            &self,
            _: &str,
            _: &ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn table_exists(
            &self,
            _: &str,
            _: &str,
            _: &ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn list_tables(
            &self,
            _: &str,
            _: &ConnectorRequestContext,
        ) -> Result<Vec<String>, ConnectorError> {
            Ok(vec!["t".into()])
        }
        fn load_table(
            &self,
            namespace: &str,
            table: &str,
            _: &ConnectorRequestContext,
        ) -> Result<StarRocksResolvedTable, ConnectorError> {
            StarRocksResolvedTable::try_new(
                namespace,
                table,
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                StarRocksTopology::SharedData,
                Bytes::from_static(b"schema-v1"),
                Bytes::from_static(b"data-v1"),
                StarRocksCapabilitySnapshot {
                    api_contract_version: 1,
                    rpc_transports: Default::default(),
                    rpc_ready: false,
                    direct_contract_version: Some(1),
                    direct_ready: true,
                },
            )
        }
    }

    struct ReadyDirectTablets;
    impl StarRocksDirectTabletPlanningSource for ReadyDirectTablets {
        fn plan_tablets(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksDirectTabletDescriptor>, ConnectorError> {
            Ok(vec![StarRocksDirectTabletDescriptor::try_new(
                1,
                2,
                3,
                StarRocksDirectMetadataLayout::Standalone,
                "meta/0001.meta",
                vec![StarRocksDirectColumnBinding::try_new(
                    0, 1, "id", "BIGINT", false, None,
                )?],
                Some(11),
            )?])
        }
    }
    struct ReadyDirectLocations;
    impl StarRocksDirectLocationSource for ReadyDirectLocations {
        fn resolve_locations(
            &self,
            _: &[i64],
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksDirectLocation>, ConnectorError> {
            Ok(vec![StarRocksDirectLocation::try_new(
                1,
                "s3://bucket/tablet",
                StarRocksStorageBindingRef::parse("volume-a")?,
                "fs-key",
            )?])
        }
    }

    fn control() -> ConnectorControlBinding {
        StarRocksControlGeneration::try_new(
            StarRocksConnectorConfig::new(
                ConnectorInstanceId::parse("catalog.starrocks").unwrap(),
                StarRocksReadPolicy::Rpc,
                StarRocksRpcTransport::BrpcChunk,
                StarRocksLocalBindingRef::parse("test").unwrap(),
            ),
            Arc::new(Source),
            Arc::new(RpcPlanner),
            Arc::new(DirectPlanner),
        )
        .unwrap()
    }

    fn direct_control() -> ConnectorControlBinding {
        StarRocksControlGeneration::try_new(
            StarRocksConnectorConfig::new(
                ConnectorInstanceId::parse("catalog.starrocks").unwrap(),
                StarRocksReadPolicy::Direct,
                StarRocksRpcTransport::BrpcChunk,
                StarRocksLocalBindingRef::parse("test").unwrap(),
            ),
            Arc::new(ReadyDirectSource),
            Arc::new(RpcPlanner),
            Arc::new(StarRocksSharedDataDirectPlanner::new(
                Arc::new(ReadyDirectTablets),
                Arc::new(ReadyDirectLocations),
            )),
        )
        .unwrap()
    }

    fn planned_read(
        binding: &ConnectorControlBinding,
    ) -> (
        ConnectorExecutionDeclaration,
        ConnectorSplit,
        arrow::datatypes::SchemaRef,
    ) {
        let context = context();
        let table = binding
            .metadata()
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: binding.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            })
            .unwrap();
        let scan = binding
            .planning()
            .begin_scan(
                &table.table,
                novarocks_spi::connector::ConnectorBeginScanRequest {
                    projection: vec![0],
                    static_predicates: vec![],
                    selector: ConnectorReadSelector::Current,
                    limit: None,
                    batch: ConnectorBatchBudget {
                        max_rows: NonZeroUsize::new(32).unwrap(),
                        max_bytes: NonZeroUsize::new(4096).unwrap(),
                    },
                    context: context.clone(),
                },
            )
            .unwrap();
        let splits = binding
            .planning()
            .plan_splits(
                &scan.handle,
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).unwrap(),
                    max_split_bytes: None,
                    context: context.clone(),
                },
            )
            .unwrap();
        (
            binding.execution_declaration(&context).unwrap(),
            splits.splits.into_iter().next().unwrap(),
            scan.output_schema,
        )
    }

    struct Reader {
        batch: Option<RecordBatch>,
    }
    impl ConnectorBatchReader for Reader {
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
            Ok(self.batch.take())
        }
        fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }
    struct RpcFactory(Arc<AtomicUsize>);
    impl StarRocksRpcReaderFactory for RpcFactory {
        fn open_rpc_reader(
            &self,
            _: StarRocksRpcTransport,
            payload: StarRocksRpcOpaquePayload,
            request: ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
            assert_eq!(payload.as_bytes(), &Bytes::from_static(b"query-token"));
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Reader {
                batch: Some(
                    RecordBatch::try_new(
                        request.expected_schema,
                        vec![Arc::new(Int64Array::from(vec![7_i64]))],
                    )
                    .unwrap(),
                ),
            }))
        }
    }
    struct DirectFactory(Arc<AtomicUsize>);
    impl StarRocksDirectReaderFactory for DirectFactory {
        fn open_direct_reader(
            &self,
            _: StarRocksDirectSplit,
            _: ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "direct factory must not be called",
            ))
        }
    }

    struct ReadyDirectFactory(Arc<AtomicUsize>);
    impl StarRocksDirectReaderFactory for ReadyDirectFactory {
        fn open_direct_reader(
            &self,
            payload: StarRocksDirectSplit,
            request: ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
            assert_eq!(payload.tablet_id(), 1);
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Reader {
                batch: Some(
                    RecordBatch::try_new(
                        request.expected_schema,
                        vec![Arc::new(Int64Array::from(vec![8_i64]))],
                    )
                    .unwrap(),
                ),
            }))
        }
    }

    #[test]
    fn composite_execution_dispatches_only_the_frozen_rpc_factory() {
        let binding = control();
        let (declaration, split, schema) = planned_read(&binding);
        assert!(String::from_utf8_lossy(split.payload()).contains("cXVlcnktdG9rZW4"));
        assert!(!format!("{declaration:?}").contains("query-token"));
        let rpc_calls = Arc::new(AtomicUsize::new(0));
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let mut bindings = StarRocksExecutionBindings::new();
        bindings.insert(
            StarRocksLocalBindingRef::parse("test").unwrap(),
            StarRocksLocalExecutionBinding {
                rpc: Some(Arc::new(RpcFactory(rpc_calls.clone()))),
                direct: Some(Arc::new(DirectFactory(direct_calls.clone()))),
            },
        );
        let installed = StarRocksExecutionInstaller::new(bindings)
            .install(&declaration, &context())
            .unwrap();
        let mut reader = installed
            .read()
            .unwrap()
            .open_reader(
                &split,
                ConnectorOpenReaderRequest {
                    expected_schema: schema,
                    batch: ConnectorBatchBudget {
                        max_rows: NonZeroUsize::new(32).unwrap(),
                        max_bytes: NonZeroUsize::new(4096).unwrap(),
                    },
                    context: context(),
                },
            )
            .unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap().num_rows(), 1);
        assert_eq!(rpc_calls.load(Ordering::SeqCst), 1);
        assert_eq!(direct_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn missing_selected_factory_does_not_fall_back() {
        let binding = control();
        let (declaration, split, schema) = planned_read(&binding);
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let mut bindings = StarRocksExecutionBindings::new();
        bindings.insert(
            StarRocksLocalBindingRef::parse("test").unwrap(),
            StarRocksLocalExecutionBinding {
                rpc: None,
                direct: Some(Arc::new(DirectFactory(direct_calls.clone()))),
            },
        );
        let installed = StarRocksExecutionInstaller::new(bindings)
            .install(&declaration, &context())
            .unwrap();
        let error = match installed.read().unwrap().open_reader(
            &split,
            ConnectorOpenReaderRequest {
                expected_schema: schema,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(32).unwrap(),
                    max_bytes: NonZeroUsize::new(4096).unwrap(),
                },
                context: context(),
            },
        ) {
            Ok(_) => panic!("missing RPC factory must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
        assert_eq!(direct_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn composite_execution_dispatches_the_frozen_shared_data_direct_factory() {
        let binding = direct_control();
        let (declaration, split, schema) = planned_read(&binding);
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let mut bindings = StarRocksExecutionBindings::new();
        bindings.insert(
            StarRocksLocalBindingRef::parse("test").unwrap(),
            StarRocksLocalExecutionBinding {
                rpc: None,
                direct: Some(Arc::new(ReadyDirectFactory(direct_calls.clone()))),
            },
        );
        let installed = StarRocksExecutionInstaller::new(bindings)
            .install(&declaration, &context())
            .unwrap();
        let mut reader = installed
            .read()
            .unwrap()
            .open_reader(
                &split,
                ConnectorOpenReaderRequest {
                    expected_schema: schema,
                    batch: ConnectorBatchBudget {
                        max_rows: NonZeroUsize::new(32).unwrap(),
                        max_bytes: NonZeroUsize::new(4096).unwrap(),
                    },
                    context: context(),
                },
            )
            .unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap().num_rows(), 1);
        assert_eq!(direct_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn separate_backend_local_bindings_do_not_share_direct_clients() {
        let binding = direct_control();
        let (declaration, split, schema) = planned_read(&binding);
        let be_one_calls = Arc::new(AtomicUsize::new(0));
        let be_two_calls = Arc::new(AtomicUsize::new(0));
        let installer = |calls: Arc<AtomicUsize>| {
            let mut bindings = StarRocksExecutionBindings::new();
            bindings.insert(
                StarRocksLocalBindingRef::parse("test").unwrap(),
                StarRocksLocalExecutionBinding {
                    rpc: None,
                    direct: Some(Arc::new(ReadyDirectFactory(calls))),
                },
            );
            StarRocksExecutionInstaller::new(bindings)
        };
        let open = |installer: StarRocksExecutionInstaller, schema: arrow::datatypes::SchemaRef| {
            let installed = installer.install(&declaration, &context()).unwrap();
            installed
                .read()
                .unwrap()
                .open_reader(
                    &split,
                    ConnectorOpenReaderRequest {
                        expected_schema: schema,
                        batch: ConnectorBatchBudget {
                            max_rows: NonZeroUsize::new(32).unwrap(),
                            max_bytes: NonZeroUsize::new(4096).unwrap(),
                        },
                        context: context(),
                    },
                )
                .unwrap()
        };
        let _reader = open(installer(Arc::clone(&be_one_calls)), Arc::clone(&schema));
        assert_eq!(be_one_calls.load(Ordering::SeqCst), 1);
        assert_eq!(be_two_calls.load(Ordering::SeqCst), 0);
        let _reader = open(installer(Arc::clone(&be_two_calls)), schema);
        assert_eq!(be_one_calls.load(Ordering::SeqCst), 1);
        assert_eq!(be_two_calls.load(Ordering::SeqCst), 1);
    }
}
