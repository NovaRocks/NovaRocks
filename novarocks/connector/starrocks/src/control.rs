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

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorExecutionDistribution,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorProviderId,
    ConnectorReadSelector, ConnectorScan, ConnectorScanHandle, ConnectorScanPlanning,
    ConnectorSplit, ConnectorSplitPlanningMetrics, ConnectorSplitPlanningRequest,
    ConnectorSplitPlanningResult, ConnectorTableHandle, ConnectorTableIdentity,
    ConnectorTableMetadata, ConnectorTableRequest, StatisticsDataVersion,
    validate_static_predicates,
};
use serde::{Deserialize, Serialize};

use crate::codec::{
    Base64Bytes, CODEC_VERSION, decode_schema_ipc, decode_v1, encode_schema_ipc, encode_v1,
    freeze_digest, schema_digest,
};
use crate::direct::DirectOuterFacts;
use crate::domain::{
    StarRocksCapabilitySnapshot, StarRocksConnectorConfig, StarRocksFreezeDigest,
    StarRocksReadAttemptId, StarRocksResolvedTable, StarRocksSelectedStrategy,
    StarRocksSplitPlanningInput, StarRocksStrategySplit, StarRocksStrategySplitPayload, invalid,
    select_read_strategy,
};
use crate::{STARROCKS_CONTRACT_VERSION, STARROCKS_PROVIDER_ID};

pub trait StarRocksMetadataSource: Send + Sync {
    fn namespace_exists(
        &self,
        namespace: &str,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, ConnectorError>;
    fn table_exists(
        &self,
        namespace: &str,
        table: &str,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, ConnectorError>;
    fn list_tables(
        &self,
        namespace: &str,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<String>, ConnectorError>;
    fn load_table(
        &self,
        namespace: &str,
        table: &str,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StarRocksResolvedTable, ConnectorError>;
}

pub trait StarRocksRpcSplitPlanner: Send + Sync {
    fn plan_rpc_splits(
        &self,
        input: &StarRocksSplitPlanningInput,
        request: &ConnectorSplitPlanningRequest,
    ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError>;
}

pub trait StarRocksDirectSplitPlanner: Send + Sync {
    fn plan_direct_splits(
        &self,
        input: &StarRocksSplitPlanningInput,
        request: &ConnectorSplitPlanningRequest,
    ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError>;
}

pub struct StarRocksControlGeneration;

impl StarRocksControlGeneration {
    pub fn try_new(
        config: StarRocksConnectorConfig,
        metadata: Arc<dyn StarRocksMetadataSource>,
        rpc_planner: Arc<dyn StarRocksRpcSplitPlanner>,
        direct_planner: Arc<dyn StarRocksDirectSplitPlanner>,
    ) -> Result<ConnectorControlBinding, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(STARROCKS_PROVIDER_ID)?,
            instance_id: config.instance_id.clone(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let provider = Arc::new(Provider {
            descriptor: descriptor.clone(),
            incarnation,
            config,
            metadata,
            rpc_planner,
            direct_planner,
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
    incarnation: ConnectorInstanceIncarnation,
    config: StarRocksConnectorConfig,
    metadata: Arc<dyn StarRocksMetadataSource>,
    rpc_planner: Arc<dyn StarRocksRpcSplitPlanner>,
    direct_planner: Arc<dyn StarRocksDirectSplitPlanner>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeclarationPayload {
    pub(crate) version: u16,
    pub(crate) contract_version: u16,
    pub(crate) local_binding: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TablePayload {
    version: u16,
    owner: String,
    incarnation: Base64Bytes,
    namespace: String,
    table: String,
    topology: crate::StarRocksTopology,
    capability: StarRocksCapabilitySnapshot,
    schema: Base64Bytes,
    schema_version: Base64Bytes,
    data_version: Base64Bytes,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FrozenRead {
    version: u16,
    owner: String,
    incarnation: Base64Bytes,
    namespace: String,
    table: String,
    topology: crate::StarRocksTopology,
    capability: StarRocksCapabilitySnapshot,
    schema_version: Base64Bytes,
    data_version: Base64Bytes,
    strategy: StarRocksSelectedStrategy,
    output_schema: Base64Bytes,
    output_schema_digest: Base64Bytes,
    projection: Vec<usize>,
    limit: Option<u64>,
    max_batch_rows: usize,
    max_batch_bytes: usize,
    predicate_dispositions: Vec<PredicateDisposition>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PredicateDisposition {
    id: u32,
    unsupported: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScanPayload {
    version: u16,
    attempt: uuid::Uuid,
    digest: Base64Bytes,
    frozen: FrozenRead,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitPayload {
    pub(crate) version: u16,
    pub(crate) attempt: uuid::Uuid,
    pub(crate) digest: Base64Bytes,
    frozen: FrozenRead,
    pub(crate) strategy_payload: StrategyPayload,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StrategyPayload {
    Rpc { payload: Base64Bytes },
    SharedDataDirect { payload: Base64Bytes },
}

impl Provider {
    fn active(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
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
        let schema = encode_schema_ipc(table.schema.as_ref())?;
        let payload = TablePayload {
            version: CODEC_VERSION,
            owner: self.descriptor.instance_id.as_str().to_string(),
            incarnation: Base64Bytes(Bytes::copy_from_slice(&self.incarnation.to_bytes())),
            namespace: table.namespace.to_string(),
            table: table.table.to_string(),
            topology: table.topology,
            capability: table.capability,
            schema,
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
            version: Some(table.schema_version),
            statistics_data_version: Some(StatisticsDataVersion::try_new(table.data_version)?),
            table: table_handle,
        })
    }

    fn decode_table(&self, table: &ConnectorTableHandle) -> Result<TablePayload, ConnectorError> {
        self.ensure_identity(table.owner())?;
        let value: TablePayload = decode_v1(table.payload(), "table handle")?;
        if value.version != CODEC_VERSION {
            return Err(unsupported_version("table handle", value.version));
        }
        validate_frozen_owner(
            &value.owner,
            &value.incarnation,
            &self.descriptor.instance_id,
            self.incarnation,
        )?;
        value.capability.validate()?;
        if value.schema_version.0.is_empty() || value.data_version.0.is_empty() {
            return Err(invalid("StarRocks table handle has an empty version"));
        }
        Ok(value)
    }

    fn freeze(
        &self,
        table: TablePayload,
        request: &ConnectorBeginScanRequest,
    ) -> Result<(FrozenRead, StarRocksFreezeDigest), ConnectorError> {
        if request.selector != ConnectorReadSelector::Current {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "StarRocks connector does not yet support historical read selectors",
            ));
        }
        validate_static_predicates(&request.static_predicates)?;
        let source_schema = decode_schema_ipc(&table.schema)?;
        let projection = if request.projection.is_empty() {
            (0..source_schema.fields().len()).collect()
        } else {
            request.projection.clone()
        };
        if projection
            .iter()
            .any(|index| *index >= source_schema.fields().len())
            || projection
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != projection.len()
        {
            return Err(invalid("StarRocks scan projection is invalid"));
        }
        let output_schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(
            projection
                .iter()
                .map(|index| source_schema.field(*index).clone())
                .collect::<Vec<_>>(),
        ));
        let strategy = select_read_strategy(
            self.config.read_policy,
            table.topology,
            &table.capability,
            self.config.rpc_transport,
        )?;
        let output_schema = encode_schema_ipc(output_schema.as_ref())?;
        let frozen = FrozenRead {
            version: CODEC_VERSION,
            owner: table.owner,
            incarnation: table.incarnation,
            namespace: table.namespace,
            table: table.table,
            topology: table.topology,
            capability: table.capability,
            schema_version: table.schema_version,
            data_version: table.data_version,
            strategy,
            output_schema_digest: Base64Bytes(Bytes::copy_from_slice(&schema_digest(
                &output_schema,
            ))),
            output_schema,
            projection,
            limit: request.limit,
            max_batch_rows: request.batch.max_rows.get(),
            max_batch_bytes: request.batch.max_bytes.get(),
            predicate_dispositions: request
                .static_predicates
                .iter()
                .map(|predicate| PredicateDisposition {
                    id: predicate.id.0,
                    unsupported: true,
                })
                .collect(),
        };
        let digest = freeze_digest(&frozen)?;
        Ok((frozen, digest))
    }

    fn decode_scan(
        &self,
        scan: &ConnectorScanHandle,
    ) -> Result<(StarRocksReadAttemptId, StarRocksFreezeDigest, FrozenRead), ConnectorError> {
        self.ensure_identity(scan.owner())?;
        let value: ScanPayload = decode_v1(scan.payload(), "scan handle")?;
        if value.version != CODEC_VERSION {
            return Err(unsupported_version("scan handle", value.version));
        }
        validate_frozen_owner(
            &value.frozen.owner,
            &value.frozen.incarnation,
            &self.descriptor.instance_id,
            self.incarnation,
        )?;
        let attempt = StarRocksReadAttemptId::from_uuid(value.attempt)?;
        let digest = digest_from_bytes(&value.digest)?;
        if digest != freeze_digest(&value.frozen)? {
            return Err(invalid(
                "StarRocks scan freeze digest does not match frozen facts",
            ));
        }
        validate_frozen_read(&value.frozen)?;
        Ok((attempt, digest, value.frozen))
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
    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        self.active(&request.context)?;
        let (frozen, digest) = self.freeze(self.decode_table(table)?, &request)?;
        let output_schema = decode_schema_ipc(&frozen.output_schema)?;
        let attempt = StarRocksReadAttemptId::new();
        let payload = ScanPayload {
            version: CODEC_VERSION,
            attempt: attempt.as_uuid(),
            digest: Base64Bytes(Bytes::copy_from_slice(&digest.0)),
            frozen,
        };
        Ok(ConnectorScan {
            handle: ConnectorScanHandle::try_new(
                self.descriptor.instance_id.clone(),
                encode_v1(
                    &payload,
                    "scan handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
            output_schema,
            predicate_dispositions: request
                .static_predicates
                .iter()
                .map(|predicate| ConnectorPredicateDisposition {
                    predicate_id: predicate.id,
                    kind: ConnectorPredicateDispositionKind::Unsupported,
                })
                .collect(),
        })
    }
    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        self.active(&request.context)?;
        let (attempt, digest, frozen) = self.decode_scan(scan)?;
        let input = StarRocksSplitPlanningInput {
            owner: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
            attempt,
            freeze: digest,
            strategy: frozen.strategy,
            topology: frozen.topology,
            namespace: Arc::from(frozen.namespace.as_str()),
            table: Arc::from(frozen.table.as_str()),
            schema_version: frozen.schema_version.0.clone(),
            data_version: frozen.data_version.0.clone(),
            output_schema: decode_schema_ipc(&frozen.output_schema)?,
            projection: frozen.projection.clone(),
            limit: frozen.limit,
        };
        let planned = match frozen.strategy {
            StarRocksSelectedStrategy::Rpc { .. } => {
                self.rpc_planner.plan_rpc_splits(&input, &request)?
            }
            StarRocksSelectedStrategy::SharedDataDirect => {
                self.direct_planner.plan_direct_splits(&input, &request)?
            }
        };
        let mut total = 0usize;
        let mut ids = std::collections::BTreeSet::new();
        let splits = planned
            .into_iter()
            .map(|split| {
                if split.split_id.is_empty() || !ids.insert(split.split_id.to_string()) {
                    return Err(invalid("StarRocks split IDs must be non-empty and unique"));
                }
                let strategy_payload = match (&frozen.strategy, split.payload) {
                    (
                        StarRocksSelectedStrategy::Rpc { .. },
                        StarRocksStrategySplitPayload::Rpc(payload),
                    ) => StrategyPayload::Rpc {
                        payload: Base64Bytes(payload.as_bytes().clone()),
                    },
                    (
                        StarRocksSelectedStrategy::SharedDataDirect,
                        StarRocksStrategySplitPayload::SharedDataDirect(payload),
                    ) => StrategyPayload::SharedDataDirect {
                        payload: Base64Bytes(payload),
                    },
                    _ => {
                        return Err(invalid(
                            "StarRocks split strategy does not match frozen scan strategy",
                        ));
                    }
                };
                let payload = encode_v1(
                    &SplitPayload {
                        version: CODEC_VERSION,
                        attempt: attempt.as_uuid(),
                        digest: Base64Bytes(Bytes::copy_from_slice(&digest.0)),
                        frozen: frozen.clone(),
                        strategy_payload,
                    },
                    "split",
                    request.context.max_handle_payload_bytes(),
                )?;
                total = total
                    .checked_add(payload.len())
                    .filter(|value| *value <= request.context.max_total_payload_bytes())
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "StarRocks split payloads exceed the request budget",
                        )
                    })?;
                ConnectorSplit::try_new(
                    self.descriptor.instance_id.clone(),
                    split.split_id,
                    payload,
                    split.estimated_bytes,
                )
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered: 0,
                candidate_units_pruned: 0,
            },
        )
    }
}

impl ConnectorExecutionDistribution for Provider {
    fn declaration(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        self.active(context)?;
        ConnectorExecutionDeclaration::try_new(
            self.descriptor.clone(),
            self.incarnation,
            encode_v1(
                &DeclarationPayload {
                    version: CODEC_VERSION,
                    contract_version: STARROCKS_CONTRACT_VERSION,
                    local_binding: self.config.local_binding.as_str().to_string(),
                },
                "execution declaration",
                context.max_handle_payload_bytes(),
            )?,
        )
    }
}

pub(crate) fn decode_declaration(bytes: &Bytes) -> Result<DeclarationPayload, ConnectorError> {
    let declaration: DeclarationPayload = decode_v1(bytes, "execution declaration")?;
    if declaration.version != CODEC_VERSION
        || declaration.contract_version != STARROCKS_CONTRACT_VERSION
    {
        return Err(unsupported_version(
            "execution declaration",
            declaration.version,
        ));
    }
    Ok(declaration)
}

pub(crate) fn decode_split(bytes: &Bytes) -> Result<SplitPayload, ConnectorError> {
    let split: SplitPayload = decode_v1(bytes, "split")?;
    if split.version != CODEC_VERSION {
        return Err(unsupported_version("split", split.version));
    }
    let _ = StarRocksReadAttemptId::from_uuid(split.attempt)?;
    if digest_from_bytes(&split.digest)? != freeze_digest(&split.frozen)? {
        return Err(invalid(
            "StarRocks split freeze digest does not match frozen facts",
        ));
    }
    validate_frozen_read(&split.frozen)?;
    Ok(split)
}

pub(crate) fn split_output_schema_digest(split: &SplitPayload) -> &[u8] {
    &split.frozen.output_schema_digest.0
}
pub(crate) fn split_strategy(split: &SplitPayload) -> StarRocksSelectedStrategy {
    split.frozen.strategy
}

pub(crate) fn direct_outer_facts(split: &SplitPayload) -> Result<DirectOuterFacts, ConnectorError> {
    let incarnation: [u8; 16] = split
        .frozen
        .incarnation
        .0
        .as_ref()
        .try_into()
        .map_err(|_| invalid("StarRocks split incarnation must be 16 bytes"))?;
    let digest = digest_from_bytes(&split.digest)?;
    let output_schema_digest: [u8; 32] = split
        .frozen
        .output_schema_digest
        .0
        .as_ref()
        .try_into()
        .map_err(|_| invalid("StarRocks split output schema digest must be 32 bytes"))?;
    Ok(DirectOuterFacts {
        owner: Arc::from(split.frozen.owner.as_str()),
        incarnation,
        attempt: split.attempt,
        freeze: digest,
        topology: split.frozen.topology,
        strategy: split.frozen.strategy,
        schema_version: split.frozen.schema_version.0.clone(),
        data_version: split.frozen.data_version.0.clone(),
        output_schema_digest,
    })
}

pub(crate) fn validate_split_generation(
    split: &SplitPayload,
    key: &ConnectorExecutionBindingKey,
) -> Result<(), ConnectorError> {
    validate_frozen_owner(
        &split.frozen.owner,
        &split.frozen.incarnation,
        &key.instance_id,
        key.incarnation,
    )
}

fn validate_frozen_owner(
    owner: &str,
    incarnation: &Base64Bytes,
    instance: &ConnectorInstanceId,
    expected: ConnectorInstanceIncarnation,
) -> Result<(), ConnectorError> {
    if owner != instance.as_str() || incarnation.0.as_ref() != expected.to_bytes() {
        return Err(invalid(
            "StarRocks payload does not belong to this connector generation",
        ));
    }
    Ok(())
}

fn validate_frozen_read(frozen: &FrozenRead) -> Result<(), ConnectorError> {
    if frozen.version != CODEC_VERSION
        || frozen.schema_version.0.is_empty()
        || frozen.data_version.0.is_empty()
        || frozen.max_batch_rows == 0
        || frozen.max_batch_bytes == 0
    {
        return Err(invalid("StarRocks frozen read has invalid required facts"));
    }
    frozen.capability.validate()?;
    match frozen.strategy {
        StarRocksSelectedStrategy::Rpc { transport } => {
            frozen.capability.rpc_is_ready(transport)?
        }
        StarRocksSelectedStrategy::SharedDataDirect => {
            if frozen.topology != crate::StarRocksTopology::SharedData {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "StarRocks direct split requires shared-data topology",
                ));
            }
            frozen.capability.direct_is_ready()?;
        }
    }
    if schema_digest(&frozen.output_schema).as_slice() != frozen.output_schema_digest.0.as_ref() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "StarRocks frozen read output schema digest is invalid",
        ));
    }
    Ok(())
}
fn digest_from_bytes(value: &Base64Bytes) -> Result<StarRocksFreezeDigest, ConnectorError> {
    let bytes: [u8; 32] = value
        .0
        .as_ref()
        .try_into()
        .map_err(|_| invalid("StarRocks freeze digest must be 32 bytes"))?;
    Ok(StarRocksFreezeDigest(bytes))
}
fn unsupported_version(subject: &str, version: u16) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        format!("unsupported StarRocks {subject} version {version}"),
    )
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorCancellation, ConnectorReadSelector,
        ConnectorTableResolution,
    };
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::StarRocksRpcOpaquePayload;

    #[derive(Default)]
    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }
    fn context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(2),
            Arc::new(NeverCancelled),
            64 * 1024,
            128 * 1024,
        )
        .unwrap()
    }
    struct Metadata {
        topology: crate::StarRocksTopology,
        capability: StarRocksCapabilitySnapshot,
    }
    impl StarRocksMetadataSource for Metadata {
        fn namespace_exists(
            &self,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn table_exists(
            &self,
            _: &str,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn list_tables(
            &self,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<Vec<String>, ConnectorError> {
            Ok(vec!["t".into()])
        }
        fn load_table(
            &self,
            namespace: &str,
            table: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<StarRocksResolvedTable, ConnectorError> {
            StarRocksResolvedTable::try_new(
                namespace,
                table,
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                self.topology,
                Bytes::from_static(b"schema-1"),
                Bytes::from_static(b"data-1"),
                self.capability.clone(),
            )
        }
    }
    struct Planner {
        calls: Arc<AtomicUsize>,
    }
    impl StarRocksRpcSplitPlanner for Planner {
        fn plan_rpc_splits(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![StarRocksStrategySplit {
                split_id: Arc::from("rpc"),
                payload: StarRocksStrategySplitPayload::Rpc(
                    StarRocksRpcOpaquePayload::new(Bytes::from_static(b"token")).unwrap(),
                ),
                estimated_bytes: None,
            }])
        }
    }
    impl StarRocksDirectSplitPlanner for Planner {
        fn plan_direct_splits(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![StarRocksStrategySplit {
                split_id: Arc::from("direct"),
                payload: StarRocksStrategySplitPayload::SharedDataDirect(Bytes::from_static(
                    b"direct",
                )),
                estimated_bytes: None,
            }])
        }
    }
    fn binding(
        policy: crate::StarRocksReadPolicy,
        topology: crate::StarRocksTopology,
        direct: bool,
    ) -> (ConnectorControlBinding, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let rpc_calls = Arc::new(AtomicUsize::new(0));
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let capability = StarRocksCapabilitySnapshot {
            api_contract_version: 1,
            rpc_transports: [crate::StarRocksRpcTransport::BrpcChunk].into(),
            rpc_ready: true,
            direct_contract_version: Some(1),
            direct_ready: direct,
        };
        let config = crate::StarRocksConnectorConfig::new(
            ConnectorInstanceId::parse("catalog.starrocks").unwrap(),
            policy,
            crate::StarRocksRpcTransport::BrpcChunk,
            crate::StarRocksLocalBindingRef::parse("default").unwrap(),
        );
        (
            StarRocksControlGeneration::try_new(
                config,
                Arc::new(Metadata {
                    topology,
                    capability,
                }),
                Arc::new(Planner {
                    calls: rpc_calls.clone(),
                }),
                Arc::new(Planner {
                    calls: direct_calls.clone(),
                }),
            )
            .unwrap(),
            rpc_calls,
            direct_calls,
        )
    }
    #[test]
    fn auto_selects_one_frozen_strategy_and_never_calls_the_other_planner() {
        let (binding, rpc, direct) = binding(
            crate::StarRocksReadPolicy::Auto,
            crate::StarRocksTopology::SharedData,
            true,
        );
        let context = context();
        let id = binding.descriptor().instance_id.clone();
        let table = binding
            .metadata()
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: id,
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
                ConnectorBeginScanRequest {
                    projection: vec![0],
                    static_predicates: vec![],
                    selector: ConnectorReadSelector::Current,
                    limit: None,
                    batch: ConnectorBatchBudget {
                        max_rows: NonZeroUsize::new(1).unwrap(),
                        max_bytes: NonZeroUsize::new(1024).unwrap(),
                    },
                    context: context.clone(),
                },
            )
            .unwrap();
        binding
            .planning()
            .plan_splits(
                &scan.handle,
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).unwrap(),
                    max_split_bytes: None,
                    context,
                },
            )
            .unwrap();
        assert_eq!(rpc.load(Ordering::SeqCst), 0);
        assert_eq!(direct.load(Ordering::SeqCst), 1);
    }
}
