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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorSplitPlanningRequest};

use crate::control::StarRocksDirectSplitPlanner;
use crate::domain::{
    StarRocksFreezeDigest, StarRocksReadAttemptId, StarRocksSelectedStrategy,
    StarRocksSplitPlanningInput, StarRocksStrategySplit, StarRocksStrategySplitPayload,
    StarRocksTopology, invalid, unsupported,
};

use super::encode_direct_split;

const MAX_DIRECT_TEXT_LEN: usize = 16 * 1024;
const MAX_DIRECT_COLUMNS: usize = 16 * 1024;

/// Non-sensitive identifier used to select a BE-local storage binding.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StarRocksStorageBindingRef(Arc<str>);

impl StarRocksStorageBindingRef {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ConnectorError> {
        let value = value.as_ref().trim();
        if value.is_empty() || value.len() > 256 || !value.is_ascii() {
            return Err(invalid("invalid StarRocks storage binding reference"));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StarRocksStorageBindingRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StarRocksStorageBindingRef")
            .field(&self.0)
            .finish()
    }
}

/// The exact metadata object layout selected during planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StarRocksDirectMetadataLayout {
    Standalone,
    Bundle,
}

/// A frozen output-to-physical-column mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksDirectColumnBinding {
    pub output_index: usize,
    pub unique_id: i32,
    pub name: Arc<str>,
    pub physical_type: Arc<str>,
    pub nullable: bool,
    pub default_value: Option<Bytes>,
}

impl StarRocksDirectColumnBinding {
    pub fn try_new(
        output_index: usize,
        unique_id: i32,
        name: impl Into<Arc<str>>,
        physical_type: impl Into<Arc<str>>,
        nullable: bool,
        default_value: Option<Bytes>,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        let physical_type = physical_type.into();
        if unique_id <= 0
            || name.is_empty()
            || physical_type.is_empty()
            || name.len() > MAX_DIRECT_TEXT_LEN
            || physical_type.len() > MAX_DIRECT_TEXT_LEN
        {
            return Err(invalid("invalid StarRocks direct column binding"));
        }
        Ok(Self {
            output_index,
            unique_id,
            name,
            physical_type,
            nullable,
            default_value,
        })
    }
}

/// Tablet facts supplied by the connector's control-plane metadata adapter.
#[derive(Clone, Debug)]
pub struct StarRocksDirectTabletDescriptor {
    pub tablet_id: i64,
    pub partition_id: i64,
    pub tablet_version: i64,
    pub metadata_layout: StarRocksDirectMetadataLayout,
    pub metadata_relative_path: Arc<str>,
    pub columns: Vec<StarRocksDirectColumnBinding>,
    pub estimated_bytes: Option<u64>,
}

impl StarRocksDirectTabletDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tablet_id: i64,
        partition_id: i64,
        tablet_version: i64,
        metadata_layout: StarRocksDirectMetadataLayout,
        metadata_relative_path: impl Into<Arc<str>>,
        columns: Vec<StarRocksDirectColumnBinding>,
        estimated_bytes: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        let metadata_relative_path = metadata_relative_path.into();
        if tablet_id <= 0
            || partition_id <= 0
            || tablet_version <= 0
            || !is_safe_metadata_relative_path(&metadata_relative_path)
            || metadata_relative_path.len() > MAX_DIRECT_TEXT_LEN
            || columns.is_empty()
            || columns.len() > MAX_DIRECT_COLUMNS
        {
            return Err(invalid("invalid StarRocks direct tablet descriptor"));
        }
        validate_column_bindings(&columns)?;
        Ok(Self {
            tablet_id,
            partition_id,
            tablet_version,
            metadata_layout,
            metadata_relative_path,
            columns,
            estimated_bytes,
        })
    }
}

fn is_safe_metadata_relative_path(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.contains('\\')
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// Location facts resolved from StarOS before a split is emitted.
#[derive(Clone, Debug)]
pub struct StarRocksDirectLocation {
    pub tablet_id: i64,
    pub tablet_root: Arc<str>,
    pub storage_binding: StarRocksStorageBindingRef,
    pub storage_identity: Arc<str>,
}

impl StarRocksDirectLocation {
    pub fn try_new(
        tablet_id: i64,
        tablet_root: impl Into<Arc<str>>,
        storage_binding: StarRocksStorageBindingRef,
        storage_identity: impl Into<Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        let tablet_root = tablet_root.into();
        let storage_identity = storage_identity.into();
        validate_storage_uri(&tablet_root)?;
        if tablet_id <= 0
            || storage_identity.is_empty()
            || storage_identity.len() > MAX_DIRECT_TEXT_LEN
        {
            return Err(invalid("invalid StarRocks direct storage location"));
        }
        Ok(Self {
            tablet_id,
            tablet_root,
            storage_binding,
            storage_identity,
        })
    }
}

/// Control-plane source for the immutable tablet/version/schema facts.
pub trait StarRocksDirectTabletPlanningSource: Send + Sync {
    fn plan_tablets(
        &self,
        input: &StarRocksSplitPlanningInput,
        request: &ConnectorSplitPlanningRequest,
    ) -> Result<Vec<StarRocksDirectTabletDescriptor>, ConnectorError>;
}

/// Startup-local source for resolving a tablet to one frozen shared-data root.
pub trait StarRocksDirectLocationSource: Send + Sync {
    fn resolve_locations(
        &self,
        tablet_ids: &[i64],
        request: &ConnectorSplitPlanningRequest,
    ) -> Result<Vec<StarRocksDirectLocation>, ConnectorError>;
}

/// Concrete direct split planner used by a StarRocks control generation.
pub struct StarRocksSharedDataDirectPlanner {
    tablets: Arc<dyn StarRocksDirectTabletPlanningSource>,
    locations: Arc<dyn StarRocksDirectLocationSource>,
}

impl StarRocksSharedDataDirectPlanner {
    pub fn new(
        tablets: Arc<dyn StarRocksDirectTabletPlanningSource>,
        locations: Arc<dyn StarRocksDirectLocationSource>,
    ) -> Self {
        Self { tablets, locations }
    }
}

impl StarRocksDirectSplitPlanner for StarRocksSharedDataDirectPlanner {
    fn plan_direct_splits(
        &self,
        input: &StarRocksSplitPlanningInput,
        request: &ConnectorSplitPlanningRequest,
    ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
        if input.strategy != StarRocksSelectedStrategy::SharedDataDirect
            || input.topology != StarRocksTopology::SharedData
        {
            return Err(unsupported(
                "StarRocks shared-data direct planner requires a shared-data direct scan",
            ));
        }
        ensure_active(&request.context)?;
        let tablets = self.tablets.plan_tablets(input, request)?;
        ensure_active(&request.context)?;
        let ids = tablets
            .iter()
            .map(|tablet| tablet.tablet_id)
            .collect::<Vec<_>>();
        if ids.is_empty() || ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
            return Err(invalid(
                "StarRocks direct tablet IDs must be non-empty and unique",
            ));
        }
        let locations = self.locations.resolve_locations(&ids, request)?;
        ensure_active(&request.context)?;
        let locations = locations
            .into_iter()
            .map(|location| (location.tablet_id, location))
            .collect::<std::collections::BTreeMap<_, _>>();
        if locations.len() != tablets.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks location source did not resolve every planned tablet",
            ));
        }
        tablets
            .into_iter()
            .map(|tablet| {
                let location = locations.get(&tablet.tablet_id).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "StarRocks location source omitted a planned tablet",
                    )
                })?;
                let split = StarRocksDirectSplit::from_planning(input, tablet, location.clone())?;
                let payload =
                    encode_direct_split(&split, request.context.max_handle_payload_bytes())?;
                Ok(StarRocksStrategySplit {
                    split_id: Arc::from(format!("tablet-{}", split.tablet_id())),
                    payload: StarRocksStrategySplitPayload::SharedDataDirect(payload),
                    estimated_bytes: split.estimated_bytes(),
                })
            })
            .collect()
    }
}

/// Typed direct payload passed to the selected reader factory only after its
/// outer carrier has been validated by the composite execution binding.
#[derive(Clone)]
pub struct StarRocksDirectSplit {
    pub(crate) owner: Arc<str>,
    pub(crate) incarnation: [u8; 16],
    pub(crate) attempt: StarRocksReadAttemptId,
    pub(crate) freeze: StarRocksFreezeDigest,
    pub(crate) schema_version: Bytes,
    pub(crate) data_version: Bytes,
    pub(crate) output_schema_digest: [u8; 32],
    pub(crate) tablet: StarRocksDirectTabletDescriptor,
    pub(crate) location: StarRocksDirectLocation,
}

impl StarRocksDirectSplit {
    pub(crate) fn from_planning(
        input: &StarRocksSplitPlanningInput,
        tablet: StarRocksDirectTabletDescriptor,
        location: StarRocksDirectLocation,
    ) -> Result<Self, ConnectorError> {
        if location.tablet_id != tablet.tablet_id {
            return Err(invalid(
                "StarRocks direct location tablet does not match descriptor",
            ));
        }
        let output_schema_digest = crate::codec::schema_digest(&crate::codec::encode_schema_ipc(
            input.output_schema.as_ref(),
        )?);
        Ok(Self {
            owner: Arc::from(input.owner.as_str()),
            incarnation: input.incarnation.to_bytes(),
            attempt: input.attempt,
            freeze: input.freeze,
            schema_version: input.schema_version.clone(),
            data_version: input.data_version.clone(),
            output_schema_digest,
            tablet,
            location,
        })
    }

    pub fn tablet_id(&self) -> i64 {
        self.tablet.tablet_id
    }
    pub fn partition_id(&self) -> i64 {
        self.tablet.partition_id
    }
    pub fn tablet_version(&self) -> i64 {
        self.tablet.tablet_version
    }
    pub fn metadata_layout(&self) -> StarRocksDirectMetadataLayout {
        self.tablet.metadata_layout
    }
    pub fn metadata_relative_path(&self) -> &str {
        &self.tablet.metadata_relative_path
    }
    pub fn tablet_root(&self) -> &str {
        &self.location.tablet_root
    }
    pub fn storage_binding(&self) -> &StarRocksStorageBindingRef {
        &self.location.storage_binding
    }
    pub fn storage_identity(&self) -> &str {
        &self.location.storage_identity
    }
    pub fn columns(&self) -> &[StarRocksDirectColumnBinding] {
        &self.tablet.columns
    }
    pub fn estimated_bytes(&self) -> Option<u64> {
        self.tablet.estimated_bytes
    }
    pub fn schema_version(&self) -> &Bytes {
        &self.schema_version
    }
    pub fn data_version(&self) -> &Bytes {
        &self.data_version
    }
}

impl fmt::Debug for StarRocksDirectSplit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksDirectSplit")
            .field("tablet_id", &self.tablet.tablet_id)
            .field("partition_id", &self.tablet.partition_id)
            .field("tablet_version", &self.tablet.tablet_version)
            .field("metadata_layout", &self.tablet.metadata_layout)
            .field("storage_binding", &self.location.storage_binding)
            .field("storage_identity", &self.location.storage_identity)
            .field("columns", &self.tablet.columns.len())
            .finish_non_exhaustive()
    }
}

pub(crate) fn validate_column_bindings(
    columns: &[StarRocksDirectColumnBinding],
) -> Result<(), ConnectorError> {
    let mut output_indexes = BTreeSet::new();
    let mut unique_ids = BTreeSet::new();
    for column in columns {
        if !output_indexes.insert(column.output_index) || !unique_ids.insert(column.unique_id) {
            return Err(invalid("StarRocks direct column mappings must be unique"));
        }
    }
    Ok(())
}

pub(crate) fn validate_storage_uri(value: &str) -> Result<(), ConnectorError> {
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_DIRECT_TEXT_LEN {
        return Err(invalid(
            "StarRocks direct storage URI is empty or oversized",
        ));
    }
    let lower = value.to_ascii_lowercase();
    if value.contains('@') || value.contains('?') || value.contains('#') || lower.contains("token=")
    {
        return Err(invalid(
            "StarRocks direct storage URI contains credential material",
        ));
    }
    if !value.starts_with("s3://") && !value.starts_with("s3a://") && !value.starts_with("oss://") {
        return Err(unsupported(
            "unsupported StarRocks direct storage URI scheme",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_active(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "StarRocks direct request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "StarRocks direct request deadline elapsed",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorRequestContext,
    };

    use super::*;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct CountingSource(Arc<AtomicUsize>);
    impl StarRocksDirectTabletPlanningSource for CountingSource {
        fn plan_tablets(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksDirectTabletDescriptor>, ConnectorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }
    impl StarRocksDirectLocationSource for CountingSource {
        fn resolve_locations(
            &self,
            _: &[i64],
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksDirectLocation>, ConnectorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }

    fn input(topology: StarRocksTopology) -> StarRocksSplitPlanningInput {
        StarRocksSplitPlanningInput {
            owner: ConnectorInstanceId::parse("catalog.direct").unwrap(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([4; 16]),
            attempt: StarRocksReadAttemptId::new(),
            freeze: StarRocksFreezeDigest([3; 32]),
            strategy: StarRocksSelectedStrategy::SharedDataDirect,
            topology,
            namespace: Arc::from("db"),
            table: Arc::from("tbl"),
            schema_version: Bytes::from_static(b"schema"),
            data_version: Bytes::from_static(b"data"),
            output_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            projection: vec![0],
            limit: None,
        }
    }

    fn request() -> ConnectorSplitPlanningRequest {
        ConnectorSplitPlanningRequest {
            target_parallelism: NonZeroUsize::new(1).unwrap(),
            max_split_bytes: None,
            context: ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                Arc::new(NeverCancelled),
                1024,
                4096,
            )
            .unwrap(),
        }
    }

    #[test]
    fn shared_nothing_never_calls_direct_sources() {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(CountingSource(calls.clone()));
        let planner = StarRocksSharedDataDirectPlanner::new(source.clone(), source);
        assert_eq!(
            planner
                .plan_direct_splits(&input(StarRocksTopology::SharedNothing), &request())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn direct_metadata_path_cannot_escape_the_frozen_tablet_root() {
        for path in ["/meta/1", "meta/../1", "meta//1", "meta\\1"] {
            assert_eq!(
                StarRocksDirectTabletDescriptor::try_new(
                    1,
                    1,
                    1,
                    StarRocksDirectMetadataLayout::Standalone,
                    path,
                    vec![
                        StarRocksDirectColumnBinding::try_new(0, 1, "id", "BIGINT", false, None,)
                            .unwrap()
                    ],
                    None,
                )
                .unwrap_err()
                .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
    }
}
