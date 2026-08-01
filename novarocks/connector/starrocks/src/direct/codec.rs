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

use bytes::Bytes;
use novarocks_spi::connector::ConnectorError;
#[cfg(test)]
use novarocks_spi::connector::ConnectorErrorKind;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::codec::{Base64Bytes, CODEC_VERSION, decode_v1, encode_v1};
use crate::domain::{
    StarRocksFreezeDigest, StarRocksReadAttemptId, StarRocksSelectedStrategy, StarRocksTopology,
    invalid, unsupported,
};

use super::planning::{
    StarRocksDirectColumnBinding, StarRocksDirectLocation, StarRocksDirectMetadataLayout,
    StarRocksDirectSplit, StarRocksDirectTabletDescriptor, StarRocksStorageBindingRef,
    validate_column_bindings, validate_storage_uri,
};

#[derive(Clone)]
pub(crate) struct DirectOuterFacts {
    pub(crate) owner: Arc<str>,
    pub(crate) incarnation: [u8; 16],
    pub(crate) attempt: Uuid,
    pub(crate) freeze: StarRocksFreezeDigest,
    pub(crate) topology: StarRocksTopology,
    pub(crate) strategy: StarRocksSelectedStrategy,
    pub(crate) schema_version: Bytes,
    pub(crate) data_version: Bytes,
    pub(crate) output_schema_digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectSplitPayload {
    version: u16,
    owner: String,
    incarnation: Base64Bytes,
    attempt: Uuid,
    freeze: Base64Bytes,
    topology: StarRocksTopology,
    strategy: StarRocksSelectedStrategy,
    schema_version: Base64Bytes,
    data_version: Base64Bytes,
    output_schema_digest: Base64Bytes,
    tablet_id: i64,
    partition_id: i64,
    tablet_version: i64,
    metadata_layout: DirectMetadataLayout,
    metadata_relative_path: String,
    tablet_root: String,
    storage_binding: String,
    storage_identity: String,
    columns: Vec<DirectColumnBinding>,
    estimated_bytes: Option<u64>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DirectMetadataLayout {
    Standalone,
    Bundle,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectColumnBinding {
    output_index: usize,
    unique_id: i32,
    name: String,
    physical_type: String,
    nullable: bool,
    default_value: Option<Base64Bytes>,
}

pub(crate) fn encode_direct_split(
    split: &StarRocksDirectSplit,
    max_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    let payload = DirectSplitPayload {
        version: CODEC_VERSION,
        owner: split.owner.to_string(),
        incarnation: Base64Bytes(Bytes::copy_from_slice(&split.incarnation)),
        attempt: split.attempt.as_uuid(),
        freeze: Base64Bytes(Bytes::copy_from_slice(&split.freeze.0)),
        topology: StarRocksTopology::SharedData,
        strategy: StarRocksSelectedStrategy::SharedDataDirect,
        schema_version: Base64Bytes(split.schema_version.clone()),
        data_version: Base64Bytes(split.data_version.clone()),
        output_schema_digest: Base64Bytes(Bytes::copy_from_slice(&split.output_schema_digest)),
        tablet_id: split.tablet.tablet_id,
        partition_id: split.tablet.partition_id,
        tablet_version: split.tablet.tablet_version,
        metadata_layout: match split.tablet.metadata_layout {
            StarRocksDirectMetadataLayout::Standalone => DirectMetadataLayout::Standalone,
            StarRocksDirectMetadataLayout::Bundle => DirectMetadataLayout::Bundle,
        },
        metadata_relative_path: split.tablet.metadata_relative_path.to_string(),
        tablet_root: split.location.tablet_root.to_string(),
        storage_binding: split.location.storage_binding.as_str().to_string(),
        storage_identity: split.location.storage_identity.to_string(),
        columns: split
            .tablet
            .columns
            .iter()
            .map(|column| DirectColumnBinding {
                output_index: column.output_index,
                unique_id: column.unique_id,
                name: column.name.to_string(),
                physical_type: column.physical_type.to_string(),
                nullable: column.nullable,
                default_value: column.default_value.clone().map(Base64Bytes),
            })
            .collect(),
        estimated_bytes: split.tablet.estimated_bytes,
    };
    encode_v1(&payload, "shared-data direct split", max_bytes)
}

pub(crate) fn decode_direct_split(
    bytes: &Bytes,
    outer: &DirectOuterFacts,
) -> Result<StarRocksDirectSplit, ConnectorError> {
    let payload: DirectSplitPayload = decode_v1(bytes, "shared-data direct split")?;
    if payload.version != CODEC_VERSION {
        return Err(unsupported(
            "unsupported StarRocks shared-data direct split version",
        ));
    }
    if payload.owner != outer.owner.as_ref()
        || payload.incarnation.0.as_ref() != outer.incarnation
        || payload.attempt != outer.attempt
        || payload.freeze.0.as_ref() != outer.freeze.0
        || payload.topology != StarRocksTopology::SharedData
        || payload.strategy != StarRocksSelectedStrategy::SharedDataDirect
        || outer.topology != StarRocksTopology::SharedData
        || outer.strategy != StarRocksSelectedStrategy::SharedDataDirect
        || payload.schema_version.0 != outer.schema_version
        || payload.data_version.0 != outer.data_version
        || payload.output_schema_digest.0.as_ref() != outer.output_schema_digest
    {
        return Err(invalid(
            "StarRocks shared-data direct split conflicts with its frozen carrier facts",
        ));
    }
    let attempt = StarRocksReadAttemptId::from_uuid(payload.attempt)?;
    let incarnation: [u8; 16] = payload
        .incarnation
        .0
        .as_ref()
        .try_into()
        .map_err(|_| invalid("StarRocks direct split incarnation must be 16 bytes"))?;
    let freeze: [u8; 32] = payload
        .freeze
        .0
        .as_ref()
        .try_into()
        .map_err(|_| invalid("StarRocks direct split freeze digest must be 32 bytes"))?;
    let schema_digest: [u8; 32] = payload
        .output_schema_digest
        .0
        .as_ref()
        .try_into()
        .map_err(|_| invalid("StarRocks direct split schema digest must be 32 bytes"))?;
    let columns = payload
        .columns
        .into_iter()
        .map(|column| {
            StarRocksDirectColumnBinding::try_new(
                column.output_index,
                column.unique_id,
                Arc::<str>::from(column.name),
                Arc::<str>::from(column.physical_type),
                column.nullable,
                column.default_value.map(|value| value.0),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_column_bindings(&columns)?;
    let tablet = StarRocksDirectTabletDescriptor::try_new(
        payload.tablet_id,
        payload.partition_id,
        payload.tablet_version,
        match payload.metadata_layout {
            DirectMetadataLayout::Standalone => StarRocksDirectMetadataLayout::Standalone,
            DirectMetadataLayout::Bundle => StarRocksDirectMetadataLayout::Bundle,
        },
        Arc::<str>::from(payload.metadata_relative_path),
        columns,
        payload.estimated_bytes,
    )?;
    let location = StarRocksDirectLocation::try_new(
        payload.tablet_id,
        Arc::<str>::from(payload.tablet_root),
        StarRocksStorageBindingRef::parse(payload.storage_binding)?,
        Arc::<str>::from(payload.storage_identity),
    )?;
    validate_storage_uri(location.tablet_root.as_ref())?;
    Ok(StarRocksDirectSplit {
        owner: Arc::from(payload.owner),
        incarnation,
        attempt,
        freeze: StarRocksFreezeDigest(freeze),
        schema_version: payload.schema_version.0,
        data_version: payload.data_version.0,
        output_schema_digest: schema_digest,
        tablet,
        location,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorInstanceIncarnation};

    use super::*;
    use crate::domain::StarRocksSplitPlanningInput;

    fn split() -> StarRocksDirectSplit {
        let input = StarRocksSplitPlanningInput {
            owner: ConnectorInstanceId::parse("catalog.direct").unwrap(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
            attempt: StarRocksReadAttemptId::from_uuid(Uuid::now_v7()).unwrap(),
            freeze: StarRocksFreezeDigest([7; 32]),
            strategy: StarRocksSelectedStrategy::SharedDataDirect,
            topology: StarRocksTopology::SharedData,
            namespace: Arc::from("db"),
            table: Arc::from("tbl"),
            schema_version: Bytes::from_static(b"schema"),
            data_version: Bytes::from_static(b"data"),
            output_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            projection: vec![0],
            limit: None,
        };
        let tablet = StarRocksDirectTabletDescriptor::try_new(
            1,
            2,
            3,
            StarRocksDirectMetadataLayout::Standalone,
            "meta/0001.meta",
            vec![StarRocksDirectColumnBinding::try_new(0, 1, "id", "BIGINT", false, None).unwrap()],
            Some(42),
        )
        .unwrap();
        let location = StarRocksDirectLocation::try_new(
            1,
            "s3://bucket/tablet",
            StarRocksStorageBindingRef::parse("volume-a").unwrap(),
            "fs-key",
        )
        .unwrap();
        StarRocksDirectSplit::from_planning(&input, tablet, location).unwrap()
    }

    #[test]
    fn direct_codec_round_trips_and_rejects_carrier_conflict() {
        let split = split();
        let bytes = encode_direct_split(&split, 4096).unwrap();
        let outer = DirectOuterFacts {
            owner: split.owner.clone(),
            incarnation: split.incarnation,
            attempt: split.attempt.as_uuid(),
            freeze: split.freeze,
            topology: StarRocksTopology::SharedData,
            strategy: StarRocksSelectedStrategy::SharedDataDirect,
            schema_version: split.schema_version.clone(),
            data_version: split.data_version.clone(),
            output_schema_digest: split.output_schema_digest,
        };
        assert_eq!(decode_direct_split(&bytes, &outer).unwrap().tablet_id(), 1);
        let other = DirectOuterFacts {
            topology: StarRocksTopology::Unknown,
            ..outer
        };
        assert_eq!(
            decode_direct_split(&bytes, &other).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn direct_storage_uri_rejects_bearer_material() {
        assert_eq!(
            StarRocksDirectLocation::try_new(
                1,
                "s3://ak:sk@bucket/path",
                StarRocksStorageBindingRef::parse("v").unwrap(),
                "fs"
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            StarRocksDirectLocation::try_new(
                1,
                "s3://bucket/path?X-Amz-Signature=x",
                StarRocksStorageBindingRef::parse("v").unwrap(),
                "fs"
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
