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

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
};

use crate::execution::StarRocksDirectReaderFactory;

use super::StarRocksDirectSplit;

/// Startup-local storage resolver.  It owns any credential/client lookup and
/// receives only facts that were frozen into the direct split.
pub trait StarRocksDirectStorageResolver: Send + Sync {
    fn open_direct_storage(
        &self,
        split: &StarRocksDirectSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError>;
}

/// Concrete `StarRocksDirectReaderFactory` that opens the provider-private
/// storage reader selected by the direct split.  It has no RPC dependency and
/// never re-plans tablet, version, or storage location.
pub struct StarRocksSharedDataDirectReaderFactory {
    storage: Arc<dyn StarRocksDirectStorageResolver>,
}

impl StarRocksSharedDataDirectReaderFactory {
    pub fn new(storage: Arc<dyn StarRocksDirectStorageResolver>) -> Self {
        Self { storage }
    }

    fn open(
        &self,
        split: StarRocksDirectSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        validate_split_schema(&split, &request.expected_schema)?;
        self.storage.open_direct_storage(&split, request)
    }
}

impl StarRocksDirectReaderFactory for StarRocksSharedDataDirectReaderFactory {
    fn open_direct_reader(
        &self,
        split: StarRocksDirectSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.open(split, request)
    }
}

fn validate_split_schema(
    split: &StarRocksDirectSplit,
    expected: &SchemaRef,
) -> Result<(), ConnectorError> {
    if split.columns().len() != expected.fields().len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "StarRocks direct split column mapping does not match requested schema",
        ));
    }
    for (expected_index, (binding, field)) in
        split.columns().iter().zip(expected.fields()).enumerate()
    {
        if binding.output_index != expected_index
            || binding.name.as_ref() != field.name()
            || binding.nullable != field.is_nullable()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks direct split column mapping conflicts with requested schema",
            ));
        }
        let physical = expected_arrow_type(binding.physical_type.as_ref())?;
        if physical != *field.data_type() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks direct physical type conflicts with requested schema",
            ));
        }
    }
    Ok(())
}

fn expected_arrow_type(physical: &str) -> Result<DataType, ConnectorError> {
    let physical = physical.trim().to_ascii_uppercase();
    if let Some(decimal) = parse_decimal_type(&physical)? {
        return Ok(decimal);
    }
    let result = match physical.as_str() {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "DATE" | "DATE_V2" => DataType::Date32,
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "CHAR" | "VARCHAR" | "STRING" => DataType::Utf8,
        "BINARY" | "VARBINARY" | "OBJECT" | "HLL" | "PERCENTILE" | "JSON" | "VARIANT" => {
            DataType::Binary
        }
        unsupported => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!("unsupported StarRocks direct physical type {unsupported}"),
            ));
        }
    };
    Ok(result)
}

fn parse_decimal_type(physical: &str) -> Result<Option<DataType>, ConnectorError> {
    let Some((kind, parameters)) = physical.split_once('(') else {
        return Ok(None);
    };
    let kind = kind.trim();
    if !matches!(
        kind,
        "DECIMAL" | "DECIMALV2" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128"
    ) {
        return Ok(None);
    }
    let parameters = parameters.strip_suffix(')').ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "invalid StarRocks direct DECIMAL physical type",
        )
    })?;
    let mut values = parameters.split(',').map(str::trim);
    let precision = values
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|value| *value > 0 && *value <= 38)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid StarRocks direct DECIMAL precision",
            )
        })?;
    let max_precision = match kind {
        "DECIMAL32" => 9,
        "DECIMAL64" => 18,
        "DECIMAL" | "DECIMALV2" | "DECIMAL128" => 38,
        _ => unreachable!("decimal kind was validated above"),
    };
    if precision > max_precision {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "StarRocks direct DECIMAL precision exceeds its physical type",
        ));
    }
    let scale = values
        .next()
        .and_then(|value| value.parse::<i8>().ok())
        .filter(|value| *value >= 0 && *value <= precision as i8)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid StarRocks direct DECIMAL scale",
            )
        })?;
    if values.next().is_some() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "invalid StarRocks direct DECIMAL physical type",
        ));
    }
    Ok(Some(DataType::Decimal128(precision, scale)))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorCancellation, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorRequestContext,
    };

    use super::*;
    use crate::direct::storage::{
        DirectStorageConnectorReader, StarRocksStorageColumn, StarRocksStorageFixture,
        StarRocksStorageMetadataLayout, StarRocksStorageRowset, StarRocksStorageSchema,
        StarRocksStorageSegment, StarRocksStorageTablet,
    };
    use crate::direct::{
        StarRocksDirectColumnBinding, StarRocksDirectLocation, StarRocksDirectMetadataLayout,
        StarRocksDirectTabletDescriptor, StarRocksStorageBindingRef,
    };
    use crate::domain::{
        StarRocksFreezeDigest, StarRocksReadAttemptId, StarRocksSelectedStrategy,
        StarRocksSplitPlanningInput, StarRocksTopology,
    };

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct FixtureResolver(StarRocksStorageFixture);
    impl StarRocksDirectStorageResolver for FixtureResolver {
        fn open_direct_storage(
            &self,
            _: &StarRocksDirectSplit,
            request: ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
            let reader = self.0.open_reader(
                request.expected_schema.clone(),
                request.batch,
                request.context.clone(),
            )?;
            Ok(Box::new(DirectStorageConnectorReader::new(
                Box::new(reader),
                request.context,
            )))
        }
    }

    fn request(schema: SchemaRef) -> ConnectorOpenReaderRequest {
        ConnectorOpenReaderRequest {
            expected_schema: schema,
            batch: ConnectorBatchBudget {
                max_rows: NonZeroUsize::new(1).unwrap(),
                max_bytes: NonZeroUsize::new(1024).unwrap(),
            },
            context: ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                Arc::new(NeverCancelled),
                4096,
                8192,
            )
            .unwrap(),
        }
    }

    fn split_and_fixture() -> (StarRocksDirectSplit, StarRocksStorageFixture, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let input = StarRocksSplitPlanningInput {
            owner: ConnectorInstanceId::parse("catalog.direct").unwrap(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
            attempt: StarRocksReadAttemptId::new(),
            freeze: StarRocksFreezeDigest([2; 32]),
            strategy: StarRocksSelectedStrategy::SharedDataDirect,
            topology: StarRocksTopology::SharedData,
            namespace: Arc::from("db"),
            table: Arc::from("tbl"),
            schema_version: Bytes::from_static(b"schema"),
            data_version: Bytes::from_static(b"data"),
            output_schema: schema.clone(),
            projection: vec![0],
            limit: None,
        };
        let split = StarRocksDirectSplit::from_planning(
            &input,
            StarRocksDirectTabletDescriptor::try_new(
                1,
                2,
                3,
                StarRocksDirectMetadataLayout::Standalone,
                "meta/1.meta",
                vec![
                    StarRocksDirectColumnBinding::try_new(0, 1, "id", "BIGINT", false, None)
                        .unwrap(),
                ],
                None,
            )
            .unwrap(),
            StarRocksDirectLocation::try_new(
                1,
                "s3://bucket/tablet",
                StarRocksStorageBindingRef::parse("volume").unwrap(),
                "fs-key",
            )
            .unwrap(),
        )
        .unwrap();
        let storage_schema = StarRocksStorageSchema::try_new(vec![
            StarRocksStorageColumn::try_new(1, "id", DataType::Int64, false).unwrap(),
        ])
        .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let fixture = StarRocksStorageFixture::try_new(
            StarRocksStorageTablet::try_new(
                1,
                2,
                3,
                Bytes::from_static(b"schema"),
                Bytes::from_static(b"data"),
                StarRocksStorageMetadataLayout::Standalone,
            )
            .unwrap(),
            storage_schema,
            vec![
                StarRocksStorageRowset::try_new(
                    1,
                    1,
                    vec![StarRocksStorageSegment::try_new(0, batch).unwrap()],
                )
                .unwrap(),
            ],
            None,
            vec![],
            vec![],
        )
        .unwrap();
        (split, fixture, schema)
    }

    #[test]
    fn concrete_factory_returns_budgeted_arrow_reader() {
        let (split, fixture, schema) = split_and_fixture();
        let factory =
            StarRocksSharedDataDirectReaderFactory::new(Arc::new(FixtureResolver(fixture)));
        let mut reader = factory.open_direct_reader(split, request(schema)).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap().num_rows(), 1);
        assert_eq!(reader.next_batch().unwrap().unwrap().num_rows(), 1);
        assert!(reader.next_batch().unwrap().is_none());
        reader.close().unwrap();
        reader.close().unwrap();
    }

    #[test]
    fn physical_scalar_types_include_temporal_and_decimal_contracts() {
        assert_eq!(expected_arrow_type("DATE").unwrap(), DataType::Date32);
        assert_eq!(
            expected_arrow_type("datetime_v2").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            expected_arrow_type("DECIMAL128(38, 9)").unwrap(),
            DataType::Decimal128(38, 9)
        );
        assert_eq!(
            expected_arrow_type("DECIMAL64(19, 4)")
                .expect_err("DECIMAL64 cannot exceed its Arrow precision ceiling")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(expected_arrow_type("JSON").unwrap(), DataType::Binary);
    }
}
