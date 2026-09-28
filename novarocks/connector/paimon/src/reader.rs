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
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use novarocks_spi::connector::read_stack::ConnectorTableHandle;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorResourceReservation};
use paimon::DataSplit;
use paimon::io::{ReadControl, ReadExecutionResources};
use paimon::spec::{DataField, DataType};
use paimon::table::{ArrowRecordBatchStream, ExecutionTableRead, Table};

use crate::domain::{PaimonColumn, PaimonMergeEngine, PaimonReadView, PaimonSplit, PaimonTable};
use crate::schema::PaimonDataType;
use crate::sdk_control::PaimonSdkExecutionResources;

/// One SDK output batch and the exact host reservation transferred with it.
pub struct PaimonReadBatch {
    batch: RecordBatch,
    output_reservation: Option<ConnectorResourceReservation>,
}

impl PaimonReadBatch {
    pub fn unreserved(batch: RecordBatch) -> Self {
        Self {
            batch,
            output_reservation: None,
        }
    }

    pub fn with_output_reservation(
        batch: RecordBatch,
        output_reservation: ConnectorResourceReservation,
    ) -> Self {
        Self {
            batch,
            output_reservation: Some(output_reservation),
        }
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    pub(crate) fn into_parts(self) -> (RecordBatch, Option<ConnectorResourceReservation>) {
        (self.batch, self.output_reservation)
    }
}

/// Poll boundary used by the page stream: the SDK stream of one split,
/// polled by the host driver. Production uses [`PaimonAwaitedReader`]; the
/// trait keeps lifecycle tests at the connector boundary.
pub trait PaimonBatchStream: Send + Unpin {
    fn poll_next_batch(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<PaimonReadBatch>, ConnectorError>>;

    /// Drops the SDK stream; an in-flight read keeps what it paid for until
    /// its own exit.
    fn close(&mut self) -> Result<(), ConnectorError>;
}

/// The execution SDK stream of one validated split, and the projection it
/// must produce; no batch is read.
fn open_execution_stream(
    sdk_table: &Table,
    sdk_split: &DataSplit,
    projected_columns: &[PaimonColumn],
    resources: &Arc<PaimonSdkExecutionResources>,
) -> Result<(ArrowRecordBatchStream, SchemaRef), ConnectorError> {
    let read_type = projected_sdk_fields(sdk_table, projected_columns)?;
    let output_schema =
        paimon::arrow::build_target_arrow_schema(&read_type).map_err(map_paimon_error)?;
    let sdk_resources: Arc<dyn ReadExecutionResources> = resources.clone();
    let stream = ExecutionTableRead::new(sdk_table, read_type, Vec::new(), sdk_resources)
        .to_arrow(std::slice::from_ref(sdk_split))
        .map_err(map_paimon_error)?;
    Ok((stream, output_schema))
}

/// BE reader polled by the host driver: the page stream's SDK stream, whose
/// every batch is paired with the output reservation the SDK handed over.
pub struct PaimonAwaitedReader {
    stream: Option<ArrowRecordBatchStream>,
    output_schema: SchemaRef,
    resources: Arc<PaimonSdkExecutionResources>,
    _schema: Arc<paimon::table::ExecutionTableSchema>,
}

impl PaimonAwaitedReader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        sdk_table: Arc<Table>,
        table: &PaimonTable,
        view: &PaimonReadView,
        split: &PaimonSplit,
        sdk_split: DataSplit,
        projected_columns: &[PaimonColumn],
        resources: Arc<PaimonSdkExecutionResources>,
        schema: Arc<paimon::table::ExecutionTableSchema>,
    ) -> Result<Self, ConnectorError> {
        validate_frozen_input(
            sdk_table.as_ref(),
            table,
            view,
            split,
            &sdk_split,
            projected_columns,
        )?;
        let (stream, output_schema) = open_execution_stream(
            sdk_table.as_ref(),
            &sdk_split,
            projected_columns,
            &resources,
        )?;
        Ok(Self {
            stream: Some(stream),
            output_schema,
            resources,
            _schema: schema,
        })
    }
}

impl PaimonBatchStream for PaimonAwaitedReader {
    fn poll_next_batch(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<PaimonReadBatch>, ConnectorError>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(Ok(None));
        };
        if let Err(error) = self.resources.checkpoint() {
            return Poll::Ready(Err(map_paimon_error(error)));
        }
        let next = match stream.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(next) => next,
        };
        Poll::Ready(match next {
            Some(Ok(batch)) => {
                if batch.schema().as_ref() != self.output_schema.as_ref() {
                    return Poll::Ready(Err(corrupt(
                        "Paimon SDK returned a batch whose schema differs from the frozen projection",
                    )));
                }
                // The SDK hands the output reservation over before yielding
                // its batch, so the pair is complete here.
                self.resources.take_output_reservation().map(|reservation| {
                    Some(match reservation {
                        Some(reservation) => {
                            PaimonReadBatch::with_output_reservation(batch, reservation)
                        }
                        None => PaimonReadBatch::unreserved(batch),
                    })
                })
            }
            Some(Err(error)) => Err(map_paimon_error(error)),
            None => {
                self.stream = None;
                self.resources.take_output_reservation().map(|_| None)
            }
        })
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.stream = None;
        drop(self.resources.take_output_reservation()?);
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_frozen_input(
    sdk_table: &Table,
    table: &PaimonTable,
    view: &PaimonReadView,
    split: &PaimonSplit,
    sdk_split: &DataSplit,
    projected_columns: &[PaimonColumn],
) -> Result<(), ConnectorError> {
    let name = table.schema_table_name();
    if sdk_table.identifier().database() != name.schema_name()
        || sdk_table.identifier().object() != name.table_name()
    {
        return Err(invalid(
            "frozen Paimon SDK table identity differs from the provider table handle",
        ));
    }
    if sdk_table.location() != table.location()
        || view.table_location() != table.location()
        || sdk_table.schema().id() != view.schema_id()
        || split.schema_id() != view.schema_id()
    {
        return Err(invalid(
            "Paimon table, frozen view and split do not identify one schema generation",
        ));
    }
    let snapshot_id = view.snapshot_id().ok_or_else(|| {
        invalid("a Paimon reader cannot be constructed for an empty frozen snapshot")
    })?;
    if split.snapshot_id() != snapshot_id || sdk_split.snapshot_id() != snapshot_id {
        return Err(invalid(
            "Paimon SDK split does not belong to the frozen snapshot",
        ));
    }
    if sdk_split.bucket() != split.bucket()
        || sdk_split.partition().arity() != split.partition_arity()
        || sdk_split.partition().data() != split.partition()
        || sdk_split.bucket_path() != split.bucket_path()
        || sdk_split.total_buckets() != split.total_buckets()
        || sdk_split.raw_convertible() != split.raw_convertible()
    {
        return Err(invalid(
            "Paimon SDK split partition or bucket differs from the provider split",
        ));
    }

    validate_table_semantics(sdk_table, table)?;
    validate_sequence_field(sdk_table, view)?;
    validate_split_files(split, sdk_split)?;
    validate_projection(sdk_table, projected_columns)?;
    Ok(())
}

fn validate_sequence_field(sdk_table: &Table, view: &PaimonReadView) -> Result<(), ConnectorError> {
    let core_options = sdk_table.schema().core_options();
    let sequence_fields = core_options.sequence_fields();
    if sequence_fields.len() > 1 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "PAI-1 supports at most one Paimon sequence field",
        ));
    }
    let sequence_id = sequence_fields
        .first()
        .map(|name| {
            sdk_table
                .schema()
                .fields()
                .iter()
                .find(|field| field.name() == *name)
                .map(DataField::id)
                .ok_or_else(|| corrupt("Paimon sequence field is missing from the frozen schema"))
        })
        .transpose()?;
    if sequence_id != view.sequence_field_id() {
        return Err(invalid(
            "Paimon sequence field differs from the frozen read recipe",
        ));
    }
    Ok(())
}

fn validate_table_semantics(sdk_table: &Table, table: &PaimonTable) -> Result<(), ConnectorError> {
    let schema = sdk_table.schema();
    let fields = schema.fields();
    let ids_for_names = |names: &[String]| -> Result<Vec<i32>, ConnectorError> {
        names
            .iter()
            .map(|name| {
                fields
                    .iter()
                    .find(|field| field.name() == name)
                    .map(DataField::id)
                    .ok_or_else(|| corrupt("Paimon key references a missing frozen schema field"))
            })
            .collect()
    };
    if ids_for_names(schema.primary_keys())? != table.primary_key_field_ids()
        || ids_for_names(schema.partition_keys())? != table.partition_field_ids()
    {
        return Err(invalid(
            "Paimon key fields differ between the SDK table and provider table handle",
        ));
    }
    let sdk_merge = if schema.primary_keys().is_empty() {
        PaimonMergeEngine::AppendOnly
    } else {
        match schema
            .core_options()
            .merge_engine()
            .map_err(map_paimon_error)?
        {
            paimon::spec::MergeEngine::Deduplicate => PaimonMergeEngine::Deduplicate,
            _ => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "PAI-1 supports only append-only and deduplicate Paimon tables",
                ));
            }
        }
    };
    if sdk_merge != table.merge_engine() {
        return Err(invalid(
            "Paimon merge engine differs between the frozen SDK table and provider handle",
        ));
    }
    Ok(())
}

fn validate_split_files(split: &PaimonSplit, sdk_split: &DataSplit) -> Result<(), ConnectorError> {
    if split.files().len() != sdk_split.data_files().len() {
        return Err(invalid(
            "Paimon provider split and SDK split contain different file groups",
        ));
    }
    for (file, sdk_file) in split.files().iter().zip(sdk_split.data_files()) {
        let sdk_size = u64::try_from(sdk_file.file_size)
            .map_err(|_| corrupt("Paimon SDK split contains a negative file size"))?;
        let sdk_rows = u64::try_from(sdk_file.row_count)
            .map_err(|_| corrupt("Paimon SDK split contains a negative row count"))?;
        let facts = file.facts();
        if facts.file_name != sdk_file.file_name
            || file.file_size() != sdk_size
            || file.schema_id() != sdk_file.schema_id
            || file.level() != sdk_file.level
            || file.min_sequence_number() != sdk_file.min_sequence_number
            || file.max_sequence_number() != sdk_file.max_sequence_number
            || file.row_count() != sdk_rows
            || facts.min_key != sdk_file.min_key
            || facts.max_key != sdk_file.max_key
            || facts.key_stats.min_values() != sdk_file.key_stats.min_values()
            || facts.key_stats.max_values() != sdk_file.key_stats.max_values()
            || facts.key_stats.null_counts() != sdk_file.key_stats.null_counts()
            || facts.value_stats.min_values() != sdk_file.value_stats.min_values()
            || facts.value_stats.max_values() != sdk_file.value_stats.max_values()
            || facts.value_stats.null_counts() != sdk_file.value_stats.null_counts()
            || facts.extra_files != sdk_file.extra_files
            || facts.creation_time_millis
                != sdk_file.creation_time.map(|value| value.timestamp_millis())
            || facts.delete_row_count
                != sdk_file
                    .delete_row_count
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| corrupt("Paimon SDK split contains a negative delete count"))?
            || facts.embedded_index != sdk_file.embedded_index
            || facts.file_source != sdk_file.file_source
            || facts.value_stats_cols != sdk_file.value_stats_cols
            || facts.external_path != sdk_file.external_path
            || facts.first_row_id != sdk_file.first_row_id
            || facts.write_cols != sdk_file.write_cols
        {
            return Err(invalid(
                "Paimon provider split file facts differ from its SDK split",
            ));
        }
    }
    if !split.contains_delete_rows()
        && sdk_split
            .data_files()
            .iter()
            .any(|file| file.delete_row_count.is_some_and(|count| count > 0))
    {
        return Err(invalid(
            "Paimon provider split hides delete rows declared by the SDK split",
        ));
    }
    validate_deletion_files(split, sdk_split)?;
    validate_row_ranges(split, sdk_split)?;
    Ok(())
}

fn validate_deletion_files(
    split: &PaimonSplit,
    sdk_split: &DataSplit,
) -> Result<(), ConnectorError> {
    let domain = split.data_deletion_files();
    let sdk = sdk_split.data_deletion_files();
    if domain.map(<[_]>::len) != sdk.map(<[_]>::len) {
        return Err(invalid(
            "Paimon provider split and SDK split contain different deletion files",
        ));
    }
    for (domain, sdk) in domain.into_iter().flatten().zip(sdk.into_iter().flatten()) {
        match (domain, sdk) {
            (None, None) => {}
            (Some(domain), Some(sdk)) => {
                let offset = u64::try_from(sdk.offset())
                    .map_err(|_| corrupt("Paimon SDK deletion file has a negative offset"))?;
                let length = u64::try_from(sdk.length())
                    .map_err(|_| corrupt("Paimon SDK deletion file has a negative length"))?;
                let cardinality = sdk
                    .cardinality()
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| corrupt("Paimon SDK deletion file has negative cardinality"))?;
                if domain.path() != sdk.path()
                    || domain.offset() != offset
                    || domain.length() != length
                    || domain.cardinality() != cardinality
                {
                    return Err(invalid(
                        "Paimon provider deletion file differs from its SDK split",
                    ));
                }
            }
            _ => {
                return Err(invalid(
                    "Paimon provider split and SDK split disagree on deletion-file presence",
                ));
            }
        }
    }
    Ok(())
}

fn validate_row_ranges(split: &PaimonSplit, sdk_split: &DataSplit) -> Result<(), ConnectorError> {
    let domain = split.row_ranges();
    let sdk = sdk_split.row_ranges();
    if domain.map(<[_]>::len) != sdk.map(<[_]>::len) {
        return Err(invalid(
            "Paimon provider split and SDK split contain different row ranges",
        ));
    }
    if domain
        .into_iter()
        .flatten()
        .zip(sdk.into_iter().flatten())
        .any(|(domain, sdk)| domain.from() != sdk.from() || domain.to() != sdk.to())
    {
        return Err(invalid(
            "Paimon provider row range differs from its SDK split",
        ));
    }
    Ok(())
}

fn validate_projection(
    sdk_table: &Table,
    projected_columns: &[PaimonColumn],
) -> Result<(), ConnectorError> {
    let mut ordinals = BTreeSet::new();
    for column in projected_columns {
        if !ordinals.insert(column.output_ordinal()) {
            return Err(invalid(
                "Paimon projection contains duplicate output ordinals",
            ));
        }
        let field = sdk_table
            .schema()
            .fields()
            .iter()
            .find(|field| field.id() == column.field_id())
            .ok_or_else(|| invalid("Paimon projection references an unknown field ID"))?;
        validate_column(field, column)?;
    }
    if projected_columns
        .iter()
        .enumerate()
        .any(|(index, column)| column.output_ordinal() != index as u32)
    {
        return Err(invalid(
            "Paimon projection output ordinals must be contiguous and ordered",
        ));
    }
    Ok(())
}

fn projected_sdk_fields(
    sdk_table: &Table,
    projected_columns: &[PaimonColumn],
) -> Result<Vec<DataField>, ConnectorError> {
    projected_columns
        .iter()
        .map(|column| {
            sdk_table
                .schema()
                .fields()
                .iter()
                .find(|field| field.id() == column.field_id())
                .cloned()
                .ok_or_else(|| invalid("Paimon projection references an unknown field ID"))
        })
        .collect()
}

fn validate_column(field: &DataField, column: &PaimonColumn) -> Result<(), ConnectorError> {
    if field.name() != column.name()
        || field.data_type().is_nullable() != column.nullable()
        || !same_type(field.data_type(), column.data_type())
    {
        return Err(invalid(
            "Paimon projected column differs from the frozen SDK schema",
        ));
    }
    Ok(())
}

fn same_type(actual: &DataType, expected: PaimonDataType) -> bool {
    match (actual, expected) {
        (DataType::Boolean(_), PaimonDataType::Boolean)
        | (DataType::TinyInt(_), PaimonDataType::Int8)
        | (DataType::SmallInt(_), PaimonDataType::Int16)
        | (DataType::Int(_), PaimonDataType::Int32)
        | (DataType::BigInt(_), PaimonDataType::Int64)
        | (DataType::Float(_), PaimonDataType::Float32)
        | (DataType::Double(_), PaimonDataType::Float64)
        | (DataType::Char(_) | DataType::VarChar(_), PaimonDataType::Utf8)
        | (DataType::Binary(_) | DataType::VarBinary(_), PaimonDataType::Binary)
        | (DataType::Date(_), PaimonDataType::Date32) => true,
        (DataType::Decimal(value), PaimonDataType::Decimal128 { precision, scale }) => {
            value.precision() == u32::from(precision) && value.scale() == u32::from(scale)
        }
        (DataType::Timestamp(value), PaimonDataType::TimestampMillis { precision }) => {
            value.precision() == u32::from(precision) && precision <= 3
        }
        (DataType::Timestamp(value), PaimonDataType::TimestampMicros { precision }) => {
            value.precision() == u32::from(precision) && precision > 3 && precision <= 6
        }
        _ => false,
    }
}

pub(crate) fn map_paimon_error(error: paimon::Error) -> ConnectorError {
    use paimon::Error;
    if let Some(host_error) = host_connector_error(&error, 0) {
        return host_error;
    }
    let kind = match &error {
        Error::DataInvalid { .. }
        | Error::DataTypeInvalid { .. }
        | Error::DataUnexpected { .. }
        | Error::FileIndexFormatInvalid { .. }
        | Error::ParquetDataUnexpected { .. } => ConnectorErrorKind::CorruptData,
        Error::Unsupported { .. } | Error::IoUnsupported { .. } => ConnectorErrorKind::Unsupported,
        Error::ConfigInvalid { .. } | Error::IdentifierInvalid { .. } => {
            ConnectorErrorKind::InvalidRequest
        }
        Error::DatabaseNotExist { .. }
        | Error::TableNotExist { .. }
        | Error::ViewNotExist { .. }
        | Error::FunctionNotExist { .. }
        | Error::ColumnNotExist { .. } => ConnectorErrorKind::NotFound,
        Error::UnexpectedError {
            source: Some(source),
            ..
        } => {
            if let Some(connector) = source.downcast_ref::<ConnectorError>() {
                return connector.clone();
            }
            if let Some(file_error) = source.downcast_ref::<novarocks_fs::FileError>() {
                return crate::io::connector_error_from_file_error(file_error);
            }
            ConnectorErrorKind::Unavailable
        }
        Error::IoUnexpected { .. } | Error::RestApi { .. } => ConnectorErrorKind::Unavailable,
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, format!("Paimon read failed: {error}"))
}

fn host_connector_error(
    error: &(dyn std::error::Error + 'static),
    depth: usize,
) -> Option<ConnectorError> {
    if depth >= 16 {
        return None;
    }
    if let Some(connector) = error.downcast_ref::<ConnectorError>() {
        return Some(connector.clone());
    }
    if let Some(file_error) = error.downcast_ref::<novarocks_fs::FileError>() {
        return Some(crate::io::connector_error_from_file_error(file_error));
    }
    // `paimon::Error::UnexpectedError` deliberately does not expose its boxed
    // source through `std::error::Error::source`, so inspect this one SDK
    // variant explicitly before walking the ordinary source chain.
    if let Some(paimon::Error::UnexpectedError {
        source: Some(source),
        ..
    }) = error.downcast_ref::<paimon::Error>()
    {
        if let Some(error) = host_connector_error(source.as_ref(), depth + 1) {
            return Some(error);
        }
    }
    error
        .source()
        .and_then(|source| host_connector_error(source, depth + 1))
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

#[cfg(test)]
mod tests {
    use std::fmt::{Display, Formatter};
    use std::ops::Range;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream;
    use novarocks_fs::{FileError, FileErrorKind};
    use novarocks_spi::connector::read_stack::{SchemaTableName, SplitWeight};
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use paimon::catalog::Identifier;
    use paimon::io::{FileIO, FileStatus, FileStatusStream, ReadControl, ReadOnlyFileIO};
    use paimon::spec::{
        BinaryRow, DataType as SdkDataType, IntType, Schema as SdkSchema, TableSchema,
    };
    use paimon::table::Table;

    use super::{map_paimon_error, projected_sdk_fields, validate_frozen_input};
    use crate::domain::{
        PaimonBucketMode, PaimonColumn, PaimonMergeEngine, PaimonReadView, PaimonSplit, PaimonTable,
    };
    use crate::schema::PaimonDataType;

    #[derive(Debug)]
    struct NoIo;

    #[async_trait]
    impl ReadOnlyFileIO for NoIo {
        async fn stat(&self, _path: &str) -> paimon::Result<FileStatus> {
            unreachable!("frozen validation performs no I/O")
        }

        async fn exists(&self, _path: &str) -> paimon::Result<bool> {
            unreachable!("frozen validation performs no I/O")
        }

        async fn read(
            &self,
            _path: &str,
            _range: Range<u64>,
            _known_size: Option<u64>,
        ) -> paimon::Result<Bytes> {
            unreachable!("frozen validation performs no I/O")
        }

        async fn list(&self, _path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
            Ok(Box::pin(stream::empty()))
        }
    }

    #[derive(Debug)]
    struct NoControl;

    impl ReadControl for NoControl {
        fn check_active(&self) -> paimon::Result<()> {
            Ok(())
        }

        fn checkpoint(&self) -> paimon::Result<()> {
            Ok(())
        }
    }

    struct FrozenRead {
        sdk_table: Arc<Table>,
        table: PaimonTable,
        view: PaimonReadView,
        split: PaimonSplit,
        sdk_split: paimon::DataSplit,
        columns: Vec<PaimonColumn>,
    }

    fn frozen_read() -> FrozenRead {
        let location = "s3://bucket/warehouse/db/table";
        let schema = SdkSchema::builder()
            .column("id", SdkDataType::Int(IntType::with_nullable(false)))
            .column("value", SdkDataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .unwrap();
        let sdk_table = Arc::new(Table::new(
            FileIO::from_read_only(Arc::new(NoIo), Arc::new(NoControl)),
            Identifier::new("db", "table"),
            location.to_string(),
            TableSchema::new(7, &schema),
            None,
        ));
        let table = PaimonTable::try_new(
            SchemaTableName::try_new("db", "table").unwrap(),
            location,
            PaimonMergeEngine::Deduplicate,
            PaimonBucketMode::Fixed,
            vec![0],
            vec![],
        )
        .unwrap();
        let view = PaimonReadView::try_new(location, Some(11), 7, [1; 32], [2; 32], None).unwrap();
        let serialized_partition = BinaryRow::new(0).to_serialized_bytes();
        let partition = serialized_partition[4..].to_vec();
        let split = PaimonSplit::try_new(
            11,
            7,
            0,
            partition.clone(),
            0,
            format!("{location}/bucket-0"),
            1,
            vec![],
            None,
            None,
            false,
            false,
            SplitWeight::STANDARD,
        )
        .unwrap();
        let sdk_split = paimon::DataSplit::builder()
            .with_snapshot(11)
            .with_partition(BinaryRow::from_bytes(0, partition))
            .with_bucket(0)
            .with_bucket_path(format!("{location}/bucket-0"))
            .with_total_buckets(1)
            .with_data_files(vec![])
            .with_raw_convertible(false)
            .build()
            .unwrap();
        let columns = vec![
            PaimonColumn::try_new(0, "id", PaimonDataType::Int32, false, 0).unwrap(),
            PaimonColumn::try_new(1, "value", PaimonDataType::Int32, true, 1).unwrap(),
        ];
        FrozenRead {
            sdk_table,
            table,
            view,
            split,
            sdk_split,
            columns,
        }
    }

    /// The output a validated split's reader produces, without opening it.
    fn output_field_names(
        read: &FrozenRead,
        view: &PaimonReadView,
        columns: &[PaimonColumn],
    ) -> Result<Vec<String>, ConnectorError> {
        validate_frozen_input(
            read.sdk_table.as_ref(),
            &read.table,
            view,
            &read.split,
            &read.sdk_split,
            columns,
        )?;
        let read_type = projected_sdk_fields(read.sdk_table.as_ref(), columns)?;
        let schema =
            paimon::arrow::build_target_arrow_schema(&read_type).map_err(map_paimon_error)?;
        Ok(schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect())
    }

    #[test]
    fn validation_is_zero_io_and_accepts_a_zero_projection() {
        let read = frozen_read();
        assert!(
            output_field_names(&read, &read.view, &[])
                .expect("zero projection")
                .is_empty()
        );
    }

    #[test]
    fn the_reader_exposes_only_the_requested_public_columns() {
        let read = frozen_read();
        assert_eq!(
            output_field_names(&read, &read.view, &read.columns).expect("projected"),
            ["id", "value"]
        );
    }

    #[test]
    fn validation_rejects_snapshot_drift_before_sdk_io() {
        let read = frozen_read();
        let drifted =
            PaimonReadView::try_new(read.table.location(), Some(12), 7, [1; 32], [2; 32], None)
                .unwrap();
        let error = output_field_names(&read, &drifted, &read.columns)
            .expect_err("snapshot drift must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn validation_rejects_projection_type_or_order_drift() {
        let read = frozen_read();
        let wrong = vec![PaimonColumn::try_new(0, "id", PaimonDataType::Int64, false, 0).unwrap()];
        let error =
            output_field_names(&read, &read.view, &wrong).expect_err("projection drift must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[derive(Debug)]
    struct ExternalWrapper(Box<dyn std::error::Error + Send + Sync>);

    impl Display for ExternalWrapper {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("external format reader error")
        }
    }

    impl std::error::Error for ExternalWrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.0.as_ref())
        }
    }

    fn format_wrapped_host_error(
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> paimon::Error {
        let sdk_io = paimon::Error::UnexpectedError {
            message: "host read failed".to_string(),
            source: Some(Box::new(error)),
        };
        paimon::Error::DataInvalid {
            message: "format reader failed".to_string(),
            source: Some(Box::new(ExternalWrapper(Box::new(sdk_io)))),
        }
    }

    #[test]
    fn parquet_io_preserves_host_file_error_classification() {
        for (file_error, expected) in [
            (
                FileError::cancelled("cancelled by request"),
                ConnectorErrorKind::Cancelled,
            ),
            (
                FileError::deadline("request deadline elapsed"),
                ConnectorErrorKind::DeadlineExceeded,
            ),
            (
                FileError::new(FileErrorKind::ResourceExhausted, "reader budget exhausted"),
                ConnectorErrorKind::ResourceExhausted,
            ),
        ] {
            assert_eq!(
                map_paimon_error(format_wrapped_host_error(file_error)).kind(),
                expected
            );
        }
    }

    #[test]
    fn parquet_io_preserves_host_resource_connector_error() {
        let host = ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "request reader-state budget exhausted",
        );
        let mapped = map_paimon_error(format_wrapped_host_error(host.clone()));
        assert_eq!(mapped, host);
    }
}
