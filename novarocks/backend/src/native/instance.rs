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

//! Backend-owned decoding for the `InstanceParams` wire portion of a native
//! fragment submission.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use novarocks::exec::fragment::program::FragmentNodeId;
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks::runtime::fragment::{
    BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
};
use novarocks::runtime::query_options::QueryOptions;
use novarocks::runtime::scan_range::{
    DatacacheOptions, DeletionVectorDescriptor, FileFormat, FilePruningMinMaxValue,
    FilePruningValueKind, FileScanRange, IcebergDeleteFile, IcebergFileContent, IcebergFileFormat,
    ScanRange, ScanRangeParams, StarRocksTabletScanRange,
};
use novarocks_protocol::{common, novarocks as proto};
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

use super::ingress::NativeFragmentIngressError;

/// Backend-decoded execution values from `InstanceParams`.
#[derive(Debug)]
pub(crate) struct NativeFragmentInstanceInput {
    pub(crate) query_id: QueryId,
    pub(crate) fragment_instance_id: FragmentInstanceId,
    pub(crate) backend_num: BackendNum,
    pub(crate) query_options: QueryOptions,
    pub(crate) pipeline_dop: NonZeroUsize,
    pub(crate) raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
    pub(crate) exchange_inputs: ExchangeInputAssignments,
    pub(crate) typed_result_sink: bool,
}

impl NativeFragmentInstanceInput {
    #[allow(clippy::too_many_arguments)]
    fn new(
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
        backend_num: BackendNum,
        query_options: QueryOptions,
        pipeline_dop: NonZeroUsize,
        raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
        exchange_inputs: ExchangeInputAssignments,
        typed_result_sink: bool,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_id,
            backend_num,
            query_options,
            pipeline_dop,
            raw_scan_ranges,
            exchange_inputs,
            typed_result_sink,
        }
    }
}

pub(crate) fn decode_instance_params(
    src: &proto::InstanceParams,
) -> Result<NativeFragmentInstanceInput, NativeFragmentIngressError> {
    let path = FieldPath::root("instance_params");
    let query_id = src.query_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_id"),
            "native InstanceParams requires query_id",
        )
    })?;
    let fragment_instance_id = src.fragment_instance_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("fragment_instance_id"),
            "native InstanceParams requires fragment_instance_id",
        )
    })?;
    if src.backend_num < 0 {
        return Err(out_of_range(
            path.clone().field("backend_num"),
            format!("backend_num must be non-negative, got {}", src.backend_num),
        ));
    }
    let backend_num = BackendNum::try_new(src.backend_num)
        .map_err(|error| NativeFragmentIngressError::new(error.to_string()))?;
    let wire_query_options = src.query_options.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_options"),
            "native InstanceParams requires query_options with explicit pipeline_dop",
        )
    })?;
    let query_options = novarocks::protocol::decode_native_query_options(wire_query_options)
        .map_err(|error| NativeFragmentIngressError::new(error.to_string()))?;
    let pipeline_dop = usize::try_from(wire_query_options.pipeline_dop)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            out_of_range(
                path.clone().field("query_options").field("pipeline_dop"),
                format!(
                    "pipeline_dop must be explicitly positive, got {}",
                    wire_query_options.pipeline_dop
                ),
            )
        })?;

    let mut scan_keys = src.per_node_scan_ranges.keys().copied().collect::<Vec<_>>();
    scan_keys.sort_unstable();
    let mut raw_scan_ranges = BTreeMap::new();
    for raw_node_id in scan_keys {
        let list_path = path
            .clone()
            .field("per_node_scan_ranges")
            .map_key(raw_node_id.to_string());
        let wire_ranges = &src.per_node_scan_ranges[&raw_node_id];
        let mut ranges = Vec::with_capacity(wire_ranges.ranges.len());
        for (index, range) in wire_ranges.ranges.iter().enumerate() {
            ranges.push(decode_scan_range_params_at(
                range,
                list_path.clone().field("ranges").index(index),
            )?);
        }
        raw_scan_ranges.insert(FragmentNodeId::new(raw_node_id), ranges);
    }

    let mut exchange_keys = src.per_exch_num_senders.keys().copied().collect::<Vec<_>>();
    exchange_keys.sort_unstable();
    let mut exchange_inputs = BTreeMap::new();
    for raw_node_id in exchange_keys {
        let sender_count = src.per_exch_num_senders[&raw_node_id];
        let count = usize::try_from(sender_count)
            .ok()
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                out_of_range(
                    path.clone()
                        .field("per_exch_num_senders")
                        .map_key(raw_node_id.to_string()),
                    format!("sender count must be positive, got {sender_count}"),
                )
            })?;
        exchange_inputs.insert(
            FragmentNodeId::new(raw_node_id),
            ExchangeInputAssignment::new(count),
        );
    }
    Ok(NativeFragmentInstanceInput::new(
        query_id_from_native(query_id),
        FragmentInstanceId::new(unique_id_from_native(fragment_instance_id)),
        backend_num,
        query_options,
        pipeline_dop,
        raw_scan_ranges,
        ExchangeInputAssignments::new(exchange_inputs),
        src.typed_result_sink,
    ))
}

fn decode_scan_range_params_at(
    src: &proto::ScanRangeParams,
    path: FieldPath,
) -> Result<ScanRangeParams, NativeFragmentIngressError> {
    let range = src.range.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("range"),
            "native ScanRangeParams requires range",
        )
    })?;
    let kind = range.kind.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("range").field("kind"),
            "native ScanRange requires kind",
        )
    })?;
    let range = match kind {
        proto::scan_range::Kind::File(file) => ScanRange::File(decode_file_scan_range(
            file,
            path.clone().field("range").field("file"),
        )?),
        proto::scan_range::Kind::StarrocksTablet(tablet) => ScanRange::StarRocksTablet(
            StarRocksTabletScanRange::try_new(
                tablet.tablet_id,
                tablet.partition_id,
                tablet.version,
            )
            .map_err(|detail| {
                invalid_value(
                    path.clone().field("range").field("starrocks_tablet"),
                    detail,
                )
            })?,
        ),
    };
    Ok(ScanRangeParams {
        range,
        volume_id: src.volume_id,
        empty: src.empty,
        has_more: src.has_more,
    })
}

fn decode_file_scan_range(
    src: &proto::FileScanRange,
    path: FieldPath,
) -> Result<FileScanRange, NativeFragmentIngressError> {
    let file_format = match src.file_format.to_ascii_uppercase().as_str() {
        "PARQUET" => FileFormat::Parquet,
        "ORC" => FileFormat::Orc,
        value => {
            return Err(invalid_enum(
                path.clone().field("file_format"),
                format!("unsupported file_format {value}"),
            ));
        }
    };
    let delete_files = src
        .delete_files
        .iter()
        .enumerate()
        .map(|(index, delete_file)| {
            decode_iceberg_delete_file(delete_file, path.clone().field("delete_files").index(index))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let file_pruning_min_max_values = if src.file_pruning_min_max_values.is_empty() {
        None
    } else {
        let mut ordinals = src
            .file_pruning_min_max_values
            .keys()
            .copied()
            .collect::<Vec<_>>();
        ordinals.sort_unstable();
        let mut values = BTreeMap::new();
        for ordinal in ordinals {
            let value = decode_file_pruning_value(
                &src.file_pruning_min_max_values[&ordinal],
                path.clone()
                    .field("file_pruning_min_max_values")
                    .map_key(ordinal.to_string()),
            )?;
            values.insert(ordinal, value);
        }
        Some(values)
    };
    let ivm_change_op = src
        .change_op
        .map(|value| {
            i8::try_from(value).map_err(|_| {
                out_of_range(
                    path.clone().field("change_op"),
                    format!("change_op {value} exceeds i8 range"),
                )
            })
        })
        .transpose()?;
    Ok(FileScanRange {
        file_format,
        full_path: src.full_path.clone(),
        relative_path: src.relative_path.clone(),
        table_id: src.table_id,
        offset: src.offset,
        length: src.length,
        file_length: src.file_length,
        delete_files,
        deletion_vector_descriptor: src.deletion_vector_descriptor.as_ref().map(|descriptor| {
            DeletionVectorDescriptor {
                storage_type: descriptor.storage_type.clone(),
                path_or_inline_dv: descriptor.path_or_inline_dv.clone(),
                offset: descriptor.offset,
                size_in_bytes: descriptor.size_in_bytes,
                cardinality: descriptor.cardinality,
            }
        }),
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        modification_time: src.modification_time,
        datacache_options: src
            .datacache_options
            .as_ref()
            .map(|options| DatacacheOptions {
                enable_populate_datacache: options.enable_populate_datacache,
                priority: options.priority,
            }),
        candidate_node: None,
        included_positions: src.included_positions.clone(),
        serialized_split: src.serialized_split.clone(),
        use_iceberg_jni_metadata_reader: src.use_iceberg_jni_metadata_reader,
        ivm_change_op,
        file_pruning_min_max_values,
    })
}

fn decode_iceberg_delete_file(
    src: &proto::IcebergDeleteFile,
    path: FieldPath,
) -> Result<IcebergDeleteFile, NativeFragmentIngressError> {
    let file_format = match src.file_format.to_ascii_uppercase().as_str() {
        "PARQUET" => IcebergFileFormat::Parquet,
        value => {
            return Err(invalid_enum(
                path.clone().field("file_format"),
                format!("unsupported Iceberg file_format {value}"),
            ));
        }
    };
    let file_content = match src.file_content.to_ascii_uppercase().as_str() {
        "POSITION_DELETES" => IcebergFileContent::PositionDeletes,
        "EQUALITY_DELETES" => IcebergFileContent::EqualityDeletes,
        value => {
            return Err(invalid_enum(
                path.field("file_content"),
                format!("unsupported Iceberg file_content {value}"),
            ));
        }
    };
    Ok(IcebergDeleteFile {
        full_path: src.full_path.clone(),
        file_format,
        file_content,
        length: src.length,
    })
}

fn decode_file_pruning_value(
    src: &proto::FilePruningMinMaxValue,
    path: FieldPath,
) -> Result<FilePruningMinMaxValue, NativeFragmentIngressError> {
    let value_kind = match src.value_kind {
        1 => FilePruningValueKind::Bool,
        2 => FilePruningValueKind::Int,
        3 => FilePruningValueKind::Float,
        value => {
            return Err(invalid_enum(
                path.field("value_kind"),
                format!("unsupported file pruning value_kind {value}"),
            ));
        }
    };
    Ok(FilePruningMinMaxValue {
        value_kind,
        has_null: src.has_null,
        all_null: src.all_null,
        min_int_value: src.min_int_value,
        max_int_value: src.max_int_value,
        min_float_value: src.min_float_value,
        max_float_value: src.max_float_value,
    })
}

fn protocol_error(
    path: FieldPath,
    kind: ProtocolErrorKind,
    detail: impl Into<String>,
) -> NativeFragmentIngressError {
    NativeFragmentIngressError::new(
        ProtocolError::new(ProtocolFamily::Native, path, kind, detail.into()).to_string(),
    )
}

fn missing(path: FieldPath, detail: impl Into<String>) -> NativeFragmentIngressError {
    protocol_error(path, ProtocolErrorKind::MissingField, detail)
}

fn invalid_value(path: FieldPath, detail: impl Into<String>) -> NativeFragmentIngressError {
    protocol_error(path, ProtocolErrorKind::InvalidValue, detail)
}

fn invalid_enum(path: FieldPath, detail: impl Into<String>) -> NativeFragmentIngressError {
    protocol_error(path, ProtocolErrorKind::InvalidEnum, detail)
}

fn out_of_range(path: FieldPath, detail: impl Into<String>) -> NativeFragmentIngressError {
    protocol_error(path, ProtocolErrorKind::OutOfRange, detail)
}

fn unique_id_from_native(src: &common::UniqueId) -> UniqueId {
    UniqueId::new(src.hi, src.lo)
}

fn query_id_from_native(src: &common::UniqueId) -> QueryId {
    QueryId::new(src.hi, src.lo)
}

#[cfg(test)]
mod tests {
    use super::decode_instance_params;
    use novarocks_protocol::{common, novarocks};

    fn valid_params() -> novarocks::InstanceParams {
        novarocks::InstanceParams {
            query_id: Some(common::UniqueId { hi: 7, lo: 8 }),
            fragment_instance_id: Some(common::UniqueId { hi: 9, lo: 10 }),
            backend_num: 1,
            query_options: Some(novarocks::QueryOptions {
                pipeline_dop: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn instance_decode_preserves_required_field_error_text() {
        let error = decode_instance_params(&novarocks::InstanceParams::default())
            .expect_err("query id is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at instance_params.query_id (missing field): native InstanceParams requires query_id"
        );
    }

    #[test]
    fn instance_decode_preserves_scan_range_error_text() {
        let mut params = valid_params();
        params.per_node_scan_ranges.insert(
            4,
            novarocks::ScanRangeList {
                ranges: vec![novarocks::ScanRangeParams::default()],
            },
        );
        let error = decode_instance_params(&params).expect_err("range is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at instance_params.per_node_scan_ranges[\"4\"].ranges[0].range (missing field): native ScanRangeParams requires range"
        );
    }
}
