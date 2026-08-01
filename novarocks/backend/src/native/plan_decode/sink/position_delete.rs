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

use super::super::error::NativeFragmentLeafDecodeError;
use crate::native::type_decode::decode_type;
use novarocks::connector::iceberg::position_delete_descriptor::{
    PositionDeleteDescriptorInput, PositionDeleteExpectedBinding, bind_position_delete_descriptor,
};
use novarocks::protocol::common::error::ProtocolErrorKind;
use novarocks_protocol::plan;

fn position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
) -> Result<PositionDeleteDescriptorInput, NativeFragmentLeafDecodeError> {
    let desc = desc.ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "position_delete_output_descriptor",
            "native position delete output descriptor is missing",
        )
    })?;
    let file_path = desc.file_path.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "file_path",
            "native position delete file_path descriptor is missing",
        )
    })?;
    let pos = desc.pos.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "pos",
            "native position delete pos descriptor is missing",
        )
    })?;
    Ok(PositionDeleteDescriptorInput {
        file_path: position_delete_output_field_from_native("file_path", file_path)
            .map_err(|error| error.prepend_field("file_path"))?,
        pos: position_delete_output_field_from_native("pos", pos)
            .map_err(|error| error.prepend_field("pos"))?,
        partition_source_fields: desc
            .partition_source_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                position_delete_partition_source_field_from_native(field).map_err(|error| {
                    error
                        .prepend_index(index)
                        .prepend_field("partition_source_fields")
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        target_partition_spec_id: desc.target_partition_spec_id,
    })
}

pub(crate) fn bind_position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
    expected: PositionDeleteExpectedBinding,
) -> Result<
    novarocks::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorBinding,
    NativeFragmentLeafDecodeError,
> {
    let desc = desc.ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "position_delete_output_descriptor",
            "native position delete output descriptor is missing",
        )
    })?;
    let desc = position_delete_descriptor_from_native(Some(desc))
        .map_err(|error| error.prepend_field("position_delete_output_descriptor"))?;
    bind_position_delete_descriptor(&desc, &expected).map_err(|err| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "position_delete_output_descriptor",
            err.to_bracketed_user_message(),
        )
    })
}

fn position_delete_output_field_from_native(
    label: &str,
    field: &plan::PositionDeleteOutputField,
) -> Result<
    novarocks::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField,
    NativeFragmentLeafDecodeError,
> {
    let output_expr_index = usize::try_from(field.output_expr_index).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "output_expr_index",
            format!("native position delete {label} output_expr_index overflows usize"),
        )
    })?;
    let data_type = field.data_type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "data_type",
            format!("native position delete {label} data_type is missing"),
        )
    })?;
    let data_type = decode_type(data_type).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "data_type", error)
    })?;
    Ok(
        novarocks::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField {
            output_expr_index,
            name: field.name.clone(),
            data_type,
            field_id: field.field_id,
        },
    )
}

fn position_delete_partition_source_field_from_native(
    field: &plan::PositionDeletePartitionSourceField,
) -> Result<
    novarocks::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField,
    NativeFragmentLeafDecodeError,
> {
    let output_expr_index = usize::try_from(field.output_expr_index).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "output_expr_index",
            "native position delete partition source output_expr_index overflows usize",
        )
    })?;
    let data_type = field.data_type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "data_type",
            "native position delete partition source data_type is missing",
        )
    })?;
    let data_type = decode_type(data_type).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "data_type", error)
    })?;
    Ok(
        novarocks::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField {
            output_expr_index,
            source_column_name: field.source_column_name.clone(),
            partition_field_name: field.partition_field_name.clone(),
            transform_expr: field.transform_expr.clone(),
            source_field_id: field.source_field_id,
            data_type,
        },
    )
}
