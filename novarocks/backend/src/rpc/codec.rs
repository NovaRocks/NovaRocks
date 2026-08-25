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

use std::any::TypeId;
use std::marker::PhantomData;

use bytes::Buf;
use prost::Message;
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

// Tonic codec guards for the Backend RPC surface.

const PLAN_FIELD: u32 = 1;
const INSTANCE_PARAMS_FIELD: u32 = 2;
const STAGE_FRAGMENTS_FIELD: u32 = 5;
const RETIRED_RUNTIME_FILTER_PARAMS_FIELD: u32 = 7;
const PLAN_RUNTIME_FILTER_BINDINGS_FIELD: u32 = 10;
const RUNTIME_FILTER_TABLE_BINDING_FIELD: u32 = 2;
const RUNTIME_FILTER_BINDING_PRODUCER_FIELD: u32 = 8;
const PRODUCER_JOIN_BUILD_KEY_FIELD: u32 = 3;
const PRODUCER_AGGREGATE_TOPN_KEY_FIELD: u32 = 4;

/// Native protobuf codec used by generated NovaRocks clients and servers.
///
/// Prost intentionally discards unknown fields. That behavior is correct for
/// every native message except the retired `InstanceParams` tag 7 and ambiguous
/// producer target oneofs: accepting either would make invalid raw wire appear
/// to satisfy the current contract after unknown-field or oneof information is
/// discarded. The decoder therefore inspects only the affected paths in raw
/// `StageFragmentsRequest` bytes before delegating to Prost.
#[derive(Debug, Clone)]
pub(crate) struct NativeProstCodec<T, U> {
    marker: PhantomData<(T, U)>,
}

impl<T, U> Default for NativeProstCodec<T, U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T, U> Codec for NativeProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = NativeProstEncoder<T>;
    type Decoder = NativeProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        NativeProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        NativeProstDecoder::default()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstEncoder<T> {
    marker: PhantomData<T>,
    buffer_settings: BufferSettings,
}

impl<T> Default for NativeProstEncoder<T> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

impl<T: Message> Encoder for NativeProstEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        item.encode(destination)
            .expect("Message only errors if not enough space");
        Ok(())
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstDecoder<U> {
    marker: PhantomData<U>,
    buffer_settings: BufferSettings,
}

impl<U> Default for NativeProstDecoder<U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

impl<U> Decoder for NativeProstDecoder<U>
where
    U: Message + Default + Send + 'static,
{
    type Item = U;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let is_stage_fragments =
            TypeId::of::<U>() == TypeId::of::<novarocks_proto::novarocks::StageFragmentsRequest>();
        if is_stage_fragments {
            let bytes = source.chunk();
            if bytes.len() != source.remaining() {
                return Err(Status::internal(
                    "native fragment request protobuf is not contiguous",
                ));
            }
            let result = scan_stage_fragments_request(bytes);
            match result {
                Ok(()) | Err(WireScanError::Decode(_)) => {}
                Err(WireScanError::RetiredInstanceParamsField) => {
                    return Err(retired_instance_params_field_status());
                }
                Err(WireScanError::AmbiguousProducerBindingTarget) => {
                    return Err(ambiguous_producer_binding_target_status());
                }
            }
        }

        U::decode(source)
            .map(Some)
            .map_err(|error| Status::internal(error.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[expect(
    clippy::result_large_err,
    reason = "The gRPC validation boundary returns tonic status directly."
)]
#[allow(
    dead_code,
    reason = "Retained for direct native wire validation callers."
)]
pub(crate) fn validate_stage_fragments_request_wire(bytes: &[u8]) -> Result<(), Status> {
    match scan_stage_fragments_request(bytes) {
        Ok(()) => Ok(()),
        Err(WireScanError::RetiredInstanceParamsField) => {
            Err(retired_instance_params_field_status())
        }
        Err(WireScanError::AmbiguousProducerBindingTarget) => {
            Err(ambiguous_producer_binding_target_status())
        }
        Err(WireScanError::Decode(error)) => Err(Status::internal(error.to_string())),
    }
}

fn retired_instance_params_field_status() -> Status {
    Status::invalid_argument(
        "retired InstanceParams tag 7 runtime_filter_params is not accepted by native submission",
    )
}

fn ambiguous_producer_binding_target_status() -> Status {
    Status::invalid_argument(
        "native runtime-filter producer target carries both join_build_key and aggregate_topn_key",
    )
}

fn scan_stage_fragments_request(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == STAGE_FRAGMENTS_FIELD && wire_type == WireType::LengthDelimited {
            scan_stage_fragment(take_length_delimited(&mut cursor)?)?;
            continue;
        }
        skip_field(wire_type, field, &mut cursor, context.clone())?;
    }
    Ok(())
}

fn scan_stage_fragment(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if wire_type == WireType::LengthDelimited {
            match field {
                PLAN_FIELD => {
                    scan_plan_fragment(take_length_delimited(&mut cursor)?)?;
                    continue;
                }
                INSTANCE_PARAMS_FIELD => {
                    scan_instance_params(take_length_delimited(&mut cursor)?)?;
                    continue;
                }
                _ => {}
            }
        }
        skip_field(wire_type, field, &mut cursor, context.clone())?;
    }
    Ok(())
}

fn scan_plan_fragment(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == PLAN_RUNTIME_FILTER_BINDINGS_FIELD && wire_type == WireType::LengthDelimited {
            let table = take_length_delimited(&mut cursor)?;
            scan_runtime_filter_binding_table(table)?;
        } else {
            skip_field(wire_type, field, &mut cursor, context.clone())?;
        }
    }
    Ok(())
}

fn scan_runtime_filter_binding_table(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == RUNTIME_FILTER_TABLE_BINDING_FIELD && wire_type == WireType::LengthDelimited {
            let binding = take_length_delimited(&mut cursor)?;
            scan_runtime_filter_binding(binding)?;
        } else {
            skip_field(wire_type, field, &mut cursor, context.clone())?;
        }
    }
    Ok(())
}

fn scan_runtime_filter_binding(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    let mut target_field = None;
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == RUNTIME_FILTER_BINDING_PRODUCER_FIELD && wire_type == WireType::LengthDelimited
        {
            let producer = take_length_delimited(&mut cursor)?;
            scan_runtime_filter_producer_role(producer, &mut target_field)?;
        } else {
            skip_field(wire_type, field, &mut cursor, context.clone())?;
        }
    }
    Ok(())
}

fn scan_runtime_filter_producer_role(
    bytes: &[u8],
    target_field: &mut Option<u32>,
) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if matches!(
            field,
            PRODUCER_JOIN_BUILD_KEY_FIELD | PRODUCER_AGGREGATE_TOPN_KEY_FIELD
        ) && wire_type == WireType::LengthDelimited
        {
            if target_field.is_some_and(|seen| seen != field) {
                return Err(WireScanError::AmbiguousProducerBindingTarget);
            }
            *target_field = Some(field);
        }
        skip_field(wire_type, field, &mut cursor, context.clone())?;
    }
    Ok(())
}

fn take_length_delimited<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8], WireScanError> {
    let length = decode_varint(cursor)?;
    if length > cursor.remaining() as u64 {
        return Err(prost::DecodeError::new("buffer underflow").into());
    }
    let length = length as usize;
    let (payload, remaining) = cursor.split_at(length);
    *cursor = remaining;
    Ok(payload)
}

fn scan_instance_params(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == RETIRED_RUNTIME_FILTER_PARAMS_FIELD {
            return Err(WireScanError::RetiredInstanceParamsField);
        }
        skip_field(wire_type, field, &mut cursor, context.clone())?;
    }
    Ok(())
}

#[derive(Debug)]
enum WireScanError {
    Decode(prost::DecodeError),
    RetiredInstanceParamsField,
    AmbiguousProducerBindingTarget,
}

impl From<prost::DecodeError> for WireScanError {
    fn from(error: prost::DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl std::fmt::Display for WireScanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(error) => error.fmt(formatter),
            Self::RetiredInstanceParamsField => formatter.write_str(
                "retired InstanceParams tag 7 runtime_filter_params is not accepted by native submission",
            ),
            Self::AmbiguousProducerBindingTarget => formatter.write_str(
                "native runtime-filter producer target carries both join_build_key and aggregate_topn_key",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        INSTANCE_PARAMS_FIELD, PLAN_FIELD, PLAN_RUNTIME_FILTER_BINDINGS_FIELD,
        PRODUCER_AGGREGATE_TOPN_KEY_FIELD, PRODUCER_JOIN_BUILD_KEY_FIELD,
        RUNTIME_FILTER_BINDING_PRODUCER_FIELD, RUNTIME_FILTER_TABLE_BINDING_FIELD,
        STAGE_FRAGMENTS_FIELD, validate_stage_fragments_request_wire,
    };

    fn length_delimited(field: u32, payload: &[u8]) -> Vec<u8> {
        assert!(field <= 15);
        assert!(payload.len() < 128);
        let mut wire = vec![((field << 3) | 2) as u8, payload.len() as u8];
        wire.extend_from_slice(payload);
        wire
    }

    fn stage_with_instance(instance: &[u8]) -> Vec<u8> {
        let stage_fragment = length_delimited(INSTANCE_PARAMS_FIELD, instance);
        length_delimited(STAGE_FRAGMENTS_FIELD, &stage_fragment)
    }

    fn stage_with_ambiguous_producer_target() -> Vec<u8> {
        let producer = [
            length_delimited(PRODUCER_JOIN_BUILD_KEY_FIELD, &[]),
            length_delimited(PRODUCER_AGGREGATE_TOPN_KEY_FIELD, &[]),
        ]
        .concat();
        let binding = length_delimited(RUNTIME_FILTER_BINDING_PRODUCER_FIELD, &producer);
        let table = length_delimited(RUNTIME_FILTER_TABLE_BINDING_FIELD, &binding);
        let plan = length_delimited(PLAN_RUNTIME_FILTER_BINDINGS_FIELD, &table);
        let stage_fragment = length_delimited(PLAN_FIELD, &plan);
        length_delimited(STAGE_FRAGMENTS_FIELD, &stage_fragment)
    }

    #[test]
    fn stage_scanner_rejects_retired_instance_params_tag_in_nested_fragments() {
        let error = validate_stage_fragments_request_wire(&stage_with_instance(&[0x3a, 0]))
            .expect_err("nested retired tag must be rejected before Prost drops it");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("tag 7"), "{error}");
    }

    #[test]
    fn stage_scanner_rejects_ambiguous_producer_target_before_prost_decode() {
        let error = validate_stage_fragments_request_wire(&stage_with_ambiguous_producer_target())
            .expect_err("ambiguous producer target must be rejected before Prost selects one arm");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error
                .message()
                .contains("both join_build_key and aggregate_topn_key"),
            "{error}"
        );
    }
}
