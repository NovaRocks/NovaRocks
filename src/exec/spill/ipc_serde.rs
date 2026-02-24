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
use std::fmt;

use arrow::array::RecordBatch;
use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use arrow::ipc::reader::FileDecoder;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
    write_message,
};
use arrow::ipc::{Block, CompressionType, MetadataVersion};

const IPC_ALIGNMENT: usize = 64;
const CONTINUATION_MARKER: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpillCodec {
    None,
    Lz4,
    Zstd,
}

impl SpillCodec {
    pub fn as_u8(self) -> u8 {
        match self {
            SpillCodec::None => 0,
            SpillCodec::Lz4 => 1,
            SpillCodec::Zstd => 2,
        }
    }

    pub fn from_str(value: &str) -> Result<Self, String> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "none" => Ok(SpillCodec::None),
            "lz4" => Ok(SpillCodec::Lz4),
            "zstd" => Ok(SpillCodec::Zstd),
            _ => Err(format!("unsupported spill ipc compression: {value}")),
        }
    }
}

impl TryFrom<u8> for SpillCodec {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SpillCodec::None),
            1 => Ok(SpillCodec::Lz4),
            2 => Ok(SpillCodec::Zstd),
            _ => Err(format!("unknown spill codec value: {value}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncodedMessage {
    pub bytes: Vec<u8>,
    pub num_rows: u32,
    pub num_cols: u16,
}

#[derive(Debug, Clone)]
pub struct IpcSerde {
    codec: SpillCodec,
    write_options: IpcWriteOptions,
}

impl IpcSerde {
    pub fn new(codec: SpillCodec) -> Result<Self, String> {
        let write_options = build_ipc_write_options(codec)?;
        Ok(Self {
            codec,
            write_options,
        })
    }

    pub fn codec(&self) -> SpillCodec {
        self.codec
    }

    pub fn encode_record_batch(&self, batch: &RecordBatch) -> Result<EncodedMessage, String> {
        if has_dictionary(batch.schema().as_ref()) {
            return Err(
                "dictionary-encoded columns are not supported in spill IPC yet".to_string(),
            );
        }

        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let mut compression_context = CompressionContext::default();
        let (encoded_dictionaries, encoded_message) = data_gen
            .encode(
                batch,
                &mut dictionary_tracker,
                &self.write_options,
                &mut compression_context,
            )
            .map_err(map_arrow_err)?;

        if !encoded_dictionaries.is_empty() {
            return Err("dictionary batch messages are not supported in spill IPC".to_string());
        }

        let bytes = write_encoded_message(encoded_message, &self.write_options)?;
        let num_rows = u32::try_from(batch.num_rows())
            .map_err(|_| "record batch row count overflows u32".to_string())?;
        let num_cols = u16::try_from(batch.num_columns())
            .map_err(|_| "record batch column count overflows u16".to_string())?;
        Ok(EncodedMessage {
            bytes,
            num_rows,
            num_cols,
        })
    }

    pub fn decode_record_batch(
        &self,
        schema: SchemaRef,
        message: &[u8],
    ) -> Result<RecordBatch, String> {
        let metadata_len = ipc_metadata_len(message, IPC_ALIGNMENT)?;
        if metadata_len > message.len() {
            return Err("ipc message metadata length exceeds buffer size".to_string());
        }
        let body_len = message.len() - metadata_len;
        let block = Block::new(0, metadata_len as i32, body_len as i64);
        let buffer = Buffer::from(message.to_vec());
        let decoder = FileDecoder::new(schema, MetadataVersion::V5);
        let batch = decoder
            .read_record_batch(&block, &buffer)
            .map_err(map_arrow_err)?
            .ok_or_else(|| "ipc message did not contain a record batch".to_string())?;
        Ok(batch)
    }
}

pub fn schema_hash(schema: &Schema) -> u64 {
    fn fnv1a(bytes: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET;
        for byte in bytes {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let schema_str = schema.to_string();
    fnv1a(schema_str.as_bytes())
}

fn build_ipc_write_options(codec: SpillCodec) -> Result<IpcWriteOptions, String> {
    let options = IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5)
        .map_err(map_arrow_err)?;
    match codec {
        SpillCodec::None => Ok(options),
        SpillCodec::Lz4 => options
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .map_err(map_arrow_err),
        SpillCodec::Zstd => options
            .try_with_compression(Some(CompressionType::ZSTD))
            .map_err(map_arrow_err),
    }
}

fn write_encoded_message(
    encoded: EncodedData,
    options: &IpcWriteOptions,
) -> Result<Vec<u8>, String> {
    let mut buffer = Vec::new();
    let (meta, body) = write_message(&mut buffer, encoded, options).map_err(map_arrow_err)?;
    let total = meta + body;
    if buffer.len() != total {
        return Err(format!(
            "ipc encoded message length mismatch: expected {total} bytes, got {}",
            buffer.len()
        ));
    }
    Ok(buffer)
}

fn ipc_metadata_len(message: &[u8], alignment: usize) -> Result<usize, String> {
    if message.len() < 4 {
        return Err("ipc message is too small to contain a header".to_string());
    }
    let (prefix_size, meta_len) = if message.len() >= 8 && message[..4] == CONTINUATION_MARKER {
        let len = i32::from_le_bytes(message[4..8].try_into().unwrap());
        if len < 0 {
            return Err("ipc message has negative metadata length".to_string());
        }
        (8usize, len as usize)
    } else {
        let len = i32::from_le_bytes(message[..4].try_into().unwrap());
        if len < 0 {
            return Err("ipc message has negative metadata length".to_string());
        }
        (4usize, len as usize)
    };

    let raw = prefix_size
        .checked_add(meta_len)
        .ok_or_else(|| "ipc metadata length overflow".to_string())?;
    Ok(align_up(raw, alignment))
}

fn align_up(value: usize, alignment: usize) -> usize {
    let mask = alignment - 1;
    (value + mask) & !mask
}

fn has_dictionary(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| data_type_has_dictionary(field.data_type()))
}

fn data_type_has_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            data_type_has_dictionary(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_has_dictionary(field.data_type())),
        DataType::Map(field, _) => data_type_has_dictionary(field.data_type()),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| data_type_has_dictionary(field.data_type())),
        DataType::RunEndEncoded(_, values) => data_type_has_dictionary(values.data_type()),
        _ => false,
    }
}

fn map_arrow_err(err: ArrowError) -> String {
    format!("arrow ipc error: {err}")
}

impl fmt::Display for SpillCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpillCodec::None => write!(f, "none"),
            SpillCodec::Lz4 => write!(f, "lz4"),
            SpillCodec::Zstd => write!(f, "zstd"),
        }
    }
}
