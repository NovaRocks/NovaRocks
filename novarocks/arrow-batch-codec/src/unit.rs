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

use std::{alloc::Layout, collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch},
    buffer::Buffer,
    datatypes::Schema,
    error::ArrowError,
    ipc::{
        MessageHeader, MetadataVersion,
        reader::{RecordBatchDecoder, read_dictionary},
        writer::{
            DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, write_message,
        },
    },
};
use novarocks_arrow_ipc_frame::{ContinuationPrefix, VerifierOptions};
use novarocks_type_contract::ValueTypeError;

use crate::{
    BatchRecipe, RecipeField,
    recipe::{UnitRecipe, reserve},
};

const MAGIC: &[u8; 4] = b"NRU1";
const HEADER_BYTES: usize = 36;

#[derive(Debug)]
pub enum CodecError<E> {
    Observer(E),
    Invalid(&'static str),
    ValueType(ValueTypeError),
    Arrow(ArrowError),
    Metadata(flatbuffers::InvalidFlatbuffer),
    Allocation(std::collections::TryReserveError),
    Layout(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Carrier(novarocks_type_contract::CarrierParameterError),
    Construction(novarocks_type_contract::ControlResourceError),
}
impl<E> From<ValueTypeError> for CodecError<E> {
    fn from(value: ValueTypeError) -> Self {
        Self::ValueType(value)
    }
}
impl<E> From<novarocks_type_contract::CarrierParameterError> for CodecError<E> {
    fn from(e: novarocks_type_contract::CarrierParameterError) -> Self {
        Self::Carrier(e)
    }
}
impl<E> From<novarocks_type_contract::ControlResourceError> for CodecError<E> {
    fn from(e: novarocks_type_contract::ControlResourceError) -> Self {
        Self::Construction(e)
    }
}
impl<E: std::fmt::Display> std::fmt::Display for CodecError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Observer(e) => e.fmt(f),
            Self::Invalid(e) => f.write_str(e),
            Self::ValueType(e) => write!(f, "invalid Arrow unit value type: {e:?}"),
            Self::Arrow(e) => e.fmt(f),
            Self::Metadata(e) => e.fmt(f),
            Self::Allocation(e) => e.fmt(f),
            Self::Layout(e) => e.fmt(f),
            Self::Carrier(e) => e.fmt(f),
            Self::Construction(e) => e.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for CodecError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Observer(error) => Some(error),
            Self::ValueType(error) => Some(error),
            Self::Arrow(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Allocation(error) => Some(error),
            Self::Layout(error) => Some(error),
            Self::Carrier(error) => Some(error),
            Self::Construction(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

/// Actual roles adapt their original resource/control owners here. The shared
/// geometry callback is compulsory and must check the entire fields forest,
/// exact FieldNode/Buffer/variadic consumption and described body extents before
/// Arrow's decoder. It is not a permissive fallback or a capacity author.
pub trait UnitObserver {
    type Error;
    fn checkpoint(&mut self) -> Result<(), Self::Error>;
    fn before_allocation(&mut self, layout: Layout) -> Result<(), Self::Error>;
    fn before_metadata_verification(&mut self, metadata: &[u8]) -> Result<(), Self::Error>;
    /// The original role creates the body and attaches retained capacity to its
    /// actual aliases. Implementations must use the pinned Arrow alignment and
    /// preserve the custody until the last returned-buffer alias really exits.
    fn copy_body(&mut self, body: &[u8]) -> Result<Buffer, Self::Error>;
    fn before_arrow_decode(
        &mut self,
        schema: &Schema,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &Buffer,
        dictionaries: &HashMap<i64, ArrayRef>,
        kind: DecodeKind,
    ) -> Result<(), Self::Error>;
    fn before_recipe(&mut self, fields: &[RecipeField<'_>]) -> Result<(), Self::Error>;
    fn before_unit_recipe(&mut self, recipe: &BatchRecipe, tags: &[u8]) -> Result<(), Self::Error>;
    fn before_encode_batch(
        &mut self,
        recipe: &BatchRecipe,
        batch: &RecordBatch,
    ) -> Result<(), Self::Error>;
    fn before_decode_message(
        &mut self,
        schema: &Schema,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &[u8],
    ) -> Result<(), Self::Error>;
}

#[derive(Clone, Copy, Debug)]
pub enum DecodeKind {
    Dictionary { id: i64 },
    RecordBatch,
}

/// Borrowed framing information. No array construction or byte copy occurs.
pub struct DecodeMessage<'a> {
    pub message: arrow::ipc::Message<'a>,
    pub body: &'a [u8],
    pub next: usize,
}

fn checked_add<E>(left: usize, right: usize) -> Result<usize, CodecError<E>> {
    left.checked_add(right)
        .ok_or(CodecError::Invalid("Arrow unit extent overflow"))
}
fn read_message<'a, O: UnitObserver>(
    bytes: &'a [u8],
    offset: usize,
    observer: &mut O,
) -> Result<DecodeMessage<'a>, CodecError<O::Error>> {
    let (start, len) = match novarocks_arrow_ipc_frame::continuation_prefix(bytes, offset)
        .map_err(|_| CodecError::Invalid("Arrow unit continuation framing is invalid"))?
    {
        ContinuationPrefix::Metadata { start, len } => (start, len),
        ContinuationPrefix::End { .. } => {
            return Err(CodecError::Invalid(
                "Arrow unit contains an unexpected End marker",
            ));
        }
    };
    let metadata = novarocks_arrow_ipc_frame::metadata_slice(bytes, start, len)
        .map_err(|_| CodecError::Invalid("Arrow unit metadata extent is invalid"))?;
    observer
        .before_metadata_verification(metadata)
        .map_err(CodecError::Observer)?;
    let message =
        novarocks_arrow_ipc_frame::verified_message(metadata, &VerifierOptions::default())
            .map_err(CodecError::Metadata)?;
    if message.version() != MetadataVersion::V5
        || message.custom_metadata().is_some_and(|m| !m.is_empty())
    {
        return Err(CodecError::Invalid(
            "Arrow unit metadata profile is invalid",
        ));
    }
    let body_len = novarocks_arrow_ipc_frame::nonnegative_length(message.bodyLength())
        .map_err(|_| CodecError::Invalid("Arrow unit body length is invalid"))?;
    let body_start = checked_add(start, len)?;
    let body_end = checked_add(body_start, body_len)?;
    let body = bytes
        .get(body_start..body_end)
        .ok_or(CodecError::Invalid("Arrow unit body is truncated"))?;
    Ok(DecodeMessage {
        message,
        body,
        next: body_end,
    })
}
pub fn encode_unit<O: UnitObserver>(
    recipe: &BatchRecipe,
    batch: &RecordBatch,
    observer: &mut O,
) -> Result<Vec<u8>, CodecError<O::Error>> {
    observer.checkpoint().map_err(CodecError::Observer)?;
    let tags = recipe.representation_tags(batch, observer)?;
    let unit = recipe.unit_recipe(&tags, observer)?;
    observer
        .before_encode_batch(recipe, batch)
        .map_err(CodecError::Observer)?;
    let mut columns = Vec::new();
    reserve(&mut columns, batch.num_columns(), observer)?;
    columns.extend(batch.columns().iter().cloned());
    let canonical = RecordBatch::try_new_with_options(
        Arc::clone(&unit.schema),
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(CodecError::Arrow)?;
    let mut tracker = DictionaryTracker::new(true);
    for dictionary in &unit.dictionaries {
        observer.checkpoint().map_err(CodecError::Observer)?;
        if tracker.next_dict_id() != dictionary.id {
            return Err(CodecError::Invalid(
                "Arrow unit dictionary sequence is invalid",
            ));
        }
    }
    // The geometry author covers the pinned generator/context's complete opaque
    // operation before this call. Cancellation next becomes visible on return.
    let options = IpcWriteOptions::default();
    let (dictionaries, record) = IpcDataGenerator::default()
        .encode(
            &canonical,
            &mut tracker,
            &options,
            &mut IpcWriteContext::default(),
        )
        .map_err(CodecError::Arrow)?;
    observer.checkpoint().map_err(CodecError::Observer)?;
    let mut output = Vec::new();
    reserve(
        &mut output,
        checked_add(HEADER_BYTES, tags.len())?,
        observer,
    )?;
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(recipe.digest());
    output.extend_from_slice(&tags);
    for message in dictionaries.into_iter().chain(std::iter::once(record)) {
        observer.checkpoint().map_err(CodecError::Observer)?;
        // Alignment is 64 in pinned default options. The added 128 covers both
        // framing/metadata and body padding; write_message never grows the Vec.
        let upper = checked_add(
            checked_add(message.ipc_message.len(), message.arrow_data.len())?,
            128,
        )?;
        reserve(&mut output, upper, observer)?;
        write_message(&mut output, message, &options).map_err(CodecError::Arrow)?;
    }
    observer.checkpoint().map_err(CodecError::Observer)?;
    Ok(output)
}

/// Validate every message before invoking any Arrow decoder. Each dictionary
/// must occur once, in the recipe's postorder, followed by exactly one batch.
fn preflight<O: UnitObserver>(
    recipe: &BatchRecipe,
    bytes: &[u8],
    observer: &mut O,
) -> Result<UnitRecipe, CodecError<O::Error>> {
    observer.checkpoint().map_err(CodecError::Observer)?;
    if bytes.get(..4) != Some(MAGIC.as_slice())
        || bytes.get(4..HEADER_BYTES) != Some(recipe.digest().as_slice())
    {
        return Err(CodecError::Invalid(
            "Arrow unit recipe digest or version differs",
        ));
    }
    let tags_end = checked_add(HEADER_BYTES, recipe.schema().fields().len())?;
    let tags = bytes
        .get(HEADER_BYTES..tags_end)
        .ok_or(CodecError::Invalid("unit representation tags are missing"))?;
    let unit = recipe.unit_recipe(tags, observer)?;
    let mut offset = tags_end;
    for expected in &unit.dictionaries {
        observer.checkpoint().map_err(CodecError::Observer)?;
        let parsed = read_message(bytes, offset, observer)?;
        if parsed.message.header_type() != MessageHeader::DictionaryBatch {
            return Err(CodecError::Invalid("Arrow unit dictionary is missing"));
        }
        let dictionary = parsed
            .message
            .header_as_dictionary_batch()
            .ok_or(CodecError::Invalid(
                "Arrow unit dictionary header is missing",
            ))?;
        if dictionary.id() != expected.id || dictionary.isDelta() {
            return Err(CodecError::Invalid(
                "Arrow unit dictionary ID, order or delta profile differs",
            ));
        }
        let batch = dictionary
            .data()
            .ok_or(CodecError::Invalid("Arrow unit dictionary data is missing"))?;
        if batch.compression().is_some() {
            return Err(CodecError::Invalid("Arrow unit compression is unsupported"));
        }
        observer
            .before_decode_message(&expected.schema, batch, parsed.body)
            .map_err(CodecError::Observer)?;
        offset = parsed.next;
    }
    observer.checkpoint().map_err(CodecError::Observer)?;
    let parsed = read_message(bytes, offset, observer)?;
    if parsed.message.header_type() != MessageHeader::RecordBatch {
        return Err(CodecError::Invalid("Arrow unit RecordBatch is missing"));
    }
    let batch = parsed
        .message
        .header_as_record_batch()
        .ok_or(CodecError::Invalid(
            "Arrow unit RecordBatch header is missing",
        ))?;
    if batch.compression().is_some() {
        return Err(CodecError::Invalid("Arrow unit compression is unsupported"));
    }
    observer
        .before_decode_message(&unit.schema, batch, parsed.body)
        .map_err(CodecError::Observer)?;
    if parsed.next != bytes.len() {
        return Err(CodecError::Invalid(
            "Arrow unit contains trailing messages or bytes",
        ));
    }
    Ok(unit)
}

pub fn decode_unit<O: UnitObserver>(
    recipe: &BatchRecipe,
    bytes: &[u8],
    observer: &mut O,
) -> Result<RecordBatch, CodecError<O::Error>> {
    let unit = preflight(recipe, bytes, observer)?;
    let mut dictionaries = HashMap::<i64, ArrayRef>::new();
    let mut offset = checked_add(HEADER_BYTES, recipe.schema().fields().len())?;
    for _ in &unit.dictionaries {
        observer.checkpoint().map_err(CodecError::Observer)?;
        let parsed = read_message(bytes, offset, observer)?;
        let buffer = observer
            .copy_body(parsed.body)
            .map_err(CodecError::Observer)?;
        let dictionary = parsed
            .message
            .header_as_dictionary_batch()
            .ok_or(CodecError::Invalid("Arrow unit dictionary header changed"))?;
        observer
            .before_arrow_decode(
                &unit.schema,
                dictionary
                    .data()
                    .ok_or(CodecError::Invalid("unit dictionary batch changed"))?,
                &buffer,
                &dictionaries,
                DecodeKind::Dictionary {
                    id: dictionary.id(),
                },
            )
            .map_err(CodecError::Observer)?;
        read_dictionary(
            &buffer,
            dictionary,
            &unit.schema,
            &mut dictionaries,
            &MetadataVersion::V5,
        )
        .map_err(CodecError::Arrow)?;
        observer.checkpoint().map_err(CodecError::Observer)?;
        offset = parsed.next;
    }
    let parsed = read_message(bytes, offset, observer)?;
    let buffer = observer
        .copy_body(parsed.body)
        .map_err(CodecError::Observer)?;
    let batch = parsed
        .message
        .header_as_record_batch()
        .ok_or(CodecError::Invalid("Arrow unit batch header changed"))?;
    observer
        .before_arrow_decode(
            &unit.schema,
            batch,
            &buffer,
            &dictionaries,
            DecodeKind::RecordBatch,
        )
        .map_err(CodecError::Observer)?;
    let output = RecordBatchDecoder::try_new(
        &buffer,
        batch,
        Arc::clone(&unit.schema),
        &dictionaries,
        &MetadataVersion::V5,
    )
    .map_err(CodecError::Arrow)?
    .with_require_alignment(false)
    .read_record_batch()
    .map_err(CodecError::Arrow)?;
    observer.checkpoint().map_err(CodecError::Observer)?;
    Ok(output)
}
