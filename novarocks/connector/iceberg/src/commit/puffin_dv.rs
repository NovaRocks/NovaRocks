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

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use anyhow::{Context, Result, anyhow, ensure};
use bytes::Bytes;
use roaring::RoaringBitmap;
use serde_json::json;

const MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
const PUFFIN_MAGIC: &[u8; 4] = b"PFA1";
const MAX_POSITIVE_I64_POSITION: u64 = 1u64 << 63;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeletionVector {
    bitmaps: BTreeMap<u32, RoaringBitmap>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrittenPuffinDv {
    pub path: String,
    pub referenced_data_file: String,
    pub cardinality: u64,
    pub content_offset: i64,
    pub content_size_in_bytes: i64,
    pub file_size_in_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct DeletionVectorBlobInput {
    pub referenced_data_file: String,
    pub deletion_vector: DeletionVector,
}

impl DeletionVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, position: u64) -> Result<()> {
        ensure_positive_64_bit_position(position)?;
        let key = high_key(position);
        let value = low_value(position);
        self.bitmaps.entry(key).or_default().insert(value);
        Ok(())
    }

    pub fn merge(&mut self, other: &DeletionVector) {
        for (key, bitmap) in &other.bitmaps {
            let target = self.bitmaps.entry(*key).or_default();
            *target |= bitmap.clone();
        }
    }

    pub fn contains(&self, position: u64) -> bool {
        if position >= MAX_POSITIVE_I64_POSITION {
            return false;
        }
        self.bitmaps
            .get(&high_key(position))
            .is_some_and(|bitmap| bitmap.contains(low_value(position)))
    }

    pub fn cardinality(&self) -> u64 {
        self.bitmaps.values().map(RoaringBitmap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.bitmaps.values().all(RoaringBitmap::is_empty)
    }

    /// Copy the compressed bitmaps into a [`RoaringTreemap`] for synchronous
    /// callers without enumerating the represented positions. Async readers
    /// use the cooperative decoder so each poll performs bounded work.
    pub fn to_roaring_treemap(&self) -> roaring::RoaringTreemap {
        roaring::RoaringTreemap::from_bitmaps(
            self.bitmaps
                .iter()
                .map(|(high_word, bitmap)| (*high_word, bitmap.clone())),
        )
    }

    pub fn to_iceberg_payload(&self) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.extend_from_slice(&(self.bitmaps.len() as u64).to_le_bytes());
        for (key, bitmap) in &self.bitmaps {
            body.extend_from_slice(&key.to_le_bytes());
            bitmap
                .serialize_into(&mut body)
                .context("failed to serialize deletion vector bitmap")?;
        }

        let body_len = u32::try_from(body.len()).context("deletion vector payload too large")?;
        let crc = crc32fast::hash(&body);

        let mut payload = Vec::with_capacity(4 + body.len() + 4);
        payload.extend_from_slice(&body_len.to_be_bytes());
        payload.extend_from_slice(&body);
        payload.extend_from_slice(&crc.to_be_bytes());
        Ok(payload)
    }

    pub fn from_iceberg_payload(payload: &[u8]) -> Result<Self> {
        ensure!(
            payload.len() >= 4 + MAGIC.len() + 8 + 4,
            "deletion vector payload is too short"
        );

        let declared_len = read_be_u32(&payload[..4])? as usize;
        ensure!(
            payload.len() == 4 + declared_len + 4,
            "deletion vector payload length mismatch: declared {}, actual {}",
            declared_len,
            payload.len().saturating_sub(8)
        );

        let body_start = 4;
        let body_end = body_start + declared_len;
        let body = &payload[body_start..body_end];
        ensure!(
            body.starts_with(&MAGIC),
            "invalid deletion vector magic bytes"
        );

        let expected_crc = read_be_u32(&payload[body_end..body_end + 4])?;
        let actual_crc = crc32fast::hash(body);
        ensure!(
            expected_crc == actual_crc,
            "deletion vector CRC mismatch: expected {expected_crc:#010x}, actual {actual_crc:#010x}"
        );

        let mut cursor = Cursor::new(&body[MAGIC.len()..]);
        let bitmap_count = read_le_u64_from(&mut cursor)?;
        let mut bitmaps = BTreeMap::new();
        for _ in 0..bitmap_count {
            let key = read_le_u32_from(&mut cursor)?;
            ensure!(
                !bitmaps.contains_key(&key),
                "deletion vector payload contains duplicate key {key}"
            );
            let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
                .context("failed to deserialize deletion vector bitmap")?;
            bitmaps.insert(key, bitmap);
        }

        ensure!(
            cursor.position() as usize == body.len() - MAGIC.len(),
            "deletion vector payload contains trailing bytes"
        );

        Ok(Self { bitmaps })
    }
}

/// Envelope and bitmap boundaries for the cooperative private read path.
/// Container values remain validated by the same roaring crate as sync reads.
pub(crate) struct DeletionVectorPayloadReader<'a> {
    body: &'a [u8],
    expected_crc: u32,
    cursor: usize,
    remaining: u64,
    keys: std::collections::HashSet<u32>,
}
impl<'a> DeletionVectorPayloadReader<'a> {
    pub(crate) fn new(payload: &'a [u8]) -> Result<Self> {
        ensure!(
            payload.len() >= 4 + MAGIC.len() + 8 + 4,
            "deletion vector payload is too short"
        );
        let declared = read_be_u32(&payload[..4])? as usize;
        ensure!(
            payload.len()
                == declared
                    .checked_add(8)
                    .ok_or_else(|| anyhow!("deletion vector length overflows"))?,
            "deletion vector payload length mismatch"
        );
        let body = &payload[4..4 + declared];
        ensure!(
            body.starts_with(&MAGIC),
            "invalid deletion vector magic bytes"
        );
        let expected_crc = read_be_u32(&payload[4 + declared..])?;
        let mut cursor = MAGIC.len();
        let remaining = u64::from_le_bytes(take_payload_bytes(body, &mut cursor, 8)?.try_into()?);
        Ok(Self {
            body,
            expected_crc,
            cursor,
            remaining,
            keys: Default::default(),
        })
    }
    pub(crate) fn crc_bytes(&self) -> &'a [u8] {
        self.body
    }
    pub(crate) fn validate_crc(&self, actual: u32) -> Result<()> {
        ensure!(
            actual == self.expected_crc,
            "deletion vector CRC mismatch: expected {:#010x}, actual {actual:#010x}",
            self.expected_crc
        );
        Ok(())
    }
    pub(crate) fn begin_bitmap(&mut self) -> Result<Option<(u32, RoaringBitmapPayload<'a>)>> {
        if self.remaining == 0 {
            ensure!(
                self.cursor == self.body.len(),
                "deletion vector payload contains trailing bytes"
            );
            return Ok(None);
        }
        let key =
            u32::from_le_bytes(take_payload_bytes(self.body, &mut self.cursor, 4)?.try_into()?);
        ensure!(
            key < (1u32 << 31),
            "deletion vector position exceeds positive 63-bit range"
        );
        ensure!(
            self.keys.insert(key),
            "deletion vector payload contains duplicate key {key}"
        );
        Ok(Some((
            key,
            RoaringBitmapPayload::new(&self.body[self.cursor..])?,
        )))
    }
    pub(crate) fn finish_bitmap(&mut self, bitmap: RoaringBitmapPayload<'a>) -> Result<()> {
        ensure!(
            bitmap.index == bitmap.count,
            "deletion vector bitmap was not fully decoded"
        );
        self.cursor = self
            .cursor
            .checked_add(bitmap.cursor)
            .ok_or_else(|| anyhow!("deletion vector cursor overflows"))?;
        self.remaining -= 1;
        Ok(())
    }
}
fn take_payload_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, count: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(count)
        .ok_or_else(|| anyhow!("deletion vector cursor overflows"))?;
    let out = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("deletion vector payload is truncated"))?;
    *cursor = end;
    Ok(out)
}

/// A borrowed standard portable Roaring bitmap header. At most one container
/// is reconstructed at a time; its interpretation is delegated to roaring.
pub(crate) struct RoaringBitmapPayload<'a> {
    body: &'a [u8],
    run_flags: Option<&'a [u8]>,
    descriptions: &'a [u8],
    offsets: Option<&'a [u8]>,
    cursor: usize,
    count: usize,
    index: usize,
    previous_key: Option<u16>,
}
impl<'a> RoaringBitmapPayload<'a> {
    fn new(body: &'a [u8]) -> Result<Self> {
        const NO_RUN_COOKIE: u32 = 12346;
        const RUN_COOKIE: u16 = 12347;
        let mut cursor = 0;
        let cookie = u32::from_le_bytes(take_payload_bytes(body, &mut cursor, 4)?.try_into()?);
        let (count, runs) = if cookie == NO_RUN_COOKIE {
            (
                u32::from_le_bytes(take_payload_bytes(body, &mut cursor, 4)?.try_into()?) as usize,
                false,
            )
        } else if cookie as u16 == RUN_COOKIE {
            (((cookie >> 16) + 1) as usize, true)
        } else {
            return Err(anyhow!("unknown deletion vector roaring cookie"));
        };
        ensure!(
            count <= 65536,
            "deletion vector roaring container count exceeds supported size"
        );
        let run_flags = if runs {
            Some(take_payload_bytes(body, &mut cursor, count.div_ceil(8))?)
        } else {
            None
        };
        let descriptions = take_payload_bytes(body, &mut cursor, count * 4)?;
        let offsets = if !runs || count >= 4 {
            Some(take_payload_bytes(body, &mut cursor, count * 4)?)
        } else {
            None
        };
        Ok(Self {
            body,
            run_flags,
            descriptions,
            offsets,
            cursor,
            count,
            index: 0,
            previous_key: None,
        })
    }
    pub(crate) fn next_container(&mut self) -> Result<Option<(RoaringBitmap, usize)>> {
        if self.index == self.count {
            return Ok(None);
        }
        let start_cursor = self.cursor;
        let desc = &self.descriptions[self.index * 4..self.index * 4 + 4];
        let key = u16::from_le_bytes(desc[..2].try_into()?);
        let cardinality = u64::from(u16::from_le_bytes(desc[2..].try_into()?)) + 1;
        ensure!(
            self.previous_key.is_none_or(|previous| previous < key),
            "deletion vector roaring container keys are not strictly increasing"
        );
        if let Some(offsets) = self.offsets {
            let offset = u32::from_le_bytes(offsets[self.index * 4..self.index * 4 + 4].try_into()?)
                as usize;
            ensure!(
                offset == self.cursor,
                "deletion vector roaring container offset differs from physical layout"
            );
        }
        let is_run = self
            .run_flags
            .is_some_and(|flags| flags[self.index / 8] & (1 << (self.index % 8)) != 0);
        let values = if is_run {
            let start = self.cursor;
            let runs =
                u16::from_le_bytes(take_payload_bytes(self.body, &mut self.cursor, 2)?.try_into()?)
                    as usize;
            let intervals = take_payload_bytes(self.body, &mut self.cursor, runs * 4)?;
            let mut previous_end = None;
            let mut actual = 0u64;
            for interval in intervals.chunks_exact(4) {
                let first = u16::from_le_bytes(interval[..2].try_into()?);
                let length = u16::from_le_bytes(interval[2..].try_into()?);
                let last = first
                    .checked_add(length)
                    .ok_or_else(|| anyhow!("deletion vector roaring run exceeds container"))?;
                ensure!(
                    previous_end.is_none_or(|previous| previous < first),
                    "deletion vector roaring runs overlap or are not sorted"
                );
                previous_end = Some(last);
                actual += u64::from(length) + 1;
            }
            ensure!(
                actual == cardinality,
                "deletion vector roaring run cardinality mismatch"
            );
            &self.body[start..self.cursor]
        } else {
            let bytes = if cardinality <= 4096 {
                cardinality as usize * 2
            } else {
                8192
            };
            take_payload_bytes(self.body, &mut self.cursor, bytes)?
        };
        // Preserve the original key and declared cardinality. Standard single
        // run containers omit offsets; standard non-run containers include one.
        let mut encoded = Vec::with_capacity(values.len() + 16);
        if is_run {
            encoded.extend_from_slice(&12347u32.to_le_bytes());
            encoded.push(1);
            encoded.extend_from_slice(desc);
        } else {
            encoded.extend_from_slice(&12346u32.to_le_bytes());
            encoded.extend_from_slice(&1u32.to_le_bytes());
            encoded.extend_from_slice(desc);
            encoded.extend_from_slice(&16u32.to_le_bytes());
        }
        encoded.extend_from_slice(values);
        let mut cursor = Cursor::new(encoded.as_slice());
        let decoded = RoaringBitmap::deserialize_from(&mut cursor)
            .context("failed to deserialize deletion vector container")?;
        ensure!(
            cursor.position() as usize == encoded.len(),
            "deletion vector container contains trailing bytes"
        );
        ensure!(
            decoded.len() == cardinality,
            "deletion vector roaring container cardinality mismatch"
        );
        self.index += 1;
        self.previous_key = Some(key);
        let physical_work_bytes = self.cursor - start_cursor
            + 4
            + if self.offsets.is_some() { 4 } else { 0 }
            + usize::from(self.run_flags.is_some());
        Ok(Some((decoded, physical_work_bytes)))
    }
}

fn ensure_positive_64_bit_position(position: u64) -> Result<()> {
    ensure!(
        position < MAX_POSITIVE_I64_POSITION,
        "deletion vector position must be a non-negative 63-bit value"
    );
    Ok(())
}

fn high_key(position: u64) -> u32 {
    (position >> 32) as u32
}

fn low_value(position: u64) -> u32 {
    position as u32
}

fn read_be_u32(bytes: &[u8]) -> Result<u32> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow!("expected 4 bytes for big-endian u32"))?;
    Ok(u32::from_be_bytes(array))
}

fn read_le_u32_from(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0u8; 4];
    cursor
        .read_exact(&mut bytes)
        .context("failed to read little-endian u32")?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_le_u64_from(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0u8; 8];
    cursor
        .read_exact(&mut bytes)
        .context("failed to read little-endian u64")?;
    Ok(u64::from_le_bytes(bytes))
}

/// Write a DV through the explicit operation or attempt artifact owner.
pub(crate) async fn write_single_deletion_vector_puffin_allocated(
    writer: &dyn super::model::ArtifactWriter,
    class: super::model::ArtifactClass,
    referenced_data_file: &str,
    dv: &DeletionVector,
) -> Result<WrittenPuffinDv> {
    validate_allocated_dv_class(class)?;
    writer.check_active()?;
    let object = writer.allocate(class, super::model::ArtifactKind::DeletionVector)?;
    let written = write_single_deletion_vector_puffin(
        writer.file_io(),
        object.path(),
        referenced_data_file,
        dv,
    )
    .await?;
    writer.check_active()?;
    Ok(written)
}

pub(crate) async fn write_multi_deletion_vector_puffin_allocated(
    writer: &dyn super::model::ArtifactWriter,
    class: super::model::ArtifactClass,
    inputs: &[DeletionVectorBlobInput],
) -> Result<Vec<WrittenPuffinDv>> {
    validate_allocated_dv_class(class)?;
    ensure!(
        !inputs.is_empty(),
        "Allocated DV Puffin requires at least one blob"
    );
    writer.check_active()?;
    let object = writer.allocate(class, super::model::ArtifactKind::DeletionVector)?;
    let written =
        write_multi_deletion_vector_puffin(writer.file_io(), object.path(), inputs).await?;
    writer.check_active()?;
    Ok(written)
}

fn validate_allocated_dv_class(class: super::model::ArtifactClass) -> Result<()> {
    ensure!(
        matches!(
            class,
            super::model::ArtifactClass::Operation | super::model::ArtifactClass::Attempt
        ),
        "DV Puffin requires operation or attempt artifact ownership"
    );
    Ok(())
}

pub async fn write_single_deletion_vector_puffin(
    file_io: &crate::iceberg::io::FileIO,
    path: &str,
    referenced_data_file: &str,
    dv: &DeletionVector,
) -> Result<WrittenPuffinDv> {
    let payload = dv.to_iceberg_payload()?;
    let content_offset = i64::try_from(PUFFIN_MAGIC.len()).context("puffin header is too large")?;
    let content_size_in_bytes =
        i64::try_from(payload.len()).context("deletion vector payload is too large")?;
    let cardinality = dv.cardinality();
    let footer = json!({
        "blobs": [{
            "type": "deletion-vector-v1",
            "fields": [],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": content_offset,
            "length": content_size_in_bytes,
            "properties": {
                "referenced-data-file": referenced_data_file,
                "cardinality": cardinality.to_string(),
            }
        }],
        "properties": {
            "created-by": "NovaRocks",
        }
    });
    let footer_json =
        serde_json::to_vec(&footer).context("failed to serialize Puffin footer metadata")?;
    let footer_json_len =
        u32::try_from(footer_json.len()).context("Puffin footer metadata is too large")?;

    let file_size_in_bytes = PUFFIN_MAGIC.len()
        + payload.len()
        + PUFFIN_MAGIC.len()
        + footer_json.len()
        + size_of::<u32>()
        + 4
        + PUFFIN_MAGIC.len();
    let mut file = Vec::with_capacity(file_size_in_bytes);
    file.extend_from_slice(PUFFIN_MAGIC);
    file.extend_from_slice(&payload);
    file.extend_from_slice(PUFFIN_MAGIC);
    file.extend_from_slice(&footer_json);
    file.extend_from_slice(&footer_json_len.to_le_bytes());
    file.extend_from_slice(&[0u8; 4]);
    file.extend_from_slice(PUFFIN_MAGIC);

    let output = file_io
        .new_output(path)
        .with_context(|| format!("failed to create Puffin output file: {path}"))?;
    output
        .write(Bytes::from(file))
        .await
        .with_context(|| format!("failed to write Puffin deletion vector file: {path}"))?;

    Ok(WrittenPuffinDv {
        path: path.to_string(),
        referenced_data_file: referenced_data_file.to_string(),
        cardinality,
        content_offset,
        content_size_in_bytes,
        file_size_in_bytes: file_size_in_bytes as u64,
    })
}

pub async fn write_multi_deletion_vector_puffin(
    file_io: &crate::iceberg::io::FileIO,
    path: &str,
    inputs: &[DeletionVectorBlobInput],
) -> Result<Vec<WrittenPuffinDv>> {
    ensure!(
        !inputs.is_empty(),
        "write_multi_deletion_vector_puffin requires at least one deletion vector"
    );

    let mut payloads = Vec::with_capacity(inputs.len());
    let mut next_content_offset =
        i64::try_from(PUFFIN_MAGIC.len()).context("puffin header is too large")?;
    for input in inputs {
        let payload = input.deletion_vector.to_iceberg_payload()?;
        let content_size_in_bytes =
            i64::try_from(payload.len()).context("deletion vector payload is too large")?;
        let content_offset = next_content_offset;
        next_content_offset = next_content_offset
            .checked_add(content_size_in_bytes)
            .context("Puffin deletion vector payload offsets overflow i64")?;
        payloads.push((payload, content_offset, content_size_in_bytes));
    }

    let blobs: Vec<_> = inputs
        .iter()
        .zip(&payloads)
        .map(|(input, (_, content_offset, content_size_in_bytes))| {
            json!({
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": *content_offset,
                "length": *content_size_in_bytes,
                "properties": {
                    "referenced-data-file": input.referenced_data_file,
                    "cardinality": input.deletion_vector.cardinality().to_string(),
                }
            })
        })
        .collect();
    let footer = json!({
        "blobs": blobs,
        "properties": {
            "created-by": "NovaRocks",
        }
    });
    let footer_json =
        serde_json::to_vec(&footer).context("failed to serialize Puffin footer metadata")?;
    let footer_json_len =
        u32::try_from(footer_json.len()).context("Puffin footer metadata is too large")?;

    let payload_size = payloads.iter().try_fold(0usize, |acc, (payload, _, _)| {
        acc.checked_add(payload.len())
            .context("Puffin deletion vector payloads are too large")
    })?;
    let file_size_in_bytes = PUFFIN_MAGIC
        .len()
        .checked_add(payload_size)
        .and_then(|size| size.checked_add(PUFFIN_MAGIC.len()))
        .and_then(|size| size.checked_add(footer_json.len()))
        .and_then(|size| size.checked_add(size_of::<u32>()))
        .and_then(|size| size.checked_add(4))
        .and_then(|size| size.checked_add(PUFFIN_MAGIC.len()))
        .context("Puffin deletion vector file is too large")?;
    let mut file = Vec::with_capacity(file_size_in_bytes);
    file.extend_from_slice(PUFFIN_MAGIC);
    for (payload, _, _) in &payloads {
        file.extend_from_slice(payload);
    }
    file.extend_from_slice(PUFFIN_MAGIC);
    file.extend_from_slice(&footer_json);
    file.extend_from_slice(&footer_json_len.to_le_bytes());
    file.extend_from_slice(&[0u8; 4]);
    file.extend_from_slice(PUFFIN_MAGIC);

    let output = file_io
        .new_output(path)
        .with_context(|| format!("failed to create Puffin output file: {path}"))?;
    output
        .write(Bytes::from(file))
        .await
        .with_context(|| format!("failed to write Puffin deletion vector file: {path}"))?;

    let file_size_in_bytes =
        u64::try_from(file_size_in_bytes).context("Puffin file size does not fit in u64")?;
    Ok(inputs
        .iter()
        .zip(payloads)
        .map(
            |(input, (_, content_offset, content_size_in_bytes))| WrittenPuffinDv {
                path: path.to_string(),
                referenced_data_file: input.referenced_data_file.clone(),
                cardinality: input.deletion_vector.cardinality(),
                content_offset,
                content_size_in_bytes,
                file_size_in_bytes,
            },
        )
        .collect())
}

pub async fn read_deletion_vector_puffin(
    file_io: &crate::iceberg::io::FileIO,
    path: &str,
    content_offset: i64,
    content_size_in_bytes: i64,
) -> Result<DeletionVector> {
    ensure!(
        content_offset >= 0,
        "Puffin deletion vector content offset must be non-negative"
    );
    ensure!(
        content_size_in_bytes >= 0,
        "Puffin deletion vector content size must be non-negative"
    );

    let start = u64::try_from(content_offset).context("invalid Puffin content offset")?;
    let size =
        u64::try_from(content_size_in_bytes).context("invalid Puffin content size in bytes")?;
    let end = start
        .checked_add(size)
        .context("Puffin deletion vector byte range overflows u64")?;
    let input = file_io
        .new_input(path)
        .with_context(|| format!("failed to create Puffin input file: {path}"))?;
    let reader = input
        .reader()
        .await
        .with_context(|| format!("failed to open Puffin input file reader: {path}"))?;
    let payload = reader
        .read(start..end)
        .await
        .with_context(|| format!("failed to read Puffin deletion vector byte range: {path}"))?;

    decode_deletion_vector_payload(payload.as_ref())
}

pub(crate) fn decode_deletion_vector_payload(payload: &[u8]) -> Result<DeletionVector> {
    DeletionVector::from_iceberg_payload(payload)
}

pub async fn read_deletion_vector_puffin_with_range_reader(
    factory: &novarocks_fs::FsAccessHandle,
    path: &str,
    content_offset: i64,
    content_size_in_bytes: i64,
) -> Result<DeletionVector> {
    ensure!(
        content_offset >= 0,
        "Puffin deletion vector content offset must be non-negative"
    );
    ensure!(
        content_size_in_bytes >= 0,
        "Puffin deletion vector content size must be non-negative"
    );

    let start = u64::try_from(content_offset).context("invalid Puffin content offset")?;
    let size =
        u64::try_from(content_size_in_bytes).context("invalid Puffin content size in bytes")?;
    let range = novarocks_fs::FileReadRange::bounded(start, size)
        .map_err(|error| anyhow!(error.to_string()))?;
    let file = factory
        .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
        .map_err(|error| anyhow!(error.to_string()))
        .with_context(|| format!("failed to bind Puffin input file: {path}"))?;
    let cancellation = novarocks_fs::FileCancellation::new();
    let payload = file
        .read(range, &cancellation)
        .await
        .map_err(|error| anyhow!(error.to_string()))
        .with_context(|| format!("failed to read Puffin deletion vector byte range: {path}"))?;

    decode_deletion_vector_payload(payload.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::iceberg::io::FileIO;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    fn local_file_io(location: &str) -> FileIO {
        let runtime = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        );
        crate::fs_io::build_file_io_for_location(location, binding)
    }

    fn bitmap_with(values: &[u32]) -> RoaringBitmap {
        let mut bitmap = RoaringBitmap::new();
        for value in values {
            bitmap.insert(*value);
        }
        bitmap
    }

    fn payload_from_body(body: &[u8]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        payload.extend_from_slice(body);
        payload.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
        payload
    }

    fn payload_from_entries(entries: &[(u32, RoaringBitmap)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (key, bitmap) in entries {
            body.extend_from_slice(&key.to_le_bytes());
            bitmap.serialize_into(&mut body).unwrap();
        }
        payload_from_body(&body)
    }

    fn assert_payload_error_contains(payload: &[u8], expected: &str) {
        let err = DeletionVector::from_iceberg_payload(payload)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(expected),
            "expected error containing {expected:?}, got {err:?}"
        );
    }

    fn factory_for_dir(dir: &std::path::Path) -> novarocks_fs::FsAccessHandle {
        let runtime = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        );
        binding
            .resolve_access(&format!("file://{}", dir.join("__binding__").display()))
            .expect("access")
    }

    async fn read_puffin_footer_metadata(
        file_io: &FileIO,
        path: &str,
    ) -> Result<(serde_json::Value, [u8; 4])> {
        let input = file_io
            .new_input(path)
            .with_context(|| format!("failed to create Puffin test input: {path}"))?;
        let metadata = input
            .metadata()
            .await
            .with_context(|| format!("failed to read Puffin test metadata: {path}"))?;
        let reader = input
            .reader()
            .await
            .with_context(|| format!("failed to open Puffin test reader: {path}"))?;
        let file = reader
            .read(0..metadata.size)
            .await
            .with_context(|| format!("failed to read Puffin test file: {path}"))?;
        let file = file.as_ref();
        let footer_json_len_offset = file
            .len()
            .checked_sub(PUFFIN_MAGIC.len() + size_of::<u32>() + 4)
            .context("Puffin test file is too short for footer trailer")?;
        ensure!(
            &file[file.len() - PUFFIN_MAGIC.len()..] == PUFFIN_MAGIC,
            "Puffin test file has invalid trailing magic"
        );
        let footer_json_len = read_le_u32_from(&mut Cursor::new(
            &file[footer_json_len_offset..footer_json_len_offset + size_of::<u32>()],
        ))? as usize;
        let flags: [u8; 4] = file[footer_json_len_offset + size_of::<u32>()
            ..footer_json_len_offset + size_of::<u32>() + 4]
            .try_into()
            .context("Puffin test file has invalid footer flags length")?;
        let footer_json_start = footer_json_len_offset
            .checked_sub(footer_json_len)
            .context("Puffin test footer length exceeds file size")?;
        let footer_magic_start = footer_json_start
            .checked_sub(PUFFIN_MAGIC.len())
            .context("Puffin test file is missing footer magic")?;
        ensure!(
            &file[footer_magic_start..footer_json_start] == PUFFIN_MAGIC,
            "Puffin test file has invalid footer magic"
        );

        let metadata = serde_json::from_slice(&file[footer_json_start..footer_json_len_offset])
            .context("failed to parse Puffin test footer metadata")?;
        Ok((metadata, flags))
    }

    #[tokio::test]
    async fn single_blob_puffin_round_trips_metadata_and_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = format!("{}/dv.puffin", dir.path().to_str().unwrap());
        let file_io = local_file_io(&path);
        let referenced_data_file = "file:///warehouse/t/data/data-1.parquet";
        let mut dv = DeletionVector::new();
        dv.insert(3).unwrap();
        dv.insert(u32::MAX as u64 + 5).unwrap();

        let written =
            write_single_deletion_vector_puffin(&file_io, &path, referenced_data_file, &dv)
                .await
                .unwrap();

        assert_eq!(written.path, path);
        assert_eq!(written.referenced_data_file, referenced_data_file);
        assert_eq!(written.cardinality, dv.cardinality());
        assert!(written.content_offset >= 4);
        assert!(written.content_size_in_bytes > 0);
        let metadata = file_io
            .new_input(&written.path)
            .unwrap()
            .metadata()
            .await
            .unwrap();
        assert_eq!(written.file_size_in_bytes, metadata.size);

        let decoded = read_deletion_vector_puffin(
            &file_io,
            &written.path,
            written.content_offset,
            written.content_size_in_bytes,
        )
        .await
        .unwrap();

        assert_eq!(decoded, dv);
    }

    #[tokio::test]
    async fn single_blob_puffin_round_trips_through_range_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = format!("{}/dv-range.puffin", dir.path().to_str().unwrap());
        let file_io = local_file_io(&path);
        let referenced_data_file = "file:///warehouse/t/data/data-1.parquet";
        let mut dv = DeletionVector::new();
        dv.insert(2).unwrap();
        dv.insert(u32::MAX as u64 + 7).unwrap();

        let written =
            write_single_deletion_vector_puffin(&file_io, &path, referenced_data_file, &dv)
                .await
                .unwrap();

        let decoded = read_deletion_vector_puffin_with_range_reader(
            &factory_for_dir(dir.path()),
            "dv-range.puffin",
            written.content_offset,
            written.content_size_in_bytes,
        )
        .await
        .unwrap();

        assert_eq!(decoded, dv);
    }

    #[tokio::test]
    async fn multi_blob_puffin_round_trips_two_dvs() {
        let dir = tempfile::tempdir().unwrap();
        let path = format!("{}/multi-dv.puffin", dir.path().to_str().unwrap());
        let file_io = local_file_io(&path);
        let mut first = DeletionVector::new();
        first.insert(1).unwrap();
        first.insert(9).unwrap();
        let mut second = DeletionVector::new();
        second.insert(3).unwrap();
        second.insert(11).unwrap();

        let written = write_multi_deletion_vector_puffin(
            &file_io,
            &path,
            &[
                DeletionVectorBlobInput {
                    referenced_data_file: "file:///warehouse/t/data/a.parquet".to_string(),
                    deletion_vector: first.clone(),
                },
                DeletionVectorBlobInput {
                    referenced_data_file: "file:///warehouse/t/data/b.parquet".to_string(),
                    deletion_vector: second.clone(),
                },
            ],
        )
        .await
        .unwrap();

        assert_eq!(written.len(), 2);
        assert_eq!(written[0].path, path);
        assert_eq!(written[1].path, path);
        assert_ne!(written[0].content_offset, written[1].content_offset);
        assert_eq!(
            read_deletion_vector_puffin(
                &file_io,
                &path,
                written[0].content_offset,
                written[0].content_size_in_bytes
            )
            .await
            .unwrap(),
            first
        );
        assert_eq!(
            read_deletion_vector_puffin(
                &file_io,
                &path,
                written[1].content_offset,
                written[1].content_size_in_bytes
            )
            .await
            .unwrap(),
            second
        );

        let metadata = file_io.new_input(&path).unwrap().metadata().await.unwrap();
        assert_eq!(written[0].file_size_in_bytes, metadata.size);
        assert_eq!(written[1].file_size_in_bytes, metadata.size);

        let (footer, flags) = read_puffin_footer_metadata(&file_io, &path).await.unwrap();
        assert_eq!(flags, [0, 0, 0, 0]);
        let blobs = footer["blobs"].as_array().unwrap();
        assert_eq!(blobs.len(), 2);
        for (blob, written_dv) in blobs.iter().zip(&written) {
            assert_eq!(blob["type"].as_str().unwrap(), "deletion-vector-v1");
            assert!(blob["fields"].as_array().unwrap().is_empty());
            assert_eq!(blob["snapshot-id"].as_i64().unwrap(), -1);
            assert_eq!(blob["sequence-number"].as_i64().unwrap(), -1);
            assert_eq!(blob["offset"].as_i64().unwrap(), written_dv.content_offset);
            assert_eq!(
                blob["length"].as_i64().unwrap(),
                written_dv.content_size_in_bytes
            );
            assert_eq!(
                blob["properties"]["referenced-data-file"].as_str().unwrap(),
                written_dv.referenced_data_file
            );
            assert_eq!(
                blob["properties"]["cardinality"].as_str().unwrap(),
                written_dv.cardinality.to_string()
            );
        }
    }

    #[tokio::test]
    async fn multi_blob_puffin_rejects_empty_input() {
        let dir = tempfile::tempdir().unwrap();
        let path = format!("{}/empty.puffin", dir.path().to_str().unwrap());
        let file_io = local_file_io(&path);
        let err = write_multi_deletion_vector_puffin(&file_io, &path, &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires at least one deletion vector"));
    }

    #[test]
    fn deletion_vector_round_trips_32_and_64_bit_positions() {
        let mut dv = DeletionVector::new();
        dv.insert(0).unwrap();
        dv.insert(7).unwrap();
        dv.insert(u32::MAX as u64 + 3).unwrap();

        let payload = dv.to_iceberg_payload().unwrap();
        let decoded = DeletionVector::from_iceberg_payload(&payload).unwrap();

        assert_eq!(decoded.cardinality(), 3);
        assert!(decoded.contains(0));
        assert!(decoded.contains(7));
        assert!(decoded.contains(u32::MAX as u64 + 3));
        assert!(!decoded.contains(6));
        assert!(!decoded.is_empty());
        assert_eq!(decoded, dv);
    }

    #[test]
    fn deletion_vector_rejects_high_bit_positions() {
        let mut dv = DeletionVector::new();
        let err = dv.insert(1u64 << 63).unwrap_err().to_string();
        assert!(err.contains("non-negative 63-bit"));
    }

    #[test]
    fn deletion_vector_rejects_duplicate_keys() {
        let payload = payload_from_entries(&[(7, bitmap_with(&[1, 2])), (7, bitmap_with(&[3, 4]))]);

        assert_payload_error_contains(&payload, "duplicate key");
    }

    #[test]
    fn deletion_vector_rejects_bad_length() {
        let mut dv = DeletionVector::new();
        dv.insert(7).unwrap();
        let mut payload = dv.to_iceberg_payload().unwrap();
        payload[..4].copy_from_slice(&1u32.to_be_bytes());

        assert_payload_error_contains(&payload, "length mismatch");
    }

    #[test]
    fn decode_deletion_vector_payload_rejects_invalid_payload() {
        let err = decode_deletion_vector_payload(b"not-a-dv")
            .unwrap_err()
            .to_string();
        assert!(err.contains("too short"));
    }

    #[test]
    fn deletion_vector_rejects_bad_magic() {
        let mut dv = DeletionVector::new();
        dv.insert(7).unwrap();
        let mut payload = dv.to_iceberg_payload().unwrap();
        payload[4] ^= 0xff;

        assert_payload_error_contains(&payload, "magic");
    }

    #[test]
    fn deletion_vector_rejects_crc_mismatch() {
        let mut dv = DeletionVector::new();
        dv.insert(7).unwrap();
        let mut payload = dv.to_iceberg_payload().unwrap();
        let last = payload.len() - 1;
        payload[last] ^= 0xff;

        assert_payload_error_contains(&payload, "CRC mismatch");
    }

    #[test]
    fn to_roaring_treemap_round_trips_positions() {
        let mut dv = DeletionVector::new();
        dv.insert(0).unwrap();
        dv.insert(7).unwrap();
        dv.insert(u32::MAX as u64 + 3).unwrap();
        let treemap = dv.to_roaring_treemap();
        assert_eq!(treemap.len(), 3);
        assert!(treemap.contains(0));
        assert!(treemap.contains(7));
        assert!(treemap.contains(u32::MAX as u64 + 3));
    }

    #[test]
    fn to_roaring_treemap_empty_for_empty_dv() {
        let dv = DeletionVector::new();
        assert!(dv.to_roaring_treemap().is_empty());
    }

    #[test]
    fn deletion_vector_rejects_trailing_bytes() {
        let bitmap = bitmap_with(&[1, 2, 3]);
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.extend_from_slice(&1u64.to_le_bytes());
        body.extend_from_slice(&9u32.to_le_bytes());
        bitmap.serialize_into(&mut body).unwrap();
        body.push(0);
        let payload = payload_from_body(&body);

        assert_payload_error_contains(&payload, "trailing bytes");
    }
}
