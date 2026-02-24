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
use std::io::{Read, Seek, SeekFrom, Write};

use crate::exec::spill::ipc_serde::SpillCodec;

const BLOCK_MAGIC: [u8; 4] = *b"SPIL";
const BLOCK_VERSION: u16 = 1;
const BLOCK_HEADER_LEN: u16 = 40;
const MESSAGE_INDEX_ENTRY_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct BlockHeader {
    pub codec: SpillCodec,
    pub num_messages: u32,
    pub index_offset: u64,
    pub index_length: u64,
    pub schema_hash: u64,
}

impl BlockHeader {
    pub fn new(codec: SpillCodec, schema_hash: u64) -> Self {
        Self {
            codec,
            num_messages: 0,
            index_offset: 0,
            index_length: 0,
            schema_hash,
        }
    }

    pub fn to_bytes(&self) -> [u8; BLOCK_HEADER_LEN as usize] {
        let mut buf = [0u8; BLOCK_HEADER_LEN as usize];
        buf[..4].copy_from_slice(&BLOCK_MAGIC);
        buf[4..6].copy_from_slice(&BLOCK_VERSION.to_le_bytes());
        buf[6..8].copy_from_slice(&BLOCK_HEADER_LEN.to_le_bytes());
        buf[8] = self.codec.as_u8();
        buf[9] = 0;
        buf[10..12].copy_from_slice(&0u16.to_le_bytes());
        buf[12..16].copy_from_slice(&self.num_messages.to_le_bytes());
        buf[16..24].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.index_length.to_le_bytes());
        buf[32..40].copy_from_slice(&self.schema_hash.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < BLOCK_HEADER_LEN as usize {
            return Err("spill block header is too small".to_string());
        }
        if &buf[..4] != BLOCK_MAGIC {
            return Err("spill block header magic mismatch".to_string());
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version != BLOCK_VERSION {
            return Err(format!("unsupported spill block version: {version}"));
        }
        let header_len = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        if header_len != BLOCK_HEADER_LEN {
            return Err(format!(
                "unsupported spill block header length: {header_len}"
            ));
        }
        let codec = SpillCodec::try_from(buf[8])?;
        let reserved = u16::from_le_bytes(buf[10..12].try_into().unwrap());
        if reserved != 0 {
            return Err("spill block header reserved field must be 0".to_string());
        }
        let num_messages = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let index_offset = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let index_length = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let schema_hash = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        Ok(Self {
            codec,
            num_messages,
            index_offset,
            index_length,
            schema_hash,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MessageIndexEntry {
    pub offset: u64,
    pub length: u64,
    pub num_rows: u32,
    pub num_cols: u16,
    pub flags: u16,
}

impl MessageIndexEntry {
    pub fn to_bytes(&self) -> [u8; MESSAGE_INDEX_ENTRY_LEN] {
        let mut buf = [0u8; MESSAGE_INDEX_ENTRY_LEN];
        buf[..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.length.to_le_bytes());
        buf[16..20].copy_from_slice(&self.num_rows.to_le_bytes());
        buf[20..22].copy_from_slice(&self.num_cols.to_le_bytes());
        buf[22..24].copy_from_slice(&self.flags.to_le_bytes());
        buf[24..32].copy_from_slice(&0u64.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < MESSAGE_INDEX_ENTRY_LEN {
            return Err("spill message index entry is too small".to_string());
        }
        let offset = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let length = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let num_rows = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let num_cols = u16::from_le_bytes(buf[20..22].try_into().unwrap());
        let flags = u16::from_le_bytes(buf[22..24].try_into().unwrap());
        let reserved = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        if reserved != 0 {
            return Err("spill message index reserved field must be 0".to_string());
        }
        Ok(Self {
            offset,
            length,
            num_rows,
            num_cols,
            flags,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub header: BlockHeader,
    pub index: Vec<MessageIndexEntry>,
}

pub fn write_block_header<W: Write>(writer: &mut W, header: &BlockHeader) -> Result<(), String> {
    writer
        .write_all(&header.to_bytes())
        .map_err(|e| format!("write spill block header failed: {e}"))
}

pub fn read_block_header<R: Read>(reader: &mut R) -> Result<BlockHeader, String> {
    let mut buf = [0u8; BLOCK_HEADER_LEN as usize];
    reader
        .read_exact(&mut buf)
        .map_err(|e| format!("read spill block header failed: {e}"))?;
    BlockHeader::from_bytes(&buf)
}

pub fn write_block_index<W: Write>(
    writer: &mut W,
    entries: &[MessageIndexEntry],
) -> Result<(), String> {
    for entry in entries {
        writer
            .write_all(&entry.to_bytes())
            .map_err(|e| format!("write spill block index failed: {e}"))?;
    }
    Ok(())
}

pub fn read_block_index<R: Read + Seek>(
    reader: &mut R,
    header: &BlockHeader,
) -> Result<Vec<MessageIndexEntry>, String> {
    if header.index_length == 0 {
        return Ok(Vec::new());
    }
    if header.index_length % MESSAGE_INDEX_ENTRY_LEN as u64 != 0 {
        return Err("spill block index length is not aligned".to_string());
    }
    reader
        .seek(SeekFrom::Start(header.index_offset))
        .map_err(|e| format!("seek to spill block index failed: {e}"))?;
    let entry_count = (header.index_length / MESSAGE_INDEX_ENTRY_LEN as u64) as usize;
    let mut entries = Vec::with_capacity(entry_count);
    let mut buf = [0u8; MESSAGE_INDEX_ENTRY_LEN];
    for _ in 0..entry_count {
        reader
            .read_exact(&mut buf)
            .map_err(|e| format!("read spill block index entry failed: {e}"))?;
        entries.push(MessageIndexEntry::from_bytes(&buf)?);
    }
    Ok(entries)
}
