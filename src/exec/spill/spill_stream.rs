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
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::exec::spill::block_manager::{
    BlockHeader, BlockMeta, MessageIndexEntry, read_block_header, read_block_index,
};
use crate::exec::spill::ipc_serde::{IpcSerde, schema_hash};

#[derive(Debug)]
pub struct SpillStream {
    file: File,
    schema: SchemaRef,
    header: BlockHeader,
    index: Vec<MessageIndexEntry>,
    position: usize,
    ipc: IpcSerde,
}

impl SpillStream {
    pub fn open(path: impl AsRef<Path>, schema: SchemaRef) -> Result<Self, String> {
        let mut file = File::open(path.as_ref())
            .map_err(|e| format!("open spill file {} failed: {e}", path.as_ref().display()))?;
        let header = read_block_header(&mut file)?;
        let index = read_block_index(&mut file, &header)?;
        validate_schema_hash(&header, schema.as_ref())?;
        let ipc = IpcSerde::new(header.codec)?;
        Ok(Self {
            file,
            schema,
            header,
            index,
            position: 0,
            ipc,
        })
    }

    pub fn meta(&self) -> BlockMeta {
        BlockMeta {
            header: self.header.clone(),
            index: self.index.clone(),
        }
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        if self.position >= self.index.len() {
            return Ok(None);
        }
        let entry = &self.index[self.position];
        self.position += 1;
        let mut buf = vec![0u8; entry.length as usize];
        self.file
            .seek(SeekFrom::Start(entry.offset))
            .map_err(|e| format!("seek spill message failed: {e}"))?;
        self.file
            .read_exact(&mut buf)
            .map_err(|e| format!("read spill message failed: {e}"))?;
        let batch = self.ipc.decode_record_batch(self.schema.clone(), &buf)?;
        Ok(Some(batch))
    }
}

fn validate_schema_hash(
    header: &BlockHeader,
    schema: &arrow::datatypes::Schema,
) -> Result<(), String> {
    let expected = schema_hash(schema);
    if header.schema_hash != expected {
        return Err("spill schema hash mismatch".to_string());
    }
    Ok(())
}
