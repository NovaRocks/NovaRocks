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
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;

use crate::common::config;
use crate::exec::chunk::Chunk;
use crate::exec::spill::block_manager::{
    BlockHeader, BlockMeta, MessageIndexEntry, write_block_header, write_block_index,
};
use crate::exec::spill::dir_manager::DirManager;
use crate::exec::spill::ipc_serde::{EncodedMessage, IpcSerde, SpillCodec, schema_hash};
use crate::exec::spill::spill_stream::SpillStream;

#[derive(Debug, Clone)]
pub struct SpillStorageConfig {
    pub local_dirs: Vec<PathBuf>,
    pub dir_max_bytes: u64,
    pub block_size_bytes: u64,
    pub ipc_compression: SpillCodec,
}

impl SpillStorageConfig {
    pub fn from_app_config() -> Result<Self, String> {
        if !config::spill_enable() {
            return Err("spill storage is disabled in config".to_string());
        }
        let local_dirs = config::spill_local_dirs()
            .into_iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let ipc_compression = SpillCodec::from_str(&config::spill_ipc_compression())?;
        Ok(Self {
            local_dirs,
            dir_max_bytes: config::spill_dir_max_bytes(),
            block_size_bytes: config::spill_block_size_bytes(),
            ipc_compression,
        })
    }
}

#[derive(Debug)]
struct BlockManager {
    dir_manager: Arc<DirManager>,
    block_size_bytes: u64,
    next_id: AtomicU64,
    pid: u32,
}

impl BlockManager {
    fn new(dir_manager: Arc<DirManager>, block_size_bytes: u64) -> Self {
        Self {
            dir_manager,
            block_size_bytes,
            next_id: AtomicU64::new(0),
            pid: std::process::id(),
        }
    }

    fn block_size_bytes(&self) -> u64 {
        self.block_size_bytes
    }

    fn create_block_file(&self) -> Result<(PathBuf, File), String> {
        let mut attempts = 0;
        loop {
            let dir = self.dir_manager.next_dir();
            let id = self.next_id.fetch_add(1, Ordering::AcqRel);
            let filename = format!("spill_{:x}_{:x}.ipc", self.pid, id);
            let path = dir.join(filename);
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path);
            match file {
                Ok(file) => return Ok((path, file)),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && attempts < 3 => {
                    attempts += 1;
                    continue;
                }
                Err(err) => {
                    return Err(format!(
                        "create spill file {} failed: {err}",
                        path.display()
                    ));
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpillFile {
    pub path: PathBuf,
    pub meta: BlockMeta,
}

#[derive(Debug, Default)]
pub struct Spiller {
    block_manager: Option<Arc<BlockManager>>,
    ipc: Option<IpcSerde>,
}

pub type SpillerHandle = Arc<Spiller>;

impl Spiller {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_storage(
        storage: SpillStorageConfig,
        codec: SpillCodec,
    ) -> Result<Self, String> {
        let dir_manager = Arc::new(DirManager::new(storage.local_dirs, storage.dir_max_bytes)?);
        let block_manager = Arc::new(BlockManager::new(dir_manager, storage.block_size_bytes));
        let ipc = IpcSerde::new(codec)?;
        Ok(Self {
            block_manager: Some(block_manager),
            ipc: Some(ipc),
        })
    }

    pub fn new_from_config(spill_encode_level: Option<i32>) -> Result<Self, String> {
        let storage = SpillStorageConfig::from_app_config()?;
        let codec = resolve_codec(storage.ipc_compression, spill_encode_level);
        Self::new_with_storage(storage, codec)
    }

    pub fn spill_chunks(&self, schema: SchemaRef, chunks: &[Chunk]) -> Result<SpillFile, String> {
        let (block_manager, ipc) = self.ensure_ready()?;
        let (path, mut file) = block_manager.create_block_file()?;

        let schema_hash_value = schema_hash(schema.as_ref());
        let mut header = BlockHeader::new(ipc.codec(), schema_hash_value);
        write_block_header(&mut file, &header)?;

        let mut index = Vec::new();
        let mut estimated_bytes = BLOCK_HEADER_LEN as u64;
        for chunk in chunks {
            let encoded = ipc.encode_record_batch(&chunk.batch)?;
            estimated_bytes = estimated_bytes
                .checked_add(encoded.bytes.len() as u64)
                .ok_or_else(|| "spill block size overflow".to_string())?;
            if estimated_bytes > block_manager.block_size_bytes() {
                return Err("spill block size exceeded configured limit".to_string());
            }
            let entry = append_message(&mut file, &encoded)?;
            index.push(entry);
        }

        let index_offset = file
            .seek(SeekFrom::Current(0))
            .map_err(|e| format!("seek spill index offset failed: {e}"))?;
        write_block_index(&mut file, &index)?;
        let index_length = (index.len() * MESSAGE_INDEX_ENTRY_LEN) as u64;
        estimated_bytes = estimated_bytes
            .checked_add(index_length)
            .ok_or_else(|| "spill block size overflow".to_string())?;
        if estimated_bytes > block_manager.block_size_bytes() {
            return Err("spill block size exceeded configured limit".to_string());
        }

        header.num_messages = index.len() as u32;
        header.index_offset = index_offset;
        header.index_length = index_length;

        file.seek(SeekFrom::Start(0))
            .map_err(|e| format!("seek spill header failed: {e}"))?;
        write_block_header(&mut file, &header)?;
        file.flush()
            .map_err(|e| format!("flush spill file failed: {e}"))?;

        Ok(SpillFile {
            path,
            meta: BlockMeta { header, index },
        })
    }

    pub fn restore_chunks(
        &self,
        schema: SchemaRef,
        file: &SpillFile,
    ) -> Result<Vec<Chunk>, String> {
        let mut stream = SpillStream::open(&file.path, schema)?;
        let mut out = Vec::new();
        while let Some(batch) = stream.next_batch()? {
            out.push(Chunk::try_new(batch)?);
        }
        Ok(out)
    }

    pub fn open_stream(&self, schema: SchemaRef, file: &SpillFile) -> Result<SpillStream, String> {
        SpillStream::open(&file.path, schema)
    }

    fn ensure_ready(&self) -> Result<(Arc<BlockManager>, &IpcSerde), String> {
        let block_manager = self
            .block_manager
            .as_ref()
            .ok_or_else(|| "spill block manager is not initialized".to_string())?;
        let ipc = self
            .ipc
            .as_ref()
            .ok_or_else(|| "spill ipc encoder is not initialized".to_string())?;
        Ok((Arc::clone(block_manager), ipc))
    }
}

const BLOCK_HEADER_LEN: usize = 40;
const MESSAGE_INDEX_ENTRY_LEN: usize = 32;

fn append_message(file: &mut File, encoded: &EncodedMessage) -> Result<MessageIndexEntry, String> {
    let offset = file
        .seek(SeekFrom::Current(0))
        .map_err(|e| format!("seek spill message offset failed: {e}"))?;
    file.write_all(&encoded.bytes)
        .map_err(|e| format!("write spill message failed: {e}"))?;
    Ok(MessageIndexEntry {
        offset,
        length: encoded.bytes.len() as u64,
        num_rows: encoded.num_rows,
        num_cols: encoded.num_cols,
        flags: 0,
    })
}

fn resolve_codec(default_codec: SpillCodec, spill_encode_level: Option<i32>) -> SpillCodec {
    match spill_encode_level {
        Some(level) if level <= 0 => SpillCodec::None,
        Some(_) => default_codec,
        None => default_codec,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tempfile::tempdir;

    #[test]
    fn spill_roundtrip_ipc_block() {
        let schema = SchemaRef::new(Schema::new(vec![
            field_with_slot_id(
                Field::new("a", arrow::datatypes::DataType::Int32, false),
                SlotId::new(1),
            ),
            field_with_slot_id(
                Field::new("b", arrow::datatypes::DataType::Utf8, true),
                SlotId::new(2),
            ),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec![Some("y"), Some("w")])),
            ],
        )
        .unwrap();
        let chunks = vec![
            Chunk::try_new(batch1).unwrap(),
            Chunk::try_new(batch2).unwrap(),
        ];

        let temp = tempdir().unwrap();
        let storage = SpillStorageConfig {
            local_dirs: vec![temp.path().to_path_buf()],
            dir_max_bytes: 0,
            block_size_bytes: 128 * 1024 * 1024,
            ipc_compression: SpillCodec::None,
        };
        let spiller = Spiller::new_with_storage(storage, SpillCodec::None).unwrap();

        let spill_file = spiller.spill_chunks(schema.clone(), &chunks).unwrap();
        let restored = spiller.restore_chunks(schema, &spill_file).unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0].len(), 3);
        assert_eq!(restored[1].len(), 2);
    }
}
