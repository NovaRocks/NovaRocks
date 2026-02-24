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
use bytes::Bytes;
use crc32c::crc32c;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crc32c::crc32c_append;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::runtime::io::io_executor;

const BLOCK_FILE_PREFIX: &str = "blockfile";
const BLOCKS_PER_SPACE: usize = 1024;
const DEFAULT_BLOCK_FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024; // 10GB
const DEFAULT_SLICE_SIZE: u64 = 64 * 1024;
const DEFAULT_IO_ALIGN_UNIT_SIZE: u64 = 4096;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct CacheKey([u8; 12]);

impl CacheKey {
    pub fn from_path(path: &str, modification_time: Option<i64>, file_size: u64) -> Self {
        let hash_value = hash64(path.as_bytes(), 0);
        let mut data = [0u8; 12];
        data[..8].copy_from_slice(&hash_value.to_le_bytes());

        let tail = if modification_time.unwrap_or(0) > 0 {
            let mtime = modification_time.unwrap_or(0);
            ((mtime >> 9) & 0x0000_0000_FFFF_FFFF) as u32
        } else {
            file_size as u32
        };
        data[8..12].copy_from_slice(&tail.to_le_bytes());
        Self(data)
    }

    fn as_bytes(&self) -> &[u8; 12] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct BlockKey {
    cache_id: u64,
    block_index: u64,
}

impl BlockKey {
    fn new(cache_key: CacheKey, block_index: u64) -> Self {
        Self {
            cache_id: cache_id(&cache_key),
            block_index,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockId {
    block_index: u64,
}

#[derive(Clone, Debug)]
pub struct BlockCacheOptions {
    pub capacity: u64,
    pub block_size: u64,
    pub enable_checksum: bool,
    pub direct_io: bool,
    pub io_align_unit_size: u64,
    pub slice_size: u64,
}

impl Default for BlockCacheOptions {
    fn default() -> Self {
        Self {
            capacity: 0,
            block_size: 0,
            enable_checksum: true,
            direct_io: false,
            io_align_unit_size: DEFAULT_IO_ALIGN_UNIT_SIZE,
            slice_size: DEFAULT_SLICE_SIZE,
        }
    }
}

#[derive(Debug)]
pub struct BlockCache {
    block_size: u64,
    slice_size: u64,
    enable_checksum: bool,
    inner: Mutex<BlockCacheInner>,
}

impl BlockCache {
    pub fn new(root: PathBuf, options: BlockCacheOptions) -> Result<Self, String> {
        if options.capacity == 0 {
            return Err("block cache capacity must be > 0".to_string());
        }
        if options.block_size == 0 {
            return Err("block cache block_size must be > 0".to_string());
        }
        if options.capacity < options.block_size {
            return Err("block cache capacity must be >= block_size".to_string());
        }
        if root.as_os_str().is_empty() {
            return Err("block cache root path is empty".to_string());
        }
        if options.io_align_unit_size == 0 {
            return Err("block cache io_align_unit_size must be > 0".to_string());
        }
        if options.enable_checksum && options.block_size % options.slice_size != 0 {
            return Err("block cache block_size must align to slice_size".to_string());
        }
        if options.direct_io && options.block_size % options.io_align_unit_size != 0 {
            return Err("block cache block_size must align to io_align_unit_size".to_string());
        }

        fs::create_dir_all(&root).map_err(|e| format!("failed to create block cache dir: {e}"))?;

        let block_file_size = DEFAULT_BLOCK_FILE_SIZE
            .min(options.capacity)
            .max(options.block_size);

        let disk = DiskSpaceManager::new(
            &root,
            options.capacity,
            options.block_size,
            block_file_size,
            options.direct_io,
            options.io_align_unit_size,
        )?;

        Ok(Self {
            block_size: options.block_size,
            slice_size: options.slice_size,
            enable_checksum: options.enable_checksum,
            inner: Mutex::new(BlockCacheInner::new(disk)),
        })
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.inner
            .lock()
            .expect("block cache lock")
            .disk
            .capacity_bytes()
    }

    pub fn used_bytes(&self) -> u64 {
        self.inner
            .lock()
            .expect("block cache lock")
            .disk
            .used_bytes()
    }

    pub fn read(&self, cache_key: CacheKey, block_index: u64, size: usize) -> Option<Bytes> {
        if size == 0 {
            return Some(Bytes::new());
        }
        let block_key = BlockKey::new(cache_key, block_index);
        let (block_id, data_len, checksums) = {
            let mut inner = self.inner.lock().expect("block cache lock");
            let (block_id, data_len, checksums) = {
                let entry = inner.entries.get(&block_key)?;
                if entry.cache_key != cache_key {
                    inner.remove_entry(&block_key);
                    return None;
                }
                (entry.block_id, entry.data_len, entry.checksums.clone())
            };
            inner.touch(&block_key);
            (block_id, data_len, checksums)
        };
        if size > data_len {
            let mut inner = self.inner.lock().expect("block cache lock");
            inner.remove_entry(&block_key);
            return None;
        }
        match self
            .inner
            .lock()
            .expect("block cache lock")
            .disk
            .read_block(block_id, 0, size)
        {
            Ok(data) => {
                if self.enable_checksum {
                    if let Some(expected) = checksums {
                        if !verify_checksums(&data, data_len, self.slice_size as usize, &expected) {
                            let mut inner = self.inner.lock().expect("block cache lock");
                            inner.remove_entry(&block_key);
                            return None;
                        }
                    }
                }
                Some(data)
            }
            Err(_) => {
                let mut inner = self.inner.lock().expect("block cache lock");
                inner.remove_entry(&block_key);
                None
            }
        }
    }

    pub fn write(
        self: &Arc<Self>,
        cache_key: CacheKey,
        block_index: u64,
        data: Bytes,
        async_populate: bool,
    ) -> Result<(), String> {
        if data.is_empty() {
            return Ok(());
        }
        let cache = Arc::clone(self);
        if async_populate {
            io_executor().submit(move |_| {
                let _ = cache.write_sync(cache_key, block_index, data);
            });
            Ok(())
        } else {
            self.write_sync(cache_key, block_index, data)
        }
    }

    fn write_sync(&self, cache_key: CacheKey, block_index: u64, data: Bytes) -> Result<(), String> {
        let block_key = BlockKey::new(cache_key, block_index);
        let (block_id, existed) = {
            let mut inner = self.inner.lock().expect("block cache lock");
            if let Some(entry) = inner.entries.get(&block_key) {
                if entry.cache_key == cache_key {
                    let block_id = entry.block_id;
                    inner.touch(&block_key);
                    (block_id, true)
                } else {
                    inner.remove_entry(&block_key);
                    let block_id = inner.allocate_block()?;
                    (block_id, false)
                }
            } else {
                let block_id = inner.allocate_block()?;
                (block_id, false)
            }
        };

        let checksums = if self.enable_checksum {
            Some(compute_checksums(
                &data,
                self.slice_size as usize,
                self.block_size as usize,
            ))
        } else {
            None
        };
        let data_len = data.len();

        let write_res = self
            .inner
            .lock()
            .expect("block cache lock")
            .disk
            .write_block(block_id, 0, &data);

        match write_res {
            Ok(()) => {
                let mut inner = self.inner.lock().expect("block cache lock");
                inner.insert_entry(block_key, cache_key, block_id, data_len, checksums);
                Ok(())
            }
            Err(err) => {
                if !existed {
                    let mut inner = self.inner.lock().expect("block cache lock");
                    inner.disk.free_block(block_id);
                }
                Err(err)
            }
        }
    }
}

#[derive(Debug)]
struct BlockCacheInner {
    entries: HashMap<BlockKey, BlockEntry>,
    head: Option<BlockKey>,
    tail: Option<BlockKey>,
    disk: DiskSpaceManager,
}

impl BlockCacheInner {
    fn new(disk: DiskSpaceManager) -> Self {
        Self {
            entries: HashMap::new(),
            head: None,
            tail: None,
            disk,
        }
    }

    fn allocate_block(&mut self) -> Result<BlockId, String> {
        if let Some(block_id) = self.disk.alloc_block() {
            return Ok(block_id);
        }

        loop {
            let key = self
                .head
                .clone()
                .ok_or_else(|| "block cache is full and cannot evict".to_string())?;
            self.remove_entry(&key);
            if let Some(block_id) = self.disk.alloc_block() {
                return Ok(block_id);
            }
        }
    }

    fn insert_entry(
        &mut self,
        key: BlockKey,
        cache_key: CacheKey,
        block_id: BlockId,
        data_len: usize,
        checksums: Option<Vec<u32>>,
    ) {
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.cache_key = cache_key;
            entry.block_id = block_id;
            entry.data_len = data_len;
            entry.checksums = checksums;
            self.touch(&key);
            return;
        }
        self.entries.insert(
            key,
            BlockEntry {
                cache_key,
                block_id,
                data_len,
                checksums,
                prev: None,
                next: None,
            },
        );
        self.attach_tail(&key);
    }

    fn touch(&mut self, key: &BlockKey) {
        if self.tail.as_ref() == Some(key) {
            return;
        }
        self.detach(key);
        self.attach_tail(key);
    }

    fn attach_tail(&mut self, key: &BlockKey) {
        let tail = self.tail.clone();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.prev = tail;
            entry.next = None;
        }
        if let Some(tail_key) = tail {
            if let Some(entry) = self.entries.get_mut(&tail_key) {
                entry.next = Some(*key);
            }
        } else {
            self.head = Some(*key);
        }
        self.tail = Some(*key);
    }

    fn detach(&mut self, key: &BlockKey) {
        let (prev, next) = match self.entries.get(key) {
            Some(entry) => (entry.prev, entry.next),
            None => return,
        };
        if let Some(prev_key) = prev {
            if let Some(entry) = self.entries.get_mut(&prev_key) {
                entry.next = next;
            }
        } else {
            self.head = next;
        }
        if let Some(next_key) = next {
            if let Some(entry) = self.entries.get_mut(&next_key) {
                entry.prev = prev;
            }
        } else {
            self.tail = prev;
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.prev = None;
            entry.next = None;
        }
    }

    fn remove_entry(&mut self, key: &BlockKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.detach(key);
            self.disk.free_block(entry.block_id);
        }
    }
}

#[derive(Debug)]
struct BlockEntry {
    cache_key: CacheKey,
    block_id: BlockId,
    data_len: usize,
    checksums: Option<Vec<u32>>,
    prev: Option<BlockKey>,
    next: Option<BlockKey>,
}

#[derive(Debug)]
struct DiskSpaceManager {
    dir: CacheDir,
}

impl DiskSpaceManager {
    fn new(
        root: &Path,
        capacity: u64,
        block_size: u64,
        block_file_size: u64,
        direct_io: bool,
        io_align_unit_size: u64,
    ) -> Result<Self, String> {
        let dir = CacheDir::new(
            root.to_path_buf(),
            capacity,
            block_size,
            block_file_size,
            direct_io,
            io_align_unit_size,
        )?;
        Ok(Self { dir })
    }

    fn alloc_block(&mut self) -> Option<BlockId> {
        self.dir
            .alloc_block()
            .map(|index| BlockId { block_index: index })
    }

    fn free_block(&mut self, block_id: BlockId) {
        self.dir.free_block(block_id.block_index);
    }

    fn write_block(
        &self,
        block_id: BlockId,
        offset_in_block: u64,
        data: &Bytes,
    ) -> Result<(), String> {
        self.dir
            .write_block(block_id.block_index, offset_in_block, data)
    }

    fn read_block(
        &self,
        block_id: BlockId,
        offset_in_block: u64,
        size: usize,
    ) -> Result<Bytes, String> {
        self.dir
            .read_block(block_id.block_index, offset_in_block, size)
    }

    fn capacity_bytes(&self) -> u64 {
        self.dir.capacity_bytes()
    }

    fn used_bytes(&self) -> u64 {
        self.dir.used_bytes()
    }
}

#[derive(Debug)]
struct CacheDir {
    path: PathBuf,
    block_size: u64,
    block_file_size: u64,
    total_block_count: u64,
    block_spaces: Vec<BlockSpace>,
    free_space_list: VecDeque<usize>,
    block_files: Vec<BlockFile>,
}

impl CacheDir {
    fn new(
        path: PathBuf,
        quota_bytes: u64,
        block_size: u64,
        block_file_size: u64,
        direct_io: bool,
        io_align_unit_size: u64,
    ) -> Result<Self, String> {
        let total_block_count = quota_bytes / block_size;
        if total_block_count == 0 {
            return Err("cache dir quota too small for block size".to_string());
        }
        fs::create_dir_all(&path).map_err(|e| format!("failed to create cache dir: {e}"))?;
        Self::clean_block_files(&path)?;

        let mut dir = Self {
            path,
            block_size,
            block_file_size,
            total_block_count,
            block_spaces: Vec::new(),
            free_space_list: VecDeque::new(),
            block_files: Vec::new(),
        };
        dir.init_free_space_list()?;
        dir.init_block_files(quota_bytes, direct_io, io_align_unit_size)?;
        Ok(dir)
    }

    fn clean_block_files(path: &Path) -> Result<(), String> {
        for entry in fs::read_dir(path).map_err(|e| format!("failed to read cache dir: {e}"))? {
            let entry = entry.map_err(|e| format!("failed to read cache dir entry: {e}"))?;
            if !entry
                .file_type()
                .map_err(|e| format!("failed to stat cache dir: {e}"))?
                .is_file()
            {
                continue;
            }
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if file_name.starts_with(BLOCK_FILE_PREFIX) {
                fs::remove_file(entry.path())
                    .map_err(|e| format!("failed to remove block file: {e}"))?;
            }
        }
        Ok(())
    }

    fn init_free_space_list(&mut self) -> Result<(), String> {
        let space_count = (self.total_block_count as usize) / BLOCKS_PER_SPACE;
        for i in 0..space_count {
            let start = (i * BLOCKS_PER_SPACE) as u64;
            let mut space = BlockSpace::new(start, BLOCKS_PER_SPACE);
            space.free_count = BLOCKS_PER_SPACE;
            self.block_spaces.push(space);
            self.free_space_list.push_back(i);
        }

        let cur_block_count = space_count * BLOCKS_PER_SPACE;
        let remain_blocks = self.total_block_count as usize - cur_block_count;
        if remain_blocks > 0 {
            let start = cur_block_count as u64;
            let mut space = BlockSpace::new(start, remain_blocks);
            space.free_count = remain_blocks;
            self.block_spaces.push(space);
            self.free_space_list.push_back(space_count);
        }
        Ok(())
    }

    fn init_block_files(
        &mut self,
        quota_bytes: u64,
        direct_io: bool,
        io_align_unit_size: u64,
    ) -> Result<(), String> {
        let mut free_bytes = quota_bytes;
        let mut file_index = 0u64;
        while free_bytes > 0 {
            let file_size = self.block_file_size.min(free_bytes);
            let file_path = self
                .path
                .join(format!("{}_{}", BLOCK_FILE_PREFIX, file_index));
            let file = BlockFile::open(file_path, file_size, false, direct_io, io_align_unit_size)?;
            self.block_files.push(file);
            free_bytes -= file_size;
            file_index += 1;
        }
        Ok(())
    }

    fn capacity_bytes(&self) -> u64 {
        self.total_block_count.saturating_mul(self.block_size)
    }

    fn used_bytes(&self) -> u64 {
        let free_blocks: u64 = self.block_spaces.iter().map(|s| s.free_count as u64).sum();
        self.total_block_count
            .saturating_sub(free_blocks)
            .saturating_mul(self.block_size)
    }

    fn alloc_block(&mut self) -> Option<u64> {
        while let Some(space_index) = self.free_space_list.front().cloned() {
            let space = &mut self.block_spaces[space_index];
            if let Some(inner_index) = space.find_first_free() {
                space.free_bits[inner_index] = false;
                space.free_count -= 1;
                if space.free_count == 0 {
                    self.free_space_list.pop_front();
                }
                return Some(space.start_block_index + inner_index as u64);
            } else {
                self.free_space_list.pop_front();
            }
        }
        None
    }

    fn free_block(&mut self, block_index: u64) {
        let space_index = (block_index as usize) / BLOCKS_PER_SPACE;
        if space_index >= self.block_spaces.len() {
            return;
        }
        let space = &mut self.block_spaces[space_index];
        let inner_index = (block_index - space.start_block_index) as usize;
        if inner_index >= space.free_bits.len() {
            return;
        }
        if !space.free_bits[inner_index] {
            space.free_bits[inner_index] = true;
            space.free_count += 1;
            if space.free_count == 1 {
                self.free_space_list.push_back(space_index);
            }
        }
    }

    fn write_block(
        &self,
        block_index: u64,
        offset_in_block: u64,
        data: &Bytes,
    ) -> Result<(), String> {
        let (file_index, offset) = self.block_file_offset(block_index, offset_in_block)?;
        let file = self
            .block_files
            .get(file_index)
            .ok_or_else(|| "block file index out of range".to_string())?;
        file.write(offset, data)
    }

    fn read_block(
        &self,
        block_index: u64,
        offset_in_block: u64,
        size: usize,
    ) -> Result<Bytes, String> {
        let (file_index, offset) = self.block_file_offset(block_index, offset_in_block)?;
        let file = self
            .block_files
            .get(file_index)
            .ok_or_else(|| "block file index out of range".to_string())?;
        file.read(offset, size)
    }

    fn block_file_offset(
        &self,
        block_index: u64,
        offset_in_block: u64,
    ) -> Result<(usize, u64), String> {
        let file_block_count = self.block_file_size / self.block_size;
        if file_block_count == 0 {
            return Err("block file size is too small".to_string());
        }
        let file_index = (block_index / file_block_count) as usize;
        let block_index_in_file = block_index % file_block_count;
        let offset = block_index_in_file * self.block_size + offset_in_block;
        Ok((file_index, offset))
    }
}

#[derive(Debug)]
struct BlockSpace {
    start_block_index: u64,
    free_bits: Vec<bool>,
    free_count: usize,
}

impl BlockSpace {
    fn new(start_block_index: u64, block_count: usize) -> Self {
        Self {
            start_block_index,
            free_bits: vec![true; block_count],
            free_count: 0,
        }
    }

    fn find_first_free(&self) -> Option<usize> {
        self.free_bits.iter().position(|v| *v)
    }
}

#[derive(Debug)]
struct BlockFile {
    file: File,
    direct_io: bool,
    io_align_unit_size: u64,
}

impl BlockFile {
    fn open(
        path: PathBuf,
        size: u64,
        pre_allocate: bool,
        direct_io: bool,
        io_align_unit_size: u64,
    ) -> Result<Self, String> {
        if direct_io && !direct_io_supported() {
            return Err("direct io is not supported on this platform".to_string());
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        if direct_io {
            #[cfg(target_os = "linux")]
            {
                options.custom_flags(libc::O_DIRECT);
            }
        }
        let file = options
            .open(&path)
            .map_err(|e| format!("failed to open block file: {e}"))?;
        if pre_allocate {
            file.set_len(size)
                .map_err(|e| format!("failed to truncate block file: {e}"))?;
        }
        Ok(Self {
            file,
            direct_io,
            io_align_unit_size,
        })
    }

    fn write(&self, offset: u64, data: &Bytes) -> Result<(), String> {
        if !self.direct_io {
            let mut written = 0usize;
            while written < data.len() {
                let n = self
                    .file
                    .write_at(&data[written..], offset + written as u64)
                    .map_err(|e| format!("failed to write block file: {e}"))?;
                if n == 0 {
                    return Err("short write on block file".to_string());
                }
                written += n;
            }
            return Ok(());
        }

        let align = self.io_align_unit_size as usize;
        let aligned_len = round_up(data.len(), align);
        let mut buffer = AlignedBuffer::new(aligned_len, align)?;
        buffer.as_mut_slice()[..data.len()].copy_from_slice(data);
        if aligned_len > data.len() {
            buffer.as_mut_slice()[data.len()..].fill(0);
        }

        let fd = self.file.as_raw_fd();
        let mut written = 0usize;
        while written < aligned_len {
            let res = unsafe {
                libc::pwrite(
                    fd,
                    buffer.as_slice()[written..].as_ptr() as *const libc::c_void,
                    (aligned_len - written) as libc::size_t,
                    (offset + written as u64) as libc::off_t,
                )
            };
            if res < 0 {
                return Err("failed to write block file with direct io".to_string());
            }
            if res == 0 {
                return Err("short write on block file".to_string());
            }
            written += res as usize;
        }
        Ok(())
    }

    fn read(&self, offset: u64, size: usize) -> Result<Bytes, String> {
        if !self.direct_io {
            let mut buf = vec![0u8; size];
            let mut read = 0usize;
            while read < size {
                let n = self
                    .file
                    .read_at(&mut buf[read..], offset + read as u64)
                    .map_err(|e| format!("failed to read block file: {e}"))?;
                if n == 0 {
                    break;
                }
                read += n;
            }
            if read < size {
                return Err("short read on block file".to_string());
            }
            return Ok(Bytes::from(buf));
        }

        let align = self.io_align_unit_size as usize;
        let aligned_len = round_up(size, align);
        let mut buffer = AlignedBuffer::new(aligned_len, align)?;
        let fd = self.file.as_raw_fd();
        let mut read = 0usize;
        while read < aligned_len {
            let res = unsafe {
                libc::pread(
                    fd,
                    buffer.as_mut_slice()[read..].as_mut_ptr() as *mut libc::c_void,
                    (aligned_len - read) as libc::size_t,
                    (offset + read as u64) as libc::off_t,
                )
            };
            if res < 0 {
                return Err("failed to read block file with direct io".to_string());
            }
            if res == 0 {
                break;
            }
            read += res as usize;
        }
        if read < aligned_len {
            return Err("short read on block file".to_string());
        }
        let mut out = vec![0u8; size];
        out.copy_from_slice(&buffer.as_slice()[..size]);
        Ok(Bytes::from(out))
    }
}

fn cache_id(cache_key: &CacheKey) -> u64 {
    murmur_hash3_x64_64(cache_key.as_bytes(), 0)
}

fn hash64(data: &[u8], seed: u64) -> u64 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("sse4.2") {
            return crc_hash64_sse42(data, seed);
        }
    }
    murmur_hash3_x64_64(data, seed)
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn crc_hash64_sse42(data: &[u8], seed: u64) -> u64 {
    let words = data.len() / 4;
    let bytes = data.len() % 4;
    let mut h1 = (seed >> 32) as u32;
    let mut h2 = seed as u32;

    for i in 0..words {
        let offset = i * 4;
        let word = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        let remaining = words - 1 - i;
        if remaining % 2 == 1 {
            h1 = crc32c_append(h1, &word.to_le_bytes());
        } else {
            h2 = crc32c_append(h2, &word.to_le_bytes());
        }
    }

    let tail = &data[words * 4..];
    for (i, byte) in tail.iter().enumerate() {
        let remaining = bytes - 1 - i;
        if remaining % 2 == 1 {
            h1 = crc32c_append(h1, &[*byte]);
        } else {
            h2 = crc32c_append(h2, &[*byte]);
        }
    }

    h1 = (h1 << 16) | (h1 >> 16);
    h2 = (h2 << 16) | (h2 >> 16);
    ((h2 as u64) << 32) | (h1 as u64)
}

fn murmur_hash3_x64_64(data: &[u8], seed: u64) -> u64 {
    let nblocks = data.len() / 8;
    let mut h1 = seed;

    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    for i in 0..nblocks {
        let offset = i * 8;
        let k1 = u64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);

        let mut k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);
    }

    let tail = &data[nblocks * 8..];
    let mut k1 = 0u64;
    match tail.len() & 7 {
        7 => k1 ^= (tail[6] as u64) << 48,
        6 => k1 ^= (tail[5] as u64) << 40,
        5 => k1 ^= (tail[4] as u64) << 32,
        4 => k1 ^= (tail[3] as u64) << 24,
        3 => k1 ^= (tail[2] as u64) << 16,
        2 => k1 ^= (tail[1] as u64) << 8,
        1 => k1 ^= tail[0] as u64,
        _ => {}
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u64;
    h1 = fmix64(h1);
    h1
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

fn compute_checksums(data: &Bytes, slice_size: usize, block_size: usize) -> Vec<u32> {
    let slice_count = block_size / slice_size;
    let mut out = Vec::with_capacity(slice_count);
    for i in 0..slice_count {
        let start = i * slice_size;
        if start >= data.len() {
            out.push(0);
            continue;
        }
        let end = std::cmp::min(start + slice_size, data.len());
        out.push(crc32c(&data[start..end]));
    }
    out
}

fn verify_checksums(data: &Bytes, data_len: usize, slice_size: usize, expected: &[u32]) -> bool {
    let slice_count = expected.len();
    for i in 0..slice_count {
        let start = i * slice_size;
        if start >= data_len {
            break;
        }
        let end = std::cmp::min(start + slice_size, data_len);
        let actual = crc32c(&data[start..end]);
        if actual != expected[i] {
            return false;
        }
    }
    true
}

fn round_up(value: usize, factor: usize) -> usize {
    if factor == 0 {
        return value;
    }
    (value + (factor - 1)) / factor * factor
}

fn direct_io_supported() -> bool {
    #[cfg(target_os = "linux")]
    {
        return true;
    }
    #[cfg(not(target_os = "linux"))]
    {
        return false;
    }
}

struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
}

impl AlignedBuffer {
    fn new(len: usize, align: usize) -> Result<Self, String> {
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        let ret = unsafe { libc::posix_memalign(&mut ptr, align, len) };
        if ret != 0 || ptr.is_null() {
            return Err("failed to allocate aligned buffer".to_string());
        }
        Ok(Self {
            ptr: ptr as *mut u8,
            len,
        })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut libc::c_void);
        }
    }
}

static BLOCK_CACHE: OnceLock<Arc<BlockCache>> = OnceLock::new();

pub fn init_block_cache(root: &str, options: BlockCacheOptions) -> Result<(), String> {
    let cache = BlockCache::new(PathBuf::from(root), options)?;
    BLOCK_CACHE
        .set(Arc::new(cache))
        .map_err(|_| "block cache already initialized".to_string())?;
    Ok(())
}

pub fn get_block_cache() -> Option<Arc<BlockCache>> {
    BLOCK_CACHE.get().cloned()
}
