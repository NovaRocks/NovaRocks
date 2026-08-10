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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct DirManager {
    dirs: Vec<PathBuf>,
    next_dir: AtomicUsize,
    dir_max_bytes: u64,
}

impl DirManager {
    pub fn new(dirs: Vec<PathBuf>, dir_max_bytes: u64) -> Result<Self, String> {
        if dirs.is_empty() {
            return Err("spill.local_dirs is empty".to_string());
        }
        for dir in &dirs {
            ensure_dir(dir)?;
        }
        Ok(Self {
            dirs,
            next_dir: AtomicUsize::new(0),
            dir_max_bytes,
        })
    }

    pub fn next_dir(&self) -> PathBuf {
        let idx = self.next_dir.fetch_add(1, Ordering::AcqRel);
        let pos = idx % self.dirs.len();
        self.dirs[pos].clone()
    }

    pub fn dir_max_bytes(&self) -> u64 {
        self.dir_max_bytes
    }
}

fn ensure_dir(path: &Path) -> Result<(), String> {
    if path.as_os_str().is_empty() {
        return Err("spill.local_dirs contains empty path".to_string());
    }
    std::fs::create_dir_all(path)
        .map_err(|e| format!("create spill directory {} failed: {e}", path.display()))
}
