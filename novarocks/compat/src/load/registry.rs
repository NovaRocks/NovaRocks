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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::thrift::types::TUniqueId;
use novarocks_types::UniqueId;

#[derive(Debug, Default)]
pub(crate) struct CompatLoadRegistry {
    stream_load_file_paths: Mutex<HashMap<UniqueId, PathBuf>>,
}

impl CompatLoadRegistry {
    pub(crate) fn register_stream_load_file(&self, load_id: &TUniqueId, path: &Path) {
        self.stream_load_file_paths
            .lock()
            .expect("stream load file path lock")
            .insert(UniqueId::new(load_id.hi, load_id.lo), path.to_path_buf());
    }

    pub(crate) fn resolve_stream_load_file_path(&self, load_id: &TUniqueId) -> Option<String> {
        self.stream_load_file_paths
            .lock()
            .expect("stream load file path lock")
            .get(&UniqueId::new(load_id.hi, load_id.lo))
            .map(|path| path.to_string_lossy().to_string())
    }

    pub(crate) fn unregister_stream_load_file(&self, load_id: &TUniqueId) {
        self.stream_load_file_paths
            .lock()
            .expect("stream load file path lock")
            .remove(&UniqueId::new(load_id.hi, load_id.lo));
    }

    pub(crate) fn clear(&self) {
        let paths = self
            .stream_load_file_paths
            .lock()
            .expect("stream load file path lock")
            .drain()
            .map(|(_, path)| path)
            .collect::<Vec<_>>();
        for path in paths {
            let _ = std::fs::remove_file(path);
        }
    }
}
