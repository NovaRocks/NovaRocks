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
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use crate::thrift::types::TUniqueId;

fn stream_load_file_paths() -> &'static Mutex<HashMap<(i64, i64), String>> {
    static FILE_PATHS: OnceLock<Mutex<HashMap<(i64, i64), String>>> = OnceLock::new();
    FILE_PATHS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_stream_load_file(load_id: &TUniqueId, path: &Path) {
    let mut guard = stream_load_file_paths()
        .lock()
        .expect("stream load file path lock");
    guard.insert(
        (load_id.hi, load_id.lo),
        path.as_os_str().to_string_lossy().to_string(),
    );
}

pub(crate) fn resolve_stream_load_file_path(load_id: &TUniqueId) -> Option<String> {
    let guard = stream_load_file_paths()
        .lock()
        .expect("stream load file path lock");
    guard.get(&(load_id.hi, load_id.lo)).cloned()
}

pub(crate) fn unregister_stream_load_file(load_id: &TUniqueId) {
    let mut guard = stream_load_file_paths()
        .lock()
        .expect("stream load file path lock");
    guard.remove(&(load_id.hi, load_id.lo));
}
