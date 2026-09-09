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

use novarocks_fs::{FsAccessHandle, FsAccessResolver, ObjectStoreAccessContext};
use novarocks_spi::connector::StorageAccessDomainId;

pub fn resolve_tool_location(
    location: &str,
    access_domain: StorageAccessDomainId,
    object_store_access: Option<ObjectStoreAccessContext<'_>>,
) -> Result<FsAccessHandle, String> {
    let resolver = FsAccessResolver::new();
    resolver
        .resolve_location(access_domain, location, object_store_access)
        .map_err(|error| error.to_string())
}

pub fn single_relative_path(handle: &FsAccessHandle, location: &str) -> Result<String, String> {
    handle
        .paths()
        .first()
        .map(|path| path.operator_relative_path().to_string())
        .ok_or_else(|| format!("resolved empty path list for {location}"))
}

pub fn list_prefix(relative_path: &str) -> String {
    let mut prefix = relative_path.trim_start_matches('/').to_string();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    prefix
}
