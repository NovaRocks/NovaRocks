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

use novarocks_spi::connector::ConnectorInstanceId;
use novarocks_state_store_api::Key;

use crate::state_family::{PersistentKeyPrefix, StateFamily};

/// Read from the closed state family manifest, which is the only place a
/// persistent prefix is defined.  The prefix already ends in `/`, so the
/// suffix below is the record path alone.
const ATTACHMENT_PREFIX: PersistentKeyPrefix =
    match StateFamily::CatalogDesiredState.persistent_prefix() {
        Some(prefix) => prefix,
        None => panic!("catalog desired state is a durable external projection"),
    };

pub fn attachment_prefix() -> Result<Key, String> {
    ATTACHMENT_PREFIX
        .key()
        .map_err(|error| format!("build catalog attachment prefix: {error}"))
}

pub fn attachment_key(instance_id: &ConnectorInstanceId) -> Result<Key, String> {
    ATTACHMENT_PREFIX
        .key_with_suffix(&hex::encode(instance_id.as_str()))
        .map_err(|error| format!("build catalog attachment key: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_uses_normalized_instance_identity() {
        let id = ConnectorInstanceId::parse("Warehouse.Main").expect("valid instance");
        let key = attachment_key(&id).expect("attachment key");
        assert_eq!(
            std::str::from_utf8(key.as_bytes()).expect("utf8 key"),
            "novarocks/frontend/catalog/v1/attachment/by-instance/77617265686f7573652e6d61696e"
        );
    }
}
