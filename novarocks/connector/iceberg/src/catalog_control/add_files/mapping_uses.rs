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

//! Path interpretations held by one registration through publication retries.

use std::collections::HashMap;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};
use novarocks_type_contract::owned_resources::hashmap::fresh_table_layout;

use crate::iceberg::spec::NameMapping;

type PathUse = (Option<i32>, ConnectorPayloadRetentionGuard);

#[derive(Default)]
pub(in crate::catalog_control) struct MappingUses {
    paths: HashMap<Box<[u8]>, PathUse>,
    table_retention: Option<ConnectorPayloadRetentionGuard>,
}

impl MappingUses {
    pub(super) fn record(
        &mut self,
        path: &[&str],
        field_id: Option<i32>,
        author: &ConnectorPayloadRetentionGuard,
    ) -> Result<(), ConnectorError> {
        let key_bytes = path
            .iter()
            .try_fold(0usize, |size, segment| {
                size.checked_add(size_of::<u32>())?
                    .checked_add(segment.len())
            })
            .ok_or_else(resource_overflow)?;
        let key_retention = author.reserve_owned_bytes(key_bytes)?;
        let mut key = Vec::with_capacity(key_bytes);
        for name in path {
            let len = u32::try_from(name.len()).map_err(|_| resource_overflow())?;
            key.extend_from_slice(&len.to_be_bytes());
            key.extend_from_slice(name.as_bytes());
        }
        if let Some((prior, _)) = self.paths.get(key.as_slice()) {
            if *prior != field_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "One Iceberg name mapping gave a path two interpretations",
                ));
            }
            return Ok(());
        }
        if self.paths.len() == self.paths.capacity() {
            let next = self
                .paths
                .len()
                .checked_mul(2)
                .ok_or_else(resource_overflow)?
                .max(4);
            let table = fresh_table_layout::<Box<[u8]>, PathUse>(next).map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::ResourceExhausted, error.to_string())
            })?;
            let retention = author.reserve_owned_bytes(table.request_bytes_upper_bound)?;
            let mut grown = HashMap::with_capacity(next);
            grown.extend(std::mem::take(&mut self.paths));
            self.paths = grown;
            self.table_retention = Some(retention);
        }
        self.paths
            .insert(key.into_boxed_slice(), (field_id, key_retention));
        Ok(())
    }

    pub(in crate::catalog_control) fn matches(
        &self,
        mapping: &NameMapping,
    ) -> Result<bool, ConnectorError> {
        for (key, (field_id, _)) in &self.paths {
            let observed = crate::schema_mapping::mapped_path_segments(mapping, segments(key))
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
            if observed != *field_id {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn segments(mut key: &[u8]) -> impl Iterator<Item = &str> {
    std::iter::from_fn(move || {
        if key.is_empty() {
            return None;
        }
        let (len, bytes) = key.split_at(size_of::<u32>());
        let n = u32::from_be_bytes(len.try_into().expect("owned mapping segment length"));
        let (name, rest) = bytes.split_at(n as usize);
        key = rest;
        Some(std::str::from_utf8(name).expect("owned mapping path is UTF-8"))
    })
}

fn resource_overflow() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg registration mapping path geometry overflow",
    )
}
