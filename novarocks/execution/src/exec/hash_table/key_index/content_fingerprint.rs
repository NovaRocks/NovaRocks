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

//! Deterministic evidence for one already-admitted content tuple.
//! This is never an equality oracle or a routing identity.

use super::{PreparedKeyBatch, invariant};
use crate::{exec::hash_table::allocation::AggregateVec, runtime::task::ExecutionResult};
use novarocks_value_arrays::ValueViolation;

// Frozen within the exact material island; not an instance-local hash seed.
const CONTENT_FINGERPRINT_SEED: u64 = 0x4e52_434f_4e54_454e;

impl PreparedKeyBatch<'_> {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "RWM-10 consumes deterministic content evidence.")
    )]
    pub(crate) fn content_fingerprint(
        &self,
        occurrence: usize,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<u128> {
        checkpoint()?;
        let row = self.input.row(occurrence)?;
        if !self.input.selected().contains(row) {
            return Err(invariant(ValueViolation::UndefinedRow));
        }
        let mut bytes = AggregateVec::new_in(self.allocator.clone());
        for column in 0..self.spec.types().len() {
            checkpoint()?;
            if let Some(decoded) = self.decoded_byte_component(column) {
                let value = decoded
                    .value(row)
                    .map_err(crate::runtime::task::ExecutionFailure::from_domain)?;
                let count = value.map_or(1, |value| 9usize.saturating_add(value.len()));
                bytes.try_reserve_exact(count).map_err(|source| {
                    bytes
                        .allocator()
                        .allocation_error("reserve content fingerprint bytes", source)
                })?;
                if let Some(value) = value {
                    bytes.push(1);
                    bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                    for part in value.chunks(64 * 1024) {
                        checkpoint()?;
                        bytes.extend_from_slice(part);
                    }
                } else {
                    bytes.push(0);
                }
            } else {
                let (array, mapping) = self.tuples().physical_component(column)?;
                let physical = super::pairs::physical_row(mapping, row)?;
                crate::exec::hash_table::key_builder::append_content_nullable_tracked_observed::<
                    false,
                    true,
                >(array, physical, &mut bytes, checkpoint)?;
            }
        }
        checkpoint()?;
        let mut hasher = xxhash_rust::xxh3::Xxh3::with_seed(CONTENT_FINGERPRINT_SEED);
        for part in bytes.chunks(64 * 1024) {
            checkpoint()?;
            hasher.update(part);
        }
        checkpoint()?;
        Ok(hasher.digest128())
    }
}
