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

//! Immutable metadata snapshots. Data size is a resource fact, never a structural quota.
use arrow_array::RecordBatch;
use arrow_schema::FieldRef;
use novarocks_type_contract::{CompileControlError, ValueTypeStorageLifetime};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum SystemRelationData {
    /// Display-only relation; execution encoders must refuse it.
    Deferred,
    Batches {
        batches: Arc<[RecordBatch]>,
        storage: ValueTypeStorageLifetime,
    },
}
impl PartialEq for SystemRelationData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Deferred, Self::Deferred) => true,
            (Self::Batches { batches: a, .. }, Self::Batches { batches: b, .. }) => a == b,
            _ => false,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SystemRelationResourceFacts {
    pub batches: usize,
    pub rows: usize,
    pub retained_bytes: usize,
}
impl SystemRelationData {
    pub fn resource_facts(&self) -> Result<SystemRelationResourceFacts, CompileControlError> {
        let mut facts = SystemRelationResourceFacts::default();
        if let Self::Batches { batches, .. } = self {
            facts.batches = batches.len();
            for batch in batches.iter() {
                facts.rows = facts
                    .rows
                    .checked_add(batch.num_rows())
                    .ok_or(CompileControlError::ResourceExhausted)?;
                facts.retained_bytes = facts
                    .retained_bytes
                    .checked_add(batch.get_array_memory_size())
                    .ok_or(CompileControlError::ResourceExhausted)?;
            }
        }
        Ok(facts)
    }
    /// Schema identity includes field names and metadata; semantic authority remains with output values.
    pub fn validate_schema(&self, fields: &[FieldRef]) -> Result<(), &'static str> {
        if let Self::Batches { batches, .. } = self {
            for batch in batches.iter() {
                if batch.num_rows() == 0 {
                    return Err("system relation empty snapshot must contain zero batches");
                }
                if batch.schema().fields().as_ref() != fields
                    || !batch.schema().metadata().is_empty()
                {
                    return Err(
                        "system relation batch schema differs from its frozen output fields",
                    );
                }
            }
        }
        Ok(())
    }
}
