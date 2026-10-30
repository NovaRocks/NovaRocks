// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Instance-local typed key equality and storage. Join rows, aggregate state,
//! set markers and distributed hash identities remain with their own authors.

// Design: ADR-0205 (docs/adr/ADR-0205-shared-local-key-index-and-consumer-row-topology.md)

mod bulk;
mod bulk_read;
mod bulk_tasks;
mod canonical_store;
mod capture;
mod compact;
mod direct_int;
mod export;
mod fixed_store;
mod hash_build;
mod hash_layout;
#[cfg(test)]
mod hash_layout_public_tests;
mod local_hash;
mod owned_batch;
mod pairs;
mod prepared;
mod read;
mod route;
mod sizing;
mod statistics;
mod store;
mod strategy;
mod table;
#[cfg(test)]
mod tests;

pub(crate) use bulk::CompleteInput;
#[expect(
    unused_imports,
    reason = "Names the frozen K batch result contract even when current consumers infer its returned type."
)]
pub(crate) use bulk_read::BulkPresenceBatch;
#[expect(
    unused_imports,
    reason = "Names the frozen K batch result contract even when current consumers infer its returned type."
)]
pub(crate) use bulk_read::BulkProbeBatch;
pub(crate) use bulk_read::{BulkKey, BulkMatch, IndexView};
#[expect(
    unused_imports,
    reason = "Names the frozen K bulk handoff contract even when current consumers infer its returned type."
)]
pub(crate) use bulk_tasks::AssignmentChunk;
#[expect(
    unused_imports,
    reason = "Names the frozen K bulk handoff contract even when current consumers infer its returned type."
)]
pub(crate) use bulk_tasks::Assignments;
#[expect(
    unused_imports,
    reason = "Names the frozen K bulk handoff contract even when current consumers infer its returned type."
)]
pub(crate) use bulk_tasks::BulkReady;
#[expect(
    unused_imports,
    reason = "Names the frozen K bulk handoff contract even when current consumers infer its returned type."
)]
pub(crate) use bulk_tasks::Completion as BulkCompletion;
pub(crate) use bulk_tasks::{BulkAssignments, BulkTaskGraph, Claim as BulkClaim, Task as BulkTask};
#[expect(
    unused_imports,
    reason = "Names the frozen K bulk handoff contract even when current consumers infer its returned type."
)]
pub(crate) use capture::CapturedPageId;
pub(crate) use capture::{
    BulkDomain, BulkInput, CaptureBuilder, CaptureWork, EvaluatedPage, SealedCapture,
};
pub(crate) use direct_int::DirectWork as BulkWork;
pub(crate) use export::ExportedKeys;
#[cfg(test)]
pub(crate) use local_hash::BYTE_HASH_INITS;
pub(crate) use local_hash::{LocalHashBatch, LocalHashPolicy, hash_batch};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "Names the signed logical key strategy used by the retained K property contract."
    )
)]
pub(crate) use prepared::KeyStrategy;
pub(crate) use prepared::{IndexPurpose, KeyId, KeyInput, KeySpec, NullPolicy, PreparedKeyBatch};
#[expect(
    unused_imports,
    reason = "Frozen K typed read/export API; current join consumes lookup and assignments."
)]
pub(crate) use read::KeyReadPage;
pub(crate) use read::{KeyReadColumn, KeyReadRow, KeyReadValues, OwnedKeyReadBatch};
#[expect(
    unused_imports,
    reason = "Names the frozen K batch result contract even when current consumers infer its returned type."
)]
pub(crate) use table::InternBatch;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "Names the frozen K batch result contract even when current consumers infer its returned type."
    )
)]
pub(crate) use table::NewKey;
#[expect(
    unused_imports,
    reason = "Names the frozen K batch result contract even when current consumers infer its returned type."
)]
pub(crate) use table::PresenceBatch;
#[expect(
    unused_imports,
    reason = "Names the frozen K batch result contract even when current consumers infer its returned type."
)]
pub(crate) use table::ProbeBatch;
pub(crate) use table::{KeyIndex, KeyMatch};

use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_value_arrays::{ValueError, ValueFailure, ValueViolation};
use std::alloc::Layout;

fn invariant(violation: ValueViolation) -> ExecutionFailure {
    ExecutionFailure::from_domain(ValueError::Invariant(violation))
}
fn original<E: Into<ExecutionFailure>>(failure: ValueFailure<E>) -> ExecutionFailure {
    match failure {
        ValueFailure::Value(error) => ExecutionFailure::from_domain(error),
        ValueFailure::Caller(error) => error.into(),
    }
}
fn request<T>(owner: &dyn InputConstructionOwner, count: usize) -> ExecutionResult<()> {
    if count != 0 {
        owner.request(Layout::array::<T>(count).map_err(|_| {
            ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
    }
    Ok(())
}
fn vector<T>(owner: &dyn InputConstructionOwner, count: usize) -> ExecutionResult<Vec<T>> {
    request::<T>(owner, count)?;
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: count.saturating_mul(std::mem::size_of::<T>()),
            source,
        })
    })?;
    Ok(values)
}
fn reserve_push<T>(
    values: &mut Vec<T>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let growth =
        novarocks_type_contract::owned_resources::vec::push_growth(values).map_err(|cause| {
            match cause {
                novarocks_type_contract::ControlResourceError::Control(cause) => {
                    ConstructionFailure::Control(cause)
                }
                novarocks_type_contract::ControlResourceError::SourceModel(cause) => {
                    ConstructionFailure::InvalidSource(cause.to_owned())
                }
            }
        })?;
    if let Some(layout) = growth.requested_backing {
        owner.request(layout)?;
        values.try_reserve(1).map_err(|source| {
            ExecutionFailure::from_domain(ValueError::Allocation {
                requested: layout.size(),
                source,
            })
        })?;
    }
    checkpoint()
}
fn shared<T>(owner: &dyn InputConstructionOwner, value: T) -> ExecutionResult<std::sync::Arc<T>> {
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(ConstructionFailure::from)?,
    )?;
    Ok(std::sync::Arc::new(value))
}
fn checkpoint_at(row: usize, checkpoint: &dyn Fn() -> ExecutionResult<()>) -> ExecutionResult<()> {
    if row.is_multiple_of(256) {
        checkpoint()?;
    }
    Ok(())
}
