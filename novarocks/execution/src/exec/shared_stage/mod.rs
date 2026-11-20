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

//! Shared collection, completion, bitmap, and range ownership for local operators.

pub(crate) mod build_collection;
pub(crate) mod key_bitmap;
pub(crate) mod key_build_worker;
pub(crate) mod stage_completion;
pub(crate) mod tail_ranges;

use crate::{
    exec::{
        hash_table::key_index::BulkWork,
        pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    },
    runtime::task::{ExecutionFailure, ExecutionResult},
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{alloc::Layout, sync::Arc};

pub(crate) type ContractFailure = fn(&'static str) -> ExecutionFailure;

pub(crate) fn arithmetic() -> ExecutionFailure {
    ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}
pub(crate) fn funded_vec<T>(
    owner: &dyn InputConstructionOwner,
    count: usize,
) -> ExecutionResult<Vec<T>> {
    let layout = Layout::array::<T>(count).map_err(|_| arithmetic())?;
    if count != 0 {
        owner.request(layout)?;
    }
    let mut result = Vec::new();
    result.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Allocation {
            requested: layout.size(),
            source,
        })
    })?;
    Ok(result)
}
/// Value must retain its actual metadata creator until its last field exits.
pub(crate) fn funded_shared<T>(
    owner: &dyn InputConstructionOwner,
    value: T,
) -> ExecutionResult<Arc<T>> {
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(ConstructionFailure::from)?,
    )?;
    Ok(Arc::new(value))
}

pub(crate) fn push_owned<T>(
    values: &mut Vec<T>,
    value: T,
    storage: &mut ValueTypeStorageLifetime,
    work: &BulkWork<'_>,
) -> ExecutionResult<()> {
    reserve_push_owned(values, storage, work)?;
    values.push(value);
    Ok(())
}

fn reserve_push_owned<T>(
    values: &mut Vec<T>,
    storage: &mut ValueTypeStorageLifetime,
    work: &BulkWork<'_>,
) -> ExecutionResult<()> {
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
        work.owner.request(layout)?;
        values.try_reserve(1).map_err(|source| {
            ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Allocation {
                requested: layout.size(),
                source,
            })
        })?;
        *storage = work.owner.storage_lifetime();
    }
    Ok(())
}
