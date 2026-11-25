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

use allocator_api2::vec::Vec as HostVec;

use crate::KernelFailure;
use crate::aggregate_host_allocator::HostAggregateAllocator;

pub(super) fn scratch<T>(
    capacity: usize,
    allocator: &HostAggregateAllocator,
) -> Result<HostVec<T, HostAggregateAllocator>, KernelFailure> {
    let mut values = HostVec::new_in(allocator.clone());
    values.try_reserve_exact(capacity).map_err(|_| {
        allocator
            .take_recorded_failure()
            .unwrap_or(KernelFailure::ResourceExhausted)
    })?;
    Ok(values)
}
