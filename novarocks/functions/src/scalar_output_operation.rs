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

//! Actual host-admitted scope around the sole original scalar builder.

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::ScalarWork;
use crate::arrow_result_custody::retain_result_backing;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_output_resources::ScalarOutputResources;
use crate::{AggregateStateAllocator, KernelFailure};
use arrow_array::ArrayRef;
use std::sync::Arc;

/// The preflight facts remain available for the original read-back stage.
/// They do not become an authorization for another operation.
pub(crate) struct RetainedScalarOutput {
    pub(crate) values: ArrayRef,
    pub(crate) resources: ScalarOutputResources,
    pub(crate) original_carrier_stock: usize,
    pub(crate) retained_envelope: usize,
}
pub(crate) fn with_scalar_output_operation(
    resources: ScalarOutputResources,
    host: Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
    compute: impl FnOnce(&mut ScalarWork<'_, '_>) -> Result<ArrayRef, KernelFailure>,
) -> Result<RetainedScalarOutput, KernelFailure> {
    let bytes = resources.operation_upper_bound()?;
    let charge = OpaqueRetainedCharge::try_new(host)?;
    work.flush()?;
    // Actual host refusal precedes the original copy/builder; all seven typed
    // causes return unchanged. Layout/stock facts alone never mint a grant.
    let mut reservation = charge.reserve_operation(bytes)?;
    work.flush()?;
    let values = compute(&mut ScalarWork::new(Some(work)))?;
    let values = retain_result_backing(values, charge, &mut reservation, allocator.clone(), work)?;
    Ok(RetainedScalarOutput {
        values: values.values,
        resources,
        original_carrier_stock: values.original_carrier_stock,
        retained_envelope: values.retained_envelope,
    })
}
