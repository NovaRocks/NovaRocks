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

use crate::{
    InputSourceStorage, InputStorageFailure, ScalarCallFailure, ScalarCallHost, ScalarCallPhase,
    ScalarCallRequestFacts,
};
use novarocks_functions::{
    AggregateStateAllocator, ConstantValidation, KernelEvaluationControl, KernelFailure,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::time::Duration;

pub mod checked_constants;
mod counting_allocator;
pub use counting_allocator::CountingAllocator;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{alloc::Layout, convert::Infallible, sync::Arc};

pub struct PassThroughCallHost;

impl ScalarCallHost for PassThroughCallHost {
    type Refusal = Infallible;

    fn begin_call(&mut self) {}

    fn run_admitted(
        &mut self,
        _phase: ScalarCallPhase,
        _facts: ScalarCallRequestFacts,
        body: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
    ) -> Result<(), ScalarCallFailure<Self::Refusal>> {
        body()
    }

    fn callback_request_facts(
        &mut self,
        _allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>> {
        Ok(ScalarCallRequestFacts::default())
    }
}

pub struct TestInputSourceStorage {
    storage: ValueTypeStorageLifetime,
}

impl TestInputSourceStorage {
    pub fn new(storage: ValueTypeStorageLifetime) -> Self {
        Self { storage }
    }
}

impl InputSourceStorage for TestInputSourceStorage {
    fn storage_lifetime_observed(
        &self,
        base: ValueTypeStorageLifetime,
        request: &mut dyn FnMut(Layout) -> Result<(), InputStorageFailure>,
    ) -> Result<ValueTypeStorageLifetime, InputStorageFailure> {
        base.retaining_observed(&self.storage, request)
    }
}

/// Unbounded compile and evaluation control. Waiting returns immediately so a
/// sleeping kernel cannot stall a differential run.
#[derive(Clone, Copy, Debug, Default)]
pub struct HarnessControl;

impl PureCompileControl for HarnessControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

impl KernelEvaluationControl for HarnessControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}

/// Generous admission limits for harness constants.
pub fn constant_policy() -> ConstantValidation {
    constant_validation(1_000_000)
}

/// Keep the harness type limits with a deliberately narrow element budget.
pub fn constant_validation(max_logical_elements: u64) -> ConstantValidation {
    novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows: 1024,
            max_array_nodes: 4096,
            max_logical_elements,
            max_retained_buffer_bytes: 4_194_304,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 16,
            max_metadata_bytes: 1_048_576,
            max_library_validation_work: 16_777_216,
            max_library_validation_bytes: 16_777_216,
        },
    )
}

/// Keep each fixture reference with its real immutable backing. Pool IDs are
/// local to this explicit table; selected-value equality is never inferred.
pub fn register_constant(
    pools: &mut novarocks_constant_contract::ConstantPools,
    value: &novarocks_functions::ConstantValue,
) -> novarocks_constant_contract::ConstantReference {
    use novarocks_constant_contract::ConstantPoolId;
    let identity = value.pool().backing_identity();
    let existing = pools
        .entries()
        .iter()
        .find_map(|(id, pool)| (pool.backing_identity() == identity).then_some(*id));
    let pool = existing.unwrap_or_else(|| {
        let id = (0..=u32::MAX)
            .map(ConstantPoolId::new)
            .find(|id| !pools.entries().contains_key(id))
            .expect("fixture has an available pool identity");
        pools.insert(id, value.pool().clone()).unwrap();
        id
    });
    novarocks_constant_contract::ConstantReference {
        pool,
        ordinal: value.ordinal(),
    }
}

mod claim_author;
pub use claim_author::{
    ClaimAuthor, add_call, declare_call_controls, finish_unit, register_call_requests,
    try_finish_unit,
};

/// Rebuild a test's complete input scope from its neutral declared port.
pub fn fixture_scope(
    port: &novarocks_expression_contract::ExprPort,
    schema: arrow::datatypes::SchemaRef,
) -> crate::ScopeLayout {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        &HarnessControl,
        CompilePhase::LowerProgram,
    )
    .unwrap();
    let layout = crate::ScopeLayout::try_new(
        schema,
        port.values.iter().map(|(_, ty)| ty.clone()).collect(),
        port.values
            .iter()
            .enumerate()
            .map(|(ordinal, (value, ty))| crate::ScopeColumn {
                value: *value,
                ordinal: u32::try_from(ordinal).unwrap(),
                ty: ty.clone(),
            })
            .collect(),
        ValueTypeStorageLifetime::pure(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    layout
}

/// Compile the fixture directly through the production pure unit compiler.
pub fn compile_fixture_unit(
    unit: &novarocks_expression_contract::BoundExprSet,
    environment: crate::EvaluationEnvironment<'_>,
    layouts: crate::ScopeLayouts<'_>,
    policy: ConstantValidation,
    catalog: &novarocks_functions::PureEngineFunctionCatalog,
) -> Arc<crate::CompiledExprSet> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        &HarnessControl,
        CompilePhase::LowerProgram,
    )
    .unwrap();
    let compiled =
        crate::compile_expr_set_pure(unit, environment, layouts, policy, catalog, &mut work)
            .unwrap();
    work.finish().unwrap();
    compiled
}

mod control;
pub use control::{CallbackControl, callback_causes};

pub use crate::evaluator::scalar_invocation::{
    call_invocation, call_scalar, call_scoped_invocation,
};

pub mod expression_text;
pub mod filter_conjunction_fixture;
pub mod frame_fixtures;
pub mod guarded_fixtures;
pub mod numeric_fixtures;
pub mod rng_fixture;
pub mod row_errors;
