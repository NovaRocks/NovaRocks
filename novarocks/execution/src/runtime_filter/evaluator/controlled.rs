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

//! The installed controlled-query policy over the original scalar decoder
//! and row loop. Public work and retained mask stock have separate lifetimes.

use super::*;
use crate::exec::arrow_work::{ArrowWorkBudget, ArrowWorkShape};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, source_custody_prefix_facts,
};
use std::sync::Arc;

/// The mask must be destroyed before its retained fee. Callers only borrow
/// its outcome during synchronous application; there is no naked take API.
pub(crate) struct ControlledRowOutcome {
    outcome: RuntimeFilterRowOutcome,
    _mask_stock: Option<OpaqueRetainedCharge>,
}
impl std::fmt::Debug for ControlledRowOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.outcome.fmt(f)
    }
}
impl ControlledRowOutcome {
    pub(crate) fn outcome(&self) -> &RuntimeFilterRowOutcome {
        &self.outcome
    }
    pub(crate) fn effect_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<RuntimeFilterRowEffect>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let effect = self.outcome.effect_observed(|| work.step())?;
        work.finish()?;
        Ok(effect)
    }
}

struct ControlledQueryPolicy<'a> {
    query: &'a dyn RuntimeFilterControlledScalarQuery,
    control: &'a dyn KernelEvaluationControl,
    work: EvaluationCheckpoints<'a>,
}
fn split_query_verdict(
    result: Result<bool, RuntimeFilterControlledQueryError>,
) -> ExecutionResult<Result<bool, RuntimeFilterArtifactQueryError>> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(RuntimeFilterControlledQueryError::Artifact(cause)) => Ok(Err(cause)),
        Err(RuntimeFilterControlledQueryError::Kernel(cause)) => Err(cause.into()),
    }
}
impl RowQueryPolicy for ControlledQueryPolicy<'_> {
    type Error = ExecutionFailure;
    fn before_row(&mut self) -> ExecutionResult<()> {
        Ok(self.work.step()?)
    }
    fn matches_null(
        &mut self,
        _: &dyn RuntimeFilterArtifactQuery,
    ) -> ExecutionResult<Result<bool, RuntimeFilterArtifactQueryError>> {
        split_query_verdict(self.query.matches_null_controlled(self.control))
    }
    fn non_null_value_may_match(
        &mut self,
        _: &dyn RuntimeFilterArtifactQuery,
        value: RuntimeFilterScalarRef<'_>,
    ) -> ExecutionResult<Result<bool, RuntimeFilterArtifactQueryError>> {
        split_query_verdict(
            self.query
                .non_null_value_may_match_controlled(value, self.control),
        )
    }
}

pub(crate) fn evaluate_rows_controlled(
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    artifact: &dyn RuntimeFilterArtifactQuery,
    input: &ArrayRef,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ControlledRowOutcome> {
    if input.data_type() != artifact.data_type() {
        return Err(violation(
            "runtime-filter row input type differs from the immutable artifact type",
        )
        .into());
    }
    let query = artifact.controlled_scalar_query().ok_or_else(|| {
        KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "compiled ordered runtime-filter artifact has no controlled scalar query",
        ))
    })?;
    let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
    let _prefix = authority.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
    let mut policy = ControlledQueryPolicy {
        query,
        control,
        work: EvaluationCheckpoints::new(control),
    };
    policy.work.flush()?;
    let _public_work = ArrowWorkBudget::admit(
        Arc::clone(&host),
        ArrowWorkShape {
            rows: input.len(),
            cells: input.len(),
            payload_bytes: input.get_array_memory_size(),
            fields: 1,
            buffers: 3,
        },
    )?;
    let mut result = ControlledRowOutcome {
        outcome: evaluate_rows_with(&mut policy, binding_id, logical_version, artifact, input)?,
        _mask_stock: None,
    };
    if let RuntimeFilterRowEvaluation::Evaluated { mask, .. } = &result.outcome.evaluation {
        let mut charge = OpaqueRetainedCharge::try_new(host)?;
        let bytes = mask.values().inner().capacity();
        let mut reservation = charge.reserve_operation(bytes)?;
        charge.reconcile_under_reservation(bytes, &mut reservation)?;
        drop(reservation);
        result._mask_stock = Some(charge);
    }
    policy.work.finish()?;
    Ok(result)
}

#[cfg(test)]
#[path = "controlled_tests.rs"]
mod tests;
