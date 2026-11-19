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

//! Cross-unit totals retain the former single-namespace projection envelopes.
//! Prefixes replace the current child; completed children join exactly once.
use crate::{
    physical_call_requests_v2::{
        CallRequestProjectionFacts as R, CallRequestProjectionLimits as RL,
    },
    physical_control_v2::{ControlProjectionFacts as C, ControlProjectionLimits as CL},
    physical_expression_v2::{
        ExpressionNamespaceWriteFacts as X, ExpressionProjectionLimits as XL,
    },
    physical_node_v2::{NodeProjectionFacts as N, NodeProjectionLimits as NL},
};
use novarocks_type_contract::CompileControlError;
#[derive(Default)]
pub(super) struct UnitProjectionTotals {
    expressions: [usize; 6],
    materialized: [usize; 6],
    control: [usize; 5],
    requests: [usize; 5],
    semantics: [usize; 5],
    root_count: usize,
}
fn prefix<const N: usize>(
    base: [usize; N],
    child: [usize; N],
    limits: [usize; N],
) -> Result<[usize; N], CompileControlError> {
    let mut total = base;
    for at in 0..N {
        total[at] = total[at]
            .checked_add(child[at])
            .ok_or(CompileControlError::ResourceExhausted)?;
        if total[at] > limits[at] {
            return Err(CompileControlError::ResourceExhausted);
        }
    }
    Ok(total)
}
impl UnitProjectionTotals {
    /// Preserve the former raw RootBinding header count across every unit.
    /// The checked flow additionally enforces its original shared reference budget.
    pub fn roots(
        &mut self,
        count: usize,
    ) -> Result<(), crate::physical_expression_v2::ExpressionCodecError> {
        let count = self
            .root_count
            .checked_add(count)
            .ok_or(novarocks_type_contract::ExpressionControlFlowError::TooManyItems);
        let count = count.map_err(novarocks_expression_contract::UnitError::from)?;
        if count > novarocks_type_contract::MAX_CONTROL_USE_REFERENCES {
            return Err(novarocks_expression_contract::UnitError::from(
                novarocks_type_contract::ExpressionControlFlowError::TooManyItems,
            )
            .into());
        }
        self.root_count = count;
        Ok(())
    }
    fn expressions_next(
        &self,
        f: &X,
        l: XL,
        materialized: bool,
    ) -> Result<[usize; 6], CompileControlError> {
        prefix(
            if materialized {
                self.materialized
            } else {
                self.expressions
            },
            [
                f.definition_count,
                f.type_reference_count,
                f.expression_reference_count,
                f.new_allocation_requests_upper_bound,
                f.new_allocation_request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            ],
            [
                l.max_definitions,
                l.max_type_references,
                l.max_expression_references,
                l.max_new_allocation_requests,
                l.max_new_allocation_request_bytes,
                l.max_cumulative_work,
            ],
        )
    }
    pub fn expressions_prefix(
        &self,
        f: &X,
        l: XL,
        materialized: bool,
    ) -> Result<(), CompileControlError> {
        self.expressions_next(f, l, materialized).map(|_| ())
    }
    pub fn expressions_commit(
        &mut self,
        f: &X,
        l: XL,
        materialized: bool,
    ) -> Result<(), CompileControlError> {
        let total = self.expressions_next(f, l, materialized)?;
        if materialized {
            self.materialized = total;
        } else {
            self.expressions = total;
        }
        Ok(())
    }
    fn control_next(&self, f: &C, l: CL) -> Result<[usize; 5], CompileControlError> {
        prefix(
            self.control,
            [
                f.region_count,
                f.use_reference_count,
                f.allocation_requests_upper_bound,
                f.allocation_request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            ],
            [
                l.max_regions,
                l.max_use_references,
                l.max_allocation_requests,
                l.max_allocation_request_bytes,
                l.max_work,
            ],
        )
    }
    pub fn control_prefix(&self, f: &C, l: CL) -> Result<(), CompileControlError> {
        self.control_next(f, l).map(|_| ())
    }
    pub fn control_commit(&mut self, f: &C, l: CL) -> Result<(), CompileControlError> {
        self.control = self.control_next(f, l)?;
        Ok(())
    }
    fn requests_next(&self, f: &R, l: RL) -> Result<[usize; 5], CompileControlError> {
        prefix(
            self.requests,
            [
                f.definition_count,
                f.type_reference_count,
                f.allocation_requests_upper_bound,
                f.request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            ],
            [
                l.max_definitions,
                l.max_type_references,
                l.max_allocation_requests,
                l.max_request_bytes,
                l.max_work,
            ],
        )
    }
    pub fn requests_prefix(&self, f: &R, l: RL) -> Result<(), CompileControlError> {
        self.requests_next(f, l).map(|_| ())
    }
    pub fn requests_commit(&mut self, f: &R, l: RL) -> Result<(), CompileControlError> {
        self.requests = self.requests_next(f, l)?;
        Ok(())
    }
    fn semantics_next(&self, f: &N, l: NL) -> Result<[usize; 5], CompileControlError> {
        prefix(
            self.semantics,
            [
                f.list_item_count,
                f.value_reference_count,
                f.allocation_requests_upper_bound,
                f.allocation_request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            ],
            [
                l.max_list_items
                    .min(novarocks_physical_plan::MAX_PLAN_DERIVED_CUT_ITEMS),
                l.max_value_references,
                l.max_allocation_requests,
                l.max_allocation_request_bytes,
                l.max_work,
            ],
        )
    }
    pub fn semantics_prefix(&self, f: &N, l: NL) -> Result<(), CompileControlError> {
        self.semantics_next(f, l).map(|_| ())
    }
    pub fn semantics_commit(&mut self, f: &N, l: NL) -> Result<(), CompileControlError> {
        self.semantics = self.semantics_next(f, l)?;
        Ok(())
    }
}
