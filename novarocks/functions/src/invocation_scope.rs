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

//! Explicit evaluated sources for an original whole operation. No expression,
//! runtime authority or domain reconstruction capability crosses this loan.

use crate::kernel_control::invalid;
use crate::kernel_input::{EvaluationCheckpoints, validate_argument_observed};
use crate::{
    ConstantValue, EvaluatedArgument, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    Selection,
};
use arrow_array::ArrayRef;
use novarocks_type_contract::FunctionValueType;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationInputDomainPolicy {
    /// Preserve the existing evaluated-argument entry and selected child demand.
    SelectedDemand,
    /// Ordered children are evaluated in the actual invocation's exact domain.
    /// Only a successful result is projected to independent outer demand.
    OriginalActualDomain,
}

#[derive(Clone, Copy)]
pub struct ScalarInvocationScope<'a> {
    actual_domain: Selection<'a>,
    demand: Selection<'a>,
}
impl<'a> ScalarInvocationScope<'a> {
    /// These are factual caller inputs. The instance validates them after its
    /// failed-instance latch, before reading arguments or invoking the owner.
    pub const fn new(actual_domain: Selection<'a>, demand: Selection<'a>) -> Self {
        Self {
            actual_domain,
            demand,
        }
    }
    pub const fn actual_domain(self) -> Selection<'a> {
        self.actual_domain
    }
    pub const fn demand(self) -> Selection<'a> {
        self.demand
    }
    pub(crate) fn validate(
        self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        if self.actual_domain.batch_rows() != self.demand.batch_rows() {
            return Err(invalid("invocation scope uses unrelated logical batches"));
        }
        if self.actual_domain.is_all() {
            return work.finish();
        }
        let mut actual = self.actual_domain.iter().peekable();
        for demanded in self.demand.iter() {
            while actual.peek().is_some_and(|row| *row < demanded) {
                actual.next();
                work.step()?;
            }
            if actual.peek().copied() != Some(demanded) {
                return Err(invalid("invocation demand is outside its actual domain"));
            }
            actual.next();
            work.step()?;
        }
        work.finish()
    }
}

#[derive(Clone, Copy)]
pub struct EvaluatedSourceLoan<'a> {
    array: &'a ArrayRef,
    domain: Selection<'a>,
}
impl<'a> EvaluatedSourceLoan<'a> {
    /// Facts only. The invocation validates length/map/type after its failed latch.
    pub const fn new(array: &'a ArrayRef, domain: Selection<'a>) -> Self {
        Self { array, domain }
    }
    pub const fn array(self) -> &'a ArrayRef {
        self.array
    }
    pub const fn domain(self) -> Selection<'a> {
        self.domain
    }
    /// Exact binary lookup in the caller-supplied ordered map, never a guess
    /// from contiguous rows, values, field names or buffer identity.
    pub fn physical_ordinal(self, logical_row: usize) -> Option<usize> {
        let mut low = 0;
        let mut high = self.domain.len();
        while low < high {
            let middle = low + (high - low) / 2;
            match self.domain.row(middle)?.cmp(&logical_row) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Equal => return Some(middle),
                std::cmp::Ordering::Greater => high = middle,
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
pub enum ScalarInvocationSource<'a> {
    Array(EvaluatedSourceLoan<'a>),
    /// Only this checked ordinal participates, irrespective of pool row count.
    Constant(&'a ConstantValue),
    /// An explicitly authored one-row broadcast, not an inferred short column.
    Scalar(&'a ArrayRef),
}
impl ScalarInvocationSource<'_> {
    pub(crate) fn validate(
        self,
        scope: ScalarInvocationScope<'_>,
        ty: &FunctionValueType,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        match self {
            Self::Array(source) => {
                if source.array.len() != source.domain.len() {
                    return Err(invalid(
                        "evaluated source array differs from its explicit domain",
                    ));
                }
                let mut work = EvaluationCheckpoints::new(control);
                if !source
                    .domain
                    .same_rows_observed(scope.actual_domain, || work.step())?
                {
                    return Err(invalid(
                        "evaluated source map differs from its actual invocation domain",
                    ));
                }
                work.finish()?;
                validate_argument_observed(
                    EvaluatedArgument::Column(source.array),
                    Selection::all(source.array.len()),
                    ty,
                    control,
                )
            }
            Self::Constant(value) => validate_argument_observed(
                EvaluatedArgument::Constant(value),
                scope.actual_domain,
                ty,
                control,
            ),
            Self::Scalar(array) => validate_argument_observed(
                EvaluatedArgument::Scalar(array),
                scope.actual_domain,
                ty,
                control,
            ),
        }
    }
}

#[derive(Clone, Copy)]
pub struct ScopedScalarCallInput<'call, 'a> {
    pub(crate) contract: &'call ScalarCallContract,
    pub(crate) scope: ScalarInvocationScope<'a>,
    pub(crate) sources: &'a [ScalarInvocationSource<'a>],
}
impl<'call, 'a> ScopedScalarCallInput<'call, 'a> {
    pub const fn contract(self) -> &'call ScalarCallContract {
        self.contract
    }
    pub const fn scope(self) -> ScalarInvocationScope<'a> {
        self.scope
    }
    pub const fn sources(self) -> &'a [ScalarInvocationSource<'a>] {
        self.sources
    }
}

/// Keep the canonical Arrow creator, independently of any equal source type.
/// This preserves no source permission and never activates the actual leaf.
pub(crate) fn retain_scoped_result_type(
    array: ArrayRef,
    result_type: &FunctionValueType,
    host: &std::sync::Arc<dyn crate::AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let layout = result_type.typed_binding().layout();
    if layout.storage_lifetime().is_pure() && layout.arrow_backing_storage_lifetime().is_pure() {
        return Ok(array);
    }
    let mut work = EvaluationCheckpoints::new(control);
    let owner = crate::SourceBackingOwner::try_from_owned_direct(
        result_type.clone(),
        host.clone(),
        &mut work,
    )?;
    let prepared = crate::PreparedSourceCustody::prepare(&array, &mut work)?;
    let authority = crate::opaque_memory::OpaqueRetainedCharge::try_new(host.clone())?;
    let mut reservation =
        authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
    let array = prepared.retain(&owner, &mut reservation, &mut work)?;
    work.finish()?;
    Ok(array)
}
