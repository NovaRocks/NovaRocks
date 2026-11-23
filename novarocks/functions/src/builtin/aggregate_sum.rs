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

//! Exact four-phase SUM with inline state.
//!
//! The value is the exact mathematical sum of a group's non-NULL inputs, and
//! NULL when it has none. An exact sum never depends on the order rows arrive
//! in, how batches split them, the driver count or the Partial/Final split:
//! - BOOLEAN and integers up to BIGINT accumulate in i128 and travel as a
//!   `Decimal128(38, 0)` intermediate;
//! - LARGEINT and DECIMAL up to 128 bits accumulate in i256 and travel as a
//!   `Decimal256(76, s)` intermediate.
//!
//! The only overflow is the result's, checked once when the final value is
//! built: BIGINT and LARGEINT report an unmaskable numeric Data failure; DECIMAL checks 38
//! digits and follows the call's frozen overflow policy, a numeric Data failure
//! or NULL for that group. The running add is checked too; it can only fail
//! past 2^64 rows of one group and then reports a numeric Data failure.
//!
//! FLOAT and DOUBLE accumulate IEEE doubles. Their rounding depends on the
//! accumulation order, which this owner declares unordered: equal inputs in
//! another order may round differently. DECIMAL256 has no SUM owner here.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;

/// Largest DECIMAL(38, s) magnitude, 10^38 - 1, as an unscaled value.
const MAX_DECIMAL38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// How one SUM accumulates. An exact domain's result never depends on the
/// order of its inputs; an unordered one may round differently by order, so
/// its oracles fix the order or compare within a tolerance. This is the
/// owner's declaration; no host consults it yet.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SumAccumulation {
    Exact,
    Unordered,
}

/// The selected input domain of one prepared SUM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SumDomain {
    /// BOOLEAN, TINYINT .. BIGINT: i128 state, BIGINT result.
    Integer,
    /// LARGEINT: i256 state, LARGEINT result.
    LargeInt,
    /// DECIMAL(p, s) up to 128 bits: i256 state, DECIMAL(38, s) result.
    Decimal,
    /// FLOAT and DOUBLE: f64 state, DOUBLE result.
    Float,
}

impl SumDomain {
    #[allow(dead_code)]
    pub(super) const fn accumulation(self) -> SumAccumulation {
        match self {
            Self::Float => SumAccumulation::Unordered,
            Self::Integer | Self::LargeInt | Self::Decimal => SumAccumulation::Exact,
        }
    }
}

/// One group's state. `Empty` means no non-NULL input has arrived yet.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(super) enum SumState {
    #[default]
    Empty,
    Narrow(i128),
    Wide(i256),
    Float(f64),
}

#[derive(Debug)]
pub(super) struct SumKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) domain: SumDomain,
}

fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = f(&mut work);
    if result.as_ref().is_err_and(KernelFailure::is_interruption) {
        return result;
    }
    work.finish()?;
    result
}

/// The SUM owner authors numeric value failures at the actual operation.
/// Result overflow is required aggregate work, with no row-error channel.
#[derive(Debug)]
enum SumNumericFailure {
    ExactState,
    BigInt(std::num::TryFromIntError),
    LargeInt,
    Decimal38,
}
impl std::fmt::Display for SumNumericFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ExactState => "sum exact state overflow",
            Self::BigInt(_) => "sum result overflows BIGINT",
            Self::LargeInt => "sum result overflows LARGEINT",
            Self::Decimal38 => "sum result overflows DECIMAL(38)",
        })
    }
}
impl std::error::Error for SumNumericFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BigInt(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for SumNumericFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        DataFailureReason::NumericOutOfRange.failure_fact()
    }
}
impl novarocks_failure_contract::OuterFailureProjection for SumNumericFailure {}
impl From<SumNumericFailure> for KernelFailure {
    fn from(source: SumNumericFailure) -> Self {
        Self::from_domain(source)
    }
}

/// One non-NULL input or merged state contribution.
#[derive(Clone, Copy, Debug)]
enum Contribution {
    Narrow(i128),
    Wide(i256),
    Float(f64),
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, KernelFailure> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| internal("SUM carrier has a foreign concrete array class"))
}

/// Read one update input value in the selected domain.
fn read_input(
    domain: SumDomain,
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<Contribution>, KernelFailure> {
    let within = row < array.len();
    work.step()?;
    if !within {
        return Err(internal("SUM selected address is outside its carrier"));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match (domain, array.data_type()) {
        (SumDomain::Integer, DataType::Boolean) => {
            Contribution::Narrow(i128::from(downcast::<BooleanArray>(array)?.value(row)))
        }
        (SumDomain::Integer, DataType::Int8) => {
            Contribution::Narrow(i128::from(downcast::<Int8Array>(array)?.value(row)))
        }
        (SumDomain::Integer, DataType::Int16) => {
            Contribution::Narrow(i128::from(downcast::<Int16Array>(array)?.value(row)))
        }
        (SumDomain::Integer, DataType::Int32) => {
            Contribution::Narrow(i128::from(downcast::<Int32Array>(array)?.value(row)))
        }
        (SumDomain::Integer, DataType::Int64) => {
            Contribution::Narrow(i128::from(downcast::<Int64Array>(array)?.value(row)))
        }
        (SumDomain::LargeInt, DataType::FixedSizeBinary(16)) => {
            let bytes: [u8; 16] = downcast::<FixedSizeBinaryArray>(array)?
                .value(row)
                .try_into()
                .map_err(|_| internal("SUM LARGEINT value width differs"))?;
            Contribution::Wide(i256::from_i128(i128::from_be_bytes(bytes)))
        }
        (SumDomain::Decimal, DataType::Decimal128(..)) => Contribution::Wide(i256::from_i128(
            downcast::<Decimal128Array>(array)?.value(row),
        )),
        (SumDomain::Float, DataType::Float32) => {
            Contribution::Float(f64::from(downcast::<Float32Array>(array)?.value(row)))
        }
        (SumDomain::Float, DataType::Float64) => {
            Contribution::Float(downcast::<Float64Array>(array)?.value(row))
        }
        _ => return Err(internal("SUM input carrier differs from its domain")),
    };
    work.step()?;
    Ok(Some(value))
}

/// Read one merged intermediate state in the selected domain.
fn read_state(
    domain: SumDomain,
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<Contribution>, KernelFailure> {
    let within = row < array.len();
    work.step()?;
    if !within {
        return Err(internal(
            "SUM selected state address is outside its carrier",
        ));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match (domain, array.data_type()) {
        (SumDomain::Integer, DataType::Decimal128(..)) => {
            Contribution::Narrow(downcast::<Decimal128Array>(array)?.value(row))
        }
        (SumDomain::LargeInt | SumDomain::Decimal, DataType::Decimal256(..)) => {
            Contribution::Wide(downcast::<Decimal256Array>(array)?.value(row))
        }
        (SumDomain::Float, DataType::Float64) => {
            Contribution::Float(downcast::<Float64Array>(array)?.value(row))
        }
        _ => return Err(internal("SUM state carrier differs from its domain")),
    };
    work.step()?;
    Ok(Some(value))
}

/// Add one contribution. The state changes only after the add succeeded.
fn accumulate(
    state: &mut SumState,
    contribution: Option<Contribution>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let Some(contribution) = contribution else {
        return Ok(());
    };
    let next = match (*state, contribution) {
        (SumState::Empty, Contribution::Narrow(value)) => SumState::Narrow(value),
        (SumState::Narrow(sum), Contribution::Narrow(value)) => SumState::Narrow(
            sum.checked_add(value)
                .ok_or_else(|| KernelFailure::from(SumNumericFailure::ExactState))?,
        ),
        (SumState::Empty, Contribution::Wide(value)) => SumState::Wide(value),
        (SumState::Wide(sum), Contribution::Wide(value)) => SumState::Wide(
            sum.checked_add(value)
                .ok_or_else(|| KernelFailure::from(SumNumericFailure::ExactState))?,
        ),
        (SumState::Empty, Contribution::Float(value)) => SumState::Float(value),
        (SumState::Float(sum), Contribution::Float(value)) => SumState::Float(sum + value),
        _ => return Err(internal("SUM state differs from its domain")),
    };
    work.step()?;
    *state = next;
    Ok(())
}

impl SumKernel {
    fn final_value(&self) -> &FunctionValueType {
        self.contract.final_type()
    }
}

impl PreparedAggregateKernel for SumKernel {
    fn supports_local_stages(&self) -> bool {
        true
    }
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(crate::kernel_control::compile_failure)?;
        Ok(Arc::new(Self {
            contract,
            domain: self.domain,
        }))
    }
    type State = SumState;
    type PreparedUpdateBatch<'batch> = SelectedAggregateUpdateInput<'batch, 'batch>;
    type PreparedMergeBatch<'batch> = SelectedAggregateMergeInput<'batch, 'batch>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::FixedZero
    }
    fn retained_bytes(&self, _: &SumState) -> usize {
        0
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SumState, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            Ok(SumState::Empty)
        })
    }
    fn prepare_update<'batch>(
        &'batch self,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && self.contract.phase().consumes_logical_arguments()
                && input.logical_arguments().len() == 1
                && input.order_arguments().is_empty();
            work.step()?;
            if !valid {
                return Err(invalid(
                    "SUM update differs from its exact phase or channels",
                ));
            }
            Ok(input)
        })
    }
    fn update_row<'batch>(
        &self,
        state: &mut SumState,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.selection().row(ordinal);
            work.step()?;
            let row = row.ok_or_else(|| invalid("SUM selected update ordinal is out of bounds"))?;
            let argument = prepared.logical_arguments()[0];
            let value = read_input(
                self.domain,
                argument.array().as_ref(),
                argument.value_row(ordinal, row),
                work,
            )?;
            accumulate(state, value, work)
        })
    }
    fn prepare_merge<'batch>(
        &'batch self,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && !self.contract.phase().consumes_logical_arguments();
            work.step()?;
            if !valid {
                return Err(invalid("SUM merge differs from its exact phase"));
            }
            Ok(input)
        })
    }
    fn merge_row<'batch>(
        &self,
        state: &mut SumState,
        prepared: &Self::PreparedMergeBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.selection().row(ordinal);
            work.step()?;
            let row = row.ok_or_else(|| invalid("SUM selected merge ordinal is out of bounds"))?;
            let argument = prepared.state();
            let value = read_state(
                self.domain,
                argument.array().as_ref(),
                argument.value_row(ordinal, row),
                work,
            )?;
            accumulate(state, value, work)
        })
    }
    fn build_intermediate<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state SumState>,
    {
        let ty = self.contract.intermediate_type().data_type().clone();
        observed(control, |work| {
            let rows = states.len();
            match (self.domain, &ty) {
                (SumDomain::Integer, DataType::Decimal128(..)) => {
                    let mut values = reserve(rows, work)?;
                    for state in states {
                        values.push(match state {
                            SumState::Empty => None,
                            SumState::Narrow(sum) => Some(*sum),
                            _ => return Err(internal("SUM state differs from its domain")),
                        });
                        work.step()?;
                    }
                    exact_rows(values.len(), rows)?;
                    Ok(
                        Arc::new(Decimal128Array::from(values).with_data_type(ty.clone()))
                            as ArrayRef,
                    )
                }
                (SumDomain::LargeInt | SumDomain::Decimal, DataType::Decimal256(..)) => {
                    let mut values = reserve(rows, work)?;
                    for state in states {
                        values.push(match state {
                            SumState::Empty => None,
                            SumState::Wide(sum) => Some(*sum),
                            _ => return Err(internal("SUM state differs from its domain")),
                        });
                        work.step()?;
                    }
                    exact_rows(values.len(), rows)?;
                    Ok(
                        Arc::new(Decimal256Array::from(values).with_data_type(ty.clone()))
                            as ArrayRef,
                    )
                }
                (SumDomain::Float, DataType::Float64) => build_float(states, rows, work),
                _ => Err(internal("SUM intermediate carrier differs from its domain")),
            }
        })
    }
    fn build_final<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state SumState>,
    {
        let ty = self.final_value().data_type().clone();
        let policy = self.contract.call().decimal_overflow_policy();
        observed(control, |work| {
            let rows = states.len();
            match (self.domain, &ty) {
                (SumDomain::Integer, DataType::Int64) => {
                    let mut values = reserve(rows, work)?;
                    for state in states {
                        values.push(match state {
                            SumState::Empty => None,
                            SumState::Narrow(sum) => {
                                Some(i64::try_from(*sum).map_err(SumNumericFailure::BigInt)?)
                            }
                            _ => return Err(internal("SUM state differs from its domain")),
                        });
                        work.step()?;
                    }
                    exact_rows(values.len(), rows)?;
                    Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
                }
                (SumDomain::LargeInt, DataType::FixedSizeBinary(16)) => {
                    let mut values: Vec<Option<[u8; 16]>> = reserve(rows, work)?;
                    for state in states {
                        values.push(match state {
                            SumState::Empty => None,
                            SumState::Wide(sum) => Some(
                                sum.to_i128()
                                    .ok_or_else(|| {
                                        KernelFailure::from(SumNumericFailure::LargeInt)
                                    })?
                                    .to_be_bytes(),
                            ),
                            _ => return Err(internal("SUM state differs from its domain")),
                        });
                        work.step()?;
                    }
                    exact_rows(values.len(), rows)?;
                    let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        values.into_iter(),
                        16,
                    )
                    .map_err(|_| internal("SUM LARGEINT result construction failed"))?;
                    Ok(Arc::new(array) as ArrayRef)
                }
                (SumDomain::Decimal, DataType::Decimal128(38, _)) => {
                    let mut values = reserve(rows, work)?;
                    for state in states {
                        values.push(match state {
                            SumState::Empty => None,
                            SumState::Wide(sum) => match sum
                                .to_i128()
                                .filter(|value| value.unsigned_abs() <= MAX_DECIMAL38 as u128)
                            {
                                Some(value) => Some(value),
                                None => match policy {
                                    DecimalOverflowPolicy::ReportError => {
                                        return Err(SumNumericFailure::Decimal38.into());
                                    }
                                    DecimalOverflowPolicy::OutputNull => None,
                                },
                            },
                            _ => return Err(internal("SUM state differs from its domain")),
                        });
                        work.step()?;
                    }
                    exact_rows(values.len(), rows)?;
                    Ok(
                        Arc::new(Decimal128Array::from(values).with_data_type(ty.clone()))
                            as ArrayRef,
                    )
                }
                (SumDomain::Float, DataType::Float64) => build_float(states, rows, work),
                _ => Err(internal("SUM result carrier differs from its domain")),
            }
        })
    }
}

fn reserve<T>(rows: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    std::alloc::Layout::array::<T>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}

fn exact_rows(actual: usize, rows: usize) -> Result<(), KernelFailure> {
    if actual != rows {
        return Err(internal("SUM emission iterator changed its exact extent"));
    }
    Ok(())
}

fn build_float<'state, I: ExactSizeIterator<Item = &'state SumState>>(
    states: I,
    rows: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let mut values = reserve(rows, work)?;
    for state in states {
        values.push(match state {
            SumState::Empty => None,
            SumState::Float(sum) => Some(*sum),
            _ => return Err(internal("SUM state differs from its domain")),
        });
        work.step()?;
    }
    exact_rows(values.len(), rows)?;
    Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

#[cfg(test)]
#[path = "aggregate_sum_tests.rs"]
mod tests;
