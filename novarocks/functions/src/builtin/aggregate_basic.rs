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

//! Inline selected lifecycle for AVG, COUNT_IF, Boolean folds and moments.
//! Arithmetic order, empty states and intermediate encodings match the v1 owners.

use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::*;
use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, ValueLogicalType};
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BasicOperation {
    Avg,
    CountIf,
    Or,
    And,
    VarPop,
    VarSamp,
    StdPop,
    StdSamp,
    CovPop,
    CovSamp,
    Corr,
}
pub(super) fn operation(name: &str) -> Option<BasicOperation> {
    use BasicOperation::*;
    Some(match name {
        "avg" => Avg,
        "count_if" => CountIf,
        "bool_or" | "boolor_agg" => Or,
        "bool_and" | "booland_agg" => And,
        "variance" | "variance_pop" | "var_pop" => VarPop,
        "variance_samp" | "var_samp" => VarSamp,
        "stddev" | "std" | "stddev_pop" => StdPop,
        "stddev_samp" => StdSamp,
        "covar_pop" => CovPop,
        "covar_samp" => CovSamp,
        "corr" => Corr,
        _ => return None,
    })
}
impl BasicOperation {
    pub fn arity(self) -> usize {
        if matches!(self, Self::CovPop | Self::CovSamp | Self::Corr) {
            2
        } else {
            1
        }
    }
    fn boolean(self) -> bool {
        matches!(self, Self::Or | Self::And)
    }
    fn variance(self) -> bool {
        matches!(
            self,
            Self::VarPop | Self::VarSamp | Self::StdPop | Self::StdSamp
        )
    }
}

pub(super) fn validate_contract(
    op: BasicOperation,
    c: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let numeric = |t: &DataType| {
        matches!(
            t,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        )
    };
    let arguments = &c.call().selected().argument_types;
    // Preserve the original control boundaries while borrowing the checked
    // declarations instead of publishing a scratch carrier-pointer Vec.
    work.flush().map_err(compile_failure)?;
    work.flush().map_err(compile_failure)?;
    for t in arguments {
        let FunctionArgumentType::Value(t) = t else {
            return Err(invalid("basic aggregate requires value arguments"));
        };
        work.step().map_err(compile_failure)?;
        if t.logical_type() != ValueLogicalType::Physical {
            return Err(invalid("basic aggregate requires physical argument types"));
        }
    }
    let FunctionResultType::Scalar(result) = &c.call().selected().result_type else {
        return Err(invalid("basic aggregate requires a scalar result"));
    };
    let types = || {
        arguments.iter().map(|argument| {
            let FunctionArgumentType::Value(value) = argument else {
                unreachable!("checked basic aggregate value argument")
            };
            value.data_type()
        })
    };
    let valid_inputs = arguments.len() == op.arity()
        && types().all(|t| {
            if op.boolean() || op == BasicOperation::CountIf {
                *t == DataType::Boolean
            } else {
                numeric(t)
                    || op == BasicOperation::Avg
                        && matches!(t, DataType::Decimal128(..) | DataType::Decimal256(..))
            }
        });
    let expected_state = if op.boolean() {
        DataType::Boolean
    } else if op == BasicOperation::CountIf {
        DataType::Int64
    } else if op == BasicOperation::Avg {
        DataType::Utf8
    } else {
        DataType::Binary
    };
    let expected_result = if op.boolean() {
        DataType::Boolean
    } else if op == BasicOperation::CountIf {
        DataType::Int64
    } else if op == BasicOperation::Avg {
        match types().next() {
            Some(t @ DataType::Decimal128(..)) => {
                novarocks_type_contract::canonical_agg_decimal_type("avg", t)
                    .ok_or_else(|| invalid("AVG decimal type is unsupported"))?
            }
            Some(t @ DataType::Decimal256(..)) => t.clone(),
            _ => DataType::Float64,
        }
    } else {
        DataType::Float64
    };
    work.step().map_err(compile_failure)?;
    if !valid_inputs
        || *result.data_type() != expected_result
        || result.logical_type() != ValueLogicalType::Physical
        || *c.intermediate_type().data_type() != expected_state
        || c.intermediate_type().logical_type() != ValueLogicalType::Physical
    {
        return Err(invalid(
            "basic aggregate selected argument, result or state type differs from its owner",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BasicState {
    pub count: i64,
    pub seen: bool,
    pub boolean: bool,
    pub sum: f64,
    pub decimal: i128,
    pub wide: i256,
    pub x: f64,
    pub y: f64,
    pub m2: f64,
    pub m2x: f64,
    pub m2y: f64,
}
#[derive(Debug)]
pub(super) struct BasicKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: BasicOperation,
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut work);
    let result = work.finish_result(result);
    observation.finish(result)
}
fn integer(a: &dyn Array, row: usize) -> Result<i64, KernelFailure> {
    Ok(match a.data_type() {
        DataType::Int8 => down::<Int8Array>(a)?.value(row) as i64,
        DataType::Int16 => down::<Int16Array>(a)?.value(row) as i64,
        DataType::Int32 => down::<Int32Array>(a)?.value(row) as i64,
        DataType::Int64 => down::<Int64Array>(a)?.value(row),
        _ => return Err(internal("basic aggregate integer state differs")),
    })
}
/// A whole aggregate state/result overflows; this is not a promoted row error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BasicNumericOverflow;
impl std::fmt::Display for BasicNumericOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("decimal overflow")
    }
}
impl std::error::Error for BasicNumericOverflow {}
impl novarocks_failure_contract::FailureProjection for BasicNumericOverflow {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &crate::DataFailureReason::NumericOutOfRange,
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for BasicNumericOverflow {}
fn numeric_overflow() -> KernelFailure {
    KernelFailure::from_domain(BasicNumericOverflow)
}
fn down<T: 'static>(a: &dyn Array) -> Result<&T, KernelFailure> {
    a.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| internal("basic aggregate has a foreign carrier class"))
}
fn numeric(a: &dyn Array, row: usize) -> Result<f64, KernelFailure> {
    Ok(match a.data_type() {
        DataType::Int8 => down::<Int8Array>(a)?.value(row) as f64,
        DataType::Int16 => down::<Int16Array>(a)?.value(row) as f64,
        DataType::Int32 => down::<Int32Array>(a)?.value(row) as f64,
        DataType::Int64 => down::<Int64Array>(a)?.value(row) as f64,
        DataType::Float32 => down::<Float32Array>(a)?.value(row) as f64,
        DataType::Float64 => down::<Float64Array>(a)?.value(row),
        _ => return Err(internal("basic aggregate numeric carrier differs")),
    })
}
fn add_count(s: &mut BasicState, n: i64) -> Result<(), KernelFailure> {
    s.count += n;
    Ok(())
}
impl super::aggregate_window_adapter::InlineAggregateWindowKernel for BasicKernel {
    fn copy_state(&self, state: &BasicState) -> Result<BasicState, KernelFailure> {
        Ok(*state)
    }
}
impl PreparedAggregateKernel for BasicKernel {
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
            operation: self.operation,
        }))
    }
    type State = BasicState;
    type PreparedUpdateBatch<'batch> = SelectedAggregateUpdateInput<'batch, 'batch>;
    type PreparedMergeBatch<'batch> = SelectedAggregateMergeInput<'batch, 'batch>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::FixedZero
    }
    fn retained_bytes(&self, _: &BasicState) -> usize {
        0
    }
    fn create_state(&self, c: &dyn KernelEvaluationControl) -> Result<BasicState, KernelFailure> {
        observed(c, |w| {
            w.step()?;
            Ok(BasicState {
                boolean: self.operation == BasicOperation::And,
                ..BasicState::default()
            })
        })
    }
    fn prepare_update<'batch>(
        &'batch self,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        c: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, KernelFailure> {
        observed(c, |w| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && self.contract.phase().consumes_logical_arguments()
                && input.logical_arguments().len() == self.operation.arity()
                && input.order_arguments().is_empty();
            w.step()?;
            if !valid {
                return Err(invalid(
                    "basic aggregate update differs from its exact phase or channels",
                ));
            }
            Ok(input)
        })
    }
    fn update_row<'batch>(
        &self,
        state: &mut BasicState,
        p: &Self::PreparedUpdateBatch<'batch>,
        ordinal: usize,
        c: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(c, |w| {
            let row = p
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("basic aggregate selected ordinal is out of bounds"))?;
            w.step()?;
            let arg = &p.logical_arguments()[0];
            let a = arg.array().as_ref();
            let address = arg.value_row(ordinal, row);
            if address >= a.len() {
                return Err(internal(
                    "basic aggregate selected address is out of bounds",
                ));
            }
            w.step()?;
            let first = read_basic_value(a, address, w)?;
            let second = if let Some(arg) = p.logical_arguments().get(1) {
                read_basic_value(arg.array().as_ref(), arg.value_row(ordinal, row), w)?
            } else {
                None
            };
            self.core().update(state, first, second, None, w)
        })
    }
    fn prepare_merge<'batch>(
        &'batch self,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        c: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, KernelFailure> {
        observed(c, |w| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && !self.contract.phase().consumes_logical_arguments();
            w.step()?;
            if !valid {
                return Err(invalid(
                    "basic aggregate merge differs from its exact phase",
                ));
            }
            Ok(input)
        })
    }
    fn merge_row<'batch>(
        &self,
        state: &mut BasicState,
        p: &Self::PreparedMergeBatch<'batch>,
        ordinal: usize,
        c: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(c, |w| {
            let row = p.selection().row(ordinal).ok_or_else(|| {
                invalid("basic aggregate selected merge ordinal is out of bounds")
            })?;
            w.step()?;
            let arg = p.state();
            let address = arg.value_row(ordinal, row);
            let a = arg.array().as_ref();
            if address >= a.len() {
                return Err(internal("basic aggregate merge address is out of bounds"));
            }
            w.step()?;
            if a.is_null(address) {
                return Ok(());
            }
            let incoming = self
                .core()
                .decode_state(a, address, w)
                .map_err(BasicStateError::into_kernel_failure)?;
            self.core().merge_state(state, incoming)?;
            w.step()?;
            Ok(())
        })
    }
    fn build_intermediate<'state, I>(
        &self,
        states: I,
        c: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state BasicState>,
    {
        self.core()
            .build(states.copied().map(Ok), true, c)
            .map_err(BasicStateError::into_kernel_failure)
    }
    fn build_final<'state, I>(
        &self,
        states: I,
        c: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state BasicState>,
    {
        self.core()
            .build(states.copied().map(Ok), false, c)
            .map_err(BasicStateError::into_kernel_failure)
    }
}
impl BasicKernel {
    fn core(&self) -> BasicComputation<'_> {
        let FunctionArgumentType::Value(source) =
            &self.contract.call().selected().argument_types[0]
        else {
            unreachable!("validated value argument")
        };
        let FunctionResultType::Scalar(output) = &self.contract.call().selected().result_type
        else {
            unreachable!("validated scalar result")
        };
        BasicComputation {
            operation: self.operation,
            domain: match source.data_type() {
                DataType::Decimal128(..) => BasicStateDomain::Decimal128,
                DataType::Decimal256(..) => BasicStateDomain::Decimal256,
                _ => BasicStateDomain::Plain,
            },
            input_scale: match source.data_type() {
                DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(*s),
                _ => None,
            },
            output_type: output.data_type(),
            intermediate_type: self.contract.intermediate_type().data_type(),
        }
    }
}
/// The carrier-neutral computation shared by selected owners and v1 shells.
/// These references are immutable type facts, never a running capability.
pub struct BasicComputation<'a> {
    pub operation: BasicOperation,
    pub domain: BasicStateDomain,
    pub input_scale: Option<i8>,
    pub output_type: &'a DataType,
    pub intermediate_type: &'a DataType,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BasicStateDomain {
    Plain,
    Decimal128,
    Decimal256,
}
/// Frozen intermediate-state and Arrow construction failures retain their
/// original primitive cause, without retaining input text or schema owners.
#[derive(Debug)]
pub enum BasicStateError {
    Kernel(KernelFailure),
    StateContract(novarocks_failure_contract::BoundedDetail),
    StateDecode {
        source: Box<dyn std::error::Error + Send + Sync>,
        detail: novarocks_failure_contract::BoundedDetail,
    },
    StateArrow(arrow_schema::ArrowError),
}
impl From<KernelFailure> for BasicStateError {
    fn from(source: KernelFailure) -> Self {
        Self::Kernel(source)
    }
}
impl std::fmt::Display for BasicStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kernel(source) => source.fmt(f),
            Self::StateContract(detail) | Self::StateDecode { detail, .. } => detail.fmt(f),
            Self::StateArrow(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for BasicStateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kernel(source) => Some(source),
            Self::StateDecode { source, .. } => Some(source.as_ref()),
            Self::StateArrow(source) => Some(source),
            Self::StateContract(_) => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for BasicStateError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::Kernel(source) => return source.failure_fact(),
            Self::StateContract(_) => (FailureClass::Invariant, 156),
            Self::StateDecode { .. } => (FailureClass::Invariant, 157),
            Self::StateArrow(arrow_schema::ArrowError::MemoryError(_)) => {
                (FailureClass::Resource, 158)
            }
            // This boundary builds fixed primitive/struct state carriers from
            // frozen metadata; it performs no I/O or variable-offset encoding.
            Self::StateArrow(_) => (FailureClass::Invariant, 159),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for BasicStateError {}
impl BasicStateError {
    pub fn into_kernel_failure(self) -> KernelFailure {
        match self {
            Self::Kernel(source) => source,
            source => KernelFailure::from_domain(source),
        }
    }
}
fn state_decode_source<E: std::error::Error + Send + Sync + 'static>(source: E) -> BasicStateError {
    let detail = novarocks_failure_contract::BoundedDetail::truncating_display(&source);
    BasicStateError::StateDecode {
        source: Box::new(source),
        detail,
    }
}
fn contextual_state_decode<E: std::error::Error + Send + Sync + 'static>(
    source: E,
    label: &str,
    component: &str,
    value: &str,
) -> BasicStateError {
    let detail = novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
        "invalid {label} state {component} '{value}': {source}"
    ));
    BasicStateError::StateDecode {
        source: Box::new(source),
        detail,
    }
}
fn observed_core<T>(
    c: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, BasicStateError>,
) -> Result<T, BasicStateError> {
    let observation = KernelControlObservation::new(c);
    observation.checkpoint(0)?;
    let mut w = EvaluationCheckpoints::new(&observation);
    let result = f(&mut w);
    match result {
        Ok(value) => observation
            .finish(w.finish_result(Ok(value)))
            .map_err(Into::into),
        // Every BasicStateError variant is an authored outer failure at all
        // actual consumers. Do not start a new cooperative tail after it.
        Err(original) => match observation.finish(Ok(())) {
            // The original observer can already own an earlier refusal. This
            // reads that latch only; it never calls the controller again.
            Err(earlier) => Err(earlier.into()),
            Ok(()) => Err(original),
        },
    }
}
fn state_error(message: impl std::fmt::Display) -> BasicStateError {
    BasicStateError::StateContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(message),
    )
}
#[derive(Clone, Copy, Debug)]
pub enum BasicValue {
    Boolean(bool),
    Float(f64),
    Decimal128(i128, i8),
    Decimal256(i256, i8),
}
pub fn read_basic_value(
    a: &dyn Array,
    row: usize,
    w: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<BasicValue>, KernelFailure> {
    if row >= a.len() {
        return Err(internal(
            "basic aggregate source address is outside its carrier",
        ));
    }
    w.step()?;
    if a.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match a.data_type() {
        DataType::Boolean => BasicValue::Boolean(down::<BooleanArray>(a)?.value(row)),
        DataType::Decimal128(_, scale) => {
            BasicValue::Decimal128(down::<Decimal128Array>(a)?.value(row), *scale)
        }
        DataType::Decimal256(_, scale) => {
            BasicValue::Decimal256(down::<Decimal256Array>(a)?.value(row), *scale)
        }
        _ => BasicValue::Float(numeric(a, row)?),
    }))
}
impl BasicComputation<'_> {
    pub fn update(
        &self,
        state: &mut BasicState,
        first: Option<BasicValue>,
        second: Option<BasicValue>,
        state_scale: Option<i8>,
        w: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let mut next = *state;
        if self.operation.boolean() {
            next.seen = true;
            if let Some(BasicValue::Boolean(v)) = first {
                next.boolean = if self.operation == BasicOperation::Or {
                    next.boolean || v
                } else {
                    next.boolean && v
                };
            }
        } else if let Some(value) = first {
            if self.operation == BasicOperation::CountIf {
                let BasicValue::Boolean(v) = value else {
                    return Err(internal("COUNT_IF value domain differs"));
                };
                if v {
                    add_count(&mut next, 1)?;
                }
            } else if self.operation == BasicOperation::Avg {
                match value {
                    BasicValue::Float(v) => next.sum += v,
                    BasicValue::Decimal128(mut v, input_scale) => {
                        let target = state_scale.unwrap_or(input_scale);
                        let diff = i32::from(target) - i32::from(input_scale);
                        let factor = 10i128
                            .checked_pow(diff.unsigned_abs())
                            .ok_or_else(numeric_overflow)?;
                        if diff > 0 {
                            v = v.checked_mul(factor).ok_or_else(numeric_overflow)?;
                        } else if diff < 0 {
                            v /= factor;
                        }
                        next.decimal += v;
                    }
                    BasicValue::Decimal256(mut v, input_scale) => {
                        let target = state_scale.unwrap_or(input_scale);
                        let diff = i32::from(target) - i32::from(input_scale);
                        let mut factor = i256::ONE;
                        for _ in 0..diff.unsigned_abs() {
                            factor = factor
                                .checked_mul(i256::from_i128(10))
                                .ok_or_else(numeric_overflow)?;
                            w.step()?;
                        }
                        if diff > 0 {
                            v = v.wrapping_mul(factor);
                        } else if diff < 0 {
                            v = v.checked_div(factor).ok_or_else(numeric_overflow)?;
                        }
                        next.wide = next.wide.wrapping_add(v);
                    }
                    _ => return Err(internal("AVG value domain differs")),
                }
                add_count(&mut next, 1)?;
            } else if self.operation.variance() {
                let BasicValue::Float(value) = value else {
                    return Err(internal("variance value domain differs"));
                };
                let old = next.count;
                add_count(&mut next, 1)?;
                let delta = value - next.x;
                let r = delta / next.count as f64;
                next.x += r;
                next.m2 += old as f64 * delta * r;
            } else if let Some(second) = second {
                let (BasicValue::Float(x), BasicValue::Float(y)) = (value, second) else {
                    return Err(internal("covariance value domain differs"));
                };
                add_count(&mut next, 1)?;
                let old_x = next.x;
                let old_y = next.y;
                next.x = old_x + (x - old_x) / next.count as f64;
                next.y = old_y + (y - old_y) / next.count as f64;
                next.m2 += (x - old_x) * (y - next.y);
                if self.operation == BasicOperation::Corr {
                    next.m2x += (x - old_x) * (x - next.x);
                    next.m2y += (y - old_y) * (y - next.y);
                }
            }
        }
        w.step()?;
        *state = next;
        Ok(())
    }

    pub fn decode_optional(
        &self,
        a: &dyn Array,
        row: usize,
        w: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<BasicState>, BasicStateError> {
        if let DataType::Struct(_) = a.data_type() {
            let a = down::<StructArray>(a)?;
            if a.num_columns() != 2 {
                return Err(state_error("avg decimal256 intermediate expects 2 fields"));
            }
            if a.column(0).is_null(row) || a.column(1).is_null(row) {
                return Ok(None);
            }
        } else if a.is_null(row) {
            return Ok(None);
        }
        self.decode_state(a, row, w).map(Some)
    }
    pub fn decode_state(
        &self,
        a: &dyn Array,
        row: usize,
        w: &mut EvaluationCheckpoints<'_>,
    ) -> Result<BasicState, BasicStateError> {
        let mut s = BasicState::default();
        if self.operation.boolean() {
            s.seen = true;
            s.boolean = down::<BooleanArray>(a)?.value(row);
            w.step()?;
            return Ok(s);
        }
        if self.operation == BasicOperation::CountIf {
            s.count = integer(a, row)?;
            w.step()?;
            return Ok(s);
        }
        match a.data_type() {
            DataType::Utf8 => self.decode_text(down::<StringArray>(a)?.value(row), w),
            DataType::Binary => self.decode_bytes(down::<BinaryArray>(a)?.value(row), w),
            DataType::Struct(_) => {
                let a = down::<StructArray>(a)?;
                let value = read_basic_value(a.column(0).as_ref(), row, w)?;
                s.count = integer(a.column(1).as_ref(), row)?;
                match value {
                    Some(BasicValue::Float(v)) => s.sum = v,
                    Some(BasicValue::Decimal128(v, _)) => s.decimal = v,
                    Some(BasicValue::Decimal256(v, _)) => s.wide = v,
                    _ => return Err(internal("AVG state sum carrier differs").into()),
                };
                Ok(s)
            }
            _ => Err(internal("basic aggregate state carrier differs").into()),
        }
    }
    pub fn decode_text(
        &self,
        text: &str,
        w: &mut EvaluationCheckpoints<'_>,
    ) -> Result<BasicState, BasicStateError> {
        for _ in text.bytes() {
            w.step()?;
        }
        w.flush()?;
        let mut s = BasicState::default();
        if self.operation == BasicOperation::Avg {
            let label = match self.domain {
                BasicStateDomain::Decimal128 => "avg decimal",
                BasicStateDomain::Decimal256 => "avg decimal256",
                _ => "avg",
            };
            let (sum, count) = text.split_once(',').ok_or_else(|| {
                state_error(format_args!(
                    "invalid {label} state '{}': missing ','",
                    text
                ))
            })?;
            match self.domain {
                BasicStateDomain::Decimal128 => {
                    s.decimal = sum
                        .parse()
                        .map_err(|e| contextual_state_decode(e, label, "sum", sum))?
                }
                BasicStateDomain::Decimal256 => {
                    s.wide = sum
                        .parse()
                        .map_err(|e| contextual_state_decode(e, label, "sum", sum))?
                }
                _ => {
                    s.sum = sum
                        .parse()
                        .map_err(|e| contextual_state_decode(e, label, "sum", sum))?
                }
            }
            s.count = count
                .parse()
                .map_err(|e| contextual_state_decode(e, label, "count", count))?;
        } else if self.operation.variance() {
            let mut it = text.split(',');
            s.x = it
                .next()
                .ok_or_else(|| state_error("variance/stddev intermediate utf8 missing mean"))?
                .parse::<f64>()
                .map_err(state_decode_source)?;
            s.m2 = it
                .next()
                .ok_or_else(|| state_error("variance/stddev intermediate utf8 missing m2"))?
                .parse::<f64>()
                .map_err(state_decode_source)?;
            s.count = it
                .next()
                .ok_or_else(|| state_error("variance/stddev intermediate utf8 missing count"))?
                .parse::<i64>()
                .map_err(state_decode_source)?;
        } else {
            let mut parts = text.split(',');
            let expected = if self.operation == BasicOperation::Corr {
                6
            } else {
                4
            };
            if parts.clone().count() != expected {
                return Err(state_error(if expected == 6 {
                    "corr utf8 state expects 6 parts"
                } else {
                    "covar utf8 state expects 4 parts"
                }));
            }
            s.x = parts
                .next()
                .unwrap()
                .parse::<f64>()
                .map_err(state_decode_source)?;
            s.y = parts
                .next()
                .unwrap()
                .parse::<f64>()
                .map_err(state_decode_source)?;
            s.m2 = parts
                .next()
                .unwrap()
                .parse::<f64>()
                .map_err(state_decode_source)?;
            s.count = parts
                .next()
                .unwrap()
                .parse::<i64>()
                .map_err(state_decode_source)?;
            if expected == 6 {
                s.m2x = parts
                    .next()
                    .unwrap()
                    .parse::<f64>()
                    .map_err(state_decode_source)?;
                s.m2y = parts
                    .next()
                    .unwrap()
                    .parse::<f64>()
                    .map_err(state_decode_source)?;
            }
        }
        w.step()?;
        Ok(s)
    }
    pub fn decode_bytes(
        &self,
        bytes: &[u8],
        w: &mut EvaluationCheckpoints<'_>,
    ) -> Result<BasicState, BasicStateError> {
        w.step()?;
        let mut s = BasicState::default();
        if self.operation == BasicOperation::Avg {
            let (width, label) = match self.domain {
                BasicStateDomain::Decimal128 => (24, "avg decimal"),
                BasicStateDomain::Decimal256 => (40, "avg decimal256"),
                _ => (16, "avg"),
            };
            if bytes.len() != width {
                return Err(state_error(format_args!(
                    "invalid {label} binary state length: {}",
                    bytes.len()
                )));
            }
            match width {
                24 => s.decimal = i128::from_le_bytes(bytes[..16].try_into().unwrap()),
                40 => s.wide = i256::from_le_bytes(bytes[..32].try_into().unwrap()),
                _ => s.sum = f64::from_le_bytes(bytes[..8].try_into().unwrap()),
            };
            s.count = i64::from_le_bytes(bytes[width - 8..width].try_into().unwrap());
        } else {
            let (width, label) = if self.operation.variance() {
                (24, "variance/stddev")
            } else if self.operation == BasicOperation::Corr {
                (48, "corr")
            } else {
                (32, "covar")
            };
            if bytes.len() != width {
                return Err(state_error(format_args!(
                    "{label} intermediate binary size mismatch: expected {width}, got {}",
                    bytes.len()
                )));
            }
            let float = |n| f64::from_le_bytes(bytes[n..n + 8].try_into().unwrap());
            s.x = float(0);
            if width == 24 {
                s.m2 = float(8);
                s.count = i64::from_le_bytes(bytes[16..24].try_into().unwrap());
            } else {
                s.y = float(8);
                s.m2 = float(16);
                s.count = i64::from_le_bytes(bytes[24..32].try_into().unwrap());
                if width == 48 {
                    s.m2x = float(32);
                    s.m2y = float(40);
                }
            }
        }
        Ok(s)
    }
    fn decimal_sum_info(&self) -> Result<(u8, i8), BasicStateError> {
        let DataType::Struct(fields) = self.intermediate_type else {
            return Err(state_error(format_args!(
                "avg decimal intermediate type mismatch: {:?}",
                self.intermediate_type
            )));
        };
        if fields.len() != 2 {
            return Err(state_error("avg decimal intermediate expects 2 fields"));
        }
        if !matches!(
            fields[1].data_type(),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ) {
            return Err(state_error(format_args!(
                "avg decimal intermediate count type mismatch: {:?}",
                fields[1].data_type()
            )));
        }
        match fields[0].data_type() {
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Ok((*p, *s)),
            other => Err(state_error(format_args!(
                "avg decimal intermediate sum type mismatch: {:?}",
                other
            ))),
        }
    }
    pub fn state_scale(&self) -> Result<Option<i8>, BasicStateError> {
        if self.domain == BasicStateDomain::Plain {
            return Ok(None);
        }
        if matches!(self.intermediate_type, DataType::Binary | DataType::Utf8) {
            self.input_scale.map(Some).ok_or_else(|| {
                state_error(if self.domain == BasicStateDomain::Decimal128 {
                    "avg decimal arg scale missing"
                } else {
                    "avg decimal256 arg scale missing"
                })
            })
        } else {
            self.decimal_sum_info().map(|(_, s)| Some(s))
        }
    }
    pub fn merge_state(
        &self,
        s: &mut BasicState,
        incoming: BasicState,
    ) -> Result<(), KernelFailure> {
        if self.operation.boolean() {
            if incoming.seen {
                s.seen = true;
                s.boolean = if self.operation == BasicOperation::Or {
                    s.boolean || incoming.boolean
                } else {
                    s.boolean && incoming.boolean
                };
            }
            return Ok(());
        }
        if self.operation == BasicOperation::CountIf {
            return add_count(s, incoming.count);
        }
        if self.operation == BasicOperation::Avg {
            s.sum += incoming.sum;
            s.decimal += incoming.decimal;
            s.wide = s.wide.wrapping_add(incoming.wide);
            return add_count(s, incoming.count);
        }
        if self.operation.variance() && incoming.count <= 0
            || !self.operation.variance() && incoming.count == 0
        {
            return Ok(());
        }
        if s.count == 0 {
            *s = incoming;
            return Ok(());
        }
        if self.operation.variance() {
            let delta = s.x - incoming.x;
            let count_s = s.count as f64;
            let count_in = incoming.count as f64;
            let sum = count_s + count_in;
            s.x = incoming.x + delta * (count_s / sum);
            s.m2 = incoming.m2 + s.m2 + (delta * delta) * (count_in * count_s / sum);
            add_count(s, incoming.count)?;
        } else {
            let dx = s.x - incoming.x;
            let dy = s.y - incoming.y;
            let count = s.count + incoming.count;
            let factor = s.count as f64 * incoming.count as f64 / count as f64;
            s.x = incoming.x + dx * (s.count as f64 / count as f64);
            s.y = incoming.y + dy * (s.count as f64 / count as f64);
            s.m2 = incoming.m2 + s.m2 + dx * dy * factor;
            if self.operation == BasicOperation::Corr {
                s.m2x = incoming.m2x + s.m2x + dx * dx * factor;
                s.m2y = incoming.m2y + s.m2y + dy * dy * factor;
            }
            s.count = count;
        }
        Ok(())
    }
    fn float_result(&self, s: &BasicState) -> Option<f64> {
        use BasicOperation::*;
        match self.operation {
            Avg => (s.count != 0).then(|| s.sum / s.count as f64),
            VarPop | StdPop => (s.count != 0).then(|| {
                let v = s.m2 / s.count as f64;
                if self.operation == StdPop {
                    v.sqrt()
                } else {
                    v
                }
            }),
            VarSamp | StdSamp => (s.count > 1).then(|| {
                let v = s.m2 / (s.count - 1) as f64;
                if self.operation == StdSamp {
                    v.sqrt()
                } else {
                    v
                }
            }),
            CovPop => (s.count != 0).then(|| s.m2 / s.count as f64),
            CovSamp => (s.count > 1).then(|| s.m2 / (s.count as f64 - 1.0)),
            Corr => (!(s.count < 2 || s.m2x <= 0.0 || s.m2y <= 0.0))
                .then(|| s.m2 / s.m2x.sqrt() / s.m2y.sqrt()),
            _ => None,
        }
    }
    pub fn build<I>(
        &self,
        states: I,
        intermediate: bool,
        c: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, BasicStateError>
    where
        I: ExactSizeIterator<Item = Result<BasicState, KernelFailure>>,
    {
        observed_core(c, |w| {
            let rows = states.len();
            // Arrow variable-width buffers use signed 32-bit offsets. The
            // owner emits at most 384 bytes per textual AVG state and 48
            // bytes per binary moment state; admit those exact upper bounds
            // before any opaque Arrow construction.
            if intermediate
                && !self.operation.boolean()
                && self.operation != BasicOperation::CountIf
            {
                let width = if self.operation == BasicOperation::Avg {
                    384
                } else if matches!(self.intermediate_type, DataType::Utf8) {
                    2048
                } else {
                    48
                };
                let bytes = rows
                    .checked_mul(width)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                if bytes > i32::MAX as usize {
                    return Err(KernelFailure::ResourceExhausted.into());
                }
            }
            Layout::array::<BasicState>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
            w.flush()?;
            let mut snapshot = Vec::new();
            snapshot
                .try_reserve_exact(rows)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            w.flush()?;
            for s in states {
                if snapshot.len() == rows {
                    return Err(
                        internal("basic aggregate emission exceeds its exact extent").into(),
                    );
                }
                snapshot.push(s?);
                w.step()?;
            }
            if snapshot.len() != rows {
                return Err(internal("basic aggregate emission changed its exact extent").into());
            }
            w.flush()?;

            let output: ArrayRef = if self.operation.boolean() {
                let values = collect(&snapshot, w, |s| Ok(s.seen.then_some(s.boolean)))?;
                Arc::new(BooleanArray::from(values))
            } else if self.operation == BasicOperation::CountIf {
                let values = collect(&snapshot, w, |s| Ok(s.count))?;
                Arc::new(Int64Array::from(values))
            } else if intermediate && matches!(self.intermediate_type, DataType::Utf8) {
                let values = collect(&snapshot, w, |s| {
                    Ok(if s.count == 0 {
                        None
                    } else {
                        Some(if self.operation == BasicOperation::Avg {
                            match self.domain {
                                BasicStateDomain::Decimal128 => {
                                    format!("{},{}", s.decimal, s.count)
                                }
                                BasicStateDomain::Decimal256 => format!("{},{}", s.wide, s.count),
                                _ => format!("{},{}", s.sum, s.count),
                            }
                        } else if self.operation.variance() {
                            format!("{},{},{}", s.x, s.m2, s.count)
                        } else if self.operation == BasicOperation::Corr {
                            format!("{},{},{},{},{},{}", s.x, s.y, s.m2, s.count, s.m2x, s.m2y)
                        } else {
                            format!("{},{},{},{}", s.x, s.y, s.m2, s.count)
                        })
                    })
                })?;
                Arc::new(StringArray::from(values))
            } else if intermediate && matches!(self.intermediate_type, DataType::Binary) {
                let mut builder = arrow_array::builder::BinaryBuilder::new();
                for s in &snapshot {
                    w.flush()?;
                    if s.count == 0 {
                        builder.append_null();
                    } else {
                        let mut bytes = [0u8; 48];
                        let len = if self.operation == BasicOperation::Avg {
                            match self.domain {
                                BasicStateDomain::Decimal128 => {
                                    bytes[..16].copy_from_slice(&s.decimal.to_le_bytes());
                                    bytes[16..24].copy_from_slice(&s.count.to_le_bytes());
                                    24
                                }
                                BasicStateDomain::Decimal256 => {
                                    bytes[..32].copy_from_slice(&s.wide.to_le_bytes());
                                    bytes[32..40].copy_from_slice(&s.count.to_le_bytes());
                                    40
                                }
                                _ => {
                                    bytes[..8].copy_from_slice(&s.sum.to_le_bytes());
                                    bytes[8..16].copy_from_slice(&s.count.to_le_bytes());
                                    16
                                }
                            }
                        } else {
                            bytes[..8].copy_from_slice(&s.x.to_le_bytes());
                            if self.operation.variance() {
                                bytes[8..16].copy_from_slice(&s.m2.to_le_bytes());
                                bytes[16..24].copy_from_slice(&s.count.to_le_bytes());
                                24
                            } else {
                                bytes[8..16].copy_from_slice(&s.y.to_le_bytes());
                                bytes[16..24].copy_from_slice(&s.m2.to_le_bytes());
                                bytes[24..32].copy_from_slice(&s.count.to_le_bytes());
                                if self.operation == BasicOperation::Corr {
                                    bytes[32..40].copy_from_slice(&s.m2x.to_le_bytes());
                                    bytes[40..48].copy_from_slice(&s.m2y.to_le_bytes());
                                    48
                                } else {
                                    32
                                }
                            }
                        };
                        builder.append_value(&bytes[..len]);
                    }
                    w.flush()?;
                }
                Arc::new(builder.finish())
            } else if intermediate && self.operation == BasicOperation::Avg {
                let sum: ArrayRef = match self.domain {
                    BasicStateDomain::Decimal128 => {
                        let (p, s) = self.decimal_sum_info()?;
                        Arc::new(
                            Decimal128Array::from(collect(&snapshot, w, |s| {
                                Ok((s.count != 0).then_some(s.decimal))
                            })?)
                            .with_precision_and_scale(p, s)
                            .map_err(BasicStateError::StateArrow)?,
                        )
                    }
                    BasicStateDomain::Decimal256 => {
                        let (p, s) = self.decimal_sum_info()?;
                        Arc::new(
                            Decimal256Array::from(collect(&snapshot, w, |s| {
                                Ok((s.count != 0).then_some(s.wide))
                            })?)
                            .with_precision_and_scale(p, s)
                            .map_err(BasicStateError::StateArrow)?,
                        )
                    }
                    _ => Arc::new(Float64Array::from(collect(&snapshot, w, |s| {
                        Ok((s.count != 0).then_some(s.sum))
                    })?)),
                };
                let count: ArrayRef = Arc::new(Int64Array::from(collect(&snapshot, w, |s| {
                    Ok((s.count != 0).then_some(s.count))
                })?));
                let fields = vec![
                    arrow_schema::Field::new("sum", sum.data_type().clone(), true),
                    arrow_schema::Field::new("count", DataType::Int64, true),
                ]
                .into();
                Arc::new(
                    StructArray::try_new(fields, vec![sum, count], None)
                        .map_err(BasicStateError::StateArrow)?,
                )
            } else if intermediate {
                return Err(state_error(if self.operation.variance() {
                    "variance/stddev intermediate type mismatch"
                } else {
                    "covar/corr intermediate type mismatch"
                }));
            } else if self.operation == BasicOperation::Avg
                && self.domain == BasicStateDomain::Decimal128
            {
                let DataType::Decimal128(precision, scale) = *self.output_type else {
                    unreachable!()
                };
                let diff =
                    i32::from(scale) - i32::from(self.state_scale()?.expect("decimal state scale"));
                let factor = 10i128
                    .checked_pow(diff.unsigned_abs())
                    .ok_or_else(numeric_overflow)?;
                let values = collect(&snapshot, w, |s| {
                    if s.count == 0 {
                        return Ok(None);
                    }
                    let sum = if diff > 0 {
                        s.decimal.checked_mul(factor).ok_or_else(numeric_overflow)?
                    } else if diff < 0 {
                        s.decimal / factor
                    } else {
                        s.decimal
                    };
                    Ok(Some(round_i128(sum, s.count)))
                })?;
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision, scale)
                        .map_err(BasicStateError::StateArrow)?,
                )
            } else if self.operation == BasicOperation::Avg
                && self.domain == BasicStateDomain::Decimal256
            {
                let DataType::Decimal256(precision, scale) = self.output_type else {
                    unreachable!()
                };
                let input_scale = self.state_scale()?.expect("decimal state scale");
                let diff = i32::from(*scale) - i32::from(input_scale);
                let mut factor = i256::ONE;
                for _ in 0..diff.unsigned_abs() {
                    factor = factor
                        .checked_mul(i256::from_i128(10))
                        .ok_or_else(numeric_overflow)?;
                    w.step()?;
                }
                let values = collect(&snapshot, w, |s| {
                    if s.count == 0 {
                        Ok(None)
                    } else {
                        let value = if diff > 0 {
                            s.wide.wrapping_mul(factor)
                        } else if diff < 0 {
                            s.wide.checked_div(factor).ok_or_else(numeric_overflow)?
                        } else {
                            s.wide
                        };
                        round_i256(value, s.count).map(Some).map_err(Into::into)
                    }
                })?;
                Arc::new(
                    Decimal256Array::from(values)
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(BasicStateError::StateArrow)?,
                )
            } else {
                let values = collect(&snapshot, w, |s| Ok(self.float_result(s)))?;
                Arc::new(Float64Array::from(values))
            };
            w.flush()?;
            Ok(output)
        })
    }
}
fn collect<T>(
    states: &[BasicState],
    w: &mut EvaluationCheckpoints<'_>,
    f: impl Fn(&BasicState) -> Result<T, BasicStateError>,
) -> Result<Vec<T>, BasicStateError> {
    Layout::array::<T>(states.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    w.flush()?;
    let mut v = Vec::new();
    v.try_reserve_exact(states.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    w.flush()?;
    for s in states {
        v.push(f(s)?);
        w.step()?;
    }
    w.flush()?;
    Ok(v)
}
fn round_i128(sum: i128, count: i64) -> i128 {
    let divisor = count as i128;
    let mut q = sum / divisor;
    let r = sum % divisor;
    let abs_b = divisor.abs();
    let threshold = (abs_b >> 1) + (abs_b & 1);
    if r.abs() >= threshold {
        q += if (sum ^ divisor) < 0 { -1 } else { 1 };
    }
    q
}
fn round_i256(sum: i256, count: i64) -> Result<i256, KernelFailure> {
    let divisor = i256::from_i128(count as i128);
    let overflow = || numeric_overflow();
    let mut q = sum.checked_div(divisor).ok_or_else(overflow)?;
    let r = sum.checked_rem(divisor).ok_or_else(overflow)?;
    if r == i256::ZERO {
        return Ok(q);
    }
    let abs_b = if divisor.is_negative() {
        divisor.checked_neg().ok_or_else(overflow)?
    } else {
        divisor
    };
    let abs_r = if r.is_negative() {
        r.checked_neg().ok_or_else(overflow)?
    } else {
        r
    };
    let threshold = (abs_b >> 1_u32)
        .checked_add(abs_b & i256::ONE)
        .ok_or_else(overflow)?;
    if abs_r >= threshold {
        let carry = if sum.is_negative() ^ divisor.is_negative() {
            i256::MINUS_ONE
        } else {
            i256::ONE
        };
        q = q.checked_add(carry).ok_or_else(overflow)?;
    }
    Ok(q)
}

#[cfg(test)]
#[path = "aggregate_basic_tests.rs"]
mod tests;
