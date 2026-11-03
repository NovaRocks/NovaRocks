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

//! ONE original scalar HLL state computation over explicit evaluated addresses.
use crate::aggregate_scalar::{
    ScalarReadFailure, ScalarReadFailureSink, ScalarReadObservation, scalar_from_array_with_failure,
};
use crate::datasketches_hll::{HllAccumulator, HllTargetType};
use crate::datasketches_hll_failure::{
    HllFailureRecipe, HllFailureSink, HllInvariantRecipe, HllObservation,
};
use crate::sketch_hash::{
    SketchHashFailure, SketchHashFailureSink, prehash_array_value_with_failure,
};
use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, builder::BinaryBuilder,
};
use arrow_schema::DataType;
use std::{fmt, sync::Arc};
type Error<F> = <F as HllFailureSink>::Error;
/// A type refusal borrows only the declaration, never a materialized value.
pub enum TuningFailure<'a> {
    LogInteger {
        value: i64,
        source: std::num::TryFromIntError,
    },
    LogRange(u8),
    LogFloat(f64),
    LogType(&'a DataType),
    TargetType(&'a DataType),
}
impl fmt::Display for TuningFailure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogInteger { value: v, .. } => {
                write!(f, "ds_hll_count_distinct_state log_k out of range: {}", v)
            }
            Self::LogRange(v) => write!(
                f,
                "ds_hll_count_distinct_state log_k must be in [4, 21], got {}",
                v
            ),
            Self::LogFloat(v) => write!(
                f,
                "ds_hll_count_distinct_state log_k must be in [4, 21], got {}",
                v
            ),
            Self::LogType(v) => write!(
                f,
                "ds_hll_count_distinct_state log_k expects numeric input, got {v:?}"
            ),
            Self::TargetType(v) => write!(
                f,
                "ds_hll_count_distinct_state target type expects string input, got {v:?}"
            ),
        }
    }
}
#[derive(Debug)]
enum TuningValueCause {
    Integer {
        value: i64,
        source: std::num::TryFromIntError,
    },
    Range(u8),
    Float(f64),
}
#[derive(Debug)]
pub struct TuningValueFailure(TuningValueCause);
#[derive(Debug)]
struct TuningContractFailure {
    target: bool,
    detail: novarocks_failure_contract::BoundedDetail,
}
#[derive(Debug)]
pub enum ClassifiedTuningFailure {
    Value(TuningValueFailure),
    Contract(crate::KernelFailure),
}
impl TuningFailure<'_> {
    pub fn classify(self) -> ClassifiedTuningFailure {
        let value = match self {
            Self::LogInteger { value, source } => TuningValueCause::Integer { value, source },
            Self::LogRange(value) => TuningValueCause::Range(value),
            Self::LogFloat(value) => TuningValueCause::Float(value),
            Self::LogType(_) | Self::TargetType(_) => {
                return ClassifiedTuningFailure::Contract(crate::KernelFailure::from_domain(
                    TuningContractFailure {
                        target: matches!(self, Self::TargetType(_)),
                        detail: novarocks_failure_contract::BoundedDetail::truncating_display(
                            &self,
                        ),
                    },
                ));
            }
        };
        ClassifiedTuningFailure::Value(TuningValueFailure(value))
    }
}
impl TuningValueFailure {
    pub const fn reason(&self) -> crate::DataFailureReason {
        crate::DataFailureReason::NumericOutOfRange
    }
}
impl fmt::Display for TuningValueFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            TuningValueCause::Integer { value, .. } => {
                write!(f, "ds_hll_count_distinct_state log_k out of range: {value}")
            }
            TuningValueCause::Range(value) => write!(
                f,
                "ds_hll_count_distinct_state log_k must be in [4, 21], got {value}"
            ),
            TuningValueCause::Float(value) => write!(
                f,
                "ds_hll_count_distinct_state log_k must be in [4, 21], got {value}"
            ),
        }
    }
}
impl std::error::Error for TuningValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            TuningValueCause::Integer { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for TuningValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.reason())
    }
}
impl fmt::Display for TuningContractFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail.as_str())
    }
}
impl std::error::Error for TuningContractFailure {}
impl novarocks_failure_contract::FailureProjection for TuningContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(
                domain::KERNEL,
                std::num::NonZeroU16::new(if self.target { 311 } else { 310 }).unwrap(),
            ),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for TuningContractFailure {}
#[derive(Debug)]
pub enum DsHllStateValueFailure {
    Hll(crate::datasketches_hll_failure::HllValueFailure),
    Tuning(TuningValueFailure),
}
impl DsHllStateValueFailure {
    pub fn reason(&self) -> crate::DataFailureReason {
        match self {
            Self::Hll(source) => source.reason(),
            Self::Tuning(source) => source.reason(),
        }
    }
}
impl fmt::Display for DsHllStateValueFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hll(source) => source.fmt(f),
            Self::Tuning(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for DsHllStateValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Hll(source) => Some(source),
            Self::Tuning(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for DsHllStateValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.reason())
    }
}
#[derive(Debug)]
pub enum DsHllStateFailure {
    Value(DsHllStateValueFailure),
    Kernel(crate::KernelFailure),
}
impl fmt::Display for DsHllStateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(source) => source.fmt(f),
            Self::Kernel(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for DsHllStateFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Value(source) => Some(source),
            Self::Kernel(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for DsHllStateFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Value(source) => source.failure_fact(),
            Self::Kernel(source) => source.failure_fact(),
        }
    }
}
impl From<crate::KernelFailure> for DsHllStateFailure {
    fn from(source: crate::KernelFailure) -> Self {
        Self::Kernel(source)
    }
}
#[derive(Debug)]
struct StateBindingFailure {
    arity: Option<usize>,
}
impl fmt::Display for StateBindingFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.arity {
            Some(n) => write!(
                f,
                "ds_hll_count_distinct_state requires one to three arguments, got {n}"
            ),
            None => f.write_str("ds_hll_count_distinct_state requires its frozen Binary result"),
        }
    }
}
impl std::error::Error for StateBindingFailure {}
impl novarocks_failure_contract::FailureProjection for StateBindingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(
                domain::KERNEL,
                std::num::NonZeroU16::new(if self.arity.is_some() { 308 } else { 309 }).unwrap(),
            ),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for StateBindingFailure {}
pub fn validate_raw_call(
    arity: usize,
    result: Option<&arrow_schema::DataType>,
) -> Result<(), crate::KernelFailure> {
    if !(1..=3).contains(&arity) {
        return Err(crate::KernelFailure::from_domain(StateBindingFailure {
            arity: Some(arity),
        }));
    }
    if result != Some(&arrow_schema::DataType::Binary) {
        return Err(crate::KernelFailure::from_domain(StateBindingFailure {
            arity: None,
        }));
    }
    Ok(())
}

pub trait ScalarStateSink:
    Sized
    + HllFailureSink
    + SketchHashFailureSink<Error = Error<Self>>
    + ScalarReadFailureSink<Error = Error<Self>>
{
    fn tuning(&mut self, recipe: TuningFailure<'_>) -> Error<Self>;
}
/// Only an actual host creates a reservation; the original legacy adapter has
/// no new authority and keeps its original unreserved allocations.
pub trait ScalarSketchPort<F: ScalarStateSink> {
    type Reservation;
    fn reserve(&self, bytes: usize, sink: &mut F) -> Result<Self::Reservation, Error<F>>;
    fn reconcile(
        &mut self,
        bytes: usize,
        reservation: &mut Self::Reservation,
        sink: &mut F,
    ) -> Result<(), Error<F>>;
}
/// Field order destroys original payload and accumulator before releasing authority.
pub struct ProducedState<R, P> {
    bytes: Vec<u8>,
    _accumulator: HllAccumulator,
    _reservation: R,
    _port: P,
}
impl<R, P> ProducedState<R, P> {
    pub fn payload(&self) -> &[u8] {
        &self.bytes
    }
}
/// Wrong-type NULL tuning retains the existing scalar-reader NULL contract.
/// Non-NULL type refusals never copy nested payload merely to reject its kind.
fn null_tuning<F: ScalarStateSink>(
    array: &ArrayRef,
    row: usize,
    sink: &mut F,
) -> Result<bool, Error<F>> {
    if array.data_type() == &DataType::Null || array.is_null(row) {
        return match scalar_from_array_with_failure(array, row, sink)? {
            None => Ok(true),
            Some(_) => Err(sink.read_contract(ScalarReadFailure::Static(
                "NULL tuning view disagrees with its scalar carrier",
            ))),
        };
    }
    Ok(false)
}
fn log_k<F: ScalarStateSink>(array: &ArrayRef, row: usize, sink: &mut F) -> Result<u8, Error<F>> {
    HllFailureSink::observe(sink, HllObservation::Step)?;
    enum Number {
        Integer(i64),
        Float(f64),
    }
    macro_rules! read {
        ($array:ty, $kind:ident, $convert:expr) => {{
            let values = array.as_any().downcast_ref::<$array>().ok_or_else(|| {
                sink.read_contract(ScalarReadFailure::Static(concat!(
                    "failed to downcast to ",
                    stringify!($array)
                )))
            })?;
            if values.is_null(row) {
                None
            } else {
                Some(Number::$kind(($convert)(values.value(row))))
            }
        }};
    }
    let value = match array.data_type() {
        DataType::Null => None,
        DataType::Int8 => read!(Int8Array, Integer, i64::from),
        DataType::Int16 => read!(Int16Array, Integer, i64::from),
        DataType::Int32 => read!(Int32Array, Integer, i64::from),
        DataType::Int64 => read!(Int64Array, Integer, |v| v),
        DataType::Float32 => read!(Float32Array, Float, f64::from),
        DataType::Float64 => read!(Float64Array, Float, |v| v),
        other => {
            if null_tuning(array, row, sink)? {
                return Ok(17);
            }
            return Err(sink.tuning(TuningFailure::LogType(other)));
        }
    };
    match value {
        Some(Number::Integer(v)) => {
            let lg = u8::try_from(v)
                .map_err(|source| sink.tuning(TuningFailure::LogInteger { value: v, source }))?;
            if !(4..=21).contains(&lg) {
                return Err(sink.tuning(TuningFailure::LogRange(lg)));
            }
            Ok(lg)
        }
        Some(Number::Float(v)) => {
            let lg = v as u8;
            if !(4..=21).contains(&lg) {
                return Err(sink.tuning(TuningFailure::LogFloat(v)));
            }
            Ok(lg)
        }
        None => Ok(17),
    }
}
fn target<F: ScalarStateSink>(
    array: &ArrayRef,
    row: usize,
    sink: &mut F,
) -> Result<HllTargetType, Error<F>> {
    HllFailureSink::observe(sink, HllObservation::Step)?;
    if array.data_type() != &DataType::Utf8 {
        if null_tuning(array, row, sink)? {
            return Ok(HllTargetType::Hll6);
        }
        return Err(sink.tuning(TuningFailure::TargetType(array.data_type())));
    }
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            sink.read_contract(ScalarReadFailure::Static(
                "failed to downcast to StringArray",
            ))
        })?;
    if values.is_null(row) {
        return Ok(HllTargetType::Hll6);
    }
    let value = values.value(row);
    // Preserve the public target hint semantics. Comparing the finite tokens
    // does not require copying/uppercasing the complete caller-owned string.
    Ok(if value.eq_ignore_ascii_case("HLL_4") {
        HllTargetType::Hll4
    } else if value.eq_ignore_ascii_case("HLL_8") {
        HllTargetType::Hll8
    } else {
        HllTargetType::Hll6
    })
}
pub fn produce_row<F: ScalarStateSink, P: ScalarSketchPort<F>>(
    values: (&ArrayRef, usize),
    log: Option<(&ArrayRef, usize)>,
    target_input: Option<(&ArrayRef, usize)>,
    mut port: P,
    sink: &mut F,
) -> Result<Option<ProducedState<P::Reservation, P>>, Error<F>> {
    let Some(hash) = prehash_array_value_with_failure(values.0, values.1, sink)? else {
        return Ok(None);
    };
    let lg = match log {
        Some((a, r)) => log_k(a, r, sink)?,
        None => 17,
    };
    let target = match target_input {
        Some((a, r)) => target(a, r, sink)?,
        None => HllTargetType::Hll6,
    };
    let preflight = HllAccumulator::new_allocation_preflight_with_failure(lg, target, sink)?;
    let mut reservation = port.reserve(preflight.bounds().operation_peak_bytes, sink)?;
    let (mut accumulator, outcome) =
        HllAccumulator::new_under_reservation_with_failure(&preflight, &reservation, sink)?;
    port.reconcile(
        outcome
            .current_bytes
            .saturating_sub(std::mem::size_of::<HllAccumulator>()),
        &mut reservation,
        sink,
    )?;
    HllFailureSink::observe(sink, HllObservation::OpaqueBoundary)?;
    drop(reservation);
    let preflight = accumulator.update_hash_allocation_preflight_with_failure(sink)?;
    let mut reservation = port.reserve(preflight.bounds().additional_headroom_bytes(), sink)?;
    let outcome = accumulator.update_hash_under_reservation_with_failure(
        hash,
        &preflight,
        &reservation,
        sink,
    )?;
    port.reconcile(
        outcome
            .current_bytes
            .saturating_sub(std::mem::size_of::<HllAccumulator>()),
        &mut reservation,
        sink,
    )?;
    HllFailureSink::observe(sink, HllObservation::OpaqueBoundary)?;
    drop(reservation);
    let preflight = accumulator.serialization_allocation_preflight_observed(&mut || {
        HllFailureSink::observe(sink, HllObservation::Step)
    })?;
    let reservation = port.reserve(preflight.bounds().additional_headroom_bytes, sink)?;
    let bytes =
        accumulator.serialize_under_reservation_with_failure(&preflight, &reservation, sink)?;
    Ok(Some(ProducedState {
        bytes,
        _accumulator: accumulator,
        _reservation: reservation,
        _port: port,
    }))
}
struct RawSink;
impl HllFailureSink for RawSink {
    type Error = DsHllStateFailure;
    fn failure(&mut self, r: HllFailureRecipe) -> DsHllStateFailure {
        match r.classify() {
            crate::datasketches_hll_failure::ClassifiedHllFailure::Value(value) => {
                DsHllStateFailure::Value(DsHllStateValueFailure::Hll(value))
            }
            crate::datasketches_hll_failure::ClassifiedHllFailure::Outer(cause) => {
                DsHllStateFailure::Kernel(crate::KernelFailure::from_domain(cause))
            }
        }
    }
    fn invariant(&mut self, r: HllInvariantRecipe) -> DsHllStateFailure {
        DsHllStateFailure::Kernel(crate::KernelFailure::from_domain(r))
    }
    fn observe(&mut self, _: HllObservation) -> Result<(), DsHllStateFailure> {
        Ok(())
    }
}
impl SketchHashFailureSink for RawSink {
    type Error = DsHllStateFailure;
    fn hash_contract(&mut self, r: SketchHashFailure<'_>) -> DsHllStateFailure {
        DsHllStateFailure::Kernel(r.into_kernel("ds_hll_count_distinct_state"))
    }
    fn observe_hash(&mut self, _: HllObservation) -> Result<(), DsHllStateFailure> {
        Ok(())
    }
}
impl ScalarReadFailureSink for RawSink {
    type Error = DsHllStateFailure;
    fn read_contract(&mut self, r: ScalarReadFailure<'_>) -> DsHllStateFailure {
        DsHllStateFailure::Kernel(r.into_kernel())
    }
    fn observe(&mut self, _: ScalarReadObservation) -> Result<(), DsHllStateFailure> {
        Ok(())
    }
    fn reserve_scalar_copy(
        &mut self,
        _: usize,
        _: usize,
        _: usize,
    ) -> Result<(), DsHllStateFailure> {
        Ok(())
    }
}
impl ScalarStateSink for RawSink {
    fn tuning(&mut self, r: TuningFailure<'_>) -> DsHllStateFailure {
        match r.classify() {
            ClassifiedTuningFailure::Value(value) => {
                DsHllStateFailure::Value(DsHllStateValueFailure::Tuning(value))
            }
            ClassifiedTuningFailure::Contract(cause) => DsHllStateFailure::Kernel(cause),
        }
    }
}
struct RawUnreservedPort;
impl ScalarSketchPort<RawSink> for RawUnreservedPort {
    type Reservation = ();
    fn reserve(&self, _: usize, _: &mut RawSink) -> Result<(), DsHllStateFailure> {
        Ok(())
    }
    fn reconcile(
        &mut self,
        _: usize,
        _: &mut (),
        _: &mut RawSink,
    ) -> Result<(), DsHllStateFailure> {
        Ok(())
    }
}
pub fn evaluate(
    values: &ArrayRef,
    log_ks: Option<ArrayRef>,
    target_types: Option<ArrayRef>,
) -> Result<ArrayRef, DsHllStateFailure> {
    let mut builder = BinaryBuilder::new();
    let mut extent = super::aggregate_ds_hll_failure::DsHllOutputTally::default();
    for row in 0..values.len() {
        let produced = produce_row(
            (values, row),
            log_ks.as_ref().map(|a| (a, row)),
            target_types.as_ref().map(|a| (a, row)),
            RawUnreservedPort,
            &mut RawSink,
        )?;
        match produced {
            None => builder.append_null(),
            Some(produced) => {
                extent.append(produced.payload().len())?;
                builder.append_value(produced.payload());
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
