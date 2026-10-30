// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{
    KeyInput, KeySpec, NullPolicy, PreparedKeyBatch, checkpoint_at, invariant, original, shared,
    vector,
};
use crate::{
    exec::{
        hash_table::hash::{canonical_f32_bits, canonical_f64_bits},
        pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit},
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_value_arrays::{
    ValueViolation,
    batch::{
        hash::{ByteHashKernel, HashOptions, HashScratch, HashSeed, hash_into},
        selected::visit_selected,
    },
    hash_eq::NullHash,
};
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3;

struct PolicyData {
    seed: u64,
    spec: KeySpec,
    _storage: ValueTypeStorageLifetime,
}
/// A live instance-local policy. Copies share the same policy owner; even equal
/// seeds in independently created policies cannot authorize saved hash reuse.
#[derive(Clone)]
pub(crate) struct LocalHashPolicy(Arc<PolicyData>);
impl LocalHashPolicy {
    pub(crate) fn new(spec: &KeySpec, owner: &dyn InputConstructionOwner) -> ExecutionResult<Self> {
        Self::with_seed(spec, rand::random(), owner)
    }
    pub(super) fn with_seed(
        spec: &KeySpec,
        seed: u64,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        Ok(Self(shared(
            owner,
            PolicyData {
                seed,
                spec: spec.clone(),
                _storage: owner.storage_lifetime(),
            },
        )?))
    }
    pub(super) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(super) fn seed(&self) -> u64 {
        self.0.seed
    }
    pub(super) fn accepts(&self, spec: &KeySpec) -> bool {
        self.0.spec.same_instance(spec)
    }
}
#[derive(Clone, Copy)]
pub(super) struct Xxh3Kernel;
#[cfg(test)]
std::thread_local! {
    pub(crate) static BYTE_HASH_INITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
impl ByteHashKernel for Xxh3Kernel {
    type Seed = u64;
    type State = Xxh3;
    fn init(&self, seed: u64) -> Xxh3 {
        #[cfg(test)]
        BYTE_HASH_INITS.with(|count| count.set(count.get() + 1));
        Xxh3::with_seed(seed)
    }
    fn update(&self, state: &mut Xxh3, bytes: &[u8]) {
        state.update(bytes);
    }
    fn finish(&self, state: Xxh3) -> u64 {
        state.digest()
    }
}
/// Selection-occurrence aligned hashes and match participation. NULL and a
/// failed/incomplete operation cannot be inferred from an unwritten hash slot.
pub(crate) struct LocalHashBatch {
    input: KeyInput,
    policy: LocalHashPolicy,
    hashes: Vec<u64>,
    validity: Vec<u8>,
    _storage: ValueTypeStorageLifetime,
}
impl LocalHashBatch {
    #[cfg(test)]
    pub(super) fn with_hashes_for_test(mut self, hashes: &[u64]) -> Self {
        assert_eq!(self.hashes.len(), hashes.len());
        self.hashes.copy_from_slice(hashes);
        self
    }
    #[cfg(test)]
    pub(crate) fn force_hash(mut self, value: u64) -> Self {
        self.hashes.fill(value);
        self
    }
    pub(crate) fn len(&self) -> usize {
        self.hashes.len()
    }
    pub(crate) fn hash(&self, occurrence: usize) -> ExecutionResult<u64> {
        self.hashes
            .get(occurrence)
            .copied()
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
    pub(crate) fn participates(&self, occurrence: usize) -> ExecutionResult<bool> {
        self.validity
            .get(occurrence)
            .map(|&v| v != 0)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
    pub(crate) fn validate(
        &self,
        input: &KeyInput,
        policy: &LocalHashPolicy,
    ) -> ExecutionResult<()> {
        if !self.input.same_instance(input) || !self.policy.same_instance(policy) {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(())
    }
    pub(crate) fn input(&self) -> &KeyInput {
        &self.input
    }
}

pub(crate) fn hash_batch(
    prepared: &PreparedKeyBatch<'_>,
    policy: &LocalHashPolicy,
    scratch: &mut HashScratch,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<LocalHashBatch> {
    checkpoint()?;
    if !policy.accepts(&prepared.spec) {
        return Err(invariant(ValueViolation::Layout));
    }
    let rows = prepared.input.unique_rows();
    let mut combined = vector(owner, rows.len())?;
    let mut valid = vector(owner, rows.len())?;
    let initial = digest_bytes(policy.seed(), &[], checkpoint)?;
    for i in 0..rows.len() {
        checkpoint_at(i, checkpoint)?;
        combined.push(initial);
        valid.push(1u8);
    }
    let mut component = vector(owner, rows.len())?;
    for i in 0..rows.len() {
        checkpoint_at(i, checkpoint)?;
        component.push(0);
    }
    for column in 0..prepared.spec.types().len() {
        checkpoint()?;
        if let Some(bytes) = prepared.bytes(column)? {
            hash_into(
                &bytes,
                &Xxh3Kernel,
                HashOptions {
                    seeds: HashSeed::Uniform(policy.seed()),
                    nulls: NullHash::Value(null_hash(policy.seed())),
                },
                &mut component,
                scratch,
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| checkpoint(),
            )
            .map_err(original)?;
            if prepared.spec.nulls()[column] == NullPolicy::Unmatchable {
                visit_selected(
                    &bytes,
                    &mut |ordinal, _, value| {
                        if value.is_none() {
                            valid[ordinal] = 0;
                        }
                        Ok(())
                    },
                    &mut |_| checkpoint(),
                )
                .map_err(original)?;
            }
        } else if super::prepared::is_fixed(prepared.spec.types()[column].data_type()) {
            fixed_hashes(
                prepared,
                column,
                policy.seed(),
                &mut component,
                &mut valid,
                checkpoint,
            )?;
        } else {
            complex_hashes(
                prepared,
                column,
                policy.seed(),
                &mut component,
                &mut valid,
                checkpoint,
            )?;
        }

        for (i, (&part, value)) in component.iter().zip(&mut combined).enumerate() {
            checkpoint_at(i, checkpoint)?;
            if prepared.spec.types().len() == 1 {
                *value = part;
            } else {
                let mut state = Xxh3::with_seed(policy.seed());
                state.update(&value.to_le_bytes());
                state.update(&part.to_le_bytes());
                *value = state.digest();
            }
        }
    }
    let mut hashes = vector(owner, prepared.len())?;
    let mut validity = vector(owner, prepared.len())?;
    for occurrence in 0..prepared.len() {
        checkpoint_at(occurrence, checkpoint)?;
        let ordinal = prepared.input.ordinal(occurrence);
        hashes.push(combined[ordinal]);
        validity.push(valid[ordinal]);
    }
    checkpoint()?;
    Ok(LocalHashBatch {
        input: prepared.input.clone(),
        policy: policy.clone(),
        hashes,
        validity,
        _storage: owner.storage_lifetime(),
    })
}
fn null_hash(seed: u64) -> u64 {
    let mut state = Xxh3::with_seed(seed);
    state.update(&[0]);
    state.digest()
}
fn digest_bytes(
    seed: u64,
    bytes: &[u8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<u64> {
    let mut state = Xxh3::with_seed(seed);
    for block in bytes.chunks(64 * 1024) {
        checkpoint()?;
        state.update(block);
    }
    Ok(state.digest())
}
fn physical_row(mapping: Option<&[usize]>, row: usize) -> ExecutionResult<usize> {
    match mapping {
        Some(rows) => rows
            .get(row)
            .copied()
            .filter(|&row| row != usize::MAX)
            .ok_or_else(|| invariant(ValueViolation::Bounds)),
        None => Ok(row),
    }
}
fn complex_hashes(
    prepared: &PreparedKeyBatch<'_>,
    column: usize,
    seed: u64,
    output: &mut [u64],
    valid: &mut [u8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let (array, mapping) = prepared.tuples().physical_component(column)?;
    for (ordinal, &row) in prepared.input.unique_rows().iter().enumerate() {
        checkpoint_at(ordinal, checkpoint)?;
        if array.is_null(physical_row(mapping, row)?) {
            output[ordinal] = null_hash(seed);
            if prepared.spec.nulls()[column] == NullPolicy::Unmatchable {
                valid[ordinal] = 0;
            }
        } else {
            let bytes = prepared.tuples().encode_component_row(
                column,
                row,
                prepared.allocator.clone(),
                checkpoint,
            )?;
            output[ordinal] = digest_bytes(seed, &bytes, checkpoint)?;
        }
    }
    Ok(())
}
fn fixed_hashes(
    prepared: &PreparedKeyBatch<'_>,
    column: usize,
    seed: u64,
    output: &mut [u64],
    valid: &mut [u8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let (array, mapping) = prepared.tuples().physical_component(column)?;
    let rows = prepared.input.unique_rows();
    let unmatchable = prepared.spec.nulls()[column] == NullPolicy::Unmatchable;
    macro_rules! primitive {
        ($ty:ty, $canonical:expr) => {{
            let values = array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            for (ordinal, &row) in rows.iter().enumerate() {
                checkpoint_at(ordinal, checkpoint)?;
                let row = physical_row(mapping, row)?;
                if values.is_null(row) {
                    output[ordinal] = null_hash(seed);
                    if unmatchable {
                        valid[ordinal] = 0;
                    }
                } else {
                    let bytes = ($canonical)(values.value(row)).to_le_bytes();
                    let mut state = Xxh3::with_seed(seed);
                    state.update(&[1]);
                    state.update(&bytes);
                    output[ordinal] = state.digest();
                }
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::Null => {
            for (i, value) in output.iter_mut().enumerate() {
                checkpoint_at(i, checkpoint)?;
                *value = null_hash(seed);
                if unmatchable {
                    valid[i] = 0;
                }
            }
            Ok(())
        }
        DataType::Boolean => primitive!(BooleanArray, |v: bool| u8::from(v)),
        DataType::Int8 => primitive!(Int8Array, |v: i8| v),
        DataType::Int16 => primitive!(Int16Array, |v: i16| v),
        DataType::Int32 => primitive!(Int32Array, |v: i32| v),
        DataType::Int64 => primitive!(Int64Array, |v: i64| v),
        DataType::UInt8 => primitive!(UInt8Array, |v: u8| v),
        DataType::UInt16 => primitive!(UInt16Array, |v: u16| v),
        DataType::UInt32 => primitive!(UInt32Array, |v: u32| v),
        DataType::UInt64 => primitive!(UInt64Array, |v: u64| v),
        DataType::Float32 => primitive!(Float32Array, canonical_f32_bits),
        DataType::Float64 => primitive!(Float64Array, canonical_f64_bits),
        DataType::Decimal128(..) => primitive!(Decimal128Array, |v: i128| v),
        DataType::Decimal256(..) => primitive!(Decimal256Array, |v: arrow_buffer::i256| v),
        DataType::Date32 => primitive!(Date32Array, |v: i32| v),
        DataType::Date64 => primitive!(Date64Array, |v: i64| v),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondArray, |v: i32| v),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray, |v: i32| v),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray, |v: i64| v),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray, |v: i64| v),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray, |v: i64| v),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondArray, |v: i64| v)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondArray, |v: i64| v)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondArray, |v: i64| v)
        }
        DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondArray, |v: i64| v),
        DataType::Duration(TimeUnit::Millisecond) => {
            primitive!(DurationMillisecondArray, |v: i64| v)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            primitive!(DurationMicrosecondArray, |v: i64| v)
        }
        DataType::Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondArray, |v: i64| v),
        DataType::FixedSizeBinary(_) => {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            for (ordinal, &row) in rows.iter().enumerate() {
                checkpoint_at(ordinal, checkpoint)?;
                let row = physical_row(mapping, row)?;
                if values.is_null(row) {
                    output[ordinal] = null_hash(seed);
                    if unmatchable {
                        valid[ordinal] = 0;
                    }
                } else {
                    output[ordinal] = digest_bytes(seed, values.value(row), checkpoint)?;
                }
            }
            Ok(())
        }
        _ => Err(invariant(ValueViolation::Carrier)),
    }
}
