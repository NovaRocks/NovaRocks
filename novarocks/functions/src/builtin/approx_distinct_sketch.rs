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

#[path = "approx_distinct_bias.rs"]
mod bias;

use crate::aggregate_scalar::{ScalarStateAllocator, ScalarWork, aggregate_vec_with_capacity};
use crate::{KernelFailure, kernel_control::invalid};
use allocator_api2::vec::Vec as ScalarVec;

const SPARSE_PRECISION: u32 = 26;
const SPARSE_RANK_BITS: u32 = u32::BITS - SPARSE_PRECISION;
const SPARSE_RANK_MASK: u32 = (1 << SPARSE_RANK_BITS) - 1;
const MAGIC: [u8; 3] = [b'N', b'H', 2];
const HEADER_LEN: usize = MAGIC.len() + 3;

enum Registers<A: ScalarStateAllocator> {
    Sparse(ScalarVec<u32, A>),
    Dense(ScalarVec<u8, A>),
}

pub(super) struct Sketch<A: ScalarStateAllocator> {
    allocator: A,
    precision: u8,
    registers: Registers<A>,
}

impl<A: ScalarStateAllocator> Sketch<A> {
    pub(super) fn new(precision: u8, allocator: A) -> Self {
        debug_assert!((4..=16).contains(&precision));
        Self {
            precision,
            registers: Registers::Sparse(ScalarVec::new_in(allocator.clone())),
            allocator,
        }
    }

    pub(super) fn retained_bytes(&self) -> usize {
        match &self.registers {
            Registers::Sparse(entries) => entries.capacity() * size_of::<u32>(),
            Registers::Dense(values) => values.capacity(),
        }
    }

    pub(super) fn add_hash(
        &mut self,
        hash: u64,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        let bucket = (hash >> (64 - SPARSE_PRECISION)) as u32;
        let rank = ((hash << SPARSE_PRECISION) | (1 << (SPARSE_PRECISION - 1))).leading_zeros() + 1;
        self.add_entry((bucket << SPARSE_RANK_BITS) | rank, work)
    }

    fn add_entry(
        &mut self,
        entry: u32,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        work.step()?;
        let Registers::Sparse(entries) = &mut self.registers else {
            let Registers::Dense(values) = &mut self.registers else {
                unreachable!()
            };
            apply_sparse_entry(self.precision, values, entry);
            return Ok(());
        };
        let bucket = entry >> SPARSE_RANK_BITS;
        match entries.binary_search_by_key(&bucket, |entry| entry >> SPARSE_RANK_BITS) {
            Ok(index) => entries[index] = entries[index].max(entry),
            Err(index) if entries.len() < sparse_capacity(self.precision) => {
                work.flush()?;
                entries.try_reserve(1).map_err(|error| {
                    self.allocator
                        .scalar_allocation_error("reserve approx_distinct sparse registers", error)
                })?;
                entries.insert(index, entry);
                work.flush()?;
            }
            Err(_) => apply_sparse_entry(self.precision, self.promote(work)?, entry),
        }
        Ok(())
    }

    fn promote(&mut self, work: &mut ScalarWork<'_, '_>) -> Result<&mut [u8], KernelFailure> {
        if let Registers::Sparse(entries) = &self.registers {
            let count = register_count(self.precision);
            let mut values = aggregate_vec_with_capacity(
                &self.allocator,
                count,
                "reserve approx_distinct dense registers",
                work,
            )?;
            values.resize(count, 0);
            for entry in entries {
                work.step()?;
                apply_sparse_entry(self.precision, &mut values, *entry);
            }
            self.registers = Registers::Dense(values);
        }
        let Registers::Dense(values) = &mut self.registers else {
            unreachable!()
        };
        Ok(values)
    }

    pub(super) fn estimate(&self, work: &mut ScalarWork<'_, '_>) -> Result<i64, KernelFailure> {
        let registers = match &self.registers {
            Registers::Sparse(entries) => {
                let count = 1 << SPARSE_PRECISION;
                return Ok(linear_count(count, count - entries.len()));
            }
            Registers::Dense(registers) => registers,
        };
        let mut zero_buckets = 0;
        let mut harmonic = 0.0;
        for register in registers {
            work.step()?;
            zero_buckets += usize::from(*register == 0);
            harmonic += 1.0 / ((1_u64 << *register) as f64);
        }
        let buckets = register_count(self.precision);
        let m = buckets as f64;
        if zero_buckets as f64 > 0.4 * m {
            return Ok(linear_count(buckets, zero_buckets));
        }
        let alpha = match self.precision {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let raw = alpha * m * m / harmonic;
        Ok(correct_bias(self.precision, raw).round() as i64)
    }

    pub(super) fn encode(
        &self,
        arity: u8,
        recipe: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<ScalarVec<u8, A>, KernelFailure> {
        let (tag, len) = match &self.registers {
            Registers::Sparse(entries) => (0, entries.len() * size_of::<u32>()),
            Registers::Dense(values) => (1, values.len()),
        };
        let mut result = aggregate_vec_with_capacity(
            &self.allocator,
            HEADER_LEN + recipe.len() + len,
            "reserve approx_distinct state payload",
            work,
        )?;
        result.extend_from_slice(&MAGIC);
        result.extend_from_slice(&[self.precision, arity, tag]);
        for chunk in recipe.chunks(256) {
            work.step()?;
            result.extend_from_slice(chunk);
        }
        match &self.registers {
            Registers::Sparse(entries) => {
                for entry in entries {
                    work.step()?;
                    result.extend_from_slice(&entry.to_le_bytes());
                }
            }
            Registers::Dense(values) => {
                for value in values {
                    work.step()?;
                    result.push(*value);
                }
            }
        }
        Ok(result)
    }

    pub(super) fn merge(
        &mut self,
        bytes: &[u8],
        arity: u8,
        recipe: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        let Some((header, rest)) = bytes.split_at_checked(HEADER_LEN) else {
            return Err(invalid("approx_distinct state header is missing"));
        };
        if header[..MAGIC.len()] != MAGIC
            || header[MAGIC.len()] != self.precision
            || header[MAGIC.len() + 1] != arity
        {
            return Err(invalid(
                "approx_distinct state has a different format, precision or overload",
            ));
        }
        let Some((encoded_recipe, payload)) = rest.split_at_checked(recipe.len()) else {
            return Err(invalid("approx_distinct state hash recipe is missing"));
        };
        for (actual, expected) in encoded_recipe.chunks(256).zip(recipe.chunks(256)) {
            work.step()?;
            if actual != expected {
                return Err(invalid("approx_distinct state has a different hash recipe"));
            }
        }
        match header[MAGIC.len() + 2] {
            0 if payload.len() <= sparse_capacity(self.precision) * size_of::<u32>()
                && payload.len().is_multiple_of(size_of::<u32>()) =>
            {
                let mut previous = None;
                let (words, _) = payload.as_chunks::<{ size_of::<u32>() }>();
                for word in words {
                    work.step()?;
                    let entry = u32::from_le_bytes(*word);
                    let bucket = entry >> SPARSE_RANK_BITS;
                    let rank = entry & SPARSE_RANK_MASK;
                    if previous.is_some_and(|last| last >= bucket)
                        || !(1..=(u64::BITS - SPARSE_PRECISION + 1)).contains(&rank)
                    {
                        return Err(invalid("approx_distinct sparse state is not canonical"));
                    }
                    self.add_entry(entry, work)?;
                    previous = Some(bucket);
                }
            }
            1 if payload.len() == register_count(self.precision) => {
                let max_rank = u64::BITS - u32::from(self.precision) + 1;
                let mut has_value = false;
                for value in payload {
                    work.step()?;
                    if u32::from(*value) > max_rank {
                        return Err(invalid("approx_distinct dense state rank is invalid"));
                    }
                    has_value |= *value != 0;
                }
                if !has_value {
                    return Err(invalid(
                        "approx_distinct empty state requires sparse representation",
                    ));
                }
                for (state, value) in self.promote(work)?.iter_mut().zip(payload) {
                    work.step()?;
                    *state = (*state).max(*value);
                }
            }
            _ => {
                return Err(invalid(
                    "approx_distinct state has an invalid representation",
                ));
            }
        }
        Ok(())
    }
}

fn register_count(precision: u8) -> usize {
    1 << precision
}

fn sparse_capacity(precision: u8) -> usize {
    // Airlift 451's sequential sparse state starts with one slot, growing by
    // ten when needed. Dense's promotion estimate uses four bits per bucket
    // and a reference object-size difference of sixteen bytes. Array headers
    // cancel, and the remaining dense size is already eight-byte aligned.
    // With the sparse growth rule applied to the distinct prefix count, this
    // transition depends on content and agrees with a single reference sketch.
    let dense_reference_bytes = register_count(precision) / 2 + 16;
    let max_sparse_slots = dense_reference_bytes / size_of::<u32>();
    1 + ((max_sparse_slots - 1) / 10) * 10
}

fn linear_count(buckets: usize, zero_buckets: usize) -> i64 {
    let m = buckets as f64;
    (m * (m / zero_buckets as f64).ln()).round() as i64
}

fn correct_bias(precision: u8, estimate: f64) -> f64 {
    let index = (precision - 4) as usize;
    let estimates = bias::RAW_ESTIMATES[index];
    let biases = bias::BIAS[index];
    let bias = match estimates.binary_search_by(|value| value.total_cmp(&estimate)) {
        Ok(index) => biases[index],
        Err(index) if index == 0 || index == estimates.len() => return estimate,
        Err(index) => {
            let x0 = estimates[index - 1];
            let x1 = estimates[index];
            let y0 = biases[index - 1];
            let y1 = biases[index];
            (estimate - x0) * (y1 - y0) / (x1 - x0) + y0
        }
    };
    estimate - bias
}

fn apply_sparse_entry(precision: u8, registers: &mut [u8], entry: u32) {
    let extra_bits = SPARSE_PRECISION - u32::from(precision);
    let bucket = entry >> (SPARSE_RANK_BITS + extra_bits);
    let tail = (entry >> SPARSE_RANK_BITS) & ((1 << extra_bits) - 1);
    let rank = if tail == 0 {
        extra_bits + (entry & SPARSE_RANK_MASK)
    } else {
        (tail << (32 - extra_bits)).leading_zeros() + 1
    };
    let register = &mut registers[bucket as usize];
    *register = (*register).max(rank as u8);
}
