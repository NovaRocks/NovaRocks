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

use crate::aggregate_scalar::{ScalarStateAllocator, ScalarWork, aggregate_vec_with_capacity};
use crate::{KernelFailure, kernel_control::invalid};
use allocator_api2::vec::Vec as ScalarVec;

const PRECISION: u32 = 11;
const SPARSE_PRECISION: u32 = 26;
const REGISTERS: usize = 1 << PRECISION;
const SPARSE_CAPACITY: usize = REGISTERS / size_of::<u32>();
const SPARSE_RANK_BITS: u32 = u32::BITS - SPARSE_PRECISION;
const SPARSE_RANK_MASK: u32 = (1 << SPARSE_RANK_BITS) - 1;
const MAGIC: [u8; 4] = [b'N', b'H', 1, PRECISION as u8];

enum Registers<A: ScalarStateAllocator> {
    Sparse(ScalarVec<u32, A>),
    Dense(ScalarVec<u8, A>),
}

pub(super) struct Sketch<A: ScalarStateAllocator> {
    allocator: A,
    registers: Registers<A>,
}

impl<A: ScalarStateAllocator> Sketch<A> {
    pub(super) fn new(allocator: A) -> Self {
        Self {
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
            apply_sparse_entry(values, entry);
            return Ok(());
        };
        let bucket = entry >> SPARSE_RANK_BITS;
        match entries.binary_search_by_key(&bucket, |entry| entry >> SPARSE_RANK_BITS) {
            Ok(index) => entries[index] = entries[index].max(entry),
            Err(index) if entries.len() < SPARSE_CAPACITY => {
                work.flush()?;
                entries.try_reserve(1).map_err(|error| {
                    self.allocator
                        .scalar_allocation_error("reserve approx_distinct sparse registers", error)
                })?;
                entries.insert(index, entry);
                work.flush()?;
            }
            Err(_) => {
                self.promote(work)?;
                let Registers::Dense(values) = &mut self.registers else {
                    unreachable!()
                };
                apply_sparse_entry(values, entry);
            }
        }
        Ok(())
    }

    fn promote(&mut self, work: &mut ScalarWork<'_, '_>) -> Result<(), KernelFailure> {
        let Registers::Sparse(entries) = &self.registers else {
            return Ok(());
        };
        let mut values = aggregate_vec_with_capacity(
            &self.allocator,
            REGISTERS,
            "reserve approx_distinct dense registers",
            work,
        )?;
        values.resize(REGISTERS, 0);
        for entry in entries {
            work.step()?;
            apply_sparse_entry(&mut values, *entry);
        }
        self.registers = Registers::Dense(values);
        Ok(())
    }

    pub(super) fn estimate(&self, work: &mut ScalarWork<'_, '_>) -> Result<i64, KernelFailure> {
        let Registers::Dense(registers) = &self.registers else {
            let Registers::Sparse(entries) = &self.registers else {
                unreachable!()
            };
            let m = f64::from(1 << SPARSE_PRECISION);
            return Ok(linear_count(m, entries.len()) as i64);
        };
        let mut zero_buckets = 0;
        let mut harmonic = 0.0;
        for register in registers {
            work.step()?;
            zero_buckets += usize::from(*register == 0);
            harmonic += 2.0_f64.powi(-i32::from(*register));
        }
        let m = REGISTERS as f64;
        let estimate = if zero_buckets as f64 > 0.4 * m {
            linear_count(m, REGISTERS - zero_buckets)
        } else {
            let alpha = 0.7213 / (1.0 + 1.079 / m);
            (alpha * m * m / harmonic).round()
        };
        Ok(estimate as i64)
    }

    pub(super) fn encode(
        &self,
        profile: u8,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<ScalarVec<u8, A>, KernelFailure> {
        let (tag, len) = match &self.registers {
            Registers::Sparse(entries) => (0, entries.len() * size_of::<u32>()),
            Registers::Dense(values) => (1, values.len()),
        };
        let mut result = aggregate_vec_with_capacity(
            &self.allocator,
            MAGIC.len() + 2 + len,
            "reserve approx_distinct state payload",
            work,
        )?;
        result.extend_from_slice(&MAGIC);
        result.extend_from_slice(&[profile, tag]);
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
        profile: u8,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        let Some((header, payload)) = bytes.split_at_checked(MAGIC.len() + 2) else {
            return Err(invalid("approx_distinct state header is missing"));
        };
        if header[..MAGIC.len()] != MAGIC || header[MAGIC.len()] != profile {
            return Err(invalid(
                "approx_distinct state has a different format or hash profile",
            ));
        }
        match header[MAGIC.len() + 1] {
            0 if payload.len() <= SPARSE_CAPACITY * size_of::<u32>()
                && payload.len().is_multiple_of(size_of::<u32>()) =>
            {
                let mut previous = None;
                for word in payload.as_chunks::<{ size_of::<u32>() }>().0 {
                    work.step()?;
                    let entry = u32::from_le_bytes(*word);
                    let bucket = entry >> SPARSE_RANK_BITS;
                    let rank = entry & SPARSE_RANK_MASK;
                    if previous.is_some_and(|last| last >= bucket)
                        || !(1..=(64 - SPARSE_PRECISION + 1)).contains(&rank)
                    {
                        return Err(invalid("approx_distinct sparse state is not canonical"));
                    }
                    self.add_entry(entry, work)?;
                    previous = Some(bucket);
                }
            }
            1 if payload.len() == REGISTERS => {
                self.promote(work)?;
                let Registers::Dense(values) = &mut self.registers else {
                    unreachable!()
                };
                for (state, value) in values.iter_mut().zip(payload) {
                    work.step()?;
                    if u32::from(*value) > 64 - PRECISION + 1 {
                        return Err(invalid("approx_distinct dense state rank is invalid"));
                    }
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

fn linear_count(buckets: f64, used: usize) -> f64 {
    (-buckets * (-(used as f64) / buckets).ln_1p()).round()
}

fn apply_sparse_entry(registers: &mut [u8], entry: u32) {
    const EXTRA_BITS: u32 = SPARSE_PRECISION - PRECISION;
    let bucket = entry >> (SPARSE_RANK_BITS + EXTRA_BITS);
    let tail = (entry >> SPARSE_RANK_BITS) & ((1 << EXTRA_BITS) - 1);
    let rank = if tail == 0 {
        EXTRA_BITS + (entry & SPARSE_RANK_MASK)
    } else {
        (tail << (32 - EXTRA_BITS)).leading_zeros() + 1
    };
    let register = &mut registers[bucket as usize];
    *register = (*register).max(rank as u8);
}
