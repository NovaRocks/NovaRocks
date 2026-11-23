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

// The midpoint interpolation and alternating centroid admission follow
// Airlift TDigest as used by Trino 484.
use crate::aggregate_scalar::{ScalarStateAllocator, ScalarWork, aggregate_vec_with_capacity};
use crate::{KernelFailure, kernel_control::invalid};
use allocator_api2::vec::Vec as ScalarVec;
use std::cmp::Ordering;

const COMPRESSION: f64 = 200.0;
const COMPACT_AT: usize = 6 * (200 + 10);

#[derive(Clone, Copy)]
struct Centroid {
    mean: f64,
    weight: f64,
}

pub(super) struct Digest<A: ScalarStateAllocator> {
    entries: ScalarVec<Centroid, A>,
    weight: f64,
    min: f64,
    max: f64,
    backwards: bool,
    compact_at: usize,
}

impl<A: ScalarStateAllocator> Digest<A> {
    pub(super) fn new(allocator: A) -> Self {
        Self {
            entries: ScalarVec::new_in(allocator),
            weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            backwards: false,
            compact_at: COMPACT_AT,
        }
    }

    pub(super) fn insert(
        &mut self,
        value: f64,
        weight: f64,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        self.add_centroid(
            Centroid {
                mean: value,
                weight,
            },
            work,
        )?;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        Ok(())
    }

    fn add_centroid(
        &mut self,
        entry: Centroid,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        work.step()?;
        let weight = self.weight + entry.weight;
        if !weight.is_finite() {
            return Err(super::approx_histogram_kernel::parameter(
                "approx_percentile total weight exceeds the digest carrier",
            ));
        }
        if self.entries.len() == self.compact_at {
            self.compact(work)?;
            if self.entries.len() == self.compact_at {
                self.compact_at = self
                    .compact_at
                    .checked_mul(2)
                    .ok_or_else(|| invalid("approx_percentile centroid count is too large"))?;
            }
        }
        work.flush()?;
        self.entries.try_reserve(1).map_err(|cause| {
            self.entries
                .allocator()
                .scalar_allocation_error("reserve approx_percentile centroid", cause)
        })?;
        self.entries.push(entry);
        self.weight = weight;
        work.flush()
    }

    fn compact(&mut self, work: &mut ScalarWork<'_, '_>) -> Result<(), KernelFailure> {
        if self.entries.len() < 2 {
            return Ok(());
        }
        let direction = self.backwards;
        work.flush()?;
        let mut failure = None;
        self.entries.sort_unstable_by(|a, b| {
            if failure.is_some() {
                return Ordering::Equal;
            }
            if let Err(cause) = work.step() {
                failure = Some(cause);
                return Ordering::Equal;
            }
            let order = a.mean.total_cmp(&b.mean);
            if direction { order.reverse() } else { order }
        });
        if let Some(cause) = failure {
            return Err(cause);
        }
        work.flush()?;
        let mut before = 0.0;
        let factor = (4.0 * (self.weight / COMPRESSION).ln() + 24.0) / COMPRESSION;
        let relative_limit = |q: f64| q * (1.0 - q) * factor;
        let mut kept = 0;
        let mut current = self.entries[0];
        for index in 1..self.entries.len() {
            work.step()?;
            let next = self.entries[index];
            let combined = current.weight + next.weight;
            let low = before / self.weight;
            let high = ((before + combined) / self.weight).min(1.0);
            if combined <= self.weight * relative_limit(low).min(relative_limit(high)) {
                let next_fraction = next.weight / combined;
                current.mean = weighted_between(current.mean, next.mean, next_fraction);
                current.weight = combined;
            } else {
                self.entries[kept] = current;
                kept += 1;
                before += current.weight;
                current = next;
            }
        }
        self.entries[kept] = current;
        self.entries.truncate(kept + 1);
        if direction {
            work.flush()?;
            self.entries.reverse();
        }
        self.backwards = !direction;
        work.flush()
    }

    pub(super) fn prepare(
        &self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<PreparedDigest<A>, KernelFailure> {
        if self.entries.is_empty() {
            return Err(invalid(
                "approx_percentile has targets with no contributing sample",
            ));
        }
        let allocator = self.entries.allocator();
        let mut entries = aggregate_vec_with_capacity(
            allocator,
            self.entries.len(),
            "reserve approx_percentile emission centroids",
            work,
        )?;
        for entry in &self.entries {
            work.step()?;
            entries.push(*entry);
        }
        let mut view = Self {
            entries,
            weight: self.weight,
            min: self.min,
            max: self.max,
            backwards: self.backwards,
            compact_at: self.compact_at,
        };
        view.compact(work)?;
        let mut centers = aggregate_vec_with_capacity(
            allocator,
            view.entries.len(),
            "reserve approx_percentile centroid ranks",
            work,
        )?;
        let mut position = 0.0;
        for entry in &view.entries {
            work.step()?;
            centers.push(position + entry.weight / 2.0);
            position += entry.weight;
        }
        Ok(PreparedDigest {
            digest: view,
            centers,
        })
    }
}

fn weighted_between(left: f64, right: f64, fraction: f64) -> f64 {
    if (right - left).is_finite() {
        left + (right - left) * fraction
    } else {
        left * (1.0 - fraction) + right * fraction
    }
}

pub(super) struct PreparedDigest<A: ScalarStateAllocator> {
    digest: Digest<A>,
    centers: ScalarVec<f64, A>,
}

impl<A: ScalarStateAllocator> PreparedDigest<A> {
    pub(super) fn quantile(
        &self,
        quantile: f64,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<f64, KernelFailure> {
        let digest = &self.digest;
        work.step()?;
        if digest.entries.len() == 1 {
            return Ok(digest.entries[0].mean);
        }
        let offset = quantile * digest.weight;
        if offset < 1.0 {
            return Ok(digest.min);
        }
        if offset >= digest.weight - 1.0 {
            return Ok(digest.max);
        }
        let right = self.centers.partition_point(|midpoint| *midpoint <= offset);
        if right == 0 {
            let first = digest.entries[0];
            return Ok(weighted_between(
                digest.min,
                first.mean,
                (offset - 1.0) / (first.weight / 2.0 - 1.0),
            ));
        }
        let left = digest.entries[right - 1];
        let left_center = self.centers[right - 1];
        if right == digest.entries.len() {
            return Ok(weighted_between(
                left.mean,
                digest.max,
                (offset - left_center) / (digest.weight - 1.0 - left_center),
            ));
        }

        // Singleton centroids occupy a full data point even when the adjoining
        // centroid represents a weighted cluster.
        let right = digest.entries[right];
        let from_left = offset - left_center;
        if left.weight == 1.0 && from_left < 0.5 {
            return Ok(left.mean);
        }
        if right.weight == 1.0 && from_left >= left.weight / 2.0 {
            return Ok(right.mean);
        }
        let start = if left.weight == 1.0 { 0.5 } else { 0.0 };
        let end = if right.weight == 1.0 {
            left.weight / 2.0
        } else {
            (left.weight + right.weight) / 2.0
        };
        Ok(weighted_between(
            left.mean,
            right.mean,
            (from_left - start) / (end - start),
        ))
    }
}
