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

//! Tail range ownership after actual publication and exact normal probe settle.
//! The caller owns mark scanning, typed gather, output custody and real EOS.

use super::{
    build_state::build_contract,
    join_completion::{
        JoinCompletion, Published, RemainingDemand, TailStatus, arithmetic, funded_shared,
    },
};
use crate::{
    exec::{
        phased_work::{PhasedRangeWork, RangeObligation},
        pipeline::operator::InputConstructionOwner,
    },
    runtime::task::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{num::NonZeroUsize, ops::Range, sync::Arc};

pub(crate) struct TailRanges<A> {
    completion: Arc<JoinCompletion<A>>,
    ranges: Arc<PhasedRangeWork<ExecutionFailure, ValueTypeStorageLifetime>>,
    rows: usize,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) enum TailClaim<A> {
    Pending,
    Unavailable,
    Withdrawn,
    Finished,
    Range(TailObligation<A>),
}
pub(crate) struct TailObligation<A> {
    published: Published<A>,
    owner: Arc<TailRanges<A>>,
    obligation: RangeObligation<ExecutionFailure, ValueTypeStorageLifetime>,
}
impl<A> TailRanges<A> {
    /// Build publication fixes physical rows. No bitmap/slot initialization or
    /// data scan happens here; those belong to actual join finalize tasks.
    pub(crate) fn new(
        completion: Arc<JoinCompletion<A>>,
        width: NonZeroUsize,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Arc<Self>> {
        control.checkpoint(0)?;
        if completion.participants().tails == 0 {
            return Err(build_contract("probe-only join has no tail ranges"));
        }
        let published = completion
            .published()?
            .ok_or_else(|| build_contract("tail ranges require actual build publication"))?;
        let rows = published.rows;
        let ranges = funded_shared(
            owner,
            PhasedRangeWork::with_retention(
                build_contract("join tail range was abandoned"),
                arithmetic(),
                owner.storage_lifetime(),
            ),
        )?;
        ranges.begin(rows, width)?;
        funded_shared(
            owner,
            Self {
                completion,
                ranges,
                rows,
                _storage: owner.storage_lifetime(),
            },
        )
    }
    pub(crate) fn claim(self: &Arc<Self>) -> ExecutionResult<TailClaim<A>> {
        self.ranges
            .check()
            .map_err(|cause| self.completion.fail(cause))?;
        let published = match self.completion.tail_status()? {
            TailStatus::Pending => return Ok(TailClaim::Pending),
            TailStatus::Unavailable => return Ok(TailClaim::Unavailable),
            TailStatus::Withdrawn => return Ok(TailClaim::Withdrawn),
            TailStatus::Ready(published) => published,
        };
        if published.rows != self.rows {
            return Err(self.completion.fail(build_contract(
                "join tail publication changed physical row domain",
            )));
        }
        match self
            .ranges
            .claim()
            .map_err(|cause| self.completion.fail(cause))?
        {
            Some(obligation) => Ok(TailClaim::Range(TailObligation {
                published,
                owner: Arc::clone(self),
                obligation,
            })),
            None if self
                .ranges
                .barrier()
                .map_err(|cause| self.completion.fail(cause))? =>
            {
                Ok(TailClaim::Finished)
            }
            None => Ok(TailClaim::Pending),
        }
    }
}
impl<A> TailObligation<A> {
    pub(crate) fn rows(&self) -> Range<usize> {
        self.obligation.range()
    }
    pub(crate) fn artifact(&self) -> &A {
        self.published.artifact.as_ref()
    }
    pub(crate) fn check(&self) -> ExecutionResult<RemainingDemand> {
        self.owner
            .ranges
            .check()
            .map_err(|cause| self.owner.completion.fail(cause))?;
        self.owner.completion.remaining_demand()
    }
    /// Actual T calls this after the last pending page for this range transfers
    /// to the original output. Merely scanning all rows is not completion.
    pub(crate) fn complete_after_output(&mut self) -> ExecutionResult<()> {
        self.owner.completion.check()?;
        self.owner
            .ranges
            .complete(&mut self.obligation)
            .map_err(|cause| self.owner.completion.fail(cause))?;
        self.owner.completion.finalized_step();
        Ok(())
    }
    pub(crate) fn fail(&mut self, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.owner.ranges.latch(cause);
        self.owner.completion.fail(cause)
    }
    /// Settle custody on normal global output withdrawal, not a successful tail
    /// contribution. Demand is checked before every later Finished decision.
    pub(crate) fn withdraw_after_demand_released(&mut self) -> ExecutionResult<()> {
        self.owner.completion.check()?;
        if self.owner.completion.remaining_demand()? != RemainingDemand::Released {
            return Err(self.fail(build_contract("tail withdrawal still has output demand")));
        }
        self.owner
            .ranges
            .complete(&mut self.obligation)
            .map_err(|cause| self.owner.completion.fail(cause))?;
        self.owner.completion.finalized_step();
        Ok(())
    }
}
impl<A> Drop for TailObligation<A> {
    fn drop(&mut self) {
        if !self.obligation.is_settled() {
            let cause = self
                .owner
                .ranges
                .check()
                .err()
                .unwrap_or_else(|| build_contract("join tail range was abandoned"));
            self.owner.completion.fail(cause);
        }
    }
}

#[cfg(test)]
#[path = "tail_ranges_tests.rs"]
mod tests;
