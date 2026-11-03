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

//! Thread-local stock. A lease must leave before await or thread migration.
use crate::sync::Ordering;
use crate::{
    domain::FundingDomain, error::CapacityError, lane::FactToken, settlement::StepReceipt,
};
use std::{marker::PhantomData, rc::Rc};
#[derive(Debug)]
pub struct FundingScope {
    domain: FundingDomain,
    stock: u64,
    miss: u64,
    threshold: u64,
    sticky: bool,
    ended: bool,
    _thread: PhantomData<Rc<()>>,
}
impl FundingDomain {
    pub fn activate(
        &self,
        stock_bytes: u64,
        threshold_bytes: u64,
    ) -> Result<FundingScope, CapacityError> {
        let mut s = self.0.state.lock().unwrap();
        if self.0.lane.production_state() != crate::lane::ProductionState::Producing {
            return Err(CapacityError::Closed {
                account: self.affiliation().id(),
            });
        }
        if s.drain_requested {
            return Err(CapacityError::Invalid {
                detail: "lane must complete pending drain before reactivation",
            });
        }
        if s.active {
            return Err(CapacityError::Invalid {
                detail: "domain already has an active allocation writer",
            });
        }
        let available = s
            .authorized
            .saturating_sub(self.0.lane.live_bytes().saturating_add(s.external));
        if stock_bytes > available {
            return Err(CapacityError::Invalid {
                detail: "stock exceeds domain workset",
            });
        }
        if !self.0.lane.enter() {
            return Err(CapacityError::Closed {
                account: self.affiliation().id(),
            });
        }
        s.active = true;
        s.generation += 1;
        self.0
            .lane
            .record()
            .sequence
            .fetch_add(1, Ordering::Release);
        Ok(FundingScope {
            domain: self.clone(),
            stock: stock_bytes,
            miss: 0,
            threshold: threshold_bytes,
            sticky: false,
            ended: false,
            _thread: PhantomData,
        })
    }
}
impl FundingScope {
    /// Call only for a successful underlying allocation. Failure records no
    /// live bytes. The origin/length must accompany exactly one later free.
    pub fn record_allocation(&mut self, bytes: u64) -> FactToken {
        let shortfall = bytes.saturating_sub(self.stock);
        self.stock = self.stock.saturating_sub(bytes);
        self.miss = self.miss.saturating_add(shortfall);
        if self.miss > self.threshold {
            self.sticky = true;
        }
        self.domain.0.lane.publish_fact(bytes)
    }
    pub fn stock_bytes(&self) -> u64 {
        self.stock
    }
    pub fn threshold_triggered(&self) -> bool {
        self.sticky
    }
    pub fn finish(mut self) -> StepReceipt {
        let receipt = self.domain.settle();
        self.detach();
        self.ended = true;
        receipt
    }
    fn detach(&mut self) {
        let mut s = self.domain.0.state.lock().unwrap();
        s.active = false;
        self.domain.0.lane.leave();
        self.domain
            .0
            .lane
            .record()
            .sequence
            .fetch_add(1, Ordering::Release);
        let drain = s.drain_requested;
        drop(s);
        if drain {
            self.domain.drain_idle();
        }
    }
}
impl Drop for FundingScope {
    fn drop(&mut self) {
        if !self.ended {
            self.domain.settle();
            self.detach();
        }
    }
}
