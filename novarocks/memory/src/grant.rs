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

//! Explicit allocation authorization obtained before the bounded operation.
use crate::{
    Account, domain::FundingDomain, error::CapacityError, lane::FactToken, stock::FundingScope,
};
#[derive(Debug)]
pub struct ExplicitGrant {
    lane: FundingDomain,
    scope: Option<FundingScope>,
}
impl Account {
    pub fn request_explicit(&self, bytes: u64) -> Result<ExplicitGrant, CapacityError> {
        let lane = self.create_domain(bytes)?;
        let scope = lane.activate(bytes, 0)?;
        Ok(ExplicitGrant {
            lane,
            scope: Some(scope),
        })
    }
}
impl ExplicitGrant {
    pub fn remaining_bytes(&self) -> u64 {
        self.scope.as_ref().unwrap().stock_bytes()
    }
    pub fn record_success(&mut self, bytes: u64) -> Result<FactToken, CapacityError> {
        if bytes > self.remaining_bytes() {
            return Err(CapacityError::Invalid {
                detail: "explicit allocation exceeds granted remainder",
            });
        }
        Ok(self.scope.as_mut().unwrap().record_allocation(bytes))
    }
}
impl Drop for ExplicitGrant {
    fn drop(&mut self) {
        self.scope.take().unwrap().finish();
        self.lane
            .stop_producing()
            .expect("explicit lane has no active publisher or external bound");
    }
}
