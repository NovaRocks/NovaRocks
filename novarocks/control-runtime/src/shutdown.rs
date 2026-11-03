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

//! A shutdown request closes admission; it never proves work or storage exit.

use crate::{
    actor::Actor,
    actor_turn::{ActorRuntime, ActorTurnError},
    transaction::{RuntimeError, RuntimeLedger},
};

pub struct Shutdown {
    ledger: RuntimeLedger,
}
impl Shutdown {
    pub fn new(ledger: RuntimeLedger) -> Self {
        Self { ledger }
    }
    pub fn close_external<A: Actor>(&self, runtime: &ActorRuntime<A>) {
        runtime.close_admission();
        self.ledger.close_admission();
    }
    pub fn ready<A: Actor>(&self, runtime: &ActorRuntime<A>) -> bool {
        self.ledger.is_empty() && runtime.is_empty()
    }
    pub fn reap(&self, budget: usize) -> Result<usize, RuntimeError> {
        self.ledger.reap_ready(budget)
    }
    pub fn require_drained<A: Actor>(
        &self,
        runtime: &ActorRuntime<A>,
    ) -> Result<(), ActorTurnError> {
        if self.ready(runtime) {
            Ok(())
        } else {
            Err(ActorTurnError::ActorNotRetired)
        }
    }
}
