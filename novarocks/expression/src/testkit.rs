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

use crate::{
    InputSourceStorage, InputStorageFailure, ScalarCallFailure, ScalarCallHost, ScalarCallPhase,
    ScalarCallRequestFacts,
};
use novarocks_functions::AggregateStateAllocator;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{alloc::Layout, convert::Infallible, sync::Arc};

pub struct PassThroughCallHost;

impl ScalarCallHost for PassThroughCallHost {
    type Refusal = Infallible;

    fn begin_call(&mut self) {}

    fn run_admitted(
        &mut self,
        _phase: ScalarCallPhase,
        _facts: ScalarCallRequestFacts,
        body: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
    ) -> Result<(), ScalarCallFailure<Self::Refusal>> {
        body()
    }

    fn callback_request_facts(
        &mut self,
        _allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>> {
        Ok(ScalarCallRequestFacts::default())
    }
}

pub struct TestInputSourceStorage {
    storage: ValueTypeStorageLifetime,
}

impl TestInputSourceStorage {
    pub fn new(storage: ValueTypeStorageLifetime) -> Self {
        Self { storage }
    }
}

impl InputSourceStorage for TestInputSourceStorage {
    fn storage_lifetime_observed(
        &self,
        base: ValueTypeStorageLifetime,
        request: &mut dyn FnMut(Layout) -> Result<(), InputStorageFailure>,
    ) -> Result<ValueTypeStorageLifetime, InputStorageFailure> {
        base.retaining_observed(&self.storage, request)
    }
}
