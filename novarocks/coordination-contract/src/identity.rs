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

use novarocks_types::identity::QueryExecutionId;

/// Immutable identity of one actor-owned attempt activation generation.
///
/// This value is diagnostic only. State transitions require the corresponding
/// move-only permit, so copying an identity cannot create authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptActivationIdentity {
    actor_instance_id: u64,
    execution: QueryExecutionId,
    generation: u64,
}

/// Opaque process-local identity of one logical execution actor instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionActorId(u64);

impl LogicalExecutionActorId {
    /// Constructs correlation data only; this grants no actor authority.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl AttemptActivationIdentity {
    /// Constructs correlation data only; transitions still require issued permits.
    pub const fn from_parts(
        actor_instance_id: u64,
        execution: QueryExecutionId,
        generation: u64,
    ) -> Self {
        Self {
            actor_instance_id,
            execution,
            generation,
        }
    }

    pub const fn actor(self) -> LogicalExecutionActorId {
        LogicalExecutionActorId(self.actor_instance_id)
    }

    pub const fn execution(self) -> QueryExecutionId {
        self.execution
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}
