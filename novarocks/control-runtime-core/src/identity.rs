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

//! Stable identities are supplied by the embedding driver.

use std::num::NonZeroU64;

macro_rules! identity {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(NonZeroU64);
        impl $name {
            pub const fn new(value: u64) -> Option<Self> {
                match NonZeroU64::new(value) { Some(value) => Some(Self(value)), None => None }
            }
            pub const fn get(self) -> u64 { self.0.get() }
        }
    )+};
}
identity!(
    ScopeId,
    Generation,
    WorkId,
    SlotId,
    ActionId,
    ActorId,
    DimensionId,
    FamilyId,
    CapacityRegistrantId,
    ReservationId,
    ConsumerId,
    InputId,
    BatchId
);

/// A neutral address within one runtime ledger. It grants no Work authority.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScopedWorkRef {
    pub scope: ScopeId,
    pub work: WorkId,
}

impl Generation {
    pub fn next(self) -> Option<Self> {
        self.get().checked_add(1).and_then(Self::new)
    }
}

/// Driver-provided monotonic logical time; zero is a valid initial instant.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Tick(pub u64);

/// An opaque host-provided key. It grants no capacity or process authority.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapacityKey(pub u128);
