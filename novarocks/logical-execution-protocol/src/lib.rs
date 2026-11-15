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

//! Deterministic logical execution decisions with explicit inputs and ordered effects.

#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

pub mod cancel;
pub mod convergence;
pub mod core;
pub mod delivery;
pub mod effect;
pub mod establish;
pub mod facts;
pub mod failure;
pub mod input;
pub mod pre_ready;
pub mod ready;
pub mod recovery;
pub mod replacement;
pub mod root_control;
pub mod stand_down;
pub mod start;
pub mod state;
pub mod visibility;

use std::num::NonZeroU64;

macro_rules! correlation_id {
    ($name:ident) => {
        /// Process-local correlation data, never a resource or execution grant.
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(NonZeroU64);
        impl $name {
            pub const fn new(value: NonZeroU64) -> Self {
                Self(value)
            }
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }
    };
}
correlation_id!(LogicalActionId);
correlation_id!(LogicalSourceId);
correlation_id!(LogicalRequestToken);

/// The host charges the current turn, without clocks, waiting or callbacks.
pub trait StepBudget {
    fn charge(&mut self, units: usize) -> bool;
}

/// Ordered effects are captured during the same pure step and handed off atomically.
pub trait EffectSink {
    fn emit(&mut self, effect: effect::LogicalEffect);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreFault {
    ForeignExecution,
    ForeignActivation,
    UnknownAction,
    UnexpectedInput,
}

/// The constructor retains already validated immutable start facts.
/// Decision handling is assembled from the component state machines.
pub struct LogicalExecutionCore {
    _start: start::LogicalStart,
}

impl LogicalExecutionCore {
    pub fn new(start: start::LogicalStart) -> Self {
        start
            .validate()
            .expect("logical start facts must satisfy the frozen contract");
        Self { _start: start }
    }
}
