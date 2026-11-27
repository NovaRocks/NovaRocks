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

//! Cumulative projection requests, using the existing complete numerical Node
//! author. Source bytes cover the caller's whole original union once. Known
//! floors are necessary subsets, not measurements or a host allocation grant.
use super::SemanticsCodecError as Error;
use crate::{
    allocation_exit_v2::reserve_exit,
    physical_node_v2::{
        self as node, Model, NodeCodecError, NodeProjectionFacts, NodeProjectionLimits,
    },
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, ControlOwnedResourceFacts};

pub(super) type Admission<'a> =
    dyn FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError> + 'a;
pub(super) struct Projection<'a> {
    model: Model,
    source: usize,
    limits: Option<NodeProjectionLimits>,
    admit: Option<&'a mut Admission<'a>>,
    known: usize,
    input_derived: bool,
}
// The numerical Model is closed: its checked arithmetic/caps produce Control,
// with static source-shape errors retained structurally rather than classified
// by diagnostic text. Other Node variants are not produced by this author.
fn numerical(error: NodeCodecError) -> Error {
    match error {
        NodeCodecError::Control(cause) => Error::Control(cause),
        NodeCodecError::InvalidShape(message) => Error::InvalidShape(message),
        _ => Error::SourceModel("semantic numerical author returned a non-numerical error"),
    }
}
impl<'a> Projection<'a> {
    pub(super) fn plain() -> Self {
        Self {
            model: Model::default(),
            source: 0,
            limits: None,
            admit: None,
            known: 0,
            input_derived: false,
        }
    }
    pub(super) fn observed(
        source: usize,
        limits: impl Into<Option<NodeProjectionLimits>>,
        admit: &'a mut Admission<'a>,
        known: usize,
    ) -> Result<Self, Error> {
        let limits = limits.into();
        let value = Self {
            model: Model::default(),
            source,
            limits,
            admit: Some(admit),
            known,
            input_derived: limits.is_none(),
        };
        Ok(value)
    }
    pub(super) fn input_derived(source: usize) -> Self {
        Self {
            model: Model::default(),
            source,
            limits: None,
            admit: None,
            known: 0,
            input_derived: true,
        }
    }
    pub(super) fn observed_mode(&self) -> bool {
        self.admit.is_some() || self.input_derived
    }
    pub(super) fn gate(&mut self) -> Result<(), Error> {
        if self.input_derived {
            if self.admit.is_none() && self.source < self.known {
                return Err(Error::InvalidShape(
                    "semantic source invoice omits original backing",
                ));
            }
            let facts = self
                .model
                .numerical_input_derived_facts(self.source.max(self.known), 0)
                .map_err(numerical)?;
            if let Some(admit) = self.admit.as_mut() {
                admit(&facts)?;
            }
        }
        if let Some(limits) = self.limits {
            if self.source < self.known {
                return Err(Error::InvalidShape(
                    "semantic source invoice omits original backing",
                ));
            }
            let facts = self
                .model
                .numerical_facts(self.source, 0, limits)
                .map_err(numerical)?;
            if let Some(admit) = self.admit.as_mut() {
                admit(&facts)?;
            }
        }
        Ok(())
    }
    pub(super) fn facts(&self) -> Result<NodeProjectionFacts, Error> {
        if self.input_derived {
            return self
                .model
                .numerical_input_derived_facts(self.source.max(self.known), 0)
                .map_err(numerical);
        }
        self.model
            .numerical_facts(
                self.source,
                0,
                self.limits.expect("observed projection has limits"),
            )
            .map_err(numerical)
    }
    pub(super) fn known<T>(&mut self, n: usize) -> Result<(), Error> {
        if self.observed_mode() {
            self.known = node::add(self.known, node::bytes::<T>(n).map_err(numerical)?)
                .map_err(numerical)?;
        }
        Ok(())
    }
    pub(super) fn items(&mut self, n: usize) -> Result<(), Error> {
        if self.observed_mode() {
            self.model.items = node::add(self.model.items, n).map_err(numerical)?;
        }
        Ok(())
    }
    pub(super) fn buffers<T>(&mut self, n: usize, copies: usize) -> Result<(), Error> {
        if self.observed_mode() {
            self.model.request::<T>(n, copies).map_err(numerical)?;
        }
        Ok(())
    }
    pub(super) fn child(
        &mut self,
        next: &ControlOwnedResourceFacts,
        previous: &mut ControlOwnedResourceFacts,
    ) -> Result<(), CompileControlError> {
        let delta = |next: usize, prev: usize| {
            next.checked_sub(prev)
                .ok_or(CompileControlError::ResourceExhausted)
        };
        self.model.requests = self
            .model
            .requests
            .checked_add(delta(
                next.allocation_requests_upper_bound,
                previous.allocation_requests_upper_bound,
            )?)
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.model.requested = self
            .model
            .requested
            .checked_add(delta(
                next.allocation_request_bytes_upper_bound,
                previous.allocation_request_bytes_upper_bound,
            )?)
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.model.delegated_work = self
            .model
            .delegated_work
            .checked_add(delta(
                next.cumulative_work_upper_bound,
                previous.cumulative_work_upper_bound,
            )?)
            .ok_or(CompileControlError::ResourceExhausted)?;
        *previous = *next;
        match self.gate() {
            Ok(()) => Ok(()),
            Err(Error::Control(cause)) => Err(cause),
            Err(_) => Err(CompileControlError::ResourceExhausted),
        }
    }
    pub(super) fn reserve<T>(
        &mut self,
        n: usize,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<T>, Error> {
        if !self.observed_mode() {
            return Ok(Vec::with_capacity(n));
        }
        self.gate()?;
        node::bytes::<T>(n).map_err(numerical)?;
        w.flush()?;
        let mut output = Vec::new();
        reserve_exit::<Error>(output.try_reserve_exact(n), w)?;
        Ok(output)
    }
    pub(super) fn boxed<T>(
        &mut self,
        value: Vec<T>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, Error> {
        if !self.observed_mode() {
            return Ok(value.into_boxed_slice());
        }
        self.gate()?;
        w.flush()?;
        let output = value.into_boxed_slice();
        w.step()?;
        w.flush()?;
        Ok(output)
    }
}
