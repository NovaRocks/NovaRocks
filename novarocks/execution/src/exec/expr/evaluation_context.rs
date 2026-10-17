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
//! Per-call evaluation loans. Immutable arena facts never own runtime authority.

use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkFieldSchema};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::FunctionValueType;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::sync::Arc;

/// The invocation caller preserves its original execution failure vocabulary.
/// This Execution-local port is independent of the Functions kernel ABI.
pub(crate) trait ExprEvaluationControl: Send + Sync {
    fn checkpoint(&self) -> ExecutionResult<()>;
    fn wait(&self, duration: std::time::Duration) -> ExecutionResult<()>;
}

/// Only legacy mathematical diagnostics may select another evaluation geometry.
/// An originating runtime failure, including a text-only Pipeline cause, is
/// never classified by its category or rendered message.
pub(crate) enum ExprEvaluationFailure {
    Mathematical(String),
    Runtime(ExecutionFailure),
}
impl From<String> for ExprEvaluationFailure {
    fn from(error: String) -> Self {
        Self::Mathematical(error)
    }
}
impl From<&'static str> for ExprEvaluationFailure {
    fn from(error: &'static str) -> Self {
        Self::Mathematical(error.to_owned())
    }
}
impl From<ExecutionFailure> for ExprEvaluationFailure {
    fn from(error: ExecutionFailure) -> Self {
        Self::Runtime(error)
    }
}
impl From<ConstructionFailure> for ExprEvaluationFailure {
    fn from(error: ConstructionFailure) -> Self {
        Self::Runtime(error.into())
    }
}
impl From<ExprEvaluationFailure> for ExecutionFailure {
    fn from(error: ExprEvaluationFailure) -> Self {
        match error {
            ExprEvaluationFailure::Mathematical(message) => message.into(),
            ExprEvaluationFailure::Runtime(error) => error,
        }
    }
}
impl std::fmt::Display for ExprEvaluationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mathematical(message) => message.fmt(f),
            Self::Runtime(error) => error.fmt(f),
        }
    }
}

/// A recursive evaluator borrows facts and the caller's exact error vocabulary.
/// The reference arena retains its historical text-only, untyped entrypoint.
/// Runtime callers use `ExprEvaluationContext`, whose recursive result is the
/// original `ExecutionResult`; no typed refusal is rendered and reconstructed.
pub trait ExprEvaluation {
    type Error: From<String> + From<&'static str> + std::fmt::Display;
    fn arena(&self) -> &ExprArena;
    fn eval(&self, id: ExprId, chunk: &Chunk) -> Result<ArrayRef, Self::Error> {
        super::evaluate(self, id, chunk)
    }
    fn slice_input(
        &self,
        chunk: &Chunk,
        offset: usize,
        length: usize,
    ) -> Result<Chunk, Self::Error>;
    /// Only the pre-existing text-only mathematical error may select another
    /// legacy evaluation geometry. Typed control/creation failures are final.
    fn recoverable_error(&self, error: &Self::Error) -> bool;
    fn annotate_error(
        &self,
        error: Self::Error,
        decorate: impl FnOnce(&str) -> String,
    ) -> Self::Error;
    fn node(&self, id: ExprId) -> Option<&ExprNode> {
        self.arena().node(id)
    }
    fn data_type(&self, id: ExprId) -> Option<&DataType> {
        self.arena().data_type(id)
    }
    fn value_type(&self, id: ExprId) -> Option<&FunctionValueType> {
        self.arena().value_type(id)
    }
    fn field_schema(&self, id: ExprId) -> Option<&ChunkFieldSchema> {
        self.arena().field_schema(id)
    }
    fn decimal_overflow_policy(&self, id: ExprId) -> Option<DecimalOverflowPolicy> {
        self.arena().decimal_overflow_policy(id)
    }
    fn allow_throw_exception(&self) -> bool {
        self.arena().allow_throw_exception()
    }
    fn query_global_dict(
        &self,
        slot: SlotId,
    ) -> Option<&Arc<std::collections::HashMap<i32, Vec<u8>>>> {
        self.arena().query_global_dict(slot)
    }
    fn session_time_zone(&self) -> Option<&str> {
        self.arena().session_time_zone()
    }
    fn require_runtime_binding(&self) -> Result<(), Self::Error> {
        self.arena()
            .runtime_error_binding()
            .map(|_| ())
            .map_err(|_| {
                Self::Error::from("SLEEP requires an exact fragment runtime binding".to_owned())
            })
    }
    fn check_runtime_error(&self) -> Result<(), Self::Error> {
        self.arena().check_runtime_error().map_err(Into::into)
    }
    fn wait_interruptibly(&self, duration: std::time::Duration) -> Result<(), Self::Error> {
        self.arena()
            .wait_interruptibly(duration)
            .map_err(Into::into)
    }
}

impl ExprEvaluation for ExprArena {
    type Error = String;
    fn arena(&self) -> &ExprArena {
        self
    }
    fn slice_input(&self, chunk: &Chunk, offset: usize, length: usize) -> Result<Chunk, String> {
        if chunk.chunk_schema().is_typed() {
            return Err(ConstructionFailure::MissingOwner.to_string());
        }
        Ok(chunk.slice(offset, length))
    }
    fn recoverable_error(&self, _: &String) -> bool {
        true
    }
    fn annotate_error(&self, error: String, decorate: impl FnOnce(&str) -> String) -> String {
        decorate(&error)
    }
}
impl ExprEvaluation for Arc<ExprArena> {
    type Error = String;
    fn arena(&self) -> &ExprArena {
        self.as_ref()
    }
    fn slice_input(&self, chunk: &Chunk, offset: usize, length: usize) -> Result<Chunk, String> {
        self.as_ref().slice_input(chunk, offset, length)
    }
    fn recoverable_error(&self, _: &String) -> bool {
        true
    }
    fn annotate_error(&self, error: String, decorate: impl FnOnce(&str) -> String) -> String {
        decorate(&error)
    }
}

/// One actual evaluation borrows the owner's construction scope and control.
/// The loan cannot outlive any of them and is never attached to the shared
/// arena, input Chunk, function catalogue, a thread-local, or a global registry.
pub(crate) struct ExprEvaluationContext<'a> {
    arena: &'a ExprArena,
    owner: &'a dyn InputConstructionOwner,
    control: &'a dyn ExprEvaluationControl,
}
impl<'a> ExprEvaluationContext<'a> {
    pub fn new(
        arena: &'a ExprArena,
        owner: &'a dyn InputConstructionOwner,
        control: &'a dyn ExprEvaluationControl,
    ) -> Self {
        Self {
            arena,
            owner,
            control,
        }
    }
    pub fn eval(&self, id: ExprId, chunk: &Chunk) -> ExecutionResult<ArrayRef> {
        <Self as ExprEvaluation>::eval(self, id, chunk).map_err(Into::into)
    }
    /// A derived lexical arena changes facts only, retaining the same runtime
    /// owner and control. No new authority is inferred from the new row shape.
    pub fn reborrow<'b>(&'b self, arena: &'b ExprArena) -> ExprEvaluationContext<'b> {
        ExprEvaluationContext::new(arena, self.owner, self.control)
    }
}
impl ExprEvaluation for ExprEvaluationContext<'_> {
    type Error = ExprEvaluationFailure;
    fn eval(&self, id: ExprId, chunk: &Chunk) -> Result<ArrayRef, Self::Error> {
        self.control.checkpoint()?;
        let value = super::evaluate(self, id, chunk)?;
        self.control.checkpoint()?;
        Ok(value)
    }
    fn arena(&self) -> &ExprArena {
        self.arena
    }
    fn slice_input(
        &self,
        chunk: &Chunk,
        offset: usize,
        length: usize,
    ) -> Result<Chunk, Self::Error> {
        self.control.checkpoint()?;
        let result = chunk.slice_with_creation_observed::<ExecutionFailure, _>(
            offset,
            length,
            self.owner.storage_lifetime(),
            |step| {
                self.control.checkpoint()?;
                self.owner.observe(0, &[], step)?;
                Ok(())
            },
        )?;
        self.control.checkpoint()?;
        Ok(result)
    }
    fn recoverable_error(&self, error: &Self::Error) -> bool {
        matches!(error, ExprEvaluationFailure::Mathematical(_))
    }
    fn annotate_error(
        &self,
        error: Self::Error,
        decorate: impl FnOnce(&str) -> String,
    ) -> Self::Error {
        match error {
            ExprEvaluationFailure::Mathematical(message) => decorate(&message).into(),
            error @ ExprEvaluationFailure::Runtime(_) => error,
        }
    }
    fn require_runtime_binding(&self) -> Result<(), Self::Error> {
        // The actual invocation has already supplied its runtime control loan.
        // It does not obtain authority from an optional shared-arena field.
        self.control.checkpoint().map_err(Into::into)
    }
    fn check_runtime_error(&self) -> Result<(), Self::Error> {
        self.control.checkpoint().map_err(Into::into)
    }
    fn wait_interruptibly(&self, duration: std::time::Duration) -> Result<(), Self::Error> {
        self.control.wait(duration).map_err(Into::into)
    }
}
