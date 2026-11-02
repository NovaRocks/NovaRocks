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

//! Shared semantic definitions in one explicit owner's sparse namespace.
//!
//! These are post-Prost projections. Raw DTO admission and source retention
//! remain with the caller. No physical layout, metadata, runtime encoding or
//! BE consumer capability is recovered or carried in this semantic table.

use std::{alloc::Layout, fmt};

use novarocks_proto_models::value_type as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ControlResourceError, ExecutionValueTypeError,
    ValueTypeRef, ValueTypeStorageLifetime,
};

use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};

pub(crate) mod decode;
mod encode;
mod graph;
pub(crate) mod resources;
pub(crate) mod scalars;

/// Explicit caller profile, independent of numeric ID magnitude.
/// Expanded nodes count every definition's unfolded semantic subtree.
#[derive(Clone, Copy, Debug)]
pub struct LogicalTypeTableLimits {
    pub max_definitions: usize,
    pub max_expanded_nodes: usize,
    pub max_string_bytes: usize,
}

#[derive(Debug)]
pub enum LogicalTypeCodecError {
    InvalidShape(&'static str),
    Control(CompileControlError),
    ValueType(ExecutionValueTypeError),
    ResourceSource(&'static str),
    Arrow(arrow::error::ArrowError),
}
impl fmt::Display for LogicalTypeCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape(message) | Self::ResourceSource(message) => f.write_str(message),
            Self::Control(error) => error.fmt(f),
            Self::ValueType(error) => error.fmt(f),
            Self::Arrow(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for LogicalTypeCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::ValueType(source) => Some(source),
            Self::Arrow(source) => Some(source),
            Self::InvalidShape(_) | Self::ResourceSource(_) => None,
        }
    }
}
impl From<CompileControlError> for LogicalTypeCodecError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExecutionValueTypeError> for LogicalTypeCodecError {
    fn from(error: ExecutionValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl<H> From<CompileControlError> for ProjectionFailure<LogicalTypeCodecError, H> {
    fn from(error: CompileControlError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<ControlResourceError> for ProjectionFailure<LogicalTypeCodecError, H> {
    fn from(error: ControlResourceError) -> Self {
        Self::Codec(match error {
            ControlResourceError::Control(error) => error.into(),
            ControlResourceError::SourceModel(message) => {
                LogicalTypeCodecError::ResourceSource(message)
            }
        })
    }
}
type Failure<H> = ProjectionFailure<LogicalTypeCodecError, H>;

/// Borrow one genuine author's root without cloning a replacement source.
#[derive(Clone, Copy, Debug)]
pub struct LogicalTypeRoot<'a> {
    pub id: u32,
    pub value: &'a ValueTypeRef,
}

/// One namespace owner. Caller root IDs are reserved before children are minted.
/// Encoding defines child occurrences; it performs no interning or address lookup.
pub struct LogicalTypeTableBuilder<'a> {
    roots: &'a [LogicalTypeRoot<'a>],
}
impl<'a> LogicalTypeTableBuilder<'a> {
    pub fn new(roots: &'a [LogicalTypeRoot<'a>]) -> Self {
        Self { roots }
    }

    /// Append no partially constructed table to the caller on refusal.
    /// The caller owns scope entry, ordinary/success tails and all cleanup.
    pub fn encode_observed<H>(
        &self,
        limits: LogicalTypeTableLimits,
        request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<wire::LogicalTypeDefinition>, Failure<H>> {
        encode::encode(
            self.roots,
            limits,
            &mut resources::Context {
                observe_scope: None,
                request,
                work,
            },
        )
    }
}

/// Sparse map backed only by the actual number of definitions, sorted by ID.
pub struct DecodedLogicalTypeTable {
    values: Vec<(u32, ValueTypeRef, usize)>,
    _storage: ValueTypeStorageLifetime,
}
impl DecodedLogicalTypeTable {
    pub fn len(&self) -> usize {
        self.values.len()
    }
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (u32, &ValueTypeRef)> {
        self.values.iter().map(|(id, value, _)| (*id, value))
    }
    pub(crate) fn retained_node_source_bytes_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<usize>, LogicalTypeCodecError> {
        Ok(resources::find(&self.values, id, |entry| entry.0, work)?
            .map(|position| self.values[position].2))
    }
    /// Every binary-search comparison belongs to the original caller's work.
    pub fn get_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ValueTypeRef>, LogicalTypeCodecError> {
        Ok(resources::find(&self.values, id, |entry| entry.0, work)?
            .map(|position| &self.values[position].1))
    }
}

/// Validate every definition and the complete reference graph before building
/// immutable types. Unreferenced definitions are not exempt from validation.
/// Each independent retained node/field carries the supplied storage lifetime.
pub fn decode_logical_type_table_observed<H>(
    definitions: &[wire::LogicalTypeDefinition],
    limits: LogicalTypeTableLimits,
    storage: ValueTypeStorageLifetime,
    request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedLogicalTypeTable, Failure<H>> {
    decode::decode(
        definitions,
        limits,
        storage,
        &mut resources::Context {
            observe_scope: None,
            request,
            work,
        },
    )
}

/// Materialize independent entities on the actual caller factory. The complete
/// graph and total grammar bounds are validated by the same decode body.
pub fn decode_logical_type_table_with_factory_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    definitions: &[wire::LogicalTypeDefinition],
    limits: LogicalTypeTableLimits,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedLogicalTypeTable, Failure<H>> {
    // A factory routes every operation to its explicit scope. This callback
    // is uninhabited on that path, rather than a successful dummy admission.
    let mut unreachable_request = |_: Layout| -> Result<(), AdmissionRefusal<H>> {
        unreachable!("scoped logical decoder lost its explicit factory")
    };
    decode::decode_with_factory(
        definitions,
        limits,
        ValueTypeStorageLifetime::pure(),
        Some(factory),
        &mut resources::Context {
            observe_scope: None,
            request: &mut unreachable_request,
            work,
        },
    )
}

/// The parent supplies its original linear numeric-model observer. The model
/// observes each real scope/header and payload request before actual creation,
/// without charging a second host or resetting complete-operation limits.
pub(crate) fn decode_logical_type_table_with_factory_observed_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    definitions: &[wire::LogicalTypeDefinition],
    limits: LogicalTypeTableLimits,
    factory: &F,
    observe: &mut dyn FnMut(Layout) -> Result<(), Failure<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedLogicalTypeTable, Failure<H>> {
    let mut unreachable_request = |_: Layout| -> Result<(), AdmissionRefusal<H>> {
        unreachable!("scoped logical decoder lost its explicit factory")
    };
    decode::decode_with_factory(
        definitions,
        limits,
        ValueTypeStorageLifetime::pure(),
        Some(factory),
        &mut resources::Context {
            request: &mut unreachable_request,
            work,
            observe_scope: Some(observe),
        },
    )
}

#[cfg(test)]
mod tests;
