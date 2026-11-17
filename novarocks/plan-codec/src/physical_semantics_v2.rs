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

//! Typed projection of the v2 semantic components after resource-safe DTO
//! admission. This is neither raw-byte preflight nor a complete package codec.
//! Decoded parts must enter the same checked `FragmentPackage` constructor to
//! prove parameter closure and actual pruning consumers. Exact installed owner
//! refinement, implication and allocation admission remain separate checks.

use novarocks_physical_plan::{
    Fragment, FragmentPackage, FrozenCallError, FrozenFragmentCalls, FrozenFragmentPruning,
    FrozenPruningError, PredicateSourceError, PruningStructureError,
};
use novarocks_proto_models::physical_semantics_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ExpressionControlFlowError,
    PureCompileControl, SemanticParameterError, SemanticParameters,
};
use std::fmt;

use crate::physical_control_v2::ControlCodecError;

mod calls;
mod owned_resources;
mod parameter_resources;
mod parameters;
mod pruning;

pub use parameter_resources::{
    ParameterProjectionFacts, ParameterProjectionLimits, PreparedSemanticParametersDecode,
    PreparedSemanticParametersEncode, decode_semantic_parameters, encode_semantic_parameters,
    prepare_semantic_parameters_decode, prepare_semantic_parameters_decode_observed_in,
    prepare_semantic_parameters_encode, prepare_semantic_parameters_encode_observed_in,
};

pub use calls::{
    CreatedFrozenCallParts, CreatedFrozenWriterCall,
    decode_frozen_call_parts_input_derived_with_creator_in, decode_frozen_calls_observed,
    decode_frozen_writer_call_with_creator_in, encode_frozen_calls_observed,
    encode_frozen_calls_pure_in, encode_frozen_writer_call_with_creator_in,
};
pub use pruning::{decode_frozen_pruning_observed, encode_frozen_pruning_observed};

pub(crate) use calls::{
    decode_expression_claims_in, decode_frozen_call_entries_observed,
    decode_policy as decode_decimal_policy, decode_site as decode_call_site,
    encode_expression_claims_in, encode_policy as encode_decimal_policy,
    encode_site as encode_call_site,
};
pub use parameters::{
    CreatedWriterSemanticParameters, decode_writer_semantic_parameters_with_creator_in,
};
pub use parameters::{
    decode_reference as decode_semantic_parameter_reference_in,
    encode_reference as encode_semantic_parameter_reference_in,
};
pub(crate) use parameters::{decode_reference, encode_reference};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SemanticsCodecError {
    Control(CompileControlError),
    InvalidShape(&'static str),
    SourceModel(&'static str),
    Parameter(SemanticParameterError),
    Calls(FrozenCallError),
    Pruning(FrozenPruningError),
    ControlComponent(ControlCodecError),
}
impl fmt::Display for SemanticsCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::InvalidShape(message) | Self::SourceModel(message) => f.write_str(message),
            Self::Parameter(error) => error.fmt(f),
            Self::Calls(error) => error.fmt(f),
            Self::Pruning(error) => error.fmt(f),
            Self::ControlComponent(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SemanticsCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Parameter(source) => Some(source),
            Self::Calls(source) => Some(source),
            Self::Pruning(source) => Some(source),
            Self::ControlComponent(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for SemanticsCodecError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<SemanticParameterError> for SemanticsCodecError {
    fn from(error: SemanticParameterError) -> Self {
        Self::Parameter(error)
    }
}
impl From<FrozenCallError> for SemanticsCodecError {
    fn from(error: FrozenCallError) -> Self {
        match error {
            FrozenCallError::Control(error) => Self::Control(error),
            error => Self::Calls(error),
        }
    }
}
impl From<FrozenPruningError> for SemanticsCodecError {
    fn from(error: FrozenPruningError) -> Self {
        match error {
            FrozenPruningError::Control(error)
            | FrozenPruningError::Structure(PruningStructureError::Control(error))
            | FrozenPruningError::Structure(PruningStructureError::Source(
                PredicateSourceError::Control(error),
            )) => Self::Control(error),
            error => Self::Pruning(error),
        }
    }
}
impl From<ControlCodecError> for SemanticsCodecError {
    fn from(error: ControlCodecError) -> Self {
        match error {
            ControlCodecError::Control(error)
            | ControlCodecError::Flow(ExpressionControlFlowError::Control(error)) => {
                Self::Control(error)
            }
            error => Self::ControlComponent(error),
        }
    }
}

fn required_id(value: Option<u32>, message: &'static str) -> Result<u32, SemanticsCodecError> {
    value.ok_or(SemanticsCodecError::InvalidShape(message))
}

fn finish_projection<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, SemanticsCodecError>,
) -> Result<T, SemanticsCodecError> {
    if matches!(&result, Err(SemanticsCodecError::Control(_))) {
        return result;
    }
    // Ordinary failures also owe observation of their completed tail. A
    // previously captured control refusal must keep its original category.
    work.finish()?;
    result
}

/// The three exact DTO fields of a fragment package. Envelope field presence
/// and raw resource admission belong to the complete package codec.
#[derive(Clone, Debug, PartialEq)]
pub struct EncodedFragmentSemantics {
    pub parameters: wire::SemanticParameters,
    pub calls: wire::FrozenCalls,
    pub pruning: wire::FrozenPruning,
}

/// Structural component results, without a claim of package closure or owner
/// authentication. No alternate package constructor is provided here.
pub struct DecodedFragmentSemantics {
    parameters: SemanticParameters,
    calls: FrozenFragmentCalls,
    pruning: FrozenFragmentPruning,
}
impl DecodedFragmentSemantics {
    pub fn into_parts(
        self,
    ) -> (
        SemanticParameters,
        FrozenFragmentCalls,
        FrozenFragmentPruning,
    ) {
        (self.parameters, self.calls, self.pruning)
    }
}

pub fn encode_fragment_semantics(
    package: &FragmentPackage,
    control: &dyn PureCompileControl,
) -> Result<EncodedFragmentSemantics, SemanticsCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let parameters = parameters::encode_parameters(package.parameters(), &mut work)?;
        let calls = calls::encode_calls(package.calls(), &mut work)?;
        let pruning = pruning::encode_pruning(package.pruning(), &mut work)?;
        Ok(EncodedFragmentSemantics {
            parameters,
            calls,
            pruning,
        })
    })();
    finish_projection(work, result)
}

pub fn decode_fragment_semantics(
    fragment: &Fragment,
    encoded: &EncodedFragmentSemantics,
    control: &dyn PureCompileControl,
) -> Result<DecodedFragmentSemantics, SemanticsCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let parameters = parameters::decode_parameters(&encoded.parameters, &mut work)?;
        let calls = calls::decode_calls(fragment, &encoded.calls, &mut work, control)?;
        let pruning = pruning::decode_pruning(fragment.id(), &encoded.pruning, &mut work, control)?;
        Ok(DecodedFragmentSemantics {
            parameters,
            calls,
            pruning,
        })
    })();
    finish_projection(work, result)
}

#[cfg(test)]
mod tests;

/// Explicit FE parameter sender using the original parameter projection body.
/// It borrows its caller's original control/checkpoints and has no creator.
pub fn encode_semantic_parameters_pure_in(
    source: &SemanticParameters,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SemanticParameters, SemanticsCodecError> {
    parameters::encode_parameters_observed(source, work)
}
