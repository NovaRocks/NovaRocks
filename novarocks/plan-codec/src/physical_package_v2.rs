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

//! Whole-package byte admission before the original generated Prost decoder.
//! The resulting DTO is untrusted input to the typed package composer; this
//! stage does not publish a PhysicalPlan FragmentPackage or bind capabilities.

use crate::resource_preflight_v2::{
    DecodeProjectionLimits, DecodeResourceProjection, DecodeResourceUsage,
    FragmentDecodeResourceModel, ResourceCursorStatus, ResourceModelError,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, PureCompileControl};
use prost::Message;
use std::fmt;

mod binding_sources;
mod decode;
mod definition_sources;
mod encode;
mod nodes;
mod provider_sources;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
mod type_sources;
mod type_views;

pub use binding_sources::BindingSourceLimits;
pub use decode::{
    PackageDecodeError, PackageDecodeLimits, decode_fragment_package,
    decode_fragment_package_with_type_host,
};
pub use definition_sources::{
    DefinitionSourceLimits, FragmentDefinitionSource, visit_fragment_definitions_observed,
};
pub use encode::{PackageEncodeError, PackageEncodeLimits, encode_fragment_package};
pub use provider_sources::{ProviderSourceError, ProviderSourceLimits};
pub use type_views::{TypeViewError, TypeViewLimits};

#[derive(Debug)]
pub enum PackageWireError {
    Control(CompileControlError),
    ResourceModel(ResourceModelError),
    Protobuf(prost::DecodeError),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for PackageWireError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ResourceModelError> for PackageWireError {
    fn from(error: ResourceModelError) -> Self {
        match error {
            ResourceModelError::Control(cause) => Self::Control(cause),
            error => Self::ResourceModel(error),
        }
    }
}
impl fmt::Display for PackageWireError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(output),
            Self::ResourceModel(error) => error.fmt(output),
            Self::Protobuf(error) => error.fmt(output),
            Self::InvalidSource(message) => output.write_str(message),
        }
    }
}
impl std::error::Error for PackageWireError {}

/// Immutable projection of this exact byte slice under the generated root
/// model. It cannot be retargeted to another input or another caller control.
/// Scanner/model/host stock and typed reconstruction remain separate owners.
pub struct PreparedPackageWire<'raw, 'control> {
    raw: &'raw [u8],
    projection: DecodeResourceProjection,
    control: &'control dyn PureCompileControl,
}

impl PreparedPackageWire<'_, '_> {
    pub fn projection(&self) -> &DecodeResourceProjection {
        &self.projection
    }

    /// Admit the complete captured DTO/error contribution again before Prost
    /// allocates. Prefix snapshots replace this child's previous contribution;
    /// the parent owns coexistence, entry and ordinary/success completion.
    pub fn materialize_in(
        self,
        admit: &mut dyn FnMut(&DecodeResourceUsage) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<wire::FragmentPackage, PackageWireError> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(PackageWireError::InvalidSource(
                "package wire materialization uses a different caller control",
            ));
        }
        admit(&self.projection.usage)?;
        work.flush()?;
        // This is the original library's format verdict. Malformed-prefix
        // projection accounts the decoder/error prefix but is not acceptance.
        let decoded =
            wire::FragmentPackage::decode(self.raw).map_err(PackageWireError::Protobuf)?;
        if self.projection.status != ResourceCursorStatus::Complete {
            return Err(PackageWireError::InvalidSource(
                "generated package decoder accepted an incomplete resource projection",
            ));
        }
        work.step()?;
        work.flush()?;
        Ok(decoded)
    }
}

/// Capture all original wire occurrences before DTO decoding, including
/// overwritten singular fields and unknown fields. No semantic table, source
/// invoice, receive policy or default package component is inferred here.
pub fn prepare_package_wire_in<'raw, 'control>(
    raw: &'raw [u8],
    model: &FragmentDecodeResourceModel,
    limits: DecodeProjectionLimits,
    admit: &mut dyn FnMut(&DecodeResourceUsage) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedPackageWire<'raw, 'control>, PackageWireError> {
    let projection = model.preflight_in(raw, limits, admit, work)?;
    Ok(PreparedPackageWire {
        raw,
        projection,
        control: work.control(),
    })
}

#[cfg(test)]
#[path = "physical_package_v2/wire_tests.rs"]
mod wire_tests;
