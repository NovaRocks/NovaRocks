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

//! Generated-layout input for a pre-Prost resource model. The model concerns
//! fresh DTO decoding from a borrowed byte slice, not merging a caller-owned
//! object, arbitrary Buf implementations, allocator RSS or a MEM grant.

use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::fmt;

mod allocation;
mod scan;
mod schema;
mod wire;

pub const RESOURCE_DECODE_MODEL_REVISION: u32 = 1;

// Prost 0.13's default decoder rejects recursion beyond 100 calls. The
// resource cursor mirrors that actual library boundary and its frame storage;
// this is not an independently chosen package-size or work quota.
pub(crate) const PROST_RECURSION_LIMIT: usize = 100;

// The source proof covers the default Prost recursion model. Feature
// unification must not silently enable the unbounded alternative.
const _: () = assert!(std::mem::size_of::<prost::encoding::DecodeContext>() != 0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceModelError {
    Control(CompileControlError),
    Malformed,
    Limit(&'static str),
    Schema(&'static str),
}
impl From<CompileControlError> for ResourceModelError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl fmt::Display for ResourceModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Malformed => f.write_str("malformed protobuf resource cursor"),
            Self::Limit(message) | Self::Schema(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for ResourceModelError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}

/// No default policy is implied by the internal plan codec. These limits are
/// supplied by the actual caller after its carrier/admission policy resolves.
#[derive(Clone, Copy, Debug)]
pub struct DecodeProjectionLimits {
    pub max_input_bytes: usize,
    pub max_requested_heap_bytes: usize,
    pub max_message_occurrences: usize,
    pub max_scalar_elements: usize,
    pub max_field_occurrences: usize,
    pub max_copied_bytes: usize,
    pub max_initialization_bytes: usize,
    pub max_wire_depth: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecodeResourceUsage {
    pub input_bytes: usize,
    pub root_inline_bytes: usize,
    pub message_occurrences: usize,
    pub scalar_elements: usize,
    pub field_occurrences: usize,
    pub copied_bytes_upper: usize,
    pub initialization_bytes_upper: usize,
    pub cumulative_requested_heap_bytes_upper: usize,
    pub peak_requested_heap_bytes_upper: usize,
    pub error_requested_heap_bytes_upper: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceCursorStatus {
    Complete,
    /// The cursor stopped at malformed input. The accounted prefix bounds the
    /// DTO allocations before Prost reaches that error; this is not acceptance
    /// of the encoding or a replacement for Prost's format verdict.
    MalformedPrefix,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeResourceProjection {
    pub status: ResourceCursorStatus,
    pub usage: DecodeResourceUsage,
}

/// One exact generated root, not an open generic Message implementation whose
/// custom Default/merge behavior could bypass the library resource model.
pub struct FragmentDecodeResourceModel {
    schema: schema::Schema,
    max_message_depth: usize,
}

fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, ResourceModelError>,
) -> Result<T, ResourceModelError> {
    if matches!(&result, Err(ResourceModelError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

impl FragmentDecodeResourceModel {
    pub fn try_new(control: &dyn PureCompileControl) -> Result<Self, ResourceModelError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            if novarocks_proto_models::resource_layout::RESOURCE_LAYOUT_REVISION != 1 {
                return Err(ResourceModelError::Schema(
                    "unsupported generated layout revision",
                ));
            }
            let schema = schema::build::<
                novarocks_proto_models::physical_package_v2::FragmentPackage,
            >(&mut work)?;
            let max_message_depth = scan::message_depth(&schema, &mut work)?;
            Ok(Self {
                schema,
                max_message_depth,
            })
        })();
        finish(work, result)
    }

    /// Bound requested DTO allocation/copy/initialization from every wire
    /// occurrence. Input backing is reported once; caller stock, schema model,
    /// scanner scratch, decoder call frames and allocator rounding require
    /// their own host accounting. No DTO or IPC payload is decoded here.
    pub fn preflight(
        &self,
        raw: &[u8],
        limits: DecodeProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<DecodeResourceProjection, ResourceModelError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = scan::scan(&self.schema, self.max_message_depth, raw, limits, &mut work);
        finish(work, result)
    }

    /// Project the same fresh-Prost DTO occurrences on the caller's meter.
    /// Admission receives replacement snapshots of this one contribution;
    /// input backing is reported once and is not a host/MEM grant. The caller
    /// owns entry and ordinary/success completion. Known numeric refusal is
    /// primary and performs no subsequent progress observation.
    pub fn preflight_in(
        &self,
        raw: &[u8],
        limits: DecodeProjectionLimits,
        admit: &mut dyn FnMut(&DecodeResourceUsage) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<DecodeResourceProjection, ResourceModelError> {
        scan::scan_in(
            &self.schema,
            self.max_message_depth,
            raw,
            limits,
            admit,
            work,
        )
    }
}

#[cfg(test)]
mod schema_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod request_tests;

#[cfg(test)]
mod owner_tests;
