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

//! Wire preflight and the structural plan contract shared by compiled packages.

use crate::physical_package_v2::PackageDecodeLimits;
use crate::resource_preflight_v2::{DecodeProjectionLimits, PROST_RECURSION_LIMIT};
use novarocks_physical_plan as p;

impl PackageDecodeLimits {
    pub const FROZEN: Self = Self {
        // Native carrier and Tasks own capacity. This pass measures the actual
        // generated-layout requests and retains the decoder's recursion limit.
        wire: DecodeProjectionLimits {
            max_input_bytes: usize::MAX,
            max_requested_heap_bytes: usize::MAX,
            max_message_occurrences: usize::MAX,
            max_scalar_elements: usize::MAX,
            max_field_occurrences: usize::MAX,
            max_copied_bytes: usize::MAX,
            max_initialization_bytes: usize::MAX,
            max_wire_depth: PROST_RECURSION_LIMIT,
        },
        plan_limits: p::PlanLimits::FROZEN,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }

    #[test]
    fn production_wire_projection_counts_legal_occurrences_without_a_second_quota() {
        use crate::resource_preflight_v2::{
            FragmentDecodeResourceModel, ResourceCursorStatus, ResourceModelError,
        };
        use novarocks_proto_models::physical_package_v2::FragmentPackage;
        use prost::Message;
        use prost::encoding::{WireType, encode_key, encode_varint};

        // Lawful unknown varints fit the existing native carrier. Their
        // number is accounted for, but is not a separate allocation quota.
        // Required package semantics remain the regular receiver's concern.
        const OCCURRENCES: usize = 1_000_001;
        let mut field = Vec::new();
        encode_key(100_000, WireType::Varint, &mut field);
        encode_varint(0, &mut field);
        let raw = field.repeat(OCCURRENCES);
        let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
        let limits = PackageDecodeLimits::FROZEN.wire;
        let projection = model.preflight(&raw, limits, &Control).unwrap();
        assert_eq!(projection.status, ResourceCursorStatus::Complete);
        assert_eq!(projection.usage.field_occurrences, OCCURRENCES);
        assert_eq!(projection.usage.input_bytes, raw.len());
        FragmentPackage::decode(raw.as_slice()).expect("the pinned decoder accepts lawful fields");

        // Explicit caller policies are still enforced. Only the unexplained
        // production default quota was removed, not the caller's authority.
        assert!(matches!(
            model.preflight(
                &raw,
                DecodeProjectionLimits {
                    max_field_occurrences: OCCURRENCES - 1,
                    ..limits
                },
                &Control,
            ),
            Err(ResourceModelError::Limit(
                "protobuf field occurrences exceed caller limit"
            ))
        ));
    }

    #[test]
    fn input_derived_projection_accounts_for_wide_scalar_type_tables() {
        use crate::physical_type_v2::encode_borrowed_type_namespaces_sources_in;
        use arrow::datatypes::DataType;
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase, FunctionValueType};
        const ROOTS: u32 = 2000;
        let roots = (0..ROOTS)
            .map(|id| (id, FunctionValueType::new(DataType::Int64, true)))
            .collect::<Vec<_>>();
        let inputs = roots.iter().map(|(id, ty)| (*id, ty)).collect::<Vec<_>>();
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let mut last = None;
        let table = encode_borrowed_type_namespaces_sources_in(
            &inputs,
            &[],
            &[],
            &[],
            std::mem::size_of_val(roots.as_slice()),
            None,
            &mut |facts| {
                last = Some(*facts);
                Ok(())
            },
            &mut work,
        )
        .expect("the source determines the wide scalar type table");
        work.finish().unwrap();
        assert_eq!(table.as_wire().value_types.len(), ROOTS as usize);
        let facts = last.expect("the sender admitted its type projection");
        assert!(
            facts.allocation_requests_upper_bound < 4 * ROOTS as usize + 1024,
            "{facts:?}"
        );
    }
}
