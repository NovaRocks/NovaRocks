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
//! Source-paired per-request layouts, before SAME host band/admission policy.
//! These probes do not grant memory or claim complete analytic stage funding.
use super::*;
use novarocks_memory::attribution::band;
use novarocks_type_contract::owned_resources::metadata_materialization::MetadataAllocationLoan;
use std::alloc::Layout;

#[test]
fn by_schema_allocation_stream_equals_original_request_contribution() {
    let (source, _) = source();
    let bound = layout(source, &Control::default());
    let contract = ChunkSchema::from_compiled_layout(&bound).unwrap();
    let mut original_steps = 0;
    let old = contract
        .original_reconcile_metadata_request_observed::<KernelCloneProbeError>(
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut || {
                original_steps += 1;
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
    let mut new_steps = 0;
    let mut requests = 0;
    let mut allocations =
        |request: Layout, occurrences: usize| -> Result<(), KernelCloneProbeError> {
            assert_ne!(request.size(), 0);
            assert_ne!(occurrences, 0);
            requests += request.size() * occurrences;
            Ok(())
        };
    let actual = contract
        .original_reconcile_metadata_allocation_requests_observed::<KernelCloneProbeError>(
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut || {
                new_steps += 1;
                Ok(())
            },
            &mut Some(&mut allocations as MetadataAllocationLoan<'_, KernelCloneProbeError>),
        )
        .unwrap()
        .unwrap();
    assert_eq!(actual, old);
    assert_eq!(new_steps, original_steps);
    assert_eq!(requests, actual.cumulative_requests_bytes);
}

#[test]
fn by_schema_allocation_shared_metadata_requests_only_the_original_field_name() {
    let mut map = MaterializedMetadataMap::with_capacity(160);
    for n in 0..160 {
        map.insert(format!("k{n:03}"), "v".to_owned());
    }
    let field = map.into_field(Field::new("n", DataType::Int64, false));
    let mut raw = 0;
    let mut tagged = 0;
    let mut small = 0;
    let mut allocations =
        |request: Layout, occurrences: usize| -> Result<(), KernelCloneProbeError> {
            raw += request.size() * occurrences;
            if band::is_tagged(request.size()) {
                tagged += band::tagged_layout(request).unwrap().size() * occurrences;
            } else {
                small += request.size() * occurrences;
            }
            Ok(())
        };
    let facts = field
        .original_field_clone_allocation_requests_observed(
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut || Ok(()),
            &mut Some(&mut allocations as MetadataAllocationLoan<'_, KernelCloneProbeError>),
        )
        .unwrap();
    assert_eq!(raw, facts.request_bytes);
    // Arrow 60 shares the immutable Metadata Arc: the 320 metadata strings
    // and every tree node make no new allocation request. The actual Field
    // clone still allocates its one-byte name, through the same host band.
    assert_eq!(facts.metadata.table_backing, None);
    assert_eq!(facts.metadata.text_requests, 0);
    assert_eq!(facts.metadata.text_bytes, 0);
    assert_eq!(raw, "n".len());
    assert_eq!(small, "n".len());
    assert_eq!(tagged, 0);
}

#[test]
fn by_schema_allocation_every_callback_retains_seven_first_causes_and_no_footer() {
    use novarocks_functions::{KernelDiagnostic, KernelFailure};
    let (source, _) = source();
    let bound = layout(source, &Control::default());
    let contract = ChunkSchema::from_compiled_layout(&bound).unwrap();
    let count = std::cell::Cell::new(0);
    let mut ok = |_: Layout, _: usize| -> Result<(), KernelCloneProbeError> {
        count.set(count.get() + 1);
        Ok(())
    };
    contract
        .original_reconcile_metadata_allocation_requests_observed(
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut || {
                count.set(count.get() + 1);
                Ok(())
            },
            &mut Some(&mut ok as MetadataAllocationLoan<'_, KernelCloneProbeError>),
        )
        .unwrap();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("allocation source")),
        KernelFailure::Internal(KernelDiagnostic::new("allocation source")),
        KernelFailure::Operational(KernelDiagnostic::new("allocation source")),
        KernelFailure::InstanceFailed,
    ] {
        for at in 0..count.get() {
            let calls = std::cell::Cell::new(0);
            let check = || -> Result<(), KernelCloneProbeError> {
                let ordinal = calls.get();
                calls.set(ordinal + 1);
                if ordinal == at {
                    Err(KernelCloneProbeError::Kernel(cause.clone()))
                } else {
                    Ok(())
                }
            };
            let mut allocations = |_: Layout, _: usize| check();
            let result = contract.original_reconcile_metadata_allocation_requests_observed(
                &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
                &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
                &mut || check(),
                &mut Some(&mut allocations as MetadataAllocationLoan<'_, KernelCloneProbeError>),
            );
            assert!(matches!(result, Err(KernelCloneProbeError::Kernel(ref e)) if e == &cause));
            assert_eq!(calls.get(), at + 1);
        }
    }
}
