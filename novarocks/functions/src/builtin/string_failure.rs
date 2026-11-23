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

//! String output allocation failures retain the exact source and layout request.
use crate::KernelFailure;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, OuterFailureProjection,
    ResourceDimension, ResourceFacts, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Debug)]
enum StringOuterFailure {
    ArrayLayout {
        count: usize,
        width: usize,
        source: std::alloc::LayoutError,
    },
    Reservation {
        count: usize,
        width: usize,
        source: std::collections::TryReserveError,
    },
}
impl fmt::Display for StringOuterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArrayLayout {
                count,
                width,
                source,
            } => write!(
                f,
                "string output layout for {count} elements of {width} bytes: {source}"
            ),
            Self::Reservation {
                count,
                width,
                source,
            } => write!(
                f,
                "string output reservation for {count} elements of {width} bytes: {source}"
            ),
        }
    }
}
impl Error for StringOuterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::ArrayLayout { source, .. } => source as &dyn Error,
            Self::Reservation { source, .. } => source as &dyn Error,
        })
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for StringOuterFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::ArrayLayout { count, width, .. } | Self::Reservation { count, width, .. } => {
                fact(
                    FailureClass::Resource,
                    if matches!(self, Self::ArrayLayout { .. }) {
                        355
                    } else {
                        356
                    },
                )
                .with_resource(ResourceFacts {
                    request_scope: None,
                    constraint_scope: None,
                    dimension: ResourceDimension::Bytes,
                    requested: u64::try_from(*count as u128 * *width as u128).ok(),
                    required_growth: None,
                    limit: None,
                    live: None,
                    committed: None,
                })
            }
        }
    }
}
impl OuterFailureProjection for StringOuterFailure {}
#[cfg(test)]
pub(super) fn original_source<E: Error + 'static>(failure: &KernelFailure) -> &E {
    let mut source: &(dyn Error + 'static) = failure;
    loop {
        if let Some(source) = source.downcast_ref::<E>() {
            return source;
        }
        source = source
            .source()
            .expect("original typed SDK cause must remain available");
    }
}

/// A layout observation is not an allocation grant.
pub(super) fn array_layout<T>(count: usize) -> Result<std::alloc::Layout, KernelFailure> {
    std::alloc::Layout::array::<T>(count).map_err(|source| {
        KernelFailure::from_domain(StringOuterFailure::ArrayLayout {
            count,
            width: std::mem::size_of::<T>(),
            source,
        })
    })
}
pub(super) fn reservation<T>(
    count: usize,
    source: std::collections::TryReserveError,
) -> KernelFailure {
    KernelFailure::from_domain(StringOuterFailure::Reservation {
        count,
        width: std::mem::size_of::<T>(),
        source,
    })
}

#[cfg(test)]
mod fixed_storage_properties {
    use super::*;
    #[test]
    fn actual_layout_failure_retains_original_std_error_without_input_representation_claim() {
        let failure = array_layout::<u64>(usize::MAX).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 355);
        assert_eq!(fact.representation(), None);
        assert_eq!(fact.resource().unwrap().limit, None);
        assert_eq!(fact.resource().unwrap().committed, None);
        assert_eq!(
            fact.resource().unwrap().requested,
            u64::try_from(usize::MAX as u128 * 8).ok()
        );
        let _ = original_source::<std::alloc::LayoutError>(&failure);
    }
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn fixed_i32_layout_has_no_variable_width_i32_byte_offset_limit() {
        let rows = i32::MAX as usize / 4 + 1;
        let layout = array_layout::<i32>(rows).unwrap();
        assert_eq!(layout.size(), rows * 4);
        assert!(layout.size() > i32::MAX as usize);
        // This observes only a layout. No memory is allocated or authorized.
    }
}
