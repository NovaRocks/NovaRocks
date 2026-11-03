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

//! Execution's domain envelope uses the common local cause carrier.
pub use novarocks_failure_contract::SharedFailureCause as SharedDomainFailure;

#[cfg(test)]
mod tests {

    use crate::runtime::task::{ExecutionFailure, ExecutionFailureCause, PipelineOperation};
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, ResourceDimension,
        ResourceFacts, ResourceScope, domain,
    };
    use std::{error::Error, fmt, num::NonZeroU16};

    #[derive(Debug)]
    struct CapacityCause {
        requested: u64,
        private_detail: &'static str,
        broken_formatter: bool,
    }
    impl fmt::Display for CapacityCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            assert!(!self.broken_formatter, "injected foreign formatter panic");
            f.write_str(self.private_detail)
        }
    }
    impl Error for CapacityCause {}
    impl FailureProjection for CapacityCause {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::MEMORY, NonZeroU16::new(60000).unwrap()),
            )
            .with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Operator),
                constraint_scope: Some(ResourceScope::Query),
                dimension: ResourceDimension::Bytes,
                requested: Some(self.requested),
                required_growth: None,
                limit: Some(32),
                live: None,
                committed: None,
            })
        }
    }

    #[test]
    fn new_domain_projects_without_a_central_type_switch_and_keeps_one_source() {
        let cause = CapacityCause {
            requested: 64,
            private_detail: "credential-marker-not-for-status",
            broken_formatter: false,
        };
        let expected = cause.failure_fact();
        let failure =
            ExecutionFailure::from_domain(cause).at_operator(2, PipelineOperation::Preparation);
        let copy = failure.clone().at_operator(99, PipelineOperation::Pull);
        assert_eq!(failure.failure_fact(), expected);
        assert_eq!(copy.failure_fact(), expected);
        assert_eq!(copy.site(), failure.site());
        let source = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<CapacityCause>()
            .unwrap();
        let copied_source = copy
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<CapacityCause>()
            .unwrap();
        assert!(std::ptr::eq(source, copied_source));
        assert_eq!(source.requested, 64);
        assert!(failure.detail().contains("credential-marker"));
        assert!(!failure.safe_detail().as_str().contains("credential-marker"));
        assert!(
            matches!(failure.cause(), ExecutionFailureCause::Domain(shared) if shared.downcast_ref::<CapacityCause>().is_some())
        );
    }

    #[test]
    fn failing_local_formatter_cannot_replace_the_original_domain_fact() {
        let cause = CapacityCause {
            requested: 128,
            private_detail: "unused",
            broken_formatter: true,
        };
        let expected = cause.failure_fact();
        let failure = ExecutionFailure::from_domain(cause);
        assert_eq!(failure.failure_fact(), expected);
        assert_eq!(failure.detail(), "domain failure detail unavailable");
        let source = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<CapacityCause>()
            .unwrap();
        assert!(source.broken_formatter);
        assert_eq!(source.requested, 128);
        assert!(format!("{failure:?}").contains("requested: Some(128)"));
    }
}
