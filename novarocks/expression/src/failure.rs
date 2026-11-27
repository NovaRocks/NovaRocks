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

use crate::*;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, ResourceDimension, ResourceFacts,
    domain,
};
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::EXPRESSION, std::num::NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for ExpressionsCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Expression(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for StaticExpressionError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::RuntimeBoundArena => fact(FailureClass::Invariant, 1),
            Self::TooManyNodes => limit(2, ResourceDimension::Items, crate::MAX_STATIC_EXPRESSIONS),
            Self::InvalidReference => fact(FailureClass::Invariant, 3),
            Self::TooDeep => limit(
                4,
                ResourceDimension::Items,
                crate::MAX_STATIC_EXPRESSION_DEPTH,
            ),
            Self::InvalidMetadataArity => fact(FailureClass::Invariant, 7),
            Self::ConstantTypeMismatch => fact(FailureClass::Invariant, 9),
        }
    }
}

fn limit(code: u16, dimension: ResourceDimension, maximum: usize) -> FailureFact {
    fact(FailureClass::Resource, code).with_resource(ResourceFacts {
        request_scope: None,
        constraint_scope: None,
        dimension,
        requested: None,
        required_growth: None,
        limit: u64::try_from(maximum).ok(),
        live: None,
        committed: None,
    })
}

impl FailureProjection for PrimitiveError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(e) => e.failure_fact(),
            Self::Comparison(e) => e.failure_fact(),
            Self::Arithmetic(e) => e.failure_fact(),
            Self::Cast(e) => e.failure_fact(),
            Self::Invalid(_) => fact(FailureClass::Invariant, 20),
        }
    }
}
impl FailureProjection for ExprCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Construction(e) => e.failure_fact(),
            Self::BTree(e) => e.failure_fact(),
            Self::Publication(e) => e.failure_fact(),
            Self::Control(e) => e.failure_fact(),
            Self::Flow(e) => e.failure_fact(),
            Self::Primitive(e) => e.failure_fact(),
            Self::Constant(e) => e.failure_fact(),
            Self::Reference(e) => e.failure_fact(),
            Self::Binding(e) => e.failure_fact(),
            Self::Type(e) => e.failure_fact(),
            Self::Expressions(e) => e.failure_fact(),
            Self::Specialization(e) => e.failure_fact(),
            Self::Effects(e) => e.failure_fact(),
            Self::Arithmetic(e) => e.failure_fact(),
            Self::Cast(e) => e.failure_fact(),
            Self::Comparison(e) => e.failure_fact(),
            Self::UnsupportedExpression(_) | Self::UnsupportedCall(_) => {
                fact(FailureClass::Unsupported, 21)
            }
            Self::Invalid(_) => fact(FailureClass::Invariant, 22),
        }
    }
}
impl<H: FailureProjection> FailureProjection for ExprCompileFailure<H> {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(e) => e.failure_fact(),
            Self::Creator(e) => e.failure_fact(),
            Self::Resource(e) => e.failure_fact(),
            Self::Owner(e) => e.failure_fact(),
        }
    }
}
