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

//! Exact call identity separates scalar control from relational lifecycles.
// Design: ADR-0236-self-contained-expression-units-and-explicit-environments.md
use crate::{CallProofScope, EvaluationDemand, ExpressionEffectContext, FunctionKind};

/// Issued by the original relation-call author and bound to one actual site.
/// It is fragment-local and never names an expression definition or region.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelationCallId(u32);
impl RelationCallId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelationEffectRole {
    Call,
    MaterializedInput,
}
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelationEffectId {
    pub call: RelationCallId,
    pub role: RelationEffectRole,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationEffectContext {
    pub identity: RelationEffectId,
    pub demand: EvaluationDemand,
}
impl RelationEffectContext {
    pub const fn call(call: RelationCallId) -> Self {
        Self {
            identity: RelationEffectId {
                call,
                role: RelationEffectRole::Call,
            },
            demand: EvaluationDemand::Value,
        }
    }
    pub const fn materialized_input(call: RelationCallId) -> Self {
        Self {
            identity: RelationEffectId {
                call,
                role: RelationEffectRole::MaterializedInput,
            },
            demand: EvaluationDemand::Value,
        }
    }
    pub const fn is_call(self) -> bool {
        matches!(self.identity.role, RelationEffectRole::Call)
            && matches!(self.demand, EvaluationDemand::Value)
    }
    pub const fn is_materialized_input_of(self, call: Self) -> bool {
        matches!(self.identity.role, RelationEffectRole::MaterializedInput)
            && matches!(self.demand, EvaluationDemand::Value)
            && call.is_call()
            && self.identity.call.get() == call.identity.call.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallEffectContext {
    Expression(ExpressionEffectContext),
    Relation(RelationEffectContext),
}
impl From<ExpressionEffectContext> for CallEffectContext {
    fn from(value: ExpressionEffectContext) -> Self {
        Self::Expression(value)
    }
}
impl From<RelationEffectContext> for CallEffectContext {
    fn from(value: RelationEffectContext) -> Self {
        Self::Relation(value)
    }
}
impl CallEffectContext {
    pub const fn expression(self) -> Option<ExpressionEffectContext> {
        match self {
            Self::Expression(value) => Some(value),
            Self::Relation(_) => None,
        }
    }
    pub const fn relation(self) -> Option<RelationEffectContext> {
        match self {
            Self::Relation(value) => Some(value),
            Self::Expression(_) => None,
        }
    }
    pub const fn demand(self) -> EvaluationDemand {
        match self {
            Self::Expression(value) => value.demand,
            Self::Relation(value) => value.demand,
        }
    }
    pub const fn proof_scope(self) -> CallProofScope {
        match self {
            Self::Expression(value) => CallProofScope::Region(value.region),
            Self::Relation(value) => CallProofScope::Relation(value.identity),
        }
    }
    pub fn accepts_proof_scope(self, scope: CallProofScope) -> bool {
        scope == CallProofScope::Unconditional || scope == self.proof_scope()
    }
    pub const fn matches_kind(self, kind: FunctionKind) -> bool {
        match (self, kind) {
            (Self::Expression(_), FunctionKind::Scalar) => true,
            (
                Self::Relation(context),
                FunctionKind::Aggregate | FunctionKind::Window | FunctionKind::Table,
            ) => context.is_call(),
            _ => false,
        }
    }
    /// A merge reads a real expression root or this call's materialized input.
    pub fn is_aggregate_state_input_of(self, call: Self) -> bool {
        let Self::Relation(call) = call else {
            return false;
        };
        if !call.is_call() {
            return false;
        }
        match self {
            Self::Expression(value) => value.demand == EvaluationDemand::Value,
            Self::Relation(input) => input.is_materialized_input_of(call),
        }
    }
}
