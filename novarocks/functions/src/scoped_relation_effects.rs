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

//! Scoped summaries retain their real scalar or relational call identity.
// Design: ADR-0238-self-contained-expression-units-and-explicit-environments.md
use crate::{CallEffectInput, RefinedCallEffects, ScopedExpressionEffects};
use novarocks_type_contract::{
    CallEffectContext, CallEffects, EffectContractError, ExpressionEffects, FunctionInstanceState,
    FunctionIntrinsicRowError, RelationEffectContext,
};

/// The original relation compiler composes its checked parameter roots before
/// binding this summary to the exact relation call. No expression flow is
/// created here, and identity alone grants no scheduling or resource right.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedRelationalEffects {
    context: RelationEffectContext,
    effects: ExpressionEffects,
}
impl ScopedRelationalEffects {
    pub const fn pure_value(context: RelationEffectContext) -> Self {
        Self {
            context,
            effects: ExpressionEffects::PURE_VALUE,
        }
    }
    pub const fn primitive(context: RelationEffectContext, effects: ExpressionEffects) -> Self {
        Self { context, effects }
    }
    pub const fn context(self) -> RelationEffectContext {
        self.context
    }
    pub fn for_use(
        self,
        context: RelationEffectContext,
    ) -> Result<ExpressionEffects, EffectContractError> {
        if self.context != context {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(self.effects)
    }
    fn with_verified_call(self, call: &CallEffects) -> Result<Self, EffectContractError> {
        if !self.context.is_call()
            || !CallEffectContext::Relation(self.context).accepts_proof_scope(call.proof_scope)
        {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(Self {
            effects: self.effects.join(ExpressionEffects {
                may_raise_invocation_data: false,
                value_stability: call.value_stability,
                may_raise_row_error: call.own_row_error == FunctionIntrinsicRowError::MayRaise,
                has_instance_state: call.instance_state != FunctionInstanceState::None,
                observable_effects: call.observable_effects,
            }),
            ..self
        })
    }
}
impl RefinedCallEffects<'_> {
    pub fn compose_for_relation(
        &self,
        input: CallEffectInput<'_>,
        arguments: ScopedRelationalEffects,
    ) -> Result<ScopedRelationalEffects, EffectContractError> {
        if input.context != CallEffectContext::Relation(arguments.context()) {
            return Err(EffectContractError::CallIdentityMismatch);
        }
        self.validate_input(input)?;
        arguments.with_verified_call(self.facts())
    }
}
/// Common immutable output of the actual typed ABI. This carrier has no
/// control-edge join API; only scalar summaries can consume a scalar flow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedCallEffects {
    context: CallEffectContext,
    effects: ExpressionEffects,
}
impl ScopedCallEffects {
    pub(crate) const fn new(context: CallEffectContext, effects: ExpressionEffects) -> Self {
        Self { context, effects }
    }
    pub const fn context(self) -> CallEffectContext {
        self.context
    }
    pub fn for_use(
        self,
        context: impl Into<CallEffectContext>,
    ) -> Result<ExpressionEffects, EffectContractError> {
        if self.context != context.into() {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(self.effects)
    }
}
impl From<ScopedExpressionEffects> for ScopedCallEffects {
    fn from(value: ScopedExpressionEffects) -> Self {
        value.into_call_effects()
    }
}
impl From<ScopedRelationalEffects> for ScopedCallEffects {
    fn from(value: ScopedRelationalEffects) -> Self {
        Self::new(value.context.into(), value.effects)
    }
}
