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

use super::*;
use crate::Distribution;
use novarocks_type_contract::{FunctionInstanceState, FunctionVolatility};

// One criterion for both claimed-broadcast validation and the private
// occurrence property proof. Relational lifecycle state is not scalar state;
// row errors do not authorize movement and do not invalidate identical replay.
pub(crate) fn call_is_replica_equivalent(kind: FunctionKind, effects: &CallEffects) -> bool {
    effects.value_stability == FunctionVolatility::Immutable
        && (kind != FunctionKind::Scalar || effects.instance_state == FunctionInstanceState::None)
        && effects.observable_effects.is_empty()
}

impl FrozenFragmentCalls {
    /// Check the complete occurrence prerequisite for claimed broadcast
    /// equivalence, retaining each invocation's own control domain.
    ///
    /// This rechecks exact definition/root/call correspondence first. It never
    /// chooses one invocation to represent a shared expression definition, and
    /// never consults legacy binding effect bits. All runtime occurrences of a
    /// broadcast-producing node participate, including guarded children and
    /// relational calls. The property author must relinquish an unsupported
    /// broadcast guarantee; this check does not rewrite the plan.
    ///
    /// Row errors remain observable and are not erased. This prerequisite
    /// compares the same invocation on identical replica inputs; it grants no
    /// movement across guards, earlier evaluation, or installed-owner proof.
    /// Admission must still recompute the complete facts with the exact owner.
    pub fn validate_replica_equivalence(
        &self,
        fragment: &Fragment,
        control: &dyn PureCompileControl,
    ) -> Result<(), FrozenCallError> {
        self.validate_fragment(fragment, control)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = super::property_proof::visit_occurrence_calls::<FrozenCallError>(
            fragment,
            self,
            &mut work,
            |site, kind, effects, work| {
                let node = fragment
                    .nodes()
                    .get(&site.node())
                    .ok_or(FrozenCallError::InvalidSite)?;
                work.step()?;
                if node.output_properties.distribution == Distribution::Broadcast
                    && !call_is_replica_equivalent(kind, effects)
                {
                    return Err(site.failure());
                }
                Ok(())
            },
        );
        if matches!(result, Err(FrozenCallError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}
