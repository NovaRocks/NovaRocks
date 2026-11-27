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

//! Canonical full base-declaration compatibility material.

use novarocks_type_contract::{
    ArgumentControl, EvaluationDemand, FunctionEffectDeclaration, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, ObservableEffects, SemanticParameterKey,
};
use sha2::{Digest, Sha256};

pub(crate) fn digest_effect_declaration(hasher: &mut Sha256, effects: &FunctionEffectDeclaration) {
    // Exhaustive destructuring makes a new semantic field a compile-time
    // requirement to extend this compatibility encoding.
    let FunctionEffectDeclaration {
        value_stability,
        own_row_error,
        failure_behavior,
        null_behavior,
        argument_control,
        instance_state,
        observable_effects,
        environment_dependencies,
    } = effects;
    hasher.update([super::function_volatility_tag(*value_stability)]);
    hasher.update([match own_row_error {
        FunctionIntrinsicRowError::NoRowError => 1,
        FunctionIntrinsicRowError::MayRaise => 2,
        FunctionIntrinsicRowError::NotRowEvaluated => 3,
    }]);
    hasher.update([match failure_behavior {
        novarocks_type_contract::FunctionFailureBehavior::Propagate => 1,
        novarocks_type_contract::FunctionFailureBehavior::ReturnsNull => 2,
    }]);
    match null_behavior {
        FunctionNullBehavior::Strict => hasher.update([1]),
        FunctionNullBehavior::CalledOnNull => hasher.update([2]),
        FunctionNullBehavior::ControlDefined => hasher.update([3]),
        FunctionNullBehavior::StrictPositions(positions) => {
            hasher.update([4]);
            hasher.update((positions.len() as u64).to_be_bytes());
            for position in positions {
                hasher.update(position.to_be_bytes());
            }
        }
    }
    match argument_control {
        ArgumentControl::Eager => hasher.update([1]),
        ArgumentControl::TypeOnly => hasher.update([2]),
        ArgumentControl::NoArguments => hasher.update([12]),
        ArgumentControl::If => hasher.update([3]),
        ArgumentControl::Coalesce => hasher.update([4]),
        ArgumentControl::Try => hasher.update([13]),
        ArgumentControl::OrderedRegexp => hasher.update([14]),
        ArgumentControl::NullIf => hasher.update([15]),
        ArgumentControl::OrderedNull => hasher.update([16]),
        ArgumentControl::CandidateGroup => hasher.update([17]),
        ArgumentControl::SimpleCase => hasher.update([5]),
        ArgumentControl::SearchedCase => hasher.update([6]),
        ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand,
        } => {
            hasher.update([7]);
            hasher.update(body_ordinal.to_be_bytes());
            hasher.update([match body_demand {
                EvaluationDemand::Value => 1,
                EvaluationDemand::TruthOnly => 2,
            }]);
        }
        ArgumentControl::Aggregate => hasher.update([8]),
        ArgumentControl::Window => hasher.update([9]),
        ArgumentControl::Table => hasher.update([10]),
        ArgumentControl::TemporalSource(kind) => hasher.update([
            11,
            match kind {
                novarocks_type_contract::TemporalSourceKind::TimeToSec => 1,
                novarocks_type_contract::TemporalSourceKind::TimeFormat => 2,
            },
        ]),
    }
    hasher.update([match instance_state {
        FunctionInstanceState::None => 1,
        FunctionInstanceState::ScalarInstance => 2,
        FunctionInstanceState::AggregateInstance => 3,
        FunctionInstanceState::WindowPartition => 4,
        FunctionInstanceState::TableInstance => 5,
    }]);
    let ObservableEffects {
        rng_sampling,
        warnings,
        controlled_wait,
    } = observable_effects;
    hasher.update([
        u8::from(*rng_sampling),
        u8::from(*warnings),
        u8::from(*controlled_wait),
    ]);
    hasher.update(
        u32::try_from(environment_dependencies.len())
            .expect("validated effect dependencies fit u32")
            .to_be_bytes(),
    );
    // Binding construction stores the set in stable key order.
    for key in environment_dependencies {
        hasher.update([match key {
            SemanticParameterKey::StatementStartUtc => 1,
            SemanticParameterKey::TimeZone => 2,
        }]);
    }
}
