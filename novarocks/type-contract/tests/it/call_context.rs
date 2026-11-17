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

use novarocks_type_contract::*;
fn expression(demand: EvaluationDemand) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(4),
        region: EvaluationRegionId::new(7),
        demand,
    }
}
#[test]
fn call_scopes_keep_scalar_regions_and_relation_channels_distinct() {
    let scalar = CallEffectContext::Expression(expression(EvaluationDemand::Value));
    let relation = RelationEffectContext::call(RelationCallId::new(7));
    let call = CallEffectContext::Relation(relation);
    assert!(scalar.matches_kind(FunctionKind::Scalar));
    assert!(!call.matches_kind(FunctionKind::Scalar));
    for kind in [
        FunctionKind::Aggregate,
        FunctionKind::Window,
        FunctionKind::Table,
    ] {
        assert!(call.matches_kind(kind));
        assert!(!scalar.matches_kind(kind));
    }
    assert!(scalar.accepts_proof_scope(CallProofScope::Region(EvaluationRegionId::new(7))));
    assert!(call.accepts_proof_scope(CallProofScope::Relation(relation.identity)));
    assert!(!call.accepts_proof_scope(scalar.proof_scope()));
    assert!(!scalar.accepts_proof_scope(call.proof_scope()));
    let input = RelationEffectContext::materialized_input(RelationCallId::new(7));
    assert!(!call.accepts_proof_scope(CallProofScope::Relation(input.identity)));
    assert!(!CallEffectContext::Relation(input).matches_kind(FunctionKind::Aggregate));
    assert!(
        !call.accepts_proof_scope(
            CallEffectContext::Relation(RelationEffectContext::call(RelationCallId::new(8)))
                .proof_scope()
        )
    );
    assert!(scalar.accepts_proof_scope(CallProofScope::Unconditional));
    assert!(call.accepts_proof_scope(CallProofScope::Unconditional));
}
#[test]
fn aggregate_state_inputs_keep_actual_expression_demand_and_the_same_materialized_call() {
    let call = CallEffectContext::Relation(RelationEffectContext::call(RelationCallId::new(41)));
    assert!(
        CallEffectContext::Expression(expression(EvaluationDemand::Value))
            .is_aggregate_state_input_of(call)
    );
    assert!(
        !CallEffectContext::Expression(expression(EvaluationDemand::TruthOnly))
            .is_aggregate_state_input_of(call)
    );
    assert!(
        CallEffectContext::Relation(RelationEffectContext::materialized_input(
            RelationCallId::new(41)
        ))
        .is_aggregate_state_input_of(call)
    );
    assert!(
        !CallEffectContext::Relation(RelationEffectContext::materialized_input(
            RelationCallId::new(42)
        ))
        .is_aggregate_state_input_of(call)
    );
    assert!(!call.is_aggregate_state_input_of(call));
    assert!(
        !CallEffectContext::Relation(RelationEffectContext::call(RelationCallId::new(42)))
            .is_aggregate_state_input_of(call)
    );
    assert!(
        !CallEffectContext::Expression(expression(EvaluationDemand::Value))
            .is_aggregate_state_input_of(CallEffectContext::Expression(expression(
                EvaluationDemand::Value
            )))
    );
}
