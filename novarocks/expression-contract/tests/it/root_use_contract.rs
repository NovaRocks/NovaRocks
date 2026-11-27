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

//! Root occurrence boundaries and exact independent use correspondence.
use super::support::*;
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::*;
fn roots(count: usize) -> BoundExprSetBuilder {
    let mut builder = BoundExprSetBuilder::new(limits());
    let definition = builder
        .add(None, boolean(false), ExprKind::input(ValueId::new(9)))
        .unwrap();
    for ordinal in 0..count {
        assert_eq!(
            builder
                .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
                .unwrap(),
            RootOrdinal::new(u32::try_from(ordinal).unwrap())
        );
    }
    builder
}
fn ports() -> Vec<ExprPort> {
    vec![ExprPort {
        values: Box::new([(ValueId::new(9), boolean(false))]),
    }]
}
#[test]
fn root_only_occurrences_keep_original_shared_near_exact_and_over_bound() {
    for count in [MAX_CONTROL_USE_REFERENCES - 1, MAX_CONTROL_USE_REFERENCES] {
        let unit = no_calls(roots(count), ports(), &Control::good()).unwrap();
        assert_eq!(unit.definitions().len(), 1);
        assert_eq!(unit.roots().len(), count);
        assert_eq!(unit.control_flow().uses().len(), count);
        assert_eq!(unit.control_flow().use_reference_count(), count);
        assert!(
            unit.root_use_ids()
                .windows(2)
                .all(|pair| pair[0] != pair[1])
        );
    }
    let error = no_calls(
        roots(MAX_CONTROL_USE_REFERENCES + 1),
        ports(),
        &Control::good(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        UnitError::Flow(ExpressionControlFlowError::TooManyItems)
    ));
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
}
fn repeated() -> BoundExprSet {
    let mut builder = BoundExprSetBuilder::new(limits());
    let leaf = builder
        .add(None, boolean(false), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let conjunction = builder
        .add(
            None,
            boolean(false),
            ExprKind::Conjunction {
                args: Box::new([leaf, leaf]),
            },
        )
        .unwrap();
    for definition in [conjunction, leaf, leaf] {
        builder
            .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
            .unwrap();
    }
    no_calls(builder, ports(), &Control::good()).unwrap()
}
fn reject(parts: UnitParts, message: &str) {
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let error = BoundExprSet::try_from_parts_in(parts, limits(), &mut work).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(error.to_string().contains(message), "{error}");
    work.finish().unwrap();
}
#[test]
fn root_binding_requires_complete_unique_actual_use_definition_and_demand() {
    let source = repeated();
    assert_eq!(
        source.root(RootOrdinal::new(1)),
        source.root(RootOrdinal::new(2))
    );
    assert_ne!(
        source.root_use(RootOrdinal::new(1)),
        source.root_use(RootOrdinal::new(2))
    );
    let mut parts = repeated().into_parts();
    parts.root_uses = parts.root_uses[..2].into();
    reject(parts, "root-use count differs from roots");
    let mut parts = repeated().into_parts();
    parts.root_uses[2] = parts.root_uses[1];
    reject(parts, "root does not bind one exact independent invocation");
    let mut parts = repeated().into_parts();
    parts.root_uses[2] = ExpressionUseId::new(u32::MAX);
    reject(parts, "root use is absent");
    let mut parts = repeated().into_parts();
    parts.roots[2].definition = parts.roots[0].definition;
    reject(parts, "root does not bind one exact independent invocation");
    let mut parts = repeated().into_parts();
    parts.roots[2].demand = EvaluationDemand::Value;
    reject(parts, "root does not bind one exact independent invocation");
}
#[test]
fn an_actual_argument_use_cannot_replace_a_matching_independent_root() {
    let mut parts = repeated().into_parts();
    let operator = parts.root_uses[0];
    let child = parts.control.uses()[&operator].arguments[0];
    let independent = parts.root_uses[1];
    let child_use = &parts.control.uses()[&child];
    let root_use = &parts.control.uses()[&independent];
    assert_eq!(child_use.definition, root_use.definition);
    assert_eq!(child_use.context.demand, root_use.context.demand);
    assert_ne!(child, independent);
    assert!(parts.control.root_use_ids().contains(&independent));
    assert!(!parts.control.root_use_ids().contains(&child));
    parts.root_uses[1] = child;
    reject(parts, "root does not bind one exact independent invocation");
}
