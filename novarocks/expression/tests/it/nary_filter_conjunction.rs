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

use arrow::array::BooleanArray;
use novarocks_expression::{
    CompiledFilterConjunctionInstance, EvaluationInput,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            NARY_CONTROL as Control,
            nary::{batch, filter_roots},
        },
    },
};
use novarocks_functions::Selection;
use novarocks_type_contract::EvaluationDemand;
use std::collections::BTreeMap;

#[test]
fn filter_conjunction_actual_repeated_definition_distinct_use_order_and_wide_roots() {
    for width in [3, 4, 320] {
        let (unit, predicates) = filter_roots(width);
        let mut distinct = BTreeMap::new();
        let mut definitions = vec![];
        assert_eq!(predicates.len(), width);
        for (ordinal, &root) in predicates.iter().enumerate() {
            let use_id = unit.root(root).unwrap().use_id;
            assert!(distinct.insert(use_id, ordinal).is_none());
            let invocation = &unit.control_flow().uses()[&use_id];
            assert_eq!(invocation.context.demand, EvaluationDemand::TruthOnly);
            definitions.push(invocation.definition);
        }
        if width > 3 {
            assert_eq!(definitions[0], definitions[3]);
        }
        let input = batch(
            &unit,
            [
                vec![Some(true), None, Some(true)],
                vec![Some(true), Some(true), Some(true)],
                vec![Some(true), Some(false), None],
            ],
        );
        let roots = predicates
            .iter()
            .map(|&root| (unit.clone(), root))
            .collect::<Vec<_>>();
        let mut instance =
            CompiledFilterConjunctionInstance::try_new_with_allocator(&roots, &Control, None)
                .unwrap();
        let actual = instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                Selection::all(3),
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let boolean = actual.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            boolean.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(false)]
        );
        let sparse = instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                Selection::try_sparse(3, &[0, 2]).unwrap(),
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            sparse
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(true), Some(false)]
        );
    }
}
