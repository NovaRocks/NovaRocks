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
//! Reuse the original checked Boolean fixture authors; only the node root
//! field changes from one connective definition to its original ordered list.
use super::*;
use crate::exec::expr::compiled_program::CompiledFilterConjunctionInstance;
fn list_fixture(width: usize) -> Fixture {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let fragment_id = FragmentId::new(91);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let root = NodeId::new(0);
    let mut builder = FragmentBuilder::new(fragment_id);
    let operands = source_columns(&mut builder, source, input, root, &mut source_pools);
    let order = [operands[2], operands[0], operands[1]];
    let predicates = (0..width)
        .map(|ordinal| order[ordinal % 3])
        .collect::<Vec<_>>()
        .into_boxed_slice();
    builder.add_filter(root, input, predicates).unwrap();
    Fixture {
        constants: source_pools,
        fragment: builder
            .finish_definition(
                root,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    }
}
fn list_program(width: usize) -> Arc<LocalProgram> {
    let source = package(list_fixture(width));
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    Arc::new(
        compile_fragment(
            validate_fragment_providers(source, &providers, &Control).unwrap(),
            &functions(),
            options(),
            &Control,
        )
        .unwrap(),
    )
}
#[test]
fn filter_conjunction_actual_repeated_definition_distinct_use_order_and_wide_roots() {
    for width in [3, 4, 320] {
        let program = list_program(width);
        let node = ProgramNodeId::new(2);
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        let mut distinct = BTreeMap::new();
        let mut definitions = vec![];
        for ordinal in 0..width {
            let site = ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::FilterPredicate {
                    predicate: u32::try_from(ordinal).unwrap(),
                },
            };
            let use_id = snapshot.bindings()[&site];
            assert!(distinct.insert(use_id, ordinal).is_none());
            let invocation = &snapshot.flows()[&site.arena()].uses()[&use_id];
            assert_eq!(invocation.context.demand, EvaluationDemand::TruthOnly);
            definitions.push(invocation.definition);
        }
        if width > 3 {
            assert_eq!(definitions[0], definitions[3]);
        }
        let input = batch(
            &program,
            [
                vec![Some(true), None, Some(true)],
                vec![Some(true), Some(true), Some(true)],
                vec![Some(true), Some(false), None],
            ],
        );
        let mut instance =
            CompiledFilterConjunctionInstance::try_new(Arc::clone(&program), node, &Control)
                .unwrap();
        let actual = instance
            .evaluate_required(&input, Selection::all(3), &Control)
            .unwrap();
        let boolean = actual.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            boolean.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(false)]
        );
        let sparse = instance
            .evaluate_required(&input, Selection::try_sparse(3, &[0, 2]).unwrap(), &Control)
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
