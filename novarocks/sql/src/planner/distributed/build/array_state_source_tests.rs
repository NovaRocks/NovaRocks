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

//! Real lexical ARRAY producers and the existing state journal consumer facet.
// Mounted as a child of physical_aggregate_journal_update_tests, so every
// source loan comes from the actual completion driver, never a fabricated entry.
use super::*;

#[test]
fn array_state_source_real_ordered_distinct_single_receipt_without_split_capability() {
    for (sql, distinct, ascending, nulls_first) in [
        (
            "SELECT ARRAY_AGG(order_key ORDER BY order_key DESC NULLS FIRST) FROM orders",
            false,
            false,
            true,
        ),
        (
            "SELECT ARRAY_AGG(DISTINCT order_key ORDER BY order_key ASC NULLS LAST) FROM orders",
            true,
            true,
            false,
        ),
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let mut single = None;
        for fragment in owner.plan().fragments().values() {
            for node in fragment.nodes().values() {
                if let NodeKind::Aggregate { calls, .. } = &node.kind {
                    for (ordinal, source) in calls.iter().enumerate() {
                        assert!(
                            matches!(source.binding.phase, AggregatePhase::Single),
                            "ordered/distinct ARRAY_AGG has no installed split law"
                        );
                        let control = Control::default();
                        let mut work = CompileCheckpoints::try_new(
                            &control,
                            CompilePhase::FunctionSpecialization,
                        )
                        .unwrap();
                        let site = PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: u32::try_from(ordinal).unwrap(),
                        };
                        let entry = owner
                            .checked_aggregate_source_observed(
                                fragment, node, site, source, &mut work,
                            )
                            .unwrap();
                        work.finish().unwrap();
                        assert!(
                            single.replace(entry).is_none(),
                            "one actual ARRAY_AGG occurrence"
                        );
                    }
                }
            }
        }
        let single_entry = single.expect("genuine Single ARRAY_AGG receipt");
        let original = single_entry
            .captured()
            .binding()
            .aggregate_state_source()
            .unwrap();
        assert_eq!(original.distinct, distinct);
        assert_eq!(
            original.order_keys.as_ref(),
            [novarocks_type_contract::AggregateStateOrderKey {
                ascending,
                nulls_first
            }]
        );
        assert!(
            single_entry
                .captured()
                .binding()
                .group_concat_source()
                .is_none()
        );
        assert_eq!(
            single_entry.source().binding.state_interpretation.as_ref(),
            Some(original)
        );
        let update = run(&single_entry, &Control::default()).unwrap();
        let PureCallPreparation::Aggregate { options, .. } =
            update.preparation(effects(&single_entry))
        else {
            panic!("Single preparation")
        };
        assert_eq!(options.phase, AggregateKernelPhase::Single);
        assert_eq!(options.distinct, distinct);
        assert_eq!(options.order_keys.len(), 1);
        assert_eq!(options.state_interpretation.as_deref(), Some(original));
        prefixes(|control| run(&single_entry, control).map(|_| ()), true);
    }
}

#[test]
fn array_state_source_actual_plain_generic_aggregate_keeps_plain_execution_flags() {
    let owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let partial = loan(&owner, true);
    let facts = partial
        .captured()
        .binding()
        .aggregate_state_source()
        .unwrap();
    assert!(!facts.distinct);
    assert!(facts.order_keys.is_empty());
    let update = run(&partial, &Control::default()).unwrap();
    let PureCallPreparation::Aggregate { options, .. } = update.preparation(effects(&partial))
    else {
        panic!("Partial preparation")
    };
    assert!(!options.distinct);
    assert!(options.order_keys.is_empty());
    assert_eq!(options.state_interpretation.as_deref(), Some(facts));
}
