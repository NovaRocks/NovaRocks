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

//! Complete/Rows endpoints compiled from validated physical packages. The
//! earlier Partial fixture cannot cover zero calls, numeric or foreign sites.
//! Expected roots, types and global binding identities come from the authored
//! plan, independently of runtime arithmetic. LocalCompiler owns this lane.

use super::*;
use arrow_schema::TimeUnit;

#[test]
fn complete_ordered_key_domains_keep_endpoint_nullability_and_global_bindings() {
    let mut types = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Utf8,
        DataType::Date32,
        DataType::Decimal128(20, 3),
    ];
    for unit in [
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, None));
    }
    let mut values = types
        .into_iter()
        .map(|dt| ValueType::new(dt, false))
        .collect::<Vec<_>>();
    values.push(
        ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    for ty in values {
        for nullable in [false, true] {
            for domain_nullable in [false, true] {
                let ty = ty.with_nullable(nullable).unwrap();
                let domain = ty.with_nullable(domain_nullable).unwrap();
                let package = aggregate_topn_typed(ty.clone(), domain.clone());
                let program = compile(&package, Some(1)).unwrap();
                let aggregate_id = node_of(&program, |kind| {
                    matches!(kind, ProgramNodeKind::Aggregate { .. })
                });
                let ProgramNodeKind::Aggregate {
                    functions,
                    topn_filters,
                    ..
                } = program.graph().nodes()[aggregate_id.index()].kind()
                else {
                    unreachable!()
                };
                assert!(functions.is_empty());
                let [producer] = topn_filters.as_slice() else {
                    panic!("one producer")
                };
                let scan_id = node_of(&program, |kind| {
                    matches!(kind, ProgramNodeKind::Scan { .. })
                });
                let ProgramNodeKind::Scan {
                    runtime_filters, ..
                } = program.graph().nodes()[scan_id.index()].kind()
                else {
                    unreachable!()
                };
                let [consumer] = runtime_filters.as_slice() else {
                    panic!("one consumer")
                };
                assert_eq!(producer.producer.contract(), consumer.consumer.contract());
                let StaticFilterContract::Ordered { keys, .. } = producer.producer.contract()
                else {
                    panic!("ordered contract")
                };
                assert_eq!(keys[0].value_type, domain);
                for (role, actual) in [
                    (
                        RuntimeFilterBindingRole::Producer(0),
                        producer.producer.binding_id(),
                    ),
                    (
                        RuntimeFilterBindingRole::Consumer(0),
                        consumer.consumer.binding_id(),
                    ),
                ] {
                    let original = package
                        .cuts()
                        .runtime_filter_bindings
                        .iter()
                        .find(|binding| binding.role == role)
                        .unwrap();
                    assert_eq!(actual, original.binding_id);
                }
                let definition = crate::test_units::root_type(&program, consumer.expr_id);
                assert_eq!(
                    definition,
                    &novarocks_functions::FunctionArgumentType::Value(ty)
                );
            }
        }
    }
}

#[test]
fn complete_ordered_unsupported_domains_are_rejected_by_name() {
    for ty in [
        ValueType::new(DataType::Boolean, false),
        ValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ] {
        let package = aggregate_topn_typed(ty.clone(), ty);
        refused(
            &package,
            Some(1),
            AGGREGATE,
            "Aggregate TopN runtime-filter order key type",
        );
    }
    // The frozen comparator refuses an unrelated fixed-width carrier or
    // nominal UUID before the narrower installation gate sees a package.
    for ty in [
        ValueType::new(DataType::FixedSizeBinary(16), false),
        ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        let Err(novarocks_physical_plan::PlanConstructionError::Structure(errors)) =
            try_aggregate_topn_typed(ty.clone(), ty)
        else {
            panic!("expected the frozen comparison contract to reject the order domain");
        };
        assert!(errors.errors().iter().any(|error| {
            error.message()
                == "ordered runtime-filter key type is unsupported by its comparison algorithm"
        }));
    }
}

fn complete_cross_fragment() -> (Arc<FragmentPackage>, Arc<FragmentPackage>) {
    let mut scan_builder = FragmentBuilder::new(PROBE);
    let keys = add_scan_typed(
        &mut scan_builder,
        Distribution::Singleton,
        &[nullable_int64(false)],
    );
    let expression = scan_builder
        .fixture_value_root(PROJECT, nullable_int64(false), ExprKind::Value(keys[0]))
        .unwrap();
    let projected = scan_builder
        .add_value(
            nullable_int64(false),
            ValueOrigin::Expr {
                node: PROJECT,
                root: expression,
            },
        )
        .unwrap();
    scan_builder
        .add_project(
            PROJECT,
            SCAN,
            Box::from([(expression, projected)]),
            Box::from([projected]),
        )
        .unwrap();
    scan_builder.attach_runtime_filter(FILTER).unwrap();
    let scan_fragment = scan_builder
        .finish_fixture_definition(
            PROJECT,
            FragmentSink::Stream { edge: EDGE },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut aggregate_builder = FragmentBuilder::new(GATHER);
    let imports = receive(
        &mut aggregate_builder,
        RECEIVER,
        EDGE,
        &[projected],
        nullable_int64(false),
        |_| Distribution::Singleton,
        RowMultiplicity::SingleCopy,
    );
    let group_key = aggregate_builder
        .fixture_value_root(
            AGGREGATE,
            nullable_int64(false),
            ExprKind::Value(imports[0]),
        )
        .unwrap();
    aggregate_builder
        .add_row_consuming(
            AGGREGATE,
            Box::from([RECEIVER]),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::from([imports[0]]),
            NodeKind::Aggregate {
                group_by: Box::from([(group_key, imports[0])]),
                calls: Box::default(),
                grouping: AggregateGrouping::Complete,
            },
        )
        .unwrap();
    let order_key = aggregate_builder
        .fixture_value_root(TOPN, nullable_int64(false), ExprKind::Value(imports[0]))
        .unwrap();
    aggregate_builder
        .add_top_n(
            TOPN,
            AGGREGATE,
            Box::from([SortExpr {
                expr: order_key,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            5,
            0,
            TopNPhase::Single,
        )
        .unwrap();
    aggregate_builder.attach_runtime_filter(FILTER).unwrap();
    let aggregate_fragment = aggregate_builder
        .finish_fixture_definition(
            TOPN,
            FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut filter = ordered_filter(
        GATHER,
        AGGREGATE,
        SCAN,
        TOPN,
        TopNPhase::Single,
        imports[0],
        nullable_int64(true),
        SortDirection::Ascending,
        NullOrdering::Last,
        5,
    );
    filter.producers[0].progress.build_edges = Box::from([EDGE]);
    filter.consumers[0].endpoint.fragment = PROBE;
    filter.consumers[0].endpoint.values = Box::from([keys[0]]);
    filter.consumers[0].target = RuntimeFilterConsumerTarget::AggregateTopNScanField {
        producer: WITNESS,
        lineage: Box::from([
            RuntimeFilterLineageStep::ExchangeMapping {
                edge: EDGE,
                mapping_ordinal: 0,
            },
            RuntimeFilterLineageStep::ProjectIdentity {
                fragment: PROBE,
                node: PROJECT,
                output_ordinal: 0,
            },
        ]),
    };
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(scan_fragment).unwrap();
    plan.add_fragment(aggregate_fragment).unwrap();
    plan.add_edge(edge(
        EDGE,
        (PROBE, &[projected]),
        (GATHER, RECEIVER, &imports),
        Distribution::Singleton,
        Distribution::Singleton,
    ))
    .unwrap();
    let mut result = result_port(GATHER, TOPN, &imports);
    result.fields[0].ty = nullable_int64(false);
    plan.set_result_port(result).unwrap();
    plan.add_runtime_filter(filter).unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    let read = frozen_read_typed(
        true,
        &[ConnectorValueType::BigInt],
        arrow_schema::Schema::new(vec![arrow_schema::Field::new("v0", DataType::Int64, false)]),
        vec![dynamic_filter()],
    );
    let mut packages = packages(&plan, Some(read));
    (
        packages.remove(&PROBE).unwrap(),
        packages.remove(&GATHER).unwrap(),
    )
}

#[test]
fn complete_ordered_foreign_producer_uses_validated_exchange_identity_lineage() {
    let (scan_package, aggregate_package) = complete_cross_fragment();
    assert!(!scan_package.fragment().nodes().contains_key(&AGGREGATE));
    let consumer_program = compile(&scan_package, None).unwrap();
    assert!(
        consumer_program
            .graph()
            .nodes()
            .iter()
            .all(|node| !matches!(node.kind(), ProgramNodeKind::Aggregate { .. }))
    );
    let scan_id = node_of(&consumer_program, |kind| {
        matches!(kind, ProgramNodeKind::Scan { .. })
    });
    let ProgramNodeKind::Scan {
        runtime_filters, ..
    } = consumer_program.graph().nodes()[scan_id.index()].kind()
    else {
        unreachable!()
    };
    let [consumer] = runtime_filters.as_slice() else {
        panic!("one scan endpoint")
    };
    let definition = crate::test_units::root_type(&consumer_program, consumer.expr_id);
    assert_eq!(
        definition,
        &novarocks_functions::FunctionArgumentType::Value(nullable_int64(false))
    );
    let producer_program = compile(&aggregate_package, Some(1)).unwrap();
    assert!(producer_program.scan_inputs().is_empty());
    let aggregate_id = node_of(&producer_program, |kind| {
        matches!(kind, ProgramNodeKind::Aggregate { .. })
    });
    let ProgramNodeKind::Aggregate { topn_filters, .. } =
        producer_program.graph().nodes()[aggregate_id.index()].kind()
    else {
        unreachable!()
    };
    let [producer] = topn_filters.as_slice() else {
        panic!("one aggregate endpoint")
    };
    assert_eq!(consumer.consumer.contract(), producer.producer.contract());
    for (package, role, id) in [
        (
            &scan_package,
            RuntimeFilterBindingRole::Consumer(0),
            consumer.consumer.binding_id(),
        ),
        (
            &aggregate_package,
            RuntimeFilterBindingRole::Producer(0),
            producer.producer.binding_id(),
        ),
    ] {
        let cut = package
            .cuts()
            .runtime_filter_bindings
            .iter()
            .find(|cut| cut.role == role)
            .unwrap();
        assert_eq!(cut.binding_id, id);
    }
}

// Two real source occurrences, a nullable Union output and a local grouping
// precede the Complete endpoint. No source/root is borrowed from its sibling.
fn complete_multi_scan_union(local_groups: bool) -> Arc<FragmentPackage> {
    let second_scan = NodeId::new(81);
    let union = NodeId::new(82);
    let local = NodeId::new(83);
    let mut builder = FragmentBuilder::new(PROBE);
    let mut scan_keys = Vec::new();
    for (ordinal, node) in [SCAN, second_scan].into_iter().enumerate() {
        scan_keys.push(
            add_scan_typed_at(
                &mut builder,
                Distribution::Singleton,
                &[nullable_int64(ordinal == 1)],
                node,
                ProviderReadOccurrenceId::new(ordinal as u32),
            )[0],
        );
    }
    let union_key = builder
        .add_value(
            nullable_int64(true),
            ValueOrigin::NodeOutput {
                node: union,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_row_consuming(
            union,
            Box::from([SCAN, second_scan]),
            RequiredInputs::AsProduced,
            Distribution::Singleton,
            Box::from([union_key]),
            NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: scan_keys.iter().map(|key| Box::from([*key])).collect(),
            },
        )
        .unwrap();
    let input = if local_groups {
        let key = builder
            .fixture_value_root(local, nullable_int64(true), ExprKind::Value(union_key))
            .unwrap();
        builder
            .add_row_consuming(
                local,
                Box::from([union]),
                RequiredInputs::AsProduced,
                Distribution::Singleton,
                Box::from([union_key]),
                NodeKind::Aggregate {
                    group_by: Box::from([(key, union_key)]),
                    calls: Box::default(),
                    grouping: AggregateGrouping::Partial,
                },
            )
            .unwrap();
        local
    } else {
        union
    };
    let key = builder
        .fixture_value_root(AGGREGATE, nullable_int64(true), ExprKind::Value(union_key))
        .unwrap();
    builder
        .add_row_consuming(
            AGGREGATE,
            Box::from([input]),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::from([union_key]),
            NodeKind::Aggregate {
                group_by: Box::from([(key, union_key)]),
                calls: Box::default(),
                grouping: AggregateGrouping::Complete,
            },
        )
        .unwrap();
    let order = builder
        .fixture_value_root(TOPN, nullable_int64(true), ExprKind::Value(union_key))
        .unwrap();
    builder
        .add_top_n(
            TOPN,
            AGGREGATE,
            Box::from([SortExpr {
                expr: order,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            5,
            0,
            TopNPhase::Single,
        )
        .unwrap();
    builder.attach_runtime_filter(FILTER).unwrap();
    let fragment = builder
        .finish_fixture_definition(
            TOPN,
            FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut filter = ordered_filter(
        PROBE,
        AGGREGATE,
        SCAN,
        TOPN,
        TopNPhase::Single,
        union_key,
        nullable_int64(true),
        SortDirection::Ascending,
        NullOrdering::Last,
        5,
    );
    let mut consumers = Vec::new();
    for (ordinal, node) in [SCAN, second_scan].into_iter().enumerate() {
        let mut consumer = filter.consumers[0].clone();
        consumer.endpoint.node = node;
        consumer.endpoint.values = Box::from([scan_keys[ordinal]]);
        let mut lineage = Vec::new();
        if local_groups {
            lineage.push(RuntimeFilterLineageStep::AggregateGroupKey {
                fragment: PROBE,
                node: local,
                group_key_ordinal: 0,
            });
        }
        lineage.push(RuntimeFilterLineageStep::UnionAllBranch {
            fragment: PROBE,
            node: union,
            input_ordinal: ordinal as u32,
            output_ordinal: 0,
        });
        consumer.target = RuntimeFilterConsumerTarget::AggregateTopNScanField {
            producer: WITNESS,
            lineage: lineage.into_boxed_slice(),
        };
        consumers.push(consumer);
    }
    filter.consumers = consumers.into_boxed_slice();
    let mut result = result_port(PROBE, TOPN, &[union_key]);
    result.fields[0].ty = nullable_int64(true);
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.add_runtime_filter(filter).unwrap();
    plan.set_result_port(result).unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    let reads = (0..2)
        .map(|ordinal| {
            (
                ProviderReadOccurrenceId::new(ordinal),
                frozen_read_typed(
                    true,
                    &[ConnectorValueType::BigInt],
                    arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        "v0",
                        DataType::Int64,
                        ordinal == 1,
                    )]),
                    vec![dynamic_filter()],
                ),
            )
        })
        .collect();
    packages_with_reads(&plan, reads).remove(&PROBE).unwrap()
}

#[test]
fn complete_ordered_union_branches_and_local_group_lineage_keep_all_scan_endpoints() {
    for local in [false, true] {
        let package = complete_multi_scan_union(local);
        let program = compile(&package, Some(1)).unwrap();
        let scans = program
            .graph()
            .nodes()
            .iter()
            .enumerate()
            .filter_map(|(id, node)| match node.kind() {
                ProgramNodeKind::Scan {
                    runtime_filters, ..
                } => Some((ProgramNodeId::new(id), node, runtime_filters)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(scans.len(), 2);
        assert_eq!(program.scan_inputs().len(), 2);
        let mut nullable_roots = Vec::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut definitions = std::collections::BTreeSet::new();
        for (local_id, node, scan) in scans {
            let physical_scan = NodeId::new(program.scan_inputs()[&local_id].scan_node);
            let original_filter = &package.cuts().runtime_filters[0];
            let (index, endpoint) = original_filter
                .consumers
                .iter()
                .enumerate()
                .find(|(_, consumer)| consumer.endpoint.node == physical_scan)
                .unwrap();
            let [value] = endpoint.endpoint.values.as_ref() else {
                panic!("one physical scan key")
            };
            let expected = &package.fragment().values()[value].ty;
            let [consumer] = scan.as_slice() else {
                panic!("one original filter per source")
            };
            let original = package
                .cuts()
                .runtime_filter_bindings
                .iter()
                .find(|cut| cut.role == RuntimeFilterBindingRole::Consumer(index))
                .unwrap();
            assert_eq!(consumer.consumer.binding_id(), original.binding_id);
            assert_eq!(consumer.consumer.channel_id(), FILTER.get());
            ids.insert(original.binding_id);
            definitions.insert(consumer.expr_id);
            assert!(matches!(
                crate::test_units::kind(&program, consumer.expr_id),
                StaticExprKind::Input
            ));
            assert_eq!(
                crate::test_units::input_ordinal(&program, consumer.expr_id),
                0
            );
            let (unit, root) = crate::test_units::compiled_root(&program, consumer.expr_id);
            assert_eq!(
                unit.scope(root.scope).unwrap().schema().as_ref(),
                node.output_layout().schema().as_ref()
            );
            let ty = crate::test_units::root_type(&program, consumer.expr_id);
            let novarocks_functions::FunctionArgumentType::Value(ty) = ty else {
                panic!("value root")
            };
            assert_eq!(
                ty, expected,
                "each root belongs to its physical Scan endpoint"
            );
            nullable_roots.push(ty.nullable());
            let StaticFilterContract::Ordered { keys, .. } = consumer.consumer.contract() else {
                panic!("ordered")
            };
            assert_eq!(keys[0].value_type, nullable_int64(true));
        }
        nullable_roots.sort();
        assert_eq!(nullable_roots, [false, true]);
        assert_eq!(ids.len(), 2);
        assert_eq!(definitions.len(), 2);
        let installed = program
            .graph()
            .nodes()
            .iter()
            .filter_map(|node| match node.kind() {
                ProgramNodeKind::Aggregate { topn_filters, .. } if !topn_filters.is_empty() => {
                    Some(topn_filters)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            installed.len(),
            1,
            "local grouping must not acquire the Complete endpoint"
        );
        let producer = &installed[0][0].producer;
        let producer_cut = package
            .cuts()
            .runtime_filter_bindings
            .iter()
            .find(|cut| cut.role == RuntimeFilterBindingRole::Producer(0))
            .unwrap();
        assert_eq!(producer.binding_id(), producer_cut.binding_id);
        assert!(!ids.contains(&producer.binding_id()));
        assert_eq!(filter_requirements(&program).len(), 3);
    }
}
