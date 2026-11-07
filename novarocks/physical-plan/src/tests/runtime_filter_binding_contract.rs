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

use std::collections::BTreeMap;

use super::*;

const PARTITIONED_FILTER: u32 = 101;
const PARTITIONED_PRODUCER_FRAGMENT: u32 = 101;
const PARTITIONED_CONSUMER_FRAGMENT: u32 = 900;
const BROADCAST_FILTER: u32 = 410;
const BROADCAST_FRAGMENT: u32 = 412;

/// One checked plan with both endpoint placements. Filter 101 is produced at
/// a join in fragment 101 and consumed by a scan in fragment 900; filter 410
/// is produced and consumed at one broadcast join in fragment 412. Fragments
/// 410 and 411 attach nothing.
pub(super) fn numbered_plan() -> PhysicalPlan {
    let (partitioned, scan_fragment, join_fragment) =
        super::contract_regressions::cross_fragment_scan_lineage_plan();
    assert_eq!(
        (scan_fragment.get(), join_fragment.get()),
        (PARTITIONED_CONSUMER_FRAGMENT, PARTITIONED_PRODUCER_FRAGMENT)
    );
    let broadcast = super::runtime_filter_wait_contract::acyclic_cross_fragment_fixture().plan;
    let mut builder = PlanBuilder::new(version());
    for plan in [&partitioned, &broadcast] {
        for fragment in plan.fragments().values() {
            builder.add_fragment(fragment.clone()).unwrap();
        }
        for edge in plan.edges().values() {
            builder.add_edge(edge.clone()).unwrap();
        }
        for filter in plan.runtime_filters().values() {
            builder.add_runtime_filter(filter.clone()).unwrap();
        }
    }
    builder.finish().unwrap()
}

/// The numbering the native v1 encoder minted before the physical plan
/// owned it, transcribed verbatim as an independent oracle.
fn legacy_v1_numbering(physical: &PhysicalPlan) -> Result<Vec<RuntimeFilterBinding>, String> {
    let mut bindings = Vec::new();
    let mut next_binding = 1_u32;
    let mut mint = |filter: RuntimeFilterId,
                    fragment: FragmentId,
                    node: NodeId,
                    role: RuntimeFilterBindingRole|
     -> Result<(), String> {
        let binding_id = next_binding;
        next_binding = next_binding.checked_add(1).ok_or_else(|| {
            "native wire v1 runtime-filter binding identity space exhausted".to_string()
        })?;
        bindings.push(RuntimeFilterBinding {
            binding_id,
            filter,
            fragment,
            node,
            role,
        });
        Ok(())
    };
    for fragment in physical.fragments().values() {
        for filter_id in fragment.runtime_filters() {
            let filter = physical.runtime_filters().get(filter_id).ok_or_else(|| {
                format!(
                    "fragment references absent runtime filter {}",
                    filter_id.get()
                )
            })?;
            for (index, producer) in filter.producers.iter().enumerate() {
                if producer.endpoint.fragment == fragment.id() {
                    mint(
                        filter.id,
                        fragment.id(),
                        producer.endpoint.node,
                        RuntimeFilterBindingRole::Producer(index),
                    )?;
                }
            }
            for (index, consumer) in filter.consumers.iter().enumerate() {
                if consumer.endpoint.fragment == fragment.id() {
                    mint(
                        filter.id,
                        fragment.id(),
                        consumer.endpoint.node,
                        RuntimeFilterBindingRole::Consumer(index),
                    )?;
                }
            }
        }
    }
    Ok(bindings)
}

fn binding(
    binding_id: u32,
    filter: u32,
    fragment: u32,
    node: NodeId,
    role: RuntimeFilterBindingRole,
) -> RuntimeFilterBinding {
    RuntimeFilterBinding {
        binding_id,
        filter: RuntimeFilterId::new(filter),
        fragment: FragmentId::new(fragment),
        node,
        role,
    }
}

fn scan_node(fragment: &Fragment) -> NodeId {
    let mut scans = fragment
        .nodes()
        .values()
        .filter(|node| matches!(node.kind, NodeKind::Scan { .. }))
        .map(|node| node.id);
    let scan = scans.next().expect("fixture fragment has a scan");
    assert!(scans.next().is_none());
    scan
}

fn cut(binding_id: u32, filter: u32, role: RuntimeFilterBindingRole) -> RuntimeFilterBindingCut {
    RuntimeFilterBindingCut {
        binding_id,
        filter: RuntimeFilterId::new(filter),
        role,
    }
}

#[test]
fn plan_numbering_crosses_fragments_in_id_order_like_native_v1() {
    use RuntimeFilterBindingRole::{Consumer, Producer};
    let plan = numbered_plan();
    let fragment = |id: u32| &plan.fragments()[&FragmentId::new(id)];
    let partitioned_join = fragment(PARTITIONED_PRODUCER_FRAGMENT).root();
    let broadcast_join = fragment(BROADCAST_FRAGMENT).root();
    let partitioned_scan = scan_node(fragment(PARTITIONED_CONSUMER_FRAGMENT));

    let numbered = runtime_filter_bindings(&plan).unwrap();
    // The partitioned filter's two endpoints are not adjacent: fragment 412's
    // broadcast filter is numbered between them, so neither fragment could
    // derive its own identities without the other attachments.
    assert_eq!(
        numbered,
        [
            binding(
                1,
                PARTITIONED_FILTER,
                PARTITIONED_PRODUCER_FRAGMENT,
                partitioned_join,
                Producer(0)
            ),
            binding(
                2,
                BROADCAST_FILTER,
                BROADCAST_FRAGMENT,
                broadcast_join,
                Producer(0)
            ),
            binding(
                3,
                BROADCAST_FILTER,
                BROADCAST_FRAGMENT,
                broadcast_join,
                Consumer(0)
            ),
            binding(
                4,
                PARTITIONED_FILTER,
                PARTITIONED_CONSUMER_FRAGMENT,
                partitioned_scan,
                Consumer(0)
            ),
        ]
    );
    assert_eq!(legacy_v1_numbering(&plan).unwrap(), numbered);

    // Every fragment's cuts carry exactly its slice, and validate alone.
    let cuts = derive_fragment_cuts(&plan).unwrap();
    for (id, fragment) in plan.fragments() {
        let expected = numbered
            .iter()
            .filter(|binding| binding.fragment == *id)
            .map(RuntimeFilterBinding::cut)
            .collect::<Vec<_>>();
        assert_eq!(&*cuts[id].runtime_filter_bindings, expected, "{}", id.get());
        assert_eq!(fragment_cuts(&plan, *id).unwrap(), cuts[id]);
        validate_fragment(fragment, &cuts[id]).unwrap();
    }
}

#[test]
fn plan_numbering_follows_each_fragments_own_attachment_order_and_endpoint_indexes() {
    use RuntimeFilterBindingRole::{Consumer, Producer};
    let checked = numbered_plan();
    // A second filter whose endpoints interleave with the checked ones. The
    // owner is a pure numbering over the plan, so this unchecked plan pins
    // the order alone: fragment attachments in their own (unsorted) order,
    // producers before consumers, each role at its exact endpoint index.
    let extra_id = RuntimeFilterId::new(7);
    let mut extra = checked.runtime_filters()[&RuntimeFilterId::new(BROADCAST_FILTER)].clone();
    extra.id = extra_id;
    let endpoint = |fragment: u32| RuntimeFilterEndpoint {
        fragment: FragmentId::new(fragment),
        node: checked.fragments()[&FragmentId::new(fragment)].root(),
        values: Box::default(),
    };
    let producer = extra.producers[0].clone();
    extra.producers = Box::from([
        RuntimeFilterProducer {
            endpoint: endpoint(BROADCAST_FRAGMENT),
            ..producer.clone()
        },
        RuntimeFilterProducer {
            endpoint: endpoint(PARTITIONED_CONSUMER_FRAGMENT),
            ..producer
        },
    ]);
    let consumer = extra.consumers[0].clone();
    extra.consumers = Box::from([
        RuntimeFilterConsumer {
            endpoint: endpoint(PARTITIONED_PRODUCER_FRAGMENT),
            ..consumer.clone()
        },
        RuntimeFilterConsumer {
            endpoint: endpoint(BROADCAST_FRAGMENT),
            ..consumer
        },
    ]);
    let attachments: BTreeMap<u32, Vec<u32>> = BTreeMap::from([
        (PARTITIONED_PRODUCER_FRAGMENT, vec![7, PARTITIONED_FILTER]),
        (BROADCAST_FRAGMENT, vec![7, BROADCAST_FILTER]),
        (PARTITIONED_CONSUMER_FRAGMENT, vec![PARTITIONED_FILTER, 7]),
    ]);
    let mut fragments = checked.fragments().clone();
    for (id, attached) in &attachments {
        let source = &checked.fragments()[&FragmentId::new(*id)];
        let parts = crate::plan::FragmentParts {
            summaries: Box::default(),
            stage_placement: crate::StagePlacementPolicy::Singleton,
            id: source.id(),
            root: source.root(),
            values: source.values().clone(),
            expressions: source.expressions().clone(),
            nodes: source.nodes().clone(),
            sink: source.sink().clone(),
            dop_domain: source.dop_domain(),
            runtime_filters: attached
                .iter()
                .copied()
                .map(RuntimeFilterId::new)
                .collect::<Box<[_]>>(),
            call_requests: source.call_requests().clone(),
        };
        fragments.insert(source.id(), Fragment::from(parts));
    }
    let mut runtime_filters = checked.runtime_filters().clone();
    runtime_filters.insert(extra_id, extra);
    let unchecked =
        |fragments: BTreeMap<FragmentId, Fragment>,
         runtime_filters: BTreeMap<RuntimeFilterId, RuntimeFilter>| {
            PhysicalPlan::from(crate::PhysicalPlanParts {
                constants: crate::ConstantPools::empty(),
                parameters: novarocks_type_contract::SemanticParameters::default(),
                version: checked.version(),
                fragments,
                edges: checked.edges().clone(),
                runtime_filters,
                result_port: checked.result_port().cloned(),
                required: checked.required(),
                annotations: Box::default(),
            })
        };
    let plan = unchecked(fragments.clone(), runtime_filters.clone());
    let node = |fragment: u32| checked.fragments()[&FragmentId::new(fragment)].root();
    let partitioned_scan = scan_node(&checked.fragments()[&FragmentId::new(900)]);

    let numbered = runtime_filter_bindings(&plan).unwrap();
    assert_eq!(
        numbered,
        [
            binding(1, 7, 101, node(101), Consumer(0)),
            binding(2, 101, 101, node(101), Producer(0)),
            binding(3, 7, 412, node(412), Producer(0)),
            binding(4, 7, 412, node(412), Consumer(1)),
            binding(5, 410, 412, node(412), Producer(0)),
            binding(6, 410, 412, node(412), Consumer(0)),
            binding(7, 101, 900, partitioned_scan, Consumer(0)),
            binding(8, 7, 900, node(900), Producer(1)),
        ]
    );
    assert_eq!(legacy_v1_numbering(&plan).unwrap(), numbered);

    // An endpoint in a fragment that does not attach its filter is never
    // numbered, exactly as before.
    let mut detached = fragments.clone();
    let source = &fragments[&FragmentId::new(PARTITIONED_CONSUMER_FRAGMENT)];
    detached.insert(
        source.id(),
        Fragment::from(crate::plan::FragmentParts {
            summaries: Box::default(),
            stage_placement: crate::StagePlacementPolicy::Singleton,
            id: source.id(),
            root: source.root(),
            values: source.values().clone(),
            expressions: source.expressions().clone(),
            nodes: source.nodes().clone(),
            sink: source.sink().clone(),
            dop_domain: source.dop_domain(),
            runtime_filters: Box::from([RuntimeFilterId::new(PARTITIONED_FILTER)]),
            call_requests: source.call_requests().clone(),
        }),
    );
    let plan = unchecked(detached, runtime_filters.clone());
    let numbered = runtime_filter_bindings(&plan).unwrap();
    assert_eq!(numbered.len(), 7);
    assert_eq!(legacy_v1_numbering(&plan).unwrap(), numbered);

    // An attachment the plan does not define is an error, never a skip.
    let mut absent = runtime_filters;
    absent.remove(&extra_id);
    let plan = unchecked(fragments, absent);
    assert_eq!(
        runtime_filter_bindings(&plan).unwrap_err(),
        RuntimeFilterBindingError::AbsentRuntimeFilter {
            fragment: FragmentId::new(PARTITIONED_PRODUCER_FRAGMENT),
            filter: extra_id,
        }
    );
    assert!(legacy_v1_numbering(&plan).is_err());
}

#[test]
fn local_cut_law_refuses_every_binding_table_but_the_plan_slice() {
    use RuntimeFilterBindingRole::{Consumer, Producer};
    let plan = numbered_plan();
    let id = FragmentId::new(BROADCAST_FRAGMENT);
    let fragment = &plan.fragments()[&id];
    let valid = fragment_cuts(&plan, id).unwrap();
    assert_eq!(
        &*valid.runtime_filter_bindings,
        [
            cut(2, BROADCAST_FILTER, Producer(0)),
            cut(3, BROADCAST_FILTER, Consumer(0)),
        ]
    );
    validate_fragment(fragment, &valid).unwrap();
    let path = "fragments[412].cuts.runtime_filter_bindings";
    let cases: [(&str, Vec<RuntimeFilterBindingCut>, &str, &str); 10] = [
        (
            "missing",
            vec![cut(2, BROADCAST_FILTER, Producer(0))],
            "",
            "local consumer 0 of runtime filter 410 has no binding",
        ),
        (
            "extra remote endpoint",
            vec![
                cut(2, BROADCAST_FILTER, Producer(0)),
                cut(3, BROADCAST_FILTER, Consumer(0)),
                cut(4, PARTITIONED_FILTER, Producer(0)),
            ],
            "[2]",
            "runtime-filter binding names producer 0 of runtime filter 101, which the fragment cuts do not define",
        ),
        (
            "extra undefined endpoint",
            vec![
                cut(2, BROADCAST_FILTER, Producer(0)),
                cut(3, BROADCAST_FILTER, Consumer(0)),
                cut(4, BROADCAST_FILTER, Consumer(1)),
            ],
            "[2]",
            "runtime-filter binding names consumer 1 of runtime filter 410, which the fragment cuts do not define",
        ),
        (
            "duplicate identity",
            vec![
                cut(2, BROADCAST_FILTER, Producer(0)),
                cut(2, BROADCAST_FILTER, Consumer(0)),
            ],
            "[1]",
            "runtime-filter binding identity 2 is not unique",
        ),
        (
            "wrong role",
            vec![
                cut(2, BROADCAST_FILTER, Producer(0)),
                cut(3, BROADCAST_FILTER, Producer(0)),
            ],
            "[1]",
            "local producer 0 of runtime filter 410 has more than one binding",
        ),
        (
            "wrong role leaves its endpoint unbound",
            vec![
                cut(2, BROADCAST_FILTER, Producer(0)),
                cut(3, BROADCAST_FILTER, Producer(0)),
            ],
            "",
            "local consumer 0 of runtime filter 410 has no binding",
        ),
        (
            "unnumbered identity",
            vec![
                cut(0, BROADCAST_FILTER, Producer(0)),
                cut(1, BROADCAST_FILTER, Consumer(0)),
            ],
            "[0]",
            "runtime-filter binding identity 0 is never numbered",
        ),
        (
            "out of numbering order",
            vec![
                cut(2, BROADCAST_FILTER, Consumer(0)),
                cut(3, BROADCAST_FILTER, Producer(0)),
            ],
            "[0]",
            "runtime-filter binding is out of numbering order: expected producer 0 of runtime filter 410",
        ),
        (
            "swapped identities",
            vec![
                cut(3, BROADCAST_FILTER, Producer(0)),
                cut(2, BROADCAST_FILTER, Consumer(0)),
            ],
            "[1]",
            "runtime-filter binding identities of one fragment are not consecutive",
        ),
        (
            "gap",
            vec![
                cut(2, BROADCAST_FILTER, Producer(0)),
                cut(4, BROADCAST_FILTER, Consumer(0)),
            ],
            "[1]",
            "runtime-filter binding identities of one fragment are not consecutive",
        ),
    ];
    for (name, bindings, suffix, message) in cases {
        let mut tampered = valid.clone();
        tampered.runtime_filter_bindings = bindings.into_boxed_slice();
        let errors = validate_fragment(fragment, &tampered).unwrap_err();
        let expected_path = format!("{path}{suffix}");
        assert!(
            errors
                .errors()
                .iter()
                .any(|error| error.path() == expected_path && error.message() == message),
            "{name}: {errors}"
        );
    }

    // A remote endpoint of a filter the fragment does carry is named as such.
    let producer_fragment = FragmentId::new(PARTITIONED_PRODUCER_FRAGMENT);
    let mut remote = fragment_cuts(&plan, producer_fragment).unwrap();
    assert_eq!(
        &*remote.runtime_filter_bindings,
        [cut(1, PARTITIONED_FILTER, Producer(0))]
    );
    remote.runtime_filter_bindings = Box::from([
        cut(1, PARTITIONED_FILTER, Producer(0)),
        cut(2, PARTITIONED_FILTER, Consumer(0)),
    ]);
    let errors = validate_fragment(&plan.fragments()[&producer_fragment], &remote).unwrap_err();
    assert!(
        errors.errors().iter().any(|error| error.path()
            == "fragments[101].cuts.runtime_filter_bindings[1]"
            && error.message()
                == "runtime-filter binding names remote consumer 0 of runtime filter 101 in fragment 900"),
        "{errors}"
    );
}
