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

//! Checked constant sources for hand-authored physical test inputs.
//! The fixture owns each source table explicitly; consumers pass `constants()`
//! to the actual physical author rather than synthesizing dangling references.
use novarocks_constant_contract::{ConstantPolicy, ConstantValue};
use novarocks_physical_plan as p;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::Arc;

struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
pub(crate) fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
pub(crate) type Source = Box<dyn FnOnce(&p::ValueType) -> ConstantValue>;
macro_rules! scalar_factory {
    ($name:ident, $payload:ty, $factory:ident) => {
        pub(crate) fn $name(value: $payload) -> Source {
            Box::new(move |ty| {
                ConstantValue::$factory(
                    Arc::new(ty.try_to_field("fixture").unwrap()),
                    ty.clone(),
                    value,
                    policy(),
                    CompilePhase::Validate,
                    &Setup,
                )
                .unwrap()
            })
        }
    };
}
scalar_factory!(i64, i64, from_i64);
scalar_factory!(boolean, bool, from_boolean);

pub(crate) fn insert(
    pools: &mut p::ConstantPools,
    source: ConstantValue,
) -> novarocks_expression_contract::ExprKind {
    let mut ordinal = 0x8000_0000u32
        .checked_add(u32::try_from(pools.entries().len()).unwrap())
        .unwrap();
    while pools
        .entries()
        .contains_key(&p::ConstantPoolId::new(ordinal))
    {
        ordinal += 1;
    }
    let pool = p::ConstantPoolId::new(ordinal);
    let reference = p::ConstantReference {
        pool,
        ordinal: source.ordinal(),
    };
    pools.insert(pool, source.pool().clone()).unwrap();
    novarocks_expression_contract::ExprKind::Constant(reference)
}

/// The actual unit root addresses used by borrowed receiving namespaces.
/// This fixture projects its explicit source; it creates no missing environment.
pub(crate) fn wire_roots(
    unit: &novarocks_expression_contract::BoundExprSet,
) -> Vec<novarocks_proto_models::physical_package_v2::ExpressionRoot> {
    use novarocks_expression_contract::RootScope;
    use novarocks_proto_models::physical_package_v2 as wire;
    unit.roots()
        .iter()
        .zip(unit.root_use_ids())
        .map(|(root, use_id)| wire::ExpressionRoot {
            definition_id: Some(root.definition.get()),
            demand: crate::physical_control_v2::encode_demand(root.demand),
            scope: Some(wire::RootScope {
                kind: Some(match root.scope {
                    RootScope::Port(port) => wire::root_scope::Kind::Port(port),
                    RootScope::AllPorts => wire::root_scope::Kind::AllPorts(
                        novarocks_proto_models::physical_control_v2::Empty {},
                    ),
                }),
            }),
            use_id: Some(use_id.get()),
        })
        .collect()
}

/// Explicit pure leaf/root author for fixed fixtures. The caller names the
/// exact node-local scope; function calls must use their real claims instead.
pub(crate) fn pure_root(
    builder: &mut p::FragmentBuilder,
    node: p::NodeId,
    ty: p::ValueType,
    kind: novarocks_expression_contract::ExprKind,
    scope: novarocks_expression_contract::RootScope,
) -> novarocks_expression_contract::RootOrdinal {
    let draft = builder.unit(node).unwrap();
    let definition = draft.add(None, ty, kind).unwrap();
    draft
        .root(
            definition,
            novarocks_type_contract::EvaluationDemand::Value,
            scope,
        )
        .unwrap()
}

/// Seal only drafts the fixture has actually authored. Empty claims are valid
/// only for these fixed non-function units; the real constructor enforces this.
pub(crate) fn seal_pure_units(builder: &mut p::FragmentBuilder) {
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    let nodes = builder
        .construction_nodes()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for node in nodes {
        if builder.construction_unit(node).is_none() {
            continue;
        }
        let ports = builder.node_ports(node).unwrap();
        let draft = builder.take_unit_builder(node).unwrap();
        let mut work = CompileCheckpoints::try_new(&Setup, CompilePhase::Validate).unwrap();
        let unit = draft
            .seal(ports, &mut work)
            .unwrap()
            .finish(std::collections::BTreeMap::new(), &mut work)
            .unwrap();
        work.finish().unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
}
