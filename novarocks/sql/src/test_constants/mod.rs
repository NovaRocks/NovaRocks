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
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

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
pub(crate) fn null() -> Source {
    Box::new(|ty| {
        ConstantValue::null(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty.clone(),
            policy(),
            CompilePhase::Validate,
            &Setup,
        )
        .unwrap()
    })
}
pub(crate) fn utf8(value: String) -> Source {
    Box::new(move |ty| {
        ConstantValue::from_utf8(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty.clone(),
            &value,
            policy(),
            CompilePhase::Validate,
            &Setup,
        )
        .unwrap()
    })
}

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
pub(crate) fn merge(target: &mut p::ConstantPools, sources: &p::ConstantPools) {
    for (&id, pool) in sources.entries() {
        if let Some(existing) = target.entries().get(&id) {
            assert_eq!(existing.backing_identity(), pool.backing_identity());
        } else {
            target.insert(id, pool.clone()).unwrap();
        }
    }
}

#[derive(Clone)]
pub(crate) struct Fragment {
    fragment: p::Fragment,
    constants: p::ConstantPools,
}
impl Fragment {
    pub(crate) fn into_parts(self) -> (p::Fragment, p::ConstantPools) {
        (self.fragment, self.constants)
    }
    pub(crate) fn constants(&self) -> &p::ConstantPools {
        &self.constants
    }
}
impl Deref for Fragment {
    type Target = p::Fragment;
    fn deref(&self) -> &Self::Target {
        &self.fragment
    }
}
impl DerefMut for Fragment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fragment
    }
}

pub(crate) struct FragmentBuilder {
    builder: p::FragmentBuilder,
    constants: p::ConstantPools,
}
impl FragmentBuilder {
    pub(crate) fn finish_definition(
        mut self,
        root: p::NodeId,
        sink: p::FragmentSink,
        dop: p::PipelineDopDomain,
        placement: p::StagePlacementPolicy,
    ) -> Result<Fragment, p::ValidationErrors> {
        self.seal_pure_units();
        Ok(Fragment {
            fragment: self.builder.finish_definition(root, sink, dop, placement)?,
            constants: self.constants,
        })
    }
    pub(crate) fn add_value(
        &mut self,
        ty: p::ValueType,
        origin: p::ValueOrigin,
    ) -> Result<p::ValueId, p::BuildError> {
        self.builder.add_value(ty, origin)
    }
    pub(crate) fn add_expression(
        &mut self,
        node: p::NodeId,
        ty: p::ValueType,
        kind: novarocks_expression_contract::ExprKind,
    ) -> Result<p::ExprId, novarocks_expression_contract::UnitError> {
        self.builder
            .unit(node)
            .expect("fixture definition belongs to an unfinished node unit")
            .add(None, ty, kind)
    }
    // Pure fixture construction consumes exactly its explicit roots/ports.
    // Function fixtures must install their own real requests/control/claims.
    fn seal_pure_units(&mut self) {
        use novarocks_type_contract::CompileCheckpoints;
        let nodes = self
            .builder
            .construction_nodes()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for node in nodes {
            if self.builder.construction_unit(node).is_none() {
                continue;
            }
            let ports = self.builder.node_ports(node).unwrap();
            let draft = self.builder.take_unit_builder(node).unwrap();
            let mut work = CompileCheckpoints::try_new(&Setup, CompilePhase::Validate).unwrap();
            let unit = draft
                .seal(ports, &mut work)
                .unwrap()
                .finish(std::collections::BTreeMap::new(), &mut work)
                .unwrap();
            work.finish().unwrap();
            self.builder.install_unit(node, Arc::new(unit)).unwrap();
        }
    }
    pub(crate) fn new(id: p::FragmentId) -> Self {
        Self {
            builder: p::FragmentBuilder::new(id),
            constants: p::ConstantPools::empty(),
        }
    }
    pub(crate) fn constant_kind(
        &mut self,
        ty: &p::ValueType,
        source: Source,
    ) -> novarocks_expression_contract::ExprKind {
        insert(&mut self.constants, source(ty))
    }
    pub(crate) fn constant(
        &mut self,
        owner: p::NodeId,
        ty: p::ValueType,
        source: Source,
    ) -> p::ExprId {
        let kind = self.constant_kind(&ty, source);
        self.add_expression(owner, ty, kind).unwrap()
    }
    pub(crate) fn finish_structure(
        mut self,
        root: p::NodeId,
        sink: p::FragmentSink,
        dop: p::PipelineDopDomain,
        limits: p::PlanLimits,
        control: &dyn PureCompileControl,
        placement: p::StagePlacementPolicy,
    ) -> Result<Fragment, p::FragmentStructureError> {
        self.seal_pure_units();
        Ok(Fragment {
            fragment: self
                .builder
                .finish_structure(root, sink, dop, limits, control, placement)?,
            constants: self.constants,
        })
    }
}
impl Deref for FragmentBuilder {
    type Target = p::FragmentBuilder;
    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}
impl DerefMut for FragmentBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.builder
    }
}
