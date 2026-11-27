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

//! Test-owned checked scalar inputs and the tables used by real plan authors.
//! Every declaration is explicit. Rebinding here authors a fixture field from
//! an existing one-row payload; it is not a production conversion or fallback.
use super::{FixtureControl, constant_policy};
use arrow::{
    array::Array,
    datatypes::{DataType, Field, TimeUnit},
};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{ConstantPool, ConstantValue};
use novarocks_physical_plan as p;
use novarocks_type_contract::{CompilePhase, FunctionValueType, PureCompileControl};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};
/// One fixture plan's constant source owner. Builders borrow this same owner;
/// no identity or source backing is stored in process-global state.
#[derive(Clone)]
pub(crate) struct Sources(Arc<Mutex<p::ConstantPools>>);
impl Sources {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(p::ConstantPools::empty())))
    }
    pub(crate) fn from_pools(pools: p::ConstantPools) -> Self {
        Self(Arc::new(Mutex::new(pools)))
    }
    pub(crate) fn snapshot(&self) -> p::ConstantPools {
        self.0.lock().unwrap().clone()
    }
    fn kind(&self, source: ConstantValue) -> ExprKind {
        insert(&mut self.0.lock().unwrap(), source)
    }
}

pub(crate) fn control() -> FixtureControl {
    FixtureControl
}
fn field(ty: &FunctionValueType) -> Arc<Field> {
    Arc::new(ty.try_to_field("fixture").unwrap())
}
fn primitive_type(carrier: DataType) -> FunctionValueType {
    FunctionValueType::new(carrier, false)
}
pub(crate) fn i64(value: i64) -> ConstantValue {
    let ty = primitive_type(DataType::Int64);
    ConstantValue::from_i64(
        field(&ty),
        ty,
        value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn boolean(value: bool) -> ConstantValue {
    let ty = primitive_type(DataType::Boolean);
    ConstantValue::from_boolean(
        field(&ty),
        ty,
        value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn f64_bits(value: u64) -> ConstantValue {
    let ty = primitive_type(DataType::Float64);
    ConstantValue::from_f64_bits(
        field(&ty),
        ty,
        value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn utf8(value: Box<str>) -> ConstantValue {
    let ty = primitive_type(DataType::Utf8);
    ConstantValue::from_utf8(
        field(&ty),
        ty,
        &value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn binary(value: Box<[u8]>) -> ConstantValue {
    let ty = primitive_type(DataType::Binary);
    ConstantValue::from_binary(
        field(&ty),
        ty,
        &value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn date32(value: i32) -> ConstantValue {
    let ty = primitive_type(DataType::Date32);
    ConstantValue::from_date32(
        field(&ty),
        ty,
        value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn timestamp(value: i64) -> ConstantValue {
    let ty = primitive_type(DataType::Timestamp(TimeUnit::Microsecond, None));
    ConstantValue::from_timestamp(
        field(&ty),
        ty,
        value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn decimal128(value: i128) -> ConstantValue {
    let ty = primitive_type(DataType::Decimal128(38, 0));
    ConstantValue::from_decimal128(
        field(&ty),
        ty,
        value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn largeint(value: i128) -> ConstantValue {
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        novarocks_type_contract::ValueLogicalType::LargeInt,
    )
    .unwrap();
    ConstantValue::from_largeint(
        field(&ty),
        ty,
        value,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn null() -> ConstantValue {
    let ty = FunctionValueType::new(DataType::Null, true);
    ConstantValue::null(
        field(&ty),
        ty,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
pub(crate) fn is_null(source: &ConstantValue) -> bool {
    source
        .is_null_observed(CompilePhase::Validate, &FixtureControl)
        .unwrap()
}
/// Preserve the exact declared field and raw scalar storage. Decimal scales
/// and timestamp units are fixture declarations over the supplied raw payload,
/// as they were in the original seeds; this never casts a runtime value.
pub(crate) fn typed(source: &ConstantValue, ty: &FunctionValueType) -> ConstantValue {
    if source.value_type() == ty {
        return source.clone();
    }
    if is_null(source) {
        return ConstantValue::null(
            field(ty),
            ty.clone(),
            constant_policy(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
    }
    let array = source.pool().array().slice(source.ordinal() as usize, 1);
    let same = source.value_type().data_type() == ty.data_type();
    let declared_payload = matches!(
        (source.value_type().data_type(), ty.data_type()),
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _))
            | (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
    );
    assert!(
        same || declared_payload,
        "fixture payload differs from declared carrier: {:?} -> {:?}",
        source.value_type(),
        ty
    );
    let data = if same {
        array.to_data()
    } else {
        array
            .to_data()
            .into_builder()
            .data_type(ty.data_type().clone())
            .build()
            .unwrap()
    };
    ConstantPool::try_new(
        field(ty),
        ty.clone(),
        data,
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
    .value(0)
    .unwrap()
}
pub(crate) fn insert(pools: &mut p::ConstantPools, source: ConstantValue) -> ExprKind {
    // A selected ordinal keeps the original admitted backing and address when
    // the fixture owner already holds that pool. This is identity, not value
    // equality: separately authored equal scalars remain separate sources.
    if let Some((&pool, _)) = pools
        .entries()
        .iter()
        .find(|(_, pool)| pool.backing_identity() == source.pool().backing_identity())
    {
        return ExprKind::Constant(p::ConstantReference {
            pool,
            ordinal: source.ordinal(),
        });
    }
    // Current fixture-authored manual addresses are low column/list IDs or
    // named source-gate IDs. Start separately, while still checking every
    // candidate against this owner's actual table; no range is reserved.
    let mut next = 0x8000_0000u32;
    while pools.entries().contains_key(&p::ConstantPoolId::new(next)) {
        next = next.checked_add(1).unwrap();
    }
    let pool = p::ConstantPoolId::new(next);
    let reference = p::ConstantReference {
        pool,
        ordinal: source.ordinal(),
    };
    pools.insert(pool, source.pool().clone()).unwrap();
    ExprKind::Constant(reference)
}
pub(crate) fn merge(target: &mut p::ConstantPools, sources: &p::ConstantPools) {
    for (&id, pool) in sources.entries() {
        if let Some(old) = target.entries().get(&id) {
            assert_eq!(old.backing_identity(), pool.backing_identity());
        } else {
            target.insert(id, pool.clone()).unwrap();
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Fragment {
    physical: p::Fragment,
    constants: p::ConstantPools,
}
impl Fragment {
    pub(crate) fn from_parts(physical: p::Fragment, constants: p::ConstantPools) -> Self {
        Self {
            physical,
            constants,
        }
    }
    pub(crate) fn into_parts(self) -> (p::Fragment, p::ConstantPools) {
        (self.physical, self.constants)
    }
}
impl Deref for Fragment {
    type Target = p::Fragment;
    fn deref(&self) -> &Self::Target {
        &self.physical
    }
}
impl DerefMut for Fragment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.physical
    }
}

pub(crate) struct FragmentBuilder {
    physical: p::FragmentBuilder,
    sources: Sources,
}
impl FragmentBuilder {
    pub(crate) fn add_value(
        &mut self,
        ty: p::ValueType,
        origin: p::ValueOrigin,
    ) -> Result<p::ValueId, p::BuildError> {
        self.physical.add_value(ty, origin)
    }
    pub(crate) fn new(id: p::FragmentId, sources: &Sources) -> Self {
        Self {
            physical: p::FragmentBuilder::new(id),
            sources: sources.clone(),
        }
    }
    pub(crate) fn constant_kind(
        &mut self,
        ty: &FunctionValueType,
        source: &ConstantValue,
    ) -> ExprKind {
        self.sources.kind(typed(source, ty))
    }
    pub(crate) fn constant(
        &mut self,
        node: p::NodeId,
        ty: FunctionValueType,
        source: ConstantValue,
    ) -> p::ExprId {
        let kind = self.constant_kind(&ty, &source);
        self.physical
            .unit(node)
            .unwrap()
            .add(None, ty, kind)
            .unwrap()
    }
    pub(crate) fn finish_structure(
        self,
        root: p::NodeId,
        sink: p::FragmentSink,
        dop: p::PipelineDopDomain,
        limits: p::PlanLimits,
        control: &dyn PureCompileControl,
        placement: p::StagePlacementPolicy,
    ) -> Result<Fragment, p::FragmentStructureError> {
        Ok(Fragment {
            physical: self
                .physical
                .finish_structure(root, sink, dop, limits, control, placement)?,
            constants: self.sources.snapshot(),
        })
    }
    pub(crate) fn finish_definition(
        self,
        root: p::NodeId,
        sink: p::FragmentSink,
        dop: p::PipelineDopDomain,
        placement: p::StagePlacementPolicy,
    ) -> Result<Fragment, p::ValidationErrors> {
        Ok(Fragment {
            physical: self
                .physical
                .finish_definition(root, sink, dop, placement)?,
            constants: self.sources.snapshot(),
        })
    }
}
impl Deref for FragmentBuilder {
    type Target = p::FragmentBuilder;
    fn deref(&self) -> &Self::Target {
        &self.physical
    }
}
impl DerefMut for FragmentBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.physical
    }
}

pub(crate) struct PlanBuilder {
    physical: p::PlanBuilder,
    _sources: Sources,
}
impl PlanBuilder {
    pub(crate) fn new(version: p::PlanVersionId, sources: &Sources) -> Self {
        Self {
            physical: p::PlanBuilder::new(version),
            _sources: sources.clone(),
        }
    }
    pub(crate) fn with_constant_pools(mut self, pools: p::ConstantPools) -> Self {
        for (&id, pool) in pools.entries() {
            self.physical
                .insert_constant_pool(id, pool.clone())
                .unwrap();
        }
        self
    }
    pub(crate) fn with_semantic_parameters(
        mut self,
        parameters: novarocks_type_contract::SemanticParameters,
    ) -> Self {
        self.physical = self.physical.with_semantic_parameters(parameters);
        self
    }
    pub(crate) fn add_fragment(&mut self, fragment: Fragment) -> Result<(), p::BuildError> {
        let (physical, constants) = fragment.into_parts();
        for (&id, pool) in constants.entries() {
            if let Some(old) = self.physical.constants().entries().get(&id) {
                assert_eq!(old.backing_identity(), pool.backing_identity());
            } else {
                self.physical
                    .insert_constant_pool(id, pool.clone())
                    .unwrap();
            }
        }
        self.physical.add_fragment(physical)
    }
    pub(crate) fn finish_observed(
        self,
        control: &dyn PureCompileControl,
    ) -> Result<p::PhysicalPlan, p::PlanConstructionError> {
        self.physical.finish_observed(control)
    }
}
impl Deref for PlanBuilder {
    type Target = p::PlanBuilder;
    fn deref(&self) -> &Self::Target {
        &self.physical
    }
}
impl DerefMut for PlanBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.physical
    }
}
