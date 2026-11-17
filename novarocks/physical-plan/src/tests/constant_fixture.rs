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

//! Test-owned checked scalar sources. The owning fixture keeps the returned
//! table for the lifetime of every fragment using one of its references.
use crate::{ConstantPoolId, ConstantPools, ConstantReference, ExprKind, ValueType};
use novarocks_constant_contract::{ConstantPolicy, ConstantValue};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::Arc;

struct Control;
impl PureCompileControl for Control {
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

pub(crate) struct ConstantFixture {
    pools: ConstantPools,
}
impl Default for ConstantFixture {
    fn default() -> Self {
        Self::new()
    }
}
impl ConstantFixture {
    pub(crate) fn new() -> Self {
        Self {
            pools: ConstantPools::empty(),
        }
    }
    fn insert(&mut self, value: ConstantValue) -> ExprKind {
        let pool = ConstantPoolId::new(u32::try_from(self.pools.entries().len() + 1).unwrap());
        let reference = ConstantReference {
            pool,
            ordinal: value.ordinal(),
        };
        self.pools.insert(pool, value.pool().clone()).unwrap();
        ExprKind::Constant(reference)
    }
    pub(crate) fn i64(&mut self, value: i64, ty: &ValueType) -> ExprKind {
        self.insert(
            ConstantValue::from_i64(
                Arc::new(ty.try_to_field("fixture").unwrap()),
                ty.clone(),
                value,
                policy(),
                CompilePhase::Validate,
                &Control,
            )
            .unwrap(),
        )
    }
    pub(crate) fn boolean(&mut self, value: bool, ty: &ValueType) -> ExprKind {
        self.insert(
            ConstantValue::from_boolean(
                Arc::new(ty.try_to_field("fixture").unwrap()),
                ty.clone(),
                value,
                policy(),
                CompilePhase::Validate,
                &Control,
            )
            .unwrap(),
        )
    }
    pub(crate) fn utf8(&mut self, value: &str, ty: &ValueType) -> ExprKind {
        self.insert(
            ConstantValue::from_utf8(
                Arc::new(ty.try_to_field("fixture").unwrap()),
                ty.clone(),
                value,
                policy(),
                CompilePhase::Validate,
                &Control,
            )
            .unwrap(),
        )
    }
    pub(crate) fn null(&mut self, ty: &ValueType) -> ExprKind {
        self.insert(
            ConstantValue::null(
                Arc::new(ty.try_to_field("fixture").unwrap()),
                ty.clone(),
                policy(),
                CompilePhase::Validate,
                &Control,
            )
            .unwrap(),
        )
    }
    pub(crate) fn u64(&mut self, value: u64, ty: &ValueType) -> ExprKind {
        self.insert(
            ConstantValue::from_u64(
                Arc::new(ty.try_to_field("fixture").unwrap()),
                ty.clone(),
                value,
                policy(),
                CompilePhase::Validate,
                &Control,
            )
            .unwrap(),
        )
    }
    pub(crate) fn try_largeint(
        &mut self,
        value: i128,
        ty: &ValueType,
    ) -> Result<ExprKind, novarocks_constant_contract::ConstantError> {
        let value = ConstantValue::from_largeint(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty.clone(),
            value,
            policy(),
            CompilePhase::Validate,
            &Control,
        )?;
        Ok(self.insert(value))
    }
    pub(crate) fn binary(&mut self, value: &[u8], ty: &ValueType) -> ExprKind {
        self.insert(
            ConstantValue::from_binary(
                Arc::new(ty.try_to_field("fixture").unwrap()),
                ty.clone(),
                value,
                policy(),
                CompilePhase::Validate,
                &Control,
            )
            .unwrap(),
        )
    }
    pub(crate) fn pools(&self) -> &ConstantPools {
        &self.pools
    }
    // Select only scalar pools authored by this fixture. Collection and other
    // independently authored inputs remain with their caller; the ordinary
    // plan/package gate still validates the assembled table and every address.
    pub(crate) fn for_fragment(&self, fragment: &crate::Fragment) -> ConstantPools {
        let mut selected = ConstantPools::empty();
        let mut references = Vec::new();
        for node in fragment.nodes().values() {
            if let Some(unit) = &node.expressions {
                for expression in unit.definitions() {
                    if let ExprKind::Constant(reference) = expression.kind {
                        references.push(reference);
                    }
                }
            }
            if let crate::NodeKind::Window(spec) = &node.kind {
                for call in &spec.calls {
                    if let Some(frame) = &call.frame {
                        for bound in [&frame.start, &frame.end] {
                            if let crate::WindowBound::Preceding(reference)
                            | crate::WindowBound::Following(reference) = bound
                            {
                                references.push(*reference);
                            }
                        }
                    }
                }
            }
        }
        for reference in references {
            if let Some(pool) = self.pools.entries().get(&reference.pool)
                && !selected.entries().contains_key(&reference.pool)
            {
                selected.insert(reference.pool, pool.clone()).unwrap();
            }
        }
        selected
    }
    pub(crate) fn into_pools(self) -> ConstantPools {
        self.pools
    }
}

pub(crate) fn publish_plan(
    builder: crate::PlanBuilder,
    constants: &ConstantFixture,
) -> Result<crate::PhysicalPlan, crate::ValidationErrors> {
    match crate::builder::fixture_constants::finish(builder, constants) {
        Ok(plan) => Ok(plan),
        Err(crate::PlanConstructionError::Structure(error)) => Err(error),
        Err(error) => panic!("checked fixture plan publication failed: {error}"),
    }
}
