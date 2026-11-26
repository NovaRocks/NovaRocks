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

//! Borrow the actual unit constructor's request facts and storage ports.
//! No second DAG walk, scope identity, quota or guessed SDK geometry lives here.
use super::PackageDecodeError;
use crate::{
    host_projection_v2::ProjectionFailure as Failure,
    physical_expression_v2::ExpressionCodecError,
    physical_node_v2::{Model, NodeProjectionFacts, NodeProjectionLimits, add},
};
use novarocks_expression_contract::{
    BoundExprSet, UnitConstructionFailure, UnitConstructionRequest, UnitConstructionStorage,
    UnitLimits, UnitParts,
};
use novarocks_type_contract::{
    CompileCheckpoints, ValueSchemaCreation, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueSchemaScopeBeginFailure, ValueTypeStorageLifetime,
};
use std::alloc::Layout;

type ReceivingFailure<H> = Failure<PackageDecodeError, H>;

/// A reborrow of the exact original host port, preserving its original error.
struct BorrowedScope<'a, H>(&'a mut dyn ValueSchemaCreationScope<H>);
impl<H> ValueSchemaCreationScope<ReceivingFailure<H>> for BorrowedScope<'_, H> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), ReceivingFailure<H>> {
        self.0.request_layout(layout).map_err(Failure::Host)
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), ReceivingFailure<H>> {
        self.0
            .observe_opaque_upper_bound(bytes)
            .map_err(Failure::Host)
    }
}
fn failure<H>(error: UnitConstructionFailure<ReceivingFailure<H>>) -> ReceivingFailure<H> {
    match error {
        UnitConstructionFailure::Control(error) => error.into(),
        UnitConstructionFailure::Creator(error) | UnitConstructionFailure::Observer(error) => error,
        UnitConstructionFailure::Resource(error) => {
            ExpressionCodecError::UnitResource(error).into()
        }
        UnitConstructionFailure::Owner(error) => {
            ExpressionCodecError::ConstructedUnit(error).into()
        }
    }
}

pub(super) fn construct<H, F: ValueSchemaCreationFactory<H>>(
    parts: UnitParts,
    unit_limits: UnitLimits,
    source: usize,
    limits: NodeProjectionLimits,
    creation: Option<&mut ValueSchemaCreation<'_, H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(BoundExprSet, NodeProjectionFacts), ReceivingFailure<H>> {
    let mut model = Model::default();
    let mut observe = |request| {
        match request {
            UnitConstructionRequest::Exact { layout, .. } => model.layout_request(layout, 1)?,
            UnitConstructionRequest::Opaque { facts, .. } => {
                model.requests = add(model.requests, facts.allocation_requests_upper_bound)?;
                model.requested = add(model.requested, facts.request_bytes_upper_bound)?;
                model.delegated_work =
                    add(model.delegated_work, facts.cumulative_work_upper_bound)?;
            }
        }
        Ok::<_, ReceivingFailure<H>>(())
    };
    let unit = if let Some(factory) = factory {
        work.flush()?;
        let mut temporary = factory
            .begin_scope_observed(&mut |layout| {
                observe(UnitConstructionRequest::Exact {
                    storage: UnitConstructionStorage::Temporary,
                    layout,
                })
            })
            .map_err(|error| match error {
                ValueSchemaScopeBeginFailure::Observer(error) => error,
                ValueSchemaScopeBeginFailure::Creator(error) => Failure::Host(error),
            })?;
        work.flush()?;
        let mut diagnostic = factory
            .begin_scope_observed(&mut |layout| {
                observe(UnitConstructionRequest::Exact {
                    storage: UnitConstructionStorage::Diagnostic,
                    layout,
                })
            })
            .map_err(|error| match error {
                ValueSchemaScopeBeginFailure::Observer(error) => error,
                ValueSchemaScopeBeginFailure::Creator(error) => Failure::Host(error),
            })?;
        BoundExprSet::try_from_parts_with_creator_observed_in(
            parts,
            unit_limits,
            &mut BorrowedScope(&mut temporary),
            &mut BorrowedScope(&mut diagnostic),
            &mut observe,
            work,
        )
        .map_err(failure)?
    } else if let Some(creation) = creation {
        BoundExprSet::try_from_parts_with_single_creator_observed_in(
            parts,
            unit_limits,
            &mut BorrowedScope(creation),
            &mut observe,
            work,
        )
        .map_err(failure)?
    } else {
        BoundExprSet::try_from_parts_observed_in(parts, unit_limits, &mut observe, work)
            .map_err(failure)?
    };
    let facts = model.numerical_facts(source, 0, limits)?;
    Ok((unit, facts))
}
