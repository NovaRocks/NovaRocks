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

//! Original node projection geometry for the new unit envelope only.
//! Definitions, control, claims and requests keep their own sole authors.
use crate::physical_node_v2::{
    Model, NodeCodecError as E, NodeProjectionFacts, NodeProjectionLimits, add, bytes,
};
use novarocks_expression_contract::{BoundExprSet, ExprPort, ExprRoot, ValueId, ValueType};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, ExpressionUseId};
use std::mem::size_of;
fn floor(source: usize, known: usize) -> Result<(), E> {
    if source < known {
        Err(E::InvalidShape(
            "unit source invoice omits envelope backing",
        ))
    } else {
        Ok(())
    }
}
pub(super) fn encode(
    unit: &BoundExprSet,
    source: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, E> {
    let mut model = Model {
        items: add(unit.ports().len(), unit.roots().len())?,
        refs: unit.roots().len(),
        ..Model::default()
    };
    let mut known = add(
        size_of::<BoundExprSet>(),
        add(
            bytes::<ExprPort>(unit.ports().len())?,
            add(
                bytes::<ExprRoot>(unit.roots().len())?,
                bytes::<ExpressionUseId>(unit.root_use_ids().len())?,
            )?,
        )?,
    )?;
    model.request::<wire::ExpressionPort>(unit.ports().len(), 1)?;
    model.request::<wire::ExpressionRoot>(unit.roots().len(), 1)?;
    if limits.is_some() {
        floor(source, known)?;
    }
    admit(&model.numerical_facts(source.max(known), 0, limits)?)?;
    for port in unit.ports() {
        model.items = add(model.items, port.values.len())?;
        model.refs = add(model.refs, port.values.len())?;
        model.request::<wire::ExpressionInput>(port.values.len(), 1)?;
        known = add(known, bytes::<(ValueId, ValueType)>(port.values.len())?)?;
        if limits.is_some() {
            floor(source, known)?;
        }
        admit(&model.numerical_facts(source.max(known), 0, limits)?)?;
        work.step()?;
    }
    model.numerical_facts(source.max(known), 0, limits)
}
pub(super) fn decode<H>(
    unit: &wire::ExpressionUnit,
    source: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    mut creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, crate::host_projection_v2::ProjectionFailure<E, H>> {
    let mut model = Model {
        items: add(unit.ports.len(), unit.roots.len())?,
        refs: unit.roots.len(),
        ..Model::default()
    };
    let mut known = add(
        size_of::<wire::ExpressionUnit>(),
        add(
            bytes::<wire::ExpressionPort>(unit.ports.capacity())?,
            bytes::<wire::ExpressionRoot>(unit.roots.capacity())?,
        )?,
    )?;
    model.request::<ExprPort>(unit.ports.len(), 2)?;
    model.request::<ExprRoot>(unit.roots.len(), 2)?;
    model.request::<ExpressionUseId>(unit.roots.len(), 2)?;
    let arc =
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            BoundExprSet,
        >())
        .map_err(|_| E::Control(CompileControlError::ResourceExhausted))?;
    model.layout_request(arc, 1)?;
    let mut previous = 0;
    observe_decode(
        &model,
        source,
        known,
        limits,
        admit,
        &mut creation,
        &mut previous,
    )?;
    for port in &unit.ports {
        model.items = add(model.items, port.values.len())?;
        model.refs = add(model.refs, port.values.len())?;
        model.request::<(ValueId, ValueType)>(port.values.len(), 2)?;
        known = add(
            known,
            bytes::<wire::ExpressionInput>(port.values.capacity())?,
        )?;
        // FVT copies retain their exact immutable original holders, no deep copy.
        model.delegated_work = add(
            model.delegated_work,
            port.values
                .len()
                .checked_mul(2)
                .ok_or_else(|| E::Control(CompileControlError::ResourceExhausted))?,
        )?;
        observe_decode(
            &model,
            source,
            known,
            limits,
            admit,
            &mut creation,
            &mut previous,
        )?;
        work.step().map_err(E::from)?;
    }
    Ok(model.numerical_facts(source.max(known), 0, limits)?)
}

fn observe_decode<H>(
    model: &Model,
    source: usize,
    known: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    creation: &mut Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
    previous: &mut usize,
) -> Result<(), crate::host_projection_v2::ProjectionFailure<E, H>> {
    use crate::host_projection_v2::ProjectionFailure as Failure;
    if limits.is_some() {
        floor(source, known)?;
    }
    let facts = model.numerical_facts(source.max(known), 0, limits)?;
    admit(&facts).map_err(E::from)?;
    let additional = facts
        .allocation_request_bytes_upper_bound
        .checked_sub(*previous)
        .ok_or_else(|| E::Control(CompileControlError::ResourceExhausted))?;
    if additional != 0
        && let Some(creation) = creation.as_deref_mut()
    {
        creation
            .observe_opaque_upper_bound(additional)
            .map_err(Failure::Host)?;
    }
    *previous = facts.allocation_request_bytes_upper_bound;
    Ok(())
}

/// Preserve the fragment's raw root-reference bound before unit decoding.
pub(super) fn admit_fragment_roots(
    previous: &mut usize,
    count: usize,
) -> Result<(), crate::physical_expression_v2::ExpressionCodecError> {
    let count = previous
        .checked_add(count)
        .filter(|count| *count <= novarocks_type_contract::MAX_CONTROL_USE_REFERENCES)
        .ok_or(novarocks_type_contract::ExpressionControlFlowError::TooManyItems)
        .map_err(novarocks_expression_contract::UnitError::from)?;
    *previous = count;
    Ok(())
}
