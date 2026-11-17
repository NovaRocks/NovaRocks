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
//! Closed lowering of the original physical integer-series source.
//!
//! Every original bound remains a dynamic cell in the existing Values owner.
//! Its original occurrence is remapped once; the ordinary compiled Frame runs
//! it once over the Values opening port. No constant is folded or recreated.

use crate::lowering::FragmentCompileError;
use arrow_array::new_empty_array;
use arrow_schema::DataType;
use novarocks_local_program::{
    DiagnosticSourceNodeId, LocalOperatorId, LocalOperatorOrigin, LocalOperatorProvenance,
    MetricAggregation, OperatorMetricAggregation, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramNode, ProgramNodeId, ProgramNodeKind, StaticValues, StaticValuesCell,
};
use novarocks_physical_plan::{
    Distribution, FragmentPackage, NodeKind, PhysicalNode, RowMultiplicity,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, ValueLogicalType,
};
use novarocks_types::SlotId;
use std::sync::Arc;

pub(crate) struct PlannedGenerateSeries {
    pub bounds: ProgramNodeId,
    pub node: ProgramNodeId,
    pub parameter_slots: Arc<[SlotId]>,
}

pub(crate) fn parameter_ids(
    node: &PhysicalNode,
) -> Result<[Option<novarocks_expression_contract::RootOrdinal>; 3], FragmentCompileError> {
    let NodeKind::GenerateSeries { start, stop, step } = node.kind else {
        return Err(FragmentCompileError::Invalid(
            "series lowering for another node family",
        ));
    };
    Ok([Some(start), Some(stop), step])
}

pub(crate) fn resource_bound(node: &PhysicalNode) -> Option<(usize, usize)> {
    match node.kind {
        NodeKind::GenerateSeries { step, .. } => Some((1, if step.is_some() { 3 } else { 2 })),
        _ => None,
    }
}

pub(crate) fn admit(
    package: &FragmentPackage,
    node: &PhysicalNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    if !node.inputs.is_empty() || node.output.columns.len() != 1 {
        return Err(FragmentCompileError::Invalid(
            "series source requires zero inputs and one output",
        ));
    }
    // Read the checked physical placement author, never task topology.
    if node.output_properties.distribution != Distribution::Singleton
        || node.output_properties.row_multiplicity != RowMultiplicity::SingleCopy
    {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "series source without Singleton SingleCopy placement",
        });
    }
    let ty = &package
        .fragment()
        .values()
        .get(&node.output.columns[0])
        .ok_or(FragmentCompileError::Invalid("missing series output value"))?
        .ty;
    // Only carriers actually read by the original core are admitted. Unsigned
    // carriers retain their original result-conversion Data error at runtime.
    let supported = match ty.logical_type() {
        ValueLogicalType::Physical => matches!(
            ty.data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::FixedSizeBinary(16)
        ),
        ValueLogicalType::LargeInt => ty.data_type() == &DataType::FixedSizeBinary(16),
        _ => false,
    };
    work.step()?;
    if !supported {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "series source outside the original integer value domains",
        });
    }
    for id in parameter_ids(node)?.into_iter().flatten() {
        let expression = crate::expressions::bound_root(package, node.id, id)?;
        let same = expression.ty == *ty;
        work.step()?;
        if !same {
            return Err(FragmentCompileError::Invalid(
                "series parameter differs from its original source type",
            ));
        }
    }
    Ok(())
}

pub(crate) struct LoweredGenerateSeries {
    pub nodes: Vec<ProgramNode>,
    pub channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    pub operators: Vec<LocalOperatorProvenance>,
}

pub(crate) fn lower_with_creation<H>(
    package: &FragmentPackage,
    physical: &PhysicalNode,
    planned: &PlannedGenerateSeries,
    output_slots: &[SlotId],
    definitions: &crate::expressions::RootMap,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredGenerateSeries, crate::FragmentCreationError<H>> {
    let ids = parameter_ids(physical)?;
    let width = ids.iter().flatten().count();
    if width != planned.parameter_slots.len() || output_slots.len() != 1 {
        return Err((FragmentCompileError::Invalid("series channel schedule differs")).into());
    }
    let output_ty = &package.fragment().values()[&physical.output.columns[0]].ty;
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(width, work, creation)?;
    let mut backing = Vec::new();
    let mut cells = Vec::new();
    let mut channels = Vec::new();
    let mut nodes = Vec::new();
    let mut operators = Vec::new();
    for (column, id) in ids.into_iter().flatten().enumerate() {
        let source = crate::expressions::bound_root(package, physical.id, id)?;
        let definition = *definitions.get(&id).ok_or(FragmentCompileError::Invalid(
            "series bound definition was not lowered",
        ))?;
        work.flush()?;
        typed_layout.push_with_creation(
            &source.ty,
            crate::typed_layout::local_name_with_creation(
                "local_series_bound",
                physical.id.get(),
                column,
                work,
                creation,
            )?,
            work,
            creation,
        )?;
        work.flush()?;
        backing.push(new_empty_array(source.ty.data_type()));
        work.flush()?;
        cells.push(StaticValuesCell {
            row: 0,
            column: column as u32,
            definition,
        });
        channels.push((
            ProgramChannelSite::Layout {
                node: planned.bounds,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: column as u32,
            },
            source.ty.clone(),
        ));
        work.step()?;
    }
    work.flush()?;
    let bounds_layout =
        typed_layout.finish_with_creation(&planned.parameter_slots, work, creation)?;
    work.flush()?;
    let values = StaticValues::try_new_with_cells_for_compile(
        1,
        backing,
        cells,
        bounds_layout.clone(),
        work.control(),
    )?;
    work.flush()?;
    let label = if package
        .result()
        .is_some_and(|result| result.output.columns == physical.output.columns)
    {
        let field = &package
            .result()
            .ok_or(FragmentCompileError::Invalid("missing series result port"))?
            .fields[0];
        crate::typed_layout::copy_name_with_creation(
            field.alias.as_deref().unwrap_or(&field.name),
            work,
            creation,
        )?
    } else {
        crate::typed_layout::local_name_with_creation(
            "local_series",
            physical.id.get(),
            0,
            work,
            creation,
        )?
    };
    let mut typed_output =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(1, work, creation)?;
    typed_output.push_with_creation(output_ty, label, work, creation)?;
    let output_layout = typed_output.finish_with_creation(output_slots, work, creation)?;
    work.flush()?;
    let source = DiagnosticSourceNodeId::new(physical.id.get());
    nodes.push(ProgramNode::new_local(
        planned.bounds,
        vec![source],
        ProgramNodeKind::Values { values },
        bounds_layout,
    ));
    work.flush()?;
    nodes.push(ProgramNode::new_local(
        planned.node,
        vec![source],
        ProgramNodeKind::GenerateSeries {
            input: planned.bounds,
            parameter_slots: Arc::clone(&planned.parameter_slots),
        },
        output_layout,
    ));
    work.flush()?;
    channels.push((
        ProgramChannelSite::Layout {
            node: planned.node,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        },
        output_ty.clone(),
    ));
    let owner = LocalOperatorId::new(
        u32::try_from(planned.node.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
    );
    for (piece, id) in [planned.bounds, planned.node].into_iter().enumerate() {
        operators.push(LocalOperatorProvenance {
            id: LocalOperatorId::new(
                u32::try_from(id.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
            ),
            lowered_nodes: Box::from([id]),
            sources: Box::from([source]),
            origin: LocalOperatorOrigin::Split {
                piece: piece as u32,
            },
            cost_owner: owner,
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        });
        work.step()?;
    }
    Ok(LoweredGenerateSeries {
        nodes,
        channels,
        operators,
    })
}
