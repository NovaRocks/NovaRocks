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

//! Lower a frozen snapshot without concatenation or SQL constant materialization.
use crate::{FragmentCompileError, FragmentCreationError};
use novarocks_local_program::{ProgramNodeKind, StaticBatchSequence, StaticLayout};
use novarocks_physical_plan::{FragmentPackage, NodeKind, PhysicalNode, SystemRelationData};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, ValueSchemaCreation};
use novarocks_types::SlotId;
use std::{alloc::Layout, sync::Arc};

pub(crate) fn lower<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    slots: &Arc<[SlotId]>,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCreationError<H>> {
    let NodeKind::SystemRelation {
        output_fields,
        data: SystemRelationData::Batches { batches, storage },
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid("system relation data is deferred").into());
    };
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(slots.len(), work, creation)?;
    for (ordinal, id) in node.output.columns.iter().enumerate() {
        let value = package
            .fragment()
            .values()
            .get(id)
            .ok_or(FragmentCompileError::Invalid(
                "system relation output value is absent",
            ))?;
        let name = if let Some(result) = package
            .result()
            .filter(|r| r.output.columns == node.output.columns)
        {
            let field = result
                .fields
                .get(ordinal)
                .ok_or(FragmentCompileError::Invalid(
                    "system relation result field is absent",
                ))?;
            field.alias.as_deref().unwrap_or(&field.name)
        } else {
            output_fields[ordinal].name()
        };
        let name = crate::typed_layout::copy_name_with_creation(name, work, creation)?;
        typed_layout.push_with_creation(&value.ty, name, work, creation)?;
    }
    let layout = typed_layout.finish_with_creation(slots, work, creation)?;
    let lifetime = creation
        .storage_lifetime()
        .retaining_observed::<FragmentCreationError<H>>(storage, |l| {
            creation
                .request_layout(l)
                .map_err(FragmentCreationError::Creator)
        })?;
    // Presentation labels may differ from the frozen snapshot labels. Only
    // RecordBatch/schema headers change; every Arrow data buffer remains shared.
    let batches = if batches
        .first()
        .is_none_or(|b| b.schema().as_ref() == layout.schema().as_ref())
    {
        Arc::clone(batches)
    } else {
        let mut rebound = Vec::new();
        crate::creation::reserve_vec(&mut rebound, batches.len(), Some(creation), work)?;
        for batch in batches.iter() {
            work.flush()?;
            creation
                .request_layout(
                    Layout::array::<arrow_array::ArrayRef>(batch.num_columns())
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                )
                .map_err(FragmentCreationError::Creator)?;
            rebound.push(
                arrow_array::RecordBatch::try_new_with_options(
                    Arc::clone(layout.schema()),
                    batch.columns().to_vec(),
                    &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )
                .map_err(|_| {
                    FragmentCompileError::Invalid(
                        "system relation batch cannot inhabit its typed layout",
                    )
                })?,
            );
            work.step()?;
        }
        creation
            .request_layout(arc_layout(
                Layout::array::<arrow_array::RecordBatch>(rebound.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            )?)
            .map_err(FragmentCreationError::Creator)?;
        Arc::from(rebound)
    };
    let batches = StaticBatchSequence::try_new_with_creator(
        batches,
        layout.clone(),
        lifetime,
        creation,
        work.control(),
    )
    .map_err(|e| match e {
        novarocks_local_program::BatchSequenceCreationFailure::Invalid(s) => {
            FragmentCreationError::Compile(FragmentCompileError::Invalid(s))
        }
        novarocks_local_program::BatchSequenceCreationFailure::Control(e) => {
            FragmentCreationError::Control(e)
        }
        novarocks_local_program::BatchSequenceCreationFailure::Creator(e) => {
            FragmentCreationError::Creator(e)
        }
        novarocks_local_program::BatchSequenceCreationFailure::Resource(e) => {
            FragmentCreationError::Resource(e)
        }
    })?;
    Ok((ProgramNodeKind::SystemRelation { batches }, layout))
}
