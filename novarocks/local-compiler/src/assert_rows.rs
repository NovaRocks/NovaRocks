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

//! Mandatory physical row assertions projected into the existing local owner.
//! Keys are supplied by the sole channel author, after complete source proof.

use crate::lowering::FragmentCompileError;
use novarocks_local_program::{
    AssertRowsMode, ProgramNodeId, ProgramNodeKind, RowAssertion, StaticLayout,
};
use novarocks_physical_plan::{NodeKind, PhysicalNode, RowCountAssertion, RowCountAssertionSpec};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{alloc::Layout, sync::Arc};

pub(crate) fn lower_assert_rows(
    node: &PhysicalNode,
    child: ProgramNodeId,
    layout: &StaticLayout,
    key_slots: &[SlotId],
    applicability: novarocks_local_program::ProgramAssertionApplicability,
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(node, child, layout, key_slots, applicability, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn lower_core(
    node: &PhysicalNode,
    child: ProgramNodeId,
    layout: &StaticLayout,
    key_slots: &[SlotId],
    applicability: novarocks_local_program::ProgramAssertionApplicability,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let NodeKind::AssertOneRow(spec) = &node.kind else {
        return Err(FragmentCompileError::Invalid(
            "row assertion source kind differs",
        ));
    };
    let mode = match spec {
        RowCountAssertionSpec::Global {
            subject,
            desired_rows,
            comparison,
        } => {
            let valid = !subject.is_empty()
                && key_slots.is_empty()
                && applicability == novarocks_local_program::ProgramAssertionApplicability::AllRows;
            let desired = usize::try_from(*desired_rows);
            let assertion = match comparison {
                RowCountAssertion::Eq => RowAssertion::Eq,
                RowCountAssertion::Ne => RowAssertion::Ne,
                RowCountAssertion::Lt => RowAssertion::Lt,
                RowCountAssertion::Le => RowAssertion::Le,
                RowCountAssertion::Gt => RowAssertion::Gt,
                RowCountAssertion::Ge => RowAssertion::Ge,
            };
            work.step()?;
            if !valid {
                return Err(FragmentCompileError::Invalid(
                    "global assertion source fields differ",
                ));
            }
            let desired = desired.map_err(|_| {
                FragmentCompileError::Invalid("assertion row count exceeds host range")
            })?;
            AssertRowsMode::Global {
                desired_num_rows: Some(desired),
                assertion,
                subquery_string: Some(copy_text(subject, work)?),
            }
        }
        RowCountAssertionSpec::PerKeyAtMostOne {
            keys,
            labels,
            message,
            applicability: source_applicability,
            declared_violation,
        } => {
            let same_applicability = matches!(
                (source_applicability, applicability),
                (
                    novarocks_physical_plan::AssertionApplicability::AllRows,
                    novarocks_local_program::ProgramAssertionApplicability::AllRows
                ) | (
                    novarocks_physical_plan::AssertionApplicability::WhenTrue(_),
                    novarocks_local_program::ProgramAssertionApplicability::WhenTrue(_)
                )
            );
            let valid = same_applicability
                && !keys.is_empty()
                && keys.len() == key_slots.len()
                && keys.len() == labels.len()
                && !message.is_empty();
            work.step()?;
            if !valid {
                return Err(FragmentCompileError::Invalid(
                    "keyed assertion source fields differ",
                ));
            }
            let mut copied_slots = Vec::new();
            let mut copied_labels = Vec::new();
            reserve_vec(&mut copied_slots, key_slots.len(), work)?;
            reserve_vec(&mut copied_labels, labels.len(), work)?;
            for (&slot, label) in key_slots.iter().zip(labels) {
                let valid = !label.is_empty();
                work.step()?;
                if !valid {
                    return Err(FragmentCompileError::Invalid(
                        "keyed assertion label is empty",
                    ));
                }
                copied_slots.push(slot);
                work.step()?;
                copied_labels.push(copy_text(label, work)?);
                work.step()?;
            }
            AssertRowsMode::PerKeyAtMostOne {
                applicability,
                declared_violation: *declared_violation,
                key_slots: copied_slots,
                key_labels: copied_labels,
                message_prefix: copy_text(message, work)?,
            }
        }
    };
    // StaticLayout is immutable shared backing. This clone copies handles only;
    // it preserves complete schema, slot order and known metadata absence.
    let layout = layout.clone();
    work.step()?;
    Ok((
        ProgramNodeKind::AssertNumRows { input: child, mode },
        layout,
    ))
}

pub(crate) fn reserve_vec<T>(
    target: &mut Vec<T>,
    additional: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CompileControlError> {
    Layout::array::<T>(additional).map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let reserved = target.try_reserve_exact(additional);
    // An actual allocation refusal is primary: never call control afterwards.
    reserved.map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    Ok(())
}

fn copy_text(
    source: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<str>, FragmentCompileError> {
    Layout::array::<u8>(source.len())
        .map_err(|_| FragmentCompileError::Control(CompileControlError::ResourceExhausted))?;
    let mut copied = String::new();
    work.flush()?;
    let reserved = copied.try_reserve_exact(source.len());
    reserved.map_err(|_| FragmentCompileError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    let mut start = 0;
    while start < source.len() {
        let mut end = start.saturating_add(256).min(source.len());
        while !source.is_char_boundary(end) {
            end -= 1;
            work.step()?;
        }
        work.flush()?;
        // This copies at most 256 actual UTF-8 bytes; observation accounts the
        // completed copy before another chunk can be written.
        copied.push_str(&source[start..end]);
        for _ in start..end {
            work.step()?;
        }
        work.flush()?;
        start = end;
    }
    work.flush()?;
    // String -> Arc is a library allocation/copy, honestly opaque here. These
    // representation checks do not grant formal host allocation authority.
    let copied = Arc::from(copied);
    work.flush()?;
    Ok(copied)
}
