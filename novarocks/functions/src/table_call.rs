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

//! Exact table calls, selected arguments and bounded borrowed output pages.
//! The operator owns outer pass-through, LEFT OUTER assembly and invocation
//! progress; the implementation produces only its exact relation columns.

use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, validate_argument_observed};
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionCallContract, FunctionResultType,
    KernelEvaluationControl, KernelFailure, RowDataError, Selection,
};
use arrow_array::ArrayRef;
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompilePhase, FunctionKind, FunctionValueType,
    PureCompileControl,
};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableCallContract(Arc<FunctionCallContract>);
impl TableCallContract {
    pub fn try_new(
        call: Arc<FunctionCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        if call.kind() != FunctionKind::Table
            || call.effects().argument_control != ArgumentControl::Table
            || !matches!(&call.selected().result_type, FunctionResultType::Relation(results) if !results.is_empty())
            || call.logical_argument_count() != call.selected().argument_types.len()
        {
            return Err(invalid(
                "table preparation requires its exact relation/control channels",
            ));
        }
        for channel in &call.selected().argument_types {
            if !matches!(channel, FunctionArgumentType::Value(_)) {
                return Err(invalid("table argument channel cannot be a lambda"));
            }
            work.step().map_err(compile_failure)?;
        }
        work.finish().map_err(compile_failure)?;
        Ok(Self(call))
    }
    pub fn call(&self) -> &Arc<FunctionCallContract> {
        &self.0
    }
    pub fn argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        self.0
            .selected()
            .argument_types
            .iter()
            .map(|argument| match argument {
                FunctionArgumentType::Value(value) => value,
                FunctionArgumentType::Lambda { .. }
                | FunctionArgumentType::CandidateGroup { .. } => {
                    unreachable!("checked table channels")
                }
            })
    }
    pub fn result_types(&self) -> &[FunctionValueType] {
        match &self.0.selected().result_type {
            FunctionResultType::Relation(results) => results,
            FunctionResultType::Scalar(_) => unreachable!("checked relation result"),
        }
    }
}

/// Borrowed inputs of one table invocation. Matching row counts do not prove
/// source-frame identity; the host supplies arguments from this exact frame.
/// An empty Selection does not authorize invoking or advancing the instance.
#[derive(Clone, Copy, Debug)]
pub struct SelectedTableInput<'call, 'a> {
    contract: &'call TableCallContract,
    selection: Selection<'a>,
    arguments: &'a [EvaluatedArgument<'a>],
}
impl<'call, 'a> SelectedTableInput<'call, 'a> {
    pub fn try_new(
        contract: &'call TableCallContract,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if arguments.len() != contract.argument_types().len() {
            return Err(invalid(
                "table input differs from its selected argument channels",
            ));
        }
        for (argument, value_type) in arguments.iter().zip(contract.argument_types()) {
            validate_argument_observed(*argument, selection, value_type, control)?;
        }
        Ok(Self {
            contract,
            selection,
            arguments,
        })
    }
    pub const fn contract(self) -> &'call TableCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'a> {
        self.selection
    }
    pub const fn arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.arguments
    }
}

/// One borrowed page produced by this invocation. Parent ordinals address the
/// invocation Selection, not raw batch rows. Repetition is legal. Completion
/// is explicit even for a parent with no output; an empty page alone never
/// proves EOF and must not cause LEFT OUTER synthesis.
///
/// The host owns exact invocation identity and checks progress across pages:
/// a completed parent cannot emit or complete again. This local page checker
/// proves neither cross-page progress nor ownership of an equal-sized frame.
/// The host also preserves the program's outer pass-through ordering, including
/// LEFT OUTER rows for successfully completed empty parents. A legal mapping
/// does not authorize emitting assembled rows in arbitrary page arrival order.
/// Capacities are actual host grants, checked before exposing the page. They
/// do not mint a memory allowance or protect allocations performed beforehand;
/// implementations must reserve through the formal host MEM scope before
/// allocating builders/cursors. Byte/peak bounds remain that scope's duty.
#[derive(Clone, Copy, Debug)]
pub struct TableOutputPage<'call, 'a> {
    contract: &'call TableCallContract,
    parents: Selection<'a>,
    columns: &'a [ArrayRef],
    parent_ordinals: &'a [usize],
    completed_parents: &'a [usize],
}
impl<'call, 'a> TableOutputPage<'call, 'a> {
    pub fn try_new(
        contract: &'call TableCallContract,
        parents: Selection<'a>,
        columns: &'a [ArrayRef],
        parent_ordinals: &'a [usize],
        completed_parents: &'a [usize],
        capacities: TablePageCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        let observation = crate::kernel_control::KernelControlObservation::new(control);
        let result = Self::try_new_observed(
            contract,
            parents,
            columns,
            parent_ordinals,
            completed_parents,
            capacities,
            &observation,
        );
        observation.finish(result)
    }
    fn try_new_observed(
        contract: &'call TableCallContract,
        parents: Selection<'a>,
        columns: &'a [ArrayRef],
        parent_ordinals: &'a [usize],
        completed_parents: &'a [usize],
        capacities: TablePageCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if parent_ordinals.len() > capacities.rows
            || completed_parents.len() > capacities.completions
        {
            return Err(internal("table output page exceeds its host grant"));
        }
        if columns.len() != contract.result_types().len() {
            return Err(internal(
                "table output columns differ from its exact relation",
            ));
        }
        let mut work = EvaluationCheckpoints::new(control);
        for parent in parent_ordinals {
            if *parent >= parents.len() {
                return Err(internal("table output parent is outside the invocation"));
            }
            work.step()?;
        }
        let mut previous = None;
        for parent in completed_parents {
            if *parent >= parents.len() || previous.is_some_and(|previous| previous >= *parent) {
                return Err(internal(
                    "table completed parents are not unique ordered invocation ordinals",
                ));
            }
            previous = Some(*parent);
            work.step()?;
        }
        work.finish()?;
        let output_rows = Selection::all(parent_ordinals.len());
        for (column, value_type) in columns.iter().zip(contract.result_types()) {
            validate_argument_observed(
                EvaluatedArgument::Column(column),
                output_rows,
                value_type,
                control,
            )
            .map_err(|failure| match failure {
                KernelFailure::InvalidProgram(diagnostic) => KernelFailure::Internal(diagnostic),
                failure => failure,
            })?;
        }
        Ok(Self {
            contract,
            parents,
            columns,
            parent_ordinals,
            completed_parents,
        })
    }
    pub const fn contract(self) -> &'call TableCallContract {
        self.contract
    }
    pub const fn parents(self) -> Selection<'a> {
        self.parents
    }
    pub const fn columns(self) -> &'a [ArrayRef] {
        self.columns
    }
    pub const fn parent_ordinals(self) -> &'a [usize] {
        self.parent_ordinals
    }
    pub const fn completed_parents(self) -> &'a [usize] {
        self.completed_parents
    }
    pub fn row_count(self) -> usize {
        self.parent_ordinals.len()
    }
    pub fn batch_parent(self, output_ordinal: usize) -> Option<usize> {
        self.parent_ordinals
            .get(output_ordinal)
            .and_then(|parent| self.parents.row(*parent))
    }
}

/// Per-page capacities supplied by the host for an already authorized output
/// buffer. Zero is useful for a completion-only or output-only grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TablePageCapacity {
    pub rows: usize,
    pub completions: usize,
}

/// Classified row errors address selected input parents, independently of
/// produced rows. A failing parent can have no output row at all. This channel
/// does not mean successful empty completion or authorize LEFT OUTER synthesis;
/// the operator applies the exact call policy and required-error responsibility.
/// Resource/control/internal failures remain the outer KernelFailure channel.
#[derive(Clone, Copy, Debug)]
pub struct TableParentErrors<'call, 'a> {
    input: SelectedTableInput<'call, 'a>,
    errors: &'a [RowDataError],
}
impl<'call, 'a> TableParentErrors<'call, 'a> {
    pub fn try_new(
        input: SelectedTableInput<'call, 'a>,
        errors: &'a [RowDataError],
        capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if errors.len() > capacity {
            return Err(internal("table parent errors exceed their host grant"));
        }
        if !errors.is_empty()
            && input.contract.call().effects().own_row_error
                != novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
        {
            return Err(internal("table call declared no row errors"));
        }
        let mut work = EvaluationCheckpoints::new(control);
        let mut previous = None;
        for error in errors {
            let ordinal = error.selected_ordinal();
            if ordinal >= input.selection.len()
                || previous.is_some_and(|previous| previous >= ordinal)
            {
                return Err(internal(
                    "table row errors are not unique ordered invocation ordinals",
                ));
            }
            previous = Some(ordinal);
            work.step()?;
        }
        work.finish()?;
        Ok(Self { input, errors })
    }
    pub const fn input(self) -> SelectedTableInput<'call, 'a> {
        self.input
    }
    pub const fn errors(self) -> &'a [RowDataError] {
        self.errors
    }
    pub fn batch_parent(self, error_ordinal: usize) -> Option<usize> {
        self.errors
            .get(error_ordinal)
            .and_then(|error| self.input.selection.row(error.selected_ordinal()))
    }
}

#[cfg(test)]
mod tests;
