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

//! Pure selected-input table cursors and explicitly bounded output steps.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    CallEffectInput, FunctionBindingError, FunctionBindingResolver, FunctionBindingSelection,
    FunctionCallContract, FunctionEffectOwner, FunctionSpecializationFailure,
    KernelEvaluationControl, KernelFailure, RowDataError, ScopedRelationalEffects,
    SelectedTableInput, TableCallContract, TableOutputPage, TablePageCapacity, TableParentErrors,
};
use arrow_array::{ArrayRef, new_empty_array};
use novarocks_type_contract::{CallEffects, CompilePhase, PureCompileControl};
use std::{fmt, sync::Arc};

#[cfg(test)]
mod tests;

/// Shape grants for one already authorized output step. They are no memory
/// allowance: cursor and output backing/temporary peaks require host scopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableStepCapacity {
    pub page: TablePageCapacity,
    pub parent_errors: usize,
}
/// At most one atomic fact in each channel is needed to make progress. An
/// implementation may ask for a missing shape grant without consuming any
/// parent/output/completion/error effect. This is not an async or MEM wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableCapacityRequirements {
    pub row: bool,
    pub completion: bool,
    pub parent_error: bool,
}
impl TableCapacityRequirements {
    fn satisfied_by(self, capacity: TableStepCapacity) -> bool {
        (!self.row || capacity.page.rows > 0)
            && (!self.completion || capacity.page.completions > 0)
            && (!self.parent_error || capacity.parent_errors > 0)
    }
}

/// Owned buffers release the cursor borrow before post-call checks. Keeping a
/// page alive beside the cursor is real simultaneous backing, not a release
/// receipt. Parent ordinals refer to this exact invocation Selection.
#[derive(Debug)]
pub struct OwnedTableOutputPage {
    pub columns: Box<[ArrayRef]>,
    pub parent_ordinals: Box<[usize]>,
    pub completed_parents: Box<[usize]>,
    pub parent_errors: Box<[RowDataError]>,
    /// Explicit end, possibly accompanied by the final data/completions/errors.
    /// An empty page is never implicitly EOF or successful empty completion.
    pub eof: bool,
}
#[derive(Debug)]
pub enum TableCursorStep {
    Page(OwnedTableOutputPage),
    CapacityRequired(TableCapacityRequirements),
}

pub trait PreparedTableKernel: Send + Sync + fmt::Debug + 'static {
    fn contract(&self) -> &Arc<TableCallContract>;
    /// O(1) complete boxed cursor body/owned heap bound, including bounded
    /// error-exit growth. Borrowed input and returned page have separate owners.
    fn cursor_retained_upper_bound(&self, selected_parents: usize) -> Result<usize, KernelFailure>;
    /// Bind only actually selected input. No ungranted expansion, successful
    /// completion or parent data failure is consumed here. Data failures are
    /// classified through next's independent parent-error grant/channel.
    fn begin_selected<'input>(
        self: Arc<Self>,
        input: SelectedTableInput<'input, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn TableKernelCursor + 'input>, KernelFailure>;
}
/// The implementation owns only exact relation expansion. Host owns outer
/// pass-through, LEFT OUTER synthesis, ordering and the cross-page journal.
/// Production kernels reserve known builder/cursor allocations and peaks in
/// formal host MEM scopes; neither grants nor post-call checks fund them.
pub trait TableKernelCursor: Send {
    fn next(
        &mut self,
        capacity: TableStepCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TableCursorStep, KernelFailure>;
    /// Normal end only. Cancellation/withdrawal destroys the cursor through
    /// ordinary Drop; a control failure cannot skip initialized state Drop.
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure>;
    fn retained_bytes(&self) -> usize;
}
pub trait PureTableImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn prepare_table(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<TableCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedTableKernel>, KernelFailure>;
    fn prepare_table_checked(
        &self,
        _input: CallEffectInput<'_>,
        _checked: &crate::CheckedCallEffectInput<'_>,
        _contract: Arc<TableCallContract>,
        _control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedTableKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        Err(crate::CheckedOwnerFailure::UnconvertedOwner)
    }
}
#[derive(Debug)]
pub struct TableSpecialization {
    prepared: Arc<dyn PreparedTableKernel>,
    effects: ScopedRelationalEffects,
}
impl TableSpecialization {
    pub fn prepared(&self) -> &Arc<dyn PreparedTableKernel> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedRelationalEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<dyn PreparedTableKernel> {
        self.prepared
    }
}
pub fn specialize_table<O: PureTableImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedRelationalEffects,
    control: &dyn PureCompileControl,
) -> Result<TableSpecialization, FunctionSpecializationFailure> {
    specialize_table_once(owner, input, selected, None, arguments, control)
}
pub fn specialize_frozen_table<O: PureTableImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedRelationalEffects,
    control: &dyn PureCompileControl,
) -> Result<TableSpecialization, FunctionSpecializationFailure> {
    specialize_table_once(owner, input, selected, Some(frozen), arguments, control)
}
fn specialize_table_once<O: PureTableImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedRelationalEffects,
    control: &dyn PureCompileControl,
) -> Result<TableSpecialization, FunctionSpecializationFailure> {
    specialize_table_once_core(
        owner,
        input,
        selected,
        frozen,
        arguments,
        control,
        |arguments| {
            crate::specialization::refine_once_for_specialization(
                owner, input, frozen, arguments, control,
            )
        },
        |contract| {
            owner
                .prepare_table(input, contract, control)
                .map_err(FunctionSpecializationFailure::from)
        },
    )
}

pub(crate) fn specialize_table_once_checked<O: PureTableImplementation + ?Sized>(
    owner: &O,
    checked: &crate::CheckedCallEffectInput<'_>,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedRelationalEffects,
    control: &dyn PureCompileControl,
) -> Result<TableSpecialization, crate::CheckedOwnerFailure<FunctionSpecializationFailure>> {
    specialize_table_once_core(
        owner,
        input,
        selected,
        frozen,
        arguments,
        control,
        |arguments| {
            crate::specialization::refine_once_checked(
                owner, checked, input, frozen, arguments, control,
            )
        },
        |contract| {
            owner
                .prepare_table_checked(input, checked, contract, control)
                .map_err(|error| match error {
                    crate::CheckedOwnerFailure::Original(error) => {
                        crate::CheckedOwnerFailure::Original(FunctionSpecializationFailure::from(
                            error,
                        ))
                    }
                    crate::CheckedOwnerFailure::UnconvertedOwner => {
                        crate::CheckedOwnerFailure::UnconvertedOwner
                    }
                })
        },
    )
}

fn specialize_table_once_core<
    'a,
    O: PureTableImplementation + ?Sized,
    E: From<FunctionSpecializationFailure>,
>(
    _owner: &O,
    input: CallEffectInput<'a>,
    selected: Arc<FunctionBindingSelection>,
    _frozen: Option<&CallEffects>,
    arguments: ScopedRelationalEffects,
    control: &dyn PureCompileControl,
    refine: impl FnOnce(
        ScopedRelationalEffects,
    ) -> Result<(crate::RefinedCallEffects<'a>, ScopedRelationalEffects), E>,
    prepare: impl FnOnce(Arc<TableCallContract>) -> Result<Arc<dyn PreparedTableKernel>, E>,
) -> Result<TableSpecialization, E> {
    let (receipt, effects) = refine(arguments)?;
    let call = Arc::new(
        FunctionCallContract::from_refined(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::from)?,
    );
    let contract = Arc::new(
        TableCallContract::try_new(call, control).map_err(FunctionSpecializationFailure::from)?,
    );
    let prepared = prepare(contract.clone())?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "table preparation replaced its exact immutable contract",
        ))
        .into());
    }
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(TableSpecialization { prepared, effects })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorStatus {
    Open,
    End,
    Failed,
    Finished,
}

/// One exact selected invocation. Returned steps have been checked locally;
/// they do not prove cross-page identity, parent terminal coverage or ordering.
/// Host must reject completion replay, output after completion/error, missing
/// parents at EOF and invalid LEFT OUTER assembly before exposing results.
pub struct TableEvaluationCursor<'input> {
    // Declaration order preserves prepared owner through typed cursor Drop.
    cursor: Option<Box<dyn TableKernelCursor + 'input>>,
    prepared: Arc<dyn PreparedTableKernel>,
    contract: Arc<TableCallContract>,
    input: SelectedTableInput<'input, 'input>,
    retained_upper_bound: usize,
    pending_capacity: Option<TableCapacityRequirements>,
    status: CursorStatus,
}
impl<'input> TableEvaluationCursor<'input> {
    pub fn begin(
        prepared: Arc<dyn PreparedTableKernel>,
        input: SelectedTableInput<'input, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        let contract = prepared.contract().clone();
        if !std::ptr::eq(contract.as_ref(), input.contract()) {
            return Err(invalid("table input differs from exact prepared contract"));
        }
        let (retained_upper_bound, cursor) = if input.selection().is_empty() {
            (0, None)
        } else {
            let bound = prepared.cursor_retained_upper_bound(input.selection().len())?;
            size_of::<Self>()
                .checked_add(bound)
                .ok_or(KernelFailure::ResourceExhausted)?;
            (
                bound,
                Some(prepared.clone().begin_selected(input, control)?),
            )
        };
        let cursor = Self {
            cursor,
            prepared,
            contract,
            input,
            retained_upper_bound,
            pending_capacity: None,
            status: CursorStatus::Open,
        };
        cursor.validate_retained()?;
        cursor.validate_metadata()?;
        control.checkpoint(0)?;
        Ok(cursor)
    }
    pub fn retained_upper_bound(&self) -> usize {
        size_of::<Self>() + self.retained_upper_bound
    }
    pub fn retained_bytes(&self) -> Result<usize, KernelFailure> {
        self.validate_retained()?;
        Ok(size_of::<Self>()
            + self
                .cursor
                .as_ref()
                .map_or(0, |cursor| cursor.retained_bytes()))
    }
    fn validate_retained(&self) -> Result<(), KernelFailure> {
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.retained_bytes() > self.retained_upper_bound)
        {
            Err(internal("table cursor exceeded its frozen retained bound"))
        } else {
            Ok(())
        }
    }
    fn validate_metadata(&self) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(self.prepared.contract(), &self.contract)
            || (self.cursor.is_some()
                && self
                    .prepared
                    .cursor_retained_upper_bound(self.input.selection().len())?
                    != self.retained_upper_bound)
        {
            Err(internal(
                "table implementation changed its immutable metadata",
            ))
        } else {
            Ok(())
        }
    }
    fn validate_page(
        &self,
        page: &OwnedTableOutputPage,
        capacity: TableStepCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        TableOutputPage::try_new(
            &self.contract,
            self.input.selection(),
            &page.columns,
            &page.parent_ordinals,
            &page.completed_parents,
            capacity.page,
            control,
        )?;
        TableParentErrors::try_new(
            self.input,
            &page.parent_errors,
            capacity.parent_errors,
            control,
        )?;
        // Both lists are individually checked ordered/unique. A failed parent
        // cannot simultaneously assert successful completion, even with no rows.
        let mut errors = page.parent_errors.iter().peekable();
        let mut work = EvaluationCheckpoints::new(control);
        for completed in &page.completed_parents {
            while errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() < *completed)
            {
                errors.next();
                work.step()?;
            }
            if errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() == *completed)
            {
                return Err(internal(
                    "table failed parent also claimed successful completion",
                ));
            }
            work.step()?;
        }
        work.finish()?;
        if !page.eof
            && page.parent_ordinals.is_empty()
            && page.completed_parents.is_empty()
            && page.parent_errors.is_empty()
        {
            return Err(internal(
                "table cursor returned an empty nonterminal step without progress",
            ));
        }
        Ok(())
    }
    pub fn next(
        &mut self,
        capacity: TableStepCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TableCursorStep, KernelFailure> {
        if self.status != CursorStatus::Open {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            if let Some(required) = self.pending_capacity {
                if !required.satisfied_by(capacity) {
                    return Ok(TableCursorStep::CapacityRequired(required));
                }
                self.pending_capacity = None;
            }
            let step = match &mut self.cursor {
                Some(cursor) => cursor.next(capacity, control)?,
                None => {
                    let mut columns = Vec::new();
                    let mut work = EvaluationCheckpoints::new(control);
                    for value in self.contract.result_types() {
                        columns.push(new_empty_array(value.data_type()));
                        work.step()?;
                    }
                    work.finish()?;
                    TableCursorStep::Page(OwnedTableOutputPage {
                        columns: columns.into_boxed_slice(),
                        parent_ordinals: Box::default(),
                        completed_parents: Box::default(),
                        parent_errors: Box::default(),
                        eof: true,
                    })
                }
            };
            match &step {
                TableCursorStep::Page(page) => {
                    self.validate_page(page, capacity, control)?;
                    if page.eof {
                        self.status = CursorStatus::End;
                    }
                }
                TableCursorStep::CapacityRequired(required) => {
                    if required.parent_error
                        && self.contract.call().effects().own_row_error
                            != novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
                    {
                        return Err(internal(
                            "table call declared no parent errors but requested their capacity",
                        ));
                    }
                    if (!required.row && !required.completion && !required.parent_error)
                        || required.satisfied_by(capacity)
                    {
                        return Err(internal("table cursor requested no missing shape capacity"));
                    }
                    self.pending_capacity = Some(*required);
                }
            }
            control.checkpoint(0)?;
            Ok(step)
        })();
        // Only successful work starts fallible post-validation. The cursor/
        // partition still owns its real finish operation and eventual Drop.
        let result = result.and_then(|value| {
            self.validate_retained()?;
            self.validate_metadata()?;
            Ok(value)
        });
        if result.is_err() {
            self.status = CursorStatus::Failed;
        }
        result
    }
    pub fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if matches!(self.status, CursorStatus::Failed | CursorStatus::Finished) {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = (|| {
            control.checkpoint(0)?;
            if self.status != CursorStatus::End {
                return Err(invalid("table cursor normal finish requires explicit EOF"));
            }
            self.status = CursorStatus::Finished;
            self.validate_metadata()?;
            self.validate_retained()?;
            if let Some(cursor) = &mut self.cursor {
                cursor.finish(control)?;
            }
            control.checkpoint(0)
        })();
        // Only successful work starts fallible post-validation. The cursor/
        // partition still owns its real finish operation and eventual Drop.
        let result = result.and_then(|value| {
            self.validate_retained()?;
            self.validate_metadata()?;
            Ok(value)
        });
        if result.is_err() {
            self.status = CursorStatus::Failed;
        }
        result
    }
}
