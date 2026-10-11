// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Iterative actual-use continuations. Owned row domains never borrow a moving
//! frame. Kernel calls temporarily borrow one stable domain, then erase that
//! borrow into owned values before resuming the parent continuation.
use super::boolean_region::BooleanRows;
use super::*;
use arrow::array::{Array, BooleanArray};
use novarocks_local_program::ProgramNodeId;
use novarocks_type_contract::EvaluationDemand;

pub(super) fn supports_result(ty: &DataType) -> bool {
    novarocks_functions::control_values::supports_indexed_result(ty)
}

enum OwnedValue {
    Constant(novarocks_functions::ConstantValue),
    Column(ArrayRef),
    Selected(ArrayRef, Box<[RowDataError]>),
}
impl OwnedValue {
    fn into_value<'a>(
        self,
        selection: Selection<'a>,
        work: &mut Work<'_, '_>,
    ) -> Result<Value<'a>, KernelFailure> {
        Ok(match self {
            Self::Constant(value) => Value::Constant(value),
            Self::Column(value) => Value::Column(value),
            Self::Selected(array, errors) => {
                let ty = array.data_type().clone();
                Value::Selected(SelectedValues::try_new_observed(
                    selection,
                    &ty,
                    array,
                    errors,
                    || work.step(),
                )?)
            }
        })
    }
    fn from_selected(output: SelectedValues<'_>) -> Self {
        let (_, array, errors) = output.into_parts();
        Self::Selected(array, errors)
    }
}
struct Child {
    // Compact parent positions, distinct from original batch rows.
    ordinals: Vec<usize>,
    value: OwnedValue,
}
struct Frame {
    occurrence: ProgramUseRef,
    activation: novarocks_functions::ScalarInvocationActivation,
    rows: Vec<usize>,
    parent_ordinals: Vec<usize>,
    next: usize,
    children: Vec<Child>,
    routes: Vec<Option<bool>>,
    remaining: Vec<usize>,
    matched: Vec<usize>,
    operand: Option<ArrayRef>,
    choices: Vec<Option<(usize, usize)>>,
    errors: BTreeMap<usize, RowDataError>,
    boolean: Option<BooleanRows>,
    temporal: Option<TemporalValues>,
    membership: Option<novarocks_functions::native_inlist::InRows>,
}
impl Frame {
    fn new(
        occurrence: ProgramUseRef,
        shape: ControlShape,
        result_type: &DataType,
        rows: Vec<usize>,
        activation: novarocks_functions::ScalarInvocationActivation,
        parent_ordinals: Vec<usize>,
        work: &mut Work<'_, '_>,
    ) -> Result<Self, KernelFailure> {
        // A demand-zero invocation does not construct or interleave a result
        // before its original arity Data. Retain the actual domain storage,
        // but do not invent an output-copy admission for nonexistent values.
        if shape != ControlShape::NoArguments && supports_result(result_type) {
            novarocks_functions::selected_copy::guarded_interleave_extent(result_type, rows.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.step()?;
        }
        let tracks_remaining = matches!(
            shape,
            ControlShape::Coalesce
                | ControlShape::Conjunction
                | ControlShape::Disjunction
                | ControlShape::Case { .. }
        );
        let tracks_choices = matches!(
            shape,
            ControlShape::If | ControlShape::Coalesce | ControlShape::Case { .. }
        );
        let mut remaining = Vec::new();
        let mut choices = Vec::new();
        if tracks_remaining {
            remaining.reserve(rows.len());
        }
        if tracks_choices {
            choices.reserve(rows.len());
        }
        if tracks_remaining || tracks_choices {
            for ordinal in 0..rows.len() {
                if tracks_remaining {
                    remaining.push(ordinal);
                }
                if tracks_choices {
                    choices.push(None);
                }
                work.step()?;
            }
        }
        let temporal = if matches!(shape, ControlShape::TemporalSource(_)) {
            Some(TemporalValues::new(rows.len(), work)?)
        } else {
            None
        };
        let boolean = if matches!(shape, ControlShape::Between { .. }) {
            Some(BooleanRows::new(rows.len(), work)?)
        } else {
            None
        };
        Ok(Self {
            occurrence,
            activation,
            rows,
            parent_ordinals,
            next: 0,
            children: Vec::new(),
            routes: Vec::new(),
            remaining,
            matched: Vec::new(),
            operand: None,
            choices,
            errors: BTreeMap::new(),
            boolean,
            temporal,
            membership: None,
        })
    }
    fn next_ordinals(
        &mut self,
        shape: ControlShape,
        arity: usize,
        next_is_pure: bool,
        batch_rows: usize,
        work: &mut Work<'_, '_>,
    ) -> Result<Option<Vec<usize>>, KernelFailure> {
        if matches!(shape, ControlShape::Conjunction | ControlShape::Disjunction)
            && self.boolean.is_none()
        {
            self.boolean = Some(BooleanRows::new(self.rows.len(), work)?);
        }
        if matches!(shape, ControlShape::Conjunction | ControlShape::Disjunction) && !next_is_pure {
            self.boolean
                .as_mut()
                .ok_or_else(|| internal("missing Boolean continuation"))?
                .boundary(&mut self.errors, &mut self.remaining, work)?;
        }
        if self.rows.is_empty()
            && self.activation == novarocks_functions::ScalarInvocationActivation::Activated
            && shape == ControlShape::If
            && self.next == 1
        {
            self.next = 2;
        }
        if self.next >= arity
            || (matches!(
                shape,
                ControlShape::Coalesce | ControlShape::Conjunction | ControlShape::Disjunction
            ) && self.remaining.is_empty())
        {
            return Ok(None);
        }
        let mut ordinals = Vec::new();
        match shape {
            ControlShape::Membership { .. } => {
                if self.next > 0 && self.rows.is_empty() {
                    return Ok(None);
                }
                for ordinal in 0..self.rows.len() {
                    if !self.errors.contains_key(&ordinal) {
                        ordinals.push(ordinal);
                    }
                    work.step()?;
                }
                if !self.rows.is_empty() && ordinals.is_empty() {
                    return Ok(None);
                }
            }
            ControlShape::Between { .. } => {
                // Original Value AND/OR demands the second comparison even
                // after a deciding lower value. Only earlier data errors are
                // terminal in this exact current invocation domain.
                for ordinal in 0..self.rows.len() {
                    if !self.errors.contains_key(&ordinal) {
                        ordinals.push(ordinal);
                    }
                    work.step()?;
                }
                if !self.rows.is_empty() && ordinals.is_empty() {
                    work.flush()?;
                    self.children.clear();
                    work.flush()?;
                    return Ok(None);
                }
            }
            ControlShape::TemporalSource(source_shape) => {
                if source_shape == novarocks_type_contract::TemporalSourceShape::SecondsCastOther
                    && self.next == 2
                {
                    let state = self
                        .temporal
                        .as_ref()
                        .ok_or_else(|| internal("missing temporal source continuation"))?;
                    let invocation_domain =
                        Selection::try_sparse_observed(batch_rows, &self.rows, || work.step())?;
                    if !state.needs_fallback(&self.errors, invocation_domain, work)? {
                        return Ok(None);
                    }
                }
                // Later source phases demand all successful invocation rows.
                // A NULL predicate guards the invocation, never just its NULL rows.
                for ordinal in 0..self.rows.len() {
                    if !self.errors.contains_key(&ordinal) {
                        ordinals.push(ordinal);
                    }
                    work.step()?;
                }
                if ordinals.is_empty() {
                    return Ok(None);
                }
            }
            ControlShape::Case { simple, arms, .. } => {
                let offset = usize::from(simple);
                let then = self.next >= offset
                    && self.next < offset + arms as usize * 2
                    && !(self.next - offset).is_multiple_of(2);
                let domain = if then { &self.matched } else { &self.remaining };
                if domain.is_empty() && self.remaining.is_empty() {
                    return Ok(None);
                }
                for &ordinal in domain {
                    ordinals.push(ordinal);
                    work.step()?;
                }
            }
            ControlShape::If if self.next != 0 => {
                let desired = self.next == 1;
                for (ordinal, &route) in self.routes.iter().enumerate() {
                    if route == Some(desired) {
                        ordinals.push(ordinal);
                    }
                    work.step()?;
                }
            }
            ControlShape::Coalesce | ControlShape::Conjunction | ControlShape::Disjunction => {
                for &ordinal in &self.remaining {
                    ordinals.push(ordinal);
                    work.step()?;
                }
            }
            _ => {
                for ordinal in 0..self.rows.len() {
                    ordinals.push(ordinal);
                    work.step()?;
                }
            }
        }
        Ok(Some(ordinals))
    }
    fn attach_temporal(
        &mut self,
        child: Child,
        source_shape: novarocks_type_contract::TemporalSourceShape,
        batch_rows: usize,
        work: &mut Work<'_, '_>,
    ) -> Result<(), KernelFailure> {
        let mut rows = Vec::with_capacity(child.ordinals.len());
        for &ordinal in &child.ordinals {
            rows.push(self.rows[ordinal]);
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
        let value = child.value.into_value(selection, work)?;
        let ty = value.argument().array().data_type().clone();
        let output = value.materialize(selection, &ty, work)?;
        let mut good = Vec::with_capacity(child.ordinals.len());
        let mut ordinals = Vec::with_capacity(child.ordinals.len());
        let mut errors = output.errors().iter().peekable();
        for (local, &parent) in child.ordinals.iter().enumerate() {
            work.step()?;
            if errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() == local)
            {
                let error = errors
                    .next()
                    .ok_or_else(|| internal("missing temporal child error"))?;
                self.errors
                    .entry(parent)
                    .or_insert_with(|| error.with_selected_ordinal(parent));
            } else {
                good.push(Some(local as u64));
                ordinals.push(parent);
            }
        }
        if !ordinals.is_empty() {
            let array = if good.len() == output.values().len() {
                Arc::clone(output.values())
            } else {
                gather(output.values(), &good, work)?
            };
            let mut good_rows = Vec::with_capacity(ordinals.len());
            for &ordinal in &ordinals {
                good_rows.push(self.rows[ordinal]);
                work.step()?;
            }
            let good_domain =
                Selection::try_sparse_observed(batch_rows, &good_rows, || work.step())?;
            let data_error = self
                .temporal
                .as_mut()
                .ok_or_else(|| internal("missing temporal continuation"))?
                .consume(
                    source_shape,
                    self.next - 1,
                    good_domain,
                    &ordinals,
                    array,
                    work,
                )?;
            if let Some(message) = data_error {
                #[cfg(test)]
                super::guarded_tests::record_temporal_invocation_data(
                    source_shape,
                    self.next - 1,
                    &message,
                    &self.rows,
                    &ordinals,
                    &self.errors,
                );
                for &parent in &ordinals {
                    work.step()?;
                    self.errors
                        .entry(parent)
                        .or_insert_with(|| RowDataError::new(parent, &message));
                }
            }
        }
        work.flush()?;
        drop(output);
        work.flush()?;
        Ok(())
    }
    fn attach_between(
        &mut self,
        child: Child,
        program: &novarocks_local_program::LocalProgram,
        negated: bool,
        batch_rows: usize,
        work: &mut Work<'_, '_>,
    ) -> Result<(), KernelFailure> {
        let ordinal = self
            .next
            .checked_sub(1)
            .ok_or_else(|| internal("BETWEEN child completed before its continuation"))?;
        if ordinal >= 4 {
            return Err(invalid("BETWEEN has exactly four source uses"));
        }
        let mut child_rows = Vec::with_capacity(child.ordinals.len());
        for &parent in &child.ordinals {
            child_rows.push(self.rows[parent]);
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &child_rows, || work.step())?;
        let value = child.value.into_value(selection, work)?;
        let ty = value.argument().array().data_type().clone();
        let output = value.materialize(selection, &ty, work)?;
        for error in output.errors() {
            let parent = *child
                .ordinals
                .get(error.selected_ordinal())
                .ok_or_else(|| invalid("BETWEEN child error has no source ordinal"))?;
            self.errors
                .entry(parent)
                .or_insert_with(|| error.with_selected_ordinal(parent));
            work.step()?;
        }
        self.children.push(Child {
            ordinals: child.ordinals,
            value: OwnedValue::from_selected(output),
        });
        if ordinal.is_multiple_of(2) {
            return Ok(());
        }
        if self.children.len() != 2 {
            return Err(invalid(
                "BETWEEN comparison requires its ordered operand and bound",
            ));
        }
        // Finish the comparison before asking for the next use occurrence.
        // Source addresses remain separate even when both definitions coincide.
        let mut pair = std::mem::take(&mut self.children).into_iter();
        let left = pair
            .next()
            .ok_or_else(|| internal("missing BETWEEN operand"))?;
        let right = pair
            .next()
            .ok_or_else(|| internal("missing BETWEEN bound"))?;
        let mut parent_ordinals = Vec::with_capacity(right.ordinals.len());
        let mut rows = Vec::with_capacity(right.ordinals.len());
        for &parent in &right.ordinals {
            if !self.errors.contains_key(&parent) {
                parent_ordinals.push(parent);
                rows.push(self.rows[parent]);
            }
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
        let left = between_comparison_operand(
            left,
            &parent_ordinals,
            &self.rows,
            selection,
            batch_rows,
            work,
        )?;
        let right = between_comparison_operand(
            right,
            &parent_ordinals,
            &self.rows,
            selection,
            batch_rows,
            work,
        )?;
        let site = if ordinal == 1 {
            novarocks_local_program::ProgramComparisonSite::BetweenLower(self.occurrence)
        } else {
            novarocks_local_program::ProgramComparisonSite::BetweenUpper(self.occurrence)
        };
        let recipe = program
            .comparison_recipe(site)
            .ok_or_else(|| invalid("missing exact BETWEEN bound comparison recipe"))?;
        let output = evaluate_comparison(
            ComparisonRecipe::Ordinary(recipe),
            &left,
            &right,
            selection,
            work,
        )?;
        let connective = novarocks_type_contract::NativeBetweenPlan::new(negated).connective();
        let _ = self
            .boolean
            .as_mut()
            .ok_or_else(|| internal("missing BETWEEN Boolean continuation"))?
            .consume(
                &output,
                &parent_ordinals,
                connective,
                EvaluationDemand::Value,
                true,
                &mut self.errors,
                work,
            )?;
        // The existing Boolean author owns 3VL. Its dominance result is not an
        // authorization to suppress original upper/volatile source invocations.
        work.flush()?;
        drop(output);
        drop(left);
        drop(right);
        work.flush()?;
        Ok(())
    }
    fn attach_membership(
        &mut self,
        child: Child,
        program: &novarocks_local_program::LocalProgram,
        batch_rows: usize,
        work: &mut Work<'_, '_>,
    ) -> Result<(), KernelFailure> {
        use novarocks_functions::native_inlist::{InRows, signed_equality_observed};
        let ordinal = self
            .next
            .checked_sub(1)
            .ok_or_else(|| internal("IN completed before its source phase"))?;
        let recipe = program
            .native_inlist_recipe(self.occurrence)
            .ok_or_else(|| invalid("missing exact IN recipe"))?;
        let mut rows = Vec::with_capacity(child.ordinals.len());
        for &parent in &child.ordinals {
            rows.push(self.rows[parent]);
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
        let value = child.value.into_value(selection, work)?;
        let ty = value.argument().array().data_type().clone();
        let output = value.materialize(selection, &ty, work)?;
        let expected = if ordinal == 0 {
            recipe.source_type()
        } else {
            recipe
                .candidate_types()
                .get(ordinal - 1)
                .ok_or_else(|| invalid("IN candidate phase exceeds its recipe"))?
        };
        work.flush()?;
        validate_membership_phase(&output, selection, expected, work)?;
        for error in output.errors() {
            let parent = *child
                .ordinals
                .get(error.selected_ordinal())
                .ok_or_else(|| invalid("IN child error has no source ordinal"))?;
            self.errors
                .entry(parent)
                .or_insert_with(|| error.with_selected_ordinal(parent));
            work.step()?;
        }
        if ordinal == 0 {
            self.membership = Some(in_observed(InRows::begin_observed(
                output.values(),
                &mut |event| in_event(event, work),
            ))?);
            self.children.push(Child {
                ordinals: child.ordinals,
                value: OwnedValue::from_selected(output),
            });
            return Ok(());
        }
        if self.children.len() != 1 {
            return Err(invalid("IN lost its original source phase"));
        }
        let right = Child {
            ordinals: child.ordinals,
            value: OwnedValue::from_selected(output),
        };
        let mut parents = Vec::with_capacity(right.ordinals.len());
        let mut rows = Vec::with_capacity(right.ordinals.len());
        for &parent in &right.ordinals {
            if !self.errors.contains_key(&parent) {
                parents.push(parent);
                rows.push(self.rows[parent]);
            }
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
        let left = duplicate_child(&self.children[0], work)?;
        let left =
            between_comparison_operand(left, &parents, &self.rows, selection, batch_rows, work)?;
        let right =
            between_comparison_operand(right, &parents, &self.rows, selection, batch_rows, work)?;
        let left = left.materialize(selection, &expected.data_type, work)?;
        let right = right.materialize(selection, &expected.data_type, work)?;
        let state = self
            .membership
            .as_mut()
            .ok_or_else(|| internal("missing IN continuation"))?;
        in_observed(state.candidate_nulls_selected_observed(
            right.values(),
            &parents,
            &mut |event| in_event(event, work),
        ))?;
        let equalities = in_observed(signed_equality_observed(
            left.values(),
            right.values(),
            &mut |event| in_event(event, work),
        ))?
        .ok_or_else(|| invalid("checked signed IN comparison lost its domain"))?;
        in_observed(
            state.equalities_selected_observed(&equalities, &parents, &mut |event| {
                in_event(event, work)
            }),
        )?;
        // A match never suppresses the next original candidate invocation.
        work.flush()?;
        drop(equalities);
        drop(left);
        drop(right);
        work.flush()?;
        Ok(())
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep the immutable recipe owner, selected child, parent semantics and work scope explicit"
    )]
    fn attach(
        &mut self,
        child: Child,
        program: &novarocks_local_program::LocalProgram,
        shape: ControlShape,
        demand: EvaluationDemand,
        child_is_pure: bool,
        batch_rows: usize,
        work: &mut Work<'_, '_>,
    ) -> Result<(), KernelFailure> {
        if let ControlShape::Membership { .. } = shape {
            return self.attach_membership(child, program, batch_rows, work);
        }
        if let ControlShape::Between { negated } = shape {
            return self.attach_between(child, program, negated, batch_rows, work);
        }
        if let ControlShape::TemporalSource(source_shape) = shape {
            return self.attach_temporal(child, source_shape, batch_rows, work);
        }
        if !matches!(
            shape,
            ControlShape::If
                | ControlShape::Coalesce
                | ControlShape::Conjunction
                | ControlShape::Disjunction
                | ControlShape::Case { .. }
        ) {
            self.children.push(child);
            return Ok(());
        }
        let child_index = self.children.len();
        let mut rows = Vec::with_capacity(child.ordinals.len());
        for &ordinal in &child.ordinals {
            rows.push(self.rows[ordinal]);
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
        let value = child.value.into_value(selection, work)?;
        let ty = value.argument().array().data_type().clone();
        let output = value.materialize(selection, &ty, work)?;
        if let ControlShape::Case { simple, arms, .. } = shape {
            let ordinal = self
                .next
                .checked_sub(1)
                .ok_or_else(|| internal("CASE child completed before its continuation"))?;
            let offset = usize::from(simple);
            if simple && ordinal == 0 {
                let mut errors = output.errors().iter().peekable();
                let mut remaining = Vec::with_capacity(child.ordinals.len());
                for (local, &parent) in child.ordinals.iter().enumerate() {
                    if errors
                        .peek()
                        .is_some_and(|error| error.selected_ordinal() == local)
                    {
                        let error = errors
                            .next()
                            .ok_or_else(|| internal("missing CASE operand error"))?;
                        self.errors
                            .insert(parent, error.with_selected_ordinal(parent));
                    } else {
                        remaining.push(parent);
                    }
                    work.step()?;
                }
                self.remaining = remaining;
                self.operand = Some(Arc::clone(output.values()));
                // The compact operand is retained once. Its errors are now
                // terminal parent evidence, excluded from all later labels.
                work.flush()?;
                drop(output);
                work.flush()?;
                return Ok(());
            }
            if ordinal >= offset
                && ordinal < offset + arms as usize * 2
                && (ordinal - offset).is_multiple_of(2)
            {
                let booleans = if simple {
                    None
                } else {
                    Some(
                        output
                            .values()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                invalid(
                                    "searched CASE WHEN differs from its frozen Boolean carrier",
                                )
                            })?,
                    )
                };
                let parent_selection =
                    Selection::try_sparse_observed(batch_rows, &self.rows, || work.step())?;
                let operand = if simple {
                    let array = self
                        .operand
                        .as_ref()
                        .ok_or_else(|| internal("missing once-evaluated simple CASE operand"))?;
                    Some(SelectedValues::try_new_observed(
                        parent_selection,
                        array.data_type(),
                        Arc::clone(array),
                        Box::default(),
                        || work.step(),
                    )?)
                } else {
                    None
                };
                let recipe = if simple {
                    Some(
                        program
                            .comparison_recipe(
                                novarocks_local_program::ProgramComparisonSite::CaseWhen {
                                    occurrence: self.occurrence,
                                    arm: ((ordinal - offset) / 2) as u32,
                                },
                            )
                            .ok_or_else(|| invalid("missing exact CASE WHEN comparison recipe"))?,
                    )
                } else {
                    None
                };
                let mut errors = output.errors().iter().peekable();
                let mut remaining = Vec::with_capacity(child.ordinals.len());
                self.matched.clear();
                for (local, &parent) in child.ordinals.iter().enumerate() {
                    if errors
                        .peek()
                        .is_some_and(|error| error.selected_ordinal() == local)
                    {
                        let error = errors
                            .next()
                            .ok_or_else(|| internal("missing CASE WHEN error"))?;
                        self.errors
                            .insert(parent, error.with_selected_ordinal(parent));
                    } else {
                        let matches = if let (Some(recipe), Some(operand)) =
                            (recipe, operand.as_ref())
                        {
                            work.flush()?;
                            recipe.compare_rows(
                                EvaluatedArgument::SelectedColumn(operand),
                                parent,
                                self.rows[parent],
                                EvaluatedArgument::SelectedColumn(&output),
                                local,
                                self.rows[parent],
                                work.control,
                            )? == Some(true)
                        } else {
                            let booleans = booleans
                                .ok_or_else(|| internal("missing searched CASE Boolean carrier"))?;
                            !booleans.is_null(local) && booleans.value(local)
                        };
                        if matches {
                            self.matched.push(parent);
                        } else {
                            remaining.push(parent);
                        }
                    }
                    work.step()?;
                }
                self.remaining = remaining;
                work.flush()?;
                drop(output);
                work.flush()?;
                return Ok(());
            }
            let mut errors = output.errors().iter().peekable();
            for (local, &parent) in child.ordinals.iter().enumerate() {
                if errors
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == local)
                {
                    let error = errors
                        .next()
                        .ok_or_else(|| internal("missing CASE result error"))?;
                    self.errors
                        .insert(parent, error.with_selected_ordinal(parent));
                } else {
                    self.choices[parent] = Some((child_index, local));
                }
                work.step()?;
            }
            self.matched.clear();
            if ordinal == offset + arms as usize * 2 {
                self.remaining.clear();
            }
            self.children.push(Child {
                ordinals: child.ordinals,
                value: OwnedValue::from_selected(output),
            });
            return Ok(());
        }
        if matches!(shape, ControlShape::Conjunction | ControlShape::Disjunction) {
            self.remaining = self
                .boolean
                .as_mut()
                .ok_or_else(|| internal("missing Boolean continuation"))?
                .consume(
                    &output,
                    &child.ordinals,
                    shape,
                    demand,
                    child_is_pure,
                    &mut self.errors,
                    work,
                )?;
            // A wide Boolean region retains only row state, not every operand's
            // selected array. Drop the consumed backing at an opaque boundary.
            work.flush()?;
            drop(output);
            work.flush()?;
            return Ok(());
        }
        let mut child_errors = output.errors().iter().peekable();
        let mut remaining = Vec::new();
        if shape == ControlShape::If && child_index == 0 {
            let booleans = output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| invalid("actual IF condition is not its frozen Boolean carrier"))?;
            self.routes.reserve(self.rows.len());
            for (ordinal, _) in rows.iter().enumerate() {
                let route = if child_errors
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == ordinal)
                {
                    let error = child_errors
                        .next()
                        .ok_or_else(|| internal("missing IF condition error"))?;
                    self.errors
                        .insert(ordinal, error.with_selected_ordinal(ordinal));
                    None
                } else {
                    Some(!booleans.is_null(ordinal) && booleans.value(ordinal))
                };
                self.routes.push(route);
                work.step()?;
            }
        } else {
            work.flush()?;
            visit_selected_nulls(
                EvaluatedArgument::SelectedColumn(&output),
                selection,
                work.control,
                |ordinal, _, is_null| {
                    let parent = child.ordinals[ordinal];
                    if child_errors
                        .peek()
                        .is_some_and(|error| error.selected_ordinal() == ordinal)
                    {
                        let error = child_errors
                            .next()
                            .ok_or_else(|| internal("missing guarded child error"))?;
                        self.errors
                            .insert(parent, error.with_selected_ordinal(parent));
                    } else if shape == ControlShape::Coalesce && is_null {
                        remaining.push(parent);
                    } else {
                        self.choices[parent] = Some((child_index, ordinal));
                    }
                    Ok(())
                },
            )?;
        }
        if shape == ControlShape::Coalesce {
            self.remaining = remaining;
        }
        self.children.push(Child {
            ordinals: child.ordinals,
            value: OwnedValue::from_selected(output),
        });
        Ok(())
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep the checked root, exact input port, selected domain, use-owned state/effects and caller control explicit"
)]
pub(super) fn evaluate_tree<'a, E: scalar_invocation::FrameFailure>(
    program: &novarocks_local_program::LocalProgram,
    root: ProgramExpressionRootSite,
    input: &RecordBatch,
    input_node: Option<(ProgramNodeId, ProgramChannelLayoutRole)>,
    selection: Selection<'a>,
    activation: novarocks_functions::ScalarInvocationActivation,
    instances: &mut BTreeMap<ProgramUseRef, scalar_invocation::CallInstance>,
    allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    effects: &BTreeMap<ProgramUseRef, ScopedExpressionEffects>,
    work: &mut Work<'_, '_>,
) -> Result<Value<'a>, E> {
    let checked = program.checked();
    let typed = checked.channels().expressions();
    let resolved = typed.resolved_calls();
    let snapshot = resolved.snapshot();
    let flow = &snapshot.flows()[&root.arena()];
    let definitions = &snapshot.roots().arenas()[&root.arena()];
    let root_use = ProgramUseRef {
        arena: root.arena(),
        use_id: snapshot.bindings()[&root],
    };
    // Admit the controller's per-row storage representation before its first
    // allocation. This is an extent gate, not a host memory grant.
    let element_width = std::mem::size_of::<RowDataError>()
        .max(std::mem::size_of::<Option<(usize, usize)>>())
        .max(std::mem::size_of::<usize>());
    if selection
        .len()
        .checked_mul(element_width)
        .is_none_or(|bytes| bytes > isize::MAX as usize)
    {
        return Err(KernelFailure::ResourceExhausted.into());
    }
    work.step()?;
    let mut rows = Vec::with_capacity(selection.len());
    for row in selection.iter() {
        rows.push(row);
        work.step()?;
    }
    let root_invocation = &flow.uses()[&root_use.use_id];
    let root_type = definitions
        .node(root_invocation.definition)
        .ok_or_else(|| invalid("missing actual root definition"))?
        .data_type();
    let mut frames = vec![Frame::new(
        root_use,
        root_invocation.control,
        root_type,
        rows,
        if E::ACTIVATE_EMPTY {
            activation
        } else {
            novarocks_functions::ScalarInvocationActivation::ValidateOnly
        },
        Vec::new(),
        work,
    )?];
    while let Some(mut frame) = frames.pop() {
        work.step()?;
        let invocation = &flow.uses()[&frame.occurrence.use_id];
        let definition = definitions
            .node(invocation.definition)
            .ok_or_else(|| invalid("missing compiled definition"))?;
        let FunctionArgumentType::Value(result_type) = typed
            .definition_type(frame.occurrence.arena, invocation.definition)
            .ok_or_else(|| invalid("missing exact compiled definition type"))?
        else {
            return Err(invalid("ordinary root cannot materialize a lambda definition").into());
        };
        // An empty actual domain never enters children, constructors or kernels.
        let next = if frame.rows.is_empty()
            && frame.activation == novarocks_functions::ScalarInvocationActivation::ValidateOnly
        {
            None
        } else {
            let next_is_pure = invocation
                .arguments
                .get(frame.next)
                .map(|child| {
                    let occurrence = ProgramUseRef {
                        arena: frame.occurrence.arena,
                        use_id: *child,
                    };
                    effects
                        .get(&occurrence)
                        .ok_or_else(|| invalid("missing exact operand effects"))?
                        .for_use(flow.uses()[child].context)
                        .map_err(|_| invalid("operand effect context differs"))
                        .map(|summary| summary.permits_boolean_reordering())
                })
                .transpose()?
                .unwrap_or(true);
            frame.next_ordinals(
                invocation.control,
                invocation.arguments.len(),
                next_is_pure,
                input.num_rows(),
                work,
            )?
        };
        if let Some(ordinals) = next {
            let child_use = invocation.arguments[frame.next];
            frame.next += 1;
            let mut rows = Vec::with_capacity(ordinals.len());
            for &ordinal in &ordinals {
                rows.push(frame.rows[ordinal]);
                work.step()?;
            }
            let child = Frame::new(
                ProgramUseRef {
                    arena: frame.occurrence.arena,
                    use_id: child_use,
                },
                flow.uses()[&child_use].control,
                definitions
                    .node(flow.uses()[&child_use].definition)
                    .ok_or_else(|| invalid("missing actual child definition"))?
                    .data_type(),
                rows,
                if !ordinals.is_empty()
                    || frame.activation
                        == novarocks_functions::ScalarInvocationActivation::Activated
                        && frame.rows.is_empty()
                {
                    novarocks_functions::ScalarInvocationActivation::Activated
                } else {
                    novarocks_functions::ScalarInvocationActivation::ValidateOnly
                },
                ordinals,
                work,
            )?;
            frames.push(frame);
            frames.push(child);
            continue;
        }
        let local_selection =
            Selection::try_sparse_observed(input.num_rows(), &frame.rows, || work.step())?;
        let value = if frame.rows.is_empty()
            && frame.activation == novarocks_functions::ScalarInvocationActivation::ValidateOnly
        {
            work.flush()?;
            let array = new_empty_array(&result_type.data_type);
            work.flush()?;
            OwnedValue::Selected(array, Box::default())
        } else {
            match definition.kind() {
                StaticExprKind::Constant(constant) => {
                    work.flush()?;
                    validate_evaluated_argument_observed(
                        EvaluatedArgument::Constant(constant),
                        local_selection,
                        result_type,
                        work.control,
                    )?;
                    OwnedValue::Constant(constant.clone())
                }
                StaticExprKind::SlotId(_) => {
                    let Some(ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                        node,
                        role,
                        ordinal,
                    })) = checked.slots().get(&frame.occurrence)
                    else {
                        return Err(
                            invalid("slot requires its actual compiled input source").into()
                        );
                    };
                    // An empty-port root has no input source at all; every
                    // other root reads exactly its own (node, role) port.
                    if Some((*node, *role)) != input_node {
                        return Err(
                            invalid("slot source differs from actual root input port").into()
                        );
                    }
                    OwnedValue::Column(Arc::clone(
                        input
                            .columns()
                            .get(*ordinal as usize)
                            .ok_or_else(|| invalid("slot source ordinal is absent"))?,
                    ))
                }
                StaticExprKind::PreparedLike { negated, .. } => {
                    if frame.children.len() != 2 {
                        return Err(invalid("LIKE requires its original ordered operands").into());
                    }
                    let mut children = std::mem::take(&mut frame.children).into_iter();
                    let text = children
                        .next()
                        .ok_or_else(|| internal("missing original LIKE source"))?
                        .value
                        .into_value(local_selection, work)?;
                    let pattern = children
                        .next()
                        .ok_or_else(|| internal("missing original LIKE pattern"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe = program
                        .native_like_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing exact LIKE recipe"))?;
                    if recipe.negated() != *negated {
                        return Err(invalid(
                            "LIKE negative expansion differs from original source",
                        )
                        .into());
                    }
                    work.flush()?;
                    let output = recipe.evaluate_selected(
                        text.argument(),
                        pattern.argument(),
                        local_selection,
                        invocation.context.demand == EvaluationDemand::TruthOnly,
                        work.control,
                    )?;
                    work.flush()?;
                    OwnedValue::from_selected(output)
                }
                StaticExprKind::PreparedInList { is_not_in, .. } => {
                    let recipe = program
                        .native_inlist_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing IN completion recipe"))?;
                    if recipe.negated() != *is_not_in || frame.children.len() != 1 {
                        return Err(invalid("IN completion differs from its ordered source").into());
                    }
                    let source = frame
                        .children
                        .pop()
                        .ok_or_else(|| internal("missing original IN source"))?;
                    let source = source.value.into_value(local_selection, work)?;
                    let output = source.materialize(
                        local_selection,
                        &recipe.source_type().data_type,
                        work,
                    )?;
                    let mut failed = Vec::with_capacity(frame.errors.len());
                    for &row in frame.errors.keys() {
                        failed.push(row);
                        work.step()?;
                    }
                    let state = frame
                        .membership
                        .take()
                        .ok_or_else(|| internal("missing IN state at completion"))?;
                    let result = in_observed(state.finish_selected_observed(
                        output.values(),
                        *is_not_in,
                        invocation.context.demand == EvaluationDemand::TruthOnly,
                        &failed,
                        &mut |event| in_event(event, work),
                    ))?;
                    let mut errors = Vec::with_capacity(frame.errors.len());
                    for (_, error) in std::mem::take(&mut frame.errors) {
                        errors.push(error);
                        work.step()?;
                    }
                    work.flush()?;
                    OwnedValue::from_selected(SelectedValues::try_new_observed(
                        local_selection,
                        &result_type.data_type,
                        Arc::new(result),
                        errors.into_boxed_slice(),
                        || work.step(),
                    )?)
                }
                StaticExprKind::PreparedBetween { plan, .. } => {
                    if !frame.children.is_empty() {
                        return Err(invalid("BETWEEN comparison phase was not completed").into());
                    }
                    let state = frame
                        .boolean
                        .take()
                        .ok_or_else(|| internal("missing BETWEEN Boolean continuation"))?;
                    let (array, errors) = state.finish(
                        plan.connective(),
                        invocation.context.demand,
                        std::mem::take(&mut frame.errors),
                        work,
                    )?;
                    OwnedValue::from_selected(SelectedValues::try_new_observed(
                        local_selection,
                        &result_type.data_type,
                        array,
                        errors,
                        || work.step(),
                    )?)
                }
                StaticExprKind::NaryAnd { .. } | StaticExprKind::NaryOr { .. } => {
                    let state = frame
                        .boolean
                        .take()
                        .ok_or_else(|| internal("missing Boolean continuation"))?;
                    let (array, errors) = state.finish(
                        invocation.control,
                        invocation.context.demand,
                        std::mem::take(&mut frame.errors),
                        work,
                    )?;
                    OwnedValue::from_selected(SelectedValues::try_new_observed(
                        local_selection,
                        &result_type.data_type,
                        array,
                        errors,
                        || work.step(),
                    )?)
                }
                StaticExprKind::PreparedCast { .. } => {
                    if frame.children.len() != 1 {
                        return Err(invalid("cast requires its exact operand").into());
                    }
                    let child = std::mem::take(&mut frame.children)
                        .into_iter()
                        .next()
                        .ok_or_else(|| internal("missing cast operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe = program
                        .cast_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing exact cast recipe"))?;
                    if recipe.is_identity() {
                        // The value passes unchanged, with its row errors;
                        // only the frozen nullability widens.
                        OwnedValue::from_selected(child.materialize(
                            local_selection,
                            &recipe.result_type().data_type,
                            work,
                        )?)
                    } else {
                        OwnedValue::from_selected(evaluate_cast(
                            recipe,
                            &child,
                            local_selection,
                            work,
                        )?)
                    }
                }
                StaticExprKind::PreparedNativeNegate(_) => {
                    if frame.children.len() != 1 {
                        return Err(
                            invalid("native negate requires its exact ordered operand").into()
                        );
                    }
                    let child = frame
                        .children
                        .pop()
                        .ok_or_else(|| internal("missing native negate operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe = program
                        .native_negate_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing exact native negate recipe"))?;
                    work.flush()?;
                    let output = recipe.evaluate_selected(
                        child.argument(),
                        local_selection,
                        work.control,
                    )?;
                    work.flush()?;
                    OwnedValue::from_selected(output)
                }
                StaticExprKind::PreparedNativeBitNot(_) => {
                    if frame.children.len() != 1 {
                        return Err(invalid(
                            "native BitwiseNot requires its exact ordered operand",
                        )
                        .into());
                    }
                    let child = frame
                        .children
                        .pop()
                        .ok_or_else(|| internal("missing native BitwiseNot operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe = program
                        .native_bitnot_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing exact native BitwiseNot recipe"))?;
                    work.flush()?;
                    let output = recipe.evaluate_selected(
                        child.argument(),
                        local_selection,
                        work.control,
                    )?;
                    work.flush()?;
                    OwnedValue::from_selected(output)
                }
                StaticExprKind::PreparedArithmetic { .. } => {
                    if frame.children.len() != 2 {
                        return Err(
                            invalid("arithmetic requires its exact ordered operands").into()
                        );
                    }
                    let mut children = std::mem::take(&mut frame.children).into_iter();
                    let left = children
                        .next()
                        .ok_or_else(|| internal("missing arithmetic left operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let right = children
                        .next()
                        .ok_or_else(|| internal("missing arithmetic right operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe = program
                        .arithmetic_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing exact arithmetic recipe"))?;
                    OwnedValue::from_selected(evaluate_arithmetic(
                        recipe,
                        &left,
                        &right,
                        local_selection,
                        work,
                    )?)
                }
                kind if kind.ordinary_comparison().is_some()
                    || matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. }) =>
                {
                    if frame.children.len() != 2 {
                        return Err(
                            invalid("comparison requires its exact ordered operands").into()
                        );
                    }
                    let mut children = std::mem::take(&mut frame.children).into_iter();
                    let left = children
                        .next()
                        .ok_or_else(|| internal("missing comparison left operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let right = children
                        .next()
                        .ok_or_else(|| internal("missing comparison right operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe =
                        if matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. }) {
                            ComparisonRecipe::NullSafe(
                                program
                                    .null_safe_comparison_recipe(frame.occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing exact null-safe comparison recipe")
                                    })?,
                            )
                        } else {
                            ComparisonRecipe::Ordinary(
                                program
                                    .comparison_recipe(
                                        novarocks_local_program::ProgramComparisonSite::Binary(
                                            frame.occurrence,
                                        ),
                                    )
                                    .ok_or_else(|| invalid("missing exact comparison recipe"))?,
                            )
                        };
                    OwnedValue::from_selected(evaluate_comparison(
                        recipe,
                        &left,
                        &right,
                        local_selection,
                        work,
                    )?)
                }
                StaticExprKind::Case { .. } => assemble(
                    &frame.children,
                    &frame.choices,
                    &mut frame.errors,
                    &result_type.data_type,
                    local_selection,
                    work,
                )?,
                StaticExprKind::Not(_)
                | StaticExprKind::IsNull(_)
                | StaticExprKind::IsNotNull(_) => {
                    if frame.children.len() != 1 {
                        return Err(invalid("unary occurrence requires its exact operand").into());
                    }
                    let child = frame
                        .children
                        .pop()
                        .ok_or_else(|| internal("missing actual unary operand"))?;
                    let value = child.value.into_value(local_selection, work)?;
                    OwnedValue::from_selected(super::unary::evaluate(
                        definition.kind(),
                        &value,
                        local_selection,
                        work,
                    )?)
                }
                StaticExprKind::BoundCall { .. } => {
                    let call = &resolved.calls()[&ProgramCallSite::Expression(frame.occurrence)];
                    match call.specialization().prepared() {
                        PreparedPureKernel::Scalar(_) | PreparedPureKernel::ScalarInvocation(_) => {
                            let prepared = match call.specialization().prepared() {
                                PreparedPureKernel::Scalar(p) => {
                                    scalar_invocation::PreparedCall::Kernel(p)
                                }
                                PreparedPureKernel::ScalarInvocation(p) => {
                                    scalar_invocation::PreparedCall::Invocation(p)
                                }
                                _ => {
                                    return Err(E::from(invalid(
                                        "ordinary call changed its prepared lifecycle",
                                    )));
                                }
                            };
                            let mut children = Vec::with_capacity(frame.children.len());
                            for child in std::mem::take(&mut frame.children) {
                                children.push(child.value.into_value(local_selection, work)?);
                                work.step()?;
                            }
                            OwnedValue::from_selected(evaluate_scalar::<E>(
                                frame.occurrence,
                                prepared,
                                &children,
                                local_selection,
                                frame.activation,
                                instances,
                                allocator,
                                work,
                            )?)
                        }
                        PreparedPureKernel::ControlIntrinsic(_)
                            if matches!(invocation.control, ControlShape::TemporalSource(_)) =>
                        {
                            let state = frame.temporal.take().ok_or_else(|| {
                                internal("missing temporal continuation at completion")
                            })?;
                            state.finish(
                                invocation.control,
                                &mut frame.errors,
                                local_selection,
                                work,
                            )?
                        }
                        PreparedPureKernel::ControlIntrinsic(_) => assemble(
                            &frame.children,
                            &frame.choices,
                            &mut frame.errors,
                            &result_type.data_type,
                            local_selection,
                            work,
                        )?,
                        _ => {
                            return Err(
                                invalid("compiled occurrence has a different lifecycle").into()
                            );
                        }
                    }
                }
                _ => return Err(invalid("unsupported compiled root definition").into()),
            }
        };
        if let Some(operand) = frame.operand.take() {
            work.flush()?;
            drop(operand);
            work.flush()?;
        }
        if let Some(parent) = frames.last_mut() {
            let parent_invocation = &flow.uses()[&parent.occurrence.use_id];
            let shape = parent_invocation.control;
            let child_is_pure = effects
                .get(&frame.occurrence)
                .ok_or_else(|| invalid("missing actual child effects"))?
                .for_use(invocation.context)
                .map_err(|_| invalid("child effects differ from actual occurrence"))?
                .permits_boolean_reordering();
            parent.attach(
                Child {
                    ordinals: frame.parent_ordinals,
                    value,
                },
                program,
                shape,
                parent_invocation.context.demand,
                child_is_pure,
                input.num_rows(),
                work,
            )?;
        } else {
            return value.into_value(selection, work).map_err(E::from);
        }
    }
    Err(internal("actual compiled root result is absent").into())
}

fn assemble(
    children: &[Child],
    choices: &[Option<(usize, usize)>],
    row_errors: &mut BTreeMap<usize, RowDataError>,
    ty: &DataType,
    selection: Selection<'_>,
    work: &mut Work<'_, '_>,
) -> Result<OwnedValue, KernelFailure> {
    // The shared pure assembly receives only already-evaluated compact values.
    // Frame scheduling and its original lazy row demand remain in this host.
    let mut sources = Vec::with_capacity(children.len());
    for child in children {
        sources.push(match &child.value {
            OwnedValue::Selected(array, _) => Some(array),
            _ => None,
        });
        work.step()?;
    }
    work.flush()?;
    let array = novarocks_functions::control_values::assemble_values(
        novarocks_functions::control_values::AssemblyPlan::Indexed {
            result_type: ty,
            sources: &sources,
            choices,
        },
        work.control,
    )
    .map_err(|error| match error {
        novarocks_functions::control_values::AssemblyFailure::Kernel(error) => error,
        novarocks_functions::control_values::AssemblyFailure::Arrow(_) => {
            internal("checked guarded result could not be assembled")
        }
    })?;
    work.flush()?;
    let mut errors = Vec::with_capacity(row_errors.len());
    for error in std::mem::take(row_errors).into_values() {
        errors.push(error);
        work.step()?;
    }
    Ok(OwnedValue::from_selected(SelectedValues::try_new_observed(
        selection,
        ty,
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )?))
}

/// Re-address one retained phase result through the existing selected COPY
/// author. Nothing reads an inactive original batch row or supplies a new FVT.
fn between_comparison_operand<'a>(
    child: Child,
    parent_ordinals: &[usize],
    parent_rows: &[usize],
    selection: Selection<'a>,
    batch_rows: usize,
    work: &mut Work<'_, '_>,
) -> Result<Value<'a>, KernelFailure> {
    let mut rows = Vec::with_capacity(child.ordinals.len());
    for &parent in &child.ordinals {
        rows.push(parent_rows[parent]);
        work.step()?;
    }
    let source_selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
    let value = child.value.into_value(source_selection, work)?;
    let ty = value.argument().array().data_type().clone();
    let output = value.materialize(source_selection, &ty, work)?;
    let mut indices = Vec::with_capacity(parent_ordinals.len());
    for &parent in parent_ordinals {
        let index = child
            .ordinals
            .binary_search(&parent)
            .map_err(|_| invalid("BETWEEN comparison row is absent from its source phase"))?;
        indices.push(Some(
            u64::try_from(index).map_err(|_| KernelFailure::ResourceExhausted)?,
        ));
        work.step()?;
    }
    let array = gather(output.values(), &indices, work)?;
    work.flush()?;
    drop(output);
    work.flush()?;
    Ok(Value::Selected(SelectedValues::try_new_observed(
        selection,
        &ty,
        array,
        Box::default(),
        || work.step(),
    )?))
}

/// The shared Boolean journal/assembly is independent of the numeric algorithm.
/// Null-safe comparison retains its own prepared recipe and scalar semantics.
enum ComparisonRecipe<'a> {
    Ordinary(&'a novarocks_functions::PreparedComparisonRecipe),
    NullSafe(&'a novarocks_functions::PreparedNullSafeComparisonRecipe),
}

fn evaluate_comparison<'a>(
    recipe: ComparisonRecipe<'_>,
    left: &Value<'_>,
    right: &Value<'_>,
    selection: Selection<'a>,
    work: &mut Work<'_, '_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    // Borrow the original callback; a nested comparison's footer cannot
    // observe again after any of its seven originating refusal categories.
    let observed = novarocks_functions::KernelControlObservation::new(work.control);
    let mut left_errors = left.errors().iter().peekable();
    let mut right_errors = right.errors().iter().peekable();
    // Representation accounting precedes fallible reservations. It is not a
    // formal host memory grant or an Arrow allocation-origin guarantee.
    selection
        .len()
        .checked_mul(
            std::mem::size_of::<Option<bool>>()
                + std::mem::size_of::<RowDataError>()
                + novarocks_functions::MAX_ROW_ERROR_MESSAGE_BYTES,
        )
        .and_then(|n| {
            selection
                .len()
                .checked_add(7)
                .and_then(|bits| n.checked_add(bits / 8))
        })
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut errors = Vec::new();
    let mut values = Vec::new();
    errors
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    for (ordinal, row) in selection.iter().enumerate() {
        let l = if left_errors
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            left_errors.next()
        } else {
            None
        };
        let r = if right_errors
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            right_errors.next()
        } else {
            None
        };
        if let Some(error) = l.or(r) {
            errors.push(error.clone());
            values.push(None);
        } else {
            work.flush()?;
            let compared = match recipe {
                ComparisonRecipe::Ordinary(recipe) => recipe.compare_rows(
                    left.argument(),
                    ordinal,
                    row,
                    right.argument(),
                    ordinal,
                    row,
                    &observed,
                )?,
                ComparisonRecipe::NullSafe(recipe) => Some(recipe.compare_rows(
                    left.argument(),
                    ordinal,
                    row,
                    right.argument(),
                    ordinal,
                    row,
                    &observed,
                )?),
            };
            values.push(compared);
        }
        work.step()?;
    }
    work.flush()?;
    let array = Arc::new(BooleanArray::from(values));
    work.flush()?;
    SelectedValues::try_new_observed(
        selection,
        &DataType::Boolean,
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )
}

fn evaluate_arithmetic<'a>(
    recipe: &novarocks_functions::PreparedArithmeticRecipe,
    left: &Value<'_>,
    right: &Value<'_>,
    selection: Selection<'a>,
    work: &mut Work<'_, '_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use arrow::array::{
        Decimal128Array, Decimal256Array, Float64Array, Int16Array, Int32Array, Int64Array,
        builder::FixedSizeBinaryBuilder,
    };
    use arrow_buffer::i256;
    use novarocks_functions::ArithmeticRowResult as R;
    let ty = &recipe.result_type().data_type;
    // This is a checked representation bound and fallible capacity reservation,
    // not a host Account grant or an Arrow allocation-origin receipt.
    let bitmap = selection
        .len()
        .checked_add(7)
        .map(|n| n / 8)
        .and_then(|n| n.checked_add(63))
        .map(|n| n / 64 * 64)
        .ok_or(KernelFailure::ResourceExhausted)?;
    selection
        .len()
        .checked_mul(
            std::mem::size_of::<Option<i256>>()
                + 32
                + std::mem::size_of::<RowDataError>()
                + novarocks_functions::MAX_ROW_ERROR_MESSAGE_BYTES,
        )
        .and_then(|n| n.checked_add(bitmap))
        .filter(|bytes| *bytes <= isize::MAX as usize)
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    enum Output {
        I16(Vec<Option<i16>>),
        I32(Vec<Option<i32>>),
        I64(Vec<Option<i64>>),
        Float(Vec<Option<f64>>),
        Decimal128(Vec<Option<i128>>),
        Decimal256(Vec<Option<i256>>),
        LargeInt(FixedSizeBinaryBuilder),
    }
    let mut output = match ty {
        DataType::Int16 => Output::I16(Vec::new()),
        DataType::Int32 => Output::I32(Vec::new()),
        DataType::Int64 => Output::I64(Vec::new()),
        DataType::Float64 => Output::Float(Vec::new()),
        DataType::Decimal128(..) => Output::Decimal128(Vec::new()),
        DataType::Decimal256(..) => Output::Decimal256(Vec::new()),
        DataType::FixedSizeBinary(16)
            if recipe.result_type().logical_type
                == novarocks_type_contract::ValueLogicalType::LargeInt =>
        {
            Output::LargeInt(FixedSizeBinaryBuilder::with_capacity(selection.len(), 16))
        }
        _ => {
            return Err(internal(
                "arithmetic recipe has an unsupported frozen result carrier",
            ));
        }
    };
    match &mut output {
        Output::I16(values) => values.try_reserve_exact(selection.len()),
        Output::I32(values) => values.try_reserve_exact(selection.len()),
        Output::I64(values) => values.try_reserve_exact(selection.len()),
        Output::Float(values) => values.try_reserve_exact(selection.len()),
        Output::Decimal128(values) => values.try_reserve_exact(selection.len()),
        Output::Decimal256(values) => values.try_reserve_exact(selection.len()),
        Output::LargeInt(_) => Ok(()),
    }
    .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut errors = Vec::new();
    errors
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut left_errors = left.errors().iter().peekable();
    let mut right_errors = right.errors().iter().peekable();
    for (ordinal, row) in selection.iter().enumerate() {
        let l = if left_errors
            .peek()
            .is_some_and(|e| e.selected_ordinal() == ordinal)
        {
            left_errors.next()
        } else {
            None
        };
        let r = if right_errors
            .peek()
            .is_some_and(|e| e.selected_ordinal() == ordinal)
        {
            right_errors.next()
        } else {
            None
        };
        let result = if let Some(error) = l.or(r) {
            R::RowError(error.clone())
        } else {
            work.flush()?;
            recipe.evaluate_row(
                left.argument(),
                ordinal,
                row,
                right.argument(),
                ordinal,
                row,
                work.control,
            )?
        };
        let result = match result {
            R::RowError(error) => {
                errors.push(error.with_selected_ordinal(ordinal));
                R::Null
            }
            result => result,
        };
        match (&mut output, result) {
            (Output::I16(values), R::Signed(value)) => values.push(i16::try_from(value).ok()),
            (Output::I32(values), R::Signed(value)) => values.push(i32::try_from(value).ok()),
            (Output::I64(values), R::Signed(value)) => values.push(Some(value)),
            (Output::Float(values), R::Float(value)) => values.push(Some(value)),
            (Output::Decimal128(values), R::Decimal128(value)) => values.push(Some(value)),
            (Output::Decimal256(values), R::Decimal256(value)) => values.push(Some(value)),
            (Output::LargeInt(values), R::LargeInt(value)) => values
                .append_value(value.to_be_bytes())
                .map_err(|_| internal("LargeInt result differs from its fixed carrier width"))?,
            (Output::I16(values), R::Null) => values.push(None),
            (Output::I32(values), R::Null) => values.push(None),
            (Output::I64(values), R::Null) => values.push(None),
            (Output::Float(values), R::Null) => values.push(None),
            (Output::Decimal128(values), R::Null) => values.push(None),
            (Output::Decimal256(values), R::Null) => values.push(None),
            (Output::LargeInt(values), R::Null) => values.append_null(),
            _ => {
                return Err(internal(
                    "arithmetic body returned a foreign frozen result carrier",
                ));
            }
        }
        work.step()?;
    }
    work.flush()?;
    let array: ArrayRef = match (ty, output) {
        (DataType::Int16, Output::I16(values)) => Arc::new(Int16Array::from(values)),
        (DataType::Int32, Output::I32(values)) => Arc::new(Int32Array::from(values)),
        (DataType::Int64, Output::I64(values)) => Arc::new(Int64Array::from(values)),
        (DataType::Float64, Output::Float(values)) => Arc::new(Float64Array::from(values)),
        (DataType::Decimal128(precision, scale), Output::Decimal128(values)) => Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|_| {
                    internal("Decimal128 result metadata differs from its prepared type")
                })?,
        ),
        (DataType::Decimal256(precision, scale), Output::Decimal256(values)) => Arc::new(
            Decimal256Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|_| {
                    internal("Decimal256 result metadata differs from its prepared type")
                })?,
        ),
        (DataType::FixedSizeBinary(16), Output::LargeInt(mut values)) => Arc::new(values.finish()),
        _ => {
            return Err(internal(
                "arithmetic recipe has an unsupported frozen result carrier",
            ));
        }
    };
    work.flush()?;
    SelectedValues::try_new_observed(selection, ty, array, errors.into_boxed_slice(), || {
        work.step()
    })
}

fn evaluate_cast<'a>(
    recipe: &novarocks_functions::PreparedCastRecipe,
    child: &Value<'_>,
    selection: Selection<'a>,
    work: &mut Work<'_, '_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use arrow::array::{
        Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    };
    use novarocks_functions::CastRowResult as R;
    let ty = &recipe.result_type().data_type;
    if recipe.is_collection() {
        work.flush()?;
        return recipe.evaluate_collection(
            child.argument(),
            selection,
            child.errors(),
            work.control,
        );
    }
    // An unzoned primitive rendering fits within 64 bytes, including the
    // widest Chrono year and nanosecond fraction. Check Arrow's i32 offset
    // extent before producing text. This is not a host memory grant.
    let variable_binary_text =
        ty == &DataType::Utf8 && recipe.source_type().data_type == DataType::Binary;
    if ty == &DataType::Utf8 && !variable_binary_text {
        selection
            .len()
            .checked_mul(64)
            .filter(|n| *n <= i32::MAX as usize)
            .ok_or(KernelFailure::ResourceExhausted)?;
    }
    // Checked representation and fallible reservation are not a host memory grant.
    let bitmap = selection
        .len()
        .checked_add(7)
        .map(|n| n / 8)
        .and_then(|n| n.checked_add(63))
        .map(|n| n / 64 * 64)
        .ok_or(KernelFailure::ResourceExhausted)?;
    selection
        .len()
        .checked_mul(
            if matches!(ty, DataType::Decimal128(..)) {
                std::mem::size_of::<Option<i128>>()
            } else {
                std::mem::size_of::<Option<i64>>()
            } + std::mem::size_of::<RowDataError>()
                + novarocks_functions::MAX_ROW_ERROR_MESSAGE_BYTES,
        )
        .and_then(|n| n.checked_add(bitmap))
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    enum Output {
        Boolean(Vec<Option<bool>>),
        Text(Vec<Option<String>>),
        I8(Vec<Option<i8>>),
        I16(Vec<Option<i16>>),
        I32(Vec<Option<i32>>),
        Date32(Vec<Option<i32>>),
        I64(Vec<Option<i64>>),
        Timestamp(Vec<Option<i64>>),
        TimeMicroseconds(Vec<Option<i64>>),
        U8(Vec<Option<u8>>),
        U16(Vec<Option<u16>>),
        U32(Vec<Option<u32>>),
        U64(Vec<Option<u64>>),
        F32(Vec<Option<f32>>),
        F64(Vec<Option<f64>>),
        Decimal128(Vec<Option<i128>>),
    }
    let mut output = match ty {
        DataType::Boolean => Output::Boolean(Vec::new()),
        DataType::Utf8 => Output::Text(Vec::new()),
        DataType::Int8 => Output::I8(Vec::new()),
        DataType::Int16 => Output::I16(Vec::new()),
        DataType::Int32 => Output::I32(Vec::new()),
        DataType::Date32 => Output::Date32(Vec::new()),
        DataType::Int64 => Output::I64(Vec::new()),
        DataType::Timestamp(_, None) => Output::Timestamp(Vec::new()),
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            Output::TimeMicroseconds(Vec::new())
        }
        DataType::UInt8 => Output::U8(Vec::new()),
        DataType::UInt16 => Output::U16(Vec::new()),
        DataType::UInt32 => Output::U32(Vec::new()),
        DataType::UInt64 => Output::U64(Vec::new()),
        DataType::Float32 => Output::F32(Vec::new()),
        DataType::Float64 => Output::F64(Vec::new()),
        DataType::Decimal128(..) => Output::Decimal128(Vec::new()),
        _ => return Err(internal("cast recipe has a foreign frozen result carrier")),
    };
    match &mut output {
        Output::Boolean(v) => v.try_reserve_exact(selection.len()),
        Output::Text(v) => v.try_reserve_exact(selection.len()),
        Output::I8(v) => v.try_reserve_exact(selection.len()),
        Output::I16(v) => v.try_reserve_exact(selection.len()),
        Output::I32(v) => v.try_reserve_exact(selection.len()),
        Output::Date32(v) => v.try_reserve_exact(selection.len()),
        Output::I64(v) => v.try_reserve_exact(selection.len()),
        Output::Timestamp(v) => v.try_reserve_exact(selection.len()),
        Output::TimeMicroseconds(v) => v.try_reserve_exact(selection.len()),
        Output::U8(v) => v.try_reserve_exact(selection.len()),
        Output::U16(v) => v.try_reserve_exact(selection.len()),
        Output::U32(v) => v.try_reserve_exact(selection.len()),
        Output::U64(v) => v.try_reserve_exact(selection.len()),
        Output::F32(v) => v.try_reserve_exact(selection.len()),
        Output::F64(v) => v.try_reserve_exact(selection.len()),
        Output::Decimal128(v) => v.try_reserve_exact(selection.len()),
    }
    .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut errors = Vec::new();
    errors
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut inherited = child.errors().iter().peekable();
    let mut binary_text_bytes = 0_usize;
    for (ordinal, row) in selection.iter().enumerate() {
        let value = if inherited
            .peek()
            .is_some_and(|e| e.selected_ordinal() == ordinal)
        {
            R::RowError(inherited.next().expect("checked inherited error").clone())
        } else {
            work.flush()?;
            recipe.evaluate_row(child.argument(), ordinal, row, work.control)?
        };
        let value = match value {
            R::RowError(error) => {
                errors.push(error.with_selected_ordinal(ordinal));
                R::Null
            }
            other => other,
        };
        match (&mut output, value) {
            (Output::Boolean(v), R::Boolean(n)) => v.push(Some(n)),
            (Output::Text(v), R::Text(n)) => {
                if variable_binary_text {
                    binary_text_bytes = binary_text_bytes
                        .checked_add(n.len())
                        .filter(|bytes| *bytes <= i32::MAX as usize)
                        .ok_or(KernelFailure::ResourceExhausted)?;
                }
                v.push(Some(n));
            }
            (Output::Text(v), R::Null) => v.push(None),
            (Output::I8(v), R::Signed(n)) => v.push(Some(
                i8::try_from(n).map_err(|_| internal("cast returned an out-of-range Int8"))?,
            )),
            (Output::I16(v), R::Signed(n)) => {
                v.push(Some(i16::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range Int16")
                })?))
            }
            (Output::I32(v), R::Signed(n)) => {
                v.push(Some(i32::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range Int32")
                })?))
            }
            (Output::I64(v), R::Signed(n)) => v.push(Some(n)),
            (Output::TimeMicroseconds(v), R::Signed(n)) => v.push(Some(n)),
            (Output::Date32(v), R::Signed(n)) => {
                v.push(Some(i32::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range Date32")
                })?))
            }
            (Output::Timestamp(v), R::Timestamp(n)) => v.push(Some(n)),
            (Output::U8(v), R::Unsigned(n)) => {
                v.push(Some(u8::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range UInt8")
                })?))
            }
            (Output::U16(v), R::Unsigned(n)) => {
                v.push(Some(u16::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range UInt16")
                })?))
            }
            (Output::U32(v), R::Unsigned(n)) => {
                v.push(Some(u32::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range UInt32")
                })?))
            }
            (Output::U64(v), R::Unsigned(n)) => v.push(Some(n)),
            (Output::F32(v), R::Float32(n)) => v.push(Some(n)),
            (Output::F64(v), R::Float64(n)) => v.push(Some(n)),
            (Output::Decimal128(v), R::Decimal128(n)) => v.push(Some(n)),
            (Output::I8(v), R::Null) => v.push(None),
            (Output::Boolean(v), R::Null) => v.push(None),
            (Output::I16(v), R::Null) => v.push(None),
            (Output::I32(v), R::Null) => v.push(None),
            (Output::Date32(v), R::Null) => v.push(None),
            (Output::I64(v), R::Null) => v.push(None),
            (Output::Timestamp(v), R::Null) => v.push(None),
            (Output::TimeMicroseconds(v), R::Null) => v.push(None),
            (Output::U8(v), R::Null) => v.push(None),
            (Output::U16(v), R::Null) => v.push(None),
            (Output::U32(v), R::Null) => v.push(None),
            (Output::U64(v), R::Null) => v.push(None),
            (Output::F32(v), R::Null) => v.push(None),
            (Output::F64(v), R::Null) => v.push(None),
            (Output::Decimal128(v), R::Null) => v.push(None),
            _ => {
                return Err(internal(
                    "cast body returned a foreign result representation",
                ));
            }
        }
        work.step()?;
    }
    work.flush()?;
    // Arrow construction is an opaque observed boundary, not internally cooperative allocation.
    let array: ArrayRef = match output {
        Output::Boolean(v) => Arc::new(BooleanArray::from(v)),
        Output::Text(v) => Arc::new(arrow::array::StringArray::from(v)),
        Output::I8(v) => Arc::new(Int8Array::from(v)),
        Output::I16(v) => Arc::new(Int16Array::from(v)),
        Output::I32(v) => Arc::new(Int32Array::from(v)),
        Output::Date32(v) => Arc::new(arrow::array::Date32Array::from(v)),
        Output::I64(v) => Arc::new(Int64Array::from(v)),
        Output::TimeMicroseconds(v) => Arc::new(arrow::array::Time64MicrosecondArray::from(v)),
        Output::Timestamp(v) => match ty {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
                Arc::new(arrow::array::TimestampSecondArray::from(v))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
                Arc::new(arrow::array::TimestampMillisecondArray::from(v))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
                Arc::new(arrow::array::TimestampMicrosecondArray::from(v))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
                Arc::new(arrow::array::TimestampNanosecondArray::from(v))
            }
            _ => {
                return Err(internal(
                    "timestamp cast has a foreign frozen result carrier",
                ));
            }
        },
        Output::U8(v) => Arc::new(arrow::array::UInt8Array::from(v)),
        Output::U16(v) => Arc::new(arrow::array::UInt16Array::from(v)),
        Output::U32(v) => Arc::new(arrow::array::UInt32Array::from(v)),
        Output::U64(v) => Arc::new(arrow::array::UInt64Array::from(v)),
        Output::F32(v) => Arc::new(Float32Array::from(v)),
        Output::F64(v) => Arc::new(Float64Array::from(v)),
        Output::Decimal128(v) => {
            let DataType::Decimal128(precision, scale) = *ty else {
                return Err(internal(
                    "Decimal128 cast output metadata differs from its frozen result",
                ));
            };
            Arc::new(
                Decimal128Array::from(v)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|_| {
                        internal(
                            "Decimal128 cast output differs from its frozen precision and scale",
                        )
                    })?,
            )
        }
    };
    work.flush()?;
    SelectedValues::try_new_observed(selection, ty, array, errors.into_boxed_slice(), || {
        work.step()
    })
}

// Pure values from ordered, separately evaluated source occurrences. Scheduling
// stays in Frame/evaluate_tree; no source definition or arena enters the math.
struct TemporalValues {
    seconds: Vec<Option<i64>>,
    normal: Option<(ArrayRef, Vec<usize>)>,
    result: Option<(ArrayRef, Vec<usize>)>,
}
fn temporal_math<T>(
    work: &mut Work<'_, '_>,
    body: impl FnOnce(
        &mut novarocks_functions::EvaluationCheckpoints<'_>,
    ) -> Result<
        T,
        novarocks_functions::builtin::calendar_time_text_shared::TimeTextError,
    >,
) -> Result<T, KernelFailure> {
    use novarocks_functions::builtin::calendar_time_text_shared::TimeTextError;
    work.flush()?;
    let mut scope = novarocks_functions::EvaluationCheckpoints::new(work.control);
    let value = body(&mut scope);
    // A primary callback refusal never reaches a second checkpoint.
    if let Err(TimeTextError::Kernel(cause)) = value {
        return Err(cause);
    }
    scope.finish()?;
    let value = value.map_err(|error| match error {
        TimeTextError::Kernel(cause) => cause,
        TimeTextError::Legacy(message) => internal(&message),
        TimeTextError::InvocationData(_) => {
            invalid("invocation data escaped its demanded temporal source phase")
        }
    })?;
    work.flush()?;
    Ok(value)
}
impl TemporalValues {
    fn new(rows: usize, work: &mut Work<'_, '_>) -> Result<Self, KernelFailure> {
        work.flush()?;
        let mut seconds = Vec::new();
        seconds
            .try_reserve_exact(rows)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for _ in 0..rows {
            seconds.push(None);
            work.step()?;
        }
        Ok(Self {
            seconds,
            normal: None,
            result: None,
        })
    }
    fn set_seconds(
        &mut self,
        ordinals: &[usize],
        values: &[Option<i64>],
        fill_null: bool,
        work: &mut Work<'_, '_>,
    ) -> Result<(), KernelFailure> {
        if ordinals.len() != values.len() {
            return Err(invalid("temporal dense source length differs"));
        }
        // Routing projects dense current values; the ONE original merge core
        // owns override/fill-NULL value decisions for both v1 and this host.
        let mut current = Vec::with_capacity(ordinals.len());
        for &parent in ordinals {
            work.step()?;
            current.push(
                *self
                    .seconds
                    .get(parent)
                    .ok_or_else(|| invalid("temporal source ordinal is outside its invocation"))?,
            );
        }
        temporal_math(work, |scope| {
            novarocks_functions::builtin::calendar_time_text_shared::merge_source(
                &mut current,
                values,
                if fill_null {
                    novarocks_functions::builtin::calendar_time_text_shared::MergeMode::FillNull
                } else {
                    novarocks_functions::builtin::calendar_time_text_shared::MergeMode::Override
                },
                scope,
            )
        })?;
        for (&parent, value) in ordinals.iter().zip(current) {
            self.seconds[parent] = value;
            work.step()?;
        }
        Ok(())
    }
    fn consume(
        &mut self,
        shape: novarocks_type_contract::TemporalSourceShape,
        stage: usize,
        domain: Selection<'_>,
        ordinals: &[usize],
        array: ArrayRef,
        work: &mut Work<'_, '_>,
    ) -> Result<Option<String>, KernelFailure> {
        use novarocks_functions::builtin::calendar_time_text_shared as math;
        use novarocks_type_contract::{TemporalSourceRole as R, TemporalSourceShape as S};
        if array.len() != domain.len() || domain.len() != ordinals.len() {
            return Err(invalid(
                "temporal source is not compact for its exact invocation",
            ));
        }
        let role = shape
            .roles()
            .get(stage)
            .copied()
            .flatten()
            .ok_or_else(|| invalid("unknown temporal source phase"))?;
        match role {
            R::RawOverride => {
                let strings = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        invalid("raw temporal override differs from its frozen Utf8 carrier")
                    })?;
                let values = temporal_math(work, |scope| math::clock_strings(strings, scope))?;
                self.set_seconds(ordinals, &values, false, work)?;
            }
            R::OriginalSeconds => {
                let values = temporal_math(work, |scope| math::sec_to_time_source(&array, scope))?;
                self.set_seconds(ordinals, &values, false, work)?;
            }
            R::Normal if shape.kind() == novarocks_type_contract::TemporalSourceKind::TimeToSec => {
                let values = temporal_math(work, |scope| {
                    if let Some(strings) =
                        array.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        math::duration_strings(strings, scope)
                    } else {
                        math::datetime_seconds(&array, scope)
                    }
                })?;
                self.set_seconds(ordinals, &values, false, work)?;
            }
            R::Normal => {
                if shape == S::FormatOrdinary {
                    self.normal = Some((array, ordinals.to_vec()));
                }
                // The UTF8 override branch still evaluates normal, but its type
                // and value do not enter the original formatter computation.
            }
            R::ImmediateCastSource => {
                if shape == S::SecondsCastString {
                    let strings = array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            invalid(
                                "immediate temporal source differs from its frozen Utf8 carrier",
                            )
                        })?;
                    let values =
                        temporal_math(work, |scope| math::duration_strings(strings, scope))?;
                    self.set_seconds(ordinals, &values, false, work)?;
                }
            }
            R::DeepestCastSource => {
                work.flush()?;
                let mut scope = novarocks_functions::EvaluationCheckpoints::new(work.control);
                let result = math::duration_cast_source(&array, &mut scope);
                if let Err(math::TimeTextError::Kernel(cause)) = result {
                    return Err(cause);
                }
                scope.finish()?;
                match result {
                    Ok(Some(values)) => self.set_seconds(ordinals, &values, true, work)?,
                    Ok(None) => {}
                    Err(math::TimeTextError::InvocationData(message)) => return Ok(Some(message)),
                    Err(math::TimeTextError::Legacy(message)) => return Err(invalid(&message)),
                    Err(math::TimeTextError::Kernel(cause)) => return Err(cause),
                }
                work.flush()?;
            }
            R::Format => {
                // Format admission precedes ordinary normal argument parsing,
                // preserving the original error/panic and evaluation order.
                let formats = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        invalid("temporal format differs from its frozen Utf8 carrier")
                    })?;
                let seconds = if shape == S::FormatOrdinary {
                    let (normal, normal_ordinals) = self
                        .normal
                        .take()
                        .ok_or_else(|| internal("missing evaluated temporal normal argument"))?;
                    let mut indices = Vec::with_capacity(ordinals.len());
                    for parent in ordinals {
                        let index = normal_ordinals
                            .binary_search(parent)
                            .map_err(|_| invalid("format demanded an absent normal source row"))?;
                        indices.push(Some(index as u64));
                        work.step()?;
                    }
                    let normal = gather(&normal, &indices, work)?;
                    temporal_math(work, |scope| math::format_argument(&normal, scope))?
                } else {
                    let mut seconds = Vec::with_capacity(ordinals.len());
                    for &parent in ordinals {
                        seconds.push(self.seconds[parent]);
                        work.step()?;
                    }
                    seconds
                };
                let result = temporal_math(work, |scope| {
                    math::format_output(&seconds, formats, ordinals.len(), scope)
                })?;
                self.result = Some((result, ordinals.to_vec()));
            }
        }
        Ok(None)
    }
    fn needs_fallback(
        &self,
        errors: &BTreeMap<usize, RowDataError>,
        domain: Selection<'_>,
        work: &mut Work<'_, '_>,
    ) -> Result<bool, KernelFailure> {
        use novarocks_functions::builtin::calendar_time_text_shared as math;
        if self.seconds.len() != domain.len() {
            return Err(invalid(
                "temporal guard differs from its current invocation domain",
            ));
        }
        let mut seconds = Vec::with_capacity(self.seconds.len());
        for (ordinal, value) in self.seconds.iter().enumerate() {
            if !errors.contains_key(&ordinal) {
                seconds.push(*value);
            }
            work.step()?;
        }
        temporal_math(work, |scope| math::any_null(&seconds, scope))
    }
    fn finish(
        mut self,
        shape: ControlShape,
        errors: &mut BTreeMap<usize, RowDataError>,
        domain: Selection<'_>,
        work: &mut Work<'_, '_>,
    ) -> Result<OwnedValue, KernelFailure> {
        use novarocks_functions::builtin::calendar_time_text_shared as math;
        let ControlShape::TemporalSource(shape) = shape else {
            return Err(invalid("temporal continuation has an ordinary shape"));
        };
        let output =
            if shape.kind() == novarocks_type_contract::TemporalSourceKind::TimeToSec {
                for &ordinal in errors.keys() {
                    self.seconds[ordinal] = None;
                    work.step()?;
                }
                temporal_math(work, |scope| math::seconds_output(self.seconds, scope))?
            } else if let Some((result, ordinals)) = self.result {
                let mut indices = Vec::with_capacity(domain.len());
                for parent in 0..domain.len() {
                    let index =
                        if errors.contains_key(&parent) {
                            None
                        } else {
                            Some(ordinals.binary_search(&parent).map_err(|_| {
                                internal("successful temporal format row has no result")
                            })? as u64)
                        };
                    indices.push(index);
                    work.step()?;
                }
                gather(&result, &indices, work)?
            } else if errors.len() == domain.len() {
                work.flush()?;
                let output = arrow::array::new_null_array(&DataType::Utf8, domain.len());
                work.flush()?;
                output
            } else {
                return Err(internal(
                    "temporal format completed without its demanded format source",
                ));
            };
        let mut row_errors = Vec::with_capacity(errors.len());
        for error in std::mem::take(errors).into_values() {
            row_errors.push(error);
            work.step()?;
        }
        Ok(OwnedValue::from_selected(SelectedValues::try_new_observed(
            domain,
            output.data_type(),
            Arc::clone(&output),
            row_errors.into_boxed_slice(),
            || work.step(),
        )?))
    }
}

#[cfg(test)]
#[path = "temporal_phase_tests.rs"]
mod temporal_phase_tests;

#[cfg(test)]
#[path = "between_comparison_control_tests.rs"]
mod between_comparison_control_tests;

fn in_event(
    event: novarocks_functions::native_inlist::InObservation,
    work: &mut Work<'_, '_>,
) -> Result<(), KernelFailure> {
    match event {
        novarocks_functions::native_inlist::InObservation::Step => work.step(),
        novarocks_functions::native_inlist::InObservation::OpaqueBoundary => work.flush(),
    }
}
fn in_observed<T>(
    result: Result<T, novarocks_functions::native_inlist::InError<KernelFailure>>,
) -> Result<T, KernelFailure> {
    match result {
        Ok(value) => Ok(value),
        Err(novarocks_functions::native_inlist::InError::Host(error)) => Err(error),
        // Exact signed recipes and actual carrier validation establish matching
        // concrete types/lengths before Arrow equality. An error here violates
        // that checked invariant; it is not a maskable SQL row-data result.
        Err(novarocks_functions::native_inlist::InError::Data(message)) => Err(internal(&message)),
    }
}
fn duplicate_child(child: &Child, work: &mut Work<'_, '_>) -> Result<Child, KernelFailure> {
    let mut ordinals = Vec::with_capacity(child.ordinals.len());
    for &ordinal in &child.ordinals {
        ordinals.push(ordinal);
        work.step()?;
    }
    work.flush()?;
    let value = match &child.value {
        OwnedValue::Constant(value) => OwnedValue::Constant(value.clone()),
        OwnedValue::Column(value) => OwnedValue::Column(Arc::clone(value)),
        OwnedValue::Selected(value, source_errors) => {
            let mut errors = Vec::with_capacity(source_errors.len());
            for error in source_errors.iter() {
                errors.push(error.clone());
                work.step()?;
            }
            work.flush()?;
            OwnedValue::Selected(Arc::clone(value), errors.into_boxed_slice())
        }
    };
    work.step()?;
    Ok(Child { ordinals, value })
}

/// RowDataError placeholders are results, not error-free kernel arguments.
/// Validate every successful selected row through the original COPY and exact
/// argument authors. The original error journal/phase value stays intact.
fn validate_membership_phase(
    output: &SelectedValues<'_>,
    selection: Selection<'_>,
    expected: &novarocks_type_contract::FunctionValueType,
    work: &mut Work<'_, '_>,
) -> Result<(), KernelFailure> {
    if output.errors().is_empty() {
        return novarocks_functions::validate_evaluated_argument_observed(
            EvaluatedArgument::SelectedColumn(output),
            selection,
            expected,
            work.control,
        );
    }
    if !output
        .selection()
        .same_rows_observed(selection, || work.step())?
    {
        return Err(invalid("IN phase result has a foreign selected domain"));
    }
    let mut errors = output.errors().iter().peekable();
    let mut rows = Vec::with_capacity(selection.len());
    let mut indices = Vec::with_capacity(selection.len());
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        if errors
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            errors.next();
        } else {
            rows.push(row);
            indices.push(Some(
                u64::try_from(ordinal).map_err(|_| KernelFailure::ResourceExhausted)?,
            ));
        }
    }
    let selected = Selection::try_sparse_observed(selection.batch_rows(), &rows, || work.step())?;
    let values = gather(output.values(), &indices, work)?;
    let successful = SelectedValues::try_new_observed(
        selected,
        &expected.data_type,
        values,
        Box::default(),
        || work.step(),
    )?;
    work.flush()?;
    novarocks_functions::validate_evaluated_argument_observed(
        EvaluatedArgument::SelectedColumn(&successful),
        selected,
        expected,
        work.control,
    )?;
    work.flush()?;
    drop(successful);
    work.flush()?;
    Ok(())
}
