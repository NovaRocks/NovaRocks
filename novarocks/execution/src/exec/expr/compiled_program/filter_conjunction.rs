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
//! One ordered Filter/Scan conjunct consumer over the actual original root sites.
//! The expression evaluator and BooleanRows remain their sole existing authors.
use super::boolean_region::BooleanRows;
use super::*;
use crate::runtime::fragment::{ExecutionResult, RequiredExpressionRowError};
use novarocks_local_program::{ProgramNodeExpressionRole, ProgramNodeKind};
use novarocks_type_contract::EvaluationDemand;

pub(crate) struct CompiledFilterConjunctionInstance {
    roots: Vec<CompiledExpressionInstance>,
    failed: bool,
}
impl CompiledFilterConjunctionInstance {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        Self::try_new_with_allocator(program, node, control, None)
    }
    pub(crate) fn try_new_with_allocator(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        control: &dyn KernelEvaluationControl,
        allocator: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    ) -> Result<Self, KernelFailure> {
        let observed = ObservedControl {
            original: control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work = Work {
            control: &observed,
            pending: 0,
            scalar_scope: None,
        };
        let result = (|| {
            work.control.checkpoint(0)?;
            let graph_node = program
                .graph()
                .nodes()
                .get(node.index())
                .ok_or_else(|| invalid("Filter conjunction node is absent"))?;
            let (predicates, scan): (&[novarocks_local_program::ProgramExprId], bool) =
                match graph_node.kind() {
                    ProgramNodeKind::Filter { predicates, .. } => (predicates.as_ref(), false),
                    ProgramNodeKind::Scan { residuals, .. } => (residuals.as_slice(), true),
                    _ => {
                        return Err(invalid(
                            "Filter conjunction requires its actual Filter or Scan node",
                        ));
                    }
                };
            if predicates.is_empty() {
                return Err(invalid("Filter conjunction has no original predicate"));
            }
            let mut roots = Vec::with_capacity(predicates.len());
            let mut actual_input = None;
            for (ordinal, definition) in predicates.iter().enumerate() {
                let ordinal =
                    u32::try_from(ordinal).map_err(|_| KernelFailure::ResourceExhausted)?;
                let site = ProgramExpressionRootSite::Node {
                    node,
                    role: if scan {
                        ProgramNodeExpressionRole::ScanResidual { predicate: ordinal }
                    } else {
                        ProgramNodeExpressionRole::FilterPredicate { predicate: ordinal }
                    },
                };
                let snapshot = program
                    .checked()
                    .channels()
                    .expressions()
                    .resolved_calls()
                    .snapshot();
                let use_id = *snapshot
                    .bindings()
                    .get(&site)
                    .ok_or_else(|| invalid("missing original Filter predicate root"))?;
                let flow = &snapshot.flows()[&site.arena()];
                let invocation = flow
                    .uses()
                    .get(&use_id)
                    .ok_or_else(|| invalid("missing original Filter predicate use"))?;
                if invocation.definition != *definition
                    || invocation.context.demand != EvaluationDemand::TruthOnly
                {
                    return Err(invalid(
                        "Filter predicate differs from its original definition or demand",
                    ));
                }
                let FunctionArgumentType::Value(ty) = program
                    .checked()
                    .channels()
                    .expressions()
                    .definition_type(site.arena(), *definition)
                    .ok_or_else(|| invalid("missing Filter predicate full type"))?
                else {
                    return Err(invalid("Filter predicate is not a value"));
                };
                if ty.data_type != DataType::Boolean
                    || ty.logical_type != novarocks_type_contract::ValueLogicalType::Physical
                {
                    return Err(invalid(
                        "Filter predicate is not its original physical Boolean",
                    ));
                }
                let instance = CompiledExpressionInstance::try_new_with_allocator(
                    Arc::clone(&program),
                    site,
                    work.control,
                    allocator.clone(),
                )?;
                if let Some(previous) = actual_input {
                    if instance.input != Some(previous) {
                        return Err(invalid(
                            "Filter predicates read different original input ports",
                        ));
                    }
                } else {
                    actual_input = instance.input;
                    if actual_input.is_none() {
                        return Err(invalid("Filter predicate has no original input port"));
                    }
                }
                roots.push(instance);
                work.step()?;
            }
            Ok(Self {
                roots,
                failed: false,
            })
        })();
        work.finish(result)
    }
    #[cfg(test)]
    pub(crate) fn constructed_root_call_instances(&self, predicate: usize) -> usize {
        self.roots[predicate].instances.len()
    }
    fn root_is_pure(root: &CompiledExpressionInstance) -> Result<bool, KernelFailure> {
        let snapshot = root
            .program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        let use_id = *snapshot
            .bindings()
            .get(&root.root)
            .ok_or_else(|| invalid("missing actual Filter root effects site"))?;
        let flow = &snapshot.flows()[&root.root.arena()];
        let invocation = &flow.uses()[&use_id];
        root.effects
            .get(&ProgramUseRef {
                arena: root.root.arena(),
                use_id,
            })
            .ok_or_else(|| invalid("missing actual Filter root effects"))?
            .for_use(invocation.context)
            .map(|effects| effects.permits_boolean_reordering())
            .map_err(|_| invalid("Filter root effects differ from their original context"))
    }
    fn evaluate_once<'a, E: scalar_invocation::FrameFailure>(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'a>,
        work: &mut Work<'_, '_>,
    ) -> Result<(SelectedValues<'a>, Box<[ProgramExpressionRootSite]>), E> {
        if input.num_rows() != selection.batch_rows() {
            return Err(invalid("Filter selection differs from its actual batch").into());
        }
        let mut boolean = BooleanRows::new(selection.len(), work)?;
        let mut terminal = BTreeMap::new();
        let mut origins = BTreeMap::new();
        let mut remaining = Vec::with_capacity(selection.len());
        for ordinal in 0..selection.len() {
            remaining.push(ordinal);
            work.step()?;
        }
        for root in &mut self.roots {
            let pure = Self::root_is_pure(root)?;
            work.step()?;
            if !pure {
                boolean.boundary(&mut terminal, &mut remaining, work)?;
            }
            if remaining.is_empty() {
                if selection.is_empty() {
                    // A zero-row invocation validates the entire original port,
                    // but the existing Frame enters no child/kernel constructor.
                    let output = root.evaluate_controlled::<E>(
                        input,
                        selection,
                        novarocks_functions::ScalarInvocationActivation::ValidateOnly,
                        work.control,
                        work.scalar_scope.as_mut().map(|scope| &mut **scope as &mut dyn crate::runtime::scalar_memory::RuntimeScalarOperationScope),
                    )?;
                    work.flush()?;
                    drop(output);
                    work.flush()?;
                    continue;
                }
                break;
            }
            let mut batch_rows = Vec::with_capacity(remaining.len());
            for &parent in &remaining {
                batch_rows.push(
                    selection
                        .row(parent)
                        .ok_or_else(|| internal("Filter surviving ordinal has no batch row"))?,
                );
                work.step()?;
            }
            let actual_selection =
                Selection::try_sparse_observed(input.num_rows(), &batch_rows, || work.step())?;
            let output = root.evaluate_controlled::<E>(
                input,
                actual_selection,
                novarocks_functions::ScalarInvocationActivation::Activated,
                work.control,
                work.scalar_scope.as_mut().map(|scope| {
                    &mut **scope
                        as &mut dyn crate::runtime::scalar_memory::RuntimeScalarOperationScope
                }),
            )?;
            for error in output.errors() {
                let parent = *remaining.get(error.selected_ordinal()).ok_or_else(|| {
                    internal("Filter child error ordinal is outside its actual selection")
                })?;
                origins.entry(parent).or_insert(root.root);
                work.step()?;
            }
            remaining = boolean.consume(
                &output,
                &remaining,
                ControlShape::Conjunction,
                EvaluationDemand::TruthOnly,
                pure,
                &mut terminal,
                work,
            )?;
            work.flush()?;
            drop(output);
            work.flush()?;
        }
        let (values, errors) = boolean.finish(
            ControlShape::Conjunction,
            EvaluationDemand::TruthOnly,
            terminal,
            work,
        )?;
        let mut sites = Vec::with_capacity(errors.len());
        for error in errors.iter() {
            sites.push(
                *origins.get(&error.selected_ordinal()).ok_or_else(|| {
                    internal("required Filter error lost its original source site")
                })?,
            );
            work.step()?;
        }
        let selected = SelectedValues::try_new(selection, &DataType::Boolean, values, errors)
            .map_err(|_| internal("Filter conjunction produced invalid selected values"))?;
        Ok((selected, sites.into_boxed_slice()))
    }
    /// Public error publication occurs only after the complete legal region.
    /// It preserves the original predicate site and actual batch-row mapping.
    #[cfg(test)]
    pub(crate) fn evaluate_required(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<ArrayRef> {
        self.evaluate_required_controlled::<novarocks_functions::ScalarInvocationFailure>(
            input, selection, control, None,
        )
    }
    pub(crate) fn evaluate_required_runtime(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'_>,
        control: &dyn KernelEvaluationControl,
        scope: &mut dyn crate::runtime::scalar_memory::RuntimeScalarOperationScope,
    ) -> ExecutionResult<ArrayRef> {
        self.evaluate_required_controlled::<crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure>(input,selection,control,Some(scope))
    }
    fn evaluate_required_controlled<
        E: scalar_invocation::FrameFailure + Into<crate::runtime::fragment::ExecutionFailure>,
    >(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'_>,
        control: &dyn KernelEvaluationControl,
        scope: Option<&mut dyn crate::runtime::scalar_memory::RuntimeScalarOperationScope>,
    ) -> ExecutionResult<ArrayRef> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let observed = ObservedControl {
            original: control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work = Work {
            control: &observed,
            pending: 0,
            scalar_scope: scope,
        };
        let result = (|| {
            work.control.checkpoint(0)?;
            self.evaluate_once::<E>(input, selection, &mut work)
        })();
        let result = E::finish(&mut work, result);
        let result = result.map_err(Into::into).and_then(|(selected, sites)| {
            let (selection, values, errors) = selected.into_parts();
            if let Some(error) = errors.into_vec().into_iter().next() {
                let site = *sites
                    .first()
                    .ok_or_else(|| internal("required Filter error has no source site"))?;
                return Err(RequiredExpressionRowError::try_new(site, selection, error)?.into());
            }
            Ok(values)
        });
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
