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

//! Original binding loans and movable ID buffers for local composition.
//! Input layers borrow preceding stable owners; no layer is self-referential.
//! The original TypeViewBudget admits every buffer and source operation.

use super::type_sources::{
    BindingOccurrence, PackageTypeChannel as Channel, PackageTypeOccurrence, PackageTypeOwner,
};
use super::type_views::{
    PackageTypeViews, SourceInputPrefix, SourceInputReserve, TypeViewBudget, TypeViewError,
    TypeViewFacts, capture_source_input_reserve,
};
use crate::physical_aggregate_binding_v2::AggregateBindingInput;
use crate::physical_binding_v2::{
    ArgumentTypeIds, BindingSource, FunctionBindingInput, ResultTypeIds,
};
use novarocks_physical_plan as p;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, PureCompileControl};
use std::ops::Range;

#[derive(Clone, Copy, Debug)]
pub struct BindingSourceLimits {
    pub max_functions: usize,
    pub max_aggregates: usize,
    pub max_arguments: usize,
    pub max_lambda_parameters: usize,
    pub max_relation_results: usize,
}
fn add(a: usize, b: usize) -> Result<usize, TypeViewError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn twice(n: usize) -> Result<usize, TypeViewError> {
    n.checked_mul(2)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn id(n: usize) -> Result<u32, TypeViewError> {
    u32::try_from(n).map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn invalid(message: &'static str) -> TypeViewError {
    TypeViewError::InvalidSource(message)
}

pub(crate) struct FunctionRow<'source> {
    pub id: u32,
    pub occurrence: BindingOccurrence,
    pub source: BindingSource<'source>,
    arguments: Range<usize>,
    result: ResultRow,
}
enum ArgumentRow {
    Value(u32),
    Lambda {
        parameters: Range<usize>,
        result: u32,
    },
}
enum ResultRow {
    Scalar(u32),
    Relation(Range<usize>),
}
pub(crate) struct AggregateRow<'source> {
    pub id: u32,
    pub source: &'source p::AggregateBinding,
    pub function_id: u32,
    pub intermediate_id: u32,
}

/// Only original package objects are borrowed; owned buffers contain IDs,
/// ranges and loans, never references into these same movable buffers.
pub(crate) struct BindingSources<'source> {
    package: &'source p::FragmentPackage,
    control: &'source dyn PureCompileControl,
    floor: TypeViewFacts,
    functions: Vec<FunctionRow<'source>>,
    aggregates: Vec<AggregateRow<'source>>,
    arguments: Vec<ArgumentRow>,
    parameters: Vec<u32>,
    relation_results: Vec<u32>,
}

#[derive(Clone, Copy)]
enum Loan<'source> {
    Function(&'source novarocks_expression_contract::BoundFunction),
    Table(&'source p::BoundTableFunction),
    Aggregate(&'source p::AggregateBinding),
}
impl<'source> Loan<'source> {
    fn source(self) -> BindingSource<'source> {
        match self {
            Self::Function(function) => BindingSource::Scalar(function),
            Self::Table(function) => BindingSource::Table(function),
            Self::Aggregate(binding) => BindingSource::Scalar(&binding.function),
        }
    }
    fn arguments(self) -> &'source [p::FunctionArgumentType] {
        match self {
            Self::Function(function) => &function.argument_types,
            Self::Table(function) => &function.argument_types,
            Self::Aggregate(binding) => &binding.function.argument_types,
        }
    }
}

#[derive(Clone, Copy)]
struct Header {
    functions: usize,
    aggregates: usize,
    arguments: usize,
    results: usize,
    loop_work: usize,
}
trait BindingVisitor<'source> {
    fn header(
        &mut self,
        header: Header,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError>;
    fn capture(
        &mut self,
        occurrence: BindingOccurrence,
        loan: Loan<'source>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError>;
}

/// Both passes observe the same actual source traversal. Complete closed row
/// headers are captured before entering their argument/call loops; the Count
/// visitor admits the future buffers before the corresponding completed step.
fn visit_bindings<'source>(
    package: &'source p::FragmentPackage,
    budget: &mut TypeViewBudget<'source, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
    visitor: &mut impl BindingVisitor<'source>,
) -> Result<(), TypeViewError> {
    let fragment = package.fragment();
    budget.before_steps(twice(fragment.nodes().len())?)?;
    for (node_id, node) in fragment.nodes() {
        if let Some(unit) = &node.expressions {
            budget.before_steps(twice(unit.definitions().len())?)?;
            for expression in unit.definitions() {
                if let novarocks_expression_contract::ExprKind::FunctionCall { function, .. } =
                    &expression.kind
                {
                    visitor.header(
                        Header {
                            loop_work: 0,
                            functions: 1,
                            aggregates: 0,
                            arguments: function.argument_types.len(),
                            results: 0,
                        },
                        budget,
                        work,
                    )?;
                    visitor.capture(
                        BindingOccurrence::Scalar {
                            node: *node_id,
                            definition: expression.id,
                        },
                        Loan::Function(function),
                        budget,
                        work,
                    )?;
                }
                work.step()?;
            }
        }
        work.step()?;
    }
    budget.before_steps(twice(fragment.nodes().len())?)?;
    for (node_id, node) in fragment.nodes() {
        if let Some((_, calls)) = node.kind.aggregate_contract() {
            visitor.header(
                Header {
                    loop_work: twice(calls.len())?,
                    functions: calls.len(),
                    aggregates: calls.len(),
                    arguments: 0,
                    results: 0,
                },
                budget,
                work,
            )?;
            for (ordinal, call) in calls.iter().enumerate() {
                visitor.header(
                    Header {
                        loop_work: 0,
                        functions: 0,
                        aggregates: 0,
                        arguments: call.binding.function.argument_types.len(),
                        results: 0,
                    },
                    budget,
                    work,
                )?;
                visitor.capture(
                    BindingOccurrence::Aggregate {
                        node: *node_id,
                        ordinal,
                        call: call.id,
                    },
                    Loan::Aggregate(&call.binding),
                    budget,
                    work,
                )?;
                work.step()?;
            }
        }
        match &node.kind {
            p::NodeKind::Window(spec) => {
                visitor.header(
                    Header {
                        loop_work: twice(spec.calls.len())?,
                        functions: 0,
                        aggregates: 0,
                        arguments: 0,
                        results: 0,
                    },
                    budget,
                    work,
                )?;
                for (ordinal, call) in spec.calls.iter().enumerate() {
                    let aggregates = usize::from(call.aggregate_binding.is_some());
                    let aggregate_arguments = call
                        .aggregate_binding
                        .as_ref()
                        .map_or(0, |binding| binding.function.argument_types.len());
                    visitor.header(
                        Header {
                            loop_work: 0,
                            functions: add(1, aggregates)?,
                            aggregates,
                            arguments: add(call.binding.argument_types.len(), aggregate_arguments)?,
                            results: 0,
                        },
                        budget,
                        work,
                    )?;
                    visitor.capture(
                        BindingOccurrence::Window {
                            node: *node_id,
                            ordinal,
                        },
                        Loan::Function(&call.binding),
                        budget,
                        work,
                    )?;
                    if let Some(binding) = &call.aggregate_binding {
                        visitor.capture(
                            BindingOccurrence::WindowAggregate {
                                node: *node_id,
                                ordinal,
                            },
                            Loan::Aggregate(binding),
                            budget,
                            work,
                        )?;
                    }
                    work.step()?;
                }
            }
            p::NodeKind::TableFunction { function, .. } => {
                visitor.header(
                    Header {
                        loop_work: 0,
                        functions: 1,
                        aggregates: 0,
                        arguments: function.argument_types.len(),
                        results: function.result_types.len(),
                    },
                    budget,
                    work,
                )?;
                visitor.capture(
                    BindingOccurrence::Table(*node_id),
                    Loan::Table(function),
                    budget,
                    work,
                )?;
            }
            p::NodeKind::TableWriter { target } => {
                visitor.header(
                    Header {
                        loop_work: twice(target.partial_aggregates.len())?,
                        functions: target.partial_aggregates.len(),
                        aggregates: target.partial_aggregates.len(),
                        arguments: 0,
                        results: 0,
                    },
                    budget,
                    work,
                )?;
                for (ordinal, call) in target.partial_aggregates.iter().enumerate() {
                    visitor.header(
                        Header {
                            loop_work: 0,
                            functions: 0,
                            aggregates: 0,
                            arguments: call.binding.function.argument_types.len(),
                            results: 0,
                        },
                        budget,
                        work,
                    )?;
                    visitor.capture(
                        BindingOccurrence::WriterPartial {
                            node: *node_id,
                            ordinal,
                        },
                        Loan::Aggregate(&call.binding),
                        budget,
                        work,
                    )?;
                    work.step()?;
                }
            }
            p::NodeKind::TableFinish(finish) => {
                visitor.header(
                    Header {
                        loop_work: twice(finish.final_aggregates.len())?,
                        functions: finish.final_aggregates.len(),
                        aggregates: finish.final_aggregates.len(),
                        arguments: 0,
                        results: 0,
                    },
                    budget,
                    work,
                )?;
                for (ordinal, call) in finish.final_aggregates.iter().enumerate() {
                    visitor.header(
                        Header {
                            loop_work: 0,
                            functions: 0,
                            aggregates: 0,
                            arguments: call.binding.function.argument_types.len(),
                            results: 0,
                        },
                        budget,
                        work,
                    )?;
                    visitor.capture(
                        BindingOccurrence::FinishFinal {
                            node: *node_id,
                            ordinal,
                        },
                        Loan::Aggregate(&call.binding),
                        budget,
                        work,
                    )?;
                    work.step()?;
                }
            }
            _ => {}
        }
        work.step()?;
    }
    Ok(())
}

#[derive(Default)]
struct Counts {
    functions: usize,
    aggregates: usize,
    arguments: usize,
    parameters: usize,
    results: usize,
}
impl Counts {
    fn copy_work(&self) -> Result<usize, TypeViewError> {
        let writes = add(
            add(self.functions, self.aggregates)?,
            add(self.arguments, add(self.parameters, self.results)?)?,
        )?;
        add(twice(writes)?, 8)
    }
    fn check(&self, limits: BindingSourceLimits) -> Result<(), TypeViewError> {
        if self.functions > limits.max_functions
            || self.aggregates > limits.max_aggregates
            || self.arguments > limits.max_arguments
            || self.parameters > limits.max_lambda_parameters
            || self.results > limits.max_relation_results
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        // A count of MAX+1 admits IDs through MAX, not an absent sentinel.
        if self.functions != 0 {
            id(self.functions - 1)?;
        }
        if self.aggregates != 0 {
            id(self.aggregates - 1)?;
        }
        Ok(())
    }
}

struct CountPass<'buffers, 'source, 'control> {
    sources: &'buffers BindingSources<'source>,
    counts: Counts,
    limits: BindingSourceLimits,
    prefix: SourceInputPrefix<'source, 'control, 5>,
    copy_work: usize,
}
impl<'source> CountPass<'_, 'source, '_> {
    fn gate(
        &mut self,
        read_work: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        self.counts.check(self.limits)?;
        // Actual output moves and closed ID/loan cleanup are known with the
        // same header. Pay only their new contribution before observing it.
        let copy_work = self.counts.copy_work()?;
        let additional = copy_work
            .checked_sub(self.copy_work)
            .ok_or_else(|| invalid("binding source copy work decreased"))?;
        self.prefix.extend_with_work_in(
            self.sources.reserves(&self.counts)?,
            add(additional, read_work)?,
            budget,
            work,
        )?;
        self.copy_work = copy_work;
        Ok(())
    }
}
impl<'source> BindingVisitor<'source> for CountPass<'_, 'source, '_> {
    fn header(
        &mut self,
        header: Header,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        self.counts.functions = add(self.counts.functions, header.functions)?;
        self.counts.aggregates = add(self.counts.aggregates, header.aggregates)?;
        self.counts.arguments = add(self.counts.arguments, header.arguments)?;
        self.counts.results = add(self.counts.results, header.results)?;
        let read_work = add(
            add(header.arguments, twice(header.functions)?)?,
            add(4, header.loop_work)?,
        )?;
        self.gate(read_work, budget, work)?;
        work.step()?;
        Ok(())
    }
    fn capture(
        &mut self,
        _: BindingOccurrence,
        loan: Loan<'source>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        for argument in loan.arguments() {
            if let p::FunctionArgumentType::Lambda {
                parameter_types, ..
            } = argument
            {
                self.counts.parameters = add(self.counts.parameters, parameter_types.len())?;
                self.gate(0, budget, work)?;
            }
            // This is the actual argument-header read, not a synthetic loop
            // over a parameter count whose slice was not traversed here.
            work.step()?;
        }
        work.step()?;
        Ok(())
    }
}

struct FillPass<'buffers, 'types, 'source> {
    sources: &'buffers mut BindingSources<'source>,
    types: &'types PackageTypeViews<'source>,
}
impl<'source> BindingVisitor<'source> for FillPass<'_, '_, 'source> {
    fn header(
        &mut self,
        header: Header,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        budget.before_steps(add(4, header.loop_work)?)?;
        work.step()?;
        Ok(())
    }
    fn capture(
        &mut self,
        occurrence: BindingOccurrence,
        loan: Loan<'source>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        self.sources
            .capture(occurrence, loan, self.types, budget, work)
    }
}

pub(crate) fn collect_binding_sources_in<'source>(
    package: &'source p::FragmentPackage,
    types: &PackageTypeViews<'source>,
    limits: BindingSourceLimits,
    budget: &mut TypeViewBudget<'source, 'source, '_>,
    work: &mut CompileCheckpoints<'source>,
) -> Result<BindingSources<'source>, TypeViewError> {
    types.check_package_in(package, budget, work)?;
    let mut sources = BindingSources {
        package,
        control: work.control(),
        floor: budget.facts(),
        functions: Vec::new(),
        aggregates: Vec::new(),
        arguments: Vec::new(),
        parameters: Vec::new(),
        relation_results: Vec::new(),
    };
    let initial = sources.reserves(&Counts::default())?;
    let copy_work = Counts::default().copy_work()?;
    let prefix = SourceInputPrefix::new_with_work_in(initial, copy_work, budget, work)?;
    let mut count = CountPass {
        sources: &sources,
        counts: Counts::default(),
        limits,
        prefix,
        copy_work,
    };
    visit_bindings(package, budget, work, &mut count)?;
    let CountPass { counts, prefix, .. } = count;
    let requests = prefix.finish_in(budget, work)?;
    // The final sealed prefix already includes the buffers and their known
    // copy/cleanup work. Reserving consumes it without charging it again.
    budget.reserve_input_in(&mut sources.functions, requests[0], work)?;
    budget.reserve_input_in(&mut sources.aggregates, requests[1], work)?;
    budget.reserve_input_in(&mut sources.arguments, requests[2], work)?;
    budget.reserve_input_in(&mut sources.parameters, requests[3], work)?;
    budget.reserve_input_in(&mut sources.relation_results, requests[4], work)?;
    visit_bindings(
        package,
        budget,
        work,
        &mut FillPass {
            sources: &mut sources,
            types,
        },
    )?;
    if sources.functions.len() != counts.functions
        || sources.aggregates.len() != counts.aggregates
        || sources.arguments.len() != counts.arguments
        || sources.parameters.len() != counts.parameters
        || sources.relation_results.len() != counts.results
    {
        return Err(invalid("binding source headers changed during collection"));
    }
    work.step()?;
    sources.floor = budget.facts();
    Ok(sources)
}

impl<'source> BindingSources<'source> {
    fn reserves(&self, counts: &Counts) -> Result<[SourceInputReserve<'source>; 5], TypeViewError> {
        Ok([
            capture_source_input_reserve(&self.functions, counts.functions)?,
            capture_source_input_reserve(&self.aggregates, counts.aggregates)?,
            capture_source_input_reserve(&self.arguments, counts.arguments)?,
            capture_source_input_reserve(&self.parameters, counts.parameters)?,
            capture_source_input_reserve(&self.relation_results, counts.results)?,
        ])
    }
    pub(crate) fn functions(&self) -> &[FunctionRow<'source>] {
        &self.functions
    }
    /// Later definition inputs borrow this exact package and retained
    /// contribution before reading any binding occurrence or source loan.
    pub(crate) fn check_package_in(
        &self,
        package: &'source p::FragmentPackage,
        budget: &TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        if !std::ptr::eq(self.package, package) {
            return Err(invalid("binding source inputs belong to another package"));
        }
        self.check(budget, work)
    }
    fn check(
        &self,
        budget: &TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid("binding source inputs use another caller control"));
        }
        budget.check(self.package, work)?;
        // Later stages retain the same original collection contribution.
        let current = budget.facts();
        if current.allocation_requests_upper_bound < self.floor.allocation_requests_upper_bound
            || current.allocation_request_bytes_upper_bound
                < self.floor.allocation_request_bytes_upper_bound
            || current.coexisting_source_and_request_bytes_upper_bound
                < self.floor.coexisting_source_and_request_bytes_upper_bound
            || current.cumulative_work_upper_bound < self.floor.cumulative_work_upper_bound
        {
            return Err(invalid(
                "binding inputs omit their original collection contribution",
            ));
        }
        Ok(())
    }
    fn capture(
        &mut self,
        occurrence: BindingOccurrence,
        loan: Loan<'source>,
        types: &PackageTypeViews<'source>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        let owner = PackageTypeOwner::Binding(occurrence);
        let fragment = self.package.fragment().id();
        let root = |channel,
                    ty: &p::ValueType,
                    budget: &mut TypeViewBudget<'source, '_, '_>,
                    work: &mut CompileCheckpoints<'_>| {
            types.value_root_for_in(
                PackageTypeOccurrence {
                    fragment,
                    owner,
                    channel,
                },
                ty,
                budget,
                work,
            )
        };
        let argument_begin = self.arguments.len();
        for (argument, ty) in loan.arguments().iter().enumerate() {
            match ty {
                p::FunctionArgumentType::Value(ty) => self.arguments.push(ArgumentRow::Value(
                    root(Channel::Argument(argument), ty, budget, work)?,
                )),
                p::FunctionArgumentType::Lambda {
                    parameter_types,
                    result_type,
                } => {
                    let begin = self.parameters.len();
                    for (ordinal, ty) in parameter_types.iter().enumerate() {
                        self.parameters.push(root(
                            Channel::LambdaParameter { argument, ordinal },
                            ty,
                            budget,
                            work,
                        )?);
                        work.step()?;
                    }
                    let result = root(Channel::LambdaResult(argument), result_type, budget, work)?;
                    self.arguments.push(ArgumentRow::Lambda {
                        parameters: begin..self.parameters.len(),
                        result,
                    });
                }
            }
            work.step()?;
        }
        let function_id = id(self.functions.len())?;
        let result = match loan {
            Loan::Function(function) => ResultRow::Scalar(root(
                Channel::Result(0),
                &function.result_type,
                budget,
                work,
            )?),
            Loan::Aggregate(binding) => ResultRow::Scalar(root(
                Channel::Result(0),
                &binding.function.result_type,
                budget,
                work,
            )?),
            Loan::Table(table) => {
                let begin = self.relation_results.len();
                for (ordinal, ty) in table.result_types.iter().enumerate() {
                    self.relation_results
                        .push(root(Channel::Result(ordinal), ty, budget, work)?);
                    work.step()?;
                }
                ResultRow::Relation(begin..self.relation_results.len())
            }
        };
        self.functions.push(FunctionRow {
            id: function_id,
            occurrence,
            source: loan.source(),
            arguments: argument_begin..self.arguments.len(),
            result,
        });
        work.step()?;
        if let Loan::Aggregate(binding) = loan {
            let intermediate_id = root(
                Channel::Intermediate,
                &binding.intermediate_type,
                budget,
                work,
            )?;
            self.aggregates.push(AggregateRow {
                id: id(self.aggregates.len())?,
                source: binding,
                function_id,
                intermediate_id,
            });
            work.step()?;
        }
        Ok(())
    }
    pub(crate) fn argument_inputs_in<'views>(
        &'views self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedArgumentInputs<'views, 'source>, TypeViewError> {
        self.check(budget, work)?;
        let mut arguments = Vec::new();
        let mut requests = [capture_source_input_reserve(
            &arguments,
            self.arguments.len(),
        )?];
        budget.admit_input_reserves_in(&mut requests, add(twice(self.arguments.len())?, 4)?)?;
        budget.reserve_input_in(&mut arguments, requests[0], work)?;
        for row in &self.arguments {
            arguments.push(match row {
                ArgumentRow::Value(id) => ArgumentTypeIds::Value(*id),
                ArgumentRow::Lambda { parameters, result } => ArgumentTypeIds::Lambda {
                    parameters: &self.parameters[parameters.clone()],
                    result: *result,
                },
            });
            work.step()?;
        }
        Ok(PreparedArgumentInputs {
            sources: self,
            arguments,
            floor: budget.facts(),
        })
    }
    pub(crate) fn aggregate_inputs_in<'views>(
        &'views self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<AggregateBindingInput<'views>>, TypeViewError> {
        self.check(budget, work)?;
        let mut inputs = Vec::new();
        let mut requests = [capture_source_input_reserve(
            &inputs,
            self.aggregates.len(),
        )?];
        budget.admit_input_reserves_in(&mut requests, add(twice(self.aggregates.len())?, 4)?)?;
        budget.reserve_input_in(&mut inputs, requests[0], work)?;
        for row in &self.aggregates {
            inputs.push(AggregateBindingInput {
                id: row.id,
                source: row.source,
                function_binding_id: row.function_id,
                intermediate_value_type_id: row.intermediate_id,
            });
            work.step()?;
        }
        Ok(inputs)
    }
}

/// Second-layer buffer borrows only the stable first layer. Function inputs
/// must stay local and borrow this owner, never be stored back inside it.
pub(crate) struct PreparedArgumentInputs<'views, 'source> {
    sources: &'views BindingSources<'source>,
    arguments: Vec<ArgumentTypeIds<'views>>,
    floor: TypeViewFacts,
}
impl<'views, 'source> PreparedArgumentInputs<'views, 'source> {
    pub(crate) fn function_inputs_in<'inputs>(
        &'inputs self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<FunctionBindingInput<'inputs>>, TypeViewError> {
        self.sources.check(budget, work)?;
        let current = budget.facts();
        if current.allocation_requests_upper_bound < self.floor.allocation_requests_upper_bound
            || current.allocation_request_bytes_upper_bound
                < self.floor.allocation_request_bytes_upper_bound
            || current.coexisting_source_and_request_bytes_upper_bound
                < self.floor.coexisting_source_and_request_bytes_upper_bound
            || current.cumulative_work_upper_bound < self.floor.cumulative_work_upper_bound
        {
            return Err(invalid(
                "function inputs omit their argument buffer contribution",
            ));
        }
        let mut inputs = Vec::new();
        let mut requests = [capture_source_input_reserve(
            &inputs,
            self.sources.functions.len(),
        )?];
        budget.admit_input_reserves_in(
            &mut requests,
            add(twice(self.sources.functions.len())?, 4)?,
        )?;
        budget.reserve_input_in(&mut inputs, requests[0], work)?;
        for row in &self.sources.functions {
            let result = match &row.result {
                ResultRow::Scalar(id) => ResultTypeIds::Scalar(*id),
                ResultRow::Relation(range) => {
                    ResultTypeIds::Relation(&self.sources.relation_results[range.clone()])
                }
            };
            inputs.push(FunctionBindingInput {
                id: row.id,
                source: row.source,
                arguments: &self.arguments[row.arguments.clone()],
                result,
            });
            work.step()?;
        }
        Ok(inputs)
    }
}

#[cfg(test)]
mod tests;
