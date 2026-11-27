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

//! Original definition-source inputs for the whole sender's local DAG.
//! Movable rows own IDs/ranges; later layers borrow stable preceding buffers.
//! This source author neither rebuilds requests nor grants namespace authority.

use super::binding_sources::BindingSources;
use super::type_sources::{
    BindingOccurrence, CutDirection, PackageTypeChannel as Channel, PackageTypeOccurrence,
    PackageTypeOwner as Owner, RequestAddress,
};
use super::type_views::{
    PackageTypeViews, SourceInputPrefix, SourceInputReserve, TypeViewBudget, TypeViewError,
    TypeViewFacts, capture_source_input_reserve,
};
use crate::physical_binding_v2::{ArgumentTypeIds, BindingSource};
use crate::physical_call_requests_v2::{CallRequestTypeIds, UnitRequestTypeIds};
use crate::physical_constant_v2::ConstantRecordTypeIds;
use crate::physical_cuts_v2::CutsTypeIds;
use crate::physical_expression_v2::ExpressionTypeIds;
use crate::physical_table_write_nodes_v2::TableWriteTypeIds;
use crate::physical_value_v2::ValueSource;
use novarocks_physical_plan as p;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, PureCompileControl};
use std::ops::Range;

type Error = TypeViewError;
fn invalid(message: &'static str) -> Error {
    Error::InvalidSource(message)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn twice(n: usize) -> Result<usize, Error> {
    add(n, n)
}

#[derive(Clone, Copy, Debug)]
pub struct DefinitionSourceLimits {
    pub max_constants: usize,
    pub max_values: usize,
    pub max_expressions: usize,
    pub max_expression_lambda_parameters: usize,
    pub max_requests: usize,
    pub max_request_arguments: usize,
    pub max_request_lambda_parameters: usize,
    pub max_cut_type_occurrences: usize,
    pub max_result_fields: usize,
    pub max_writer_nodes: usize,
    pub max_writer_node_fields: usize,
}
struct ExpressionRow {
    node: p::NodeId,
    expression: p::ExprId,
    value: u32,
    parameters: Range<usize>,
    function: Option<u32>,
}
enum RequestArgumentRow {
    Value(u32),
    CandidateGroup {
        element: u32,
        members: Range<usize>,
    },
    Lambda {
        parameters: Range<usize>,
        result: u32,
    },
}
struct RequestRow {
    definition: RequestAddress,
    arguments: Range<usize>,
    expected: Option<u32>,
}
enum WriterNodeKind {
    Writer {
        target: Range<usize>,
        output: Range<usize>,
    },
    Finish {
        input: Range<usize>,
        output: Range<usize>,
    },
}
struct WriterNodeRow<'source> {
    source: &'source p::PhysicalNode,
    kind: WriterNodeKind,
}
/// All stored source references point into the original immutable Package.
/// ID/range storage may move; no row borrows storage owned by this object.
pub(crate) struct DefinitionSources<'source> {
    package: &'source p::FragmentPackage,
    control: &'source dyn PureCompileControl,
    floor: TypeViewFacts,
    constants: Vec<ConstantRecordTypeIds>,
    values: Vec<ValueSource<'source>>,
    expressions: Vec<ExpressionRow>,
    expression_parameters: Vec<u32>,
    requests: Vec<RequestRow>,
    arguments: Vec<RequestArgumentRow>,
    request_parameters: Vec<u32>,
    cuts: Vec<u32>,
    result: Vec<u32>,
    writers: Vec<WriterNodeRow<'source>>,
    writer_fields: Vec<u32>,
}
#[derive(Default)]
struct Counts {
    constants: usize,
    values: usize,
    expressions: usize,
    expression_parameters: usize,
    requests: usize,
    arguments: usize,
    request_parameters: usize,
    cuts: usize,
    result: usize,
    writers: usize,
    writer_fields: usize,
}
impl Counts {
    fn add(&mut self, other: Self) -> Result<(), Error> {
        macro_rules! sum { ($($field:ident),*) => { $(self.$field=add(self.$field,other.$field)?;)* }; }
        sum!(
            constants,
            values,
            expressions,
            expression_parameters,
            requests,
            arguments,
            request_parameters,
            cuts,
            result,
            writers,
            writer_fields
        );
        Ok(())
    }
    fn writes(&self) -> Result<usize, Error> {
        let mut n = 0;
        for count in [
            self.constants,
            self.values,
            self.expressions,
            self.expression_parameters,
            self.requests,
            self.arguments,
            self.request_parameters,
            self.cuts,
            self.result,
            self.writers,
            self.writer_fields,
        ] {
            n = add(n, count)?;
        }
        Ok(n)
    }
    fn copy_work(&self) -> Result<usize, Error> {
        add(twice(self.writes()?)?, 8)
    }
    fn check(&self, l: DefinitionSourceLimits) -> Result<(), Error> {
        for (actual, maximum) in [
            (self.constants, l.max_constants),
            (self.values, l.max_values),
            (self.expressions, l.max_expressions),
            (
                self.expression_parameters,
                l.max_expression_lambda_parameters,
            ),
            (self.requests, l.max_requests),
            (self.arguments, l.max_request_arguments),
            (self.request_parameters, l.max_request_lambda_parameters),
            (self.cuts, l.max_cut_type_occurrences),
            (self.result, l.max_result_fields),
            (self.writers, l.max_writer_nodes),
            (self.writer_fields, l.max_writer_node_fields),
        ] {
            if actual > maximum {
                return Err(CompileControlError::ResourceExhausted.into());
            }
        }
        Ok(())
    }
}
trait Visitor<'source> {
    fn header(
        &mut self,
        counts: Counts,
        read_work: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
    fn constant(
        &mut self,
        id: p::ConstantPoolId,
        pool: &'source p::ConstantPool,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
    fn value(
        &mut self,
        id: p::ValueId,
        value: &'source p::ValueDef,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
    fn expression(
        &mut self,
        node: p::NodeId,
        id: p::ExprId,
        expression: &'source novarocks_expression_contract::ExprNode,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
    fn request(
        &mut self,
        definition: RequestAddress,
        request: &'source novarocks_expression_contract::PhysicalCallRequest,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
    fn ty(
        &mut self,
        owner: Owner,
        channel: Channel,
        ty: &'source p::ValueType,
        destination: Destination,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
    fn writer(
        &mut self,
        id: p::NodeId,
        node: &'source p::PhysicalNode,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
}
#[derive(Clone, Copy)]
enum Destination {
    Cut,
    Result,
}
use novarocks_physical_plan::visit_fragment_definitions_observed as visit_fragment_definitions;
/// One original definition lent from an unsealed or packaged fragment.
/// The expression loan retains its exact kind, full value type and original
/// scalar binding. Window bindings are owned by relation nodes. A request retains its own definition identity,
/// logical argument count, constant references and optional result constraint.
/// No installed owner, execution use, inferred source or copied type is needed.
pub use novarocks_physical_plan::{FragmentDefinitionSource, visit_fragment_definitions_observed};

/// Only source-order/header enumeration lives here. Original codecs retain
/// expression, cut, request, Writer-schema and full-type validation grammars.
fn visit_request<'source>(
    visitor: &mut impl Visitor<'source>,
    definition: RequestAddress,
    request: &'source novarocks_expression_contract::PhysicalCallRequest,
    budget: &mut TypeViewBudget<'source, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    visitor.header(
        Counts {
            arguments: request.arguments.len(),
            ..Counts::default()
        },
        add(request.arguments.len(), 4)?,
        budget,
        work,
    )?;
    visitor.request(definition, request, budget, work)
}

fn visit<'source>(
    package: &'source p::FragmentPackage,
    budget: &mut TypeViewBudget<'source, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
    visitor: &mut impl Visitor<'source>,
) -> Result<(), Error> {
    let fragment = package.fragment();
    for (id, pool) in package.constants().entries() {
        visitor.constant(*id, pool, budget, work)?;
    }
    visit_fragment_definitions(fragment, work, |source, work| match source {
        FragmentDefinitionSource::Value { id, value } => visitor.value(id, value, budget, work),
        FragmentDefinitionSource::Expression {
            node,
            id,
            expression,
        } => {
            let parameters = match &expression.kind {
                novarocks_expression_contract::ExprKind::Lambda {
                    parameter_types, ..
                } => parameter_types.len(),
                _ => 0,
            };
            visitor.header(
                Counts {
                    expression_parameters: parameters,
                    ..Counts::default()
                },
                add(parameters, 4)?,
                budget,
                work,
            )?;
            visitor.expression(node, id, expression, budget, work)
        }
        FragmentDefinitionSource::ExpressionRequest {
            node,
            definition,
            request,
        } => visit_request(
            visitor,
            RequestAddress::Unit { node, definition },
            request,
            budget,
            work,
        ),
        FragmentDefinitionSource::Request {
            definition,
            request,
        } => visit_request(
            visitor,
            RequestAddress::Relation(definition),
            request,
            budget,
            work,
        ),
    })?;

    for (ordinal, cut) in package.cuts().inbound.iter().enumerate() {
        let fields = match &cut.payload {
            p::ExchangePayloadContract::WriterResults(r) => r.fields.len(),
            _ => 0,
        };
        visitor.header(
            Counts {
                cuts: add(cut.imports.len(), fields)?,
                ..Counts::default()
            },
            8,
            budget,
            work,
        )?;
        let owner = Owner::Cut {
            direction: CutDirection::Inbound,
            ordinal,
            edge: cut.edge,
        };
        for (ordinal, import) in cut.imports.iter().enumerate() {
            visitor.ty(
                owner,
                Channel::CutImport(ordinal),
                &import.source.ty,
                Destination::Cut,
                budget,
                work,
            )?;
        }
        if let p::ExchangePayloadContract::WriterResults(result) = &cut.payload {
            for (ordinal, field) in result.fields.iter().enumerate() {
                visitor.ty(
                    owner,
                    Channel::CutWriterResult(ordinal),
                    &field.ty,
                    Destination::Cut,
                    budget,
                    work,
                )?;
            }
        }
    }
    for (ordinal, cut) in package.cuts().outbound.iter().enumerate() {
        let fields = match &cut.payload {
            p::ExchangePayloadContract::WriterResults(r) => r.fields.len(),
            _ => 0,
        };
        let n = add(
            add(cut.projection.len(), cut.destination_imports.len())?,
            fields,
        )?;
        visitor.header(
            Counts {
                cuts: n,
                ..Counts::default()
            },
            10,
            budget,
            work,
        )?;
        let owner = Owner::Cut {
            direction: CutDirection::Outbound,
            ordinal,
            edge: cut.edge,
        };
        for (ordinal, value) in cut.projection.iter().enumerate() {
            visitor.ty(
                owner,
                Channel::CutProjection(ordinal),
                &value.ty,
                Destination::Cut,
                budget,
                work,
            )?;
        }
        for (ordinal, import) in cut.destination_imports.iter().enumerate() {
            visitor.ty(
                owner,
                Channel::CutDestinationImport(ordinal),
                &import.source.ty,
                Destination::Cut,
                budget,
                work,
            )?;
        }
        if let p::ExchangePayloadContract::WriterResults(result) = &cut.payload {
            for (ordinal, field) in result.fields.iter().enumerate() {
                visitor.ty(
                    owner,
                    Channel::CutWriterResult(ordinal),
                    &field.ty,
                    Destination::Cut,
                    budget,
                    work,
                )?;
            }
        }
    }
    visitor.header(
        Counts {
            cuts: package.cuts().runtime_filters.len(),
            ..Counts::default()
        },
        4,
        budget,
        work,
    )?;
    for (ordinal, filter) in package.cuts().runtime_filters.iter().enumerate() {
        let ty = match &filter.domain {
            p::RuntimeFilterDomain::Membership { ty, .. } => ty,
            p::RuntimeFilterDomain::Ordered { key, .. } => &key.ty,
        };
        visitor.ty(
            Owner::RuntimeFilter {
                ordinal,
                id: filter.id,
            },
            Channel::Value,
            ty,
            Destination::Cut,
            budget,
            work,
        )?;
    }
    if let Some(result) = package.result() {
        for (ordinal, field) in result.fields.iter().enumerate() {
            visitor.ty(
                Owner::Result,
                Channel::Field(ordinal),
                &field.ty,
                Destination::Result,
                budget,
                work,
            )?;
        }
    }
    // Even non-Writer nodes are actual inspected headers and are observed.
    for (id, node) in fragment.nodes() {
        let fields = match &node.kind {
            p::NodeKind::TableWriter { target } => Some(add(
                target.target_fields.len(),
                target.output_schema.fields.len(),
            )?),
            p::NodeKind::TableFinish(finish) => Some(add(
                finish.input_schema.fields.len(),
                finish.output_schema.fields.len(),
            )?),
            _ => None,
        };
        visitor.header(
            Counts {
                writers: usize::from(fields.is_some()),
                writer_fields: fields.unwrap_or(0),
                ..Counts::default()
            },
            add(fields.unwrap_or(0), 6)?,
            budget,
            work,
        )?;
        if fields.is_some() {
            visitor.writer(*id, node, budget, work)?;
        }
    }
    Ok(())
}
struct CountPass<'buffers, 'source, 'control> {
    sources: &'buffers DefinitionSources<'source>,
    counts: Counts,
    limits: Option<DefinitionSourceLimits>,
    prefix: SourceInputPrefix<'source, 'control, 11>,
    copy_work: usize,
}
impl<'source> CountPass<'_, 'source, '_> {
    fn gate(
        &mut self,
        read: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        if let Some(limits) = self.limits {
            self.counts.check(limits)?;
        }
        let copy = self.counts.copy_work()?;
        let delta = copy
            .checked_sub(self.copy_work)
            .ok_or_else(|| invalid("definition input work decreased"))?;
        self.prefix.extend_with_work_in(
            self.sources.reserves(&self.counts)?,
            add(delta, read)?,
            budget,
            work,
        )?;
        self.copy_work = copy;
        Ok(())
    }
    fn completed(
        &mut self,
        n: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.gate(n, budget, work)?;
        work.step()?;
        Ok(())
    }
}
impl<'source> Visitor<'source> for CountPass<'_, 'source, '_> {
    fn header(
        &mut self,
        n: Counts,
        read: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.counts.add(n)?;
        self.completed(read, budget, work)
    }
    fn constant(
        &mut self,
        _: p::ConstantPoolId,
        _: &'source p::ConstantPool,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.completed(4, budget, work)
    }
    fn value(
        &mut self,
        _: p::ValueId,
        _: &'source p::ValueDef,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.completed(4, budget, work)
    }
    fn expression(
        &mut self,
        _: p::NodeId,
        _: p::ExprId,
        e: &'source novarocks_expression_contract::ExprNode,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        if let novarocks_expression_contract::ExprKind::Lambda {
            parameter_types, ..
        } = &e.kind
        {
            for _ in parameter_types {
                self.completed(2, budget, work)?;
            }
        }
        self.completed(4, budget, work)
    }
    fn request(
        &mut self,
        _: RequestAddress,
        r: &'source novarocks_expression_contract::PhysicalCallRequest,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        for argument in &r.arguments {
            let repeated_types = match argument {
                p::StaticFunctionArgument::Lambda {
                    parameter_types, ..
                } => parameter_types.len(),
                p::StaticFunctionArgument::CandidateGroup { members, .. } => members.len(),
                p::StaticFunctionArgument::Value { .. } => 0,
            };
            if repeated_types > 0 {
                self.counts.request_parameters =
                    add(self.counts.request_parameters, repeated_types)?;
                self.gate(add(repeated_types, 4)?, budget, work)?;
                for _ in 0..repeated_types {
                    self.completed(2, budget, work)?;
                }
            }
            self.completed(4, budget, work)?;
        }
        self.completed(4, budget, work)
    }
    fn ty(
        &mut self,
        _: Owner,
        _: Channel,
        _: &'source p::ValueType,
        _: Destination,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.completed(4, budget, work)
    }
    fn writer(
        &mut self,
        _: p::NodeId,
        n: &'source p::PhysicalNode,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        match &n.kind {
            p::NodeKind::TableWriter { target } => {
                for field in &target.target_fields {
                    self.completed(2, budget, work)?;
                    if field.storage_type.is_some() {
                        self.counts.writer_fields = add(self.counts.writer_fields, 1)?;
                        self.completed(2, budget, work)?;
                    }
                }
                for _ in &target.output_schema.fields {
                    self.completed(2, budget, work)?;
                }
            }
            p::NodeKind::TableFinish(finish) => {
                for _ in &finish.input_schema.fields {
                    self.completed(2, budget, work)?;
                }
                for _ in &finish.output_schema.fields {
                    self.completed(2, budget, work)?;
                }
            }
            _ => return Err(invalid("definition Writer source kind changed")),
        }
        self.completed(4, budget, work)
    }
}
struct FillPass<'buffers, 'types, 'bindings, 'source> {
    sources: &'buffers mut DefinitionSources<'source>,
    types: &'types PackageTypeViews<'source>,
    bindings: &'bindings BindingSources<'source>,
}
impl<'source> FillPass<'_, '_, '_, 'source> {
    fn root(
        &self,
        owner: Owner,
        channel: Channel,
        ty: &p::ValueType,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        self.types.value_root_for_in(
            PackageTypeOccurrence {
                fragment: self.sources.package.fragment().id(),
                owner,
                channel,
            },
            ty,
            budget,
            work,
        )
    }
    fn function(
        &self,
        occurrence: BindingOccurrence,
        actual: &novarocks_expression_contract::BoundFunction,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        budget.before_steps(add(twice(self.bindings.functions().len())?, 4)?)?;
        let mut found = None;
        for row in self.bindings.functions() {
            let matches = row.occurrence == occurrence;
            if matches {
                if !matches!(row.source,BindingSource::Scalar(original) if std::ptr::eq(original,actual))
                {
                    return Err(invalid("expression binding uses another original function"));
                }
                if found.replace(row.id).is_some() {
                    return Err(invalid("expression function source is ambiguous"));
                }
            }
            work.step()?;
        }
        found.ok_or_else(|| invalid("expression function source is absent"))
    }
}
impl<'source> Visitor<'source> for FillPass<'_, '_, '_, 'source> {
    fn header(
        &mut self,
        _: Counts,
        read: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        budget.before_steps(read)?;
        work.step()?;
        Ok(())
    }
    fn constant(
        &mut self,
        id: p::ConstantPoolId,
        pool: &'source p::ConstantPool,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let owner = Owner::Constant(id);
        let value_type_id = self.root(owner, Channel::Value, pool.value_type(), budget, work)?;
        let field_id = self.types.source_field_root_for_in(
            PackageTypeOccurrence {
                fragment: self.sources.package.fragment().id(),
                owner,
                channel: Channel::ConstantField,
            },
            pool.field_ref(),
            budget,
            work,
        )?;
        self.sources.constants.push(ConstantRecordTypeIds {
            pool: id,
            value_type_id,
            field_id,
        });
        work.step()?;
        Ok(())
    }
    fn value(
        &mut self,
        id: p::ValueId,
        value: &'source p::ValueDef,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let value_type_id = self.root(Owner::Value(id), Channel::Value, &value.ty, budget, work)?;
        self.sources.values.push(ValueSource {
            source: value,
            value_type_id,
        });
        work.step()?;
        Ok(())
    }
    fn expression(
        &mut self,
        node: p::NodeId,
        id: p::ExprId,
        e: &'source novarocks_expression_contract::ExprNode,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let owner = Owner::Expression {
            node,
            definition: id,
        };
        let value = self.root(owner, Channel::Value, &e.ty, budget, work)?;
        let start = self.sources.expression_parameters.len();
        let function = match &e.kind {
            novarocks_expression_contract::ExprKind::FunctionCall { function, .. } => {
                Some(self.function(
                    BindingOccurrence::Scalar {
                        node,
                        definition: id,
                    },
                    function,
                    budget,
                    work,
                )?)
            }
            novarocks_expression_contract::ExprKind::Lambda {
                parameter_types, ..
            } => {
                for (ordinal, ty) in parameter_types.iter().enumerate() {
                    let root = self.root(
                        owner,
                        Channel::ExpressionLambdaParameter(ordinal),
                        ty,
                        budget,
                        work,
                    )?;
                    self.sources.expression_parameters.push(root);
                    work.step()?;
                }
                None
            }
            _ => None,
        };
        self.sources.expressions.push(ExpressionRow {
            node,
            expression: id,
            value,
            parameters: start..self.sources.expression_parameters.len(),
            function,
        });
        work.step()?;
        Ok(())
    }
    fn request(
        &mut self,
        definition: RequestAddress,
        r: &'source novarocks_expression_contract::PhysicalCallRequest,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let owner = Owner::Request(definition);
        let start = self.sources.arguments.len();
        for (argument, value) in r.arguments.iter().enumerate() {
            let row = match value {
                p::StaticFunctionArgument::Value { value_type, .. } => RequestArgumentRow::Value(
                    self.root(owner, Channel::Argument(argument), value_type, budget, work)?,
                ),
                p::StaticFunctionArgument::CandidateGroup {
                    element_type,
                    members,
                } => {
                    let element = self.root(
                        owner,
                        Channel::Argument(argument),
                        element_type,
                        budget,
                        work,
                    )?;
                    let start = self.sources.request_parameters.len();
                    for (ordinal, member) in members.iter().enumerate() {
                        let root = self.root(
                            owner,
                            Channel::CandidateMember { argument, ordinal },
                            &member.value_type,
                            budget,
                            work,
                        )?;
                        self.sources.request_parameters.push(root);
                        work.step()?;
                    }
                    RequestArgumentRow::CandidateGroup {
                        element,
                        members: start..self.sources.request_parameters.len(),
                    }
                }
                p::StaticFunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => {
                    let start = self.sources.request_parameters.len();
                    for (ordinal, ty) in parameter_types.iter().enumerate() {
                        let root = self.root(
                            owner,
                            Channel::LambdaParameter { argument, ordinal },
                            ty,
                            budget,
                            work,
                        )?;
                        self.sources.request_parameters.push(root);
                        work.step()?;
                    }
                    RequestArgumentRow::Lambda {
                        parameters: start..self.sources.request_parameters.len(),
                        result: self.root(
                            owner,
                            Channel::LambdaResult(argument),
                            result_type,
                            budget,
                            work,
                        )?,
                    }
                }
            };
            self.sources.arguments.push(row);
            work.step()?;
        }
        let expected = r
            .expected_result_type
            .as_ref()
            .map(|ty| self.root(owner, Channel::ExpectedResult, ty, budget, work))
            .transpose()?;
        self.sources.requests.push(RequestRow {
            definition,
            arguments: start..self.sources.arguments.len(),
            expected,
        });
        work.step()?;
        Ok(())
    }
    fn ty(
        &mut self,
        owner: Owner,
        channel: Channel,
        ty: &'source p::ValueType,
        d: Destination,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let root = self.root(owner, channel, ty, budget, work)?;
        match d {
            Destination::Cut => self.sources.cuts.push(root),
            Destination::Result => self.sources.result.push(root),
        }
        work.step()?;
        Ok(())
    }
    fn writer(
        &mut self,
        id: p::NodeId,
        n: &'source p::PhysicalNode,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let first = self.sources.writer_fields.len();
        let kind = match &n.kind {
            p::NodeKind::TableWriter { target } => {
                for (ordinal, field) in target.target_fields.iter().enumerate() {
                    let root = self.root(
                        Owner::WriterTarget(id),
                        Channel::Field(ordinal),
                        &field.ty,
                        budget,
                        work,
                    )?;
                    self.sources.writer_fields.push(root);
                    work.step()?;
                    if let Some(storage) = &field.storage_type {
                        let root = self.root(
                            Owner::WriterTarget(id),
                            Channel::StorageField(ordinal),
                            storage,
                            budget,
                            work,
                        )?;
                        self.sources.writer_fields.push(root);
                        work.step()?;
                    }
                }
                let middle = self.sources.writer_fields.len();
                for (ordinal, field) in target.output_schema.fields.iter().enumerate() {
                    let root = self.root(
                        Owner::WriterOutput(id),
                        Channel::Field(ordinal),
                        &field.ty,
                        budget,
                        work,
                    )?;
                    self.sources.writer_fields.push(root);
                    work.step()?;
                }
                WriterNodeKind::Writer {
                    target: first..middle,
                    output: middle..self.sources.writer_fields.len(),
                }
            }
            p::NodeKind::TableFinish(finish) => {
                for (ordinal, field) in finish.input_schema.fields.iter().enumerate() {
                    let root = self.root(
                        Owner::FinishInput(id),
                        Channel::Field(ordinal),
                        &field.ty,
                        budget,
                        work,
                    )?;
                    self.sources.writer_fields.push(root);
                    work.step()?;
                }
                let middle = self.sources.writer_fields.len();
                for (ordinal, field) in finish.output_schema.fields.iter().enumerate() {
                    let root = self.root(
                        Owner::FinishOutput(id),
                        Channel::Field(ordinal),
                        &field.ty,
                        budget,
                        work,
                    )?;
                    self.sources.writer_fields.push(root);
                    work.step()?;
                }
                WriterNodeKind::Finish {
                    input: first..middle,
                    output: middle..self.sources.writer_fields.len(),
                }
            }
            _ => return Err(invalid("definition Writer source kind changed")),
        };
        self.sources.writers.push(WriterNodeRow { source: n, kind });
        work.step()?;
        Ok(())
    }
}

pub(crate) fn collect_definition_sources_in<'source>(
    package: &'source p::FragmentPackage,
    types: &PackageTypeViews<'source>,
    bindings: &BindingSources<'source>,
    limits: impl Into<Option<DefinitionSourceLimits>>,
    budget: &mut TypeViewBudget<'source, 'source, '_>,
    work: &mut CompileCheckpoints<'source>,
) -> Result<DefinitionSources<'source>, Error> {
    types.check_package_in(package, budget, work)?;
    bindings.check_package_in(package, budget, work)?;
    let mut sources = DefinitionSources {
        package,
        control: work.control(),
        floor: budget.facts(),
        constants: Vec::new(),
        values: Vec::new(),
        expressions: Vec::new(),
        expression_parameters: Vec::new(),
        requests: Vec::new(),
        arguments: Vec::new(),
        request_parameters: Vec::new(),
        cuts: Vec::new(),
        result: Vec::new(),
        writers: Vec::new(),
        writer_fields: Vec::new(),
    };
    // These complete root row buffers are already known from closed headers.
    // Admit them together with initial read/copy work before any parent hook
    // or completed observation; no empty prefix precedes this captured batch.
    budget.before_steps(add(twice(package.fragment().nodes().len())?, 4)?)?;
    let mut expression_count = 0usize;
    let mut request_count = package.fragment().call_requests().entries().len();
    for node in package.fragment().nodes().values() {
        if let Some(unit) = &node.expressions {
            expression_count = add(expression_count, unit.definitions().len())?;
            request_count = add(request_count, unit.requests().len())?;
        }
        work.step()?;
    }
    let initial = Counts {
        constants: package.constants().entries().len(),
        values: package.fragment().values().len(),
        expressions: expression_count,
        requests: request_count,
        result: package.result().map_or(0, |r| r.fields.len()),
        ..Counts::default()
    };
    let limits = limits.into();
    if let Some(limits) = limits {
        initial.check(limits)?;
    }
    let copy_work = initial.copy_work()?;
    let prefix = SourceInputPrefix::new_with_work_in(
        sources.reserves(&initial)?,
        add(copy_work, 12)?,
        budget,
        work,
    )?;
    work.step()?;
    let mut count = CountPass {
        sources: &sources,
        counts: initial,
        limits,
        prefix,
        copy_work,
    };
    visit(package, budget, work, &mut count)?;
    let CountPass { counts, prefix, .. } = count;
    let requests = prefix.finish_in(budget, work)?;
    macro_rules! reserve { ($($n:literal => $field:ident),*) => { $(budget.reserve_input_in(&mut sources.$field,requests[$n],work)?;)* }; }
    reserve!(0=>constants,1=>values,2=>expressions,3=>expression_parameters,4=>requests,
        5=>arguments,6=>request_parameters,7=>cuts,8=>result,9=>writers,10=>writer_fields);
    budget.before_steps(12)?;
    work.step()?;
    visit(
        package,
        budget,
        work,
        &mut FillPass {
            sources: &mut sources,
            types,
            bindings,
        },
    )?;
    let lengths = [
        sources.constants.len(),
        sources.values.len(),
        sources.expressions.len(),
        sources.expression_parameters.len(),
        sources.requests.len(),
        sources.arguments.len(),
        sources.request_parameters.len(),
        sources.cuts.len(),
        sources.result.len(),
        sources.writers.len(),
        sources.writer_fields.len(),
    ];
    let expected = [
        counts.constants,
        counts.values,
        counts.expressions,
        counts.expression_parameters,
        counts.requests,
        counts.arguments,
        counts.request_parameters,
        counts.cuts,
        counts.result,
        counts.writers,
        counts.writer_fields,
    ];
    if lengths != expected {
        return Err(invalid(
            "definition source headers changed during collection",
        ));
    }
    budget.before_steps(12)?;
    work.step()?;
    sources.floor = budget.facts();
    Ok(sources)
}
impl<'source> DefinitionSources<'source> {
    fn reserves(&self, n: &Counts) -> Result<[SourceInputReserve<'source>; 11], Error> {
        Ok([
            capture_source_input_reserve(&self.constants, n.constants)?,
            capture_source_input_reserve(&self.values, n.values)?,
            capture_source_input_reserve(&self.expressions, n.expressions)?,
            capture_source_input_reserve(&self.expression_parameters, n.expression_parameters)?,
            capture_source_input_reserve(&self.requests, n.requests)?,
            capture_source_input_reserve(&self.arguments, n.arguments)?,
            capture_source_input_reserve(&self.request_parameters, n.request_parameters)?,
            capture_source_input_reserve(&self.cuts, n.cuts)?,
            capture_source_input_reserve(&self.result, n.result)?,
            capture_source_input_reserve(&self.writers, n.writers)?,
            capture_source_input_reserve(&self.writer_fields, n.writer_fields)?,
        ])
    }
    fn check(
        &self,
        budget: &TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        budget.check(self.package, work)?;
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid("definition inputs use another caller control"));
        }
        check_floor(budget.facts(), self.floor)
    }
    pub(crate) fn constant_type_ids(&self) -> &[ConstantRecordTypeIds] {
        &self.constants
    }
    pub(crate) fn values(&self) -> &[ValueSource<'source>] {
        &self.values
    }
    pub(crate) fn cuts_type_ids(&self) -> CutsTypeIds<'_> {
        CutsTypeIds::new(self.package.cuts(), &self.cuts)
    }
    pub(crate) fn result_type_ids(&self) -> &[u32] {
        &self.result
    }
    pub(crate) fn expressions_in<'rows>(
        &'rows self,
        node: p::NodeId,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<ExpressionTypeIds<'rows>>, Error> {
        self.check(budget, work)?;
        let rows = source_node_range_in(
            &self.expressions,
            Some(node),
            |row| Some(row.node),
            budget,
            work,
        )?;
        let count = rows.len();
        let mut inputs = Vec::new();
        let mut requests = [capture_source_input_reserve(&inputs, count)?];
        budget.admit_input_reserves_in(&mut requests, add(twice(count)?, 4)?)?;
        budget.reserve_input_in(&mut inputs, requests[0], work)?;
        for row in &self.expressions[rows] {
            inputs.push(ExpressionTypeIds {
                expr: row.expression,
                value_type_id: row.value,
                lambda_parameter_type_ids: &self.expression_parameters[row.parameters.clone()],
                function_binding_id: row.function,
            });
            work.step()?;
        }
        Ok(inputs)
    }
    pub(crate) fn request_arguments_in<'rows>(
        &'rows self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedRequestArguments<'rows, 'source>, Error> {
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
                RequestArgumentRow::Value(id) => ArgumentTypeIds::Value(*id),
                RequestArgumentRow::CandidateGroup { element, members } => {
                    ArgumentTypeIds::CandidateGroup {
                        element: *element,
                        members: &self.request_parameters[members.clone()],
                    }
                }
                RequestArgumentRow::Lambda { parameters, result } => ArgumentTypeIds::Lambda {
                    parameters: &self.request_parameters[parameters.clone()],
                    result: *result,
                },
            });
            work.step()?;
        }
        Ok(PreparedRequestArguments {
            sources: self,
            arguments,
            floor: budget.facts(),
        })
    }
    pub(crate) fn writer_type_ids_in<'rows>(
        &'rows self,
        node: &p::PhysicalNode,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<TableWriteTypeIds<'rows>>, Error> {
        self.check(budget, work)?;
        if !matches!(
            node.kind,
            p::NodeKind::TableWriter { .. } | p::NodeKind::TableFinish(_)
        ) {
            return Ok(None);
        }
        budget.before_steps(add(twice(self.writers.len())?, 4)?)?;
        let mut found = None;
        for row in &self.writers {
            if std::ptr::eq(row.source, node) {
                let value = match &row.kind {
                    WriterNodeKind::Writer { target, output } => TableWriteTypeIds::Writer {
                        target_fields: &self.writer_fields[target.clone()],
                        output_schema: &self.writer_fields[output.clone()],
                    },
                    WriterNodeKind::Finish { input, output } => TableWriteTypeIds::Finish {
                        input_schema: &self.writer_fields[input.clone()],
                        output_schema: &self.writer_fields[output.clone()],
                    },
                };
                if found.replace(value).is_some() {
                    return Err(invalid("Writer node source is ambiguous"));
                }
            }
            work.step()?;
        }
        found
            .map(Some)
            .ok_or_else(|| invalid("Writer node source belongs to another package"))
    }
}
fn check_floor(actual: TypeViewFacts, floor: TypeViewFacts) -> Result<(), Error> {
    if actual.allocation_requests_upper_bound < floor.allocation_requests_upper_bound
        || actual.allocation_request_bytes_upper_bound < floor.allocation_request_bytes_upper_bound
        || actual.coexisting_source_and_request_bytes_upper_bound
            < floor.coexisting_source_and_request_bytes_upper_bound
        || actual.cumulative_work_upper_bound < floor.cumulative_work_upper_bound
    {
        return Err(invalid(
            "definition inputs omit their original collection contribution",
        ));
    }
    Ok(())
}
/// Source-order rows are node-grouped by the physical definition visitor;
/// relation requests follow all units. Borrow their ranges without another
/// namespace or an O(nodes * definitions) projection scan.
fn source_node_range_in<T>(
    rows: &[T],
    target: Option<p::NodeId>,
    key: impl Fn(&T) -> Option<p::NodeId>,
    budget: &mut TypeViewBudget<'_, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Range<usize>, Error> {
    let levels = (usize::BITS - rows.len().leading_zeros()) as usize;
    budget.before_steps(add(
        levels
            .checked_mul(4)
            .ok_or(CompileControlError::ResourceExhausted)?,
        4,
    )?)?;
    let mut bound = |upper: bool| -> Result<usize, Error> {
        let (mut low, mut high) = (0, rows.len());
        while low < high {
            let middle = low + (high - low) / 2;
            let order = match (key(&rows[middle]), target) {
                (Some(actual), Some(target)) => actual.cmp(&target),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            work.step()?;
            if order.is_lt() || (upper && order.is_eq()) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    };
    let start = bound(false)?;
    let end = bound(true)?;
    Ok(start..end)
}

/// Local second layer borrows the stable first layer. Its final request rows
/// must remain local and cannot be stored back in either owning layer.
pub(crate) struct PreparedRequestArguments<'rows, 'source> {
    sources: &'rows DefinitionSources<'source>,
    arguments: Vec<ArgumentTypeIds<'rows>>,
    floor: TypeViewFacts,
}
impl<'rows, 'source> PreparedRequestArguments<'rows, 'source> {
    pub(crate) fn request_type_ids_in<'arguments>(
        &'arguments self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<CallRequestTypeIds<'arguments>>, Error> {
        self.sources.check(budget, work)?;
        check_floor(budget.facts(), self.floor)?;
        let rows = source_node_range_in(
            &self.sources.requests,
            None,
            |row| match row.definition {
                RequestAddress::Unit { node, .. } => Some(node),
                RequestAddress::Relation(_) => None,
            },
            budget,
            work,
        )?;
        let count = rows.len();
        let mut inputs = Vec::new();
        let mut reserves = [capture_source_input_reserve(&inputs, count)?];
        budget.admit_input_reserves_in(&mut reserves, add(twice(count)?, 4)?)?;
        budget.reserve_input_in(&mut inputs, reserves[0], work)?;
        for row in &self.sources.requests[rows] {
            if let RequestAddress::Relation(definition) = row.definition {
                inputs.push(CallRequestTypeIds {
                    definition,
                    arguments: &self.arguments[row.arguments.clone()],
                    expected_result_type: row.expected,
                });
            }
            work.step()?;
        }
        Ok(inputs)
    }
    pub(crate) fn unit_request_type_ids_in<'arguments>(
        &'arguments self,
        node: p::NodeId,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<UnitRequestTypeIds<'arguments>>, Error> {
        self.sources.check(budget, work)?;
        check_floor(budget.facts(), self.floor)?;
        let rows = source_node_range_in(
            &self.sources.requests,
            Some(node),
            |row| match row.definition {
                RequestAddress::Unit { node, .. } => Some(node),
                RequestAddress::Relation(_) => None,
            },
            budget,
            work,
        )?;
        let count = rows.len();
        let mut inputs = Vec::new();
        let mut reserves = [capture_source_input_reserve(&inputs, count)?];
        budget.admit_input_reserves_in(&mut reserves, add(twice(count)?, 4)?)?;
        budget.reserve_input_in(&mut inputs, reserves[0], work)?;
        for row in &self.sources.requests[rows] {
            if let RequestAddress::Unit {
                node: actual,
                definition,
            } = row.definition
                && actual == node
            {
                inputs.push(UnitRequestTypeIds {
                    definition,
                    arguments: &self.arguments[row.arguments.clone()],
                    expected_result_type: row.expected,
                });
            }
            work.step()?;
        }
        Ok(inputs)
    }
}

#[cfg(test)]
pub(super) mod tests;
