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

//! Actual provider-source loans and movable input buffers for composition.
//! Pointer aliases are shared only within the same original namespace/role.
//! Equal metadata never supplies another owner; no type or payload is cloned.

use super::type_sources::{PackageTypeChannel as Channel, PackageTypeOccurrence, PackageTypeOwner};
use super::type_views::{
    PackageTypeViews, SourceInputPrefix, SourceInputReserve, TypeViewBudget, TypeViewError,
    TypeViewFacts, capture_source_input_reserve,
};
use crate::physical_provider_binding_v2::ProviderBindingSource;
use crate::physical_public_source_schema_v2::SourceSchemaFactsSource;
use crate::physical_read_scan_v2::ReadScanSource;
use crate::physical_relation_v2::RelationSource;
use crate::physical_schema_v2::SchemaSource;
use crate::physical_type_v2::WriterTypeSource;
use crate::physical_writer_recipe_v2::WriterRecipeSource;
use arrow::datatypes::Schema;
use novarocks_connector_contract as c;
use novarocks_physical_plan as p;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, PureCompileControl};
use std::{fmt, ops::Range};

#[derive(Debug)]
pub enum ProviderSourceError {
    Control(CompileControlError),
    Source(TypeViewError),
    Connector(c::ConnectorError),
}
impl From<CompileControlError> for ProviderSourceError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<TypeViewError> for ProviderSourceError {
    fn from(value: TypeViewError) -> Self {
        match value {
            TypeViewError::Control(cause) => Self::Control(cause),
            other => Self::Source(other),
        }
    }
}
impl From<c::ConnectorError> for ProviderSourceError {
    fn from(value: c::ConnectorError) -> Self {
        Self::Connector(value)
    }
}
impl fmt::Display for ProviderSourceError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(out),
            Self::Source(e) => e.fmt(out),
            Self::Connector(e) => e.fmt(out),
        }
    }
}
impl std::error::Error for ProviderSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Source(source) => Some(source),
            Self::Connector(source) => Some(source),
        }
    }
}
type Error = ProviderSourceError;
fn invalid(message: &'static str) -> Error {
    TypeViewError::InvalidSource(message).into()
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn id(n: usize) -> Result<u32, Error> {
    u32::try_from(n).map_err(|_| CompileControlError::ResourceExhausted.into())
}

/// Caps count actual source occurrences, including aliases. Reserved capacity
/// remains the actual occurrence upper even when pointer aliases reduce len.
#[derive(Clone, Copy, Debug)]
pub struct ProviderSourceLimits {
    pub max_provider_occurrences: usize,
    pub max_payload_occurrences: usize,
    pub max_read_occurrences: usize,
    pub max_relation_occurrences: usize,
    pub max_schema_occurrences: usize,
    pub max_scan_occurrences: usize,
    pub max_writer_occurrences: usize,
    pub max_relation_fields: usize,
    pub max_schema_fields: usize,
    pub max_connector_expression_occurrences: usize,
}
struct RelationRow<'source> {
    id: u32,
    source: &'source p::Relation,
    types: Range<usize>,
}
struct SchemaRow<'source> {
    id: u32,
    source: &'source Schema,
    fields: Range<usize>,
}
struct ScanRow<'source> {
    node: p::NodeId,
    source: &'source c::FrozenConnectorRead,
    schema: u32,
    source_shape_schema: u32,
    source_facts: u32,
    expressions: Range<usize>,
}
struct WriterRow<'source, 'ids> {
    node: p::NodeId,
    source: &'source c::ConnectorWriteRecipeDraft,
    fields: &'ids [u32],
}

/// Only IDs/ranges and original outer-object loans are owned here. Writer IDs
/// borrow a preceding stable TypeViews input, not this object's own buffers.
pub(crate) struct ProviderSources<'source, 'ids> {
    package: &'source p::FragmentPackage,
    control: &'source dyn PureCompileControl,
    floor: TypeViewFacts,
    providers: Vec<(u32, ProviderBindingSource<'source>)>,
    payloads: Vec<(u32, &'source c::ConnectorEncodedPayload)>,
    reads: Vec<(u32, &'source p::ProviderReadReference)>,
    relations: Vec<RelationRow<'source>>,
    schemas: Vec<SchemaRow<'source>>,
    scans: Vec<ScanRow<'source>>,
    writers: Vec<WriterRow<'source, 'ids>>,
    relation_ids: Vec<u32>,
    field_ids: Vec<u32>,
    expression_ids: Vec<u32>,
}
#[derive(Default)]
struct Counts {
    providers: usize,
    payloads: usize,
    reads: usize,
    relations: usize,
    schemas: usize,
    scans: usize,
    writers: usize,
    relation_ids: usize,
    field_ids: usize,
    expression_ids: usize,
}
impl Counts {
    fn check(&self, limits: Option<ProviderSourceLimits>) -> Result<(), Error> {
        if let Some(limits) = limits
            && (self.providers > limits.max_provider_occurrences
                || self.payloads > limits.max_payload_occurrences
                || self.reads > limits.max_read_occurrences
                || self.relations > limits.max_relation_occurrences
                || self.schemas > limits.max_schema_occurrences
                || self.scans > limits.max_scan_occurrences
                || self.writers > limits.max_writer_occurrences
                || self.relation_ids > limits.max_relation_fields
                || self.field_ids > limits.max_schema_fields
                || self.expression_ids > limits.max_connector_expression_occurrences)
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        for n in [
            self.providers,
            self.payloads,
            self.reads,
            self.relations,
            self.schemas,
            self.expression_ids,
        ] {
            if n != 0 {
                id(n - 1)?;
            }
        }
        Ok(())
    }
    fn future_work(&self) -> Result<usize, Error> {
        let mut writes = 0;
        for n in [
            self.providers,
            self.payloads,
            self.reads,
            self.relations,
            self.schemas,
            self.scans,
            self.writers,
            self.relation_ids,
            self.field_ids,
            self.expression_ids,
        ] {
            writes = add(writes, n)?;
        }
        let mut work = add(mul(writes, 2)?, 8)?;
        // Closed pointer comparisons, not metadata equality or library hashes.
        // Each original loan scans at most the previously captured occurrences.
        for n in [
            self.providers,
            self.payloads,
            self.reads,
            self.relations,
            self.schemas,
        ] {
            work = add(work, mul(mul(n, n)?, 2)?)?;
        }
        Ok(work)
    }
}
#[derive(Clone, Copy)]
enum Event<'source> {
    Node,
    Value,
    Column(&'source c::ConnectorEncodedPayload),
    Relation {
        node: p::NodeId,
        source: &'source p::Relation,
        columns: &'source [(p::ProviderColumnReference, p::ValueId)],
    },
    Scan {
        node: p::NodeId,
        source: &'source c::FrozenConnectorRead,
    },
    Writer {
        node: p::NodeId,
        source: &'source c::ConnectorWriteRecipeDraft,
    },
    Layout(&'source c::ConnectorWriteLayoutRecipeDraft),
}
impl Event<'_> {
    fn header(self) -> Result<Counts, Error> {
        Ok(match self {
            Self::Node | Self::Value => Counts::default(),
            Self::Column(_) => Counts {
                payloads: 1,
                ..Counts::default()
            },
            Self::Relation {
                source, columns, ..
            } => Counts {
                providers: 1,
                payloads: add(add(2, source.schema().len())?, columns.len())?,
                reads: 1,
                relations: 1,
                relation_ids: source.schema().len(),
                ..Counts::default()
            },
            Self::Scan { source, .. } => Counts {
                providers: 1,
                payloads: add(2, source.scan().recipe().columns().len())?,
                schemas: 2,
                scans: 1,
                field_ids: add(
                    source
                        .public_facts()
                        .execution_schema()
                        .ok_or_else(|| invalid("frozen read has no complete execution schema"))?
                        .schema()
                        .fields()
                        .len(),
                    source
                        .public_facts()
                        .source_schema_facts()
                        .ok_or_else(|| {
                            invalid("frozen read has no independent source schema facts")
                        })?
                        .shape()
                        .schema()
                        .fields()
                        .len(),
                )?,
                expression_ids: usize::from(source.scan().remaining_expression().is_some()),
                ..Counts::default()
            },
            Self::Layout(_) => Counts {
                providers: 1,
                payloads: 1,
                ..Counts::default()
            },
            Self::Writer { .. } => Counts {
                providers: 1,
                payloads: 1,
                writers: 1,
                ..Counts::default()
            },
        })
    }
}
trait SourceVisitor<'source> {
    fn capture(
        &mut self,
        event: Event<'source>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>;
}
fn visit_sources<'source>(
    package: &'source p::FragmentPackage,
    visitor: &mut impl SourceVisitor<'source>,
    budget: &mut TypeViewBudget<'source, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    for (node, source) in package.fragment().nodes() {
        match &source.kind {
            p::NodeKind::Scan {
                relation,
                provider_outputs,
                ..
            } => visitor.capture(
                Event::Relation {
                    node: *node,
                    source: relation,
                    columns: provider_outputs,
                },
                budget,
                work,
            )?,
            p::NodeKind::TableWriter { target } => {
                visitor.capture(Event::Column(&target.handle), budget, work)?
            }
            _ => visitor.capture(Event::Node, budget, work)?,
        }
    }
    for source in package.fragment().values().values() {
        match &source.origin {
            p::ValueOrigin::ProviderField { field, .. } => {
                visitor.capture(Event::Column(&field.column_payload), budget, work)?
            }
            _ => visitor.capture(Event::Value, budget, work)?,
        }
    }
    for (node, source) in package.scans() {
        visitor.capture(
            Event::Scan {
                node: *node,
                source,
            },
            budget,
            work,
        )?;
    }
    for (node, source) in package.writes() {
        visitor.capture(
            Event::Writer {
                node: *node,
                source,
            },
            budget,
            work,
        )?;
    }
    for recipe in package.layouts().values() {
        visitor.capture(Event::Layout(recipe), budget, work)?;
    }
    Ok(())
}
struct CountPass<'buffers, 'source, 'control, 'ids> {
    sources: &'buffers ProviderSources<'source, 'ids>,
    counts: Counts,
    limits: Option<ProviderSourceLimits>,
    prefix: SourceInputPrefix<'source, 'control, 10>,
    paid_work: usize,
}
impl<'source> CountPass<'_, 'source, '_, '_> {
    fn gate(
        &mut self,
        read_work: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.counts.check(self.limits)?;
        let next = self.counts.future_work()?;
        let delta = next
            .checked_sub(self.paid_work)
            .ok_or_else(|| invalid("provider source future work decreased"))?;
        self.prefix.extend_with_work_in(
            self.sources.reserves(&self.counts)?,
            add(delta, read_work)?,
            budget,
            work,
        )?;
        self.paid_work = next;
        Ok(())
    }
}
impl<'source> SourceVisitor<'source> for CountPass<'_, 'source, '_, '_> {
    fn capture(
        &mut self,
        event: Event<'source>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let h = event.header()?;
        macro_rules! count { ($($field:ident),*) => { $(self.counts.$field = add(self.counts.$field, h.$field)?;)* }; }
        count!(
            providers,
            payloads,
            reads,
            relations,
            schemas,
            scans,
            writers,
            relation_ids,
            field_ids,
            expression_ids
        );
        self.gate(16, budget, work)?;
        work.step()?;
        if let Event::Scan { source, .. } = event
            && let Some(expression) = source.scan().remaining_expression()
        {
            expression.validate_observed::<Error>(|node| {
                // Only the captured shallow source arity is read here.
                // The original Connector walker alone traverses children
                // and checks type/count/depth laws. Root + all arities is
                // the exact preorder occurrence count for that tree.
                let children = match node {
                    c::ConnectorExpression::FieldDereference { .. } => 1,
                    c::ConnectorExpression::Call { arguments, .. } => arguments.len(),
                    c::ConnectorExpression::Constant { .. }
                    | c::ConnectorExpression::Variable { .. } => 0,
                };
                self.counts.expression_ids = add(self.counts.expression_ids, children)?;
                self.gate(4, budget, work)?;
                work.step()?;
                Ok(())
            })?;
        }
        Ok(())
    }
}

pub(crate) fn collect_provider_sources_in<'source, 'ids>(
    package: &'source p::FragmentPackage,
    types: &PackageTypeViews<'source>,
    writers: &'ids [WriterTypeSource<'ids>],
    limits: impl Into<Option<ProviderSourceLimits>>,
    budget: &mut TypeViewBudget<'source, 'source, '_>,
    work: &mut CompileCheckpoints<'source>,
) -> Result<ProviderSources<'source, 'ids>, Error> {
    types.check_package_in(package, budget, work)?;
    if writers.len() != package.writes().len() {
        return Err(invalid(
            "writer source input count differs from the original package",
        ));
    }
    let mut sources = ProviderSources {
        package,
        control: work.control(),
        floor: budget.facts(),
        providers: Vec::new(),
        payloads: Vec::new(),
        reads: Vec::new(),
        relations: Vec::new(),
        schemas: Vec::new(),
        scans: Vec::new(),
        writers: Vec::new(),
        relation_ids: Vec::new(),
        field_ids: Vec::new(),
        expression_ids: Vec::new(),
    };
    let empty = Counts::default();
    let paid_work = empty.future_work()?;
    // This external stable loan's complete matching bound is known before
    // observing any package source; keep it in the same initial gate.
    let writer_work = mul(mul(writers.len(), add(package.writes().len(), 1)?)?, 2)?;
    let prefix = SourceInputPrefix::new_with_work_in(
        sources.reserves(&empty)?,
        add(paid_work, writer_work)?,
        budget,
        work,
    )?;
    let mut count = CountPass {
        sources: &sources,
        counts: empty,
        limits: limits.into(),
        prefix,
        paid_work,
    };
    visit_sources(package, &mut count, budget, work)?;
    let CountPass { counts, prefix, .. } = count;
    let requests = prefix.finish_in(budget, work)?;
    macro_rules! reserve { ($($n:literal => $field:ident),*) => { $(budget.reserve_input_in(&mut sources.$field, requests[$n], work)?;)* }; }
    reserve!(0=>providers,1=>payloads,2=>reads,3=>relations,4=>schemas,5=>scans,6=>writers,7=>relation_ids,8=>field_ids,9=>expression_ids);
    let mut fill = FillPass {
        sources: &mut sources,
        types,
        writers,
    };
    visit_sources(package, &mut fill, budget, work)?;
    if sources.expression_ids.len() != counts.expression_ids
        || sources.scans.len() != counts.scans
        || sources.writers.len() != counts.writers
    {
        return Err(invalid("provider source headers changed during collection"));
    }
    work.step()?;
    sources.floor = budget.facts();
    Ok(sources)
}

impl<'source, 'ids> ProviderSources<'source, 'ids> {
    fn reserves(&self, n: &Counts) -> Result<[SourceInputReserve<'source>; 10], Error> {
        Ok([
            capture_source_input_reserve(&self.providers, n.providers)?,
            capture_source_input_reserve(&self.payloads, n.payloads)?,
            capture_source_input_reserve(&self.reads, n.reads)?,
            capture_source_input_reserve(&self.relations, n.relations)?,
            capture_source_input_reserve(&self.schemas, n.schemas)?,
            capture_source_input_reserve(&self.scans, n.scans)?,
            capture_source_input_reserve(&self.writers, n.writers)?,
            capture_source_input_reserve(&self.relation_ids, n.relation_ids)?,
            capture_source_input_reserve(&self.field_ids, n.field_ids)?,
            capture_source_input_reserve(&self.expression_ids, n.expression_ids)?,
        ])
    }
    pub(crate) fn provider_inputs(&self) -> &[(u32, ProviderBindingSource<'source>)] {
        &self.providers
    }
    pub(crate) fn payload_inputs(&self) -> &[(u32, &'source c::ConnectorEncodedPayload)] {
        &self.payloads
    }
    pub(crate) fn read_inputs(&self) -> &[(u32, &'source p::ProviderReadReference)] {
        &self.reads
    }
    fn check(
        &self,
        budget: &TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        budget.check(self.package, work)?;
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid("provider source inputs use another caller control"));
        }
        let current = budget.facts();
        if current.allocation_requests_upper_bound < self.floor.allocation_requests_upper_bound
            || current.allocation_request_bytes_upper_bound
                < self.floor.allocation_request_bytes_upper_bound
            || current.coexisting_source_and_request_bytes_upper_bound
                < self.floor.coexisting_source_and_request_bytes_upper_bound
            || current.cumulative_work_upper_bound < self.floor.cumulative_work_upper_bound
        {
            return Err(invalid(
                "provider source inputs omit their original contribution",
            ));
        }
        Ok(())
    }
    fn provider(
        &mut self,
        source: ProviderBindingSource<'source>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        for (id, original) in &self.providers {
            let same = match (original, source) {
                (ProviderBindingSource::Read(a), ProviderBindingSource::Read(b)) => {
                    std::ptr::eq(*a, b)
                }
                (ProviderBindingSource::Write(a), ProviderBindingSource::Write(b)) => {
                    std::ptr::eq(*a, b)
                }
                _ => false,
            };
            work.step()?;
            if same {
                return Ok(*id);
            }
        }
        let id = id(self.providers.len())?;
        self.providers.push((id, source));
        work.step()?;
        Ok(id)
    }
    fn payload(
        &mut self,
        source: &'source c::ConnectorEncodedPayload,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        for (id, original) in &self.payloads {
            let same = std::ptr::eq(*original, source);
            work.step()?;
            if same {
                return Ok(*id);
            }
        }
        let id = id(self.payloads.len())?;
        self.payloads.push((id, source));
        work.step()?;
        Ok(id)
    }
    fn read(
        &mut self,
        source: &'source p::ProviderReadReference,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        self.provider(ProviderBindingSource::Read(&source.binding), work)?;
        self.payload(source.relation.table(), work)?;
        self.payload(source.relation.view(), work)?;
        for (id, original) in &self.reads {
            let same = std::ptr::eq(*original, source);
            work.step()?;
            if same {
                return Ok(*id);
            }
        }
        let id = id(self.reads.len())?;
        self.reads.push((id, source));
        work.step()?;
        Ok(id)
    }
}
struct FillPass<'buffers, 'types, 'source, 'ids> {
    sources: &'buffers mut ProviderSources<'source, 'ids>,
    types: &'types PackageTypeViews<'source>,
    writers: &'ids [WriterTypeSource<'ids>],
}
impl<'source> SourceVisitor<'source> for FillPass<'_, '_, 'source, '_> {
    fn capture(
        &mut self,
        event: Event<'source>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        budget.before_steps(16)?;
        match event {
            Event::Node | Event::Value => {}
            Event::Column(payload) => {
                self.sources.payload(payload, work)?;
            }
            Event::Relation {
                node,
                source,
                columns,
            } => {
                self.sources.read(source.read(), work)?;
                for field in source.schema() {
                    self.sources.payload(&field.column.column_payload, work)?;
                }
                for (field, _) in columns {
                    self.sources.payload(&field.column_payload, work)?;
                }
                let mut found = false;
                for original in &self.sources.relations {
                    let same = std::ptr::eq(original.source, source);
                    work.step()?;
                    if same {
                        found = true;
                        break;
                    }
                }
                if !found {
                    let begin = self.sources.relation_ids.len();
                    for (ordinal, field) in source.schema().iter().enumerate() {
                        let id = self.types.value_root_for_in(
                            PackageTypeOccurrence {
                                fragment: self.sources.package.fragment().id(),
                                owner: PackageTypeOwner::Relation(node),
                                channel: Channel::Field(ordinal),
                            },
                            &field.ty,
                            budget,
                            work,
                        )?;
                        self.sources.relation_ids.push(id);
                        work.step()?;
                    }
                    let id = id(self.sources.relations.len())?;
                    self.sources.relations.push(RelationRow {
                        id,
                        source,
                        types: begin..self.sources.relation_ids.len(),
                    });
                    work.step()?;
                }
            }
            Event::Scan { node, source } => {
                let recipe = source.scan().recipe();
                self.sources
                    .provider(ProviderBindingSource::Read(recipe.binding()), work)?;
                self.sources.payload(recipe.relation().table(), work)?;
                self.sources.payload(recipe.relation().view(), work)?;
                for field in recipe.columns() {
                    self.sources.payload(field, work)?;
                }
                let public = source.public_facts();
                let execution = public
                    .execution_schema()
                    .ok_or_else(|| invalid("frozen read has no complete execution schema"))?;
                let source_facts = public
                    .source_schema_facts()
                    .ok_or_else(|| invalid("frozen read has no independent source schema facts"))?;
                // Each occurrence owns its exact role. Pointer-equal Arrow projections
                // can still have distinct complete value bindings or source authority.
                let schema_id = id(self.sources.schemas.len())?;
                let source_shape_schema_id = id(add(self.sources.schemas.len(), 1)?)?;
                for (schema, channel, schema_id) in [
                    (
                        execution.schema(),
                        Channel::Field as fn(usize) -> Channel,
                        schema_id,
                    ),
                    (
                        source_facts.shape().schema(),
                        Channel::ScanSourceField as fn(usize) -> Channel,
                        source_shape_schema_id,
                    ),
                ] {
                    let begin = self.sources.field_ids.len();
                    for (ordinal, field) in schema.fields().iter().enumerate() {
                        let field_id = self.types.field_root_for_in(
                            PackageTypeOccurrence {
                                fragment: self.sources.package.fragment().id(),
                                owner: PackageTypeOwner::Scan(node),
                                channel: channel(ordinal),
                            },
                            field,
                            budget,
                            work,
                        )?;
                        self.sources.field_ids.push(field_id);
                        work.step()?;
                    }
                    self.sources.schemas.push(SchemaRow {
                        id: schema_id,
                        source: schema,
                        fields: begin..self.sources.field_ids.len(),
                    });
                    work.step()?;
                }
                let begin = self.sources.expression_ids.len();
                if let Some(expression) = source.scan().remaining_expression() {
                    expression.validate_observed::<Error>(|_| {
                        let id = id(self.sources.expression_ids.len())?;
                        self.sources.expression_ids.push(id);
                        work.step()?;
                        Ok(())
                    })?;
                }
                self.sources.scans.push(ScanRow {
                    node,
                    source,
                    schema: schema_id,
                    source_shape_schema: source_shape_schema_id,
                    source_facts: id(self.sources.scans.len())?,
                    expressions: begin..self.sources.expression_ids.len(),
                });
                work.step()?;
            }
            Event::Layout(recipe) => {
                self.sources
                    .provider(ProviderBindingSource::Write(recipe.binding()), work)?;
                self.sources.payload(recipe.payload(), work)?;
            }
            Event::Writer { node, source } => {
                self.sources
                    .provider(ProviderBindingSource::Write(source.binding()), work)?;
                self.sources.payload(source.payload(), work)?;
                let mut found = None;
                for original in self.writers {
                    let same = std::ptr::eq(original.recipe(), source);
                    work.step()?;
                    if same && found.replace(original.field_ids()).is_some() {
                        return Err(invalid("writer type input source is ambiguous"));
                    }
                }
                let fields = found.ok_or_else(|| invalid("writer type input source is absent"))?;
                if fields.len() != source.input().field_count() {
                    return Err(invalid(
                        "writer type input field count differs from the original recipe",
                    ));
                }
                self.sources.writers.push(WriterRow {
                    node,
                    source,
                    fields,
                });
                work.step()?;
            }
        }
        work.step()?;
        Ok(())
    }
}

/// Layer two borrows stable flat buffers/Writer IDs; it never lives inside its
/// borrowed ProviderSources owner. All four Vec requests precede first copy.
pub(crate) struct PreparedProviderInputs<'rows> {
    relations: Vec<RelationSource<'rows>>,
    schemas: Vec<SchemaSource<'rows>>,
    scans: Vec<ReadScanSource<'rows>>,
    source_schemas: Vec<SourceSchemaFactsSource<'rows>>,
    writers: Vec<WriterRecipeSource<'rows>>,
}
impl PreparedProviderInputs<'_> {
    pub(crate) fn relations(&self) -> &[RelationSource<'_>] {
        &self.relations
    }
    pub(crate) fn schemas(&self) -> &[SchemaSource<'_>] {
        &self.schemas
    }
    pub(crate) fn source_schemas(&self) -> &[SourceSchemaFactsSource<'_>] {
        &self.source_schemas
    }
    pub(crate) fn scans(&self) -> &[ReadScanSource<'_>] {
        &self.scans
    }
    pub(crate) fn writers(&self) -> &[WriterRecipeSource<'_>] {
        &self.writers
    }
}
impl<'source, 'ids> ProviderSources<'source, 'ids> {
    pub(crate) fn prepare_inputs_in<'rows>(
        &'rows self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedProviderInputs<'rows>, Error> {
        self.check(budget, work)?;
        let mut out = PreparedProviderInputs {
            relations: Vec::new(),
            schemas: Vec::new(),
            scans: Vec::new(),
            source_schemas: Vec::new(),
            writers: Vec::new(),
        };
        let mut requests = [
            capture_source_input_reserve(&out.relations, self.relations.len())?,
            capture_source_input_reserve(&out.schemas, self.schemas.len())?,
            capture_source_input_reserve(&out.scans, self.scans.len())?,
            capture_source_input_reserve(&out.writers, self.writers.len())?,
            capture_source_input_reserve(&out.source_schemas, self.scans.len())?,
        ];
        let n = add(
            add(self.relations.len(), self.schemas.len())?,
            add(self.scans.len(), self.writers.len())?,
        )?;
        let n = add(n, self.scans.len())?;
        budget.admit_input_reserves_in(&mut requests, add(mul(n, 2)?, 8)?)?;
        budget.reserve_input_in(&mut out.relations, requests[0], work)?;
        budget.reserve_input_in(&mut out.schemas, requests[1], work)?;
        budget.reserve_input_in(&mut out.scans, requests[2], work)?;
        budget.reserve_input_in(&mut out.writers, requests[3], work)?;
        budget.reserve_input_in(&mut out.source_schemas, requests[4], work)?;
        for row in &self.relations {
            out.relations.push(RelationSource {
                id: row.id,
                relation: row.source,
                value_type_ids: &self.relation_ids[row.types.clone()],
            });
            work.step()?;
        }
        for row in &self.schemas {
            out.schemas.push(SchemaSource {
                id: row.id,
                source: row.source,
                field_ids: &self.field_ids[row.fields.clone()],
            });
            work.step()?;
        }
        for row in &self.scans {
            out.scans.push(ReadScanSource {
                node: row.node,
                read: row.source,
                schema_id: row.schema,
                source_schema_id: row.source_facts,
                expression_ids: &self.expression_ids[row.expressions.clone()],
            });
            work.step()?;
        }
        for row in &self.scans {
            out.source_schemas.push(SourceSchemaFactsSource {
                id: row.source_facts,
                source_shape_schema_id: row.source_shape_schema,
                facts: row
                    .source
                    .public_facts()
                    .source_schema_facts()
                    .ok_or_else(|| invalid("frozen read has no independent source schema facts"))?,
            });
            work.step()?;
        }
        for row in &self.writers {
            out.writers.push(WriterRecipeSource {
                node: row.node,
                recipe: row.source,
                field_ids: row.fields,
            });
            work.step()?;
        }
        Ok(out)
    }
}

#[cfg(test)]
pub(super) mod tests;
