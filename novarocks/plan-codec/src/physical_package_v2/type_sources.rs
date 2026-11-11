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

//! Original checked-package type occurrences for the sender composer.
//! This visitor borrows owners without validating, cloning, interning, or
//! assigning IDs. Admission, type laws, source invoices and scopes belong to
//! the caller. Repeated stored occurrences remain repeated callbacks.

use crate::physical_type_v2::WriterTypeRootRole;
#[cfg(test)]
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use novarocks_connector_contract::{
    ConnectorWriteFieldBinding, ConnectorWriteInputShape, ConnectorWriteRecipeDraft,
};
use novarocks_physical_plan as p;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BindingOccurrence {
    Scalar(p::ExprId),
    Window(p::ExprId),
    WindowAggregate(p::ExprId),
    Aggregate {
        node: p::NodeId,
        ordinal: usize,
        call: p::AggregateCallId,
    },
    Table(p::NodeId),
    WriterPartial {
        node: p::NodeId,
        ordinal: usize,
    },
    FinishFinal {
        node: p::NodeId,
        ordinal: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CutDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PackageTypeOwner {
    Value(p::ValueId),
    SystemRelation(p::NodeId),
    Expression(p::ExprId),
    Binding(BindingOccurrence),
    Relation(p::NodeId),
    WriterTarget(p::NodeId),
    WriterOutput(p::NodeId),
    FinishInput(p::NodeId),
    FinishOutput(p::NodeId),
    Cut {
        direction: CutDirection,
        ordinal: usize,
        edge: p::EdgeId,
    },
    RuntimeFilter {
        ordinal: usize,
        id: p::RuntimeFilterId,
    },
    Constant(p::ConstantPoolId),
    Request(p::PhysicalCallDefinition),
    Result,
    Scan(p::NodeId),
    Recipe(p::NodeId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PackageTypeChannel {
    Value,
    Result(usize),
    Argument(usize),
    LambdaParameter {
        argument: usize,
        ordinal: usize,
    },
    LambdaResult(usize),
    ExpressionLambdaParameter(usize),
    CastTarget,
    Intermediate,
    Field(usize),
    ScanExecutionValue(usize),
    ScanSourceField(usize),
    CutImport(usize),
    CutProjection(usize),
    CutDestinationImport(usize),
    CutWriterResult(usize),
    ExpectedResult,
    ConstantField,
    WriterField {
        role: WriterTypeRootRole,
        ordinal: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PackageTypeOccurrence {
    pub fragment: p::FragmentId,
    pub owner: PackageTypeOwner,
    pub channel: PackageTypeChannel,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PackageTypeSource<'source> {
    Value(&'source p::ValueType),
    MissingReadSchema,
    /// Cast contributes a carrier-only occurrence without inventing logical flags.
    Carrier,
    StrictField(&'source Arc<Field>),
    SourceField(&'source Arc<Field>),
    ExecutionField {
        field: &'source Arc<Field>,
        value: &'source p::ValueType,
    },
    /// The original immutable draft owns this inline field and writer law.
    WriterField {
        recipe: &'source ConnectorWriteRecipeDraft,
        role: WriterTypeRootRole,
        ordinal: usize,
        binding: &'source ConnectorWriteFieldBinding,
    },
}

#[cfg(test)]
type Visitor<'a, 'b, E> = dyn FnMut(
        PackageTypeOccurrence,
        PackageTypeSource<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>
    + 'b;

/// Numerical admission of a captured root and each actual visitor step.
/// Implementations retain caller ownership; this port adds no entry/footer.
pub(crate) trait PackageTypeSourceSink<'source, E> {
    fn capture(
        &mut self,
        occurrence: PackageTypeOccurrence,
        source: PackageTypeSource<'source>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>;
    fn before_completed(&mut self) -> Result<(), E>;
}
#[cfg(test)]
struct CallbackSink<'a, 'b, E> {
    visitor: &'b mut Visitor<'a, 'b, E>,
}
#[cfg(test)]
impl<'a, E> PackageTypeSourceSink<'a, E> for CallbackSink<'a, '_, E> {
    fn capture(
        &mut self,
        occurrence: PackageTypeOccurrence,
        source: PackageTypeSource<'a>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        (self.visitor)(occurrence, source, work)
    }
    fn before_completed(&mut self) -> Result<(), E> {
        Ok(())
    }
}

struct Walk<'a, 'b, 'work, 'control, E> {
    fragment: p::FragmentId,
    sink: &'b mut dyn PackageTypeSourceSink<'a, E>,
    work: &'work mut CompileCheckpoints<'control>,
}
impl<'a, E: From<CompileControlError>> Walk<'a, '_, '_, '_, E> {
    fn emit(
        &mut self,
        owner: PackageTypeOwner,
        channel: PackageTypeChannel,
        source: PackageTypeSource<'a>,
    ) -> Result<(), E> {
        // Capture admission occurs before this operation's completed step,
        // including a caller carrying pending work from another real owner.
        self.sink.capture(
            PackageTypeOccurrence {
                fragment: self.fragment,
                owner,
                channel,
            },
            source,
            self.work,
        )?;
        self.completed()
    }
    fn value(
        &mut self,
        owner: PackageTypeOwner,
        channel: PackageTypeChannel,
        value: &'a p::ValueType,
    ) -> Result<(), E> {
        self.emit(owner, channel, PackageTypeSource::Value(value))
    }
    fn completed(&mut self) -> Result<(), E> {
        self.sink.before_completed()?;
        self.work.step().map_err(E::from)
    }
    fn arguments(
        &mut self,
        owner: PackageTypeOwner,
        arguments: &'a [p::FunctionArgumentType],
    ) -> Result<(), E> {
        for (argument, ty) in arguments.iter().enumerate() {
            match ty {
                p::FunctionArgumentType::Value(ty) => {
                    self.value(owner, PackageTypeChannel::Argument(argument), ty)?
                }
                p::FunctionArgumentType::Lambda {
                    parameter_types,
                    result_type,
                } => {
                    for (ordinal, ty) in parameter_types.iter().enumerate() {
                        self.value(
                            owner,
                            PackageTypeChannel::LambdaParameter { argument, ordinal },
                            ty,
                        )?;
                    }
                    self.value(
                        owner,
                        PackageTypeChannel::LambdaResult(argument),
                        result_type,
                    )?;
                }
            }
            self.completed()?;
        }
        Ok(())
    }
    fn function(
        &mut self,
        occurrence: BindingOccurrence,
        function: &'a p::BoundFunction,
    ) -> Result<(), E> {
        let owner = PackageTypeOwner::Binding(occurrence);
        self.arguments(owner, &function.argument_types)?;
        self.value(owner, PackageTypeChannel::Result(0), &function.result_type)
    }
    fn aggregate(
        &mut self,
        occurrence: BindingOccurrence,
        binding: &'a p::AggregateBinding,
    ) -> Result<(), E> {
        self.function(occurrence, &binding.function)?;
        self.value(
            PackageTypeOwner::Binding(occurrence),
            PackageTypeChannel::Intermediate,
            &binding.intermediate_type,
        )
    }
    fn writer_schema(
        &mut self,
        owner: PackageTypeOwner,
        schema: &'a p::WriterRelationSchema,
    ) -> Result<(), E> {
        for (ordinal, field) in schema.fields.iter().enumerate() {
            self.value(owner, PackageTypeChannel::Field(ordinal), &field.ty)?;
        }
        self.completed()
    }
    fn writer_fields(
        &mut self,
        node: p::NodeId,
        recipe: &'a ConnectorWriteRecipeDraft,
        role: WriterTypeRootRole,
        fields: &'a [ConnectorWriteFieldBinding],
    ) -> Result<(), E> {
        for (ordinal, binding) in fields.iter().enumerate() {
            self.emit(
                PackageTypeOwner::Recipe(node),
                PackageTypeChannel::WriterField { role, ordinal },
                PackageTypeSource::WriterField {
                    recipe,
                    role,
                    ordinal,
                    binding,
                },
            )?;
        }
        // Empty original roles are actual input occurrences as well.
        self.completed()
    }
}

/// Walk the checked owner in map/slice order. No root is structurally interned.
/// All callback errors return directly, with no enumerator entry or footer.
/// Work inside a callback is additional caller work, not this visitor's bill.
#[cfg(test)]
pub(crate) fn visit_package_type_sources<'source, E: From<CompileControlError>>(
    package: &'source p::FragmentPackage,
    visitor: &mut Visitor<'source, '_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    visit_package_type_sources_admitted_in(package, &mut CallbackSink { visitor }, work)
}

/// The exact same occurrence walk, with a parent gate before each completed
/// step, including empty roles and nodes that carry no type roots.
pub(crate) fn visit_package_type_sources_admitted_in<'source, E: From<CompileControlError>>(
    package: &'source p::FragmentPackage,
    sink: &mut dyn PackageTypeSourceSink<'source, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    use PackageTypeChannel as C;
    use PackageTypeOwner as O;
    let fragment = package.fragment();
    let mut walk = Walk {
        fragment: fragment.id(),
        sink,
        work,
    };
    for (id, value) in fragment.values() {
        walk.value(O::Value(*id), C::Value, &value.ty)?;
        walk.completed()?;
    }
    for (id, expression) in fragment.expressions().iter() {
        walk.value(O::Expression(*id), C::Value, &expression.ty)?;
        match &expression.kind {
            p::ExprKind::FunctionCall { function, .. } => {
                walk.function(BindingOccurrence::Scalar(*id), function)?
            }
            p::ExprKind::WindowCall {
                function,
                aggregate_binding,
                ..
            } => {
                walk.function(BindingOccurrence::Window(*id), function)?;
                if let Some(binding) = aggregate_binding {
                    walk.aggregate(BindingOccurrence::WindowAggregate(*id), binding)?;
                }
            }
            p::ExprKind::Lambda {
                parameter_types, ..
            } => {
                for (ordinal, ty) in parameter_types.iter().enumerate() {
                    walk.value(
                        O::Expression(*id),
                        C::ExpressionLambdaParameter(ordinal),
                        ty,
                    )?;
                }
            }
            p::ExprKind::Cast { .. } => walk.emit(
                O::Expression(*id),
                C::CastTarget,
                PackageTypeSource::Carrier,
            )?,
            p::ExprKind::Value(_)
            | p::ExprKind::LambdaParameter { .. }
            | p::ExprKind::Literal(_)
            | p::ExprKind::Constant(_)
            | p::ExprKind::Unary { .. }
            | p::ExprKind::Binary { .. }
            | p::ExprKind::Conjunction { .. }
            | p::ExprKind::Disjunction { .. }
            | p::ExprKind::IsNull { .. }
            | p::ExprKind::InList { .. }
            | p::ExprKind::Between { .. }
            | p::ExprKind::Like { .. }
            | p::ExprKind::Case { .. }
            | p::ExprKind::IsTruthValue { .. } => {}
        }
        walk.completed()?;
    }
    for (id, node) in fragment.nodes() {
        // The physical owner is the sole Aggregate/TopN GroupedStates author.
        if let Some((_, calls)) = node.kind.aggregate_contract() {
            for (ordinal, call) in calls.iter().enumerate() {
                walk.aggregate(
                    BindingOccurrence::Aggregate {
                        node: *id,
                        ordinal,
                        call: call.id,
                    },
                    &call.binding,
                )?;
                walk.completed()?;
            }
        }
        match &node.kind {
            p::NodeKind::Scan { relation, .. } => {
                for (ordinal, field) in relation.schema().iter().enumerate() {
                    walk.value(O::Relation(*id), C::Field(ordinal), &field.ty)?;
                }
            }
            p::NodeKind::TableFunction { function, .. } => {
                let owner = O::Binding(BindingOccurrence::Table(*id));
                walk.arguments(owner, &function.argument_types)?;
                for (ordinal, ty) in function.result_types.iter().enumerate() {
                    walk.value(owner, C::Result(ordinal), ty)?;
                }
            }
            p::NodeKind::TableWriter { target } => {
                for (ordinal, field) in target.target_fields.iter().enumerate() {
                    walk.value(O::WriterTarget(*id), C::Field(ordinal), &field.ty)?;
                }
                walk.writer_schema(O::WriterOutput(*id), &target.output_schema)?;
                for (ordinal, call) in target.partial_aggregates.iter().enumerate() {
                    walk.aggregate(
                        BindingOccurrence::WriterPartial { node: *id, ordinal },
                        &call.binding,
                    )?;
                    walk.completed()?;
                }
            }
            p::NodeKind::TableFinish(finish) => {
                walk.writer_schema(O::FinishInput(*id), &finish.input_schema)?;
                walk.writer_schema(O::FinishOutput(*id), &finish.output_schema)?;
                for (ordinal, call) in finish.final_aggregates.iter().enumerate() {
                    walk.aggregate(
                        BindingOccurrence::FinishFinal { node: *id, ordinal },
                        &call.binding,
                    )?;
                    walk.completed()?;
                }
            }
            p::NodeKind::SystemRelation { output_fields, .. } => {
                for (ordinal, field) in output_fields.iter().enumerate() {
                    walk.emit(
                        O::SystemRelation(*id),
                        C::Field(ordinal),
                        PackageTypeSource::StrictField(field),
                    )?;
                }
            }
            p::NodeKind::Aggregate { .. }
            | p::NodeKind::TopN { .. }
            | p::NodeKind::Filter { .. }
            | p::NodeKind::Project { .. }
            | p::NodeKind::HashJoin { .. }
            | p::NodeKind::NestLoopJoin { .. }
            | p::NodeKind::Sort { .. }
            | p::NodeKind::Limit { .. }
            | p::NodeKind::Window(_)
            | p::NodeKind::SetOp { .. }
            | p::NodeKind::Values { .. }
            | p::NodeKind::Repeat { .. }
            | p::NodeKind::Unpivot { .. }
            | p::NodeKind::GenerateSeries { .. }
            | p::NodeKind::AssertOneRow(_)
            | p::NodeKind::ChangeEventExpand { .. }
            | p::NodeKind::ExchangeSource { .. } => {}
        }
        walk.completed()?;
    }
    for (ordinal, cut) in package.cuts().inbound.iter().enumerate() {
        let owner = O::Cut {
            direction: CutDirection::Inbound,
            ordinal,
            edge: cut.edge,
        };
        for (ordinal, import) in cut.imports.iter().enumerate() {
            walk.value(owner, C::CutImport(ordinal), &import.source.ty)?;
        }
        if let p::ExchangePayloadContract::WriterResults(result) = &cut.payload {
            for (ordinal, field) in result.fields.iter().enumerate() {
                walk.value(owner, C::CutWriterResult(ordinal), &field.ty)?;
            }
            walk.completed()?;
        }
        walk.completed()?;
    }
    for (ordinal, cut) in package.cuts().outbound.iter().enumerate() {
        let owner = O::Cut {
            direction: CutDirection::Outbound,
            ordinal,
            edge: cut.edge,
        };
        for (ordinal, value) in cut.projection.iter().enumerate() {
            walk.value(owner, C::CutProjection(ordinal), &value.ty)?;
        }
        for (ordinal, import) in cut.destination_imports.iter().enumerate() {
            walk.value(owner, C::CutDestinationImport(ordinal), &import.source.ty)?;
        }
        if let p::ExchangePayloadContract::WriterResults(result) = &cut.payload {
            for (ordinal, field) in result.fields.iter().enumerate() {
                walk.value(owner, C::CutWriterResult(ordinal), &field.ty)?;
            }
            walk.completed()?;
        }
        walk.completed()?;
    }
    for (ordinal, filter) in package.cuts().runtime_filters.iter().enumerate() {
        let ty = match &filter.domain {
            p::RuntimeFilterDomain::Membership { ty, .. } => ty,
            p::RuntimeFilterDomain::Ordered { key, .. } => &key.ty,
        };
        walk.value(
            O::RuntimeFilter {
                ordinal,
                id: filter.id,
            },
            C::Value,
            ty,
        )?;
        walk.completed()?;
    }
    for (id, pool) in package.constants().entries() {
        walk.value(O::Constant(*id), C::Value, pool.value_type())?;
        walk.emit(
            O::Constant(*id),
            C::ConstantField,
            PackageTypeSource::SourceField(pool.field_ref()),
        )?;
        walk.completed()?;
    }
    for (definition, request) in fragment.call_requests().entries() {
        let owner = O::Request(*definition);
        for (argument, value) in request.arguments.iter().enumerate() {
            match value {
                p::StaticFunctionArgument::Value { value_type, .. } => {
                    walk.value(owner, C::Argument(argument), value_type)?
                }
                p::StaticFunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => {
                    for (ordinal, ty) in parameter_types.iter().enumerate() {
                        walk.value(owner, C::LambdaParameter { argument, ordinal }, ty)?;
                    }
                    walk.value(owner, C::LambdaResult(argument), result_type)?;
                }
            }
            walk.completed()?;
        }
        if let Some(ty) = &request.expected_result_type {
            walk.value(owner, C::ExpectedResult, ty)?;
        }
        walk.completed()?;
    }
    if let Some(result) = package.result() {
        for (ordinal, field) in result.fields.iter().enumerate() {
            walk.value(O::Result, C::Field(ordinal), &field.ty)?;
        }
        walk.completed()?;
    }
    for (node, scan) in package.scans() {
        let public = scan.public_facts();
        let (Some(execution), Some(source)) =
            (public.execution_schema(), public.source_schema_facts())
        else {
            walk.emit(
                O::Scan(*node),
                C::Value,
                PackageTypeSource::MissingReadSchema,
            )?;
            continue;
        };
        for (ordinal, (field, value)) in execution
            .schema()
            .fields()
            .iter()
            .zip(execution.bindings())
            .enumerate()
        {
            walk.value(O::Scan(*node), C::ScanExecutionValue(ordinal), value)?;
            walk.emit(
                O::Scan(*node),
                C::Field(ordinal),
                PackageTypeSource::ExecutionField { field, value },
            )?;
        }
        for (ordinal, field) in source.shape().schema().fields().iter().enumerate() {
            walk.emit(
                O::Scan(*node),
                C::ScanSourceField(ordinal),
                PackageTypeSource::StrictField(field),
            )?;
        }
        walk.completed()?;
    }
    for (node, recipe) in package.writes() {
        use WriterTypeRootRole as R;
        match recipe.input() {
            ConnectorWriteInputShape::Data { fields } => {
                walk.writer_fields(*node, recipe, R::Data, fields)?
            }
            ConnectorWriteInputShape::RowLineage {
                data_fields,
                row_identity_fields,
            } => {
                walk.writer_fields(*node, recipe, R::RowLineageData, data_fields)?;
                walk.writer_fields(*node, recipe, R::RowLineageIdentity, row_identity_fields)?;
            }
            ConnectorWriteInputShape::PositionDelete {
                identity_fields,
                partition_source_fields,
            } => {
                walk.writer_fields(*node, recipe, R::PositionDeleteIdentity, identity_fields)?;
                walk.writer_fields(
                    *node,
                    recipe,
                    R::PositionDeletePartition,
                    partition_source_fields,
                )?;
            }
            ConnectorWriteInputShape::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => {
                walk.writer_fields(*node, recipe, R::DeletionVectorIdentity, identity_fields)?;
                walk.writer_fields(
                    *node,
                    recipe,
                    R::DeletionVectorPartition,
                    partition_source_fields,
                )?;
            }
            ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
                walk.writer_fields(*node, recipe, R::EqualityDelete, equality_fields)?
            }
        }
        walk.completed()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_connector_contract as c;
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    use std::{collections::HashMap, sync::Mutex};

    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];
    #[derive(Default)]
    struct Control {
        events: Mutex<Vec<(CompilePhase, u32)>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut events = self.events.lock().unwrap();
            let at = events.len();
            if let Some((stop, _)) = self.stop {
                assert!(at <= stop, "callback after refusal");
            }
            events.push((phase, units));
            if let Some((stop, cause)) = self.stop.clone()
                && at == stop
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn package() -> p::FragmentPackage {
        let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let catalog =
            c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
        let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
        let binding = c::ConnectorWriteBinding::new(
            c::ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let payload = c::ConnectorEncodedPayload::new(
            c::ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                c::ConnectorCodecCategory::WriteHandle,
                c::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![7u8].into(),
        );
        let recipe = c::ConnectorWriteRecipeDraft::try_new(
            binding,
            payload,
            c::ConnectorWriteInputShape::Data {
                fields: vec![c::ConnectorWriteFieldBinding::new(
                    c::ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("v", DataType::Int64, false).with_metadata(HashMap::from([
                        ("large".into(), "雪".repeat(6826) + "ab"),
                        ("embedded".into(), "a\0b".into()),
                    ])),
                )],
            },
        )
        .unwrap();
        // Original physical producer + stream + finisher, original cut
        // derivation and full Package publication; not an unchecked DTO.
        crate::physical_type_v2::sender_tests::checked_writer_package(recipe)
    }

    #[test]
    fn checked_writer_package_preserves_original_root_addresses_and_cut_repetitions() {
        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut roots = Vec::new();
        visit_package_type_sources::<CompileControlError>(
            &package,
            &mut |occurrence, source, _| {
                roots.push((occurrence, source));
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        // This actual fixture has five Values, one literal, one target field,
        // four Writer output fields, three ordered four-field cut copies,
        // and one independent original recipe Field.
        assert_eq!(roots.len(), 24);
        for ((id, value), (occurrence, source)) in package.fragment().values().iter().zip(&roots) {
            assert_eq!(occurrence.owner, PackageTypeOwner::Value(*id));
            let PackageTypeSource::Value(actual) = source else {
                panic!("Value loan");
            };
            assert!(std::ptr::eq(*actual, &value.ty));
        }
        let (node, recipe) = package.writes().iter().next().unwrap();
        let (occurrence, source) = roots.last().unwrap();
        assert_eq!(occurrence.owner, PackageTypeOwner::Recipe(*node));
        assert_eq!(
            occurrence.channel,
            PackageTypeChannel::WriterField {
                role: WriterTypeRootRole::Data,
                ordinal: 0
            }
        );
        let PackageTypeSource::WriterField {
            recipe: actual,
            binding,
            role,
            ordinal,
        } = source
        else {
            panic!("writer loan");
        };
        assert!(std::ptr::eq(*actual, recipe));
        assert!(std::ptr::eq(
            *binding,
            recipe.input().fields_iter().next().unwrap()
        ));
        assert_eq!((*role, *ordinal), (WriterTypeRootRole::Data, 0));
        assert_eq!(binding.field().metadata()["large"].len(), 20 * 1024);
        assert_eq!(binding.field().metadata()["embedded"], "a\0b");
        let cut = &package.cuts().outbound[0];
        let cut_roots: Vec<_> = roots
            .iter()
            .filter(|(occurrence, _)| matches!(occurrence.owner, PackageTypeOwner::Cut { .. }))
            .collect();
        assert_eq!(cut_roots.len(), 12);
        for ordinal in 0..4 {
            assert_eq!(
                cut_roots[ordinal].0.channel,
                PackageTypeChannel::CutProjection(ordinal)
            );
            assert_eq!(
                cut_roots[ordinal + 4].0.channel,
                PackageTypeChannel::CutDestinationImport(ordinal)
            );
            assert_eq!(
                cut_roots[ordinal + 8].0.channel,
                PackageTypeChannel::CutWriterResult(ordinal)
            );
            for (root, expected) in [
                (cut_roots[ordinal], &cut.projection[ordinal].ty),
                (
                    cut_roots[ordinal + 4],
                    &cut.destination_imports[ordinal].source.ty,
                ),
                (
                    cut_roots[ordinal + 8],
                    &match &cut.payload {
                        p::ExchangePayloadContract::WriterResults(result) => result,
                        _ => panic!("expected writer result proof"),
                    }
                    .fields[ordinal]
                        .ty,
                ),
            ] {
                let PackageTypeSource::Value(actual) = root.1 else {
                    panic!("cut Value loan");
                };
                assert!(std::ptr::eq(actual, expected));
            }
        }
        assert_eq!(roots[0].0.fragment, package.fragment().id());
        assert!(
            control
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Encode)
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    enum Error {
        Control(CompileControlError),
        Captured(usize),
    }
    impl From<CompileControlError> for Error {
        fn from(cause: CompileControlError) -> Self {
            Self::Control(cause)
        }
    }

    #[test]
    fn checked_package_every_actual_small_callback_and_capture_error_returns_without_footer() {
        let package = package();
        let run = |stop| {
            let control = Control {
                stop,
                ..Default::default()
            };
            let outcome = (|| {
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
                visit_package_type_sources::<CompileControlError>(
                    &package,
                    &mut |_, _, work| {
                        // A real caller boundary after receiving each loan. This
                        // callback adds no synthetic work or second scope.
                        work.flush()
                    },
                    &mut work,
                )?;
                work.finish()
            })();
            let trace = control.events.into_inner().unwrap();
            (outcome, trace)
        };
        let (outcome, trace) = run(None);
        assert!(outcome.is_ok());
        assert!(trace.len() > 24);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let (outcome, stopped) = run(Some((at, cause.clone())));
                assert_eq!(outcome, Err(cause));
                assert_eq!(stopped, trace[..=at]);
            }
        }
        for rejected in 0..24 {
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut captured = 0;
            let outcome = visit_package_type_sources::<Error>(
                &package,
                &mut |_, _, _| {
                    let at = captured;
                    captured += 1;
                    if at == rejected {
                        return Err(Error::Captured(at));
                    }
                    Ok(())
                },
                &mut work,
            );
            assert_eq!(outcome, Err(Error::Captured(rejected)));
            assert_eq!(captured, rejected + 1);
            // No ordinary enumerator footer; the caller still owns it.
            assert_eq!(
                control.events.lock().unwrap().as_slice(),
                &[(CompilePhase::Decode, 0)]
            );
        }
    }

    #[test]
    fn first_actual_root_capture_precedes_pending_completed_callback() {
        let package = package();
        for cause in CAUSES {
            let control = Control {
                stop: Some((3, cause)),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let spelling = "x".repeat(255);
            let copied = novarocks_type_contract::owned_resources::copy::copy_string::<
                CompileControlError,
            >(&spelling, &mut work)
            .unwrap();
            assert_eq!(copied, spelling);
            assert_eq!(
                control.events.lock().unwrap().as_slice(),
                &[
                    (CompilePhase::Encode, 0),
                    (CompilePhase::Encode, 0),
                    (CompilePhase::Encode, 1)
                ]
            );
            let mut captured = 0;
            let outcome = visit_package_type_sources::<CompileControlError>(
                &package,
                &mut |occurrence, source, _| {
                    captured += 1;
                    let (id, value) = package.fragment().values().iter().next().unwrap();
                    assert_eq!(occurrence.owner, PackageTypeOwner::Value(*id));
                    let PackageTypeSource::Value(actual) = source else {
                        panic!("actual root");
                    };
                    assert!(std::ptr::eq(actual, &value.ty));
                    // The parent rejects the captured root before its completed
                    // operation could flush the existing 255 actual copy steps.
                    Err(CompileControlError::ResourceExhausted)
                },
                &mut work,
            );
            assert_eq!(outcome, Err(CompileControlError::ResourceExhausted));
            assert_eq!(captured, 1);
            assert_eq!(control.events.lock().unwrap().len(), 3);
        }
    }
}
