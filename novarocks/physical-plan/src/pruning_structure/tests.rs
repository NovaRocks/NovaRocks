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

use super::*;
use crate::*;
use arrow_schema::DataType;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
    ConnectorReadRelationPayload, ConnectorReadRelationRecipeDraft, ConnectorValue,
    ConnectorValueType, Domain, FrozenConnectorScan, StaticScanAssignment,
};
use novarocks_type_contract::{
    ControlShape, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, SemanticParameters, control_argument_semantics,
};
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Control {
    failure: Option<CompileControlError>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.work.lock().unwrap().push(units);
        if (!self.positive_only || units > 0)
            && let Some(failure) = self.failure.clone()
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn ty(data_type: DataType) -> ValueType {
    ValueType::new(data_type, false)
}
fn budget() -> ScanReadBudget {
    ScanReadBudget {
        max_batch_rows: 4096,
        max_batch_bytes: 8388608,
    }
}
fn encoded(
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
    byte: u8,
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![byte].into(),
    )
}
fn append_scan(builder: &mut FragmentBuilder, residual: bool) -> (NodeId, ValueId) {
    let instance = ConnectorInstanceId::parse("pruning-fixture").unwrap();
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    );
    let node = builder.reserve_node_id().unwrap();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 3),
    };
    let value = builder
        .add_value(
            ty(DataType::Int64),
            ValueOrigin::ProviderField {
                scan_node: node,
                field: column.clone(),
            },
        )
        .unwrap();
    let predicate = residual.then(|| predicate(builder, node, value));
    let relation = Relation::Data(DataRelation {
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new(vec![9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                encoded(&binding, ConnectorCodecCategory::ReadTable, 1),
                encoded(&binding, ConnectorCodecCategory::ReadView, 2),
            ),
        },

        selection_digest: [8; 32],
        schema: Box::from([RelationField {
            column: column.clone(),
            ty: ty(DataType::Int64),
        }]),
        predicate_guarantees: predicate
            .into_iter()
            .map(|predicate| PredicateGuarantee {
                predicate,
                kind: PredicateGuaranteeKind::PruningOnly,
            })
            .collect(),
        provided_properties: properties(),
    });
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(17),
                relation: Box::new(relation),
                read_budget: budget(),
                provider_outputs: Box::from([(column, value)]),
                residuals: predicate.into_iter().collect(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    (node, value)
}
fn predicate(builder: &mut FragmentBuilder, node: NodeId, value: ValueId) -> ExprId {
    let left = builder
        .add_expression(node, ty(DataType::Int64), ExprKind::Value(value))
        .unwrap();
    let right = builder
        .add_expression(
            node,
            ty(DataType::Int64),
            ExprKind::Literal(LiteralValue::Int64(0)),
        )
        .unwrap();
    builder
        .add_expression(
            node,
            ty(DataType::Boolean),
            ExprKind::Binary {
                allow_throw_exception: None,
                left,
                op: BinaryOperator::Gt,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
        )
        .unwrap()
}
fn append_filter(
    builder: &mut FragmentBuilder,
    child: NodeId,
    value: ValueId,
    count: usize,
) -> NodeId {
    let node = builder.reserve_node_id().unwrap();
    let predicates = (0..count)
        .map(|_| predicate(builder, node, value))
        .collect();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([child]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Filter { predicates },
        })
        .unwrap();
    node
}
fn expression_uses(fragment: &Fragment) -> PhysicalRootUses {
    fn add(
        fragment: &Fragment,
        definition: ExprId,
        demand: EvaluationDemand,
        uses: &mut Vec<ExpressionInvocation<ExprId>>,
    ) -> ExpressionUseId {
        let id = ExpressionUseId::new(uses.len() as u32);
        let shape = match &fragment.expressions().get(definition).unwrap().kind {
            ExprKind::Conjunction { .. } => ControlShape::Conjunction,
            _ => ControlShape::Eager,
        };
        let children = match &fragment.expressions().get(definition).unwrap().kind {
            ExprKind::Binary { left, right, .. } => vec![*left, *right],
            ExprKind::Conjunction { args } => args.to_vec(),
            ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
            other => panic!("fixture requires explicit control for {other:?}"),
        };
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain: EvaluationDomainId::new(0),
                demand,
            },
            definition,
            control: shape,
            arguments: Box::default(),
        });
        let arguments = children
            .iter()
            .enumerate()
            .map(|(ordinal, child)| {
                let (child_demand, guard) =
                    control_argument_semantics(shape, children.len(), ordinal, demand).unwrap();
                assert_eq!(guard, None);
                add(fragment, *child, child_demand, uses)
            })
            .collect();
        uses[id.get() as usize].arguments = arguments;
        id
    }
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control::default()).unwrap();
    let mut uses = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .map(|(site, root)| (*site, add(fragment, root.expr, root.demand, &mut uses)))
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control::default()).unwrap()
}
fn public_read(
    fragment: &Fragment,
    scan_id: NodeId,
    scan: FrozenConnectorScan,
) -> Result<FrozenConnectorRead, novarocks_connector_contract::ConnectorError> {
    use novarocks_connector_contract::*;
    let NodeKind::Scan {
        relation,
        provider_outputs,
        ..
    } = &fragment.nodes()[&scan_id].kind
    else {
        unreachable!()
    };
    let properties = relation.provided_properties();
    let distribution = match properties.distribution {
        Distribution::Unconstrained => ConnectorReadDistribution::Unconstrained,
        Distribution::Singleton => ConnectorReadDistribution::Singleton,
        Distribution::RoundRobin => ConnectorReadDistribution::RoundRobin,
        _ => unreachable!("fixture uses no provider partitioned guarantee"),
    };
    let ordering = properties
        .ordering
        .iter()
        .map(|key| {
            ConnectorReadOrderingKey::new(
                ScanColumnId::new(
                    provider_outputs
                        .iter()
                        .position(|(_, value)| *value == key.value)
                        .unwrap(),
                ),
                match key.direction {
                    SortDirection::Ascending => ConnectorReadSortDirection::Ascending,
                    SortDirection::Descending => ConnectorReadSortDirection::Descending,
                },
                match key.null_ordering {
                    NullOrdering::First => ConnectorReadNullOrdering::First,
                    NullOrdering::Last => ConnectorReadNullOrdering::Last,
                },
            )
        })
        .collect::<Vec<_>>();
    let (kind, coverage) = match relation.as_ref() {
        Relation::Data(_) => (None, vec![]),
        Relation::Metadata(metadata) => (
            Some(ConnectorReadMetadataKind::try_new(metadata.kind.as_str())?),
            metadata.coverage_evidence.to_vec(),
        ),
    };
    let facts = ConnectorReadStaticFacts::try_new(
        relation.read().input_version.clone(),
        relation.selection_digest(),
        ConnectorReadProperties::try_new(distribution, ordering)?,
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        coverage,
    )?;
    let schema = arrow_schema::Schema::new(
        relation
            .schema()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                arrow_schema::Field::new(
                    format!("v{ordinal}"),
                    field.ty.data_type().clone(),
                    field.ty.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    );
    let public = ConnectorReadPublicFacts::try_new(
        facts,
        kind,
        schema,
        relation
            .schema()
            .iter()
            .map(|field| field.ty.logical_type())
            .collect(),
    )?;
    FrozenConnectorRead::try_new(scan, public)
}

fn package(builder: FragmentBuilder, root: NodeId, scan: NodeId) -> FragmentPackage {
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let expression_uses = expression_uses(&fragment);
    let NodeKind::Scan { relation, .. } = &fragment.nodes()[&scan].kind else {
        unreachable!()
    };
    let recipe = ConnectorReadRelationRecipeDraft::try_new(
        relation.read().binding.clone(),
        relation.read().relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let domain = TupleDomain::with_column_domains(BTreeMap::from([(
        ScanColumnId::new(0),
        Domain::single_value(ConnectorValue::BigInt(1)).unwrap(),
    )]))
    .unwrap();
    let frozen = FrozenConnectorScan::try_new(
        recipe,
        vec![StaticScanAssignment::new(
            Arc::from("v0"),
            ConnectorValueType::BigInt,
        )],
        domain.clone(),
        domain,
        None,
        vec![],
        NonZeroU64::new(budget().max_batch_rows).unwrap(),
        NonZeroU64::new(budget().max_batch_bytes).unwrap(),
    )
    .unwrap();
    let read = public_read(&fragment, scan, frozen).unwrap();
    let version = PlanVersionId::try_new([7; 16]).unwrap();
    // Derive cuts from a fully checked real plan, never bypass package checks.
    let mut plan = PlanBuilder::new(version);
    plan.add_fragment(fragment.clone()).unwrap();
    let plan = plan.finish().unwrap();
    let mut cuts = derive_fragment_cuts(&plan).unwrap();
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &expression_uses, Vec::new(), &Control::default())
            .unwrap();
    let pruning =
        FrozenFragmentPruning::try_new(fragment.id(), Vec::new(), &Control::default()).unwrap();
    FragmentPackage::try_new(
        FragmentPackageInput {
            constants: crate::ConstantPools::empty(),
            version,
            required: RequiredContracts::default(),
            cuts: cuts.remove(&fragment.id()).unwrap(),
            fragment,
            expression_uses,
            calls,
            pruning,
            result: None,
            parameters: SemanticParameters::default(),
            scans: BTreeMap::from([(scan, read)]),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        package_admission(),
        &Control::default(),
    )
    .unwrap()
}
fn source_claim(
    package: &FragmentPackage,
    site: ExpressionRootSite,
    path: &[u32],
) -> (PredicateResponsibilityRef, ExpressionEffectContext) {
    let source = PredicateConjunctSource::try_new(
        package.fragment(),
        package.expression_uses(),
        site,
        path.to_vec(),
        &Control::default(),
    )
    .unwrap();
    (source.responsibility().anchor(), source.context())
}
fn fixture(residual: bool, count: usize) -> (FragmentPackage, PruningDomainWitness) {
    let mut builder = FragmentBuilder::new(FragmentId::new(930));
    let (scan, value) = append_scan(&mut builder, residual);
    let node = if residual {
        scan
    } else {
        append_filter(&mut builder, scan, value, count)
    };
    let package = package(builder, node, scan);
    let witness = PruningDomainWitness {
        target: PruningDomainSite {
            fragment: package.fragment().id(),
            scan,
            occurrence: ProviderReadOccurrenceId::new(17),
            field: PruningDomainField::Enforced,
        },
        sources: (0..if residual { 1 } else { count })
            .map(|ordinal| {
                let site = ExpressionRootSite {
                    node,
                    role: if residual {
                        ExpressionRootRole::ScanResidual { predicate: 0 }
                    } else {
                        ExpressionRootRole::FilterPredicate {
                            predicate: ordinal as u32,
                        }
                    },
                };
                let (responsibility, context) = source_claim(&package, site, &[]);
                PruningSourceWitness {
                    responsibility,
                    context,
                    conjunct_path: Box::default(),
                    input_path: if residual {
                        Box::default()
                    } else {
                        Box::from([PruningInputEdge {
                            consumer: node,
                            input_ordinal: 0,
                            producer: scan,
                        }])
                    },
                    columns: Box::from([PruningColumnTrace {
                        column: ScanColumnId::new(0),
                        values: vec![value; if residual { 1 } else { 2 }].into_boxed_slice(),
                    }]),
                }
            })
            .collect(),
    };
    (package, witness)
}
#[test]
fn actual_enforced_and_unenforced_domain_fields_borrow_exact_read_and_keep_original_p() {
    let (package, mut witness) = fixture(false, 1);
    for field in [PruningDomainField::Enforced, PruningDomainField::Unenforced] {
        witness.target.field = field;
        let checked =
            PruningDomainStructure::try_new(&package, &witness, &Control::default()).unwrap();
        assert!(std::ptr::eq(checked.package(), &package));
        assert!(std::ptr::eq(checked.witness(), &witness));
        let read = &package.scans()[&witness.target.scan];
        assert!(std::ptr::eq(checked.read(), read));
        let expected = match field {
            PruningDomainField::Enforced => read.scan().enforced_predicate(),
            PruningDomainField::Unenforced => read.scan().unenforced_predicate(),
        };
        assert!(std::ptr::eq(checked.domain(), expected));
        let NodeKind::Filter { predicates } =
            &package.fragment().nodes()[&witness.sources[0].responsibility.site.node].kind
        else {
            unreachable!()
        };
        assert_eq!(checked.sources()[0].definition(), predicates[0]);
        assert_eq!(
            checked.sources()[0].responsibility().anchor().site,
            witness.sources[0].responsibility.site
        );
    }
}

#[test]
fn stronger_arbitrary_q_can_be_structurally_sound_without_implication_or_pruning_authorization() {
    let (package, witness) = fixture(false, 1);
    let checked = PruningDomainStructure::try_new(&package, &witness, &Control::default()).unwrap();
    let expression = package
        .fragment()
        .expressions()
        .get(checked.sources()[0].definition())
        .unwrap();
    assert!(matches!(
        expression.kind,
        ExprKind::Binary {
            op: BinaryOperator::Gt,
            ..
        }
    ));
    let domain = &checked.domain().domains().unwrap()[&ScanColumnId::new(0)];
    assert_eq!(
        domain,
        &Domain::single_value(ConnectorValue::BigInt(1)).unwrap()
    );
    assert!(
        !domain
            .values()
            .contains_value(&ConnectorValue::BigInt(2))
            .unwrap()
    );
    // v=2 satisfies this fixture's original v>0 while q(v)=v=1 is false.
    // Structural success therefore establishes no p=>q or execute privilege.
    assert_eq!(checked.sources().len(), 1);
}
#[test]
fn scan_owned_residual_has_a_zero_edge_path_and_remains_an_exact_p_responsibility() {
    let (package, witness) = fixture(true, 1);
    assert!(witness.sources[0].input_path.is_empty());
    assert_eq!(witness.sources[0].columns[0].values.len(), 1);
    let checked = PruningDomainStructure::try_new(&package, &witness, &Control::default()).unwrap();
    let NodeKind::Scan {
        residuals,
        relation,
        occurrence,
        ..
    } = &package.fragment().nodes()[&witness.target.scan].kind
    else {
        unreachable!()
    };
    assert_eq!(*occurrence, witness.target.occurrence);
    assert_eq!(checked.sources()[0].definition(), residuals[0]);
    assert_eq!(relation.predicate_guarantees()[0].predicate, residuals[0]);
    assert_eq!(
        relation.predicate_guarantees()[0].kind,
        PredicateGuaranteeKind::PruningOnly
    );
}
fn project_fixture() -> (FragmentPackage, PruningDomainWitness) {
    let mut builder = FragmentBuilder::new(FragmentId::new(931));
    let (scan, value) = append_scan(&mut builder, false);
    let project = builder.reserve_node_id().unwrap();
    let expression = builder
        .add_expression(project, ty(DataType::Int64), ExprKind::Value(value))
        .unwrap();
    let alias = builder
        .add_value(
            ty(DataType::Int64),
            ValueOrigin::Expr {
                node: project,
                expr: expression,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: project,
            inputs: Box::from([scan]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: project,
                columns: Box::from([alias]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([(expression, alias)]),
            },
        })
        .unwrap();
    let filter = append_filter(&mut builder, project, alias, 1);
    let package = package(builder, filter, scan);
    let (responsibility, context) = source_claim(
        &package,
        ExpressionRootSite {
            node: filter,
            role: ExpressionRootRole::FilterPredicate { predicate: 0 },
        },
        &[],
    );
    let witness = PruningDomainWitness {
        target: PruningDomainSite {
            fragment: package.fragment().id(),
            scan,
            occurrence: ProviderReadOccurrenceId::new(17),
            field: PruningDomainField::Unenforced,
        },
        sources: Box::from([PruningSourceWitness {
            responsibility,
            context,
            conjunct_path: Box::default(),
            input_path: Box::from([
                PruningInputEdge {
                    consumer: filter,
                    input_ordinal: 0,
                    producer: project,
                },
                PruningInputEdge {
                    consumer: project,
                    input_ordinal: 0,
                    producer: scan,
                },
            ]),
            columns: Box::from([PruningColumnTrace {
                column: ScanColumnId::new(0),
                values: Box::from([alias, alias, value]),
            }]),
        }]),
    };
    (package, witness)
}
#[test]
fn actual_project_alias_fields_transport_each_column_identity_on_adjacent_edges() {
    let (package, witness) = project_fixture();
    PruningDomainStructure::try_new(&package, &witness, &Control::default()).unwrap();
    assert_ne!(
        witness.sources[0].columns[0].values[1],
        witness.sources[0].columns[0].values[2]
    );
    let mut forged = witness.clone();
    forged.sources[0].input_path = Box::from([PruningInputEdge {
        consumer: witness.sources[0].responsibility.site.node,
        input_ordinal: 0,
        producer: witness.target.scan,
    }]);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidPath
    );
    let mut forged = witness.clone();
    forged.sources[0].columns[0].values[1] = witness.sources[0].columns[0].values[2];
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidTransport
    );
}
#[test]
fn fragment_occurrence_scan_and_exact_source_sites_are_checked_against_the_package() {
    let (package, witness) = fixture(false, 1);
    let mut forged = witness.clone();
    forged.target.fragment = FragmentId::new(u32::MAX);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidFragment
    );
    let mut forged = witness.clone();
    forged.target.occurrence = ProviderReadOccurrenceId::new(18);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidOccurrence
    );
    for node in [
        NodeId::new(u32::MAX),
        witness.sources[0].responsibility.site.node,
    ] {
        let mut forged = witness.clone();
        forged.target.scan = node;
        assert_eq!(
            PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
            PruningStructureError::InvalidScan
        );
    }
    let mut forged = witness.clone();
    forged.sources[0].responsibility.site.role =
        ExpressionRootRole::FilterPredicate { predicate: 99 };
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::Source(PredicateSourceError::InvalidSite)
    );
    let mut forged = witness.clone();
    forged.sources[0].conjunct_path = Box::from([0]);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::Source(PredicateSourceError::NotPositiveConjunction)
    );
}

#[test]
fn exact_p_responsibility_rejects_wrong_fragment_and_another_actual_root_use() {
    let (package, witness) = fixture(false, 2);
    let mut forged = witness.clone();
    forged.sources[0].responsibility.fragment = FragmentId::new(u32::MAX);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::WrongResponsibility
    );

    // Both IDs name actual roots, but only one belongs to the claimed p site.
    assert_ne!(
        witness.sources[0].responsibility.use_id,
        witness.sources[1].responsibility.use_id
    );
    let mut forged = witness.clone();
    forged.sources[0].responsibility.use_id = witness.sources[1].responsibility.use_id;
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::WrongResponsibility
    );
}

#[test]
fn selected_conjunct_context_rejects_wrong_use_domain_and_demand() {
    let (package, witness) = fixture(false, 2);
    let context = witness.sources[0].context;
    let wrong = [
        ExpressionEffectContext {
            use_id: witness.sources[1].context.use_id,
            ..context
        },
        ExpressionEffectContext {
            domain: EvaluationDomainId::new(u32::MAX),
            ..context
        },
        ExpressionEffectContext {
            demand: EvaluationDemand::Value,
            ..context
        },
    ];
    for context in wrong {
        let mut forged = witness.clone();
        forged.sources[0].context = context;
        assert_eq!(
            PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
            PruningStructureError::WrongContext
        );
    }
}

#[test]
fn shared_conjunct_definition_preserves_distinct_uses_and_cannot_swap_claimed_context() {
    let mut builder = FragmentBuilder::new(FragmentId::new(933));
    let (scan, value) = append_scan(&mut builder, false);
    let filter = builder.reserve_node_id().unwrap();
    let shared = predicate(&mut builder, filter, value);
    let conjunction = builder
        .add_expression(
            filter,
            ty(DataType::Boolean),
            ExprKind::Conjunction {
                args: Box::from([shared, shared]),
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: filter,
            inputs: Box::from([scan]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: filter,
                columns: Box::from([value]),
            },
            kind: NodeKind::Filter {
                predicates: Box::from([conjunction]),
            },
        })
        .unwrap();
    let package = package(builder, filter, scan);
    let site = ExpressionRootSite {
        node: filter,
        role: ExpressionRootRole::FilterPredicate { predicate: 0 },
    };
    let witness = PruningDomainWitness {
        target: PruningDomainSite {
            fragment: package.fragment().id(),
            scan,
            occurrence: ProviderReadOccurrenceId::new(17),
            field: PruningDomainField::Enforced,
        },
        sources: (0..2)
            .map(|ordinal| {
                let (responsibility, context) = source_claim(&package, site, &[ordinal]);
                PruningSourceWitness {
                    responsibility,
                    context,
                    conjunct_path: Box::from([ordinal]),
                    input_path: Box::from([PruningInputEdge {
                        consumer: filter,
                        input_ordinal: 0,
                        producer: scan,
                    }]),
                    columns: Box::from([PruningColumnTrace {
                        column: ScanColumnId::new(0),
                        values: Box::from([value, value]),
                    }]),
                }
            })
            .collect(),
    };
    let checked = PruningDomainStructure::try_new(&package, &witness, &Control::default()).unwrap();
    assert_eq!(checked.sources()[0].definition(), shared);
    assert_eq!(checked.sources()[1].definition(), shared);
    assert_eq!(
        witness.sources[0].responsibility,
        witness.sources[1].responsibility
    );
    assert_ne!(
        witness.sources[0].context.use_id,
        witness.sources[1].context.use_id
    );
    assert_ne!(
        witness.sources[0].responsibility.use_id,
        witness.sources[0].context.use_id
    );

    let mut forged = witness.clone();
    forged.sources[0].context = witness.sources[1].context;
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::WrongContext
    );
    let mut forged = witness.clone();
    // Even an actual p root ID cannot stand in for the selected child use.
    forged.sources[0].context.use_id = witness.sources[0].responsibility.use_id;
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::WrongContext
    );
    let mut forged = witness.clone();
    forged.sources[0].conjunct_path = witness.sources[1].conjunct_path.clone();
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::WrongContext
    );
}

#[test]
fn input_occurrence_paths_and_column_traces_cannot_invent_local_transport() {
    let (package, witness) = fixture(false, 1);
    for edge in [
        PruningInputEdge {
            consumer: witness.sources[0].responsibility.site.node,
            input_ordinal: 1,
            producer: witness.target.scan,
        },
        PruningInputEdge {
            consumer: witness.target.scan,
            input_ordinal: 0,
            producer: witness.sources[0].responsibility.site.node,
        },
        PruningInputEdge {
            consumer: witness.sources[0].responsibility.site.node,
            input_ordinal: 0,
            producer: NodeId::new(u32::MAX),
        },
        PruningInputEdge {
            consumer: witness.sources[0].responsibility.site.node,
            input_ordinal: 0,
            producer: witness.sources[0].responsibility.site.node,
        },
    ] {
        let mut forged = witness.clone();
        forged.sources[0].input_path = Box::from([edge]);
        assert_eq!(
            PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
            PruningStructureError::InvalidPath
        );
    }
    let mut forged = witness.clone();
    forged.sources[0].input_path = Box::default();
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidPath
    );
    let mut forged = witness.clone();
    forged.sources[0].columns[0].column = ScanColumnId::new(1);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidColumn
    );
    let mut forged = witness.clone();
    forged.sources[0].columns[0].values = Box::from([witness.sources[0].columns[0].values[0]]);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidColumn
    );
    let mut forged = witness.clone();
    forged.sources[0].columns[0].values[1] = ValueId::new(u32::MAX);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidColumn
    );
    let mut forged = witness.clone();
    forged.sources[0].columns[0].values[0] = ValueId::new(u32::MAX);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidTransport
    );
    let mut forged = witness.clone();
    forged.sources[0].columns = Box::from([
        witness.sources[0].columns[0].clone(),
        witness.sources[0].columns[0].clone(),
    ]);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidColumn
    );
}
#[test]
fn every_domain_column_requires_a_trace_and_source_occurrences_are_unique() {
    let (package, witness) = fixture(false, 1);
    let mut forged = witness.clone();
    forged.sources[0].columns = Box::default();
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::MissingDomainColumn
    );
    let mut forged = witness.clone();
    forged.sources = Box::from([witness.sources[0].clone(), witness.sources[0].clone()]);
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::DuplicateSource
    );
    let mut forged = witness.clone();
    forged.sources = Box::default();
    assert_eq!(
        PruningDomainStructure::try_new(&package, &forged, &Control::default()).unwrap_err(),
        PruningStructureError::EmptySources
    );
}

fn shared_scan_fixture(two_parents: bool) -> (FragmentPackage, PruningDomainWitness) {
    let mut builder = FragmentBuilder::new(FragmentId::new(932));
    let (scan, value) = append_scan(&mut builder, false);
    let (left, right) = if two_parents {
        (
            append_filter(&mut builder, scan, value, 1),
            append_filter(&mut builder, scan, value, 1),
        )
    } else {
        (scan, scan)
    };
    let union = builder.reserve_node_id().unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: union,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([properties(), properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: union,
                columns: Box::from([value]),
            },
            // Pruning ownership is independent of input order. UNION ALL
            // keeps both occurrences without a build-before-probe data gate.
            kind: NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: Box::from([Box::from([value]), Box::from([value])]),
            },
        })
        .unwrap();
    let source = if two_parents {
        left
    } else {
        append_filter(&mut builder, union, value, 1)
    };
    let root = if two_parents { union } else { source };
    let package = package(builder, root, scan);
    let input_path: Box<[PruningInputEdge]> = if two_parents {
        Box::from([PruningInputEdge {
            consumer: left,
            input_ordinal: 0,
            producer: scan,
        }])
    } else {
        Box::from([
            PruningInputEdge {
                consumer: source,
                input_ordinal: 0,
                producer: union,
            },
            PruningInputEdge {
                consumer: union,
                input_ordinal: 0,
                producer: scan,
            },
        ])
    };
    let values = vec![value; input_path.len() + 1].into_boxed_slice();
    let (responsibility, context) = source_claim(
        &package,
        ExpressionRootSite {
            node: source,
            role: ExpressionRootRole::FilterPredicate { predicate: 0 },
        },
        &[],
    );
    let witness = PruningDomainWitness {
        target: PruningDomainSite {
            fragment: package.fragment().id(),
            scan,
            occurrence: ProviderReadOccurrenceId::new(17),
            field: PruningDomainField::Enforced,
        },
        sources: Box::from([PruningSourceWitness {
            responsibility,
            context,
            conjunct_path: Box::default(),
            input_path,
            columns: Box::from([PruningColumnTrace {
                column: ScanColumnId::new(0),
                values,
            }]),
        }]),
    };
    (package, witness)
}
#[test]
fn shared_scan_is_rejected_for_distinct_parents_and_for_two_input_occurrences_of_one_parent() {
    for two_parents in [true, false] {
        let (package, witness) = shared_scan_fixture(two_parents);
        // Both fixtures have passed the same real plan/cut/package validators.
        assert_eq!(
            PruningDomainStructure::try_new(&package, &witness, &Control::default()).unwrap_err(),
            PruningStructureError::SharedLocalProducer
        );
    }
}
#[test]
fn entry_and_midwork_control_errors_remain_outer_and_typed() {
    let (package, witness) = fixture(false, 300);
    PruningDomainStructure::try_new(&package, &witness, &Control::default()).unwrap();
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            failure: Some(failure.clone()),
            ..Default::default()
        };
        assert_eq!(
            PruningDomainStructure::try_new(&package, &witness, &control).unwrap_err(),
            PruningStructureError::Control(failure.clone())
        );
        assert_eq!(*control.work.lock().unwrap(), [0]);
        let control = Control {
            failure: Some(failure.clone()),
            positive_only: true,
            ..Default::default()
        };
        assert_eq!(
            PruningDomainStructure::try_new(&package, &witness, &control).unwrap_err(),
            PruningStructureError::Control(failure)
        );
        assert_eq!(*control.work.lock().unwrap(), [0, 256]);
    }
}
#[test]
fn large_witness_preflight_precedes_projection_and_exact_bound_reaches_shape_validation() {
    let (package, witness) = fixture(false, 1);
    let mut oversized = witness.clone();
    oversized.sources =
        vec![witness.sources[0].clone(); MAX_CONTROL_USE_REFERENCES + 1].into_boxed_slice();
    let mut oversized_trace = witness.clone();
    oversized_trace.sources[0].columns[0].values =
        vec![ValueId::new(0); MAX_CONTROL_USE_REFERENCES].into_boxed_slice();
    let mut deep = witness.clone();
    deep.sources[0].conjunct_path = vec![0; MAX_CONTROL_DEPTH].into_boxed_slice();
    for forged in [oversized, oversized_trace, deep] {
        let control = Control::default();
        assert_eq!(
            PruningDomainStructure::try_new(&package, &forged, &control).unwrap_err(),
            PruningStructureError::TooLarge
        );
        assert_eq!(*control.work.lock().unwrap(), [0]);
    }
    let mut exact = witness.clone();
    // 1 source + 1 edge + 1 trace + trace values = the preflight limit.
    // This malformed trace deliberately claims no valid-program acceptance.
    exact.sources[0].columns[0].values =
        vec![ValueId::new(0); MAX_CONTROL_USE_REFERENCES - 3].into_boxed_slice();
    assert_eq!(
        PruningDomainStructure::try_new(&package, &exact, &Control::default()).unwrap_err(),
        PruningStructureError::InvalidColumn
    );
}

fn pruning_table(
    package: &FragmentPackage,
    witnesses: Vec<PruningDomainWitness>,
) -> FrozenFragmentPruning {
    FrozenFragmentPruning::try_new(package.fragment().id(), witnesses, &Control::default()).unwrap()
}

#[test]
fn mandatory_package_pruning_preserves_actual_witnesses_and_full_source_claims() {
    let (package, witness) = fixture(false, 1);
    assert!(package.pruning().witnesses().is_empty());
    let expected = pruning_table(&package, vec![witness.clone()]);
    let mut input = package.into_input();
    input.pruning = expected.clone();
    let package =
        FragmentPackage::try_new(input, package_admission(), &Control::default()).unwrap();
    assert_eq!(package.pruning(), &expected);
    assert_eq!(package.pruning().fragment(), package.fragment().id());
    assert_eq!(
        package.pruning().witnesses(),
        std::slice::from_ref(&witness)
    );
    let actual = &package.pruning().witnesses()[0];
    let checked = PruningDomainStructure::try_new(&package, actual, &Control::default()).unwrap();
    assert_eq!(
        checked.sources()[0].responsibility().anchor(),
        witness.sources[0].responsibility
    );
    assert_eq!(checked.sources()[0].context(), witness.sources[0].context);
    assert_eq!(
        checked.sources()[0].argument_ordinals(),
        witness.sources[0].conjunct_path.as_ref()
    );
}

#[test]
fn mandatory_package_pruning_rejects_invalid_target_paths_and_occurrence_claims() {
    let (package, witness) = fixture(false, 2);
    let mut invalid_scan = witness.clone();
    invalid_scan.target.scan = NodeId::new(u32::MAX);
    let mut invalid_occurrence = witness.clone();
    invalid_occurrence.target.occurrence = ProviderReadOccurrenceId::new(18);
    let mut invalid_path = witness.clone();
    invalid_path.sources[0].input_path = Box::default();
    let mut invalid_context = witness.clone();
    invalid_context.sources[0].context = witness.sources[1].context;
    let mut invalid_responsibility = witness.clone();
    invalid_responsibility.sources[0].responsibility.use_id =
        witness.sources[1].responsibility.use_id;
    for (witness, error) in [
        (invalid_scan, PruningStructureError::InvalidScan),
        (invalid_occurrence, PruningStructureError::InvalidOccurrence),
        (invalid_path, PruningStructureError::InvalidPath),
        (invalid_context, PruningStructureError::WrongContext),
        (
            invalid_responsibility,
            PruningStructureError::WrongResponsibility,
        ),
    ] {
        let mut input = package.clone().into_input();
        // The table constructor checks the bounded declaration shape only.
        input.pruning = pruning_table(&package, vec![witness]);
        assert_eq!(
            FragmentPackage::try_new(input, package_admission(), &Control::default()).unwrap_err(),
            FragmentPackageError::Pruning(FrozenPruningError::Structure(error))
        );
    }
    let mut foreign = witness.clone();
    foreign.target.fragment = FragmentId::new(999);
    let mut input = package.into_input();
    input.pruning =
        FrozenFragmentPruning::try_new(foreign.target.fragment, vec![foreign], &Control::default())
            .unwrap();
    assert_eq!(
        FragmentPackage::try_new(input, package_admission(), &Control::default()).unwrap_err(),
        FragmentPackageError::Pruning(FrozenPruningError::WrongFragment)
    );
}

#[test]
fn frozen_pruning_constructor_rejects_duplicate_target_foreign_fragment_and_empty_sources() {
    let (package, witness) = fixture(false, 1);
    let mut foreign = witness.clone();
    foreign.target.fragment = FragmentId::new(999);
    let mut empty = witness.clone();
    empty.sources = Box::default();
    for (witnesses, error) in [
        (
            vec![witness.clone(), witness],
            FrozenPruningError::DuplicateTarget,
        ),
        (vec![foreign], FrozenPruningError::WrongFragment),
        (vec![empty], FrozenPruningError::EmptySources),
    ] {
        assert_eq!(
            FrozenFragmentPruning::try_new(package.fragment().id(), witnesses, &Control::default())
                .unwrap_err(),
            error
        );
    }
}

#[test]
fn frozen_pruning_reference_limit_is_combined_across_distinct_targets() {
    let (package, witness) = fixture(false, 1);
    let mut enforced = witness.clone();
    let mut unenforced = witness;
    unenforced.target.field = PruningDomainField::Unenforced;
    // Two targets + two sources + two edges + two traces = eight references.
    // Trace lengths here exercise declaration counting, not semantic validity.
    let values_per_target = (MAX_CONTROL_USE_REFERENCES - 8) / 2;
    enforced.sources[0].columns[0].values = vec![ValueId::new(0); values_per_target].into();
    unenforced.sources[0].columns[0].values = vec![ValueId::new(0); values_per_target].into();
    for witness in [&enforced, &unenforced] {
        assert_eq!(
            pruning_table(&package, vec![witness.clone()])
                .dynamic_items_observed(&Control::default())
                .unwrap(),
            MAX_CONTROL_USE_REFERENCES / 2
        );
    }
    let exact = pruning_table(&package, vec![enforced.clone(), unenforced.clone()]);
    assert_eq!(
        exact.dynamic_items_observed(&Control::default()).unwrap(),
        MAX_CONTROL_USE_REFERENCES
    );
    let mut near = unenforced.clone();
    near.sources[0].columns[0].values = vec![ValueId::new(0); values_per_target - 1].into();
    assert_eq!(
        pruning_table(&package, vec![enforced.clone(), near])
            .dynamic_items_observed(&Control::default())
            .unwrap(),
        MAX_CONTROL_USE_REFERENCES - 1
    );
    unenforced.sources[0].columns[0].values = vec![ValueId::new(0); values_per_target + 1].into();
    assert_eq!(
        FrozenFragmentPruning::try_new(
            package.fragment().id(),
            vec![enforced, unenforced],
            &Control::default(),
        )
        .unwrap_err(),
        FrozenPruningError::TooLarge
    );
}

#[test]
fn frozen_pruning_and_mandatory_package_preserve_typed_entry_and_first_positive_work_failures() {
    let (package, witness) = fixture(false, 300);
    let table = pruning_table(&package, vec![witness.clone()]);
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = Control {
                failure: Some(failure.clone()),
                positive_only,
                ..Default::default()
            };
            assert_eq!(
                FrozenFragmentPruning::try_new(
                    package.fragment().id(),
                    vec![witness.clone()],
                    &control,
                )
                .unwrap_err(),
                FrozenPruningError::Control(failure.clone())
            );
            assert_eq!(
                *control.work.lock().unwrap(),
                if positive_only { vec![0, 256] } else { vec![0] }
            );
            let control = Control {
                failure: Some(failure.clone()),
                positive_only,
                ..Default::default()
            };
            assert_eq!(
                table.dynamic_items_observed(&control).unwrap_err(),
                FrozenPruningError::Control(failure.clone())
            );
            assert_eq!(
                *control.work.lock().unwrap(),
                if positive_only { vec![0, 256] } else { vec![0] }
            );
            let control = Control {
                failure: Some(failure.clone()),
                positive_only,
                ..Default::default()
            };
            let mut input = package.clone().into_input();
            input.pruning = table.clone();
            assert_eq!(
                FragmentPackage::try_new(input, package_admission(), &control).unwrap_err(),
                FragmentPackageError::Control(failure.clone())
            );
            let work = control.work.lock().unwrap();
            assert_eq!(work.first(), Some(&0));
            if positive_only {
                // Package admission now observes a short preflight before the
                // pruning walk. A first-positive refusal must stop there;
                // the two direct pruning ports above still exercise real256.
                let last = *work.last().unwrap();
                assert!((1..=256).contains(&last));
                assert!(work[..work.len() - 1].iter().all(|units| *units == 0));
            } else {
                assert_eq!(work.as_slice(), &[0]);
            }
        }
    }
}

#[test]
fn two_real_domain_targets_share_one_actual_consumer_index() {
    let (package, enforced) = fixture(false, 1);
    let mut unenforced = enforced.clone();
    unenforced.target.field = PruningDomainField::Unenforced;
    let single = pruning_table(&package, vec![enforced.clone()]);
    let other = pruning_table(&package, vec![unenforced.clone()]);
    let both = pruning_table(&package, vec![enforced, unenforced]);
    let observed_units = |table: &FrozenFragmentPruning| -> u32 {
        let control = Control::default();
        table.validate_package(&package, &control).unwrap();
        let work = control.work.lock().unwrap();
        work.iter().sum()
    };
    let index_units: u32 = package
        .fragment()
        .nodes()
        .values()
        .map(|node| 1 + node.inputs.len() as u32)
        .sum();
    assert!(index_units > 0);
    assert_eq!(
        observed_units(&both),
        observed_units(&single) + observed_units(&other) - index_units
    );
    let mut input = package.into_input();
    input.pruning = both.clone();
    let installed =
        FragmentPackage::try_new(input, package_admission(), &Control::default()).unwrap();
    assert_eq!(installed.pruning(), &both);
}

#[test]
fn consumer_index_cannot_be_reused_for_a_different_package_with_the_same_fragment_id() {
    let (package, witness) = fixture(false, 1);
    let other = package.clone();
    assert_eq!(package.fragment().id(), other.fragment().id());
    let control = Control::default();
    let mut work = PruningWork::try_new(&control).unwrap();
    let index = PruningConsumerIndex::try_new(&package, &mut work).unwrap();
    assert_eq!(
        PruningDomainStructure::try_new_indexed(&other, &witness, &index, &control, &mut work)
            .unwrap_err(),
        PruningStructureError::WrongSnapshot,
    );
}

// Explicit small-fixture source invoice and independent property projection
// ceilings. These are test inputs, not a production default or MEM grant.
fn package_admission() -> crate::FragmentPackageAdmission {
    crate::FragmentPackageAdmission {
        plan_limits: crate::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: crate::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

mod owned_tests {
    include!("owned_tests.rs");
}
