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
use crate::physical_connector_payload_v2::ConnectorPayloadCodecError;
use crate::physical_provider_binding_v2::ProviderBindingCodecError;
use crate::physical_provider_read_v2::ProviderReadCodecError;
use crate::physical_public_source_schema_v2::PublicSourceSchemaCodecError;
use crate::physical_read_scan_v2::ReadScanCodecError;
use crate::physical_relation_v2::RelationCodecError;
use crate::physical_schema_v2::SchemaCodecError;
use crate::physical_type_v2::TypeCodecError;
use crate::physical_writer_recipe_v2::WriterRecipeCodecError;

use crate::physical_connector_payload_v2::{self, ConnectorPayloadProjectionLimits};
use crate::physical_node_v2::{NodeCodecError, NodeProjectionFacts, NodeProjectionLimits};
use crate::physical_package_v2::type_sources::{
    PackageTypeChannel as Channel, PackageTypeOccurrence, PackageTypeOwner,
};
use crate::physical_package_v2::type_views::{
    TypeViewBudget, TypeViewFacts, TypeViewLimits, collect_package_type_views_in,
};
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_provider_binding_v2::{
    self, ProviderBindingProjectionLimits, ProviderBindingSource,
};
use crate::physical_provider_read_v2::{self, ProviderReadProjectionLimits};
use crate::physical_read_scan_v2::{self, ReadScanEncodeContext, ReadScanProjectionLimits};
use crate::physical_relation_v2::{self, RelationProjectionLimits};
use crate::physical_schema_v2;
use crate::physical_type_v2::{self, PackageTypeProjectionLimits};
use crate::physical_writer_recipe_v2::{
    self, WriterRecipeEncodeContext, WriterRecipeProjectionLimits,
};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_connector_contract as c;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::*;
use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

// Conservative invoice for these bounded fresh fixtures and their projections.
// It is not a general retained-capacity measurement or allocator/MEM grant.
const SOURCE: usize = 128 * 1024 * 1024;
const REQUEST_BYTES: usize = 512 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Mutex<Option<(usize, CompileControlError)>>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = *self.stop.lock().unwrap() {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = self.stop.lock().unwrap().clone()
            && stop == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn ty() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn properties() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Unconstrained,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn admission() -> p::FragmentPackageAdmission {
    p::FragmentPackageAdmission {
        plan_limits: p::PlanLimits::FROZEN,
        source_retained_bytes: SOURCE,
        property_projection_limits: p::PropertyProofProjectionLimits {
            max_request_bytes: REQUEST_BYTES,
            max_coexisting_bytes: SOURCE + REQUEST_BYTES,
            max_projection_work: usize::MAX / 4,
        },
    }
}

pub(in crate::physical_package_v2) fn checked_read(metadata: bool) -> p::FragmentPackage {
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let binding = c::ConnectorReadBinding::new(
        c::ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = |category, byte| {
        c::ConnectorEncodedPayload::new(
            c::ConnectorEnvelopeHeader::new(
                provider.clone(),
                catalog.clone(),
                category,
                c::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![byte].into(),
        )
    };
    let read = p::ProviderReadReference {
        binding,
        input_version: c::ConnectorReadInputVersion::try_new(vec![9]).unwrap(),
        relation: c::ConnectorReadRelationPayload::new(
            if metadata {
                c::ConnectorReadRelationKind::SystemTable
            } else {
                c::ConnectorReadRelationKind::Table
            },
            payload(c::ConnectorCodecCategory::ReadTable, 1),
            payload(c::ConnectorCodecCategory::ReadView, 2),
        ),
    };
    let field = p::RelationField {
        column: p::ProviderColumnReference {
            column_payload: payload(c::ConnectorCodecCategory::ReadColumn, 3),
        },
        ty: ty(),
    };
    let relation = if metadata {
        p::Relation::Metadata(p::MetadataRelation {
            kind: p::MetadataRelationKind::try_new("iceberg.manifest.entries").unwrap(),
            read,

            selection_digest: [8; 32],
            schema: Box::from([field]),
            predicate_guarantees: Box::default(),
            provided_properties: properties(),
            coverage_evidence: Box::from([4]),
        })
    } else {
        p::Relation::Data(p::DataRelation {
            read,

            selection_digest: [8; 32],
            schema: Box::from([field]),
            predicate_guarantees: Box::default(),
            provided_properties: properties(),
        })
    };
    let column = relation.schema()[0].column.clone();
    let scan = p::NodeId::new(u32::MAX);
    let value = p::ValueId::new(0);
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            stage_placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            id: p::FragmentId::new(71),
            root: scan,
            values: BTreeMap::from([(
                value,
                p::ValueDef {
                    id: value,
                    ty: ty(),
                    origin: p::ValueOrigin::ProviderField {
                        scan_node: scan,
                        field: column.clone(),
                    },
                },
            )]),
            expressions: p::ExprArena::try_from_definitions_observed(
                std::iter::empty(),
                &p::PlanLimits::FROZEN,
                &Setup,
            )
            .unwrap(),
            nodes: BTreeMap::from([(
                scan,
                p::PhysicalNode {
                    id: scan,
                    inputs: Box::default(),
                    required_inputs: Box::default(),
                    output_properties: properties(),
                    output: p::OutputPort {
                        node: scan,
                        columns: Box::from([value]),
                    },
                    kind: p::NodeKind::Scan {
                        occurrence: p::ProviderReadOccurrenceId::new(0),
                        relation: Box::new(relation),
                        read_budget: p::ScanReadBudget {
                            max_batch_rows: 4096,
                            max_batch_bytes: 4 * 1024 * 1024,
                        },
                        provider_outputs: Box::from([(column, value)]),
                        residuals: Box::default(),
                        derived_values: Box::default(),
                    },
                },
            )]),
            sink: p::FragmentSink::Noop,
            dop_domain: p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            runtime_filters: Box::default(),
        },
        p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let p::NodeKind::Scan { relation, .. } = &fragment.nodes()[&scan].kind else {
        unreachable!()
    };
    let original = relation.read();
    // The original scan recipe copies equal metadata carriers, creating a
    // second outer binding/table/view/column family. They must not be interned.
    let recipe = c::ConnectorReadRelationRecipeDraft::try_new(
        original.binding.clone(),
        original.relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let scan_owner = c::FrozenConnectorScan::try_new(
        recipe,
        vec![c::StaticScanAssignment::new(
            Arc::from("v0"),
            c::ConnectorValueType::BigInt,
        )],
        c::TupleDomain::all(),
        c::TupleDomain::all(),
        Some(c::ConnectorExpression::Call {
            function: c::ConnectorFunctionName::try_new("$equal").unwrap(),
            value_type: c::ConnectorValueType::Boolean,
            arguments: vec![
                c::ConnectorExpression::Variable {
                    name: Arc::from("v0"),
                    value_type: c::ConnectorValueType::BigInt,
                },
                c::ConnectorExpression::Constant {
                    value: None,
                    value_type: c::ConnectorValueType::BigInt,
                },
            ],
        }),
        vec![],
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(4 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let facts = c::ConnectorReadStaticFacts::try_new(
        original.input_version.clone(),
        [8; 32],
        c::ConnectorReadProperties::try_new(c::ConnectorReadDistribution::Unconstrained, vec![])
            .unwrap(),
        c::ConnectorReadArtifactCoverage::NoArtifactInputs,
        if metadata { vec![4] } else { vec![] },
    )
    .unwrap();
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("v0", DataType::Int64, false)
                .with_metadata(HashMap::from([("note".into(), "original\0field".into())])),
        ],
        HashMap::from([("schema".into(), "read\0metadata".into())]),
    );
    // Actual explicit execution declaration; generic format attributes belong
    // only to the independent source-facts namespace.
    let pure = ValueTypeStorageLifetime::pure();
    let execution = c::ConnectorReadExecutionSchema::try_new(
        Arc::new(Schema::new(vec![Field::new("v0", DataType::Int64, false)])),
        vec![ty()].into_boxed_slice(),
        pure.clone(),
    )
    .unwrap();
    let shape = c::ConnectorSourceSchemaShape::try_new(
        Arc::new(Schema::new(vec![Field::new("v0", DataType::Int64, false)])),
        pure.clone(),
    )
    .unwrap();
    let attrs = |original: &arrow::datatypes::Metadata| {
        // Borrow Arrow attributes, then author the independent domain HashMap
        // with its original reserve and copied String invoice.
        let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
            String,
            String,
        >(original.len())
        .unwrap();
        let mut copied = HashMap::with_capacity(original.len());
        let mut invoice = table.request_bytes_upper_bound;
        for (key, value) in original {
            let key = key.clone();
            let value = value.clone();
            invoice += key.capacity() + value.capacity();
            copied.insert(key, value);
        }
        (copied, invoice)
    };
    let (field_attrs, field_invoice) = attrs(schema.field(0).metadata());
    let field_fact = c::ConnectorSourceFieldFacts::try_new_observed(
        vec![0].into(),
        None,
        None,
        field_attrs,
        &[],
        pure.clone(),
        field_invoice,
        |_| Ok::<_, c::ConnectorError>(()),
        |_| Ok(()),
    )
    .unwrap();
    let (schema_attrs, schema_invoice) = attrs(schema.metadata());
    let source_facts = c::ConnectorSourceSchemaFacts::try_new_observed_with_source_invoice(
        shape,
        schema_attrs,
        vec![field_fact].into_boxed_slice(),
        pure,
        schema_invoice,
        |_| Ok::<_, c::ConnectorError>(()),
        |_| Ok(()),
    )
    .unwrap();
    let public = c::ConnectorReadPublicFacts::try_new_typed_with_control(
        facts,
        metadata
            .then(|| c::ConnectorReadMetadataKind::try_new("iceberg.manifest.entries").unwrap()),
        &execution,
        &source_facts,
        &Setup,
    )
    .unwrap();
    let frozen = c::FrozenConnectorRead::try_new(scan_owner, public).unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(&fragment, flow, vec![], &Setup).unwrap();
    let calls = p::FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Setup).unwrap();
    let pruning = p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            constants: p::ConstantPools::empty(),
            version: p::PlanVersionId::try_new([7; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            fragment,
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
                runtime_filter_bindings: Box::default(),
            },
            result: None,
            expression_uses: uses,
            calls,
            pruning,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::from([(scan, frozen)]),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        admission(),
        &Setup,
    )
    .unwrap()
}
pub(in crate::physical_package_v2) fn checked_writer() -> p::FragmentPackage {
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
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
        vec![7].into(),
    );
    let recipe = c::ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        c::ConnectorWriteInputShape::Data {
            fields: vec![
                c::ConnectorWriteFieldBinding::try_new_typed(
                    c::ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("v", DataType::Int64, false).with_metadata(HashMap::from([
                        ("large".into(), "雪".repeat(6826) + "ab"),
                        ("embedded".into(), "a\0b".into()),
                    ])),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .unwrap(),
            ],
        },
    )
    .unwrap();
    crate::physical_type_v2::sender_tests::checked_writer_package(recipe)
}

fn limits() -> TypeViewLimits {
    TypeViewLimits {
        max_occurrences: 100_000,
        max_value_roots: 100_000,
        max_field_roots: 100_000,
        max_writer_recipes: 1000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn type_limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn binding_limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 1000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 1000,
        max_payload_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn read_limits() -> ProviderReadProjectionLimits {
    ProviderReadProjectionLimits {
        max_definitions: 1000,
        max_input_version_bytes: 1024,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn property_limits() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 1000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn node_limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 1000,
        max_value_references: 1000,
        max_list_items: 100_000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
        properties: property_limits(),
    }
}
fn relation_limits() -> RelationProjectionLimits {
    RelationProjectionLimits {
        max_definitions: 1000,
        max_schema_fields: 1000,
        max_predicate_guarantees: 1000,
        max_metadata_kind_bytes: 1024,
        max_coverage_bytes: 1024,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
        properties: property_limits(),
    }
}
fn scan_limits() -> ReadScanProjectionLimits {
    ReadScanProjectionLimits {
        max_scans: 1000,
        max_items: 100_000,
        max_scalar_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn writer_limits() -> WriterRecipeProjectionLimits {
    WriterRecipeProjectionLimits {
        max_recipes: 1000,
        max_input_fields: 1000,
        max_token_bytes: 32_000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn resource_axes(
    requests: usize,
    bytes: usize,
    coexist: usize,
    work: usize,
) -> Result<(), CompileControlError> {
    resource_axes_at(SOURCE, requests, bytes, coexist, work)
}
fn resource_axes_at(
    source: usize,
    requests: usize,
    bytes: usize,
    coexist: usize,
    work: usize,
) -> Result<(), CompileControlError> {
    if requests > 100_000
        || bytes > REQUEST_BYTES
        || coexist != source + bytes
        || work > usize::MAX / 4
    {
        return Err(CompileControlError::ResourceExhausted);
    }
    Ok(())
}
fn node_admit_at(source: usize, f: &NodeProjectionFacts) -> Result<(), CompileControlError> {
    assert!(
        f.input_node_count <= 1000
            && f.value_reference_count <= 1000
            && f.list_item_count <= 100_000
    );
    resource_axes_at(
        source,
        f.allocation_requests_upper_bound,
        f.allocation_request_bytes_upper_bound,
        f.coexisting_source_and_request_bytes_upper_bound,
        f.cumulative_work_upper_bound,
    )
}
#[derive(Debug)]
enum Failure {
    Control(CompileControlError),
    Ordinary(String),
}
impl From<CompileControlError> for Failure {
    fn from(c: CompileControlError) -> Self {
        Self::Control(c)
    }
}
macro_rules! error_from {($($t:ident),+)=>{$(impl From<$t> for Failure {fn from(e:$t)->Self {match e {$t::Control(c)=>Self::Control(c),other=>Self::Ordinary(other.to_string())}}})+};}
error_from!(
    ProviderSourceError,
    TypeViewError,
    TypeCodecError,
    ProviderBindingCodecError,
    ConnectorPayloadCodecError,
    ProviderReadCodecError,
    RelationCodecError,
    SchemaCodecError,
    NodeCodecError,
    ReadScanCodecError,
    PublicSourceSchemaCodecError,
    WriterRecipeCodecError
);
fn finish<T>(result: Result<T, Failure>, work: CompileCheckpoints<'_>) -> Result<T, Failure> {
    if matches!(result, Err(Failure::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn source_limits() -> ProviderSourceLimits {
    ProviderSourceLimits {
        max_provider_occurrences: 1000,
        max_payload_occurrences: 1000,
        max_read_occurrences: 1000,
        max_relation_occurrences: 1000,
        max_schema_occurrences: 1000,
        max_scan_occurrences: 1000,
        max_writer_occurrences: 1000,
        max_relation_fields: 1000,
        max_schema_fields: 1000,
        max_connector_expression_occurrences: 1000,
    }
}
fn payload_id(
    inputs: &[(u32, &c::ConnectorEncodedPayload)],
    original: &c::ConnectorEncodedPayload,
) -> u32 {
    let matched = inputs
        .iter()
        .filter(|(_, loan)| std::ptr::eq(*loan, original))
        .collect::<Vec<_>>();
    assert_eq!(matched.len(), 1, "exact outer payload occurs once");
    matched[0].0
}
fn read_provider_id(
    inputs: &[(u32, ProviderBindingSource<'_>)],
    original: &c::ConnectorReadBinding,
) -> u32 {
    let matched=inputs.iter().filter(|(_,loan)|matches!(loan,ProviderBindingSource::Read(actual) if std::ptr::eq(*actual,original))).collect::<Vec<_>>();
    assert_eq!(
        matched.len(),
        1,
        "exact Read role outer binding occurs once"
    );
    matched[0].0
}
fn write_provider_id(
    inputs: &[(u32, ProviderBindingSource<'_>)],
    original: &c::ConnectorWriteBinding,
) -> u32 {
    let matched=inputs.iter().filter(|(_,loan)|matches!(loan,ProviderBindingSource::Write(actual) if std::ptr::eq(*actual,original))).collect::<Vec<_>>();
    assert_eq!(
        matched.len(),
        1,
        "exact Write role outer binding occurs once"
    );
    matched[0].0
}
// The initial SOURCE covers this bounded fresh Package/Views/input fixture.
// Each completed namespace adds its original requested-backing upper and
// actual inline header once. Temporary requests make this conservative.
// Never use a retained lower floor as an upper or sum floors containing B.
fn next_stage_source<T>(source: usize, requested_bytes: usize, namespace: &T) -> usize {
    source
        .checked_add(requested_bytes)
        .unwrap()
        .checked_add(std::mem::size_of_val(namespace))
        .unwrap()
}

fn project(
    package: &p::FragmentPackage,
    control: &Control,
    view_limits: TypeViewLimits,
) -> Result<TypeViewFacts, Failure> {
    let original: &dyn PureCompileControl = control;
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Encode)?;
    let result = (|| {
        let mut previous = None;
        let mut parent = |f: &TypeViewFacts| {
            assert_eq!(
                f.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + f.allocation_request_bytes_upper_bound
            );
            if let Some(old) = previous {
                let old: TypeViewFacts = old;
                assert!(f.allocation_requests_upper_bound >= old.allocation_requests_upper_bound);
                assert!(
                    f.allocation_request_bytes_upper_bound
                        >= old.allocation_request_bytes_upper_bound
                );
                assert!(f.cumulative_work_upper_bound >= old.cumulative_work_upper_bound);
            }
            resource_axes(
                f.allocation_requests_upper_bound,
                f.allocation_request_bytes_upper_bound,
                f.coexisting_source_and_request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            )?;
            previous = Some(*f);
            Ok(())
        };
        let mut budget = TypeViewBudget::new_in(package, SOURCE, view_limits, &mut parent, &work)?;
        let views = collect_package_type_views_in(&mut budget, &mut work)?;
        let writer_types = views.writer_sources_in(&mut budget, &mut work)?;
        let sources = collect_provider_sources_in(
            package,
            &views,
            &writer_types,
            source_limits(),
            &mut budget,
            &mut work,
        )?;
        let prepared = sources.prepare_inputs_in(&mut budget, &mut work)?;
        let type_bytes = std::cell::Cell::new(0usize);
        let types = physical_type_v2::encode_borrowed_type_namespaces_sources_in(
            views.values(),
            views.fields(),
            views.source_fields(),
            &writer_types,
            SOURCE,
            type_limits(),
            &mut |f| {
                type_bytes.set(type_bytes.get().max(f.allocation_request_bytes_upper_bound));
                resource_axes(
                    f.allocation_requests_upper_bound,
                    f.allocation_request_bytes_upper_bound,
                    f.coexisting_source_and_request_bytes_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            },
            &mut work,
        )?;
        let provider_source = next_stage_source(SOURCE, type_bytes.get(), &types);
        let providers = physical_provider_binding_v2::encode_joint_provider_bindings_in(
            sources.provider_inputs(),
            provider_source,
            ProviderBindingProjectionLimits {
                max_coexisting_source_and_request_bytes: provider_source + REQUEST_BYTES,
                ..binding_limits()
            },
            &mut |f| {
                assert_eq!(f.definition_count, sources.provider_inputs().len());
                resource_axes_at(
                    provider_source,
                    f.allocation_requests_upper_bound,
                    f.allocation_request_bytes_upper_bound,
                    f.coexisting_source_and_request_bytes_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            },
            &mut work,
        )?;
        let payload_source = next_stage_source(
            provider_source,
            providers.facts().allocation_request_bytes_upper_bound,
            &providers,
        );
        let payloads = physical_connector_payload_v2::encode_connector_payloads_in(
            sources.payload_inputs(),
            payload_source,
            ConnectorPayloadProjectionLimits {
                max_coexisting_source_and_request_bytes: payload_source + REQUEST_BYTES,
                ..payload_limits()
            },
            &mut |f| {
                assert_eq!(f.definition_count, sources.payload_inputs().len());
                resource_axes_at(
                    payload_source,
                    f.allocation_requests_upper_bound,
                    f.allocation_request_bytes_upper_bound,
                    f.coexisting_source_and_request_bytes_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            },
            &mut work,
        )?;
        let read_source = next_stage_source(
            payload_source,
            payloads.facts().allocation_request_bytes_upper_bound,
            &payloads,
        );
        let reads = physical_provider_read_v2::encode_provider_reads_in(
            sources.read_inputs(),
            &providers,
            &payloads,
            read_source,
            ProviderReadProjectionLimits {
                max_coexisting_source_and_request_bytes: read_source + REQUEST_BYTES,
                ..read_limits()
            },
            &mut |f| {
                resource_axes_at(
                    read_source,
                    f.allocation_requests_upper_bound,
                    f.allocation_request_bytes_upper_bound,
                    f.coexisting_source_and_request_bytes_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            },
            &mut work,
        )?;
        let relation_source = next_stage_source(
            read_source,
            reads.facts().allocation_request_bytes_upper_bound,
            &reads,
        );
        let relations = physical_relation_v2::encode_relations_in(
            prepared.relations(),
            &reads,
            &types,
            relation_source,
            RelationProjectionLimits {
                max_coexisting_source_and_request_bytes: relation_source + REQUEST_BYTES,
                properties: PhysicalPropertyProjectionLimits {
                    max_coexisting_source_and_request_bytes: relation_source + REQUEST_BYTES,
                    ..property_limits()
                },
                ..relation_limits()
            },
            &mut |f| {
                resource_axes_at(
                    relation_source,
                    f.allocation_requests_upper_bound,
                    f.allocation_request_bytes_upper_bound,
                    f.coexisting_source_and_request_bytes_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            },
            &mut work,
        )?;
        let schema_source = next_stage_source(
            relation_source,
            relations.facts().allocation_request_bytes_upper_bound,
            &relations,
        );
        let schemas = physical_schema_v2::prepare_schemas_encode_observed_in(
            prepared.schemas(),
            &types,
            schema_source,
            NodeProjectionLimits {
                max_coexisting_source_and_request_bytes: schema_source + REQUEST_BYTES,
                properties: PhysicalPropertyProjectionLimits {
                    max_coexisting_source_and_request_bytes: schema_source + REQUEST_BYTES,
                    ..property_limits()
                },
                ..node_limits()
            },
            &mut |f| node_admit_at(schema_source, f),
            &mut work,
        )?
        .emit_observed_in(&mut |f| node_admit_at(schema_source, f), &mut work)?;
        let terminal_source = next_stage_source(
            schema_source,
            schemas.facts().allocation_request_bytes_upper_bound,
            &schemas,
        );
        if let Some((node, read)) = package.scans().iter().next() {
            assert_eq!(
                (
                    sources.provider_inputs().len(),
                    sources.payload_inputs().len(),
                    sources.read_inputs().len()
                ),
                (2, 8, 1)
            );
            assert_eq!(
                (
                    prepared.relations().len(),
                    prepared.schemas().len(),
                    prepared.scans().len(),
                    prepared.writers().len()
                ),
                (1, 2, 1, 0)
            );
            let p::NodeKind::Scan {
                relation,
                provider_outputs,
                ..
            } = &package.fragment().nodes()[node].kind
            else {
                unreachable!()
            };
            let p::ValueOrigin::ProviderField { field, .. } =
                &package.fragment().values()[&p::ValueId::new(0)].origin
            else {
                unreachable!()
            };
            let recipe = read.scan().recipe();
            let a = &relation.read().binding;
            let b = recipe.binding();
            assert_eq!(a, b);
            assert!(!std::ptr::eq(a, b));
            let a_id = read_provider_id(sources.provider_inputs(), a);
            let b_id = read_provider_id(sources.provider_inputs(), b);
            assert_ne!(a_id, b_id);
            assert_eq!(providers.source_id_observed(a, &mut work)?, a_id);
            assert_eq!(providers.source_id_observed(b, &mut work)?, b_id);
            let columns = [
                &relation.schema()[0].column.column_payload,
                &provider_outputs[0].0.column_payload,
                &field.column_payload,
                &recipe.columns()[0],
            ];
            let mut column_ids = Vec::new();
            for original in columns {
                assert_eq!(original, columns[0]);
                let id = payload_id(sources.payload_inputs(), original);
                assert_eq!(payloads.source_id_observed(original, &mut work)?, id);
                assert_eq!(payloads.source_id_observed(original, &mut work)?, id);
                column_ids.push(id);
            }
            column_ids.sort_unstable();
            column_ids.dedup();
            assert_eq!(
                column_ids.len(),
                4,
                "four equal but different outer column owners"
            );
            assert!(std::ptr::eq(
                prepared.relations()[0].relation,
                relation.as_ref()
            ));
            assert!(std::ptr::eq(
                prepared.schemas()[0].source,
                read.public_facts().schema()
            ));
            assert!(std::ptr::eq(prepared.scans()[0].read, read));
            let relation_id = prepared.relations()[0].value_type_ids[0];
            assert!(std::ptr::eq(
                types.value_type_observed(relation_id, &mut work)?.unwrap(),
                &relation.schema()[0].ty
            ));
            let field_id = prepared.schemas()[0].field_ids[0];
            assert!(std::ptr::eq(
                types.field_source_observed(field_id, &mut work)?.unwrap(),
                read.public_facts().schema().fields()[0].as_ref()
            ));
            let raw_read = &reads.as_wire()[0];
            assert_eq!(raw_read.id, 0);
            assert_eq!(raw_read.provider_binding_id, Some(a_id));
            assert_eq!(raw_read.input_version, vec![9]);
            assert_eq!(
                raw_read.table_payload_id,
                Some(payload_id(
                    sources.payload_inputs(),
                    relation.read().relation.table()
                ))
            );
            assert_eq!(
                raw_read.view_payload_id,
                Some(payload_id(
                    sources.payload_inputs(),
                    relation.read().relation.view()
                ))
            );
            let (scan_wire, expression_wire, _) =
                physical_read_scan_v2::encode_read_scans_observed(
                    prepared.scans(),
                    ReadScanEncodeContext {
                        bindings: &providers,
                        payloads: &payloads,
                        schemas: &schemas,
                        source_schemas: prepared.source_schemas(),
                    },
                    terminal_source,
                    ReadScanProjectionLimits {
                        max_coexisting_source_and_request_bytes: terminal_source + REQUEST_BYTES,
                        ..scan_limits()
                    },
                    &mut |f| {
                        resource_axes_at(
                            terminal_source,
                            f.allocation_requests_upper_bound,
                            f.allocation_request_bytes_upper_bound,
                            f.coexisting_source_and_request_bytes_upper_bound,
                            f.cumulative_work_upper_bound,
                        )
                    },
                    &mut work,
                )?;
            assert_eq!(prepared.scans()[0].expression_ids, [0, 1, 2]);
            assert_eq!(
                expression_wire.iter().map(|e| e.id).collect::<Vec<_>>(),
                [1, 2, 0]
            );
            let Some(wire::connector_expression_definition::Kind::Call(call)) =
                &expression_wire.iter().find(|e| e.id == 0).unwrap().kind
            else {
                panic!("original residual Call root")
            };
            assert_eq!(call.function_name, "$equal");
            assert_eq!(call.argument_connector_expression_ids, [1, 2]);
            let Some(wire::connector_expression_definition::Kind::Variable(variable)) =
                &expression_wire.iter().find(|e| e.id == 1).unwrap().kind
            else {
                panic!("original residual Variable")
            };
            assert_eq!(variable.name, "v0");
            let Some(wire::connector_expression_definition::Kind::Constant(constant)) =
                &expression_wire.iter().find(|e| e.id == 2).unwrap().kind
            else {
                panic!("original typed NULL residual")
            };
            assert!(constant.value.is_none());
            assert_eq!(
                constant.value_type.as_ref().unwrap().kind,
                novarocks_proto_models::connector_read::ValueTypeKind::BigInt as i32
            );
            assert_eq!(scan_wire.len(), 1);
            assert_eq!(scan_wire[0].node_id, Some(u32::MAX));
            let raw_recipe = scan_wire[0].recipe.as_ref().unwrap();
            assert_eq!(raw_recipe.provider_binding_id, Some(b_id));
            assert_eq!(
                raw_recipe.column_payload_ids,
                vec![payload_id(sources.payload_inputs(), &recipe.columns()[0])]
            );
            assert_eq!(
                raw_recipe.table_payload_id,
                Some(payload_id(
                    sources.payload_inputs(),
                    recipe.relation().table()
                ))
            );
            assert_eq!(
                raw_recipe.view_payload_id,
                Some(payload_id(
                    sources.payload_inputs(),
                    recipe.relation().view()
                ))
            );
            let facts = scan_wire[0].facts.as_ref().unwrap();
            assert_eq!(
                (facts.max_batch_rows, facts.max_batch_bytes),
                (4096, 4 * 1024 * 1024)
            );
            assert!(facts.dynamic_filters.is_empty());
            assert_eq!(facts.remaining_connector_expression_id, Some(0));
            assert_eq!(facts.assignments[0].variable, "v0");
            let raw_schema = &schemas.as_wire()[0];
            assert_eq!(raw_schema.field_ids, vec![field_id]);
            match &relations.as_wire()[0].kind {
                Some(wire::relation_definition::Kind::Data(raw)) => {
                    assert!(matches!(relation.as_ref(), p::Relation::Data(_)));
                    assert_eq!(raw.schema[0].value_type_id, Some(relation_id));
                    assert_eq!(
                        raw.schema[0].column_payload_id,
                        Some(payload_id(sources.payload_inputs(), columns[0]))
                    );
                    assert_eq!(raw.selection_digest, vec![8; 32]);
                }
                Some(wire::relation_definition::Kind::Metadata(raw)) => {
                    assert!(matches!(relation.as_ref(), p::Relation::Metadata(_)));
                    assert_eq!(raw.kind, "iceberg.manifest.entries");
                    assert_eq!(raw.coverage_evidence, vec![4]);
                    assert_eq!(raw.schema[0].value_type_id, Some(relation_id));
                }
                other => panic!("actual relation kind: {other:?}"),
            }
            let execution_field = types
                .as_wire()
                .fields
                .iter()
                .find(|field| field.id == field_id)
                .unwrap();
            assert!(execution_field.carrier_type_id.is_some());
            // Reserved shape slots cannot carry source attributes; the original
            // attributes remain in the independent source facts asserted below.
            let public = scan_wire[0].public_facts.as_ref().unwrap();
            assert_eq!(
                public.execution_schema_id,
                Some(prepared.scans()[0].schema_id)
            );
            assert_eq!(
                public.source_schema_id,
                Some(prepared.scans()[0].source_schema_id)
            );
            let source_wire =
                crate::physical_public_source_schema_v2::encode_public_source_schemas_pure_in(
                    prepared.source_schemas(),
                    &mut work,
                )?;
            assert_eq!(source_wire[0].schema_attributes[0].value, "read\0metadata");
            assert_eq!(
                source_wire[0].field_facts[0].attributes[0].value,
                "original\0field"
            );
        } else {
            let (node, recipe) = package.writes().iter().next().unwrap();
            let p::NodeKind::TableWriter { target } = &package.fragment().nodes()[node].kind else {
                unreachable!()
            };
            assert_eq!(
                (
                    sources.provider_inputs().len(),
                    sources.payload_inputs().len(),
                    sources.read_inputs().len()
                ),
                (1, 2, 0)
            );
            assert_eq!(
                (
                    prepared.relations().len(),
                    prepared.schemas().len(),
                    prepared.scans().len(),
                    prepared.writers().len()
                ),
                (0, 0, 0, 1)
            );
            assert_eq!(target.handle, *recipe.payload());
            assert!(!std::ptr::eq(&target.handle, recipe.payload()));
            let target_id = payload_id(sources.payload_inputs(), &target.handle);
            let draft_id = payload_id(sources.payload_inputs(), recipe.payload());
            assert_ne!(target_id, draft_id);
            assert_eq!(
                payloads.source_id_observed(&target.handle, &mut work)?,
                target_id
            );
            assert_eq!(
                payloads.source_id_observed(recipe.payload(), &mut work)?,
                draft_id
            );
            let provider = write_provider_id(sources.provider_inputs(), recipe.binding());
            assert_eq!(
                providers.write_source_id_observed(recipe.binding(), &mut work)?,
                provider
            );
            assert!(std::ptr::eq(prepared.writers()[0].recipe, recipe));
            let original = recipe.input().fields_iter().next().unwrap().field();
            let field_id = prepared.writers()[0].field_ids[0];
            assert!(std::ptr::eq(
                types
                    .source_field_source_observed(field_id, &mut work)?
                    .unwrap(),
                original
            ));
            let (writer_wire, _) = physical_writer_recipe_v2::encode_writer_recipes_observed(
                prepared.writers(),
                WriterRecipeEncodeContext {
                    bindings: &providers,
                    payloads: &payloads,
                    types: &types,
                },
                terminal_source,
                WriterRecipeProjectionLimits {
                    max_coexisting_source_and_request_bytes: terminal_source + REQUEST_BYTES,
                    ..writer_limits()
                },
                &mut |f| {
                    resource_axes_at(
                        terminal_source,
                        f.allocation_requests_upper_bound,
                        f.allocation_request_bytes_upper_bound,
                        f.coexisting_source_and_request_bytes_upper_bound,
                        f.cumulative_work_upper_bound,
                    )
                },
                &mut work,
            )?;
            assert_eq!(writer_wire.len(), 1);
            assert_eq!(writer_wire[0].node_id, Some(node.get()));
            assert_eq!(writer_wire[0].provider_binding_id, Some(provider));
            assert_eq!(writer_wire[0].handle_payload_id, Some(draft_id));
            let Some(wire::connector_write_input_shape::Kind::Data(data)) =
                writer_wire[0].input.as_ref().unwrap().kind.as_ref()
            else {
                panic!("checked original Data role")
            };
            assert_eq!(data.fields[0].source_field_id, Some(field_id));
            assert_eq!(data.fields[0].field_token, vec![1; 32]);
            let source_table = types.as_source_wire().unwrap();
            let field = source_table
                .shape
                .as_ref()
                .unwrap()
                .fields
                .iter()
                .find(|field| field.id == field_id)
                .unwrap();
            assert_eq!(field.name, "v");
            assert!(field.logical_type_id.is_none() && field.value_binding_id.is_none());
            let value_id = types
                .source_field_value_type_id_observed(field_id, &mut work)?
                .unwrap();
            assert_eq!(data.fields[0].value_type_id, Some(value_id));
            assert!(std::ptr::eq(
                types.value_type_observed(value_id, &mut work)?.unwrap(),
                recipe
                    .input()
                    .fields_iter()
                    .next()
                    .unwrap()
                    .execution_type()
                    .unwrap()
            ));
            let field = source_table
                .field_facts
                .iter()
                .find(|facts| facts.field_id == Some(field_id))
                .unwrap();

            assert_eq!(
                field
                    .metadata
                    .iter()
                    .find(|m| m.key == "large")
                    .unwrap()
                    .value
                    .len(),
                20 * 1024
            );
            assert_eq!(
                field
                    .metadata
                    .iter()
                    .find(|m| m.key == "embedded")
                    .unwrap()
                    .value,
                "a\0b"
            );
        }
        for (index, (id, source)) in sources.payload_inputs().iter().enumerate() {
            assert_eq!(*id, u32::try_from(index).unwrap());
            assert_eq!(
                payloads.as_wire()[index]
                    .payload
                    .as_ref()
                    .unwrap()
                    .payload
                    .as_slice(),
                source.payload().as_ref()
            );
        }
        Ok(budget.facts())
    })();
    finish(result, work)
}

#[test]
fn actual_checked_data_and_metadata_read_sources_preserve_all_outer_payloads_and_feed_original_encoders()
 {
    for metadata in [false, true] {
        let package = checked_read(metadata);
        project(&package, &Control::default(), limits()).unwrap();
    }
}
#[test]
fn actual_checked_data_writer_preserves_node_handle_draft_and_large_field_loans() {
    project(&checked_writer(), &Control::default(), limits()).unwrap();
}
#[test]
fn actual_small_success_consumer_callbacks_keep_three_primary_causes_without_footer() {
    for package in [checked_read(false), checked_writer()] {
        let c = Control::default();
        project(&package, &c, limits()).unwrap();
        let trace = c.trace.into_inner().unwrap();
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let c = Control {
                    stop: Mutex::new(Some((at, cause.clone()))),
                    ..Default::default()
                };
                assert!(
                    matches!(project(&package,&c,limits()),Err(Failure::Control(actual)) if actual==cause)
                );
                assert_eq!(c.trace.into_inner().unwrap(), trace[..=at]);
            }
        }
    }
}
fn exact(f: TypeViewFacts) -> TypeViewLimits {
    TypeViewLimits {
        max_occurrences: f.occurrence_count,
        max_value_roots: f.value_root_count,
        max_field_roots: f.field_root_count,
        max_writer_recipes: f.writer_recipe_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
#[test]
fn actual_source_inputs_exact_replay_and_each_nonzero_axis_one_under_refuse() {
    for package in [checked_read(true), checked_writer()] {
        let facts = project(&package, &Control::default(), limits()).unwrap();
        assert_eq!(
            project(&package, &Control::default(), exact(facts)).unwrap(),
            facts
        );
        for axis in [0, 1, 2, 3, 4, 5, 6, 7] {
            let mut l = exact(facts);
            let cap = match axis {
                0 => &mut l.max_occurrences,
                1 => &mut l.max_value_roots,
                2 => &mut l.max_field_roots,
                3 => &mut l.max_writer_recipes,
                4 => &mut l.max_allocation_requests,
                5 => &mut l.max_allocation_request_bytes,
                6 => &mut l.max_coexisting_source_and_request_bytes,
                _ => &mut l.max_work,
            };
            if *cap == 0 {
                continue;
            }
            *cap -= 1;
            assert!(
                matches!(
                    project(&package, &Control::default(), l),
                    Err(Failure::Control(CompileControlError::ResourceExhausted))
                ),
                "axis {axis}"
            );
        }
    }
}

fn reject_source_loan(
    package: &p::FragmentPackage,
    equal_foreign: &p::FragmentPackage,
    mode: usize,
    control: &Control,
) -> Result<(), Failure> {
    let original: &dyn PureCompileControl = control;
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Encode)?;
    let result = (|| {
        let mut parent = |f: &TypeViewFacts| {
            resource_axes(
                f.allocation_requests_upper_bound,
                f.allocation_request_bytes_upper_bound,
                f.coexisting_source_and_request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            )
        };
        let mut budget = TypeViewBudget::new_in(package, SOURCE, limits(), &mut parent, &work)?;
        let views = collect_package_type_views_in(&mut budget, &mut work)?;
        let (node, read) = package.scans().iter().next().unwrap();
        let field = &read.public_facts().schema().fields()[0];
        let field_occurrence = PackageTypeOccurrence {
            fragment: package.fragment().id(),
            owner: PackageTypeOwner::Scan(*node),
            channel: Channel::Field(0),
        };
        let real_id = views.field_root_for_in(field_occurrence, field, &mut budget, &mut work)?;
        assert!(std::ptr::eq(
            views
                .fields()
                .iter()
                .find(|root| root.id == real_id)
                .unwrap()
                .field,
            field
        ));
        match mode {
            0 => {
                let equal = Arc::clone(field);
                assert!(Arc::ptr_eq(&equal, field));
                assert!(!std::ptr::eq(&equal, field));
                views.field_root_for_in(field_occurrence, &equal, &mut budget, &mut work)?;
            }
            1 => {
                let p::NodeKind::Scan { relation, .. } = &package.fragment().nodes()[node].kind
                else {
                    unreachable!()
                };
                let equal = relation.schema()[0].ty.clone();
                assert_eq!(equal, relation.schema()[0].ty);
                assert!(!std::ptr::eq(&equal, &relation.schema()[0].ty));
                views.value_root_for_in(
                    PackageTypeOccurrence {
                        fragment: package.fragment().id(),
                        owner: PackageTypeOwner::Relation(*node),
                        channel: Channel::Field(0),
                    },
                    &equal,
                    &mut budget,
                    &mut work,
                )?;
            }
            2 => {
                collect_provider_sources_in(
                    equal_foreign,
                    &views,
                    &[],
                    source_limits(),
                    &mut budget,
                    &mut work,
                )?;
            }
            3 => {
                let sources = collect_provider_sources_in(
                    package,
                    &views,
                    &[],
                    source_limits(),
                    &mut budget,
                    &mut work,
                )?;
                let mut reset_parent = |f: &TypeViewFacts| {
                    resource_axes(
                        f.allocation_requests_upper_bound,
                        f.allocation_request_bytes_upper_bound,
                        f.coexisting_source_and_request_bytes_upper_bound,
                        f.cumulative_work_upper_bound,
                    )
                };
                let mut reset =
                    TypeViewBudget::new_in(package, SOURCE, limits(), &mut reset_parent, &work)?;
                sources.prepare_inputs_in(&mut reset, &mut work)?;
            }
            _ => unreachable!(),
        }
        panic!("foreign or reset loan must be refused");
    })();
    finish(result, work)
}
#[test]
fn original_field_arc_fvt_package_and_cumulative_budget_loans_reject_foreign_with_ordinary_tail() {
    let package = checked_read(false);
    let equal_foreign = checked_read(false);
    assert!(!std::ptr::eq(&package, &equal_foreign));
    for mode in 0..4 {
        let c = Control::default();
        let Err(Failure::Ordinary(message)) =
            reject_source_loan(&package, &equal_foreign, mode, &c)
        else {
            panic!("original source law must stay ordinary")
        };
        assert!(!message.is_empty());
        let trace = c.trace.into_inner().unwrap();
        assert!(trace.len() > 1);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let c = Control {
                    stop: Mutex::new(Some((at, cause.clone()))),
                    ..Default::default()
                };
                assert!(
                    matches!(reject_source_loan(&package,&equal_foreign,mode,&c),Err(Failure::Control(actual)) if actual==cause)
                );
                assert_eq!(c.trace.into_inner().unwrap(), trace[..=at]);
            }
        }
    }
    // Original controller object identity is checked before any new source
    // operation; equal setup values do not mint a source loan.
    let c = Control::default();
    let foreign = Control::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let mut parent = |f: &TypeViewFacts| {
        resource_axes(
            f.allocation_requests_upper_bound,
            f.allocation_request_bytes_upper_bound,
            f.coexisting_source_and_request_bytes_upper_bound,
            f.cumulative_work_upper_bound,
        )
    };
    let mut budget =
        TypeViewBudget::new_in(&package, SOURCE, limits(), &mut parent, &work).unwrap();
    let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
    let mut foreign_work = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
    let before = foreign.trace.lock().unwrap().clone();
    assert!(matches!(
        collect_provider_sources_in(
            &package,
            &views,
            &[],
            source_limits(),
            &mut budget,
            &mut foreign_work
        ),
        Err(ProviderSourceError::Source(TypeViewError::InvalidSource(_)))
    ));
    assert_eq!(*foreign.trace.lock().unwrap(), before);

    // A real checked Writer's stable type input cannot be supplied to a Read
    // package with no writes. The count law runs before new buffers or parent
    // admission even when an actual copied spelling leaves pending255.
    let writer_package = checked_writer();
    for cause in CAUSES {
        let c = Control::default();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        let parent_calls = std::cell::Cell::new(0usize);
        let mut parent = |f: &TypeViewFacts| {
            parent_calls.set(parent_calls.get() + 1);
            resource_axes(
                f.allocation_requests_upper_bound,
                f.allocation_request_bytes_upper_bound,
                f.coexisting_source_and_request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            )
        };
        let mut writer_parent = |f: &TypeViewFacts| {
            resource_axes(
                f.allocation_requests_upper_bound,
                f.allocation_request_bytes_upper_bound,
                f.coexisting_source_and_request_bytes_upper_bound,
                f.cumulative_work_upper_bound,
            )
        };
        let mut writer_budget =
            TypeViewBudget::new_in(&writer_package, SOURCE, limits(), &mut writer_parent, &work)
                .unwrap();
        let writer_views = collect_package_type_views_in(&mut writer_budget, &mut work).unwrap();
        let writer_inputs = writer_views
            .writer_sources_in(&mut writer_budget, &mut work)
            .unwrap();
        assert_eq!(writer_inputs.len(), 1);
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut parent, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        work.flush().unwrap();
        let spelling = "w".repeat(255);
        let copied =
            owned_resources::copy::copy_string::<CompileControlError>(&spelling, &mut work)
                .unwrap();
        assert_eq!(copied, spelling);
        let before = c.trace.lock().unwrap().clone();
        let calls = parent_calls.get();
        *c.stop.lock().unwrap() = Some((before.len(), cause));
        assert!(matches!(
            collect_provider_sources_in(
                &package,
                &views,
                &writer_inputs,
                source_limits(),
                &mut budget,
                &mut work
            ),
            Err(ProviderSourceError::Source(TypeViewError::InvalidSource(_)))
        ));
        assert_eq!(parent_calls.get(), calls);
        assert_eq!(*c.trace.lock().unwrap(), before);
    }
}

#[test]
fn first_known_relation_five_buffer_layouts_refuse_before_real_pending_copy_callback() {
    use std::{alloc::Layout, cell::Cell};
    let package = checked_read(false);
    for request_axis in [true, false] {
        for cause in CAUSES {
            let c = Control::default();
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            let ceiling = Cell::new(None::<(usize, usize)>);
            let mut parent = |f: &TypeViewFacts| {
                if let Some((requests, bytes)) = ceiling.get()
                    && (f.allocation_requests_upper_bound > requests
                        || f.allocation_request_bytes_upper_bound > bytes)
                {
                    return Err(CompileControlError::ResourceExhausted);
                }
                resource_axes(
                    f.allocation_requests_upper_bound,
                    f.allocation_request_bytes_upper_bound,
                    f.coexisting_source_and_request_bytes_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            };
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, limits(), &mut parent, &work).unwrap();
            let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
            work.flush().unwrap();
            let source = "x".repeat(255);
            let copied =
                owned_resources::copy::copy_string::<CompileControlError>(&source, &mut work)
                    .unwrap();
            assert_eq!(copied, source);
            let before = c.trace.lock().unwrap().clone();
            // The first actual Relation source captures exactly these five
            // positive buffers. No later Scan/schema/expression header is read.
            let bytes = Layout::array::<(u32, ProviderBindingSource<'_>)>(1)
                .unwrap()
                .size()
                + Layout::array::<(u32, &c::ConnectorEncodedPayload)>(4)
                    .unwrap()
                    .size()
                + Layout::array::<(u32, &p::ProviderReadReference)>(1)
                    .unwrap()
                    .size()
                + Layout::array::<RelationRow<'_>>(1).unwrap().size()
                + Layout::array::<u32>(1).unwrap().size();
            let f = budget.facts();
            ceiling.set(Some(if request_axis {
                (f.allocation_requests_upper_bound + 5 - 1, usize::MAX)
            } else {
                (
                    usize::MAX,
                    f.allocation_request_bytes_upper_bound + bytes - 1,
                )
            }));
            *c.stop.lock().unwrap() = Some((before.len(), cause));
            assert!(matches!(
                collect_provider_sources_in(
                    &package,
                    &views,
                    &[],
                    source_limits(),
                    &mut budget,
                    &mut work
                ),
                Err(ProviderSourceError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*c.trace.lock().unwrap(), before);
        }
    }
}

#[test]
fn actual_residual_root_and_call_children_numeric_capture_precede_pending_count_observation() {
    let package = checked_read(false);
    for (cap, characters) in [(0, 253), (1, 252)] {
        for cause in CAUSES {
            let c = Control::default();
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            let mut parent = |f: &TypeViewFacts| {
                resource_axes(
                    f.allocation_requests_upper_bound,
                    f.allocation_request_bytes_upper_bound,
                    f.coexisting_source_and_request_bytes_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            };
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, limits(), &mut parent, &work).unwrap();
            let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
            work.flush().unwrap();
            let source = "r".repeat(characters);
            let copied =
                owned_resources::copy::copy_string::<CompileControlError>(&source, &mut work)
                    .unwrap();
            assert_eq!(copied, source);
            let before = c.trace.lock().unwrap().clone();
            *c.stop.lock().unwrap() = Some((before.len(), cause));
            let mut l = source_limits();
            l.max_connector_expression_occurrences = cap;
            // Two real source Count completions (Relation, Value column) precede
            // the Scan root. A third Scan completion precedes the Call arity.
            // Combined with the actual copied characters these leave pending255.
            // No synthetic work preload or completed opaque-inner-loop claim.
            assert!(matches!(
                collect_provider_sources_in(&package, &views, &[], l, &mut budget, &mut work),
                Err(ProviderSourceError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*c.trace.lock().unwrap(), before);
        }
    }
}
