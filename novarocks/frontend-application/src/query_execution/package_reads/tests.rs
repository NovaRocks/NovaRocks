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

//! Provider-neutral contract tests for the join between physical scans and
//! their frozen encodings. The fixture authors a complete read through existing
//! public constructors; each refusal changes only the fact under examination.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadArtifactCoverage, ConnectorReadBinding,
    ConnectorReadDistribution, ConnectorReadExecutionSchema, ConnectorReadInputVersion,
    ConnectorReadProperties, ConnectorReadRelationKind, ConnectorReadRelationPayload,
    ConnectorReadStaticFacts, ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape, TupleDomain,
};
use novarocks_physical_plan::{
    DataRelation, Distribution, FragmentBuilder, FragmentId, FragmentSink, NodeKind, PhysicalPlan,
    PhysicalProperties, PipelineDopDomain, PlanBuilder, PlanVersionId, ProviderColumnReference,
    ProviderReadOccurrenceId, ProviderReadReference, Relation, RelationField, RowMultiplicity,
    ScanReadBudget, StagePlacementPolicy, ValueOrigin, ValueType,
};
use novarocks_query_application::api::RuntimeSplitPlacement;
use novarocks_spi::connector::read_stack::adapter::{ProviderReadRuntime, ReadRuntimeAdapter};
use novarocks_spi::connector::read_stack::{
    Assignment, ColumnHandle, ConnectorReadConstraint, ConnectorReadPublicSchema, ConnectorSplit,
    ConnectorValueType,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_sql::compiler::SqlCompileControl;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};

use super::author_frozen_reads;
use crate::query_execution::package_freeze::PackageFreezeError;
use crate::query_execution::provider_read_facts::FrozenReadEncoding;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FixtureColumn;
impl ColumnHandle for FixtureColumn {}

#[derive(Debug)]
struct FixtureSplit;
impl ConnectorSplit for FixtureSplit {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

struct FixtureRuntime {
    descriptor: ConnectorInstanceDescriptor,
    catalog: CatalogHandle,
}

impl ProviderReadRuntime for FixtureRuntime {
    type Table = ();
    type Column = FixtureColumn;
    type Transaction = ();
    type Split = FixtureSplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog
    }

    fn transaction(&self) -> Self::Transaction {}
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

fn fixture() -> (
    PhysicalPlan,
    BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
) {
    let instance = ConnectorInstanceId::parse("fixture_catalog").unwrap();
    let adapter = ReadRuntimeAdapter::new(Arc::new(FixtureRuntime {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("fixture_provider").unwrap(),
            instance_id: instance.clone(),
        },
        catalog: CatalogHandle::new(instance, CatalogVersion::from_bytes([5; 32])),
    }));
    let binding = adapter.binding();
    let column = ProviderColumnReference {
        column_payload: encoded(binding, ConnectorCodecCategory::ReadColumn, 3),
    };
    let input_version = ConnectorReadInputVersion::try_new(vec![9]).unwrap();
    let relation = Relation::Data(DataRelation {
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: input_version.clone(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                encoded(binding, ConnectorCodecCategory::ReadTable, 1),
                encoded(binding, ConnectorCodecCategory::ReadView, 2),
            ),
        },
        selection_digest: [8; 32],
        schema: Box::from([RelationField {
            column: column.clone(),
            ty: ValueType::new(DataType::Int32, true),
        }]),
        predicate_guarantees: Box::default(),
        provided_properties: PhysicalProperties {
            distribution: Distribution::Unconstrained,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        },
    });
    let occurrence = ProviderReadOccurrenceId::new(1);
    let mut fragment = FragmentBuilder::new(FragmentId::new(1));
    let scan = fragment.reserve_node_id().unwrap();
    let value = fragment
        .add_value(
            ValueType::new(DataType::Int32, true),
            ValueOrigin::ProviderField {
                scan_node: scan,
                field: column.clone(),
            },
        )
        .unwrap();
    fragment
        .add_scan(
            scan,
            NodeKind::Scan {
                occurrence,
                relation: Box::new(relation),
                read_budget: ScanReadBudget {
                    max_batch_rows: 4096,
                    max_batch_bytes: 8 << 20,
                },
                provider_outputs: Box::from([(column.clone(), value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
            Box::from([value]),
        )
        .unwrap();
    let fragment = fragment
        .finish_definition(
            scan,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([9; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    let plan = plan.finish().unwrap();

    // These declarations come from the fixture provider, independently of the
    // plan's relation fields. No SDK or production I/O is needed for this join.
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, true)]));
    let storage = ValueTypeStorageLifetime::pure();
    let execution = ConnectorReadExecutionSchema::try_new(
        schema.clone(),
        Box::from([FunctionValueType::new(DataType::Int32, true)]),
        storage.clone(),
    )
    .unwrap();
    let shape = ConnectorSourceSchemaShape::try_new(schema, storage.clone()).unwrap();
    let source =
        ConnectorSourceSchemaFacts::try_new(shape, HashMap::new(), Box::default(), storage.clone())
            .unwrap();
    let encoding = FrozenReadEncoding {
        columns: Box::from([(
            column,
            novarocks_sql::compiler::fixtures::provider_read_column_need(
                0,
                "c",
                ValueType::new(DataType::Int32, true),
                ConnectorValueType::Integer,
            )
            .unwrap(),
        )]),
        assignments: vec![
            Assignment::try_new(
                "c",
                adapter.wrap_column(FixtureColumn),
                ConnectorValueType::Integer,
            )
            .unwrap(),
        ],
        offered_constraint: ConnectorReadConstraint::of_summary(TupleDomain::all()),
        enforced_predicate: TupleDomain::all(),
        unenforced_predicate: TupleDomain::all(),
        remaining_expression: None,
        placement: RuntimeSplitPlacement::SingleUnit,
        static_facts: ConnectorReadStaticFacts::try_new(
            input_version,
            [8; 32],
            ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, Vec::new())
                .unwrap(),
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            Arc::<[u8]>::from(&b"fixture coverage"[..]),
        )
        .unwrap(),
        public_schema: Ok(
            ConnectorReadPublicSchema::try_new_typed(execution, source, storage).unwrap(),
        ),
    };
    (plan, BTreeMap::from([(occurrence, encoding)]))
}

#[test]
fn a_complete_frozen_read_is_authored() {
    let (plan, encodings) = fixture();
    let authored = author_frozen_reads(&plan, &encodings, &SqlCompileControl::unbounded())
        .expect("the complete fixture authors");
    assert_eq!(
        authored.keys().collect::<Vec<_>>(),
        encodings.keys().collect::<Vec<_>>()
    );
}

#[test]
fn a_frozen_read_without_public_schema_is_refused() {
    let (plan, mut encodings) = fixture();
    let encoding = encodings.values_mut().next().unwrap();
    encoding.public_schema = Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "fixture provider publishes no schema",
    ));
    assert!(matches!(
        author_frozen_reads(&plan, &encodings, &SqlCompileControl::unbounded()),
        Err(PackageFreezeError::Read(reason))
            if reason.contains("published no public read schema")
                && reason.contains("fixture provider publishes no schema")
    ));
}

#[test]
fn a_frozen_assignment_without_a_relation_column_is_refused() {
    let (plan, mut encodings) = fixture();
    let encoding = encodings.values_mut().next().unwrap();
    encoding.assignments.push(encoding.assignments[0].clone());
    assert!(matches!(
        author_frozen_reads(&plan, &encodings, &SqlCompileControl::unbounded()),
        Err(PackageFreezeError::Read(reason)) if reason.contains("relation has 1 columns")
    ));
}

#[test]
fn a_frozen_read_without_a_scan_is_refused() {
    let (plan, mut encodings) = fixture();
    let spare = encodings.values().next().unwrap().clone();
    encodings.insert(ProviderReadOccurrenceId::new(u32::MAX), spare);
    assert!(matches!(
        author_frozen_reads(&plan, &encodings, &SqlCompileControl::unbounded()),
        Err(PackageFreezeError::Facts(reason)) if reason.contains("scanned by no node")
    ));
}
