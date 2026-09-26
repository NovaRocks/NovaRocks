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

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use crate::exec::chunk::{ChunkFieldSchema, ChunkSchemaRef};
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::{
    FragmentNodeId, FragmentProgram, FragmentSinkAssignmentKind, FragmentSinkAssignmentRequirement,
};
use crate::exec::node::LocalRuntimeBindings;
use crate::runtime::fragment::instance::{FragmentInstanceSpec, FragmentSinkAssignment};
use novarocks_local_program::{BindingRequirement, LocalProgram, ProgramNodeKind, StaticLayout};
use novarocks_types::SlotId;

pub struct FragmentSubmission {
    program: Arc<FragmentProgram>,
    runtime_bindings: LocalRuntimeBindings,
    instance: FragmentInstanceSpec,
}

impl std::fmt::Debug for FragmentSubmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FragmentSubmission")
            .field("program", &self.program)
            .field("scan_bindings", &self.runtime_bindings.scan_count())
            .field("writer_bindings", &self.runtime_bindings.writer_count())
            .field("finish_bindings", &self.runtime_bindings.finish_count())
            .field("instance", &self.instance)
            .finish()
    }
}

impl FragmentSubmission {
    pub fn try_new(
        program: Arc<FragmentProgram>,
        runtime_bindings: LocalRuntimeBindings,
        instance: FragmentInstanceSpec,
    ) -> Result<Self, FragmentBindingError> {
        let expected_version = program.program_options().contract_version();
        let actual_version = instance.contract_version();
        if expected_version != actual_version {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::ContractVersionMismatch,
                format!(
                    "expected fragment contract version {}, got {}",
                    expected_version.get(),
                    actual_version.get()
                ),
            ));
        }
        let expected_dop = program.local_program().profile().pipeline_dop();
        if expected_dop != instance.pipeline_dop() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "local program compiled for pipeline DOP {}, got {}",
                    expected_dop.get(),
                    instance.pipeline_dop().get()
                ),
            ));
        }
        let inventory = ProgramInventory::try_collect(program.local_program())?;
        validate_runtime_bindings(program.local_program(), &runtime_bindings)?;
        validate_scan_contracts(&program, &inventory)?;
        validate_exchange_contracts(&program, &inventory)?;
        validate_scan_assignments(&inventory, &instance)?;
        validate_exchange_assignments(&program, &instance)?;
        validate_sink_assignment(&program, &instance)?;
        Ok(Self {
            program,
            runtime_bindings,
            instance,
        })
    }

    pub fn program(&self) -> &Arc<FragmentProgram> {
        &self.program
    }

    pub fn runtime_bindings(&self) -> &LocalRuntimeBindings {
        &self.runtime_bindings
    }

    pub const fn instance(&self) -> &FragmentInstanceSpec {
        &self.instance
    }

    pub fn incremental_scan_contracts(&self) -> HashMap<i32, Option<SlotId>> {
        let mut contracts = HashMap::new();
        for node in self.program.local_program().nodes() {
            if matches!(node.kind(), ProgramNodeKind::Scan { .. }) {
                contracts.insert(node.native_node_id(), None);
            }
        }
        contracts
    }

    pub fn root_plan_node_id(&self) -> i32 {
        self.program.root_plan_node_id().get()
    }

    pub const fn query_id(&self) -> novarocks_types::QueryId {
        self.instance.query_id()
    }

    pub const fn fragment_instance_id(&self) -> novarocks_types::UniqueId {
        self.instance.fragment_instance_id().get()
    }

    pub const fn backend_num(&self) -> i32 {
        self.instance.backend_num().get()
    }

    pub fn query_options(&self) -> &crate::runtime::query_options::QueryOptions {
        self.instance.runtime_options().query_options()
    }

    pub fn uses_split_data_stream_sink(&self) -> bool {
        self.program.sink_kind()
            == crate::exec::fragment::program::FragmentSinkKind::SplitDataStream
    }
}

struct ProgramInventory {
    /// All pure scan nodes in the frozen LocalProgram.
    /// Used to cross-check the static `scan_sources` contracts.
    scan_nodes: BTreeSet<FragmentNodeId>,
    /// Pure scan nodes are materialized by `materialize_scan_bindings` and
    /// therefore require an instance `ScanAssignment`.
    materializable_scan_nodes: BTreeSet<FragmentNodeId>,
    exchange_nodes: BTreeMap<FragmentNodeId, StaticLayout>,
}

impl ProgramInventory {
    fn try_collect(plan: &LocalProgram) -> Result<Self, FragmentBindingError> {
        let mut inventory = Self {
            scan_nodes: BTreeSet::new(),
            materializable_scan_nodes: BTreeSet::new(),
            exchange_nodes: BTreeMap::new(),
        };
        for node in plan.nodes() {
            let id = FragmentNodeId::new(node.native_node_id());
            match node.kind() {
                ProgramNodeKind::ExchangeSource { .. } => {
                    if inventory
                        .exchange_nodes
                        .insert(id, node.output_layout().clone())
                        .is_some()
                    {
                        return Err(FragmentBindingError::new(
                            FragmentBindingTarget::ExchangeNode(id.get()),
                            FragmentBindingErrorKind::InvalidAssignment,
                            format!("duplicate exchange node id {}", id.get()),
                        ));
                    }
                }
                ProgramNodeKind::Scan { .. } => {
                    inventory.insert_scan(id)?;
                    inventory.materializable_scan_nodes.insert(id);
                }
                _ => {}
            }
        }
        Ok(inventory)
    }

    fn insert_scan(&mut self, id: FragmentNodeId) -> Result<(), FragmentBindingError> {
        if !self.scan_nodes.insert(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::InvalidAssignment,
                format!("duplicate scan node id {}", id.get()),
            ));
        }
        Ok(())
    }
}

fn validate_runtime_bindings(
    program: &LocalProgram,
    runtime: &LocalRuntimeBindings,
) -> Result<(), FragmentBindingError> {
    let mut scans = BTreeSet::new();
    let mut writers = BTreeSet::new();
    let mut finishers = BTreeSet::new();
    for requirement in program.requirements().entries() {
        match requirement {
            BindingRequirement::Scan { node, .. } => {
                scans.insert(*node);
            }
            BindingRequirement::TableWriter { node, .. } => {
                writers.insert(*node);
            }
            BindingRequirement::TableFinish { node, .. } => {
                finishers.insert(*node);
            }
            _ => {}
        }
    }
    let actual_scans = runtime.scans.keys().copied().collect::<BTreeSet<_>>();
    let actual_writers = runtime.writers.keys().copied().collect::<BTreeSet<_>>();
    let actual_finishers = runtime.finishers.keys().copied().collect::<BTreeSet<_>>();
    if scans != actual_scans || writers != actual_writers || finishers != actual_finishers {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::Instance,
            FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "local runtime sidecar mismatch: scans={scans:?}/{actual_scans:?}, writers={writers:?}/{actual_writers:?}, finishers={finishers:?}/{actual_finishers:?}"
            ),
        ));
    }
    Ok(())
}

fn validate_scan_contracts(
    program: &FragmentProgram,
    inventory: &ProgramInventory,
) -> Result<(), FragmentBindingError> {
    for id in program.scan_sources().keys() {
        if !inventory.scan_nodes.contains(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "declared scan contract {} does not resolve to a scan-shaped plan node",
                    id.get()
                ),
            ));
        }
    }
    for id in &inventory.scan_nodes {
        if !program.scan_sources().contains_key(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::InvalidAssignment,
                format!("local scan node {} has no static scan contract", id.get()),
            ));
        }
    }
    Ok(())
}

fn schema_summary(schema: &ChunkSchemaRef) -> String {
    let slots = schema
        .slots()
        .iter()
        .map(|slot| {
            let unique_id = slot
                .unique_id()
                .map_or_else(|| "none".to_string(), |id| id.to_string());
            let metadata = metadata_suffix(slot.field().metadata());
            format!(
                "slot={},name={},type={},nullable={},unique_id={},field_schema={}{}",
                slot.slot_id(),
                slot.name(),
                data_type_summary(slot.data_type()),
                slot.nullable(),
                unique_id,
                field_schema_summary(slot.field_schema()),
                metadata,
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    let schema_metadata = schema.arrow_schema_ref();
    let schema_metadata = metadata_suffix(schema_metadata.metadata());
    format!("[{slots}]{schema_metadata}")
}

fn static_schema_summary(layout: &StaticLayout) -> String {
    let slots = layout
        .slots()
        .iter()
        .enumerate()
        .map(|(index, slot)| {
            let field = layout.schema().field(index);
            let (field_schema, unique_id) = match layout.slot_metadata_at(index) {
                Some((schema, id)) => (
                    format!(
                        "{:?}",
                        crate::exec::expr::static_program::thaw_field_schema(schema)
                    ),
                    id.map_or_else(|| "none".to_string(), |id| id.to_string()),
                ),
                None => ("unspecified".to_string(), "unspecified".to_string()),
            };
            format!(
                "slot={},name={},type={},nullable={},unique_id={},field_schema={}{}",
                slot,
                field.name(),
                data_type_summary(field.data_type()),
                field.is_nullable(),
                unique_id,
                field_schema,
                metadata_suffix(field.metadata()),
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!("[{slots}]{}", metadata_suffix(layout.schema().metadata()))
}

fn field_schema_summary(field_schema: &ChunkFieldSchema) -> String {
    format!("{field_schema:?}")
}

fn data_type_summary(data_type: &DataType) -> String {
    match data_type {
        DataType::List(field) => format!("List({})", field_summary(field)),
        DataType::LargeList(field) => format!("LargeList({})", field_summary(field)),
        DataType::ListView(field) => format!("ListView({})", field_summary(field)),
        DataType::LargeListView(field) => format!("LargeListView({})", field_summary(field)),
        DataType::FixedSizeList(field, size) => {
            format!("FixedSizeList(size={size},{})", field_summary(field))
        }
        DataType::Struct(fields) => format!(
            "Struct([{}])",
            fields
                .iter()
                .map(|field| field_summary(field))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::Union(fields, mode) => format!(
            "Union(mode={mode:?},[{}])",
            fields
                .iter()
                .map(|(type_id, field)| format!("type_id={type_id},{}", field_summary(field)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::Dictionary(key, value) => format!(
            "Dictionary(key={},value={})",
            data_type_summary(key),
            data_type_summary(value)
        ),
        DataType::Map(field, sorted) => {
            format!("Map(sorted={sorted},{})", field_summary(field))
        }
        DataType::RunEndEncoded(run_ends, values) => format!(
            "RunEndEncoded(run_ends={},values={})",
            field_summary(run_ends),
            field_summary(values)
        ),
        _ => data_type.to_string(),
    }
}

fn field_summary(field: &Field) -> String {
    format!(
        "field(name={:?},type={},nullable={}{})",
        field.name(),
        data_type_summary(field.data_type()),
        field.is_nullable(),
        metadata_suffix(field.metadata()),
    )
}

fn metadata_suffix(metadata: &std::collections::HashMap<String, String>) -> String {
    if metadata.is_empty() {
        String::new()
    } else {
        format!(",metadata={}", sorted_metadata(metadata))
    }
}

fn sorted_metadata(metadata: &std::collections::HashMap<String, String>) -> String {
    let entries = metadata
        .iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(key, value)| format!("{key:?}:{value:?}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{entries}}}")
}

fn validate_exchange_contracts(
    program: &FragmentProgram,
    inventory: &ProgramInventory,
) -> Result<(), FragmentBindingError> {
    let mut mismatches = BTreeSet::new();
    for id in program.exchange_inputs().keys() {
        if !inventory.exchange_nodes.contains_key(id) {
            mismatches.insert(*id);
        }
    }
    for id in inventory.exchange_nodes.keys() {
        if !program.exchange_inputs().contains_key(id) {
            mismatches.insert(*id);
        }
    }
    if let Some(id) = mismatches.first().copied() {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::ExchangeNode(id.get()),
            FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "static exchange inventory mismatch for node {}: declared={} plan={}",
                id.get(),
                program.exchange_inputs().contains_key(&id),
                inventory.exchange_nodes.contains_key(&id)
            ),
        ));
    }
    for (id, contract) in program.exchange_inputs() {
        let actual = inventory
            .exchange_nodes
            .get(id)
            .expect("exchange key sets were validated");
        if contract.expected_schema().slot_ids() != actual.slots() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::LayoutMismatch,
                format!(
                    "exchange node {} expected slots {:?}, got {:?}",
                    id.get(),
                    contract.expected_schema().slot_ids(),
                    actual.slots()
                ),
            ));
        }
        if contract.expected_schema().arrow_schema_ref().as_ref() != actual.schema().as_ref() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::SchemaMismatch,
                format!(
                    "exchange node {} expected schema {}, got {}",
                    id.get(),
                    schema_summary(contract.expected_schema()),
                    static_schema_summary(actual)
                ),
            ));
        }
        for (index, slot) in contract.expected_schema().slots().iter().enumerate() {
            let expected = (
                crate::exec::expr::static_program::freeze_field_schema(slot.field_schema().clone()),
                slot.unique_id(),
            );
            let observed = actual.slot_metadata_at(index);
            if observed != Some((&expected.0, expected.1)) {
                return Err(FragmentBindingError::new(
                    FragmentBindingTarget::ExchangeNode(id.get()),
                    FragmentBindingErrorKind::SchemaMismatch,
                    format!(
                        "exchange node {} expected schema {}, got {}",
                        id.get(),
                        schema_summary(contract.expected_schema()),
                        static_schema_summary(actual),
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Presence-only cross-check between the LocalProgram scan nodes and the
/// instance's scan assignments (mirrors
/// `validate_exchange_assignments`).
///
/// Every materializable scan must have an instance assignment, and no
/// assignment may lack a scan node. The source checks the range variant at
/// materialization.
fn validate_scan_assignments(
    inventory: &ProgramInventory,
    instance: &FragmentInstanceSpec,
) -> Result<(), FragmentBindingError> {
    for id in &inventory.materializable_scan_nodes {
        if instance.scan_assignments().get(id).is_none() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::MissingAssignment,
                format!("missing scan assignment for node {}", id.get()),
            ));
        }
    }
    for (id, _) in instance.scan_assignments().iter() {
        if !inventory.materializable_scan_nodes.contains(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::ExtraAssignment,
                format!(
                    "scan assignment for node {} has no materializable scan node",
                    id.get()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_exchange_assignments(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> Result<(), FragmentBindingError> {
    for id in program.exchange_inputs().keys() {
        if instance.exchange_inputs().get(id).is_none() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::MissingAssignment,
                format!("missing exchange assignment for node {}", id.get()),
            ));
        }
    }
    for (id, _) in instance.exchange_inputs().iter() {
        if !program.exchange_inputs().contains_key(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::ExtraAssignment,
                format!(
                    "exchange assignment for node {} has no static contract",
                    id.get()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_sink_assignment(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> Result<(), FragmentBindingError> {
    use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
    use FragmentSinkAssignmentRequirement as Requirement;
    let requirement = program.sink_assignment_requirement();
    let assignment = instance.sink_assignment();
    match (requirement, assignment) {
        (Requirement::None, FragmentSinkAssignment::None)
        | (
            Requirement::Required(StreamDestinations),
            FragmentSinkAssignment::StreamDestinations { .. },
        ) => Ok(()),
        (
            Requirement::Required(DestinationGroups(expected)),
            FragmentSinkAssignment::DestinationGroups { groups, .. },
        ) if groups.len() == expected.get() => Ok(()),
        (
            Requirement::Required(DestinationGroups(expected)),
            FragmentSinkAssignment::DestinationGroups { groups, .. },
        ) => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "sink expected {} destination groups, got {}",
                expected.get(),
                groups.len()
            ),
        )),
        (Requirement::Required(_), FragmentSinkAssignment::None) => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
            format!(
                "sink expected {}, got none",
                sink_requirement_summary(requirement)
            ),
        )),
        _ => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
            format!(
                "sink expected {}, got {}",
                sink_requirement_summary(requirement),
                sink_assignment_summary(assignment)
            ),
        )),
    }
}

fn sink_requirement_summary(requirement: FragmentSinkAssignmentRequirement) -> String {
    use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
    use FragmentSinkAssignmentRequirement::{None, Required};
    match requirement {
        None => "none".to_string(),
        Required(StreamDestinations) => "stream_destinations".to_string(),
        Required(DestinationGroups(count)) => format!("destination_groups(count={})", count.get()),
    }
}

fn sink_assignment_summary(assignment: &FragmentSinkAssignment) -> String {
    match assignment {
        FragmentSinkAssignment::None => "none".to_string(),
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations".to_string(),
        FragmentSinkAssignment::DestinationGroups { groups, .. } => {
            format!("destination_groups(count={})", groups.len())
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::exec::chunk::{
        Chunk, ChunkFieldSchema, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema,
    };
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::fragment::error::{
        FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
    };
    use crate::exec::fragment::program::{
        ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
        FragmentProgramOptions, FragmentSinkSpec, RuntimeFilterContract, RuntimeFilterId,
        ScanAssignmentKind, ScanSourceContract,
    };
    use crate::exec::fragment::sink::DataStreamPartitionType;
    use crate::exec::fragment::sink::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
    };
    use crate::exec::node::BoxedExecIter;
    use crate::exec::node::exchange_source::ExchangeSourceNode;
    use crate::exec::node::filter::FilterNode;
    use crate::exec::node::join::{
        JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
    };
    use crate::exec::node::runtime_filter::RuntimeFilterConsumerNode;
    use crate::exec::node::scan::{
        BoundScanRanges, RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanNode, ScanOp,
    };
    use crate::exec::node::set_op::{SetOpKind, SetOpNode};
    use crate::exec::node::union_all::UnionAllNode;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan, ExternalSinkRequirement};
    use novarocks_connector_contract::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
        ConnectorReadRecipeSplitDraft, ConnectorReadRelationKind, ConnectorReadRelationPayload,
        ConnectorReadRelationRecipe, ConnectorReadRelationRecipeCompiler,
        ConnectorReadRelationRecipeDraft, ConnectorReadRelationRecipeError,
        ConnectorReadWorkSource, ConnectorValueType, TupleDomain,
    };
    use novarocks_local_program::{StaticConnectorScan, StaticScanAssignment, StaticSinkProgram};

    use crate::runtime::exchange::ExchangeKey;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
        FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::profile::RuntimeProfile;
    use crate::runtime::query_options::QueryOptions;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use novarocks_types::QueryId;
    use novarocks_types::SlotId;
    use novarocks_types::UniqueId;
    use novarocks_types::logical::{LogicalType, field_with_logical_type};

    use super::*;

    fn uid(hi: i64, lo: i64) -> UniqueId {
        UniqueId::new(hi, lo)
    }

    fn query_id(hi: i64, lo: i64) -> QueryId {
        QueryId::new(hi, lo)
    }

    fn values_plan(node_id: i32) -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id,
                }),
            },
        }
    }

    struct DummyScanOp;

    impl ScanOp for DummyScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<RuntimeProfile>,
            _runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::default())
        }
    }

    fn scan_node(node_id: Option<i32>) -> ExecNode {
        let mut scan = ScanNode::new_for_test(Arc::new(DummyScanOp));
        if let Some(node_id) = node_id {
            scan = scan.with_node_id(node_id);
        }
        scan = scan.with_output_chunk_schema(Arc::new(ChunkSchema::empty()));
        ExecNode {
            kind: ExecNodeKind::Scan(scan),
        }
    }

    fn scan_plan(node_id: Option<i32>) -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: scan_node(node_id),
        }
    }

    fn schema(slot_id: u32, nullable: bool) -> ChunkSchemaRef {
        let arrow_schema = Schema::new(vec![Field::new("v", DataType::Int32, nullable)]);
        ChunkSchema::try_ref_from_schema_and_slot_ids(&arrow_schema, &[SlotId::new(slot_id)])
            .expect("chunk schema")
    }

    fn schema_with_metadata(
        slot_id: u32,
        field: Field,
        schema_metadata: HashMap<String, String>,
    ) -> ChunkSchemaRef {
        let slot = ChunkSlotSchema::from_field(SlotId::new(slot_id), &field, None)
            .expect("chunk slot schema");
        Arc::new(
            ChunkSchema::try_new_with_schema_metadata(vec![slot], schema_metadata)
                .expect("chunk schema with metadata"),
        )
    }

    fn exchange_node(node_id: i32, expected_schema: ChunkSchemaRef, _finst: UniqueId) -> ExecNode {
        ExecNode {
            kind: ExecNodeKind::ExchangeSource(ExchangeSourceNode::new(
                node_id,
                Duration::from_secs(1),
                expected_schema,
            )),
        }
    }

    fn union_plan(inputs: Vec<ExecNode>) -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: 99,
                }),
            },
        }
    }

    fn result_sink() -> FragmentSinkSpec {
        FragmentSinkSpec::try_new(FragmentSinkProgram::Result).expect("result sink")
    }

    fn data_stream_sink() -> FragmentSinkSpec {
        FragmentSinkSpec::try_new(FragmentSinkProgram::DataStream(
            DataStreamSinkProgram::try_new(
                9,
                Vec::new(),
                DataStreamPartitionType::Unpartitioned,
                Vec::new(),
                vec![],
                None,
                ExprArena::default(),
            )
            .expect("data stream program"),
        ))
        .expect("data stream sink")
    }

    fn multicast_sink(branch_count: usize) -> FragmentSinkSpec {
        let branches = (0..branch_count)
            .map(|index| {
                DataStreamSinkBranchProgram::try_new(
                    i32::try_from(index).expect("branch index fits i32"),
                    Vec::new(),
                    DataStreamPartitionType::Unpartitioned,
                    Vec::new(),
                    vec![],
                    None,
                )
                .expect("data stream branch")
            })
            .collect();
        FragmentSinkSpec::try_new(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(branches, ExprArena::default())
                .expect("multicast program"),
        ))
        .expect("multicast sink")
    }

    fn split_sink(branch_count: usize) -> FragmentSinkSpec {
        let mut arena = ExprArena::default();
        let split_exprs = (0..branch_count)
            .map(|_| {
                arena.push_typed(
                    ExprNode::Literal(LiteralValue::Bool(true)),
                    DataType::Boolean,
                )
            })
            .collect();
        let branches = (0..branch_count)
            .map(|index| {
                DataStreamSinkBranchProgram::try_new(
                    i32::try_from(index).expect("branch index fits i32"),
                    Vec::new(),
                    DataStreamPartitionType::Unpartitioned,
                    Vec::new(),
                    vec![],
                    None,
                )
                .expect("split stream branch")
            })
            .collect();
        FragmentSinkSpec::try_new(FragmentSinkProgram::SplitDataStream(
            SplitDataStreamSinkProgram::try_new(branches, split_exprs, arena)
                .expect("split stream program"),
        ))
        .expect("split stream sink")
    }

    struct IdentityRecipeCompiler;

    impl ConnectorReadRelationRecipeCompiler for IdentityRecipeCompiler {
        type Error = ConnectorReadRelationRecipeError;

        fn compile_private(
            &self,
            draft: &ConnectorReadRelationRecipeDraft,
        ) -> Result<ConnectorReadRelationRecipeDraft, Self::Error> {
            Ok(draft.clone())
        }

        fn compile_split_private(
            &self,
            _binding: &ConnectorReadBinding,
            draft: &ConnectorReadRecipeSplitDraft,
        ) -> Result<ConnectorReadRecipeSplitDraft, Self::Error> {
            Ok(draft.clone())
        }
    }

    pub(crate) fn static_scan_for_test() -> StaticConnectorScan {
        let instance = ConnectorInstanceId::try_from_canonical("test_lake").unwrap();
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([1; 32])),
        );
        let payload = |category| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    binding.descriptor().provider_id.clone(),
                    binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(1).unwrap(),
                ),
                bytes::Bytes::from_static(b"test"),
            )
        };
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            binding.clone(),
            ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                payload(ConnectorCodecCategory::ReadTable),
                payload(ConnectorCodecCategory::ReadView),
            ),
            vec![payload(ConnectorCodecCategory::ReadColumn)],
        )
        .unwrap();
        let recipe =
            ConnectorReadRelationRecipe::try_compile_with_provider(&draft, &IdentityRecipeCompiler)
                .unwrap();
        StaticConnectorScan::try_new(
            recipe,
            vec![StaticScanAssignment::new(
                Arc::from("v"),
                ConnectorValueType::BigInt,
            )],
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![],
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            ConnectorReadWorkSource::RuntimeSplits,
        )
        .unwrap()
    }

    fn scan_ids(node: &ExecNode, ids: &mut Vec<i32>) {
        match &node.kind {
            ExecNodeKind::Scan(scan) => {
                if let Some(id) = scan.node_id() {
                    ids.push(id);
                }
            }
            ExecNodeKind::AssertNumRows(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Project(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Unpivot(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Filter(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Repeat(n) => scan_ids(&n.input, ids),
            ExecNodeKind::ChangeEventExpand(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Limit(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Aggregate(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Sort(n) => scan_ids(&n.input, ids),
            ExecNodeKind::TableFunction(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Analytic(n) => scan_ids(&n.input, ids),
            ExecNodeKind::RuntimeFilterConsumer(n) => scan_ids(&n.input, ids),
            ExecNodeKind::TableWriter(n) => scan_ids(&n.input, ids),
            ExecNodeKind::Join(n) => {
                scan_ids(&n.left, ids);
                scan_ids(&n.right, ids);
            }
            ExecNodeKind::NestedLoopJoin(n) => {
                scan_ids(&n.left, ids);
                scan_ids(&n.right, ids);
            }
            ExecNodeKind::UnionAll(n) => n.inputs.iter().for_each(|input| scan_ids(input, ids)),
            ExecNodeKind::SetOp(n) => n.inputs.iter().for_each(|input| scan_ids(input, ids)),
            ExecNodeKind::TableFinish(n) => n.inputs.iter().for_each(|input| scan_ids(input, ids)),
            ExecNodeKind::Values(_) | ExecNodeKind::ExchangeSource(_) => {}
        }
    }

    fn lower_result_for_test(
        plan: ExecPlan,
    ) -> Result<(LocalProgram, LocalRuntimeBindings), crate::exec::node::LocalProgramLoweringError>
    {
        let mut ids = Vec::new();
        scan_ids(&plan.root, &mut ids);
        let static_scans = ids
            .into_iter()
            .map(|id| (id, static_scan_for_test()))
            .collect();
        let profile = plan.local_compile_profile(NonZeroUsize::new(1).unwrap(), None)?;
        plan.into_local_program_and_bindings(
            profile,
            static_scans,
            vec![ExternalSinkRequirement::Result],
            StaticSinkProgram::Result,
        )
    }

    fn program_with(
        plan: ExecPlan,
        sink: FragmentSinkSpec,
        scans: BTreeMap<FragmentNodeId, ScanAssignmentKind>,
        exchanges: BTreeMap<FragmentNodeId, ChunkSchemaRef>,
        build_filters: BTreeSet<RuntimeFilterId>,
    ) -> Arc<FragmentProgram> {
        let mut ids = Vec::new();
        scan_ids(&plan.root, &mut ids);
        let static_scans = ids
            .into_iter()
            .map(|id| (id, static_scan_for_test()))
            .collect();
        let profile = plan
            .local_compile_profile(NonZeroUsize::new(1).unwrap(), None)
            .expect("test compile profile");
        let static_sink = sink.into_program().into_static().expect("static test sink");
        let sink_requirements = match &static_sink {
            StaticSinkProgram::Result => vec![ExternalSinkRequirement::Result],
            StaticSinkProgram::Noop => vec![],
            _ => (0..static_sink.branches().len())
                .map(|branch| ExternalSinkRequirement::ExchangeOutput { branch })
                .collect(),
        };
        let (local_program, _runtime) = plan
            .into_local_program_and_bindings(profile, static_scans, sink_requirements, static_sink)
            .expect("lower test local program");
        let scans = scans
            .into_iter()
            .map(|(id, kind)| (id, ScanSourceContract::new(kind)))
            .collect();
        let exchanges = exchanges
            .into_iter()
            .map(|(id, schema)| (id, ExchangeInputContract::new(schema)))
            .collect();
        Arc::new(
            FragmentProgram::try_new(
                Arc::new(local_program),
                FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
                scans,
                exchanges,
                RuntimeFilterContract::new(build_filters, BTreeSet::new()),
            )
            .expect("test fragment program"),
        )
    }

    fn test_submission(
        program: Arc<FragmentProgram>,
        instance: FragmentInstanceSpec,
    ) -> Result<FragmentSubmission, FragmentBindingError> {
        let mut runtime_bindings = LocalRuntimeBindings {
            scans: BTreeMap::new(),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        };
        for requirement in program.local_program().requirements().entries() {
            if let BindingRequirement::Scan { node, .. } = requirement {
                runtime_bindings.scans.insert(
                    *node,
                    ScanNode::new_for_test(Arc::new(DummyScanOp)).source(),
                );
            }
        }
        FragmentSubmission::try_new(program, runtime_bindings, instance)
    }

    #[allow(clippy::too_many_arguments)]
    fn instance_with(
        version: FragmentContractVersion,
        query: QueryId,
        finst: UniqueId,
        scans: BTreeMap<FragmentNodeId, ScanAssignmentKind>,
        exchanges: BTreeMap<FragmentNodeId, usize>,
        sink: FragmentSinkAssignment,
        prober_params: BTreeMap<i32, Vec<crate::runtime::endpoint::RuntimeFilterProberDestination>>,
        builder_counts: BTreeMap<i32, i32>,
    ) -> FragmentInstanceSpec {
        // Kind is no longer stored on the assignment; presence is what the
        // submission validates. The `BoundScanRanges` variant is irrelevant here.
        let scans = ScanAssignments::try_new(
            scans
                .into_keys()
                .map(|id| (id, BoundScanRanges::None))
                .collect(),
        )
        .expect("scan assignments");
        let exchanges = ExchangeInputAssignments::new(
            exchanges
                .into_iter()
                .map(|(id, count)| {
                    (
                        id,
                        ExchangeInputAssignment::new(
                            NonZeroUsize::new(count).expect("non-zero sender count"),
                        ),
                    )
                })
                .collect(),
        );
        let _ = (prober_params, builder_counts);
        FragmentInstanceSpec::new_native(
            version,
            query,
            FragmentInstanceId::new(finst),
            scans,
            exchanges,
            sink,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).expect("pipeline DOP"),
            BackendNum::try_new(0).expect("backend number"),
        )
    }

    fn empty_instance(finst_lo: i64) -> FragmentInstanceSpec {
        instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, finst_lo),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        )
    }

    fn assert_error(
        result: Result<FragmentSubmission, FragmentBindingError>,
        target: FragmentBindingTarget,
        kind: FragmentBindingErrorKind,
    ) -> FragmentBindingError {
        let error = result.expect_err("fragment binding error");
        assert_eq!(error.target(), target);
        assert_eq!(error.kind(), kind);
        error
    }

    #[test]
    fn rejects_contract_version_mismatch_before_composition() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::new(9),
            query_id(1, 2),
            uid(1, 11),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::Instance,
            FragmentBindingErrorKind::ContractVersionMismatch,
        );
    }

    #[test]
    fn composes_empty_contract_without_runtime_resources() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let submission =
            test_submission(Arc::clone(&program), empty_instance(11)).expect("empty submission");
        assert!(Arc::ptr_eq(submission.program(), &program));
        assert_eq!(submission.instance().query_id(), query_id(1, 2));
        assert_eq!(
            submission.instance().fragment_instance_id().get(),
            uid(1, 11)
        );
    }

    #[test]
    fn rejects_missing_task_scan_capability_even_with_assignment() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            scan_plan(Some(10)),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 13),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let empty_sidecar = LocalRuntimeBindings {
            scans: BTreeMap::new(),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        };
        assert_error(
            FragmentSubmission::try_new(program, empty_sidecar, instance),
            FragmentBindingTarget::Instance,
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_extra_task_scan_capability_for_values_program() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let sidecar = LocalRuntimeBindings {
            scans: BTreeMap::from([(
                novarocks_local_program::ProgramNodeId::new(0),
                ScanNode::new_for_test(Arc::new(DummyScanOp)).source(),
            )]),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        };
        assert_error(
            FragmentSubmission::try_new(program, sidecar, empty_instance(14)),
            FragmentBindingTarget::Instance,
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_instance_dop_different_from_static_compile_profile() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            FragmentInstanceId::new(uid(1, 15)),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(2).unwrap(),
            BackendNum::try_new(0).unwrap(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::Instance,
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn shares_immutable_program_across_independent_instances() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let first =
            test_submission(Arc::clone(&program), empty_instance(11)).expect("first submission");
        let second =
            test_submission(Arc::clone(&program), empty_instance(12)).expect("second submission");
        assert!(Arc::ptr_eq(first.program(), &program));
        assert!(Arc::ptr_eq(second.program(), &program));
        assert_ne!(
            first.instance().fragment_instance_id(),
            second.instance().fragment_instance_id()
        );
        assert_eq!(Arc::strong_count(&program), 3);
    }

    #[test]
    fn rejects_scan_without_node_id_during_local_lowering() {
        let plan = scan_plan(None);
        let profile = plan
            .local_compile_profile(NonZeroUsize::new(1).unwrap(), None)
            .expect("profile");
        assert!(
            plan.into_local_program_and_bindings(
                profile,
                BTreeMap::new(),
                vec![ExternalSinkRequirement::Result],
                StaticSinkProgram::Result,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_declared_scan_bound_to_non_scan_node() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            values_plan(10),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 21),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_duplicate_scan_node_identity() {
        assert!(
            lower_result_for_test(union_plan(vec![scan_node(Some(10)), scan_node(Some(10)),]))
                .is_err()
        );
    }

    #[test]
    fn rejects_missing_scan_assignment() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            scan_plan(Some(10)),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            test_submission(program, empty_instance(23)),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn rejects_extra_scan_assignment() {
        let id = FragmentNodeId::new(12);
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 24),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ScanNode(12),
            FragmentBindingErrorKind::ExtraAssignment,
        );
    }

    // NOTE: the old `rejects_wrong_scan_assignment_kind` test was removed: the
    // instance assignment no longer carries a `ScanAssignmentKind`, and
    // variant-vs-source correctness is now enforced at materialize time by
    // `ScanSource::bind` rather than by a submission-time kind cross-check.

    #[test]
    fn reports_smallest_scan_error_first() {
        let id10 = FragmentNodeId::new(10);
        let id20 = FragmentNodeId::new(20);
        let program = program_with(
            union_plan(vec![scan_node(Some(20)), scan_node(Some(10))]),
            result_sink(),
            BTreeMap::from([
                (id20, ScanAssignmentKind::File),
                (id10, ScanAssignmentKind::File),
            ]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            test_submission(program, empty_instance(26)),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn rejects_plan_exchange_without_static_contract() {
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, schema(1, true), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            test_submission(program, empty_instance(30)),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_static_exchange_without_plan_node() {
        let id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 31),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_duplicate_exchange_node_identity() {
        let expected = schema(1, true);
        assert!(
            lower_result_for_test(union_plan(vec![
                exchange_node(20, Arc::clone(&expected), uid(5, 8)),
                exchange_node(20, Arc::clone(&expected), uid(5, 9)),
            ]))
            .is_err()
        );
    }

    #[test]
    fn rejects_exchange_layout_mismatch_before_schema() {
        let id = FragmentNodeId::new(20);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, schema(2, true), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, schema(1, false))]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 33),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::LayoutMismatch,
        );
    }

    #[test]
    fn rejects_exchange_schema_mismatch_after_layout_match() {
        let id = FragmentNodeId::new(20);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, schema(1, true), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, schema(1, false))]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 34),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let error = assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }], got [slot=1,name=v,type=Int32,nullable=true,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }]"
        );
    }

    #[test]
    fn reports_field_metadata_only_exchange_schema_mismatch() {
        let id = FragmentNodeId::new(20);
        let expected = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false).with_metadata(HashMap::from([(
                "contract".to_string(),
                "expected".to_string(),
            )])),
            HashMap::new(),
        );
        let actual = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false).with_metadata(HashMap::from([(
                "contract".to_string(),
                "actual".to_string(),
            )])),
            HashMap::new(),
        );
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, actual, uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let error = assert_error(
            test_submission(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 341),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] },metadata={\"contract\":\"expected\"}], got [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] },metadata={\"contract\":\"actual\"}]"
        );
    }

    #[test]
    fn reports_field_schema_only_exchange_schema_mismatch() {
        let id = FragmentNodeId::new(20);
        let field = Field::new("v", DataType::Int32, false);
        let expected_field_schema = ChunkFieldSchema::empty();
        let actual_field_schema = ChunkFieldSchema::from_field(&field_with_logical_type(
            Field::new("logical", DataType::Utf8, true),
            LogicalType::Json,
        ))
        .expect("logical field schema");
        let with_field_schema = |field_schema| {
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    field.clone(),
                    Some(field_schema),
                    None,
                )])
                .expect("chunk schema with explicit field schema"),
            )
        };
        let expected = with_field_schema(expected_field_schema);
        let actual = with_field_schema(actual_field_schema);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, actual, uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let error = assert_error(
            test_submission(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 344),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }], got [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: Some(Json), children: [] }]"
        );
    }

    #[test]
    fn reports_nested_type_metadata_in_sorted_exact_detail() {
        let id = FragmentNodeId::new(20);
        let nested = |last: &str| {
            schema_with_metadata(
                1,
                Field::new(
                    "v",
                    DataType::Struct(Fields::from(vec![Arc::new(
                        Field::new("item", DataType::Int64, true).with_metadata(HashMap::from([
                            ("zeta".to_string(), last.to_string()),
                            ("alpha".to_string(), "first".to_string()),
                        ])),
                    )])),
                    false,
                ),
                HashMap::new(),
            )
        };
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, nested("actual"), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, nested("expected"))]),
            BTreeSet::new(),
        );
        let error = assert_error(
            test_submission(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 342),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Struct([field(name=\"item\",type=Int64,nullable=true,metadata={\"alpha\":\"first\",\"zeta\":\"expected\"})]),nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [ChunkFieldSchema { logical_type: None, children: [] }] }], got [slot=1,name=v,type=Struct([field(name=\"item\",type=Int64,nullable=true,metadata={\"alpha\":\"first\",\"zeta\":\"actual\"})]),nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [ChunkFieldSchema { logical_type: None, children: [] }] }]"
        );
    }

    #[test]
    fn reports_schema_metadata_only_exchange_schema_mismatch() {
        let id = FragmentNodeId::new(20);
        let expected = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false),
            HashMap::from([("owner".to_string(), "expected".to_string())]),
        );
        let actual = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false),
            HashMap::from([("owner".to_string(), "actual".to_string())]),
        );
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, actual, uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let error = assert_error(
            test_submission(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 343),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }],metadata={\"owner\":\"expected\"}, got [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }],metadata={\"owner\":\"actual\"}"
        );
    }

    #[test]
    fn rejects_missing_exchange_assignment() {
        let id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, Arc::clone(&expected), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        assert_error(
            test_submission(program, empty_instance(35)),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn rejects_extra_exchange_assignment() {
        let id = FragmentNodeId::new(21);
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 36),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ExchangeNode(21),
            FragmentBindingErrorKind::ExtraAssignment,
        );
    }

    #[test]
    fn accepts_matching_exchange_contract_and_assignment() {
        let id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, Arc::clone(&expected), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let submission = test_submission(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 37),
                BTreeMap::new(),
                BTreeMap::from([(id, 2)]),
                FragmentSinkAssignment::None,
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        )
        .expect("matching exchange");
        assert_eq!(
            submission
                .instance()
                .exchange_inputs()
                .get(&id)
                .expect("exchange assignment")
                .sender_count()
                .get(),
            2
        );
    }

    #[test]
    fn reports_smallest_static_exchange_inventory_error_first() {
        let plan_id = FragmentNodeId::new(10);
        let contract_id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(10, Arc::clone(&expected), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(contract_id, expected)]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 38),
            BTreeMap::new(),
            BTreeMap::from([(contract_id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert!(!program.exchange_inputs().contains_key(&plan_id));
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ExchangeNode(10),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_sink_assignment_for_result_sink() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 40),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let error = assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
        );
        assert_eq!(
            error.detail(),
            "sink expected none, got stream_destinations"
        );
    }

    #[test]
    fn requires_stream_destinations_for_data_stream_sink() {
        let program = program_with(
            values_plan(7),
            data_stream_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            test_submission(program, empty_instance(41)),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn accepts_stream_destinations_and_preserves_sender_id() {
        let program = program_with(
            values_plan(7),
            data_stream_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let submission = test_submission(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 42),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::StreamDestinations {
                    destinations: Vec::new(),
                    sender_id: Some(7),
                },
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        )
        .expect("stream sink submission");
        assert!(matches!(
            submission.instance().sink_assignment(),
            FragmentSinkAssignment::StreamDestinations {
                sender_id: Some(7),
                ..
            }
        ));
    }

    #[test]
    fn rejects_wrong_destination_group_count() {
        let program = program_with(
            values_plan(7),
            multicast_sink(2),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 43),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::DestinationGroups {
                groups: vec![Vec::new()],
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn accepts_empty_destination_groups_when_group_count_matches() {
        let program = program_with(
            values_plan(7),
            multicast_sink(2),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        test_submission(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 44),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::DestinationGroups {
                    groups: vec![Vec::new(), Vec::new()],
                    sender_id: Some(9),
                },
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        )
        .expect("matching grouped sink");
    }

    #[test]
    fn rejects_stream_assignment_for_grouped_sink() {
        let program = program_with(
            values_plan(7),
            multicast_sink(2),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 45),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let error = assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
        );
        assert_eq!(
            error.detail(),
            "sink expected destination_groups(count=2), got stream_destinations"
        );
    }

    #[test]
    fn split_sink_validates_destination_groups_only_in_submission() {
        let program = program_with(
            values_plan(7),
            split_sink(2),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );

        assert_error(
            test_submission(Arc::clone(&program), empty_instance(46)),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
        );
        assert_error(
            test_submission(
                Arc::clone(&program),
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 47),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    FragmentSinkAssignment::DestinationGroups {
                        groups: vec![Vec::new()],
                        sender_id: None,
                    },
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::InvalidAssignment,
        );
        assert_error(
            test_submission(
                Arc::clone(&program),
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 48),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    FragmentSinkAssignment::StreamDestinations {
                        destinations: Vec::new(),
                        sender_id: None,
                    },
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
        );
        test_submission(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 49),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::DestinationGroups {
                    groups: vec![Vec::new(), Vec::new()],
                    sender_id: Some(9),
                },
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        )
        .expect("matching split destination groups");
    }

    #[test]
    fn static_exchange_validation_precedes_dynamic_scan_validation() {
        let scan_id = FragmentNodeId::new(10);
        let exchange_id = FragmentNodeId::new(20);
        let program = program_with(
            union_plan(vec![
                scan_node(Some(10)),
                exchange_node(20, schema(2, true), uid(5, 8)),
            ]),
            result_sink(),
            BTreeMap::from([(scan_id, ScanAssignmentKind::File)]),
            BTreeMap::from([(exchange_id, schema(1, true))]),
            BTreeSet::new(),
        );
        assert_error(
            test_submission(program, empty_instance(60)),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::LayoutMismatch,
        );
    }

    #[test]
    fn dynamic_scan_validation_precedes_dynamic_exchange_sink_and_rf() {
        let scan_id = FragmentNodeId::new(10);
        let exchange_id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            union_plan(vec![
                scan_node(Some(10)),
                exchange_node(20, Arc::clone(&expected), uid(5, 8)),
            ]),
            result_sink(),
            BTreeMap::from([(scan_id, ScanAssignmentKind::File)]),
            BTreeMap::from([(exchange_id, expected)]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 61),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::from([(11, 0)]),
        );
        assert_error(
            test_submission(program, instance),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn regular_unary_wrapper_is_traversed() {
        let id = FragmentNodeId::new(10);
        let mut arena = ExprArena::default();
        arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        let program = program_with(
            ExecPlan {
                arena,
                root: ExecNode {
                    kind: ExecNodeKind::Filter(FilterNode {
                        input: Box::new(scan_node(Some(10))),
                        node_id: 30,
                        predicate: crate::exec::expr::ExprId(0),
                    }),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 63),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        test_submission(program, instance).expect("filter child scan");
    }

    #[test]
    fn native_runtime_filter_consumer_wrapper_is_traversed() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::RuntimeFilterConsumer(RuntimeFilterConsumerNode {
                        input: Box::new(scan_node(Some(10))),
                        owner_node_id: 30,
                        bindings: Vec::new(),
                    }),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 67),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );

        test_submission(program, instance).expect("native runtime filter consumer child scan");
    }

    #[test]
    fn binary_right_child_is_traversed() {
        let id = FragmentNodeId::new(10);
        let empty_schema = Arc::new(ChunkSchema::empty());
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Join(JoinNode {
                        left: Box::new(values_plan(7).root),
                        right: Box::new(scan_node(Some(10))),
                        node_id: 30,
                        join_type: JoinType::Inner,
                        distribution_mode: JoinDistributionMode::Partitioned,
                        left_chunk_schema: Arc::clone(&empty_schema),
                        right_chunk_schema: Arc::clone(&empty_schema),
                        join_scope_chunk_schema: empty_schema,
                        probe_keys: Vec::new(),
                        build_keys: Vec::new(),
                        eq_null_safe: Vec::new(),
                        residual_predicate: None,
                        runtime_filter_execution: JoinRuntimeFilterExecution {
                            producers: Vec::new(),
                        },
                    }),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 64),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        test_submission(program, instance).expect("join right child scan");
    }

    #[test]
    fn set_op_children_are_traversed() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::SetOp(SetOpNode {
                        kind: SetOpKind::Intersect,
                        inputs: vec![values_plan(7).root, scan_node(Some(10))],
                        node_id: 30,
                        output_chunk_schema: Arc::new(ChunkSchema::empty()),
                    }),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 65),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        test_submission(program, instance).expect("set op child scan");
    }

    use std::sync::atomic::{AtomicI64, Ordering};

    static NEXT_TEST_ID: AtomicI64 = AtomicI64::new(8_500_000_000_000_000_000);

    fn assert_runtime_state_absent(
        _query: QueryId,
        _finst: UniqueId,
        _exchange_key: ExchangeKey,
        _rf_key: impl Sized,
    ) {
    }

    #[test]
    fn malformed_submission_does_not_touch_runtime_state_or_retain_program_arc() {
        let unique = NEXT_TEST_ID.fetch_add(10, Ordering::Relaxed);
        let query = query_id(unique, unique + 1);
        let finst = uid(unique, unique + 2);
        let exchange_id = FragmentNodeId::new(41);
        let exchange_key = ExchangeKey {
            finst_id_hi: finst.high(),
            finst_id_lo: finst.low(),
            node_id: 41,
        };
        let rf_key = ();
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(41, Arc::clone(&expected), finst),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(exchange_id, expected)]),
            BTreeSet::from([RuntimeFilterId::new(11)]),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query,
            finst,
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );

        assert_runtime_state_absent(query, finst, exchange_key, rf_key);
        let before = Arc::strong_count(&program);
        assert_error(
            test_submission(Arc::clone(&program), instance),
            FragmentBindingTarget::ExchangeNode(exchange_id.get()),
            FragmentBindingErrorKind::MissingAssignment,
        );
        assert_eq!(Arc::strong_count(&program), before);
        assert_runtime_state_absent(query, finst, exchange_key, rf_key);
    }
}
