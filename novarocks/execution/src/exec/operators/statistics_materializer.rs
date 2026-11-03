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

//! Final StatisticsArtifactV1 source. Only the frozen root purpose selects it.
//! Its immutable plan facts remain with preparation's owner; every new batch
//! backing is preflighted before growth under the original last-edge permit.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::{ExecutionFailure, ExecutionReason, ExecutionResult};

#[cfg(test)]
use crate::runtime::runtime_state::RuntimeState;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::{Buffer, MutableBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_result_contract::RootProfileV1;
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    MAX_CONNECTOR_STATISTICS_COLUMNS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES, MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES,
};
use novarocks_types::SlotId;

use crate::exec::chunk::{
    Chunk, ChunkSchemaRef, RootArrayStorageLimits, borrowed_root_chunk_schema_storage,
};
use crate::exec::node::unpivot::{UnpivotConstant, UnpivotValueMapping};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::{AcceptedInput, InputPreparationFailure, RawInput},
    shape::{
        HydrateInput, InputPreparation, PreparationStep, ReceiverPreparation, ReceiverSlot,
        StepOperator,
    },
    step::{AdmissionNext, InputResult, NextStep, RootPending, RootStep, StepResult},
};
use crate::runtime::task::io::RootInputPermit;
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::ValueKind;

const NAME: &str = "BOUNDED_STATISTICS_MATERIALIZER";
// Finalization validates four offset vectors and fixed canonical carriers.
// Keep even their scalar loads/comparisons within a separate 1024-work turn.
const BATCH_ROWS: usize = 32;
const BYTE_QUANTUM: usize = RootProfileV1::EMIT_BYTES_PER_TURN;
const WORK_QUANTUM: usize = RootProfileV1::CELLS_PER_TURN;

enum StatisticsLiteralSource {
    #[cfg(test)]
    Fixture(Arc<[novarocks_local_program::StaticLiteral]>),
    Compiled(Arc<novarocks_local_program::LocalProgram>),
}
impl StatisticsLiteralSource {
    fn blob(&self, expr: novarocks_local_program::ProgramExprId) -> Option<&str> {
        match self {
            #[cfg(test)]
            Self::Fixture(literals) => match literals.get(expr.index())? {
                novarocks_local_program::StaticLiteral::Utf8(value) => Some(value),
                _ => None,
            },
            Self::Compiled(program) => match program.graph().expressions().node(expr)?.kind() {
                novarocks_local_program::StaticExprKind::Literal(
                    novarocks_local_program::StaticLiteral::Utf8(value),
                ) => Some(value),
                novarocks_local_program::StaticExprKind::Constant(value) => {
                    Self::constant_blob(value)
                }
                _ => None,
            },
        }
    }
    fn constant_blob(value: &novarocks_functions::ConstantValue) -> Option<&str> {
        // The frozen constant retains its complete declaration. Read its
        // actual ordinal through the one constant reader.
        if value.pool().array().data_type() != &DataType::Utf8
            || value.value_type().nullable()
            || !matches!(value.value_type().value_type().kind(), ValueKind::String)
        {
            return None;
        }
        value.try_utf8().ok().flatten()
    }
}
/// Compiled execution retains its original immutable declaration until the
/// actual BE preparation owner creates a runtime schema. Unit fixtures retain
/// their independently authored schema without an expression evaluator.
enum StatisticsSchemaPreparation {
    #[cfg(test)]
    Fixture {
        schema: ChunkSchemaRef,
        fixed_capacity: usize,
    },
    Compiled(StaticLayout),
}
impl StatisticsSchemaPreparation {
    fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
        match self {
            #[cfg(test)]
            Self::Fixture { schema, .. } => schema.arrow_schema_ref(),
            Self::Compiled(layout) => layout.schema().clone(),
        }
    }
    fn slots(&self) -> &[SlotId] {
        match self {
            #[cfg(test)]
            Self::Fixture { schema, .. } => schema.slot_ids(),
            Self::Compiled(layout) => layout.slots(),
        }
    }
    fn fixture_schema(&self) -> Option<&ChunkSchemaRef> {
        match self {
            #[cfg(test)]
            Self::Fixture { schema, .. } => Some(schema),
            Self::Compiled(_) => None,
        }
    }
}
struct Definition {
    source: StatisticsLiteralSource,
    mappings: Vec<UnpivotValueMapping>,
    input_slots: Vec<SlotId>,
    input_ordinals: Vec<usize>,
    schema: StatisticsSchemaPreparation,
    batch_rows: usize,
}

pub(crate) struct StatisticsMaterializerFactory(Arc<Definition>);
impl StatisticsMaterializerFactory {
    #[cfg(test)]
    fn from_test_facts(
        literals: Arc<[novarocks_local_program::StaticLiteral]>,
        value_slot: SlotId,
        literal_slots: Vec<SlotId>,
        mappings: Vec<UnpivotValueMapping>,
        input_slots: &[SlotId],
        schema: ChunkSchemaRef,
        passthrough_count: usize,
        max_rows: usize,
        max_bytes: usize,
    ) -> ExecutionResult<Self> {
        Self::try_new_from_source(
            StatisticsLiteralSource::Fixture(literals),
            value_slot,
            literal_slots,
            mappings,
            input_slots,
            StatisticsSchemaPreparation::Fixture {
                fixed_capacity: 0,
                schema,
            },
            passthrough_count,
            max_rows,
            max_bytes,
        )
    }
    pub(crate) fn try_new_compiled(
        program: Arc<novarocks_local_program::LocalProgram>,
        value_slot: SlotId,
        literal_slots: Vec<SlotId>,
        mappings: Vec<UnpivotValueMapping>,
        input_slots: &[SlotId],
        output_layout: StaticLayout,
        passthrough_count: usize,
        max_rows: usize,
        max_bytes: usize,
    ) -> ExecutionResult<Self> {
        Self::try_new_from_source(
            StatisticsLiteralSource::Compiled(program),
            value_slot,
            literal_slots,
            mappings,
            input_slots,
            StatisticsSchemaPreparation::Compiled(output_layout),
            passthrough_count,
            max_rows,
            max_bytes,
        )
    }
    fn try_new_from_source(
        source: StatisticsLiteralSource,
        value_slot: SlotId,
        literal_slots: Vec<SlotId>,
        mappings: Vec<UnpivotValueMapping>,
        input_slots: &[SlotId],
        schema: StatisticsSchemaPreparation,
        passthrough_count: usize,
        max_rows: usize,
        max_bytes: usize,
    ) -> ExecutionResult<Self> {
        let arrow_schema = schema.arrow_schema();
        let fields = arrow_schema.fields();
        let slots = schema.slots();
        if fields.len() != 4
            || slots.len() != 4
            || passthrough_count != 0
            || value_slot != slots[2]
            || literal_slots.as_slice() != [slots[0], slots[1], slots[3]]
            || max_rows == 0
            || max_rows > MAX_CONNECTOR_STATISTICS_ARTIFACTS
            || max_bytes != MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
            || mappings.is_empty()
            || mappings.len() > MAX_CONNECTOR_STATISTICS_ARTIFACTS
        {
            return Err(statistics_contract(
                "statistics materializer roles or bounds differ from its frozen domain",
            ));
        }
        if !schema.arrow_schema().metadata().is_empty() {
            return Err(statistics_contract(
                "statistics schema metadata must be empty",
            ));
        }
        let names = ["input_fields", "blob_type", "body", "properties"];
        if fields
            .iter()
            .zip(names)
            .any(|(field, name)| field.name() != name || !field.metadata().is_empty())
            || [0, 1, 3].iter().any(|&i| fields[i].is_nullable())
            || !matches!(fields[1].data_type(), DataType::Utf8)
            || !matches!(fields[2].data_type(), DataType::Binary)
        {
            return Err(statistics_contract(
                "statistics materializer output schema differs from its frozen domain",
            ));
        }
        let DataType::List(item) = fields[0].data_type() else {
            return Err(statistics_contract(
                "statistics input_fields must be List<Int32>",
            ));
        };
        if item.name() != "item"
            || item.is_nullable()
            || !item.metadata().is_empty()
            || !matches!(item.data_type(), DataType::Int32)
        {
            return Err(statistics_contract(
                "statistics field IDs have an invalid item schema",
            ));
        }
        let DataType::Map(entries, false) = fields[3].data_type() else {
            return Err(statistics_contract(
                "statistics properties must be an unordered Map",
            ));
        };
        let DataType::Struct(children) = entries.data_type() else {
            return Err(statistics_contract("statistics map entries must be Struct"));
        };
        if entries.name() != "entries"
            || entries.is_nullable()
            || !entries.metadata().is_empty()
            || children.len() != 2
            || children.iter().zip(["key", "value"]).any(|(f, name)| {
                f.name() != name
                    || f.is_nullable()
                    || !f.metadata().is_empty()
                    || !matches!(f.data_type(), DataType::Utf8)
            })
        {
            return Err(statistics_contract(
                "statistics properties have an invalid entries schema",
            ));
        }
        let layout = match &schema {
            StatisticsSchemaPreparation::Compiled(layout) => Some(layout),
            #[cfg(test)]
            StatisticsSchemaPreparation::Fixture { .. } => None,
        };
        if let Some(layout) = layout {
            if !layout.is_typed() || layout.slots().len() != 4 {
                return Err(statistics_contract(
                    "compiled statistics require their complete frozen layout",
                ));
            }
            let binding = |ordinal: usize| {
                layout
                    .value_binding(layout.slots()[ordinal])
                    .ok_or_else(|| {
                        statistics_contract("compiled statistics value binding is absent")
                    })
            };
            if !matches!(binding(0)?.value_type().kind(), ValueKind::List(item) if matches!(item.kind(), ValueKind::Scalar(DataType::Int32)))
                || !matches!(binding(1)?.value_type().kind(), ValueKind::String)
                || !matches!(binding(2)?.value_type().kind(), ValueKind::Binary)
                || !matches!(binding(3)?.value_type().kind(), ValueKind::Map { key, value } if matches!(key.kind(), ValueKind::String) && matches!(value.kind(), ValueKind::String))
            {
                return Err(statistics_contract(
                    "compiled statistics declarations differ from the fixed artifact protocol",
                ));
            }
        }
        for mapping in &mappings {
            let [
                UnpivotConstant::Int32List(ids),
                UnpivotConstant::Scalar {
                    expr_id,
                    nullable: false,
                },
                UnpivotConstant::Utf8Map(properties),
            ] = mapping.constants.as_slice()
            else {
                return Err(statistics_contract(
                    "statistics constants differ from the frozen roles",
                ));
            };
            let Some(blob) = source.blob(*expr_id) else {
                return Err(statistics_contract(
                    "statistics blob_type must be an exact UTF8 literal",
                ));
            };
            if ids.is_empty()
                || ids.len() > MAX_CONNECTOR_STATISTICS_COLUMNS
                || blob.is_empty()
                || blob.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                || !properties.is_empty()
            {
                return Err(statistics_contract(
                    "statistics literal exceeds its domain limits",
                ));
            }
            let mut sorted = [0i32; MAX_CONNECTOR_STATISTICS_COLUMNS];
            sorted[..ids.len()].copy_from_slice(ids);
            sorted[..ids.len()].sort_unstable();
            if sorted[0] <= 0
                || sorted[..ids.len()]
                    .windows(2)
                    .any(|pair| pair[0] == pair[1])
            {
                return Err(statistics_contract(
                    "statistics field IDs must be positive and unique",
                ));
            }
        }
        // Resolve exact frozen input positions once under preparation's owner.
        // CPU continuations never perform data-dependent hash-table probes.
        let input_ordinals = mappings
            .iter()
            .map(|mapping| {
                input_slots
                    .iter()
                    .position(|slot| *slot == mapping.input_value_slot_id)
                    .ok_or_else(|| {
                        statistics_contract("statistics body is absent from the frozen input port")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        #[cfg(test)]
        let mut schema = schema;
        #[cfg(test)]
        if let StatisticsSchemaPreparation::Fixture {
            schema,
            fixed_capacity,
        } = &mut schema
        {
            *fixed_capacity = statistics_fixed_capacity(schema)?;
        }
        Ok(Self(Arc::new(Definition {
            source,
            mappings,
            input_slots: input_slots.to_vec(),
            input_ordinals,
            schema,
            batch_rows: max_rows.min(BATCH_ROWS),
        })))
    }
}
/// The original root grant's fixed-backing calculation. Compiled execution
/// calls it after actual schema preparation; numeric limits and coefficients
/// are the same as the explicit legacy source adapter.
fn statistics_fixed_capacity(schema: &ChunkSchemaRef) -> ExecutionResult<usize> {
    let metadata = borrowed_root_chunk_schema_storage(
        schema,
        RootArrayStorageLimits {
            bytes: MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
            nodes: 2 * RootProfileV1::SCHEMA_TYPE_NODES,
            depth: RootProfileV1::MAX_DEPTH,
        },
    )
    .map_err(ExecutionFailure::from_domain)?;
    // One extra full metadata copy conservatively covers each actual
    // List/Map/inline Struct DataType alias inspected by the input oracle.
    // Object allowance mirrors that oracle, including inline Map entries.
    let objects = size_of::<ListArray>()
        + size_of::<Int32Array>()
        + 3 * size_of::<StringArray>()
        + size_of::<BinaryArray>()
        + size_of::<MapArray>()
        + size_of::<StructArray>()
        + 8 * 4 * size_of::<usize>();
    let buffers = crate::exec::chunk::ARROW_BUFFER_OWNER_METADATA_BOUND;
    let fixed_capacity = metadata
        .checked_mul(2)
        .and_then(|n| n.checked_add(objects))
        .and_then(|n| n.checked_add(11 * buffers))
        .and_then(|n| n.checked_add(6 * size_of::<ArrayRef>()))
        .and_then(|n| {
            n.checked_add(size_of::<Chunk>() + size_of::<RecordBatch>() + size_of::<Workspace>())
        })
        .filter(|&n| n < MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES)
        .ok_or_else(|| statistics_contract("statistics fixed backing exceeds its batch profile"))?;
    Ok(fixed_capacity)
}
impl OperatorFactory for StatisticsMaterializerFactory {
    fn name(&self) -> &str {
        NAME
    }
    fn create(&self, _: i32, _: i32) -> Box<dyn Operator> {
        Box::new(StatisticsMaterializer {
            receiver: ReceiverSlot::new(StatisticsPreparation::default()),
            next: NextStep::NeedInput,
            failure: None,
            definition: self.0.clone(),
            output: self.0.schema.fixture_schema().cloned(),
            fixed_capacity: match &self.0.schema {
                #[cfg(test)]
                StatisticsSchemaPreparation::Fixture { fixed_capacity, .. } => *fixed_capacity,
                StatisticsSchemaPreparation::Compiled(_) => 0,
            },
            construction: None,
            input: None,
            workspace: None,
            cursor: 0,
            rows: 0,
            body_bytes: 0,
            metadata_bytes: 0,
            finishing: false,
        })
    }
}
struct StatisticsMaterializer {
    receiver: ReceiverSlot<StatisticsPreparation>,
    next: NextStep,
    failure: Option<crate::runtime::task::ExecutionFailure>,
    definition: Arc<Definition>,
    output: Option<ChunkSchemaRef>,
    fixed_capacity: usize,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    input: Option<Chunk>,
    workspace: Option<Workspace>,
    cursor: usize,
    rows: usize,
    body_bytes: usize,
    metadata_bytes: usize,
    finishing: bool,
}
impl OperatorLifecycle for StatisticsMaterializer {
    fn creates_typed_storage(&self) -> bool {
        matches!(
            &self.definition.schema,
            StatisticsSchemaPreparation::Compiled(_)
        )
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.receiver
            .preparation_mut()
            .hydrate
            .install_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.output.is_some() {
            return Ok(());
        }
        #[cfg(test)]
        let layout = match &self.definition.schema {
            StatisticsSchemaPreparation::Compiled(layout) => layout,
            StatisticsSchemaPreparation::Fixture {
                schema,
                fixed_capacity,
            } => {
                self.output = Some(Arc::clone(schema));
                self.fixed_capacity = *fixed_capacity;
                return Ok(());
            }
        };
        #[cfg(not(test))]
        let StatisticsSchemaPreparation::Compiled(layout) = &self.definition.schema;
        let factory = self
            .construction
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner)?;
        let schema = factory.create_compiled_chunk_schema(layout)?;
        let fixed_capacity = statistics_fixed_capacity(&schema)?;
        self.fixed_capacity = fixed_capacity;
        self.output = Some(schema);
        Ok(())
    }
    fn name(&self) -> &str {
        NAME
    }

    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.receiver
            .preparation_mut()
            .hydrate
            .install_tracker(tracker);
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.cancel();
        Ok(())
    }
    fn cancel(&mut self) {
        self.receiver.discard();
        drop(self.workspace.take());
        drop(self.input.take());
        self.next = NextStep::Finished;
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
}
#[derive(Default)]
struct StatisticsPreparation {
    hydrate: HydrateInput,
}
struct StatisticsInputPolicy;
impl InputEncodingPolicy for StatisticsInputPolicy {}
impl InputPreparation<StatisticsInputPolicy> for StatisticsPreparation {
    fn admission(
        &mut self,
        _: &mut DriverStep<'_>,
        input: &RawInput,
        _: &StatisticsInputPolicy,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        HydrateInput::validate_uncovered(input)?;
        Ok(None)
    }
    fn accept(&mut self, raw: RawInput) -> AcceptedInput {
        self.hydrate.accept_raw(raw)
    }
    fn prepare(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: AcceptedInput,
        policy: &StatisticsInputPolicy,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        self.hydrate.prepare_accepted(ctx, input, policy)
    }
}
#[derive(Debug)]
enum StatisticsStepFailure {
    InputAfterFinish,
    MissingRootGrant,
}
impl std::fmt::Display for StatisticsStepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InputAfterFinish => "statistics input reached a finished receiver",
            Self::MissingRootGrant => {
                "statistics materialization requires the original root input grant"
            }
        })
    }
}
impl std::error::Error for StatisticsStepFailure {}
impl novarocks_failure_contract::FailureProjection for StatisticsStepFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::InputAfterFinish => ExecutionReason::StatisticsInputAfterFinish,
            Self::MissingRootGrant => ExecutionReason::StatisticsMissingRootGrant,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl StatisticsMaterializer {
    fn data_next(&self) -> NextStep {
        if self.input.is_some() || self.receiver.is_occupied() {
            NextStep::Runnable
        } else if self.finishing {
            NextStep::Finished
        } else {
            NextStep::NeedInput
        }
    }
    fn fail(
        &mut self,
        ctx: &DriverStep<'_>,
        error: crate::runtime::task::ExecutionFailure,
    ) -> crate::runtime::task::ExecutionFailure {
        ctx.report_failure(error.clone());
        self.failure.get_or_insert(error).clone()
    }
    fn validate_input(&self, chunk: &Chunk) -> ExecutionResult<()> {
        if self.output.is_none() {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        (|| -> ExecutionResult<()> {
            if self.input.is_some() {
                return Err(statistics_contract("statistics input position is full"));
            }
            let expanded = chunk
                .len()
                .checked_mul(self.definition.mappings.len())
                .ok_or_else(|| statistics_contract("statistics expanded row count overflow"))?;
            if self
                .rows
                .checked_add(expanded)
                .is_none_or(|n| n > MAX_CONNECTOR_STATISTICS_ARTIFACTS)
            {
                return Err(statistics_limit(
                    "statistics expanded row count",
                    novarocks_failure_contract::ResourceDimension::Items,
                    self.rows as u128 + expanded as u128,
                    MAX_CONNECTOR_STATISTICS_ARTIFACTS,
                ));
            }
            if chunk.chunk_schema_ref().slot_ids() != self.definition.input_slots {
                return Err(statistics_contract(
                    "statistics input order differs from its frozen port",
                ));
            }
            for &ordinal in &self.definition.input_ordinals {
                let array = body_array(chunk, ordinal)?;
                if array.len() != chunk.len() {
                    return Err(statistics_contract(
                        "statistics body row count differs from input",
                    ));
                }
            }
            Ok(())
        })()
    }
    fn materialize_root(&mut self, permit: &RootInputPermit) -> ExecutionResult<RootStep> {
        (|| -> ExecutionResult<RootStep> {
            let Some(input) = self.input.as_ref() else {
                return Ok(RootStep::Complete(StepResult::new(
                    None,
                    self.data_next(),
                    0,
                )));
            };
            if self.workspace.is_none() {
                let total = input.len() * self.definition.mappings.len();
                let mut sizes = Sizes::default();
                let mut rows = 0;
                for flat in self.cursor..total.min(self.cursor + self.definition.batch_rows) {
                    let (ids, blob, body) = record(input, &self.definition, flat)?;
                    if body.len() > MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES {
                        return Err(statistics_extent(
                            "statistics artifact body",
                            body.len() as u128,
                            MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES,
                        ));
                    }
                    let next = sizes.add(ids.len(), blob.len(), body.len())?;
                    if self
                        .body_bytes
                        .checked_add(next.body)
                        .is_none_or(|n| n > MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES)
                    {
                        return Err(statistics_limit(
                            "statistics result body",
                            novarocks_failure_contract::ResourceDimension::Bytes,
                            self.body_bytes as u128 + next.body as u128,
                            MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES,
                        ));
                    }
                    if self
                        .metadata_bytes
                        .checked_add(next.metadata(rows + 1)?)
                        .is_none_or(|n| n > 16 * 1024 * 1024)
                    {
                        return Err(statistics_limit(
                            "statistics result metadata",
                            novarocks_failure_contract::ResourceDimension::Bytes,
                            self.metadata_bytes as u128 + next.metadata(rows + 1)? as u128,
                            16 * 1024 * 1024,
                        ));
                    }
                    let backing = next.capacity(rows + 1, self.fixed_capacity)?;
                    if backing > MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                        || backing > permit.retained_bytes()
                    {
                        if rows == 0 {
                            return Err(statistics_extent(
                                "statistics artifact backing",
                                backing as u128,
                                MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                                    .min(permit.retained_bytes()),
                            ));
                        }
                        break;
                    }
                    sizes = next;
                    rows += 1;
                }
                // Every capacity, fixed owner and simultaneous temporary is known
                // before any MutableBuffer can grow. No candidate/probe batch.
                self.workspace = Some(Workspace::new(
                    self.cursor,
                    rows,
                    sizes,
                    permit.generation(),
                ));
                return Ok(RootStep::Pending(RootPending::Yield));
            }
            let workspace = self.workspace.as_mut().unwrap();
            if workspace.generation != permit.generation() {
                return Err(statistics_contract(
                    "statistics continuation changed its original grant",
                ));
            }
            if workspace.row < workspace.rows {
                let (examined, work) = workspace.advance(input, &self.definition)?;
                debug_assert!(examined <= BYTE_QUANTUM && work <= WORK_QUANTUM);
                // Final offset/array validation gets its own finite turn, even
                // when this copy used the entire byte quantum.
                return Ok(RootStep::Pending(RootPending::Yield));
            }
            let workspace = self.workspace.take().unwrap();
            let rows = workspace.rows;
            let body = workspace.sizes.body;
            let metadata = workspace.sizes.metadata(rows)?;
            let chunk = workspace.finish(self.output.as_ref().ok_or_else(|| {
                statistics_contract("statistics output schema has not been prepared")
            })?)?;
            self.cursor += rows;
            self.rows += rows;
            self.body_bytes += body;
            self.metadata_bytes += metadata;
            if self.cursor == input.len() * self.definition.mappings.len() {
                drop(self.input.take());
                self.cursor = 0;
            }
            Ok(RootStep::Complete(StepResult::new(
                Some(chunk),
                self.data_next(),
                rows as u64,
            )))
        })()
    }
}
impl StepOperator for StatisticsMaterializer {
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        if let Some(failure) = self.failure.clone().or_else(|| ctx.checkpoint().err()) {
            return InputResult::Rejected { input, failure };
        }
        if self.finishing || matches!(self.next, NextStep::Finished) {
            return InputResult::Rejected {
                input,
                failure: crate::runtime::task::ExecutionFailure::from_domain(
                    StatisticsStepFailure::InputAfterFinish,
                ),
            };
        }
        if !matches!(self.next, NextStep::NeedInput) {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        let result = self.receiver.try_accept(ctx, input, &StatisticsInputPolicy);
        if matches!(result, InputResult::Accepted(_)) {
            self.next = NextStep::Runnable;
        }
        result
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        ctx.checkpoint()?;
        if self.input.is_none() && !self.receiver.is_occupied() {
            self.next = self.data_next();
            return Ok(StepResult::new(None, self.next.clone(), 0));
        }
        Err(self.fail(
            ctx,
            crate::runtime::task::ExecutionFailure::from_domain(
                StatisticsStepFailure::MissingRootGrant,
            ),
        ))
    }
    fn drive_with_root_input(
        &mut self,
        ctx: &mut DriverStep<'_>,
        permit: &RootInputPermit,
    ) -> ExecutionResult<RootStep> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        ctx.checkpoint()?;
        if !ctx.work().try_consume(1) {
            self.next = NextStep::Yield;
            return Ok(RootStep::Pending(RootPending::Yield));
        }
        match self.receiver.prepare(ctx, &StatisticsInputPolicy) {
            Ok(ReceiverPreparation::Ready(input)) => {
                // Validation borrows the prepared payload; a returned failure is
                // reported while this owner still retains that payload.
                if let Err(error) = ctx
                    .perform(crate::runtime::task::PipelineOperation::Push, |_| {
                        self.validate_input(input.chunk())
                    })
                {
                    return Err(self.fail(ctx, error));
                }
                let chunk = input.into_uncovered_chunk()?;
                self.cursor = 0;
                if !chunk.is_empty() {
                    self.input = Some(chunk);
                }
            }
            Ok(ReceiverPreparation::Pending(next)) => {
                self.next = next.clone();
                return Ok(RootStep::Pending(match next {
                    NextStep::Wait(wait) => RootPending::Wait(wait),
                    NextStep::Yield => RootPending::Yield,
                    _ => RootPending::Runnable,
                }));
            }
            Err(error) => return Err(self.fail(ctx, error)),
            Ok(ReceiverPreparation::Empty) => {}
        }
        let result = ctx.perform(crate::runtime::task::PipelineOperation::Pull, |_| {
            self.materialize_root(permit)
        });
        match result {
            Ok(step) => {
                self.next = match &step {
                    RootStep::Complete(step) => step.next.clone(),
                    RootStep::Pending(pending) => pending.next(),
                };
                Ok(step)
            }
            Err(error) => Err(self.fail(ctx, error)),
        }
    }
    fn release_root_workspace(&mut self) {
        drop(self.workspace.take());
    }
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        ctx.checkpoint()?;
        self.finishing = true;
        if matches!(self.next, NextStep::NeedInput) {
            self.next = NextStep::Runnable;
        }
        Ok(())
    }
}

fn body_array(input: &Chunk, index: usize) -> ExecutionResult<&BinaryArray> {
    input
        .record_batch()
        .columns()
        .get(index)
        .ok_or_else(|| statistics_contract("statistics body ordinal is absent"))?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            statistics_contract("statistics aggregate must produce an exact BinaryArray")
        })
}
fn record<'a>(
    input: &'a Chunk,
    definition: &'a Definition,
    flat: usize,
) -> ExecutionResult<(&'a [i32], &'a str, &'a [u8])> {
    let mapping = &definition.mappings[flat / input.len()];
    let row = flat % input.len();
    let array = body_array(input, definition.input_ordinals[flat / input.len()])?;
    if array.is_null(row) {
        return Err(statistics_contract(
            "statistics aggregate produced a null body",
        ));
    }
    let UnpivotConstant::Int32List(ids) = &mapping.constants[0] else {
        unreachable!()
    };
    let UnpivotConstant::Scalar { expr_id, .. } = mapping.constants[1] else {
        unreachable!()
    };
    let Some(blob) = definition.source.blob(expr_id) else {
        unreachable!()
    };
    Ok((ids, blob, array.value(row)))
}
#[derive(Clone, Copy, Default)]
struct Sizes {
    ids: usize,
    blob: usize,
    body: usize,
}
impl Sizes {
    fn metadata(self, rows: usize) -> ExecutionResult<usize> {
        rows.checked_mul(24)
            .and_then(|n| n.checked_add(self.ids.checked_mul(4)?))
            .and_then(|n| n.checked_add(self.blob))
            .ok_or_else(|| statistics_contract("statistics metadata size overflow"))
    }
    fn add(self, ids: usize, blob: usize, body: usize) -> ExecutionResult<Self> {
        Ok(Self {
            ids: self
                .ids
                .checked_add(ids)
                .ok_or_else(|| statistics_contract("statistics ID size overflow"))?,
            blob: self
                .blob
                .checked_add(blob)
                .ok_or_else(|| statistics_contract("statistics label size overflow"))?,
            body: self
                .body
                .checked_add(body)
                .ok_or_else(|| statistics_contract("statistics body size overflow"))?,
        })
    }
    fn capacity(self, rows: usize, fixed: usize) -> ExecutionResult<usize> {
        let offsets = round64(
            (rows + 1)
                .checked_mul(4)
                .ok_or_else(|| statistics_contract("statistics offset overflow"))?,
        )?;
        fixed
            .checked_add(
                offsets
                    .checked_mul(4)
                    .ok_or_else(|| statistics_contract("statistics offsets capacity overflow"))?,
            )
            .and_then(|n| n.checked_add(round64(self.ids.checked_mul(4)?).ok()?))
            .and_then(|n| n.checked_add(round64(self.blob).ok()?))
            .and_then(|n| n.checked_add(round64(self.body).ok()?))
            .and_then(|n| n.checked_add(2 * 64))
            .ok_or_else(|| statistics_contract("statistics backing capacity overflow"))
    }
}
fn round64(bytes: usize) -> ExecutionResult<usize> {
    bytes
        .checked_add(63)
        .map(|n| n & !63)
        .ok_or_else(|| statistics_contract("statistics alignment capacity overflow"))
}
struct Workspace {
    start: usize,
    rows: usize,
    sizes: Sizes,
    generation: u64,
    row: usize,
    phase: u8,
    offset: usize,
    ids_offsets: MutableBuffer,
    ids: MutableBuffer,
    blob_offsets: MutableBuffer,
    blob: MutableBuffer,
    body_offsets: MutableBuffer,
    body: MutableBuffer,
    map_offsets: MutableBuffer,
}
impl Workspace {
    fn new(start: usize, rows: usize, sizes: Sizes, generation: u64) -> Self {
        let offsets = || {
            let mut b = MutableBuffer::with_capacity(4 * (rows + 1));
            b.push(0i32);
            b
        };
        Self {
            start,
            rows,
            sizes,
            generation,
            row: 0,
            phase: 0,
            offset: 0,
            ids_offsets: offsets(),
            ids: MutableBuffer::with_capacity(4 * sizes.ids),
            blob_offsets: offsets(),
            blob: MutableBuffer::with_capacity(sizes.blob),
            body_offsets: offsets(),
            body: MutableBuffer::with_capacity(sizes.body),
            map_offsets: offsets(),
        }
    }
    fn advance(
        &mut self,
        input: &Chunk,
        definition: &Definition,
    ) -> ExecutionResult<(usize, usize)> {
        let mut examined = 0;
        let mut work = 0;
        // 64 bytes cover record lookup/source offsets and four output offsets;
        // eight work units cover fixed carrier/slot/constant lookups and writes.
        while self.row < self.rows && work + 8 <= WORK_QUANTUM && examined + 64 <= BYTE_QUANTUM {
            let (ids, blob, body) = record(input, definition, self.start + self.row)?;
            work += 8;
            examined += 64;
            let left = (BYTE_QUANTUM - examined) / 2;
            let copied = match self.phase {
                0 => {
                    let count = (left / 4).min(ids.len() - self.offset);
                    self.ids
                        .extend_from_slice(&ids[self.offset..self.offset + count]);
                    self.offset += count;
                    count * 4
                }
                1 => {
                    let count = left.min(blob.len() - self.offset);
                    self.blob
                        .extend_from_slice(&blob.as_bytes()[self.offset..self.offset + count]);
                    self.offset += count;
                    count
                }
                2 => {
                    let count = left.min(body.len() - self.offset);
                    self.body
                        .extend_from_slice(&body[self.offset..self.offset + count]);
                    self.offset += count;
                    count
                }
                _ => unreachable!(),
            };
            examined += 2 * copied;
            let len = match self.phase {
                0 => ids.len(),
                1 => blob.len(),
                _ => body.len(),
            };
            if self.offset == len {
                self.phase += 1;
                self.offset = 0;
                if self.phase == 3 {
                    self.ids_offsets.push((self.ids.len() / 4) as i32);
                    self.blob_offsets.push(self.blob.len() as i32);
                    self.body_offsets.push(self.body.len() as i32);
                    self.map_offsets.push(0i32);
                    self.row += 1;
                    self.phase = 0;
                }
            }
            if copied == 0 && self.offset != len {
                break;
            }
        }
        Ok((examined, work))
    }
    fn finish(self, schema: &ChunkSchemaRef) -> ExecutionResult<Chunk> {
        let offsets = |b: MutableBuffer| OffsetBuffer::new(ScalarBuffer::from(Buffer::from(b)));
        let DataType::List(item) = schema.slots()[0].field().data_type() else {
            unreachable!()
        };
        let DataType::Map(entries_field, false) = schema.slots()[3].field().data_type() else {
            unreachable!()
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            unreachable!()
        };
        let ids: ArrayRef = Arc::new(Int32Array::new(
            ScalarBuffer::from(Buffer::from(self.ids)),
            None,
        ));
        let fields: ArrayRef = Arc::new(
            ListArray::try_new(item.clone(), offsets(self.ids_offsets), ids, None)
                .map_err(|source| statistics_arrow("statistics input_fields list", source))?,
        );
        // SAFETY: each source is an immutable Rust str; its bytes are copied
        // unchanged. Offsets are appended only after that whole str, start at
        // zero, are monotone, and end exactly at this preallocated buffer len.
        // Revalidating the entire concatenation would defeat CPU continuation.
        let labels: ArrayRef = Arc::new(unsafe {
            StringArray::new_unchecked(offsets(self.blob_offsets), Buffer::from(self.blob), None)
        });
        let body: ArrayRef = Arc::new(BinaryArray::new(
            offsets(self.body_offsets),
            Buffer::from(self.body),
            None,
        ));
        let empty_utf8 = || {
            let mut zero = MutableBuffer::with_capacity(4);
            zero.push(0i32);
            Arc::new(StringArray::new(
                offsets(zero),
                Buffer::from(MutableBuffer::with_capacity(0)),
                None,
            )) as ArrayRef
        };
        let entries =
            StructArray::try_new(entry_fields.clone(), vec![empty_utf8(), empty_utf8()], None)
                .map_err(|source| statistics_arrow("statistics properties entries", source))?;
        let properties: ArrayRef = Arc::new(
            MapArray::try_new(
                entries_field.clone(),
                offsets(self.map_offsets),
                entries,
                None,
                false,
            )
            .map_err(|source| statistics_arrow("statistics properties map", source))?,
        );
        let batch = RecordBatch::try_new(
            schema.arrow_schema_ref(),
            vec![fields, labels, body, properties],
        )
        .map_err(crate::exec::chunk::ChunkConstructionFailure::record_batch)?;
        Ok(Chunk::try_new_with_chunk_schema(batch, schema.clone())?)
    }
}

#[derive(Debug)]
enum StatisticsFailure {
    Arrow {
        operation: &'static str,
        source: arrow::error::ArrowError,
    },
    Contract(novarocks_failure_contract::BoundedDetail),
    Limit {
        label: &'static str,
        dimension: novarocks_failure_contract::ResourceDimension,
        requested: u128,
        limit: usize,
    },
    ArtifactExtent {
        label: &'static str,
        required: u128,
        limit: usize,
    },
}
impl std::fmt::Display for StatisticsFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arrow { operation, source } => write!(f, "{operation}: {source}"),
            Self::Contract(detail) => detail.fmt(f),
            Self::Limit {
                label,
                requested,
                limit,
                ..
            } => write!(
                f,
                "{label} requested extent {requested} exceeds its limit {limit}"
            ),
            Self::ArtifactExtent {
                label,
                required,
                limit,
            } => write!(
                f,
                "{label} extent {required} exceeds its minimum-unit limit {limit}"
            ),
        }
    }
}
impl std::error::Error for StatisticsFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for StatisticsFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        match self {
            Self::Arrow { source, .. } => {
                let (class, reason) = if matches!(source, arrow::error::ArrowError::MemoryError(_))
                {
                    (
                        FailureClass::Resource,
                        ExecutionReason::StatisticsArrowAllocation,
                    )
                } else {
                    (
                        FailureClass::Invariant,
                        ExecutionReason::StatisticsArrowContract,
                    )
                };
                FailureFact::new(class, reason.reason())
            }
            Self::Contract(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::StatisticsContract.reason(),
            ),
            Self::Limit {
                dimension,
                requested,
                limit,
                ..
            } => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::StatisticsResultLimit.reason(),
            )
            .with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Operator),
                constraint_scope: Some(ResourceScope::Operator),
                dimension: *dimension,
                requested: u64::try_from(*requested).ok(),
                required_growth: None,
                limit: u64::try_from(*limit).ok(),
                live: None,
                committed: None,
            }),
            Self::ArtifactExtent {
                required, limit, ..
            } => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::StatisticsArtifactExtent.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: ExecutionReason::StatisticsArtifactExtent.code(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: u64::try_from(*limit).ok(),
                representable_prefix: None,
            }),
        }
    }
}
fn statistics_arrow(operation: &'static str, source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(StatisticsFailure::Arrow { operation, source })
}
fn statistics_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(StatisticsFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
fn statistics_limit(
    label: &'static str,
    dimension: novarocks_failure_contract::ResourceDimension,
    requested: u128,
    limit: usize,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(StatisticsFailure::Limit {
        label,
        dimension,
        requested,
        limit,
    })
}
fn statistics_extent(label: &'static str, required: u128, limit: usize) -> ExecutionFailure {
    ExecutionFailure::from_domain(StatisticsFailure::ArtifactExtent {
        label,
        required,
        limit,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, RootArrayStorageLimits, borrowed_root_chunk_storage};
    use crate::runtime::task::io::{
        ResultWriteCredit, RootInputAdmission, RootInputAuthority, RootResultWriteSpec,
    };
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
    };
    use novarocks_types::arrow_metadata_owner::{
        ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnedField, MetadataOwnerLimits,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn with_context<T>(
        state: &RuntimeState,
        action: impl FnOnce(&mut DriverStep<'_>) -> ExecutionResult<T>,
    ) -> ExecutionResult<T> {
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    fn push(
        step: &mut dyn StepOperator,
        state: &RuntimeState,
        chunk: Chunk,
    ) -> ExecutionResult<()> {
        with_context(state, |ctx| {
            match step.try_push(ctx, RawInput::for_test(chunk)) {
                InputResult::Accepted(result) => result.map(|_| ()),
                InputResult::Rejected { failure, .. } => Err(failure),
                InputResult::Deferred { .. } => panic!("fixture receiver has a live continuation"),
            }
        })
    }
    fn finish(step: &mut dyn StepOperator, state: &RuntimeState) -> ExecutionResult<()> {
        with_context(state, |ctx| step.finish(ctx))
    }
    fn covered(
        step: &mut dyn StepOperator,
        state: &RuntimeState,
        permit: &RootInputPermit,
    ) -> ExecutionResult<RootStep> {
        with_context(state, |ctx| step.drive_with_root_input(ctx, permit))
    }

    fn metadata() -> ArrowMetadataOwner {
        ArrowMetadataOwner::try_new(
            Vec::new(),
            MetadataOwnerLimits {
                entries: 0,
                construction_bytes: 0,
            },
        )
        .unwrap()
    }
    fn schema() -> ChunkSchemaRef {
        let mut owners = Vec::<MetadataOwnedField>::new();
        let mut field = |name: &str, ty| {
            let owner = metadata().into_field(name.into(), ty, false);
            let field = owner.field().clone();
            owners.push(owner);
            field
        };
        let item = field("item", DataType::Int32);
        let key = field("key", DataType::Utf8);
        let value = field("value", DataType::Utf8);
        let entries = field("entries", DataType::Struct(vec![key, value].into()));
        let fields = vec![
            field("input_fields", DataType::List(item)),
            field("blob_type", DataType::Utf8),
            field("body", DataType::Binary),
            field("properties", DataType::Map(entries, false)),
        ];
        let source = metadata().into_schema(fields.into());
        ChunkSchema::try_ref_from_owned_schema_and_slot_ids(
            &source,
            &FieldMetadataOrigins::try_new(owners, 8).unwrap(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                SlotId::new(3),
                SlotId::new(4),
            ],
        )
        .unwrap()
    }
    fn factory(
        ids: Vec<i32>,
        blob: String,
        max_rows: usize,
        mappings: usize,
    ) -> ExecutionResult<StatisticsMaterializerFactory> {
        let expr_id = novarocks_local_program::ProgramExprId::new(0);
        let literals = Arc::from([novarocks_local_program::StaticLiteral::Utf8(blob.into())]);
        let mapping = UnpivotValueMapping {
            input_value_slot_id: SlotId::new(99),
            constants: vec![
                UnpivotConstant::Int32List(ids),
                UnpivotConstant::Scalar {
                    expr_id,
                    nullable: false,
                },
                UnpivotConstant::Utf8Map(Vec::new()),
            ],
        };
        StatisticsMaterializerFactory::from_test_facts(
            literals,
            SlotId::new(3),
            vec![SlotId::new(1), SlotId::new(2), SlotId::new(4)],
            vec![mapping; mappings],
            &[SlotId::new(99)],
            schema(),
            0,
            max_rows,
            MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
        )
    }
    fn operator(factory: StatisticsMaterializerFactory) -> StatisticsMaterializer {
        StatisticsMaterializer {
            receiver: ReceiverSlot::new(StatisticsPreparation::default()),
            next: NextStep::NeedInput,
            failure: None,
            output: factory.0.schema.fixture_schema().cloned(),
            fixed_capacity: match &factory.0.schema {
                #[cfg(test)]
                StatisticsSchemaPreparation::Fixture { fixed_capacity, .. } => *fixed_capacity,
                StatisticsSchemaPreparation::Compiled(_) => 0,
            },
            definition: factory.0,
            construction: None,
            input: None,
            workspace: None,
            cursor: 0,
            rows: 0,
            body_bytes: 0,
            metadata_bytes: 0,
            finishing: false,
        }
    }
    fn input(rows: &[&[u8]]) -> (Chunk, std::sync::Weak<BinaryArray>) {
        let array = Arc::new(BinaryArray::from_iter_values(rows.iter().copied()));
        let weak = Arc::downgrade(&array);
        let schema = Arc::new(
            ChunkSchema::try_new(vec![crate::exec::chunk::ChunkSlotSchema::new_with_field(
                SlotId::new(99),
                arrow::datatypes::Field::new("aggregate", DataType::Binary, false),
                None,
                None,
            )])
            .unwrap(),
        );
        (
            Chunk::try_new_with_chunk_schema(
                RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap(),
                schema,
            )
            .unwrap(),
            weak,
        )
    }
    fn permit() -> RootInputPermit {
        let spec = RootResultWriteSpec {
            task: TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            )),
        };
        let authority = RootInputAuthority::new(
            &spec,
            Arc::new(crate::runtime::execution_health::ExecutionHealth::new()),
        );
        let crate::runtime::task::io::RootInputReservationAdmission::Granted(mut reservation) =
            authority.try_reserve().unwrap()
        else {
            panic!("one original reservation");
        };
        let mut credit = Some(ResultWriteCredit::new(reservation.required_bytes(), |_| {}));
        let RootInputAdmission::Granted(permit) = reservation.try_bind(&mut credit).unwrap() else {
            panic!("one original grant");
        };
        permit
    }
    #[test]
    fn exhausted_shared_allowance_keeps_accepted_input_unprepared_and_retains_the_same_workspace() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 1, 1).unwrap());
        let (input, weak) = input(&[b"body"]);
        push(&mut source, &state, input).unwrap();
        let grant = permit();
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 0);
        for _ in 0..2 {
            let result = source
                .drive_with_root_input(
                    &mut DriverStep::new(
                        &control,
                        &mut work,
                        &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
                    ),
                    &grant,
                )
                .unwrap();
            assert!(matches!(result, RootStep::Pending(RootPending::Yield)));
            assert!(source.workspace.is_none());
            assert!(source.input.is_none());
            assert!(
                source.receiver.is_occupied(),
                "the same accepted raw responsibility has not been prepared"
            );
            assert!(weak.upgrade().is_some());
        }
        work.begin_turn(1).unwrap();
        assert!(matches!(
            source
                .drive_with_root_input(
                    &mut DriverStep::new(
                        &control,
                        &mut work,
                        &mut novarocks_value_arrays::decode::ValueConversionContext::default()
                    ),
                    &grant
                )
                .unwrap(),
            RootStep::Pending(RootPending::Yield)
        ));
        let original = source.workspace.as_ref().unwrap();
        let generation = original.generation;
        let buffer = original.body.as_ptr();
        let row = original.row;
        let offset = original.offset;
        assert!(matches!(
            source
                .drive_with_root_input(
                    &mut DriverStep::new(
                        &control,
                        &mut work,
                        &mut novarocks_value_arrays::decode::ValueConversionContext::default()
                    ),
                    &grant
                )
                .unwrap(),
            RootStep::Pending(RootPending::Yield)
        ));
        let retained = source.workspace.as_ref().unwrap();
        assert_eq!(retained.generation, generation);
        assert_eq!(generation, grant.generation());
        assert_eq!(retained.body.as_ptr(), buffer);
        assert_eq!((retained.row, retained.offset), (row, offset));
        source.release_root_workspace();
        source.cancel();
        assert!(weak.upgrade().is_none());
        assert!(state.error().is_none());
    }

    #[test]
    fn unicode_large_body_continues_without_growth_and_finalizes_on_a_separate_turn() {
        let label = "统计λ".repeat(7000);
        let body = vec![0xff; 512 * 1024];
        let (chunk, weak) = input(&[&body]);
        let mut source = operator(factory(vec![1, 3, 9], label.clone(), 4096, 1).unwrap());
        let state = RuntimeState::default();
        let permit = permit();
        push(&mut source, &state, chunk).unwrap();
        finish(&mut source, &state).unwrap();
        assert!(matches!(
            covered(&mut source, &state, &permit).unwrap(),
            RootStep::Pending(RootPending::Yield)
        ));
        let reserved = source
            .workspace
            .as_ref()
            .unwrap()
            .sizes
            .capacity(1, source.fixed_capacity)
            .unwrap();
        let capacities = |w: &Workspace| {
            [
                w.ids_offsets.capacity(),
                w.ids.capacity(),
                w.blob_offsets.capacity(),
                w.blob.capacity(),
                w.body_offsets.capacity(),
                w.body.capacity(),
                w.map_offsets.capacity(),
            ]
        };
        let original = capacities(source.workspace.as_ref().unwrap());
        let mut turns = 0;
        while source.workspace.as_ref().unwrap().row == 0 {
            let w = source.workspace.as_mut().unwrap();
            let (examined, work) = w
                .advance(source.input.as_ref().unwrap(), &source.definition)
                .unwrap();
            assert!(examined <= BYTE_QUANTUM && work <= WORK_QUANTUM);
            assert_eq!(capacities(w), original);
            turns += 1;
        }
        assert!(turns > 16);
        let RootStep::Complete(StepResult {
            output: Some(output),
            ..
        }) = covered(&mut source, &state, &permit).unwrap()
        else {
            panic!("separate finalization turn");
        };
        assert_eq!(
            output
                .record_batch()
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            label
        );
        assert_eq!(
            output
                .record_batch()
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            body
        );
        let actual = borrowed_root_chunk_storage(
            &output,
            RootArrayStorageLimits {
                bytes: MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
                nodes: 8192,
                depth: 64,
            },
        )
        .unwrap();
        assert!(
            actual <= reserved,
            "actual {actual} exceeds prepaid {reserved}"
        );
        assert!(weak.upgrade().is_none());
        assert!(matches!(source.data_next(), NextStep::Finished));
    }
    fn actual_factory(
        tracker: &Arc<crate::runtime::mem_tracker::MemTracker>,
        error: &Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> Arc<dyn InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            error.clone(),
        )
        .unwrap()
    }

    #[test]
    fn compiled_statistics_schema_last_slot_retains_prepare_owner_after_output_exit() {
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled statistics last schema slot",
        );
        let state = RuntimeState::default();
        let graph = compiled_graph(true, &state);
        let error = state.error_state();
        let error_weak = Arc::downgrade(&error);
        let creation = actual_factory(&tracker, &error);
        let mut values = graph.pipelines[0].factories[0].create(0, 0);
        values.set_mem_tracker(tracker.clone());
        values.set_construction_factory(creation.clone());
        values.bind_runtime_state(&state).unwrap();
        values.prepare().unwrap();
        let input = with_context(&state, |ctx| values.as_mut().drive(ctx))
            .unwrap()
            .output
            .unwrap();
        let mut operator = graph.pipelines[0].factories[1].create(0, 0);
        assert!(operator.creates_typed_storage());
        operator.set_construction_factory(creation.clone());
        operator.prepare().unwrap();
        let processor = operator.as_mut();
        push(processor, &state, input).unwrap();
        finish(processor, &state).unwrap();
        let permit = permit();
        let output = loop {
            match covered(processor, &state, &permit).unwrap() {
                RootStep::Pending(RootPending::Yield) => {}
                RootStep::Complete(StepResult {
                    output: Some(chunk),
                    ..
                }) => break chunk,
                RootStep::Complete(StepResult { output: None, .. }) => {
                    panic!("the admitted body must be emitted")
                }
                RootStep::Pending(other) => panic!("unexpected continuation: {other:?}"),
            }
        };
        assert_eq!(
            output
                .record_batch()
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"actual decoded aggregate"
        );
        let slot = output.chunk_schema().slots()[0].clone();
        drop(operator);
        drop(values);
        drop(graph);
        drop(creation);
        drop(error);
        drop(state);
        drop(permit);
        assert!(
            error_weak.upgrade().is_none(),
            "the output keeps no RuntimeErrorState"
        );
        assert!(tracker.current() > 0);
        drop(output);
        assert!(
            tracker.current() > 0,
            "a last slot retains its actual schema creator"
        );
        drop(slot);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn compiled_statistics_prepare_refusal_preserves_actual_memory_cause_and_old_host() {
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled statistics prepare refusal",
        );
        let state = RuntimeState::default();
        let graph = compiled_graph(true, &state);
        let creation = actual_factory(&tracker, &state.error_state());
        let mut operator = graph.pipelines[0].factories[1].create(0, 0);
        operator.set_construction_factory(creation.clone());
        let before = tracker.current();
        tracker.install_limit_once(before + 1).unwrap();
        let refused = operator.prepare().unwrap_err();
        assert!(
            matches!(
                refused.cause(),
                crate::runtime::task::ExecutionFailureCause::Construction(
                    ConstructionFailure::Memory(_)
                )
            ),
            "{refused:?}"
        );
        assert_eq!(tracker.current(), before);
        drop(operator);
        drop(graph);
        drop(creation);
        drop(state);
        assert_eq!(tracker.current(), 0);
    }

    /// Real published PhysicalPlan facts go through local-compiler and the
    /// compiled pipeline, including the exact protected-result sink boundary.
    fn compiled_graph(
        protected: bool,
        state: &RuntimeState,
    ) -> crate::exec::pipeline::builder::PipelineGraph {
        use crate::exec::pipeline::builder::{
            build_compiled_pipeline_graph, compile_family_fixture,
            family_package_with_sink_and_names, family_values,
        };
        use crate::exec::pipeline::{
            binding::{ExchangeBindings, ScanBindings},
            dependency::DependencyManager,
        };
        use novarocks_physical_plan::{
            ConstantPoolId, ConstantPools, ConstantReference, ExprKind, FragmentBuilder,
            FragmentId, FragmentSink, LiteralValue as PhysicalLiteral, NodeId, NodeKind,
            UnpivotConstant as PhysicalConstant, UnpivotSpec,
            UnpivotValueMapping as PhysicalMapping, ValueOrigin,
        };
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
        };
        struct PureFixtureControl;
        impl PureCompileControl for PureFixtureControl {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                Ok(())
            }
        }
        let pure = PureFixtureControl;
        // Explicit tiny FE fixture admission. It is neither a BE grant nor a
        // production candidate/default receiving profile.
        let policy = novarocks_functions::ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        };
        let item = Arc::new(arrow::datatypes::Field::new("item", DataType::Int32, false));
        let list_type = DataType::List(item.clone());
        let list = ListArray::try_new(
            item,
            OffsetBuffer::from_lengths([1]),
            Arc::new(Int32Array::from(vec![1])),
            None,
        )
        .unwrap();
        let list_binding = FunctionValueType::new(list_type.clone(), false);
        let list_pool = novarocks_functions::ConstantPool::try_new(
            Arc::new(arrow::datatypes::Field::new("ids", list_type, false)),
            list_binding.clone(),
            list.to_data(),
            policy,
            CompilePhase::Validate,
            &pure,
        )
        .unwrap();
        let entry_fields: arrow::datatypes::Fields = vec![
            Arc::new(arrow::datatypes::Field::new("key", DataType::Utf8, false)),
            Arc::new(arrow::datatypes::Field::new("value", DataType::Utf8, false)),
        ]
        .into();
        let entries = Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(entry_fields.clone()),
            false,
        ));
        let map_type = DataType::Map(entries.clone(), false);
        let map = MapArray::try_new(
            entries,
            OffsetBuffer::from_lengths([0]),
            StructArray::try_new(
                entry_fields,
                vec![
                    Arc::new(StringArray::from(Vec::<&str>::new())),
                    Arc::new(StringArray::from(Vec::<&str>::new())),
                ],
                None,
            )
            .unwrap(),
            None,
            false,
        )
        .unwrap();
        let map_binding = FunctionValueType::new(map_type.clone(), false);
        let map_pool = novarocks_functions::ConstantPool::try_new(
            Arc::new(arrow::datatypes::Field::new("properties", map_type, false)),
            map_binding.clone(),
            map.to_data(),
            policy,
            CompilePhase::Validate,
            &pure,
        )
        .unwrap();
        let mut constants = ConstantPools::empty();
        constants.insert(ConstantPoolId::new(1), list_pool).unwrap();
        constants.insert(ConstantPoolId::new(2), map_pool).unwrap();
        let mut builder = FragmentBuilder::new(FragmentId::new(73));
        let source = NodeId::new(0);
        let target = NodeId::new(1);
        let body_type = FunctionValueType::new(DataType::Binary, false);
        let source_columns = family_values(
            &mut builder,
            source,
            std::slice::from_ref(&body_type),
            &[vec![PhysicalLiteral::Binary(
                b"actual decoded aggregate".to_vec().into_boxed_slice(),
            )]],
        );
        let label_type = FunctionValueType::new(DataType::Utf8, false);
        let label = builder
            .add_expression(
                target,
                label_type.clone(),
                ExprKind::Literal(PhysicalLiteral::Utf8("theta".into())),
            )
            .unwrap();
        let output = [list_binding, label_type, body_type, map_binding]
            .into_iter()
            .enumerate()
            .map(|(ordinal, ty)| {
                builder
                    .add_value(
                        ty,
                        ValueOrigin::NodeOutput {
                            node: target,
                            output_ordinal: ordinal as u32,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        builder
            .add_row_rewriting(
                target,
                source,
                Some(&std::collections::BTreeMap::new()),
                output.clone().into_boxed_slice(),
                NodeKind::Unpivot {
                    spec: UnpivotSpec {
                        passthrough: Box::default(),
                        value_output: output[2],
                        literal_outputs: Box::from([output[0], output[1], output[3]]),
                        mappings: Box::from([PhysicalMapping {
                            input: source_columns[0],
                            constants: Box::from([
                                PhysicalConstant::Int32List(ConstantReference {
                                    pool: ConstantPoolId::new(1),
                                    ordinal: 0,
                                }),
                                PhysicalConstant::Scalar(label),
                                PhysicalConstant::Utf8Map(ConstantReference {
                                    pool: ConstantPoolId::new(2),
                                    ordinal: 0,
                                }),
                            ]),
                        }]),
                        max_output_rows: 4096,
                        max_output_bytes: u64::try_from(
                            MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
                        )
                        .unwrap(),
                    },
                },
            )
            .unwrap();
        let package = family_package_with_sink_and_names(
            builder,
            target,
            constants,
            1,
            if protected {
                FragmentSink::RootResult(Box::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
                )))
            } else {
                FragmentSink::Result
            },
            Some(&["input_fields", "blob_type", "body", "properties"]),
        );
        let program = compile_family_fixture(package, 1);
        build_compiled_pipeline_graph(
            &program,
            ExchangeBindings::default(),
            ScanBindings::default(),
            crate::runtime::task::CompiledWriterBindings::default(),
            None,
            DependencyManager::new(),
            1,
            Some(1),
            crate::runtime::execution_runtime::test_execution_function_set(),
            state.error_state(),
        )
        .unwrap()
    }

    #[test]
    fn real_compiled_builder_selects_the_protected_source_only_for_statistics_root() {
        for protected in [true, false] {
            let state = RuntimeState::default();
            let graph = compiled_graph(protected, &state);
            assert_eq!(graph.pipelines.len(), 1);
            let factories = &graph.pipelines[0].factories;
            assert_eq!(factories.len(), 2);
            assert_eq!(factories[1].name() == NAME, protected);
            if protected {
                let tracker = crate::runtime::mem_tracker::MemTracker::new_root(
                    "compiled statistics protected source",
                );
                let construction = actual_factory(&tracker, &state.error_state());
                let mut source = factories[0].create(0, 0);
                source.set_mem_tracker(tracker.clone());
                source.set_construction_factory(construction.clone());
                source.bind_runtime_state(&state).unwrap();
                source.prepare().unwrap();
                let input = with_context(&state, |ctx| source.as_mut().drive(ctx))
                    .unwrap()
                    .output
                    .unwrap();
                let mut invalid = factories[1].create(0, 0);
                invalid.set_construction_factory(construction.clone());
                invalid.prepare().unwrap();
                let invalid_state = RuntimeState::default();
                push(invalid.as_mut(), &invalid_state, input.clone()).unwrap();
                assert!(with_context(&invalid_state, |ctx| invalid.as_mut().drive(ctx)).is_err());
                invalid.cancel();
                drop(invalid);
                let mut materializer = factories[1].create(0, 0);
                materializer.set_construction_factory(construction);
                materializer.prepare().unwrap();
                let materializer = materializer.as_mut();
                push(materializer, &state, input).unwrap();
                let permit = permit();
                loop {
                    match covered(materializer, &state, &permit).unwrap() {
                        RootStep::Pending(RootPending::Yield) => {}
                        RootStep::Complete(StepResult {
                            output: Some(chunk),
                            ..
                        }) => {
                            assert_eq!(
                                chunk
                                    .record_batch()
                                    .column(2)
                                    .as_any()
                                    .downcast_ref::<BinaryArray>()
                                    .unwrap()
                                    .value(0),
                                b"actual decoded aggregate"
                            );
                            assert!(
                                borrowed_root_chunk_storage(
                                    &chunk,
                                    RootArrayStorageLimits {
                                        bytes: 32 << 20,
                                        nodes: 8192,
                                        depth: 64
                                    }
                                )
                                .is_ok()
                            );
                            break;
                        }
                        RootStep::Complete(StepResult { output: None, .. }) => {
                            panic!("actual output disappeared")
                        }
                        RootStep::Pending(other) => panic!("unexpected continuation: {other:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn a_completed_copy_always_yields_before_arrow_finalization() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "test".into(), 1, 1).unwrap());
        let bytes = vec![0; 32752];
        let (chunk, _) = input(&[&bytes]);
        push(&mut source, &state, chunk).unwrap();
        let permit = permit();
        assert!(matches!(
            covered(&mut source, &state, &permit).unwrap(),
            RootStep::Pending(RootPending::Yield)
        ));
        while source.workspace.as_ref().unwrap().row == 0 {
            assert!(matches!(
                covered(&mut source, &state, &permit).unwrap(),
                RootStep::Pending(RootPending::Yield)
            ));
        }
        assert!(matches!(
            covered(&mut source, &state, &permit).unwrap(),
            RootStep::Complete(StepResult {
                output: Some(_),
                ..
            })
        ));
    }
    #[test]
    fn one_bounded_artifact_per_batch_does_not_shrink_the_upstream_aggregate() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 4096, 1).unwrap());
        let bytes = vec![0xff; MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES];
        let (chunk, weak) = input(&[&bytes, &bytes]);
        push(&mut source, &state, chunk).unwrap();
        finish(&mut source, &state).unwrap();
        for index in 0..2 {
            let permit = permit();
            loop {
                match covered(&mut source, &state, &permit).unwrap() {
                    RootStep::Pending(RootPending::Yield) => {}
                    RootStep::Complete(StepResult {
                        output: Some(chunk),
                        ..
                    }) => {
                        assert_eq!(chunk.len(), 1);
                        assert_eq!(
                            chunk
                                .record_batch()
                                .column(2)
                                .as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .value(0),
                            bytes
                        );
                        assert!(
                            borrowed_root_chunk_storage(
                                &chunk,
                                RootArrayStorageLimits {
                                    bytes: 32 << 20,
                                    nodes: 8192,
                                    depth: 64
                                }
                            )
                            .is_ok()
                        );
                        break;
                    }
                    RootStep::Complete(StepResult { output: None, .. }) => {
                        panic!("expected bounded artifact")
                    }
                    RootStep::Pending(other) => panic!("unexpected continuation: {other:?}"),
                }
            }
            assert_eq!(weak.upgrade().is_some(), index == 0);
        }
        assert!(matches!(source.data_next(), NextStep::Finished));
    }

    #[test]
    fn per_node_row_limit_preserves_mapping_major_order_across_original_grants() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 1, 2).unwrap());
        let (chunk, _) = input(&[b"a", b"b", b"c"]);
        push(&mut source, &state, chunk).unwrap();
        finish(&mut source, &state).unwrap();
        let mut bodies = Vec::new();
        for _ in 0..6 {
            let permit = permit();
            assert!(matches!(
                covered(&mut source, &state, &permit).unwrap(),
                RootStep::Pending(RootPending::Yield)
            ));
            loop {
                match covered(&mut source, &state, &permit).unwrap() {
                    RootStep::Pending(RootPending::Yield) => {}
                    RootStep::Complete(StepResult {
                        output: Some(chunk),
                        ..
                    }) => {
                        assert_eq!(chunk.len(), 1);
                        bodies.push(
                            chunk
                                .record_batch()
                                .column(2)
                                .as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .value(0)
                                .to_vec(),
                        );
                        break;
                    }
                    RootStep::Complete(StepResult { output: None, .. }) => {
                        panic!("pending record disappeared")
                    }
                    RootStep::Pending(other) => panic!("unexpected continuation: {other:?}"),
                }
            }
        }
        assert_eq!(bodies, [b"a", b"b", b"c", b"a", b"b", b"c"]);
        assert!(matches!(source.data_next(), NextStep::Finished));
    }
    #[test]
    fn invalid_static_ids_fail_before_materialization_and_dynamic_limits_before_workspace() {
        for ids in [vec![-1], vec![1, 1], Vec::new()] {
            use novarocks_failure_contract::FailureProjection;
            let error = match factory(ids, "theta".into(), 1, 1) {
                Ok(_) => panic!("invalid static IDs must fail"),
                Err(error) => error,
            };
            assert_eq!(
                error.failure_fact().class(),
                novarocks_failure_contract::FailureClass::Invariant
            );
            assert_eq!(error.failure_fact().reason().code(), 536);
        }
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 1, 1).unwrap());
        source.metadata_bytes = 16 * 1024 * 1024;
        let (chunk, weak) = input(&[b"body"]);
        push(&mut source, &state, chunk).unwrap();
        let failure = covered(&mut source, &state, &permit()).err().unwrap();
        use novarocks_failure_contract::{FailureClass, FailureProjection, ResourceDimension};
        assert!(failure.to_string().contains("metadata"));
        assert_eq!(failure.failure_fact().class(), FailureClass::Resource);
        let facts = failure.failure_fact().resource().unwrap();
        assert_eq!(facts.dimension, ResourceDimension::Bytes);
        assert_eq!(facts.requested, Some(16 * 1024 * 1024 + 33));
        assert_eq!(facts.limit, Some(16 * 1024 * 1024));
        assert_eq!(facts.live, None);
        assert_eq!(facts.committed, None);
        assert!(source.workspace.is_none());
        source.cancel();
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn cancellation_and_generation_conflict_leave_cleanup_under_the_original_grant() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 1, 1).unwrap());
        let (chunk, weak) = input(&[b"body"]);
        push(&mut source, &state, chunk).unwrap();
        let permit = permit();
        covered(&mut source, &state, &permit).unwrap();
        // An issuer change is validated by the final edge/session; source
        // additionally rejects a changed continuation generation.
        source.workspace.as_mut().unwrap().generation += 1;
        assert!(covered(&mut source, &state, &permit).is_err());
        assert!(source.workspace.is_some());
        source.release_root_workspace();
        assert!(source.workspace.is_none());
        assert!(weak.upgrade().is_some());
        source.cancel();
        assert!(weak.upgrade().is_none());
        assert!(matches!(source.initial_step(), NextStep::Finished));
    }
}
