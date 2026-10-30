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

//! Closed ownership of a Local result graph and its pure render cursor.

use super::{LocalResultBound, QueryResult};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_result_contract::{
    ClientPresentationFamily, ClientRenderSchema, NativeRenderType, RenderColumn, RenderField,
    RenderPresentation, RootProfileV1,
};
use novarocks_result_render::{
    BoundedClientRowEncoder, ClientRowEncoder, RenderError, RenderTurn, RenderTurnStatus,
};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkScope};
use std::sync::Arc;

// Frozen Local source + conversion + output overlap, not another wallet.
const LOCAL_PEAK_BYTES: u64 = (16 + 1 + 4 + 32 + 32 + 8) * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalColumnKind {
    Utf8,
    Boolean,
    Int32,
    Int64,
}

/// Borrowed metadata cannot publish an Arrow/schema/ResultField alias.
#[derive(Clone, Copy, Debug)]
pub struct LocalColumnView<'a> {
    pub name: &'a str,
    pub nullable: bool,
    pub kind: LocalColumnKind,
}

/// One admitted, closed application producer. Its callback must construct a
/// fresh graph, without retaining or publishing Arrow aliases. This contract
/// is established by the application source audit; it cannot be inferred from
/// an arbitrary QueryResult's capacities. Internal/legacy results do not enter
/// this factory merely because they contain Arrow.
pub struct LocalResultProducer {
    window: ResultWindowAlias,
    family: ClientPresentationFamily,
}
impl LocalResultProducer {
    pub fn try_new(
        scope: &WorkScope,
        window: ResultWindowAlias,
        family: ClientPresentationFamily,
    ) -> Result<Self, String> {
        if !window.is_for_scope(scope)
            || !matches!(
                window.class(),
                ResultWindowClass::Local | ResultWindowClass::Internal
            )
        {
            return Err("Local producer requires its exact Local or Internal window".into());
        }
        scope.check().map_err(|error| error.to_string())?;
        window
            .check_backing_total(LOCAL_PEAK_BYTES)
            .map_err(|error| error.to_string())?;
        Ok(Self { window, family })
    }
    /// The complete allowance already exists before the source is called.
    /// Failure/panic destroys source locals before this capability can exit.
    pub fn produce(
        self,
        source: impl FnOnce() -> Result<QueryResult, String>,
    ) -> Result<OwnedLocalResult, String> {
        let result = source()?;
        OwnedLocalResult::seal_exclusive(result, self.window, self.family)
    }
}

/// No Clone, raw-batch getter or graph extraction is provided. All copies made
/// by rendering stay inside the same guarded graph.
///
/// ```compile_fail
/// use novarocks_query_application::api::OwnedLocalResult;
/// fn escape(result: OwnedLocalResult) { let _ = result.into_batches(); }
/// ```
pub struct OwnedLocalResult {
    result: QueryResult,
    schema: Arc<ClientRenderSchema>,
    retained_backing_bytes: u64,
    // Fields drop in declaration order: every graph owner precedes the guard.
    window: ResultWindowAlias,
}
struct LocalSourceInput {
    result: QueryResult,
    window: ResultWindowAlias,
}
impl OwnedLocalResult {
    /// Pure identity observation; no alias or capacity is created.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.window.is_for_scope(scope)
    }

    pub(crate) fn seal_exclusive(
        result: QueryResult,
        window: ResultWindowAlias,
        family: ClientPresentationFamily,
    ) -> Result<Self, String> {
        let input = LocalSourceInput { result, window };
        if !matches!(
            input.window.class(),
            ResultWindowClass::Local | ResultWindowClass::Internal
        ) {
            return Err("Local graph requires a Local or Internal window".into());
        }
        let fields = &input.result.columns;
        if fields.is_empty() || fields.len() > RootProfileV1::MAX_COLUMNS {
            return Err("Local result requires a bounded nonempty schema".into());
        }
        let mut backing =
            size_of::<ClientRenderSchema>() + fields.len() * size_of::<RenderColumn>();
        let mut wire = 0usize;
        for field in fields {
            match field.value_type().value_type().kind() {
                novarocks_type_contract::ValueKind::String => {}
                novarocks_type_contract::ValueKind::Scalar(
                    arrow::datatypes::DataType::Boolean
                    | arrow::datatypes::DataType::Int32
                    | arrow::datatypes::DataType::Int64,
                ) => {}
                _ => return Err("Local result field is outside its closed value contract".into()),
            }
            kind(field.data_type(), false)?;
            if field.name().is_empty() || field.name().len() > RootProfileV1::MAX_NAME_BYTES {
                return Err("Local result name exceeds its schema bound".into());
            }
            backing = backing
                .checked_add(field.name().len())
                .ok_or("Local schema size overflows")?;
            wire = wire
                .checked_add(field.name().len())
                .and_then(|n| n.checked_add(48))
                .ok_or("Local schema size overflows")?;
        }
        if backing > RootProfileV1::SCHEMA_BACKING_BYTES || wire > RootProfileV1::SCHEMA_WIRE_BYTES
        {
            return Err("Local render schema exceeds its backing/wire bound".into());
        }
        let mut rows = 0usize;
        let mut buffers = 0usize;
        for batch in &input.result.batches {
            if batch.num_columns() != fields.len() {
                return Err("Local result batch differs from its declared schema".into());
            }
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or("Local row count overflows")?;
            for (array, (arrow_field, field)) in batch
                .columns()
                .iter()
                .zip(batch.schema_ref().fields().iter().zip(fields))
            {
                if array.data_type() != field.data_type()
                    || arrow_field.data_type() != field.data_type()
                    || arrow_field.is_nullable() != field.nullable()
                {
                    return Err("Local result batch differs from its declared schema".into());
                }
                buffers = buffers
                    .checked_add(array.get_buffer_memory_size())
                    .ok_or("Local buffer capacities overflow")?;
            }
        }
        if rows > LocalResultBound::V1.rows
            || input.result.batches.len() > LocalResultBound::V1.rows + 1
        {
            return Err("Local result exceeds its row/batch bound".into());
        }
        // Collector payload plus fixed per-column buffer padding/offset zero
        // occupies at most 1 MiB of the already admitted 8 MiB root share.
        // Source growth checks, not this transfer check, prove construction.
        if buffers > LocalResultBound::V1.bytes + RootProfileV1::SCHEMA_BACKING_BYTES {
            return Err("Local result exceeds its actual buffer capacity bound".into());
        }
        input
            .window
            .check_backing_total(LOCAL_PEAK_BYTES)
            .map_err(|error| error.to_string())?;
        let columns = fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                Ok(RenderColumn {
                    source_ordinal: ordinal as u32,
                    source_slot: None,
                    name: field.name().to_owned(),
                    field: RenderField {
                        presentation: match family {
                            ClientPresentationFamily::MysqlText => RenderPresentation::ScalarText,
                            ClientPresentationFamily::TrinoJson(_) => {
                                RenderPresentation::TrinoValue
                            }
                        },
                        nullable: field.nullable(),
                        native_type: native(kind(field.data_type(), false)?),
                    },
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let schema = Arc::new(
            ClientRenderSchema::try_new(family, columns, fields.len())
                .map_err(|error| error.to_string())?,
        );
        let retained_backing_bytes = buffers
            .checked_add(schema.backing_bytes())
            .and_then(|bytes| bytes.checked_add(ClientRowEncoder::scratch_capacity_bound_bytes()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or("Local delivery backing size overflows")?;
        let LocalSourceInput { result, window } = input;
        Ok(Self {
            result,
            schema,
            retained_backing_bytes,
            window,
        })
    }
    /// Borrow the exact pure declaration sealed with this owned graph.
    pub fn client_render(&self) -> &ClientRenderSchema {
        &self.schema
    }
    pub fn row_count(&self) -> usize {
        self.result.row_count()
    }
    pub fn columns(&self) -> impl ExactSizeIterator<Item = LocalColumnView<'_>> {
        self.result.columns.iter().map(|field| LocalColumnView {
            name: field.name(),
            nullable: field.nullable(),
            kind: kind(field.data_type(), false).expect("sealed Local column kind"),
        })
    }
    pub fn into_cursor(self) -> LocalRenderCursor {
        let Self {
            result,
            schema,
            retained_backing_bytes,
            window,
        } = self;
        LocalRenderCursor {
            encoder: None,
            remaining: result.batches.into_iter(),
            schema,
            terminal_error: None,
            retained_backing_bytes,
            _window: window,
        }
    }
}
fn kind(data_type: &DataType, has_logical_type: bool) -> Result<LocalColumnKind, String> {
    if has_logical_type {
        return Err("Local result has an unsupported logical domain".into());
    }
    match data_type {
        DataType::Utf8 => Ok(LocalColumnKind::Utf8),
        DataType::Boolean => Ok(LocalColumnKind::Boolean),
        DataType::Int32 => Ok(LocalColumnKind::Int32),
        DataType::Int64 => Ok(LocalColumnKind::Int64),
        _ => Err("Local result has an unsupported declared type".into()),
    }
}
fn native(kind: LocalColumnKind) -> NativeRenderType {
    match kind {
        LocalColumnKind::Utf8 => NativeRenderType::String,
        LocalColumnKind::Boolean => NativeRenderType::Boolean,
        LocalColumnKind::Int32 => NativeRenderType::SignedInteger(32),
        LocalColumnKind::Int64 => NativeRenderType::SignedInteger(64),
    }
}

/// A move-only cursor that exposes only encoded bytes. Its private renderer,
/// remaining batches, schema and scratch all exit before the final guard.
pub struct LocalRenderCursor {
    encoder: Option<ClientRowEncoder>,
    remaining: std::vec::IntoIter<RecordBatch>,
    schema: Arc<ClientRenderSchema>,
    terminal_error: Option<RenderError>,
    retained_backing_bytes: u64,
    _window: ResultWindowAlias,
}
impl LocalRenderCursor {
    /// The original buffer-capacity sum, immutable render schema and maximum
    /// future encoder scratch coexist with delivery. Keep the frozen observation
    /// through actual cursor exit, including after cancellation or batch release.
    pub fn retained_backing_bytes(&self) -> u64 {
        self.retained_backing_bytes
    }
    pub fn client_render(&self) -> &ClientRenderSchema {
        &self.schema
    }
    pub fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, RenderError> {
        if let Some(error) = self.terminal_error {
            return Err(error);
        }
        let outcome = self.step_live(output);
        if let Err(error) = outcome {
            self.terminal_error = Some(error);
        }
        outcome
    }
    fn step_live(&mut self, output: &mut [u8]) -> Result<RenderTurn, RenderError> {
        if self.encoder.is_none() {
            let Some(batch) = self.remaining.next() else {
                return Ok(RenderTurn {
                    emitted_bytes: 0,
                    examined_bytes: 0,
                    visited_cells: 0,
                    completed_rows: 0,
                    status: RenderTurnStatus::InputComplete,
                });
            };
            self.encoder = Some(ClientRowEncoder::try_new(Arc::clone(&self.schema), batch)?);
        }
        let mut turn = self
            .encoder
            .as_mut()
            .expect("installed Local encoder")
            .step(output)?;
        if turn.status == RenderTurnStatus::InputComplete {
            self.encoder = None;
            if !self.remaining.as_slice().is_empty() {
                turn.status = RenderTurnStatus::Yielded;
            }
        }
        Ok(turn)
    }
    pub fn retain_physical_guard(&self) -> ResultWindowAlias {
        self._window.clone()
    }
    pub fn cancel(&mut self) {
        if self.terminal_error.is_none() {
            self.terminal_error = Some(RenderError {
                kind: novarocks_result_render::RenderErrorKind::Cancelled,
                output_ordinal: None,
                required: None,
            });
        }
        if let Some(encoder) = &mut self.encoder {
            encoder.cancel();
        }
    }
    pub fn scratch_capacity_bytes(&self) -> usize {
        self.encoder
            .as_ref()
            .map_or(0, BoundedClientRowEncoder::scratch_capacity_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    fn fixture() -> (
        WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let host = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = host
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        host.mark_ready().unwrap();
        (host, capacity)
    }
    fn produce_text(host: &WorkloadControl, text: &str) -> OwnedLocalResult {
        produce_text_for_family(host, text, ClientPresentationFamily::MysqlText)
    }
    fn produce_text_for_family(
        host: &WorkloadControl,
        text: &str,
        family: ClientPresentationFamily,
    ) -> OwnedLocalResult {
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let source =
            LocalResultProducer::try_new(&root.owner.scope(), window.retain_alias(), family)
                .unwrap();
        let result = source
            .produce(|| super::super::build_string_query_result("value", vec![text.to_owned()]))
            .unwrap();
        root.owner.complete();
        root.business.release();
        drop(window);
        result
    }
    #[test]
    fn opaque_graph_cursor_and_last_payload_guard_share_one_position() {
        let (host, capacity) = fixture();
        let result = produce_text(&host, "hello");
        assert_eq!(result.row_count(), 1);
        let column = result.columns().next().unwrap();
        assert_eq!(column.name, "value");
        assert_eq!(column.kind, LocalColumnKind::Utf8);
        let mut cursor = result.into_cursor();
        let physical = cursor.retain_physical_guard();
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let mut bytes = [0; 256];
        let turn = cursor.step(&mut bytes).unwrap();
        assert_eq!(turn.status, RenderTurnStatus::InputComplete);
        assert_eq!(
            &bytes[..turn.emitted_bytes],
            &[6, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o']
        );
        drop(cursor);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(physical);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(host.snapshot().root_responsibilities, 0);
    }
    #[test]
    fn cancellation_latches_before_first_encoder_and_after_a_counting_turn() {
        for (family, after_count) in [
            (ClientPresentationFamily::MysqlText, false),
            (ClientPresentationFamily::MysqlText, true),
            (
                ClientPresentationFamily::TrinoJson(Default::default()),
                false,
            ),
            (
                ClientPresentationFamily::TrinoJson(Default::default()),
                true,
            ),
        ] {
            let (host, capacity) = fixture();
            let result = produce_text_for_family(&host, &"x".repeat(128 * 1024), family);
            let mut cursor = result.into_cursor();
            let retained = cursor.retained_backing_bytes();
            assert!(retained >= ClientRowEncoder::scratch_capacity_bound_bytes() as u64);
            assert_eq!(cursor.scratch_capacity_bytes(), 0);
            let mut output = [0xab; 128];
            if after_count {
                let turn = cursor.step(&mut output).unwrap();
                assert!(turn.examined_bytes <= RootProfileV1::EMIT_BYTES_PER_TURN);
                assert_eq!(turn.emitted_bytes, 0);
                assert_eq!(
                    cursor.scratch_capacity_bytes(),
                    ClientRowEncoder::scratch_capacity_bound_bytes()
                );
                assert_eq!(cursor.retained_backing_bytes(), retained);
            }
            cursor.cancel();
            assert_eq!(cursor.retained_backing_bytes(), retained);
            for _ in 0..2 {
                assert_eq!(
                    cursor.step(&mut output).unwrap_err().kind,
                    novarocks_result_render::RenderErrorKind::Cancelled
                );
            }
            assert!(output.iter().all(|byte| *byte == 0xab));
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
            drop(cursor);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        }
    }
    #[test]
    fn delivery_backing_counts_spare_buffers_near_collector_for_both_window_classes() {
        use super::super::{ResultField, build_arrow_query_result};
        use arrow::array::{Array, StringArray};
        use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};

        for class in [ResultWindowClass::Local, ResultWindowClass::Internal] {
            for family in [
                ClientPresentationFamily::MysqlText,
                ClientPresentationFamily::TrinoJson(Default::default()),
            ] {
                let (host, capacity) = fixture();
                let (root, window) = host
                    .root_admission()
                    .try_begin_root_with_result(WorkRequest::new(WorkClass::Management), class)
                    .unwrap();
                let source = LocalResultProducer::try_new(
                    &root.owner.scope(),
                    window.retain_alias(),
                    family,
                )
                .unwrap();
                let measured = std::cell::Cell::new(0usize);
                let result = source
                    .produce(|| {
                        let mut values = Vec::with_capacity(LocalResultBound::V1.bytes - 4096);
                        values.push(b'v');
                        let array = StringArray::new(
                            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1])),
                            Buffer::from(values),
                            None,
                        );
                        measured.set(array.get_buffer_memory_size());
                        build_arrow_query_result(
                            vec![ResultField::new(
                                "value",
                                novarocks_type_contract::FunctionValueType::new(
                                    DataType::Utf8,
                                    false,
                                ),
                            )],
                            vec![Arc::new(array)],
                        )
                    })
                    .unwrap();
                assert!(measured.get() > LocalResultBound::V1.bytes - 8192);
                let expected = measured.get()
                    + result.client_render().backing_bytes()
                    + ClientRowEncoder::scratch_capacity_bound_bytes();
                root.owner.complete();
                root.business.release();
                drop(window);

                let mut cursor = result.into_cursor();
                assert_eq!(cursor.client_render().family(), &family);
                assert_eq!(cursor.retained_backing_bytes(), expected as u64);
                assert!(cursor.retained_backing_bytes() < LOCAL_PEAK_BYTES);
                assert_eq!(cursor.scratch_capacity_bytes(), 0);
                let mut output = [0; 256];
                let turn = cursor.step(&mut output).unwrap();
                assert_eq!(turn.status, RenderTurnStatus::InputComplete);
                let expected_bytes: &[u8] = match family {
                    ClientPresentationFamily::MysqlText => &[2, 0, 0, 0, 1, b'v'],
                    ClientPresentationFamily::TrinoJson(_) => {
                        &[5, 0, 0, 0, b'[', b'"', b'v', b'"', b']']
                    }
                };
                assert_eq!(&output[..turn.emitted_bytes], expected_bytes);
                assert_eq!(cursor.retained_backing_bytes(), expected as u64);
                cursor.cancel();
                assert_eq!(cursor.retained_backing_bytes(), expected as u64);
                let index = match class {
                    ResultWindowClass::Local => 1,
                    ResultWindowClass::Internal => 2,
                    _ => unreachable!(),
                };
                assert_eq!(capacity.snapshot().held_positions[index], 1);
                drop(cursor);
                assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            }
        }
    }

    #[test]
    fn empty_local_graph_keeps_schema_and_future_scratch_observation_until_exit() {
        use super::super::ResultField;
        let (host, capacity) = fixture();
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let result = LocalResultProducer::try_new(
            &root.owner.scope(),
            window.retain_alias(),
            ClientPresentationFamily::TrinoJson(Default::default()),
        )
        .unwrap()
        .produce(|| {
            Ok(QueryResult {
                columns: vec![ResultField::new(
                    "value",
                    novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                )],
                batches: Vec::new(),
            })
        })
        .unwrap();
        let expected = result.client_render().backing_bytes()
            + ClientRowEncoder::scratch_capacity_bound_bytes();
        root.owner.complete();
        root.business.release();
        drop(window);

        let mut cursor = result.into_cursor();
        assert_eq!(cursor.retained_backing_bytes(), expected as u64);
        assert_eq!(cursor.scratch_capacity_bytes(), 0);
        let turn = cursor.step(&mut [0; 32]).unwrap();
        assert_eq!(turn.status, RenderTurnStatus::InputComplete);
        assert_eq!(turn.emitted_bytes, 0);
        assert_eq!(cursor.retained_backing_bytes(), expected as u64);
        cursor.cancel();
        assert_eq!(cursor.retained_backing_bytes(), expected as u64);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(cursor);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    #[test]
    fn foreign_same_number_scope_and_client_class_refuse_before_source() {
        let (first, _) = fixture();
        let (second, _) = fixture();
        let (root, window) = first
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let other = second
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        assert_eq!(root.owner.scope().id(), other.owner.scope().id());
        assert!(
            LocalResultProducer::try_new(
                &other.owner.scope(),
                window.retain_alias(),
                ClientPresentationFamily::MysqlText
            )
            .is_err()
        );
        let (client_root, client_window) = first
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Client,
            )
            .unwrap();
        assert!(
            LocalResultProducer::try_new(
                &client_root.owner.scope(),
                client_window.retain_alias(),
                ClientPresentationFamily::MysqlText
            )
            .is_err()
        );
        root.owner.complete();
        root.business.release();
        drop(window);
        other.owner.complete();
        other.business.release();
        client_root.owner.complete();
        client_root.business.release();
        drop(client_window);
    }
    #[test]
    fn declared_local_primitives_keep_exact_metadata_and_shared_render_bytes() {
        use super::super::{ResultField, build_arrow_query_result};
        use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
        let (host, capacity) = fixture();
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let result = LocalResultProducer::try_new(
            &root.owner.scope(),
            window.retain_alias(),
            ClientPresentationFamily::MysqlText,
        )
        .unwrap()
        .produce(|| {
            build_arrow_query_result(
                vec![
                    ResultField::new(
                        "active",
                        novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
                    ),
                    ResultField::new(
                        "small",
                        novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
                    ),
                    ResultField::new(
                        "large",
                        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                    ),
                    ResultField::new(
                        "text",
                        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
                    ),
                ],
                vec![
                    Arc::new(BooleanArray::from(vec![true])),
                    Arc::new(Int32Array::from(vec![-2])),
                    Arc::new(Int64Array::from(vec![7])),
                    Arc::new(StringArray::from(vec![None::<&str>])),
                ],
            )
        })
        .unwrap();
        assert_eq!(
            result
                .columns()
                .map(|column| column.kind)
                .collect::<Vec<_>>(),
            vec![
                LocalColumnKind::Boolean,
                LocalColumnKind::Int32,
                LocalColumnKind::Int64,
                LocalColumnKind::Utf8
            ]
        );
        root.owner.complete();
        root.business.release();
        drop(window);
        let mut cursor = result.into_cursor();
        let mut bytes = [0; 256];
        let turn = cursor.step(&mut bytes).unwrap();
        assert_eq!(turn.completed_rows, 1);
        assert_eq!(
            &bytes[..turn.emitted_bytes],
            &[8, 0, 0, 0, 1, b'1', 2, b'-', b'2', 1, b'7', 0xfb]
        );
        drop(cursor);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    #[test]
    fn trino_local_primitives_freeze_family_and_render_json_rows() {
        use super::super::{ResultField, build_arrow_query_result};
        use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
        use novarocks_result_contract::TrinoJsonFacts;
        let (host, capacity) = fixture();
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let family = ClientPresentationFamily::TrinoJson(TrinoJsonFacts {
            parametric_datetime: true,
            variant: false,
            variant_binary: false,
        });
        let result =
            LocalResultProducer::try_new(&root.owner.scope(), window.retain_alias(), family)
                .unwrap()
                .produce(|| {
                    build_arrow_query_result(
                        vec![
                            ResultField::new(
                                "active",
                                novarocks_type_contract::FunctionValueType::new(
                                    DataType::Boolean,
                                    false,
                                ),
                            ),
                            ResultField::new(
                                "small",
                                novarocks_type_contract::FunctionValueType::new(
                                    DataType::Int32,
                                    false,
                                ),
                            ),
                            ResultField::new(
                                "large",
                                novarocks_type_contract::FunctionValueType::new(
                                    DataType::Int64,
                                    false,
                                ),
                            ),
                            ResultField::new(
                                "text",
                                novarocks_type_contract::FunctionValueType::new(
                                    DataType::Utf8,
                                    true,
                                ),
                            ),
                        ],
                        vec![
                            Arc::new(BooleanArray::from(vec![true, false])),
                            Arc::new(Int32Array::from(vec![-2, i32::MIN])),
                            Arc::new(Int64Array::from(vec![i64::MAX, 7])),
                            Arc::new(StringArray::from(vec![None, Some("a\"\n")])),
                        ],
                    )
                })
                .unwrap();
        assert_eq!(result.schema.family(), &family);
        assert!(
            result
                .schema
                .columns()
                .iter()
                .all(|column| column.field.presentation == RenderPresentation::TrinoValue)
        );
        assert_eq!(
            result
                .schema
                .columns()
                .iter()
                .map(|column| &column.field.native_type)
                .collect::<Vec<_>>(),
            vec![
                &NativeRenderType::Boolean,
                &NativeRenderType::SignedInteger(32),
                &NativeRenderType::SignedInteger(64),
                &NativeRenderType::String
            ],
        );
        root.owner.complete();
        root.business.release();
        drop(window);
        let mut cursor = result.into_cursor();
        let mut body = Vec::new();
        let mut rows = 0;
        for _ in 0..256 {
            let mut output = [0; 7];
            let turn = cursor.step(&mut output).unwrap();
            assert!(turn.emitted_bytes <= output.len());
            assert!(turn.examined_bytes <= RootProfileV1::EMIT_BYTES_PER_TURN);
            rows += turn.completed_rows;
            body.extend_from_slice(&output[..turn.emitted_bytes]);
            if turn.status == RenderTurnStatus::InputComplete {
                break;
            }
        }
        let mut expected = Vec::new();
        for payload in [
            br#"[true,-2,9223372036854775807,null]"#.as_slice(),
            br#"[false,-2147483648,7,"a\"\n"]"#.as_slice(),
        ] {
            expected.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            expected.extend_from_slice(payload);
        }
        assert_eq!(rows, 2);
        assert_eq!(body, expected);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(cursor);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn empty_input_batch_yields_before_the_following_batch_without_false_end() {
        let (host, _) = fixture();
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let result = LocalResultProducer::try_new(
            &root.owner.scope(),
            window.retain_alias(),
            ClientPresentationFamily::MysqlText,
        )
        .unwrap()
        .produce(|| {
            let mut empty = super::super::build_string_query_result("value", vec![])?;
            let mut next = super::super::build_string_query_result("value", vec!["a".into()])?;
            empty.batches.append(&mut next.batches);
            Ok(empty)
        })
        .unwrap();
        let expected = result
            .result
            .batches
            .iter()
            .flat_map(|batch| batch.columns())
            .map(|array| array.get_buffer_memory_size())
            .sum::<usize>()
            + result.client_render().backing_bytes()
            + ClientRowEncoder::scratch_capacity_bound_bytes();
        root.owner.complete();
        root.business.release();
        drop(window);
        let mut cursor = result.into_cursor();
        assert_eq!(cursor.retained_backing_bytes(), expected as u64);
        let mut bytes = [0; 256];
        assert_eq!(
            cursor.step(&mut bytes).unwrap().status,
            RenderTurnStatus::Yielded
        );
        assert_eq!(cursor.retained_backing_bytes(), expected as u64);
        let turn = cursor.step(&mut bytes).unwrap();
        assert_eq!(turn.status, RenderTurnStatus::InputComplete);
        assert_eq!(&bytes[..turn.emitted_bytes], &[2, 0, 0, 0, 1, b'a']);
        assert_eq!(cursor.retained_backing_bytes(), expected as u64);
        cursor.cancel();
        assert_eq!(cursor.retained_backing_bytes(), expected as u64);
    }

    #[test]
    fn inconsistent_batch_and_unknown_local_type_fail_without_guessing() {
        use super::super::{ResultField, build_arrow_query_result};
        use arrow::array::{Int16Array, Int64Array};
        let (host, capacity) = fixture();
        for mismatch in [false, true] {
            let (root, window) = host
                .root_admission()
                .try_begin_root_with_result(
                    WorkRequest::new(WorkClass::Management),
                    ResultWindowClass::Local,
                )
                .unwrap();
            let source = LocalResultProducer::try_new(
                &root.owner.scope(),
                window.retain_alias(),
                ClientPresentationFamily::MysqlText,
            )
            .unwrap();
            let result = source.produce(|| {
                if mismatch {
                    let mut result = build_arrow_query_result(
                        vec![ResultField::new(
                            "value",
                            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                        )],
                        vec![Arc::new(Int64Array::from(vec![1]))],
                    )?;
                    result.columns = vec![ResultField::new(
                        "value",
                        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                    )];
                    Ok(result)
                } else {
                    build_arrow_query_result(
                        vec![ResultField::new(
                            "value",
                            novarocks_type_contract::FunctionValueType::new(DataType::Int16, false),
                        )],
                        vec![Arc::new(Int16Array::from(vec![1]))],
                    )
                }
            });
            assert!(result.is_err());
            root.owner.complete();
            root.business.release();
            drop(window);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        }
    }
}
