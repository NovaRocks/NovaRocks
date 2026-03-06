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
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanOp};
use crate::runtime::backend_id;

use super::be_compaction_stats_store;
use super::be_tablet_write_log_store;
use super::be_txn_store;
use super::chunk_builder::{SchemaRow, SchemaValue, build_chunk, normalize_column_key};
use super::{BeSchemaTable, SchemaScanContext};

const DEFAULT_CHUNK_ROWS: usize = 4096;

#[derive(Clone)]
pub(crate) struct SchemaScanOp {
    table: BeSchemaTable,
    context: SchemaScanContext,
    output_schema: SchemaRef,
    should_scan: bool,
}

impl SchemaScanOp {
    pub(crate) fn new(
        table: BeSchemaTable,
        context: SchemaScanContext,
        output_schema: SchemaRef,
        should_scan: bool,
    ) -> Self {
        Self {
            table,
            context,
            output_schema,
            should_scan,
        }
    }

    fn collect_rows(&self) -> Vec<SchemaRow> {
        let mut rows = match self.table {
            BeSchemaTable::TabletWriteLog => be_tablet_write_log_store::snapshot(&self.context)
                .into_iter()
                .map(|entry| {
                    let mut row = SchemaRow::new();
                    row.insert(
                        normalize_column_key("BE_ID"),
                        SchemaValue::Int64(entry.backend_id),
                    );
                    row.insert(
                        normalize_column_key("BEGIN_TIME"),
                        SchemaValue::TimestampMicrosecond(entry.begin_time_ms.saturating_mul(1000)),
                    );
                    row.insert(
                        normalize_column_key("FINISH_TIME"),
                        SchemaValue::TimestampMicrosecond(
                            entry.finish_time_ms.saturating_mul(1000),
                        ),
                    );
                    row.insert(
                        normalize_column_key("TXN_ID"),
                        SchemaValue::Int64(entry.txn_id),
                    );
                    row.insert(
                        normalize_column_key("TABLET_ID"),
                        SchemaValue::Int64(entry.tablet_id),
                    );
                    row.insert(
                        normalize_column_key("TABLE_ID"),
                        SchemaValue::Int64(entry.table_id),
                    );
                    row.insert(
                        normalize_column_key("PARTITION_ID"),
                        SchemaValue::Int64(entry.partition_id),
                    );
                    row.insert(
                        normalize_column_key("LOG_TYPE"),
                        SchemaValue::Utf8(entry.log_type.as_str().to_string()),
                    );
                    row.insert(
                        normalize_column_key("INPUT_ROWS"),
                        SchemaValue::Int64(entry.input_rows),
                    );
                    row.insert(
                        normalize_column_key("INPUT_BYTES"),
                        SchemaValue::Int64(entry.input_bytes),
                    );
                    row.insert(
                        normalize_column_key("OUTPUT_ROWS"),
                        SchemaValue::Int64(entry.output_rows),
                    );
                    row.insert(
                        normalize_column_key("OUTPUT_BYTES"),
                        SchemaValue::Int64(entry.output_bytes),
                    );
                    if let Some(input_segments) = entry.input_segments {
                        row.insert(
                            normalize_column_key("INPUT_SEGMENTS"),
                            SchemaValue::Int32(input_segments),
                        );
                    }
                    row.insert(
                        normalize_column_key("OUTPUT_SEGMENTS"),
                        SchemaValue::Int32(entry.output_segments),
                    );
                    if let Some(label) = entry.label {
                        row.insert(normalize_column_key("LABEL"), SchemaValue::Utf8(label));
                    }
                    if let Some(compaction_score) = entry.compaction_score {
                        row.insert(
                            normalize_column_key("COMPACTION_SCORE"),
                            SchemaValue::Int64(compaction_score),
                        );
                    }
                    if let Some(compaction_type) = entry.compaction_type {
                        row.insert(
                            normalize_column_key("COMPACTION_TYPE"),
                            SchemaValue::Utf8(compaction_type),
                        );
                    }
                    row
                })
                .collect(),
            BeSchemaTable::Txns => be_txn_store::snapshot(&self.context)
                .into_iter()
                .map(|entry| {
                    let mut row = SchemaRow::new();
                    row.insert(
                        normalize_column_key("BE_ID"),
                        SchemaValue::Int64(entry.backend_id),
                    );
                    row.insert(
                        normalize_column_key("LOAD_ID"),
                        SchemaValue::Utf8(entry.load_id),
                    );
                    row.insert(
                        normalize_column_key("TXN_ID"),
                        SchemaValue::Int64(entry.txn_id),
                    );
                    row.insert(
                        normalize_column_key("PARTITION_ID"),
                        SchemaValue::Int64(entry.partition_id),
                    );
                    row.insert(
                        normalize_column_key("TABLET_ID"),
                        SchemaValue::Int64(entry.tablet_id),
                    );
                    row.insert(
                        normalize_column_key("CREATE_TIME"),
                        SchemaValue::Int64(entry.create_time),
                    );
                    row.insert(
                        normalize_column_key("COMMIT_TIME"),
                        SchemaValue::Int64(entry.commit_time),
                    );
                    row.insert(
                        normalize_column_key("PUBLISH_TIME"),
                        SchemaValue::Int64(entry.publish_time),
                    );
                    row.insert(
                        normalize_column_key("ROWSET_ID"),
                        SchemaValue::Utf8(entry.rowset_id),
                    );
                    row.insert(
                        normalize_column_key("NUM_SEGMENT"),
                        SchemaValue::Int64(entry.num_segment),
                    );
                    row.insert(
                        normalize_column_key("NUM_DELFILE"),
                        SchemaValue::Int64(entry.num_delfile),
                    );
                    row.insert(
                        normalize_column_key("NUM_ROW"),
                        SchemaValue::Int64(entry.num_row),
                    );
                    row.insert(
                        normalize_column_key("DATA_SIZE"),
                        SchemaValue::Int64(entry.data_size),
                    );
                    row.insert(
                        normalize_column_key("VERSION"),
                        SchemaValue::Int64(entry.version),
                    );
                    row
                })
                .collect(),
            BeSchemaTable::Compactions => {
                let be_id = backend_id::backend_id().unwrap_or(-1);
                let stats = be_compaction_stats_store::snapshot();
                let mut row = SchemaRow::new();
                row.insert(normalize_column_key("BE_ID"), SchemaValue::Int64(be_id));
                row.insert(
                    normalize_column_key("CANDIDATES_NUM"),
                    SchemaValue::Int64(stats.candidates_num),
                );
                row.insert(
                    normalize_column_key("BASE_COMPACTION_CONCURRENCY"),
                    SchemaValue::Int64(stats.base_compaction_concurrency),
                );
                row.insert(
                    normalize_column_key("CUMULATIVE_COMPACTION_CONCURRENCY"),
                    SchemaValue::Int64(stats.cumulative_compaction_concurrency),
                );
                row.insert(
                    normalize_column_key("LATEST_COMPACTION_SCORE"),
                    SchemaValue::Float64(stats.latest_compaction_score),
                );
                row.insert(
                    normalize_column_key("CANDIDATE_MAX_SCORE"),
                    SchemaValue::Float64(stats.candidate_max_score),
                );
                row.insert(
                    normalize_column_key("MANUAL_COMPACTION_CONCURRENCY"),
                    SchemaValue::Int64(stats.manual_compaction_concurrency),
                );
                row.insert(
                    normalize_column_key("MANUAL_COMPACTION_CANDIDATES_NUM"),
                    SchemaValue::Int64(stats.manual_compaction_candidates_num),
                );
                vec![row]
            }
            BeSchemaTable::Unsupported(_) => Vec::new(),
        };

        if let Some(limit) = self.context.limit_as_usize()
            && rows.len() > limit
        {
            rows.truncate(limit);
        }
        rows
    }
}

impl ScanOp for SchemaScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        _profile: Option<crate::runtime::profile::RuntimeProfile>,
        _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        match morsel {
            ScanMorsel::Schema { .. } => {}
            _ => return Err("schema scan received unexpected morsel".to_string()),
        }
        if !self.should_scan {
            return Ok(Box::new(std::iter::empty()));
        }

        let rows = self.collect_rows();
        if rows.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let mut chunks = Vec::new();
        for batch_rows in rows.chunks(DEFAULT_CHUNK_ROWS.max(1)) {
            let chunk = build_chunk(Arc::clone(&self.output_schema), batch_rows)?;
            chunks.push(chunk);
        }

        Ok(Box::new(chunks.into_iter().map(Ok)))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        Ok(ScanMorsels::new(
            vec![ScanMorsel::Schema {
                table_name: self.table.table_name().to_string(),
            }],
            false,
        ))
    }

    fn profile_name(&self) -> Option<String> {
        Some(format!("SCHEMA_SCAN (table={})", self.table.table_name()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;

    fn ctx(table_name: &str) -> SchemaScanContext {
        SchemaScanContext {
            table_name: table_name.to_string(),
            db: None,
            table: None,
            table_id: None,
            partition_id: None,
            tablet_id: None,
            txn_id: None,
            type_: None,
            state: None,
            limit: None,
            log_start_ts: None,
            log_end_ts: None,
            log_level: None,
            log_pattern: None,
            log_limit: None,
        }
    }

    #[test]
    fn schema_scan_op_respects_slot_projection_order_and_nullable_columns() {
        let _guard = crate::connector::schema::test_lock()
            .lock()
            .expect("schema scan test lock");
        super::be_tablet_write_log_store::clear_for_test();
        super::be_tablet_write_log_store::set_options_for_test(true, 16);
        super::be_tablet_write_log_store::record_load(
            crate::connector::schema::BeTabletWriteLoadLogRecord {
                backend_id: 1,
                begin_time_ms: 1000,
                finish_time_ms: 2000,
                txn_id: 888,
                tablet_id: 9,
                table_id: 7,
                partition_id: 8,
                input_rows: 10,
                input_bytes: 20,
                output_rows: 10,
                output_bytes: 20,
                output_segments: 1,
                label: None,
            },
        );

        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("LABEL", DataType::Utf8, true), SlotId::new(2)),
            field_with_slot_id(Field::new("TXN_ID", DataType::Int64, false), SlotId::new(1)),
        ]));
        let op = SchemaScanOp::new(
            BeSchemaTable::TabletWriteLog,
            ctx("be_tablet_write_log"),
            schema,
            true,
        );
        let mut iter = op
            .execute_iter(
                ScanMorsel::Schema {
                    table_name: "be_tablet_write_log".to_string(),
                },
                None,
                None,
            )
            .expect("schema scan execute");
        let chunk = iter.next().expect("one chunk").expect("chunk should be ok");
        assert_eq!(
            chunk
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>(),
            vec!["LABEL".to_string(), "TXN_ID".to_string()]
        );
        let label_col = chunk.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label as utf8");
        assert!(label_col.is_null(0));
        let txn_col = chunk.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("txn as int64");
        assert_eq!(txn_col.value(0), 888);

        super::be_tablet_write_log_store::clear_for_test();
    }
}
