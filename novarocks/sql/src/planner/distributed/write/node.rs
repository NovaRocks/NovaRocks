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

//! Dataflow write nodes: `TableWriter` and `TableFinish`.
//!
//! NCP-6 replaces the "writer is a terminal sink" model with a Trino-style
//! dataflow. A [`TableWriterNode`] is an ordinary relational operator that
//! *emits a small relation* instead of terminating the plan:
//!
//! ```text
//! writer fragment:  child plan -> TableWriter -> stream edge (gather)
//! finish fragment:  Exchange(s) -> TableFinish -> DataSink::Result
//! ```
//!
//! Every writer emits exactly one `ROW_COUNT` row followed by zero or more
//! `COMMIT_FRAGMENT` rows, each tagged with the writer's
//! [`WriteTargetOrdinal`]. All writer fragments of one query gather into a
//! single finish fragment, whose [`TableFinishNode`] aggregates the rows and
//! emits the query result through the ordinary result sink.
//!
//! The two four-column relations themselves are **not** defined here.
//! `novarocks_spi::connector::write_stack::relation` is their single definition
//! point, because SQL, execution, backend, and frontend must all agree on them.
//! This module only attaches planner identity ([`ColumnId`]) to the SPI Arrow
//! fields; the names, types, and nullability come from SPI verbatim.

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::write_stack::{
    RootWriteResultSchema, WriteTargetOrdinal, WriterMultiplexSchema,
    validate_query_target_ordinals,
};

use crate::analysis::OutputColumn;
use crate::column_id::ColumnId;
use crate::planner::distributed::output::ConnectorWriteOutputContract;

use super::auxiliary::{WriterFinalAggregatePlan, WriterPartialAggregatePlan};
use super::contract::ConnectorWriteInputBinding;

/// The `TableWriter` output relation, as planner output columns.
///
/// Schema facts (names, types, nullability) come from
/// [`writer_output_schema`]; only the [`ColumnId`]s are planner-owned.
pub(crate) fn table_writer_output_columns(schema: &WriterMultiplexSchema) -> Vec<OutputColumn> {
    relation_output_columns(schema.arrow_schema(), &schema.slot_ids())
}

/// The `TableFinish` output relation, as planner output columns. Schema facts
/// come from [`root_output_schema`].
pub(crate) fn table_finish_output_columns() -> Vec<OutputColumn> {
    let schema = RootWriteResultSchema::new();
    relation_output_columns(&schema.arrow_schema(), &schema.slot_ids())
}

/// The stream-edge `output_slot_ids` of the write relations, in field order.
/// The reserved column ids fit `i32` by construction, so this cannot overflow
/// the wire slot-id space.
pub(crate) fn writer_multiplex_output_slot_ids(schema: &WriterMultiplexSchema) -> Vec<i32> {
    schema
        .slot_ids()
        .into_iter()
        .map(|slot_id| {
            i32::try_from(slot_id).expect("validated writer slot ids fit the wire slot id space")
        })
        .collect()
}

fn relation_output_columns(schema: &SchemaRef, slot_ids: &[u32]) -> Vec<OutputColumn> {
    debug_assert_eq!(schema.fields().len(), slot_ids.len());
    schema
        .fields()
        .iter()
        .zip(slot_ids)
        .map(|(field, slot_id)| OutputColumn {
            column_id: ColumnId(*slot_id),
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            // The write relations are engine machinery, never a user-visible
            // SQL projection.
            is_internal: true,
        })
        .collect()
}

/// A dataflow table writer.
///
/// It consumes its child's rows, hands them to the bound connector writer, and
/// emits the writer relation tagged with `write_target_ordinal`. The Arrow/SQL
/// output contract is the same [`ConnectorWriteOutputContract`] the terminal
/// write sink froze: it is pure SQL/Arrow fact and survives NCP-6 unchanged.
#[derive(Clone, Debug)]
pub struct TableWriterNode {
    pub(crate) write_target_ordinal: WriteTargetOrdinal,
    pub(crate) input: ConnectorWriteInputBinding,
    pub(crate) output_contract: ConnectorWriteOutputContract,
    pub(crate) writer_multiplex_schema: WriterMultiplexSchema,
    pub(crate) partial_aggregate_plan: WriterPartialAggregatePlan,
}

impl TableWriterNode {
    pub(crate) fn new_with_aggregate_plan(
        write_target_ordinal: WriteTargetOrdinal,
        input: ConnectorWriteInputBinding,
        output_contract: ConnectorWriteOutputContract,
        writer_multiplex_schema: WriterMultiplexSchema,
        partial_aggregate_plan: WriterPartialAggregatePlan,
    ) -> Self {
        Self {
            write_target_ordinal,
            input,
            output_contract,
            writer_multiplex_schema,
            partial_aggregate_plan,
        }
    }
}

/// The single per-query write finish operator.
///
/// It gathers the writer relation of every [`TableWriterNode`] in the query and
/// emits the root relation. `expected_target_ordinals` is the set of writer
/// ordinals it must observe; it is the plan-level record of "which logical write
/// targets *this query* feeds", not a routing table.
///
/// That set is not required to be dense from zero, and must not be: a
/// copy-on-write statement drives one query per rewritten file against a single
/// write session, and each of those queries compiles exactly one writer, at that
/// group's own ordinal. Query `k` therefore expects `[k]`. Denseness is a
/// property of the *session's* sealed target set and is enforced there, by
/// `ConnectorWriteSessionPlan::try_new`.
#[derive(Clone, Debug)]
pub struct TableFinishNode {
    pub(crate) expected_target_ordinals: Vec<WriteTargetOrdinal>,
    pub(crate) writer_multiplex_schema: WriterMultiplexSchema,
    pub(crate) root_result_schema: RootWriteResultSchema,
    pub(crate) final_aggregate_plan: WriterFinalAggregatePlan,
}

impl TableFinishNode {
    /// Build a finish node over one query's expected ordinal set: non-empty,
    /// duplicate-free, and inside the frozen target bound. Cardinality and
    /// duplication are checked by the SPI owner of the ordinal vocabulary and
    /// not restated here; the ascending listing below is this encoding's own
    /// determinism rule.
    pub(crate) fn try_new_with_aggregate_plan(
        expected_target_ordinals: Vec<WriteTargetOrdinal>,
        writer_multiplex_schema: WriterMultiplexSchema,
        root_result_schema: RootWriteResultSchema,
        final_aggregate_plan: WriterFinalAggregatePlan,
    ) -> Result<Self, String> {
        validate_query_target_ordinals(&expected_target_ordinals)
            .map_err(|error| format!("table finish write target ordinals rejected: {error}"))?;
        for pair in expected_target_ordinals.windows(2) {
            if pair[0].get() >= pair[1].get() {
                return Err(format!(
                    "table finish write target ordinals must be listed in strictly ascending order: found {} before {}",
                    pair[0].get(),
                    pair[1].get()
                ));
            }
        }
        Ok(Self {
            expected_target_ordinals,
            writer_multiplex_schema,
            root_result_schema,
            final_aggregate_plan,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    fn finish(ordinals: Vec<WriteTargetOrdinal>) -> Result<TableFinishNode, String> {
        TableFinishNode::try_new_with_aggregate_plan(
            ordinals,
            WriterMultiplexSchema::empty(),
            RootWriteResultSchema::new(),
            WriterFinalAggregatePlan::empty(),
        )
    }

    #[test]
    fn planner_output_columns_mirror_the_spi_write_relations() {
        let writer_schema = WriterMultiplexSchema::empty();
        let writer = table_writer_output_columns(&writer_schema);
        let finish = table_finish_output_columns();

        assert_eq!(writer.len(), writer_schema.arrow_schema().fields().len());
        assert_eq!(finish.len(), RootWriteResultSchema::new().slot_ids().len());

        for (index, (column, field)) in writer
            .iter()
            .zip(writer_schema.arrow_schema().fields())
            .enumerate()
        {
            assert_eq!(column.name, *field.name());
            assert_eq!(column.data_type, *field.data_type());
            assert_eq!(column.nullable, field.is_nullable());
            assert_eq!(column.column_id.0, writer_schema.slot_ids()[index]);
            assert!(column.is_internal);
        }
        for (index, (column, field)) in finish
            .iter()
            .zip(RootWriteResultSchema::new().arrow_schema().fields())
            .enumerate()
        {
            assert_eq!(column.name, *field.name());
            assert_eq!(column.data_type, *field.data_type());
            assert_eq!(column.nullable, field.is_nullable());
            assert_eq!(
                column.column_id.0,
                RootWriteResultSchema::new().slot_ids()[index]
            );
        }

        // Only the writer-ordinal nullability differs between the two relations.
        assert_eq!(
            writer
                .iter()
                .map(|column| column.nullable)
                .collect::<Vec<_>>(),
            vec![false, false, true, true]
        );
        assert_eq!(
            finish
                .iter()
                .map(|column| column.nullable)
                .collect::<Vec<_>>(),
            vec![false, true, true, true, true, true, true, true]
        );
        // Signed primitives: the FE/BE native `TypeDesc` mapping names only
        // `Int8..Int64`, so an unsigned relation column could not be encoded.
        assert_eq!(
            writer
                .iter()
                .map(|column| column.data_type.clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::Int8,
                DataType::Int32,
                DataType::Int64,
                DataType::Binary
            ]
        );
    }

    #[test]
    fn write_relation_column_ids_fit_the_wire_slot_id_space() {
        let slots = writer_multiplex_output_slot_ids(&WriterMultiplexSchema::empty());
        assert_eq!(slots.len(), 4);
        assert!(slots.iter().all(|slot| *slot > 0));
        assert_eq!(slots.last().copied(), Some(i32::MAX));
    }

    #[test]
    fn planner_uses_the_frozen_auxiliary_slot_instead_of_prefix_arithmetic() {
        let schema = WriterMultiplexSchema::try_new(vec![
            novarocks_spi::connector::write_stack::WriterAuxiliaryChannel::try_new(
                7,
                "partial",
                DataType::Binary,
            )
            .expect("channel"),
        ])
        .expect("schema");
        let columns = table_writer_output_columns(&schema);
        assert_eq!(columns.last().unwrap().column_id, ColumnId(7));
        assert_eq!(writer_multiplex_output_slot_ids(&schema).last(), Some(&7));
        assert_eq!(
            table_finish_output_columns()
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            RootWriteResultSchema::new().slot_ids()
        );
    }

    #[test]
    fn table_finish_rejects_an_empty_repeated_or_unordered_write_target_set() {
        let ordinal = |value: u32| WriteTargetOrdinal::try_new(value).expect("bounded ordinal");
        assert!(finish(vec![ordinal(0), ordinal(1), ordinal(2)]).is_ok());

        let error = finish(Vec::new()).expect_err("empty ordinals");
        assert!(error.contains("rejected"), "unexpected error: {error}");

        let error = finish(vec![ordinal(1), ordinal(1)]).expect_err("repeated ordinal");
        assert!(error.contains("rejected"), "unexpected error: {error}");

        let error = finish(vec![ordinal(1), ordinal(0)]).expect_err("descending ordinals");
        assert!(
            error.contains("strictly ascending order"),
            "unexpected error: {error}"
        );
    }

    /// A copy-on-write statement runs one query per rewritten file against one
    /// write session, and query `k` compiles exactly one writer -- the one at
    /// ordinal `k`. Its finish node therefore sees `[k]`, which is correctly not
    /// dense from zero.
    #[test]
    fn table_finish_accepts_a_single_writer_query_at_a_non_zero_ordinal() {
        let ordinal = |value: u32| WriteTargetOrdinal::try_new(value).expect("bounded ordinal");
        let node = finish(vec![ordinal(2)]).expect("single non-zero target");
        assert_eq!(node.expected_target_ordinals, vec![ordinal(2)]);
        // A gap between two targets is the same kind of fact.
        assert!(finish(vec![ordinal(0), ordinal(2)]).is_ok());
    }

    #[test]
    fn table_finish_preserves_the_plan_frozen_typed_writer_tail() {
        let ordinal = |value: u32| WriteTargetOrdinal::try_new(value).expect("bounded ordinal");
        let writer_schema = WriterMultiplexSchema::try_new(vec![
            novarocks_spi::connector::write_stack::WriterAuxiliaryChannel::try_new(
                7,
                "generic_aux",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    arrow::datatypes::Field::new("value", DataType::Utf8, true),
                ])),
            )
            .expect("channel"),
        ])
        .expect("writer schema");
        let finish = TableFinishNode::try_new_with_aggregate_plan(
            vec![ordinal(0)],
            writer_schema.clone(),
            RootWriteResultSchema::new(),
            WriterFinalAggregatePlan::empty(),
        )
        .expect("finish");
        assert_eq!(finish.writer_multiplex_schema, writer_schema);
        assert_eq!(finish.root_result_schema, RootWriteResultSchema::new());
    }
}
