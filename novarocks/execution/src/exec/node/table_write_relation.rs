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

//! The execution-side binding of the two write row relations, plus the two
//! codec ports the write data plane needs.
//!
//! The relations themselves — their columns, their kind codes, and their
//! kind-versus-nullability invariants — are defined once in
//! `novarocks_spi::connector::write_stack::relation`, because the SQL planner,
//! the execution engine, the backend, and the frontend must all agree on them.
//! This module adds only what is execution-local: the `SlotId` each column
//! carries inside a [`Chunk`], and the two narrow ports below.
//!
//! `novarocks-execution` depends only on `novarocks-types` and `novarocks-spi`,
//! and its dependency closure is forbidden from ever reaching the generated
//! wire crates, so this layer structurally cannot encode or decode a commit
//! fragment. Canonical bytes enter and leave through the ports: execution moves
//! opaque buffers and counts them, and never interprets one.
//!
//! [`Chunk`]: crate::exec::chunk::Chunk

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ROOT_WRITE_RESULT_COLUMN_COUNT, RootWriteResultSchema,
    WRITE_RELATION_FRAGMENT_INDEX, WRITE_RELATION_KIND_INDEX, WRITE_RELATION_ROW_COUNT_INDEX,
    WRITE_RELATION_TARGET_INDEX, WriteTargetOrdinal, WriterMultiplexSchema,
    write_relation_column_id,
};
use novarocks_types::SlotId;

use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};

/// Slot id of the `kind` column in both write relations.
///
/// These are the ids the write contract reserves, not a fresh tuple layout:
/// the exchange edge from a writer fragment to the root carries this relation
/// and declares those ids as its `output_slot_ids`, so a chunk numbered any
/// other way would not line up with the plan that routes it.
pub const WRITE_RELATION_KIND_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_KIND_INDEX));
/// Slot id of the `write_target_ordinal` column in both write relations.
pub const WRITE_RELATION_TARGET_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_TARGET_INDEX));
/// Slot id of the `row_count` column in both write relations.
pub const WRITE_RELATION_ROW_COUNT_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_ROW_COUNT_INDEX));
/// Slot id of the `commit_fragment` column in both write relations.
pub const WRITE_RELATION_FRAGMENT_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_FRAGMENT_INDEX));

const _: () = {
    assert!(WRITE_RELATION_KIND_INDEX == 0);
    assert!(WRITE_RELATION_TARGET_INDEX == 1);
    assert!(WRITE_RELATION_ROW_COUNT_INDEX == 2);
    assert!(WRITE_RELATION_FRAGMENT_INDEX == 3);
};

/// Execution-local slot binding of a plan-frozen writer multiplex schema.
/// Auxiliary fields remain generic Arrow slots.
#[derive(Clone, Debug)]
pub struct WriterMultiplexRelationSchema {
    contract: WriterMultiplexSchema,
    chunk_schema: ChunkSchemaRef,
}

impl WriterMultiplexRelationSchema {
    pub fn try_new(contract: WriterMultiplexSchema) -> Result<Self, String> {
        let slot_ids = contract
            .slot_ids()
            .into_iter()
            .map(SlotId::new)
            .collect::<Vec<_>>();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            contract.arrow_schema().as_ref(),
            &slot_ids,
        )
        .map_err(|error| format!("writer multiplex chunk schema: {error}"))?;
        Ok(Self {
            contract,
            chunk_schema,
        })
    }

    pub fn empty() -> Self {
        Self::try_new(WriterMultiplexSchema::empty()).expect("fixed writer prefix")
    }

    pub const fn contract(&self) -> &WriterMultiplexSchema {
        &self.contract
    }

    pub const fn chunk_schema(&self) -> &ChunkSchemaRef {
        &self.chunk_schema
    }
}

/// Execution-local slot binding of the fixed Root write result relation.
#[derive(Clone, Debug)]
pub struct RootWriteResultRelationSchema {
    contract: RootWriteResultSchema,
    chunk_schema: ChunkSchemaRef,
}

impl RootWriteResultRelationSchema {
    pub fn try_new(contract: RootWriteResultSchema) -> Result<Self, String> {
        let slot_ids = contract.slot_ids().map(SlotId::new);
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            contract.arrow_schema().as_ref(),
            &slot_ids,
        )
        .map_err(|error| format!("root write result chunk schema: {error}"))?;
        debug_assert_eq!(slot_ids.len(), ROOT_WRITE_RESULT_COLUMN_COUNT);
        Ok(Self {
            contract,
            chunk_schema,
        })
    }

    pub fn fixed() -> Self {
        Self::try_new(RootWriteResultSchema::new()).expect("fixed root write result")
    }

    pub const fn contract(&self) -> &RootWriteResultSchema {
        &self.contract
    }

    pub const fn chunk_schema(&self) -> &ChunkSchemaRef {
        &self.chunk_schema
    }
}

/// Canonical commit-fragment egress port.
///
/// `TableWriter` hands one finished provider fragment to its owner and receives
/// the canonical carrier bytes back. The execution layer never performs that
/// encoding itself: it cannot reach the generated wire crates, so the backend
/// installs the implementation for the exact catalog generation this query
/// froze.
pub trait ConnectorCommitFragmentEncoder: Send + Sync {
    /// Encode one commit fragment produced for `target` into its canonical
    /// carrier bytes.
    fn encode(
        &self,
        target: WriteTargetOrdinal,
        fragment: &ConnectorCommitFragment,
    ) -> Result<Vec<u8>, ConnectorError>;
}

/// Canonical commit-fragment ingress port.
///
/// `TableFinish` receives opaque canonical bytes from many senders. It must
/// reject a foreign, truncated, or non-canonical carrier before it enters the
/// prepared write set, but it must not decode one into a provider domain
/// object: that happens only in the frontend, on the provider's own control
/// binding. The implementation therefore performs a structural check and
/// nothing more.
pub trait ConnectorCommitFragmentCarrierValidator: Send + Sync {
    /// Structurally verify that `encoded` is a canonical, in-bounds commit
    /// fragment carrier of the provider expected for `target`, without
    /// decoding it into a provider value.
    fn validate(&self, target: WriteTargetOrdinal, encoded: &[u8]) -> Result<(), ConnectorError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_binds_the_spi_relations_without_redefining_them() {
        let writer = WriterMultiplexRelationSchema::empty();
        assert_eq!(
            writer.contract().arrow_schema().as_ref(),
            novarocks_spi::connector::write_stack::writer_output_schema().as_ref()
        );
        let root = RootWriteResultRelationSchema::fixed();
        assert_eq!(
            root.contract().arrow_schema().as_ref(),
            RootWriteResultSchema::new().arrow_schema().as_ref()
        );
    }

    #[test]
    fn every_relation_column_carries_its_slot_id_at_the_spi_column_index() {
        let writer = WriterMultiplexRelationSchema::empty();
        for (index, slot_id) in writer.contract().slot_ids().into_iter().enumerate() {
            assert_eq!(
                writer.chunk_schema().index_of(SlotId::new(slot_id)),
                Some(index)
            );
        }
        let root = RootWriteResultRelationSchema::fixed();
        for (index, slot_id) in root.contract().slot_ids().into_iter().enumerate() {
            assert_eq!(
                root.chunk_schema().index_of(SlotId::new(slot_id)),
                Some(index)
            );
        }
    }

    #[test]
    fn typed_relation_carriers_bind_every_generic_slot() {
        let writer = WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(vec![
                novarocks_spi::connector::write_stack::WriterAuxiliaryChannel::try_new(
                    7,
                    "opaque_aux",
                    arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from(vec![
                        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Utf8, true),
                    ])),
                )
                .expect("channel"),
            ])
            .expect("contract"),
        )
        .expect("execution schema");
        assert_eq!(writer.chunk_schema().slots().len(), 5);
        assert_eq!(writer.chunk_schema().index_of(SlotId::new(7)), Some(4));

        let root = RootWriteResultRelationSchema::fixed();
        assert_eq!(
            root.chunk_schema().slots().len(),
            ROOT_WRITE_RESULT_COLUMN_COUNT
        );
    }
}
