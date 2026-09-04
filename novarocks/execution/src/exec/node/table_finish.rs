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

//! The `TableFinish` plan node.
//!
//! `TableFinish` is a single-driver processor on one Root BE. It consumes every
//! writer row, validates row shape and budgets, checked-sums the row count,
//! collects canonical commit fragments by target ordinal, and emits its
//! complete relation into the ordinary RESULT sink only after all of its inputs
//! reached EOS. It performs bounded aggregation and nothing else: it holds no
//! commit handle, decodes no fragment, and never touches external catalog
//! metadata.
//!
//! The node is n-ary. An exchange receiver names exactly one source fragment,
//! so a query with several writer fragments gives the finish node one receiver
//! per writer rather than one shared receiver. The pipeline converges them the
//! way `UnionAll` does, straight onto a single-driver pipeline.

use std::sync::Arc;

use novarocks_spi::connector::write_stack::{WriteTargetOrdinal, validate_query_target_ordinals};

use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::node::ExecNode;
use crate::exec::node::table_write_aggregate::WriterFinalAggregatePlan;
use crate::exec::node::table_write_relation::ConnectorCommitFragmentCarrierValidator;
use crate::exec::node::table_write_relation::{
    RootWriteResultRelationSchema, WriterMultiplexRelationSchema,
};

/// The bounded aggregation stage of one distributed write.
#[derive(Clone)]
pub struct TableFinishNode {
    pub inputs: Vec<ExecNode>,
    pub node_id: i32,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    writer_multiplex_schema: WriterMultiplexRelationSchema,
    root_result_schema: RootWriteResultRelationSchema,
    final_aggregate_plan: WriterFinalAggregatePlan,
}

impl TableFinishNode {
    pub fn try_new(
        inputs: Vec<ExecNode>,
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    ) -> Result<Self, ExecPlanBuildError> {
        Self::try_new_with_relations(
            inputs,
            node_id,
            expected_targets,
            fragment_validator,
            WriterMultiplexRelationSchema::empty(),
            RootWriteResultRelationSchema::fixed(),
            WriterFinalAggregatePlan::default(),
        )
    }

    pub fn try_new_with_relations(
        inputs: Vec<ExecNode>,
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
        writer_multiplex_schema: WriterMultiplexRelationSchema,
        root_result_schema: RootWriteResultRelationSchema,
        final_aggregate_plan: WriterFinalAggregatePlan,
    ) -> Result<Self, ExecPlanBuildError> {
        if inputs.is_empty() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                "table finish requires at least one writer input".to_string(),
            ));
        }
        // The expected set is the targets *this query's* writers feed, so it is
        // validated as a query set: non-empty, inside the frozen bound, and
        // free of duplicates. It is deliberately not required to be dense from
        // zero -- a copy-on-write statement compiles one writer per query, at
        // that group's own ordinal, so query `k` legitimately expects `[k]`.
        // Denseness stays a property of the session's sealed set, where
        // `ConnectorWriteSessionPlan::try_new` enforces it.
        validate_query_target_ordinals(&expected_targets).map_err(|error| {
            ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                format!("table finish expected write targets: {error}"),
            )
        })?;
        Ok(Self {
            inputs,
            node_id,
            expected_targets: Arc::new(expected_targets),
            fragment_validator,
            writer_multiplex_schema,
            root_result_schema,
            final_aggregate_plan,
        })
    }

    pub fn expected_targets(&self) -> &Arc<Vec<WriteTargetOrdinal>> {
        &self.expected_targets
    }

    /// Whether `ordinal` is one of the targets this query's writers feed.
    ///
    /// This is an exact set test rather than a comparison against the highest
    /// ordinal: a query set need not be dense from zero, so "at or below the
    /// highest" would admit a target this query never compiled a writer for.
    pub fn accepts_target(&self, ordinal: WriteTargetOrdinal) -> bool {
        self.expected_targets.contains(&ordinal)
    }

    pub const fn fragment_validator(&self) -> &Arc<dyn ConnectorCommitFragmentCarrierValidator> {
        &self.fragment_validator
    }

    pub const fn writer_multiplex_schema(&self) -> &WriterMultiplexRelationSchema {
        &self.writer_multiplex_schema
    }

    pub const fn root_result_schema(&self) -> &RootWriteResultRelationSchema {
        &self.root_result_schema
    }

    pub const fn final_aggregate_plan(&self) -> &WriterFinalAggregatePlan {
        &self.final_aggregate_plan
    }
}

impl std::fmt::Debug for TableFinishNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TableFinishNode")
            .field("node_id", &self.node_id)
            .field("inputs", &self.inputs.len())
            .field("expected_targets", &self.expected_targets)
            .finish_non_exhaustive()
    }
}
