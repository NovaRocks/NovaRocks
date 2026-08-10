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
//! Materialized build artifact for hash-join probing.
//!
//! Responsibilities:
//! - Packages hash tables, row references, and build-side schema artifacts for probe operators.
//! - Separates build-time materialization from probe-time read access semantics.
//!
//! Key exported interfaces:
//! - Types: `JoinBuildArtifact`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use super::build_requirements::{
    BuildComponentRequirements, NullKeyRequirement, RowPayloadRequirement,
};
use super::join_hash_map::build_store::BuildStore;
use super::join_hash_map::method::JoinHashMap;
use crate::exec::chunk::Chunk;

#[derive(Clone)]
/// Materialized build-side artifact consumed by join probe operators.
pub(crate) struct JoinBuildArtifact {
    pub(crate) provided: BuildComponentRequirements,
    pub(crate) build_store: Option<Arc<BuildStore>>,
    pub(crate) build_table: Option<Arc<JoinHashMap>>,
    pub(crate) build_row_count: usize,
    pub(crate) build_has_null_key: bool,
    pub(crate) build_null_key_rows: Option<Arc<Vec<u32>>>,
}

#[derive(Clone)]
pub(crate) struct JoinBuildRuntimeFilterView;

impl JoinBuildArtifact {
    pub(crate) fn new_native(
        provided: BuildComponentRequirements,
        build_store: Option<BuildStore>,
        build_table: Option<JoinHashMap>,
        build_row_count: usize,
        build_has_null_key: bool,
        build_null_key_rows: Option<Arc<Vec<u32>>>,
    ) -> Self {
        Self {
            provided,
            build_store: build_store.map(Arc::new),
            build_table: build_table.map(Arc::new),
            build_row_count,
            build_has_null_key,
            build_null_key_rows,
        }
    }

    pub(crate) fn validate_components(
        &self,
        required: BuildComponentRequirements,
    ) -> Result<(), String> {
        if self.provided != required {
            return Err(format!(
                "hash join build artifact invalid: component contract mismatch; required={required:?} provided={:?}",
                self.provided
            ));
        }
        if required.row_payload == RowPayloadRequirement::Required
            && self.build_row_count > 0
            && self.build_store.is_none()
        {
            return Err(format!(
                "hash join build artifact invalid: row payload required but missing; required={required:?} provided={:?}",
                self.provided
            ));
        }
        if required.requires_lookup_table()
            && self.build_table.is_none()
            && self.build_row_count > 0
        {
            return Err(format!(
                "hash join build artifact invalid: lookup table required but missing; required={required:?} provided={:?}",
                self.provided
            ));
        }
        if required.null_keys == NullKeyRequirement::NullKeyRows
            && self.build_row_count > 0
            && self.build_null_key_rows.is_none()
        {
            return Err(format!(
                "hash join build artifact invalid: null-key rows required but missing; required={required:?} provided={:?}",
                self.provided
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct BuildView {
    artifact: Arc<JoinBuildArtifact>,
    required: BuildComponentRequirements,
}

impl BuildView {
    pub(crate) fn new(
        artifact: Arc<JoinBuildArtifact>,
        required: BuildComponentRequirements,
    ) -> Result<Self, String> {
        artifact.validate_components(required)?;
        Ok(Self { artifact, required })
    }

    pub(crate) fn build_table(&self) -> Option<Arc<JoinHashMap>> {
        self.artifact.build_table.clone()
    }

    pub(crate) fn build_chunk(&self, context: &'static str) -> Result<Arc<Chunk>, String> {
        let store = self.artifact.build_store.as_ref().ok_or_else(|| {
            format!(
                "hash join row payload required for {context} but missing; required={:?} provided={:?}",
                self.required, self.artifact.provided
            )
        })?;
        Ok(store.chunk())
    }

    pub(crate) fn optional_build_chunk(&self) -> Option<Arc<Chunk>> {
        self.artifact
            .build_store
            .as_ref()
            .map(|store| store.chunk())
    }

    pub(crate) fn build_row_count(&self) -> usize {
        self.artifact.build_row_count
    }

    pub(crate) fn build_has_null_key(&self) -> bool {
        self.artifact.build_has_null_key
    }

    pub(crate) fn build_null_key_rows(&self) -> Option<Arc<Vec<u32>>> {
        self.artifact.build_null_key_rows.clone()
    }

    pub(crate) fn runtime_filter_view(&self) -> JoinBuildRuntimeFilterView {
        JoinBuildRuntimeFilterView
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    use crate::exec::operators::hashjoin::build_requirements::{
        BuildComponentRequirements, LookupRequirement, MatchFlagRequirement, NullKeyRequirement,
        RowPayloadRequirement,
    };

    fn lookup_table() -> JoinHashMap {
        JoinHashMap::new_chained(vec![DataType::Int32], vec![false]).expect("lookup table")
    }

    fn membership_only_requirements() -> BuildComponentRequirements {
        BuildComponentRequirements {
            lookup: LookupRequirement::Membership,
            row_payload: RowPayloadRequirement::NotNeeded,
            match_flags: MatchFlagRequirement::NotNeeded,
            null_keys: NullKeyRequirement::NotNeeded,
        }
    }

    fn row_payload_requirements() -> BuildComponentRequirements {
        BuildComponentRequirements {
            lookup: LookupRequirement::RowMatches,
            row_payload: RowPayloadRequirement::Required,
            match_flags: MatchFlagRequirement::NotNeeded,
            null_keys: NullKeyRequirement::NotNeeded,
        }
    }

    fn no_lookup_requirements() -> BuildComponentRequirements {
        BuildComponentRequirements {
            lookup: LookupRequirement::NotNeeded,
            row_payload: RowPayloadRequirement::NotNeeded,
            match_flags: MatchFlagRequirement::NotNeeded,
            null_keys: NullKeyRequirement::NotNeeded,
        }
    }

    fn null_key_rows_requirements() -> BuildComponentRequirements {
        BuildComponentRequirements {
            lookup: LookupRequirement::NotNeeded,
            row_payload: RowPayloadRequirement::NotNeeded,
            match_flags: MatchFlagRequirement::NotNeeded,
            null_keys: NullKeyRequirement::NullKeyRows,
        }
    }

    #[test]
    fn membership_only_artifact_allows_missing_row_payload_when_lookup_table_present() {
        let artifact = JoinBuildArtifact::new_native(
            membership_only_requirements(),
            None,
            Some(lookup_table()),
            3,
            false,
            None,
        );
        assert!(
            artifact
                .validate_components(membership_only_requirements())
                .is_ok()
        );
    }

    #[test]
    fn membership_lookup_rejects_missing_table_when_nonempty() {
        let artifact = JoinBuildArtifact::new_native(
            membership_only_requirements(),
            None,
            None,
            3,
            false,
            None,
        );
        let err = artifact
            .validate_components(membership_only_requirements())
            .expect_err("nonempty membership lookup requires table");
        assert!(err.contains("lookup table required"));
    }

    #[test]
    fn row_payload_requirement_rejects_missing_build_store() {
        let artifact =
            JoinBuildArtifact::new_native(row_payload_requirements(), None, None, 3, false, None);
        let err = artifact
            .validate_components(row_payload_requirements())
            .expect_err("missing row payload must fail");
        assert!(err.contains("row payload required"));
    }

    #[test]
    fn row_payload_requirement_allows_empty_build_without_store() {
        let artifact =
            JoinBuildArtifact::new_native(row_payload_requirements(), None, None, 0, false, None);
        assert!(
            artifact
                .validate_components(row_payload_requirements())
                .is_ok()
        );
    }

    #[test]
    fn no_lookup_requirement_allows_missing_table() {
        let artifact =
            JoinBuildArtifact::new_native(no_lookup_requirements(), None, None, 3, false, None);
        assert!(
            artifact
                .validate_components(no_lookup_requirements())
                .is_ok()
        );
    }

    #[test]
    fn provided_contract_mismatch_reports_component_contract_mismatch() {
        let artifact = JoinBuildArtifact::new_native(
            membership_only_requirements(),
            None,
            Some(lookup_table()),
            3,
            false,
            None,
        );
        let err = artifact
            .validate_components(row_payload_requirements())
            .expect_err("mismatched provided contract must fail");
        assert!(err.contains("component contract mismatch"));
    }

    #[test]
    fn missing_null_key_rows_for_nonempty_build_reports_required_rows() {
        let artifact =
            JoinBuildArtifact::new_native(null_key_rows_requirements(), None, None, 3, false, None);
        let err = artifact
            .validate_components(null_key_rows_requirements())
            .expect_err("nonempty build must carry required null-key rows");
        assert!(err.contains("null-key rows required"));
    }

    #[test]
    fn build_view_build_chunk_reports_missing_payload_context() {
        let artifact = Arc::new(JoinBuildArtifact::new_native(
            no_lookup_requirements(),
            None,
            None,
            3,
            false,
            None,
        ));
        let view = BuildView::new(artifact, no_lookup_requirements()).expect("build view");

        let err = view
            .build_chunk("test context")
            .expect_err("missing payload should report context");
        assert!(
            err.contains("hash join row payload required for test context but missing"),
            "err={err}"
        );
        assert!(err.contains("required="), "err={err}");
        assert!(err.contains("provided="), "err={err}");
    }
}
