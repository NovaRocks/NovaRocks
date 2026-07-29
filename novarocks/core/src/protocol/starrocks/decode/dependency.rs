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

use std::cell::RefCell;
use std::collections::BTreeMap;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

#[cfg(feature = "compat")]
use super::node::LakeMetaValuesPatch;
use crate::exec::expr::ExprId;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentExprArenaOwner {
    Plan,
    DataStream,
    MultiCastDataStream,
    SplitDataStream,
    IcebergTable,
    IcebergChangeStreamRouter,
    StarRocksOutputProjection,
    StarRocksPartition,
    StarRocksIndexPredicate { index: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OwnedExprId {
    owner: FragmentExprArenaOwner,
    expr_id: ExprId,
}

impl OwnedExprId {
    pub(crate) const fn owner(self) -> FragmentExprArenaOwner {
        self.owner
    }

    pub(crate) const fn expr_id(self) -> ExprId {
        self.expr_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QueryProfilePatch {
    dependency_id: u64,
    target: OwnedExprId,
}

impl QueryProfilePatch {
    pub(crate) const fn dependency_id(self) -> u64 {
        self.dependency_id
    }

    pub(crate) const fn target(self) -> OwnedExprId {
        self.target
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StarRocksExternalDependency {
    QueryProfile {
        id: u64,
        query_id: String,
    },
    LakeMetaStorage {
        id: u64,
        request: LakeMetaStorageRequest,
    },
}

impl StarRocksExternalDependency {
    pub fn id(&self) -> u64 {
        match self {
            Self::QueryProfile { id, .. } => *id,
            Self::LakeMetaStorage { id, .. } => *id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LakeMetaColumnKind {
    Dictionary,
    Value(DataType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeMetaColumnRequest {
    pub(crate) column_id: String,
    pub(crate) kind: LakeMetaColumnKind,
}

impl LakeMetaColumnRequest {
    pub(crate) fn storage_key(&self) -> String {
        format!("{}:{:?}", self.column_id, self.kind)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeMetaTabletRequest {
    pub(crate) tablet_id: i64,
    pub(crate) version: i64,
    pub(crate) row_count_hint: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeMetaStorageRequest {
    id: u64,
    pub(crate) query_id: QueryId,
    pub(crate) catalog: String,
    pub(crate) db_name: String,
    pub(crate) table_name: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) tablets: Vec<LakeMetaTabletRequest>,
    pub(crate) columns: Vec<LakeMetaColumnRequest>,
}

impl LakeMetaStorageRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_id: QueryId,
        catalog: String,
        db_name: String,
        table_name: String,
        db_id: i64,
        table_id: i64,
        schema_id: i64,
        tablets: Vec<LakeMetaTabletRequest>,
        columns: Vec<LakeMetaColumnRequest>,
    ) -> Self {
        let stable_key = format!(
            "{query_id}:{catalog}:{db_name}:{table_name}:{db_id}:{table_id}:{schema_id}:{tablets:?}:{columns:?}"
        );
        Self {
            id: stable_dependency_id("lake-meta-storage", &stable_key),
            query_id,
            catalog,
            db_name,
            table_name,
            db_id,
            table_id,
            schema_id,
            tablets,
            columns,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Clone, Debug)]
pub struct LakeMetaStorageFacts {
    pub(crate) total_rows: i64,
    pub(crate) column_arrays: BTreeMap<String, Vec<ArrayRef>>,
}

impl LakeMetaStorageFacts {
    pub fn new(total_rows: i64, column_arrays: BTreeMap<String, Vec<ArrayRef>>) -> Self {
        Self {
            total_rows,
            column_arrays,
        }
    }

    pub fn total_rows(&self) -> i64 {
        self.total_rows
    }
}

#[derive(Clone, Debug)]
pub enum StarRocksResolvedDependencyValue {
    QueryProfile(String),
    LakeMetaStorage(LakeMetaStorageFacts),
}

impl StarRocksResolvedDependencyValue {
    pub(crate) const fn kind_name(&self) -> &'static str {
        match self {
            Self::QueryProfile(_) => "query_profile",
            Self::LakeMetaStorage(_) => "lake_meta_storage",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct StarRocksResolvedDependencies(BTreeMap<u64, StarRocksResolvedDependencyValue>);

impl StarRocksResolvedDependencies {
    pub(crate) fn new(values: BTreeMap<u64, StarRocksResolvedDependencyValue>) -> Self {
        Self(values)
    }

    pub fn insert(
        &mut self,
        id: u64,
        value: StarRocksResolvedDependencyValue,
    ) -> Option<StarRocksResolvedDependencyValue> {
        self.0.insert(id, value)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&u64, &StarRocksResolvedDependencyValue)> {
        self.0.iter()
    }

    pub fn get(&self, id: u64) -> Option<&StarRocksResolvedDependencyValue> {
        self.0.get(&id)
    }
}

pub struct StarRocksExternalDependencyDraft {
    frontend_endpoint: Option<RuntimeEndpoint>,
    resolved_query_profiles: BTreeMap<String, String>,
    resolved_lake_meta_storage: BTreeMap<u64, LakeMetaStorageFacts>,
    requirements: RefCell<BTreeMap<u64, StarRocksExternalDependency>>,
    query_profile_owner: RefCell<FragmentExprArenaOwner>,
    query_profile_patches: RefCell<Vec<QueryProfilePatch>>,
    #[cfg(feature = "compat")]
    lake_meta_values_patches: RefCell<Vec<LakeMetaValuesPatch>>,
}

impl StarRocksExternalDependencyDraft {
    pub(crate) fn new(
        frontend_endpoint: Option<RuntimeEndpoint>,
        resolved_query_profiles: BTreeMap<String, String>,
    ) -> Self {
        Self {
            frontend_endpoint,
            resolved_query_profiles,
            resolved_lake_meta_storage: BTreeMap::new(),
            requirements: RefCell::new(BTreeMap::new()),
            query_profile_owner: RefCell::new(FragmentExprArenaOwner::Plan),
            query_profile_patches: RefCell::new(Vec::new()),
            #[cfg(feature = "compat")]
            lake_meta_values_patches: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn new_with_lake_meta_storage(
        frontend_endpoint: Option<RuntimeEndpoint>,
        resolved_query_profiles: BTreeMap<String, String>,
        resolved_lake_meta_storage: BTreeMap<u64, LakeMetaStorageFacts>,
    ) -> Self {
        Self {
            frontend_endpoint,
            resolved_query_profiles,
            resolved_lake_meta_storage,
            requirements: RefCell::new(BTreeMap::new()),
            query_profile_owner: RefCell::new(FragmentExprArenaOwner::Plan),
            query_profile_patches: RefCell::new(Vec::new()),
            #[cfg(feature = "compat")]
            lake_meta_values_patches: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn frontend_endpoint(&self) -> Option<&RuntimeEndpoint> {
        self.frontend_endpoint.as_ref()
    }

    pub(crate) fn query_profile(&self, query_id: &str) -> Result<String, String> {
        if let Some(profile) = self.resolved_query_profiles.get(query_id) {
            return Ok(profile.clone());
        }
        let id = stable_dependency_id("query-profile", query_id);
        let requirement = StarRocksExternalDependency::QueryProfile {
            id,
            query_id: query_id.to_string(),
        };
        let mut requirements = self.requirements.borrow_mut();
        if let Some(existing) = requirements.get(&id)
            && existing != &requirement
        {
            return Err(format!("external dependency id collision for id={id}"));
        }
        requirements.insert(id, requirement);
        // Decode attempts are drafts until their requirement set is empty.  The
        // placeholder keeps discovery type-correct; the fragment decoder must
        // never publish or execute a draft that recorded this requirement.
        Ok(String::new())
    }

    pub(crate) fn query_profile_value(
        &self,
        query_id: &str,
    ) -> Result<DraftDependencyValue<String>, String> {
        if let Some(profile) = self.resolved_query_profiles.get(query_id) {
            return Ok(DraftDependencyValue::Resolved(profile.clone()));
        }
        let id = stable_dependency_id("query-profile", query_id);
        let requirement = StarRocksExternalDependency::QueryProfile {
            id,
            query_id: query_id.to_string(),
        };
        self.insert_requirement(requirement)?;
        Ok(DraftDependencyValue::Pending(id))
    }

    pub(crate) fn record_query_profile_slot(&self, dependency_id: u64, expr_id: ExprId) {
        let owner = *self.query_profile_owner.borrow();
        self.query_profile_patches
            .borrow_mut()
            .push(QueryProfilePatch {
                dependency_id,
                target: OwnedExprId { owner, expr_id },
            });
    }

    pub(crate) fn query_profile_patches(&self) -> Vec<QueryProfilePatch> {
        self.query_profile_patches.borrow().clone()
    }

    pub(crate) fn with_expr_arena_owner<T>(
        &self,
        owner: FragmentExprArenaOwner,
        lower: impl FnOnce() -> T,
    ) -> T {
        let previous = self.query_profile_owner.replace(owner);
        let result = lower();
        self.query_profile_owner.replace(previous);
        result
    }

    pub(crate) fn lake_meta_storage(
        &self,
        request: &LakeMetaStorageRequest,
    ) -> Result<LakeMetaStorageFacts, String> {
        if let Some(facts) = self.resolved_lake_meta_storage.get(&request.id()) {
            return Ok(facts.clone());
        }
        let requirement = StarRocksExternalDependency::LakeMetaStorage {
            id: request.id(),
            request: request.clone(),
        };
        let mut requirements = self.requirements.borrow_mut();
        if let Some(existing) = requirements.get(&request.id())
            && existing != &requirement
        {
            return Err(format!(
                "external dependency id collision for id={}",
                request.id()
            ));
        }
        requirements.insert(request.id(), requirement);
        // Preserve the requested keys so the rest of LAKE_META_SCAN_NODE can
        // finish structural validation without touching storage during discovery.
        let column_arrays = request
            .columns
            .iter()
            .map(|column| (column.storage_key(), Vec::new()))
            .collect();
        Ok(LakeMetaStorageFacts {
            total_rows: 0,
            column_arrays,
        })
    }

    #[cfg(feature = "compat")]
    pub(crate) fn lake_meta_storage_value(
        &self,
        request: &LakeMetaStorageRequest,
    ) -> Result<DraftDependencyValue<LakeMetaStorageFacts>, String> {
        if let Some(facts) = self.resolved_lake_meta_storage.get(&request.id()) {
            return Ok(DraftDependencyValue::Resolved(facts.clone()));
        }
        self.insert_requirement(StarRocksExternalDependency::LakeMetaStorage {
            id: request.id(),
            request: request.clone(),
        })?;
        Ok(DraftDependencyValue::Pending(request.id()))
    }

    #[cfg(feature = "compat")]
    pub(crate) fn record_lake_meta_values_patch(&self, patch: LakeMetaValuesPatch) {
        self.lake_meta_values_patches.borrow_mut().push(patch);
    }

    #[cfg(feature = "compat")]
    pub(crate) fn lake_meta_values_patches(&self) -> Vec<LakeMetaValuesPatch> {
        self.lake_meta_values_patches.borrow().clone()
    }

    pub(crate) fn external_dependencies(&self) -> Vec<StarRocksExternalDependency> {
        self.requirements.borrow().values().cloned().collect()
    }

    fn insert_requirement(&self, requirement: StarRocksExternalDependency) -> Result<(), String> {
        let id = requirement.id();
        let mut requirements = self.requirements.borrow_mut();
        if let Some(existing) = requirements.get(&id)
            && existing != &requirement
        {
            return Err(format!("external dependency id collision for id={id}"));
        }
        requirements.insert(id, requirement);
        Ok(())
    }
}

pub enum DraftDependencyValue<T> {
    Resolved(T),
    Pending(u64),
}

fn stable_dependency_id(kind: &str, key: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in kind.bytes().chain([0]).chain(key.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
