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

//! Task/scan-owned execution of FE-normalized Iceberg delete closures.
//! Shared flights belong to their live waiters; completed applications belong
//! to this manager. No split becomes ready through a sequence watermark.

use super::column_handle::{IcebergColumnHandle, corrupt, unsupported};
use super::split::{IcebergDeleteFile, IcebergDeleteFileContent, IcebergFileFormat, IcebergSplit};
use crate::access_binding::IcebergReadBinding;
use crate::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent as PhysicalDeleteContent,
    IcebergFileFormat as PhysicalDeleteFormat, validate_delete_apply_cost,
};
use crate::delete_semantics::*;
use crate::file_reader::equality_delete::{
    EqualityColumnBinding, EqualityKey, load_bound_equality_keys,
};
use crate::file_reader::map_file_error;
use crate::iceberg::spec::{PartitionSpec, Schema};
use crate::position_delete::load_position_deletes_async_with_metrics;
use arrow::array::{Array, BooleanArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared, WeakShared};
use novarocks_fs::FileReadContext;
use novarocks_spi::connector::ConnectorError;
#[cfg(test)]
use novarocks_spi::connector::ConnectorErrorKind;
use roaring::RoaringTreemap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::result::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteDomainBindings {
    Single(Arc<ReadDomain>),
    Window {
        from: Arc<ReadDomain>,
        to: Arc<ReadDomain>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointDeleteClosure {
    pub read_domain: Arc<ReadDomain>,
    pub deletes: Vec<IcebergDeleteFile>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteEvaluationMode {
    ExcludeDeleted,
    VisibleDifference {
        from: EndpointDeleteClosure,
        to: EndpointDeleteClosure,
    },
}

type Flight<T> = Shared<BoxFuture<'static, Result<Arc<T>, ConnectorError>>>;
type WeakFlight<T> = WeakShared<BoxFuture<'static, Result<Arc<T>, ConnectorError>>>;
struct SharedLoad<T> {
    ready: tokio::sync::OnceCell<Result<Arc<T>, ConnectorError>>,
    flight: Mutex<Option<WeakFlight<T>>>,
}
impl<T> Default for SharedLoad<T> {
    fn default() -> Self {
        Self {
            ready: tokio::sync::OnceCell::new(),
            flight: Mutex::new(None),
        }
    }
}
impl<T: Send + Sync + 'static> SharedLoad<T> {
    async fn get(
        self: &Arc<Self>,
        context: FileReadContext,
        run: impl FnOnce() -> BoxFuture<'static, Result<T, ConnectorError>> + Send + 'static,
    ) -> Result<Arc<T>, ConnectorError> {
        context.check_active().map_err(map_file_error)?;
        if let Some(value) = self.ready.get() {
            return value.clone();
        }
        let flight: Flight<T> = {
            let mut slot = self.flight.lock().map_err(|e| corrupt(e.to_string()))?;
            if let Some(value) = self.ready.get() {
                return value.clone();
            }
            if let Some(flight) = slot.as_ref().and_then(WeakShared::upgrade) {
                flight
            } else {
                let cell = Arc::clone(self);
                let flight = async move {
                    let result = run().await.map(Arc::new);
                    // Stop/deadline is a demand outcome, not artifact corruption.
                    // Physical helper errors must not mask the owner's typed stop.
                    match context.check_active() {
                        Ok(()) => {
                            let _ = cell.ready.set(result.clone());
                            result
                        }
                        Err(stop) => Err(map_file_error(stop)),
                    }
                }
                .boxed()
                .shared();
                *slot = flight.downgrade();
                flight
            }
        };
        flight.await
    }
}

const UNION_SHARDS: usize = 32;
struct EqualityUnion {
    group: EqualityFieldGroup,
    shards: [RwLock<HashMap<EqualityKey, i64>>; UNION_SHARDS],
    probes: AtomicUsize,
}
impl EqualityUnion {
    fn new(group: EqualityFieldGroup) -> Self {
        Self {
            group,
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            probes: AtomicUsize::new(0),
        }
    }
    fn shard(key: &EqualityKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % UNION_SHARDS
    }
    fn merge(&self, keys: &[EqualityKey], sequence: i64) -> Result<(), ConnectorError> {
        let mut by_shard: [Vec<&EqualityKey>; UNION_SHARDS] = std::array::from_fn(|_| Vec::new());
        for key in keys {
            by_shard[Self::shard(key)].push(key);
        }
        for (shard, keys) in self.shards.iter().zip(by_shard) {
            if keys.is_empty() {
                continue;
            }
            let mut map = shard.write().map_err(|e| corrupt(e.to_string()))?;
            for key in keys {
                map.entry(key.clone())
                    .and_modify(|old| *old = (*old).max(sequence))
                    .or_insert(sequence);
            }
        }
        Ok(())
    }
    async fn merge_ready(
        &self,
        keys: &[EqualityKey],
        sequence: i64,
        context: &FileReadContext,
    ) -> Result<(), ConnectorError> {
        // A complete artifact is validated before any chunk enters the union.
        // Each driver poll does bounded key work; no shard lock survives a yield.
        for (index, chunk) in keys.chunks(4096).enumerate() {
            if index != 0 {
                tokio::task::yield_now().await;
            }
            context.check_active().map_err(map_file_error)?;
            self.merge(chunk, sequence)?;
        }
        Ok(())
    }
    fn apply_keys(
        &self,
        keys: &[EqualityKey],
        rows: &[Vec<usize>; UNION_SHARDS],
        sequence: i64,
        deleted: &mut [bool],
    ) -> Result<(), ConnectorError> {
        for (shard, rows) in self.shards.iter().zip(rows) {
            if rows.is_empty() {
                continue;
            }
            let map = shard.read().map_err(|e| corrupt(e.to_string()))?;
            for &row in rows {
                deleted[row] |= map.get(&keys[row]).is_some_and(|max| *max > sequence);
            }
        }
        self.probes.fetch_add(keys.len(), Ordering::Relaxed);
        Ok(())
    }
}
#[derive(Clone, Eq, Hash, PartialEq)]
struct UnionKey {
    domain: usize,
    scope: EqualityScope,
    group: EqualityFieldGroup,
}
#[derive(Clone, Eq, Hash, PartialEq)]
struct DecodeKey {
    address: DeleteContentAddress,
    format: DeleteFormat,
    size: u64,
    key_metadata: Arc<[u8]>,
    group: Option<EqualityFieldGroup>,
    target: Option<Arc<str>>,
}
#[derive(Clone, Eq, Hash, PartialEq)]
struct ApplicationKey {
    domain: usize,
    application: DeleteApplication,
    target: Option<Arc<str>>,
}
enum DecodedContent {
    Equality(Vec<EqualityKey>),
    Position(Arc<RoaringTreemap>),
}
enum AppliedContent {
    Equality(Arc<EqualityUnion>),
    Position(Arc<RoaringTreemap>),
}
#[derive(Default)]
struct ManagerState {
    domains: HashMap<Arc<ReadDomain>, usize>,
    unions: HashMap<UnionKey, Arc<EqualityUnion>>,
    decodes: HashMap<DecodeKey, Arc<SharedLoad<DecodedContent>>>,
    applications: HashMap<ApplicationKey, Arc<SharedLoad<AppliedContent>>>,
}
#[derive(Default)]
struct Counters {
    attempted: AtomicUsize,
    loaded: AtomicUsize,
    decoded_rows: AtomicUsize,
    merged: AtomicUsize,
    completed: AtomicUsize,
    row_key_batches: AtomicUsize,
    row_key_rows: AtomicUsize,
}
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct DeleteManagerDiagnostics {
    pub domains: Vec<DeleteDomainDiagnostics>,
    pub physical_loads: usize,
    pub physical_load_attempts: usize,
    pub decoded_rows: usize,
    pub application_merges: usize,
    pub completed_applications: usize,
    pub applications: usize,
    pub buckets: usize,
    pub keys: usize,
    pub probes: usize,
    pub row_key_batches: usize,
    pub row_key_rows: usize,
    pub retained_key_bytes: usize,
    pub retained_decode_bytes: usize,
    pub position_bitmap_bytes: usize,
}
/// Explicit snapshots only: these map/key capacity figures exclude allocator
/// bookkeeping and are observations, never a hard memory cap.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct DeleteDomainDiagnostics {
    pub snapshot_id: i64,
    pub applications: usize,
    pub completed_applications: usize,
    pub buckets: usize,
    pub keys: usize,
    pub probes: usize,
    pub retained_key_bytes: usize,
}
// Design: ADR-0164 (docs/adr/ADR-0164-iceberg-delete-closures-and-execution-unions.md)
pub struct DeleteManager {
    binding: IcebergReadBinding,
    context: FileReadContext,
    state: Mutex<ManagerState>,
    counters: Arc<Counters>,
}
impl DeleteManager {
    pub fn new(binding: IcebergReadBinding, context: FileReadContext) -> Self {
        Self {
            binding,
            context,
            state: Mutex::new(ManagerState::default()),
            counters: Arc::new(Counters::default()),
        }
    }
    pub fn loaded_artifacts(&self) -> Result<usize, ConnectorError> {
        Ok(self.counters.loaded.load(Ordering::Relaxed))
    }
    #[cfg(test)]
    pub(crate) fn diagnostics(&self) -> Result<DeleteManagerDiagnostics, ConnectorError> {
        let state = self.state.lock().map_err(|e| corrupt(e.to_string()))?;
        let mut d = DeleteManagerDiagnostics {
            physical_loads: self.counters.loaded.load(Ordering::Relaxed),
            physical_load_attempts: self.counters.attempted.load(Ordering::Relaxed),
            decoded_rows: self.counters.decoded_rows.load(Ordering::Relaxed),
            application_merges: self.counters.merged.load(Ordering::Relaxed),
            completed_applications: self.counters.completed.load(Ordering::Relaxed),
            applications: state.applications.len(),
            buckets: state.unions.len(),
            row_key_batches: self.counters.row_key_batches.load(Ordering::Relaxed),
            row_key_rows: self.counters.row_key_rows.load(Ordering::Relaxed),
            ..Default::default()
        };
        d.domains = vec![DeleteDomainDiagnostics::default(); state.domains.len()];
        for (domain, id) in &state.domains {
            d.domains[*id].snapshot_id = domain.endpoint().snapshot_id();
        }
        for (key, load) in &state.applications {
            d.domains[key.domain].applications += 1;
            d.domains[key.domain].completed_applications +=
                usize::from(matches!(load.ready.get(), Some(Ok(_))));
        }
        let mut payloads = HashSet::new();
        for (key, union) in &state.unions {
            let domain = &mut d.domains[key.domain];
            domain.buckets += 1;
            let probes = union.probes.load(Ordering::Relaxed);
            domain.probes += probes;
            d.probes += probes;
            for shard in &union.shards {
                let map = shard.read().map_err(|e| corrupt(e.to_string()))?;
                d.keys += map.len();
                domain.keys += map.len();
                let capacity_bytes = map.capacity() * std::mem::size_of::<(EqualityKey, i64)>();
                d.retained_key_bytes += capacity_bytes;
                domain.retained_key_bytes += capacity_bytes;
                for key in map.keys() {
                    let bytes = key_retained_bytes(key, &mut payloads);
                    d.retained_key_bytes += bytes;
                    domain.retained_key_bytes += bytes;
                }
            }
        }
        for load in state.decodes.values() {
            if let Some(Ok(content)) = load.ready.get() {
                match content.as_ref() {
                    DecodedContent::Equality(keys) => {
                        d.retained_decode_bytes +=
                            keys.capacity() * std::mem::size_of::<EqualityKey>();
                        for key in keys {
                            d.retained_decode_bytes += key_retained_bytes(key, &mut payloads);
                        }
                    }
                    DecodedContent::Position(bitmap) => {
                        d.position_bitmap_bytes += bitmap.serialized_size()
                    }
                }
            }
        }
        Ok(d)
    }
    pub fn preview_hidden_columns(
        split: &IcebergSplit,
        schema: &Schema,
        bindings: &DeleteDomainBindings,
        mode: &DeleteEvaluationMode,
    ) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
        let sides = plan_sides(split, schema, bindings, mode)?;
        hidden_columns(&sides, schema)
    }

    pub async fn open_split(
        &self,
        split: &IcebergSplit,
        schema: &Schema,
        bindings: &DeleteDomainBindings,
        mode: DeleteEvaluationMode,
    ) -> Result<SplitDeleteFilter, ConnectorError> {
        self.context.check_active().map_err(map_file_error)?;
        let sides = plan_sides(split, schema, bindings, &mode)?;
        let hidden = hidden_columns(&sides, schema)?;
        let mut loaded = Vec::new();
        for side in sides {
            loaded.push(self.load_side(split, side).await?);
        }
        let verdict = match mode {
            DeleteEvaluationMode::ExcludeDeleted => FilterVerdict::Exclude(loaded.remove(0)),
            DeleteEvaluationMode::VisibleDifference { .. } => FilterVerdict::Difference {
                from: loaded.remove(0),
                to: loaded.remove(0),
            },
        };
        let probe_groups = ProbeGroup::from_verdict(&verdict);
        Ok(SplitDeleteFilter {
            verdict,
            probe_groups,
            row_bindings: Mutex::new(None),
            counters: self.counters.clone(),
            data_file_path: Arc::from(split.path()),
            sequence: split
                .data_sequence_number()
                .ok_or_else(|| corrupt("Iceberg split requires data sequence"))?,
            hidden_columns: hidden,
        })
    }
    async fn load_side(
        &self,
        split: &IcebergSplit,
        side: SidePlan,
    ) -> Result<ResolvedDeletes, ConnectorError> {
        let domain_id = {
            let mut state = self.state.lock().map_err(|e| corrupt(e.to_string()))?;
            let next = state.domains.len();
            *state.domains.entry(side.domain.clone()).or_insert(next)
        };
        let mut positions: Option<Arc<RoaringTreemap>> = None;
        let mut unions = Vec::new();
        let mut seen = HashSet::new();
        for (delete, fact) in side.members {
            let target = if matches!(fact.kind(), DeleteKind::Equality(_)) {
                None
            } else {
                Some(Arc::from(split.path()))
            };
            let group = if let DeleteKind::Equality(group) = fact.kind() {
                Some(group.clone())
            } else {
                None
            };
            let decode_key = DecodeKey {
                address: fact.address().clone(),
                format: fact.read().format,
                size: fact.read().file_size,
                key_metadata: fact.read().key_metadata.clone(),
                group: group.clone(),
                target: target.clone(),
            };
            let app_key = ApplicationKey {
                domain: domain_id,
                application: fact.application().clone(),
                target: target.clone(),
            };
            let union_key = group.clone().map(|group| UnionKey {
                domain: domain_id,
                scope: fact.equality_scope(),
                group,
            });
            let (decode, application, union) = {
                let mut state = self.state.lock().map_err(|e| corrupt(e.to_string()))?;
                let decode = state.decodes.entry(decode_key).or_default().clone();
                let application = state.applications.entry(app_key).or_default().clone();
                let union = union_key.as_ref().map(|key| {
                    state
                        .unions
                        .entry(key.clone())
                        .or_insert_with(|| Arc::new(EqualityUnion::new(key.group.clone())))
                        .clone()
                });
                (decode, application, union)
            };
            let binding = self.binding.clone();
            let context = self.context.clone();
            let counters = self.counters.clone();
            let data_path = split.path().to_string();
            let sequence = fact.sequence().get();
            let applied = application
                .get(self.context.clone(), move || {
                    async move {
                        let load_context = context.clone();
                        let counters_load = counters.clone();
                        let content = decode
                            .get(context.clone(), move || {
                                async move {
                                    if delete.decryption_data().is_some_and(|data| {
                                        !data.key_metadata().is_empty()
                                            || !data.aad_prefix().is_empty()
                                    }) {
                                        return Err(unsupported(
                                            "Encrypted delete files are unsupported",
                                        ));
                                    }
                                    counters_load.attempted.fetch_add(1, Ordering::Relaxed);
                                    let access = binding
                                        .resolve_access_for_locations_async(
                                            [delete.path()],
                                            &load_context,
                                        )
                                        .await?;
                                    let spec = physical_delete_spec(&delete)?;
                                    let content = if let Some(group) = group {
                                        let (keys, _) = load_bound_equality_keys(
                                            &spec,
                                            &group,
                                            &access,
                                            &load_context,
                                            |rows| {
                                                counters_load
                                                    .decoded_rows
                                                    .fetch_add(rows, Ordering::Relaxed);
                                            },
                                        )
                                        .await?;
                                        DecodedContent::Equality(keys)
                                    } else {
                                        let (positions, _) =
                                            load_position_deletes_async_with_metrics(
                                                std::slice::from_ref(&spec),
                                                &data_path,
                                                &access,
                                                &load_context,
                                                |rows| {
                                                    counters_load
                                                        .decoded_rows
                                                        .fetch_add(rows, Ordering::Relaxed);
                                                },
                                            )
                                            .await?;
                                        DecodedContent::Position(Arc::new(positions))
                                    };
                                    counters_load.loaded.fetch_add(1, Ordering::Relaxed);
                                    Ok(content)
                                }
                                .boxed()
                            })
                            .await?;
                        context.check_active().map_err(map_file_error)?;
                        let applied = match (content.as_ref(), union) {
                            (DecodedContent::Equality(keys), Some(union)) => {
                                union.merge_ready(keys, sequence, &context).await?;
                                counters.merged.fetch_add(1, Ordering::Relaxed);
                                AppliedContent::Equality(union)
                            }
                            (DecodedContent::Position(bitmap), None) => {
                                AppliedContent::Position(bitmap.clone())
                            }
                            _ => {
                                return Err(corrupt(
                                    "Delete physical content differs from its application kind",
                                ));
                            }
                        };
                        counters.completed.fetch_add(1, Ordering::Relaxed);
                        Ok(applied)
                    }
                    .boxed()
                })
                .await?;
            match applied.as_ref() {
                AppliedContent::Position(bitmap) => match &mut positions {
                    None => positions = Some(bitmap.clone()),
                    Some(existing) if Arc::ptr_eq(existing, bitmap) => {}
                    Some(existing) => *Arc::make_mut(existing) |= bitmap.as_ref(),
                },
                AppliedContent::Equality(union) => {
                    if seen.insert(Arc::as_ptr(union) as usize) {
                        unions.push(union.clone());
                    }
                }
            }
        }
        Ok(ResolvedDeletes {
            positions: positions.unwrap_or_default(),
            equality: unions,
        })
    }
}
#[cfg(test)]
fn key_retained_bytes(key: &EqualityKey, payloads: &mut HashSet<(u8, usize)>) -> usize {
    key.capacity() * std::mem::size_of::<Option<CanonicalScalar>>()
        + key
            .iter()
            .flatten()
            .map(|scalar| match scalar {
                CanonicalScalar::String(s) if payloads.insert((0, s.as_ptr() as usize)) => s.len(),
                CanonicalScalar::Binary(b) if payloads.insert((1, b.as_ptr() as usize)) => b.len(),
                _ => 0,
            })
            .sum::<usize>()
}
fn hidden_columns(
    sides: &[SidePlan],
    schema: &Schema,
) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
    let mut fields = HashSet::new();
    let mut columns = BTreeMap::new();
    for side in sides {
        for (_, fact) in &side.members {
            if let DeleteKind::Equality(group) = fact.kind() {
                for (id, _) in group.fields() {
                    if fields.insert(*id) {
                        columns.insert(
                            top_level_schema_position(schema, *id)?,
                            IcebergColumnHandle::base_column_of(schema, *id)?,
                        );
                    }
                }
            }
        }
    }
    Ok(columns.into_values().collect())
}
struct BoundDomain {
    schema: Schema,
    partition_types: HashMap<i32, crate::iceberg::spec::StructType>,
    specs: HashMap<i32, PartitionSpec>,
    groups: HashMap<Vec<i32>, EqualityFieldGroup>,
}
impl BoundDomain {
    fn new(domain: &ReadDomain) -> Result<Self, ConnectorError> {
        let schema = domain
            .endpoint()
            .schema()
            .map_err(|e| corrupt(e.to_string()))?;
        let specs = domain
            .endpoint()
            .partition_spec_jsons()
            .iter()
            .map(|(id, json)| {
                let spec: PartitionSpec =
                    serde_json::from_str(json).map_err(|e| corrupt(e.to_string()))?;
                if *id != spec.spec_id() {
                    return Err(corrupt(
                        "Pinned partition spec ID differs from its definition",
                    ));
                }
                Ok((*id, spec))
            })
            .collect::<Result<HashMap<_, _>, ConnectorError>>()?;
        let partition_types = domain
            .endpoint()
            .partition_type_jsons()
            .keys()
            .map(|id| {
                domain
                    .endpoint()
                    .partition_type(*id)
                    .map(|ty| (*id, ty))
                    .map_err(|e| corrupt(e.to_string()))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(Self {
            schema,
            partition_types,
            specs,
            groups: HashMap::new(),
        })
    }
    fn partition(&self, id: i32, json: &str) -> Result<TypedPartition, ConnectorError> {
        let spec = self
            .specs
            .get(&id)
            .ok_or_else(|| corrupt("Partition spec is not pinned by read domain"))?;
        let partition_type = self
            .partition_types
            .get(&id)
            .ok_or_else(|| corrupt("Partition storage type is not pinned by read domain"))?;
        let values = crate::delete_semantics::decode_partition_data_json_with_type(
            spec,
            partition_type,
            json,
        )
        .map_err(|e| corrupt(e.to_string()))?;
        TypedPartition::bind_type(spec, partition_type, &values).map_err(|e| corrupt(e.to_string()))
    }
    fn group(&mut self, ids: &[i32]) -> Result<EqualityFieldGroup, ConnectorError> {
        let mut canonical = ids.to_vec();
        canonical.sort_unstable();
        if canonical.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corrupt("Equality field IDs contain duplicates"));
        }
        if let Some(group) = self.groups.get(&canonical) {
            return Ok(group.clone());
        }
        let group = EqualityFieldGroup::bind(&canonical, &self.schema)
            .map_err(|e| corrupt(e.to_string()))?;
        self.groups.insert(canonical, group.clone());
        Ok(group)
    }
}
struct SidePlan {
    domain: Arc<ReadDomain>,
    members: Vec<(IcebergDeleteFile, Arc<DeleteFact>)>,
}
fn ordinary_domain<'a>(
    split: &IcebergSplit,
    bindings: &'a DeleteDomainBindings,
) -> Result<&'a Arc<ReadDomain>, ConnectorError> {
    match bindings {
        DeleteDomainBindings::Single(domain) => Ok(domain),
        DeleteDomainBindings::Window { from, to } if split.read_domain() == from => Ok(from),
        DeleteDomainBindings::Window { to, .. } if split.read_domain() == to => Ok(to),
        _ => Err(corrupt(
            "Iceberg split domain is not pinned by its relation",
        )),
    }
}
fn plan_sides(
    split: &IcebergSplit,
    schema: &Schema,
    bindings: &DeleteDomainBindings,
    mode: &DeleteEvaluationMode,
) -> Result<Vec<SidePlan>, ConnectorError> {
    let mut requests: Vec<(Arc<ReadDomain>, Arc<ReadDomain>, Vec<IcebergDeleteFile>)> = Vec::new();
    match mode {
        DeleteEvaluationMode::ExcludeDeleted => requests.push((
            ordinary_domain(split, bindings)?.clone(),
            split.read_domain().clone(),
            split.deletes().to_vec()?,
        )),
        DeleteEvaluationMode::VisibleDifference { from, to } => {
            let DeleteDomainBindings::Window {
                from: expected_from,
                to: expected_to,
            } = bindings
            else {
                return Err(corrupt(
                    "Visibility difference requires independently pinned From/To domains",
                ));
            };
            requests.push((
                expected_from.clone(),
                from.read_domain.clone(),
                from.deletes.clone(),
            ));
            requests.push((
                expected_to.clone(),
                to.read_domain.clone(),
                to.deletes.clone(),
            ));
        }
    }
    requests
        .into_iter()
        .map(|(expected, received, deletes)| {
            if expected != received {
                return Err(corrupt(
                    "Iceberg received read domain differs from independently pinned relation",
                ));
            }
            let mut bound = BoundDomain::new(&received)?;
            if bound.schema != *schema {
                return Err(corrupt(
                    "Iceberg relation schema differs from pinned read domain",
                ));
            }
            validate_split_delete_cost(split, &deletes)?;
            let data = DataFileFact::try_new(
                split.path(),
                DataSequenceNumber::try_new(
                    split
                        .data_sequence_number()
                        .ok_or_else(|| corrupt("Iceberg data sequence is missing"))?,
                )
                .map_err(|e| corrupt(e.to_string()))?,
                bound.partition(split.partition_spec_id(), split.partition_data_json())?,
                split.file_record_count() as u64,
                FileMetrics::default(),
            )
            .map_err(|e| corrupt(e.to_string()))?;
            let members = deletes
                .into_iter()
                .map(|delete| {
                    let fact = Arc::new(bind_delete(&mut bound, &delete)?);
                    Ok((delete, fact))
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;
            validate_normalized_closure(
                &expected,
                received.clone(),
                &data,
                members.iter().map(|(_, fact)| fact.clone()),
            )
            .map_err(|e| corrupt(e.to_string()))?;
            let mut seen = HashSet::new();
            Ok(SidePlan {
                domain: received,
                members: members
                    .into_iter()
                    .filter(|(_, fact)| seen.insert(fact.application().clone()))
                    .collect(),
            })
        })
        .collect()
}
fn bind_delete(
    bound: &mut BoundDomain,
    delete: &IcebergDeleteFile,
) -> Result<DeleteFact, ConnectorError> {
    if delete
        .decryption_data()
        .is_some_and(|data| !data.key_metadata().is_empty() || !data.aad_prefix().is_empty())
    {
        return Err(unsupported("Encrypted delete files are unsupported"));
    }
    let format = match delete.format() {
        IcebergFileFormat::Parquet => DeleteFormat::Parquet,
        IcebergFileFormat::Orc => DeleteFormat::Orc,
        IcebergFileFormat::Avro => DeleteFormat::Avro,
        IcebergFileFormat::Puffin => DeleteFormat::Puffin,
    };
    let kind = match delete.content() {
        IcebergDeleteFileContent::EqualityDeletes => {
            DeleteKind::Equality(bound.group(delete.equality_field_ids())?)
        }
        IcebergDeleteFileContent::PositionDeletes
            if delete.format() == IcebergFileFormat::Puffin =>
        {
            DeleteKind::DeletionVector {
                exact_target: Arc::from(
                    delete
                        .referenced_data_file()
                        .ok_or_else(|| corrupt("DV requires exact referenced target"))?,
                ),
            }
        }
        _ => DeleteKind::Position {
            exact_target: delete.referenced_data_file().map(Arc::from),
        },
    };
    let address = if let (Some(offset), Some(length)) =
        (delete.content_offset(), delete.content_size_in_bytes())
    {
        DeleteContentAddress::puffin(
            delete.path(),
            offset,
            length,
            delete.file_size_in_bytes() as u64,
        )
    } else {
        DeleteContentAddress::file(delete.path())
    }
    .map_err(|e| corrupt(e.to_string()))?;
    DeleteFact::try_new(DeleteFactParams {
        address,
        kind,
        sequence: DataSequenceNumber::try_new(delete.data_sequence_number())
            .map_err(|e| corrupt(e.to_string()))?,
        partition: bound.partition(delete.partition_spec_id(), delete.partition_data_json())?,
        read: DeleteReadFacts {
            format,
            file_size: delete.file_size_in_bytes() as u64,
            record_count: delete.record_count() as u64,
            key_metadata: Arc::from(
                delete
                    .decryption_data()
                    .map_or(&[][..], |d| d.key_metadata()),
            ),
        },
        metrics: FileMetrics::default(),
    })
    .map_err(|e| corrupt(e.to_string()))
}
struct ResolvedDeletes {
    positions: Arc<RoaringTreemap>,
    equality: Vec<Arc<EqualityUnion>>,
}
impl ResolvedDeletes {
    fn is_empty(&self) -> bool {
        self.positions.is_empty() && self.equality.is_empty()
    }
    fn position_mask(&self, positions: &UInt64Array) -> Vec<bool> {
        (0..positions.len())
            .map(|row| self.positions.contains(positions.value(row)))
            .collect()
    }
}
enum FilterVerdict {
    Exclude(ResolvedDeletes),
    Difference {
        from: ResolvedDeletes,
        to: ResolvedDeletes,
    },
}
struct ProbeGroup {
    group: EqualityFieldGroup,
    from: Vec<Arc<EqualityUnion>>,
    to: Vec<Arc<EqualityUnion>>,
}
impl ProbeGroup {
    fn from_verdict(verdict: &FilterVerdict) -> Vec<Self> {
        let mut groups = Vec::<Self>::new();
        let mut indices = HashMap::new();
        let mut add = |unions: &[Arc<EqualityUnion>], is_from: bool| {
            for union in unions {
                let index = *indices.entry(union.group.clone()).or_insert_with(|| {
                    let index = groups.len();
                    groups.push(Self {
                        group: union.group.clone(),
                        from: Vec::new(),
                        to: Vec::new(),
                    });
                    index
                });
                if is_from {
                    groups[index].from.push(union.clone());
                } else {
                    groups[index].to.push(union.clone());
                }
            }
        };
        match verdict {
            FilterVerdict::Exclude(to) => add(&to.equality, false),
            FilterVerdict::Difference { from, to } => {
                add(&from.equality, true);
                add(&to.equality, false);
            }
        }
        groups
    }
}
struct SplitRowBindings {
    schema: arrow::datatypes::SchemaRef,
    columns: Vec<Arc<EqualityColumnBinding>>,
}
pub struct SplitDeleteFilter {
    verdict: FilterVerdict,
    probe_groups: Vec<ProbeGroup>,
    row_bindings: Mutex<Option<SplitRowBindings>>,
    counters: Arc<Counters>,
    data_file_path: Arc<str>,
    sequence: i64,
    hidden_columns: Vec<IcebergColumnHandle>,
}
impl std::fmt::Debug for SplitDeleteFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitDeleteFilter")
            .field("data_file_path", &self.data_file_path)
            .finish_non_exhaustive()
    }
}
impl SplitDeleteFilter {
    pub fn required_hidden_columns(&self) -> &[IcebergColumnHandle] {
        &self.hidden_columns
    }
    pub fn is_empty(&self) -> bool {
        matches!(&self.verdict,FilterVerdict::Exclude(applied) if applied.is_empty())
    }
    pub fn evaluate(
        &self,
        batch: &RecordBatch,
        positions: &UInt64Array,
    ) -> Result<BooleanArray, ConnectorError> {
        if positions.len() != batch.num_rows() || positions.null_count() != 0 {
            return Err(corrupt("Iceberg absolute position count/null mismatch"));
        }
        let bindings = {
            let mut slot = self
                .row_bindings
                .lock()
                .map_err(|e| corrupt(e.to_string()))?;
            let schema = batch.schema();
            if let Some(bound) = slot.as_ref() {
                if !Arc::ptr_eq(&bound.schema, &schema) && bound.schema != schema {
                    return Err(corrupt(
                        "Iceberg split projected batch schema changed after binding",
                    ));
                }
            } else {
                let columns = self
                    .probe_groups
                    .iter()
                    .map(|group| {
                        EqualityColumnBinding::bind_materialized_page(schema.clone(), &group.group)
                            .map(Arc::new)
                            .map_err(corrupt)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                *slot = Some(SplitRowBindings { schema, columns });
            }
            slot.as_ref().expect("bound split columns").columns.clone()
        };
        let (mut from_mask, mut to_mask) = match &self.verdict {
            FilterVerdict::Exclude(to) => {
                (vec![false; batch.num_rows()], to.position_mask(positions))
            }
            FilterVerdict::Difference { from, to } => {
                (from.position_mask(positions), to.position_mask(positions))
            }
        };
        // Retain only one field group's keys at a time, shared by all scopes/endpoints.
        for (group, columns) in self.probe_groups.iter().zip(bindings) {
            let keys = columns
                .keys_after_schema_validation(batch)
                .map_err(corrupt)?;
            self.counters
                .row_key_batches
                .fetch_add(1, Ordering::Relaxed);
            self.counters
                .row_key_rows
                .fetch_add(keys.len(), Ordering::Relaxed);
            let mut rows: [Vec<usize>; UNION_SHARDS] = std::array::from_fn(|_| Vec::new());
            for (row, key) in keys.iter().enumerate() {
                rows[EqualityUnion::shard(key)].push(row);
            }
            for union in &group.from {
                union.apply_keys(&keys, &rows, self.sequence, &mut from_mask)?;
            }
            for union in &group.to {
                union.apply_keys(&keys, &rows, self.sequence, &mut to_mask)?;
            }
        }
        let keep: Vec<bool> = match &self.verdict {
            FilterVerdict::Exclude(_) => to_mask.into_iter().map(|v| !v).collect(),
            FilterVerdict::Difference { .. } => {
                if from_mask
                    .iter()
                    .zip(&to_mask)
                    .any(|(from, to)| *from && !*to)
                {
                    return Err(unsupported(
                        "Iceberg change window makes a previously deleted row visible",
                    ));
                }
                from_mask
                    .into_iter()
                    .zip(to_mask)
                    .map(|(from, to)| !from && to)
                    .collect()
            }
        };
        Ok(BooleanArray::from(keep))
    }
}

fn top_level_schema_position(
    table_schema: &Schema,
    field_id: i32,
) -> Result<usize, ConnectorError> {
    table_schema
        .as_struct()
        .fields()
        .iter()
        .position(|field| field.id == field_id)
        .ok_or_else(|| {
            corrupt(format!(
                "iceberg field id {field_id} is not a top-level field of the frozen table schema"
            ))
        })
}

/// Project one frozen delete descriptor onto the crate's physical read spec.
fn physical_delete_spec(
    delete: &IcebergDeleteFile,
) -> Result<IcebergDeleteFileSpec, ConnectorError> {
    let length = u64::try_from(delete.file_size_in_bytes()).map_err(|_| {
        corrupt(format!(
            "iceberg delete file {} has a negative file size",
            delete.path()
        ))
    })?;
    let file_format = match delete.format() {
        IcebergFileFormat::Parquet => PhysicalDeleteFormat::Parquet,
        IcebergFileFormat::Puffin => PhysicalDeleteFormat::Puffin,
        IcebergFileFormat::Orc | IcebergFileFormat::Avro => {
            return Err(unsupported(format!(
                "iceberg delete file {} is neither parquet nor puffin",
                delete.path()
            )));
        }
    };
    let file_content = match delete.content() {
        IcebergDeleteFileContent::PositionDeletes => PhysicalDeleteContent::PositionDeletes,
        IcebergDeleteFileContent::EqualityDeletes => PhysicalDeleteContent::EqualityDeletes,
    };
    Ok(IcebergDeleteFileSpec {
        path: delete.path().to_string(),
        file_format,
        file_content,
        length: Some(length),
        content_offset: delete.content_offset(),
        content_size_in_bytes: delete.content_size_in_bytes(),
        referenced_data_file: delete.referenced_data_file().map(str::to_string),
    })
}

/// Reuse the crate's single delete-apply cost bound.
///
/// Restating the 1024-file / 512-MiB limits here would create a second
/// authority that could drift from the one every other reader is admitted
/// against. The projection below carries only what the bound reads -- the data
/// file's identity and each attached delete's size -- and is handed to nothing
/// else.
///
/// Each independently pinned endpoint is admitted against the existing bound.
/// Window domains retain separate unions; their summed memory/work is reported
/// explicitly rather than disguised as one incomplete closure.
fn validate_split_delete_cost(
    split: &IcebergSplit,
    applied: &[IcebergDeleteFile],
) -> Result<(), ConnectorError> {
    let mut delete_files = Vec::with_capacity(applied.len());
    for delete in applied {
        let file_format = match delete.format() {
            IcebergFileFormat::Parquet => crate::scan_model::IcebergDeleteFileFormat::Parquet,
            IcebergFileFormat::Puffin => crate::scan_model::IcebergDeleteFileFormat::Puffin,
            IcebergFileFormat::Orc | IcebergFileFormat::Avro => {
                return Err(unsupported(format!(
                    "iceberg delete file {} is neither parquet nor puffin",
                    delete.path()
                )));
            }
        };
        let file_content = match delete.content() {
            IcebergDeleteFileContent::PositionDeletes => {
                crate::scan_model::IcebergDeleteFileContent::Position
            }
            IcebergDeleteFileContent::EqualityDeletes => {
                crate::scan_model::IcebergDeleteFileContent::Equality
            }
        };
        delete_files.push(crate::scan_model::IcebergDeleteFileInfo {
            path: delete.path().to_string(),
            file_format,
            file_content,
            length: Some(delete.file_size_in_bytes()),
            content_offset: delete.content_offset(),
            content_size_in_bytes: delete.content_size_in_bytes(),
            sequence_number: Some(delete.data_sequence_number()),
            partition_spec_id: Some(delete.partition_spec_id()),
            partition_data_json: Some(delete.partition_data_json().to_string()),
            record_count: Some(delete.record_count()),
            partition_key: None,
            referenced_data_file: delete.referenced_data_file().map(str::to_string),
            equality_column_names: Vec::new(),
            equality_field_ids: delete.equality_field_ids().to_vec(),
        });
    }
    validate_delete_apply_cost(&crate::scan_model::IcebergDataFileInfo {
        path: split.path().to_string(),
        size: split.file_size(),
        row_count: Some(split.file_record_count()),
        column_stats: None,
        partition_spec_id: Some(split.partition_spec_id()),
        partition_key: None,
        first_row_id: split.file_first_row_id(),
        data_sequence_number: split.data_sequence_number(),
        ivm_change_op: None,
        included_positions: None,
        delete_files,
        manifest_path: None,
        partition_values: Vec::new(),
    })
}

#[cfg(test)]
mod tests;
