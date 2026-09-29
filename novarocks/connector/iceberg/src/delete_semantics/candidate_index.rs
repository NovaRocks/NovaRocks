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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use super::facts::{PartitionTypeBinding, validate_partition_binding};
use super::{
    BucketView, DataFileFact, DataSequenceNumber, DeleteFact, DeleteKind, DeleteObservation,
    DeleteSemanticsError as Error, DeleteSemanticsErrorKind as Kind, DeleteSet, EqualityFieldGroup,
    EqualityScope, PositionSource, ReadDomain, Result, TypedPartition,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EqualityBucketKey {
    pub scope: EqualityScope,
    pub fields: EqualityFieldGroup,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum BucketKind {
    Equality(EqualityBucketKey),
    PartitionPositions(TypedPartition),
    PathPositions(Arc<str>),
}

/// One immutable sequence-sorted allocation shared by all file suffixes.
#[derive(Debug)]
pub struct FrozenBucket {
    kind: BucketKind,
    members: Box<[Arc<DeleteFact>]>,
    descriptor_variable_prefix: Box<[usize]>,
}

impl FrozenBucket {
    fn new(kind: BucketKind, mut members: Vec<Arc<DeleteFact>>) -> Arc<Self> {
        members.sort_by(|a, b| {
            a.sequence()
                .cmp(&b.sequence())
                .then_with(|| a.address().path().cmp(b.address().path()))
        });
        let mut descriptor_variable_prefix = Vec::with_capacity(members.len() + 1);
        descriptor_variable_prefix.push(0usize);
        for member in &members {
            descriptor_variable_prefix.push(
                descriptor_variable_prefix
                    .last()
                    .unwrap()
                    .saturating_add(member.descriptor_variable_bytes()),
            );
        }
        Arc::new(Self {
            kind,
            members: members.into_boxed_slice(),
            descriptor_variable_prefix: descriptor_variable_prefix.into_boxed_slice(),
        })
    }
    pub fn kind(&self) -> &BucketKind {
        &self.kind
    }
    pub fn members(&self) -> &[Arc<DeleteFact>] {
        &self.members
    }
    pub(crate) fn descriptor_variable_suffix_bytes(&self, start: usize) -> usize {
        self.descriptor_variable_prefix[self.members.len()]
            .saturating_sub(self.descriptor_variable_prefix[start])
    }

    fn suffix(
        self: &Arc<Self>,
        sequence: DataSequenceNumber,
        cost: &mut CandidateLookupCost,
    ) -> Option<BucketView> {
        let strict = matches!(self.kind, BucketKind::Equality(_));
        cost.bucket_lookups += 1;
        let start = self.members.partition_point(|fact| {
            cost.sequence_comparisons += 1;
            if strict {
                fact.sequence() <= sequence
            } else {
                fact.sequence() < sequence
            }
        });
        if start == self.members.len() {
            return None;
        }
        cost.view_count += 1;
        cost.candidate_members += self.members.len() - start;
        Some(BucketView::new(Arc::clone(self), start))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CandidateLookupCost {
    pub bucket_lookups: usize,
    pub sequence_comparisons: usize,
    pub view_count: usize,
    pub candidate_members: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CandidateIndexSize {
    pub member_references: usize,
    pub buckets: usize,
    pub deletion_vector_targets: usize,
}

/// Complete metadata index for one pinned endpoint. Building it validates all
/// observed DVs before any data-file statistics or sequence suffix can hide a
/// duplicate. It owns no per-data expanded delete lists.
#[derive(Debug)]
// Design: ADR-0164 (docs/adr/ADR-0164-iceberg-delete-closures-and-execution-unions.md)
pub struct DeleteCandidateIndex {
    domain: Arc<ReadDomain>,
    partition_bindings: BTreeMap<i32, PartitionTypeBinding>,
    global_equality: Vec<Arc<FrozenBucket>>,
    partition_equality: HashMap<TypedPartition, Vec<Arc<FrozenBucket>>>,
    partition_positions: HashMap<TypedPartition, Arc<FrozenBucket>>,
    path_positions: HashMap<Arc<str>, Arc<FrozenBucket>>,
    deletion_vectors: HashMap<Arc<str>, Arc<DeleteFact>>,
    size: CandidateIndexSize,
}

impl DeleteCandidateIndex {
    pub fn try_new(domain: Arc<ReadDomain>, observation: DeleteObservation) -> Result<Self> {
        let schema = domain.endpoint().schema()?;
        let partition_bindings = domain.endpoint().bind_partition_specs()?;
        let mut validated_groups = HashSet::new();
        let mut groups: HashMap<BucketKind, Vec<Arc<DeleteFact>>> = HashMap::new();
        let mut deletion_vectors = HashMap::new();
        for fact in observation.facts {
            validate_partition_binding(&partition_bindings, fact.partition())?;
            let kind = match fact.kind() {
                DeleteKind::Equality(fields) => {
                    if validated_groups.insert(fields.clone()) {
                        fields.validate_schema(&schema)?;
                    }
                    BucketKind::Equality(EqualityBucketKey {
                        scope: fact.equality_scope(),
                        fields: fields.clone(),
                    })
                }
                DeleteKind::Position {
                    exact_target: Some(path),
                } => BucketKind::PathPositions(Arc::clone(path)),
                DeleteKind::Position { exact_target: None } => {
                    BucketKind::PartitionPositions(fact.partition().clone())
                }
                DeleteKind::DeletionVector { exact_target } => {
                    if deletion_vectors
                        .insert(Arc::clone(exact_target), fact)
                        .is_some()
                    {
                        return Err(Error::new(
                            Kind::MultipleDeletionVectors,
                            "closed observation contains multiple DVs for one target",
                        ));
                    }
                    continue;
                }
            };
            groups.entry(kind).or_default().push(fact);
        }
        let mut result = Self {
            domain,
            partition_bindings,
            global_equality: Vec::new(),
            partition_equality: HashMap::new(),
            partition_positions: HashMap::new(),
            path_positions: HashMap::new(),
            size: CandidateIndexSize {
                deletion_vector_targets: deletion_vectors.len(),
                ..Default::default()
            },
            deletion_vectors,
        };
        for (kind, members) in groups {
            result.size.member_references += members.len();
            result.size.buckets += 1;
            let bucket = FrozenBucket::new(kind.clone(), members);
            match kind {
                BucketKind::Equality(EqualityBucketKey {
                    scope: EqualityScope::Global,
                    ..
                }) => result.global_equality.push(bucket),
                BucketKind::Equality(EqualityBucketKey {
                    scope: EqualityScope::Partition(partition),
                    ..
                }) => result
                    .partition_equality
                    .entry(partition)
                    .or_default()
                    .push(bucket),
                BucketKind::PartitionPositions(partition) => {
                    result.partition_positions.insert(partition, bucket);
                }
                BucketKind::PathPositions(path) => {
                    result.path_positions.insert(path, bucket);
                }
            }
        }
        fn order(buckets: &mut [Arc<FrozenBucket>]) {
            buckets.sort_by(|a, b| {
                let (BucketKind::Equality(a), BucketKind::Equality(b)) = (a.kind(), b.kind())
                else {
                    unreachable!("only equality buckets are ordered")
                };
                a.fields
                    .fields()
                    .iter()
                    .map(|(id, _)| id)
                    .cmp(b.fields.fields().iter().map(|(id, _)| id))
            });
        }
        order(&mut result.global_equality);
        for buckets in result.partition_equality.values_mut() {
            order(buckets);
        }
        Ok(result)
    }

    pub fn domain(&self) -> &Arc<ReadDomain> {
        &self.domain
    }
    pub const fn size(&self) -> CandidateIndexSize {
        self.size
    }

    /// O(relevant buckets × log(bucket length)) metadata lookup, plus O(B)
    /// view storage. Expanding the view for inline wire remains a separate cost.
    pub fn for_data(&self, data: &DataFileFact) -> Result<DeleteSet> {
        validate_partition_binding(&self.partition_bindings, data.partition())?;
        let mut cost = CandidateLookupCost::default();
        let position = if let Some(dv) = self.deletion_vectors.get(data.path()) {
            if dv.sequence() < data.sequence() {
                return Err(Error::new(
                    Kind::DeletionVectorOlderThanData,
                    format!(
                        "DV sequence {} is older than exact target {} sequence {}",
                        dv.sequence().get(),
                        data.path(),
                        data.sequence().get()
                    ),
                ));
            }
            cost.candidate_members += 1;
            PositionSource::OneDv(Arc::clone(dv))
        } else {
            let mut views = Vec::with_capacity(2);
            for bucket in [
                self.path_positions.get(data.path()),
                self.partition_positions.get(data.partition()),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(view) = bucket.suffix(data.sequence(), &mut cost) {
                    views.push(view);
                }
            }
            if views.is_empty() {
                PositionSource::None
            } else {
                PositionSource::Files(views)
            }
        };
        let mut equality = Vec::new();
        for bucket in self.global_equality.iter().chain(
            self.partition_equality
                .get(data.partition())
                .into_iter()
                .flatten(),
        ) {
            if let Some(view) = bucket.suffix(data.sequence(), &mut cost) {
                equality.push(view);
            }
        }
        Ok(DeleteSet::new(
            Arc::clone(&self.domain),
            Arc::new(data.clone()),
            position,
            equality,
            cost,
        ))
    }
}

/// Pure semantic membership. Raw observation validation happens first; this
/// predicate must not be used to filter malformed raw metadata into validity.
pub fn delete_applies(data: &DataFileFact, delete: &DeleteFact) -> Result<bool> {
    match delete.kind() {
        DeleteKind::Equality(_) => Ok(delete.sequence() > data.sequence()
            && (delete.partition().is_unpartitioned() || delete.partition() == data.partition())),
        DeleteKind::Position { exact_target } => Ok(delete.sequence() >= data.sequence()
            && match exact_target {
                Some(target) => target.as_ref() == data.path(),
                None => delete.partition() == data.partition(),
            }),
        DeleteKind::DeletionVector { exact_target } => {
            if exact_target.as_ref() != data.path() {
                return Ok(false);
            }
            if delete.sequence() < data.sequence() {
                return Err(Error::new(
                    Kind::DeletionVectorOlderThanData,
                    "DV is older than its exact target",
                ));
            }
            Ok(true)
        }
    }
}

#[derive(Clone, Debug)]
pub enum ValidatedPositionSource {
    None,
    Files(Vec<Arc<DeleteFact>>),
    OneDv(Arc<DeleteFact>),
}

#[derive(Clone, Debug)]
pub struct ValidatedClosure {
    pub domain: Arc<ReadDomain>,
    pub position: ValidatedPositionSource,
    pub equality: Vec<Arc<DeleteFact>>,
}

/// BE admission: validates exactly the received normalized members, including
/// the exclusive position shape. It cannot prove FE completeness and never
/// repairs a mixed closure, re-plans statistics, or silently drops a member
/// that fails scope/sequence/target binding. Exact normalized recall is safe.
pub fn validate_normalized_closure(
    expected_domain: &ReadDomain,
    received_domain: Arc<ReadDomain>,
    data: &DataFileFact,
    members: impl IntoIterator<Item = Arc<DeleteFact>>,
) -> Result<ValidatedClosure> {
    if expected_domain != received_domain.as_ref() {
        return Err(Error::new(
            Kind::DomainMismatch,
            "delete closure does not belong to the pinned read domain",
        ));
    }
    let schema = received_domain.endpoint().schema()?;
    let partition_bindings = received_domain.endpoint().bind_partition_specs()?;
    validate_partition_binding(&partition_bindings, data.partition())?;
    let mut seen = HashSet::new();
    let mut validated_groups = HashSet::new();
    let mut positions = Vec::new();
    let mut equality = Vec::new();
    let mut dv = None;
    for fact in members {
        validate_partition_binding(&partition_bindings, fact.partition())?;
        if !seen.insert(fact.application().clone()) {
            continue;
        }
        if !delete_applies(data, &fact)? {
            return Err(Error::new(
                Kind::InvalidClosure,
                "received delete member does not apply to this data file",
            ));
        }
        match fact.kind() {
            DeleteKind::Equality(fields) => {
                if validated_groups.insert(fields.clone()) {
                    fields.validate_schema(&schema)?;
                }
                equality.push(fact);
            }
            DeleteKind::Position { .. } => positions.push(fact),
            DeleteKind::DeletionVector { .. } => {
                if dv.replace(fact).is_some() {
                    return Err(Error::new(
                        Kind::MultipleDeletionVectors,
                        "normalized closure contains distinct DVs",
                    ));
                }
            }
        }
    }
    let position = match (dv, positions.is_empty()) {
        (Some(_), false) => {
            return Err(Error::new(
                Kind::InvalidClosure,
                "normalized closure mixes DV and position files",
            ));
        }
        (Some(dv), true) => ValidatedPositionSource::OneDv(dv),
        (None, false) => ValidatedPositionSource::Files(positions),
        (None, true) => ValidatedPositionSource::None,
    };
    Ok(ValidatedClosure {
        domain: received_domain,
        position,
        equality,
    })
}
