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

//! Selected owned relations through the original receiving grammar. No
//! provider interpretation or unchecked Relation clone is introduced here.

use super::*;

/// A loan of one original definition and its exact checked read/type owners.
/// Temporary single-definition index/Vec requests remain in the original
/// relation model even though they are dropped after moving out the value.
pub struct PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control> {
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    definition: &'wire wire::RelationDefinition,
    source: usize,
    limits: Option<RelationProjectionLimits>,
    facts: RelationProjectionFacts,
}
impl PreparedRelationMaterialization<'_, '_, '_, '_> {
    pub fn facts(&self) -> &RelationProjectionFacts {
        &self.facts
    }
    pub fn source_id(&self) -> u32 {
        self.definition.id
    }
    pub fn emit(self) -> Result<p::Relation, Error> {
        let mut work =
            CompileCheckpoints::try_new(self.namespace.original_control(), CompilePhase::Decode)?;
        let result = self.emit_observed(&mut work);
        finish(result, work)
    }
    /// Consume the same selected definition in the caller's original scope.
    /// Preparation already admitted both passes; replacement snapshots never
    /// subtract that known work ceiling during materialization.
    pub fn emit_in(
        self,
        admit: &mut impl FnMut(&RelationProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<p::Relation, Error> {
        check_control(self.namespace.original_control(), work)?;
        let floor = self.facts;
        let mut update = |next: &RelationProjectionFacts| {
            let mut next = *next;
            next.definition_count = next.definition_count.max(floor.definition_count);
            next.schema_field_count = next.schema_field_count.max(floor.schema_field_count);
            next.predicate_guarantee_count = next
                .predicate_guarantee_count
                .max(floor.predicate_guarantee_count);
            next.metadata_kind_bytes = next.metadata_kind_bytes.max(floor.metadata_kind_bytes);
            next.coverage_bytes = next.coverage_bytes.max(floor.coverage_bytes);
            next.allocation_requests_upper_bound = next
                .allocation_requests_upper_bound
                .max(floor.allocation_requests_upper_bound);
            next.allocation_request_bytes_upper_bound = next
                .allocation_request_bytes_upper_bound
                .max(floor.allocation_request_bytes_upper_bound);
            next.coexisting_source_and_request_bytes_upper_bound = next
                .coexisting_source_and_request_bytes_upper_bound
                .max(floor.coexisting_source_and_request_bytes_upper_bound);
            next.cumulative_work_upper_bound = next
                .cumulative_work_upper_bound
                .max(floor.cumulative_work_upper_bound);
            admit(&next)
        };
        let mut decoded = decode_core(
            std::slice::from_ref(self.definition),
            self.namespace.reads,
            self.namespace.types,
            self.source,
            self.limits,
            Some(&mut update),
            work,
        )?;
        let relation = decoded.relations.pop();
        work.step()?;
        relation.ok_or_else(|| invalid("selected relation materialization produced no value"))
    }
    pub(crate) fn emit_observed(
        self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<p::Relation, Error> {
        let mut decoded = decode_core(
            std::slice::from_ref(self.definition),
            self.namespace.reads,
            self.namespace.types,
            self.source,
            self.limits,
            None,
            work,
        )?;
        let relation = decoded.relations.pop();
        work.step()?;
        relation.ok_or_else(|| invalid("selected relation materialization produced no value"))
    }
}

pub fn prepare_relation_materialization<'borrow, 'loan, 'wire, 'control>(
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    id: u32,
    source: usize,
    limits: impl Into<Option<RelationProjectionLimits>>,
) -> Result<PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control>, Error> {
    let limits = limits.into();
    let mut work = CompileCheckpoints::try_new(namespace.original_control(), CompilePhase::Decode)?;
    let result = prepare_observed(namespace, id, source, limits, &mut work);
    finish(result, work)
}

/// The combined route shares one checkpoint meter across preparation and
/// materialization. The caller supplies the complete retained union invoice.
pub fn materialize_relation(
    namespace: &DecodedRelations<'_, '_, '_>,
    id: u32,
    source: usize,
    limits: impl Into<Option<RelationProjectionLimits>>,
) -> Result<p::Relation, Error> {
    let limits = limits.into();
    let mut work = CompileCheckpoints::try_new(namespace.original_control(), CompilePhase::Decode)?;
    let result = prepare_observed(namespace, id, source, limits, &mut work)
        .and_then(|prepared| prepared.emit_observed(&mut work));
    finish(result, work)
}

pub(crate) fn prepare_observed<'borrow, 'loan, 'wire, 'control>(
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    id: u32,
    source: usize,
    limits: Option<RelationProjectionLimits>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control>, Error> {
    prepare_core(namespace, id, source, limits, None, work)
}

pub fn prepare_relation_materialization_in<'borrow, 'loan, 'wire, 'control>(
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    id: u32,
    source: usize,
    limits: impl Into<Option<RelationProjectionLimits>>,
    admit: &mut impl FnMut(&RelationProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control>, Error> {
    let limits = limits.into();
    check_control(namespace.original_control(), work)?;
    prepare_core(namespace, id, source, limits, Some(admit), work)
}

fn numeric_add(left: usize, right: usize, observed: bool) -> Result<usize, Error> {
    if observed {
        left.checked_add(right)
            .ok_or_else(|| CompileControlError::ResourceExhausted.into())
    } else {
        add(left, right)
    }
}
fn numeric_mul(left: usize, right: usize, observed: bool) -> Result<usize, Error> {
    if observed {
        left.checked_mul(right)
            .ok_or_else(|| CompileControlError::ResourceExhausted.into())
    } else {
        mul(left, right)
    }
}
fn prepare_core<'borrow, 'loan, 'wire, 'control, 'admit>(
    namespace: &'borrow DecodedRelations<'loan, 'wire, 'control>,
    id: u32,
    source: usize,
    limits: Option<RelationProjectionLimits>,
    mut admit: Option<&'admit mut Admit<'admit>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationMaterialization<'borrow, 'loan, 'wire, 'control>, Error> {
    // Admission for the actual retained namespace floor walk and sparse index
    // lookup precedes either operation. It is not charged as output memory.
    let observed = admit.is_some();
    let lookup = if observed {
        crate::btree_resources_v2::lookup_work_typed(namespace.source_count()).map_err(|error| {
            match error {
                crate::btree_resources_v2::BTreeResourceError::SourceModel(message) => {
                    invalid(message)
                }
                crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
                    Error::Control(CompileControlError::ResourceExhausted)
                }
            }
        })?
    } else {
        crate::btree_resources_v2::lookup_work(namespace.source_count()).map_err(invalid)?
    };
    let overhead = numeric_add(
        1024,
        numeric_add(
            numeric_mul(namespace.source_count(), 8, observed)?,
            lookup,
            observed,
        )?,
        observed,
    )?;
    let mut root_floor = None;
    if let Some(callback) = admit.as_mut() {
        let mut root = Model {
            work: overhead,
            admission: Some(Admission {
                definitions: 1,
                source,
                limits,
                callback: &mut **callback,
            }),
            ..Model::default()
        };
        root.request(bytes::<usize>(1)?)?;
        root.request(bytes::<p::Relation>(1)?)?;
        root.prefix()?;
        root_floor = Some(root.numerical_facts(1, source)?);
    }
    cap(overhead, limits.map(|limits| limits.max_work), work)?;
    floor(source, namespace.retained_floor_observed(work)?, work)?;
    let at = namespace
        .indices
        .find(id, |at| namespace.wire[at].id, work)?;
    work.step()?;
    let at = at.ok_or_else(|| invalid("selected relation ID is absent from original namespace"))?;
    let definition = &namespace.wire[at];
    let mut facts = if let Some(callback) = admit.as_mut() {
        let mut update = |next: &RelationProjectionFacts| {
            let mut next = *next;
            next.cumulative_work_upper_bound = next
                .cumulative_work_upper_bound
                .checked_mul(2)
                .and_then(|value| value.checked_add(overhead))
                .ok_or(CompileControlError::ResourceExhausted)?;
            if let Some(root) = root_floor {
                next.allocation_requests_upper_bound = next
                    .allocation_requests_upper_bound
                    .max(root.allocation_requests_upper_bound);
                next.allocation_request_bytes_upper_bound = next
                    .allocation_request_bytes_upper_bound
                    .max(root.allocation_request_bytes_upper_bound);
                next.coexisting_source_and_request_bytes_upper_bound = next
                    .coexisting_source_and_request_bytes_upper_bound
                    .max(root.coexisting_source_and_request_bytes_upper_bound);
            }
            if limits.is_some_and(|limits| next.cumulative_work_upper_bound > limits.max_work) {
                return Err(CompileControlError::ResourceExhausted);
            }
            callback(&next)
        };
        preflight_decode_observed(
            std::slice::from_ref(definition),
            namespace.reads,
            namespace.types,
            source,
            limits,
            Some(&mut update),
            work,
        )?
    } else {
        preflight_decode_observed(
            std::slice::from_ref(definition),
            namespace.reads,
            namespace.types,
            source,
            limits,
            None,
            work,
        )?
    };
    // The original preflight bound includes its own emission. A consuming
    // materialization reuses that same author and grammar, including its
    // preflight, so charge two complete passes without copying formulas.
    facts.cumulative_work_upper_bound = numeric_add(
        overhead,
        numeric_mul(facts.cumulative_work_upper_bound, 2, observed)?,
        observed,
    )?;
    cap(
        facts.cumulative_work_upper_bound,
        limits.map(|limits| limits.max_work),
        work,
    )?;
    Ok(PreparedRelationMaterialization {
        namespace,
        definition,
        source,
        limits,
        facts,
    })
}
