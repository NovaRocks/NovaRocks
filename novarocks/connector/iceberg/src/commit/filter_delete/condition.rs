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

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::iceberg::expr::{
    BinaryExpression, BoundPredicate, BoundReference, SetExpression, UnaryExpression,
};
use crate::iceberg::spec::{
    DataContentType, Datum, FormatVersion, ManifestFile, PrimitiveType, TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::partition_types::legal_partition_type_join;
use crate::scalar_integer_domain::metadata_declarations;

use super::super::conflict::CandidateFile;
use super::super::delete_condition::{DeleteCondition, IcebergDeleteConditionFact};
use super::super::model::CommitOperation;
use super::super::new_manifests::commit_validation_error_for;
use super::FileVerdict;
use super::metrics::MetricEvidence;
use super::projection::PartitionProjection;

pub(super) struct ConditionJudge<'a> {
    never: bool,
    fields: Vec<FieldJudge<'a>>,
    specs: BTreeMap<i32, usize>,
}

struct FieldJudge<'a> {
    id: i32,
    primitive: PrimitiveType,
    alternatives: Vec<Vec<LeafJudge<'a>>>,
}

struct LeafJudge<'a> {
    predicate: Cow<'a, BoundPredicate>,
    projections: BTreeMap<i32, Vec<PartitionProjection>>,
}

impl<'a> ConditionJudge<'a> {
    pub(super) fn for_base(
        fact: &'a IcebergDeleteConditionFact,
        metadata: &TableMetadata,
        operation: CommitOperation,
    ) -> Result<Self> {
        if fact.table_uuid != metadata.uuid() {
            return Err(commit_validation_error_for(
                operation,
                "Filtered delete target table identity changed",
            ));
        }
        Self::bind(&fact.condition, metadata, operation)
    }

    pub(super) fn bind(
        condition: &'a DeleteCondition,
        metadata: &TableMetadata,
        operation: CommitOperation,
    ) -> Result<Self> {
        let mut fields = Vec::new();
        if let DeleteCondition::Fields(conditions) = condition {
            let schema = metadata.current_schema();
            let declarations = metadata_declarations(metadata).map_err(|error| {
                Error::new(ErrorKind::DataInvalid, error.message()).with_source(error)
            })?;
            for field in conditions {
                let id = field.field_id;
                let conflict = || {
                    commit_validation_error_for(
                        operation,
                        format!(
                            "Cannot preserve filtered delete field {id} in the current table schema"
                        ),
                    )
                };
                let target = schema.as_struct().field_by_id(id).ok_or_else(conflict)?;
                let primitive = target.field_type.as_primitive_type().ok_or_else(conflict)?;
                if !is_order_preserving_promotion(
                    metadata.format_version(),
                    &field.primitive,
                    primitive,
                ) || declarations.get(&id).copied() != field.integer_domain
                {
                    return Err(conflict());
                }
                let promoted = if field.primitive == *primitive {
                    None
                } else {
                    let accessor = schema.accessor_by_field_id(id).ok_or_else(conflict)?;
                    Some(BoundReference::new(
                        target.name.clone(),
                        target.clone(),
                        accessor,
                    ))
                };
                let alternatives = field
                    .alternatives
                    .iter()
                    .map(|arm| {
                        arm.iter()
                            .map(|leaf| {
                                let predicate = match &promoted {
                                    Some(term) => Cow::Owned(
                                        promote_leaf(leaf, term, primitive).ok_or_else(conflict)?,
                                    ),
                                    None => Cow::Borrowed(leaf),
                                };
                                Ok(LeafJudge {
                                    predicate,
                                    projections: BTreeMap::new(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                fields.push(FieldJudge {
                    id,
                    primitive: primitive.clone(),
                    alternatives,
                });
            }
        }
        Ok(Self {
            never: matches!(condition, DeleteCondition::Never),
            fields,
            specs: BTreeMap::new(),
        })
    }

    pub(super) fn prepare_specs(
        &mut self,
        metadata: &TableMetadata,
        spec_ids: impl IntoIterator<Item = i32>,
        active: &impl Fn() -> Result<()>,
    ) -> Result<()> {
        if self.fields.is_empty() {
            return Ok(());
        }
        for spec_id in spec_ids {
            active()?;
            let std::collections::btree_map::Entry::Vacant(slot) = self.specs.entry(spec_id) else {
                continue;
            };
            let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Unknown delete manifest partition spec {spec_id}"),
                )
            })?;
            slot.insert(spec.fields().len());
            for field in &mut self.fields {
                let partitions: Vec<_> = spec
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, part)| part.source_id == field.id)
                    .collect();
                if partitions.is_empty() {
                    continue;
                }
                // A historical transform such as bucket can change when its
                // source type changes. Only project when the retained source
                // declarations preserve the ordering and transform meaning.
                let mut stable = true;
                for schema in metadata.schemas_iter() {
                    active()?;
                    if let Some(source) = schema.field_by_id(field.id) {
                        stable &= source
                            .field_type
                            .as_primitive_type()
                            .is_some_and(|previous| {
                                is_order_preserving_promotion(
                                    metadata.format_version(),
                                    previous,
                                    &field.primitive,
                                )
                            });
                    }
                }
                if !stable {
                    continue;
                }
                for leaf in field.alternatives.iter_mut().flatten() {
                    active()?;
                    let mut projections = Vec::new();
                    for &(ordinal, partition) in &partitions {
                        if let Some(projection) = PartitionProjection::new(
                            ordinal,
                            partition,
                            &field.primitive,
                            &leaf.predicate,
                        )? {
                            projections.push(projection);
                        }
                    }
                    if !projections.is_empty() {
                        leaf.projections.insert(spec_id, projections);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn manifest_may_match(&self, manifest: &ManifestFile) -> bool {
        if self.never {
            return false;
        }
        let Some(summaries) = manifest.partitions.as_deref() else {
            return true;
        };
        let spec = manifest.partition_spec_id;
        if self.specs.get(&spec) != Some(&summaries.len()) {
            return true;
        }
        self.fields.iter().all(|field| {
            field.alternatives.iter().any(|arm| {
                arm.iter().all(|leaf| {
                    leaf.projections.get(&spec).is_none_or(|projections| {
                        projections
                            .iter()
                            .all(|projection| projection.summary_may_match(summaries))
                    })
                })
            })
        })
    }

    pub(super) fn file_verdict(&self, candidate: &CandidateFile<'_>) -> Result<FileVerdict> {
        let file = candidate.file;
        if file.record_count() == 0 {
            return Ok(FileVerdict::All);
        }
        if self.never {
            return Ok(FileVerdict::None);
        }
        if self.fields.is_empty() {
            return Ok(FileVerdict::All);
        }
        let values = file.partition().fields();
        let spec = candidate.spec_id;
        if self.specs.get(&spec) != Some(&values.len()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid partition tuple for spec {spec}: {}",
                    file.file_path()
                ),
            ));
        }
        let equality_ids = if file.content_type() == DataContentType::EqualityDeletes {
            file.equality_ids()
        } else {
            None
        };
        let mut result = FileVerdict::All;
        for field in &self.fields {
            let metrics_apply = file.content_type() == DataContentType::Data
                || equality_ids
                    .as_deref()
                    .is_some_and(|ids| ids.contains(&field.id));
            let mut metrics = None;
            let mut field_verdict = FileVerdict::None;
            for arm in &field.alternatives {
                let mut arm_verdict = FileVerdict::All;
                for leaf in arm {
                    let mut partition = FileVerdict::Partial;
                    if let Some(projections) = leaf.projections.get(&spec) {
                        for projection in projections {
                            match projection.file_verdict(values)? {
                                FileVerdict::None => {
                                    partition = FileVerdict::None;
                                    break;
                                }
                                FileVerdict::All => partition = FileVerdict::All,
                                FileVerdict::Partial => {}
                            }
                        }
                    }
                    let verdict = if partition == FileVerdict::Partial && metrics_apply {
                        metrics
                            .get_or_insert_with(|| {
                                MetricEvidence::new(
                                    file,
                                    field.id,
                                    &field.primitive,
                                    candidate.metadata.format_version(),
                                )
                            })
                            .verdict(&leaf.predicate)
                    } else {
                        partition
                    };
                    arm_verdict = arm_verdict.and(verdict);
                    if arm_verdict == FileVerdict::None {
                        break;
                    }
                }
                field_verdict = field_verdict.or(arm_verdict);
                if field_verdict == FileVerdict::All {
                    break;
                }
            }
            result = result.and(field_verdict);
            if result == FileVerdict::None {
                break;
            }
        }
        Ok(result)
    }
}

impl FileVerdict {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::All, Self::All) => Self::All,
            _ => Self::Partial,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::None, Self::None) => Self::None,
            _ => Self::Partial,
        }
    }
}

/// These promotions preserve the ordering and literal meaning of every
/// predicate issued in E. Changing the date/timestamp domain can introduce
/// new values within a day and change the transform, so needs a different proof.
pub(super) fn is_order_preserving_promotion(
    version: FormatVersion,
    source: &PrimitiveType,
    target: &PrimitiveType,
) -> bool {
    source == target
        || (!matches!(source, PrimitiveType::Date)
            && legal_partition_type_join(version, source, target).as_ref() == Some(target))
}

fn promote_leaf(
    leaf: &BoundPredicate,
    term: &BoundReference,
    target: &PrimitiveType,
) -> Option<BoundPredicate> {
    let promote = |value: &Datum| value.clone().to(&target.clone().into()).ok();
    match leaf {
        BoundPredicate::Unary(expression) => Some(BoundPredicate::Unary(UnaryExpression::new(
            expression.op(),
            term.clone(),
        ))),
        BoundPredicate::Binary(expression) => Some(BoundPredicate::Binary(BinaryExpression::new(
            expression.op(),
            term.clone(),
            promote(expression.literal())?,
        ))),
        BoundPredicate::Set(expression) => Some(BoundPredicate::Set(SetExpression::new(
            expression.op(),
            term.clone(),
            expression
                .literals()
                .iter()
                .map(promote)
                .collect::<Option<_>>()?,
        ))),
        _ => None,
    }
}

pub(super) fn describe(condition: &DeleteCondition) -> String {
    let fields = match condition {
        DeleteCondition::AllRows => return "true".to_owned(),
        DeleteCondition::Never => return "false".to_owned(),
        DeleteCondition::Fields(fields) => fields,
    };
    let ids = fields
        .iter()
        .map(|field| field.field_id.to_string())
        .collect::<Vec<_>>();
    format!("fields [{}]", ids.join(", "))
}
