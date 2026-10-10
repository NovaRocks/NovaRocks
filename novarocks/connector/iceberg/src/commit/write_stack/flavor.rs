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

//! How each write-session flavor plans its logical branches.
//!
//! `begin_write` performs the external *reads* and freezes the material every
//! branch is cut from; this module is the pure decision that turns that
//! material into a branch list. Keeping it pure is what makes each flavor's
//! branch structure assertable without a catalog.
//!
//! The four flavors differ in exactly one thing — how many branches the write
//! needs and what each one accepts:
//!
//! * `Ordinary` keeps the canonical data-plus-delete shape unchanged.
//! * `ManagedPublication` publishes data only, and carries its technique and
//!   empty-input disposition (never its publication id) into the sealed
//!   session.
//! * `RowMutation` seals one branch per change-event route, each carrying the
//!   routing facts SQL needs. This is the port of the old
//!   `activate_row_mutation` route graph: a route's `cohort_id` becomes its
//!   [`WriteTargetOrdinal`](novarocks_spi::connector::write_stack::WriteTargetOrdinal)
//!   and its `preparation` is subsumed by the provider-private writer recipe,
//!   so neither comes back.
//! * `DistributedRewrite` seals one branch per frozen rewrite group: a data
//!   branch when the frozen operation republishes data files, a deletion-vector
//!   branch when it repacks position deletes.
//! * `CopyOnWrite` seals one data branch per frozen copy-on-write recipe: one
//!   per rewritten data file, plus a trailing append branch when the statement
//!   also has net-new rows.

use crate::commit::model::EntryIdentity;
use novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape;
use novarocks_spi::connector::write_stack::session::{
    ConnectorWriteProviderDerivedValue, ConnectorWriteRouteFacts, ConnectorWriteSelectionBinding,
    ConnectorWriteSelectionBindingRole,
};
use novarocks_spi::connector::{
    ConnectorDistributedRewriteShape, ConnectorError, ConnectorErrorKind,
    ConnectorManagedPublicationTechnique, ConnectorMutationMatchContract,
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteFieldBinding,
    ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteRouteId,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::commit::write_stack::copy_on_write::{IcebergCowBranchInput, IcebergCowBranchRecipe};
use crate::commit::write_stack::domain::{
    IcebergDataBranchRecipe, IcebergDocumentPublicationFacts, IcebergEqualityDeleteRecipe,
    IcebergFrozenRewriteBranchInput, IcebergManagedPublicationFacts, IcebergWriteBranch,
    IcebergWriteFlavor, IcebergWriteTableFacts, IcebergWriterOutput, invalid,
};
use crate::commit::write_stack::old_delete::IcebergOldDeleteMergeTarget;
use crate::commit::write_stack::planning::{
    IcebergDataBranchPlan, IcebergDeleteBranchPlan, IcebergEqualityDeleteBranchPlan,
    IcebergWriteBranchPlan,
};
use crate::delete_file::IcebergFileFormat;
use crate::distributed_rewrite::IcebergFrozenRewriteGroupV1;
use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};

/// The Iceberg row-identity column naming a physical row's file.
const ICEBERG_FILE_COL: &str = "_file";
/// The Iceberg row-identity column naming a physical row's position.
const ICEBERG_POS_COL: &str = "_pos";

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}

/// The frozen material every branch of one session is cut from.
///
/// Everything here was read and validated by `begin_write` before any branch
/// decision runs, so branch planning itself performs no I/O and can fail only
/// on the request's own shape.
#[derive(Clone, Debug)]
pub(crate) struct IcebergSessionMaterial {
    /// The exact table generation the session is frozen against.
    pub table: IcebergWriteTableFacts,
    /// The provider-signed input shape the caller's fields were bound into.
    pub input: ConnectorWriteInputShape,
    /// The physical output every data branch writes.
    pub data_output: IcebergWriterOutput,
    /// The partitioning and schema recipe every data branch writes through.
    pub data_recipe: IcebergDataBranchRecipe,
    /// The exact old delete artifacts a delete branch must supersede. Frozen by
    /// reference only: `begin_write` never opens one.
    pub merge_targets: Vec<IcebergOldDeleteMergeTarget>,
    /// The match key an equality-delete branch writes its file on. Present
    /// exactly when the signed input is an equality delete, because only then
    /// is there a branch to attach it to.
    pub equality: Option<IcebergEqualityDeleteRecipe>,
}

impl IcebergSessionMaterial {
    fn data_plan(&self, input: ConnectorWriteInputShape) -> IcebergDataBranchPlan {
        IcebergDataBranchPlan {
            output: self.data_output.clone(),
            recipe: self.data_recipe.clone(),
            input,
        }
    }

    fn delete_plan(
        &self,
        branch: IcebergWriteBranch,
        input: ConnectorWriteInputShape,
    ) -> Result<IcebergDeleteBranchPlan, ConnectorError> {
        self.rewrite_delete_plan(branch, input, self.merge_targets.clone())
    }

    /// A delete branch whose owned data files are stated rather than taken from
    /// the session-wide freeze.
    ///
    /// A distributed rewrite seals several delete branches, and each owns only
    /// its own group's data files; giving every branch the session-wide set
    /// would make each one claim every file, which the unique-owner proof
    /// refuses.
    fn rewrite_delete_plan(
        &self,
        branch: IcebergWriteBranch,
        input: ConnectorWriteInputShape,
        merge_targets: Vec<IcebergOldDeleteMergeTarget>,
    ) -> Result<IcebergDeleteBranchPlan, ConnectorError> {
        Ok(IcebergDeleteBranchPlan {
            branch,
            output: IcebergWriterOutput::try_new(
                delete_branch_format(branch)?,
                parquet::basic::Compression::SNAPPY,
                None,
            )?,
            merge_targets,
            input,
        })
    }
}

fn delete_branch_format(branch: IcebergWriteBranch) -> Result<IcebergFileFormat, ConnectorError> {
    match branch {
        IcebergWriteBranch::DeletionVector => Ok(IcebergFileFormat::Puffin),
        IcebergWriteBranch::PositionDelete => Ok(IcebergFileFormat::Parquet),
        // An equality delete is planned by `plan_equality_delete_branches`: it
        // owns no old-delete merge, so it is not a position-delete branch even
        // though both write delete files.
        IcebergWriteBranch::Data | IcebergWriteBranch::EqualityDelete => Err(invalid(format!(
            "Iceberg {} branch cannot be planned as a position-delete branch",
            branch.as_str()
        ))),
    }
}

/// One flavor's resolved branch structure.
#[derive(Clone, Debug)]
pub(crate) struct IcebergSessionFlavorPlan {
    /// The provider-private flavor the session commits as.
    pub flavor: IcebergWriteFlavor,
    /// Present exactly on a managed publication.
    pub publication: Option<IcebergManagedPublicationFacts>,
    /// Present exactly on an application-document publication.
    pub document_publication: Option<IcebergDocumentPublicationFacts>,
    /// Present exactly on a distributed rewrite: the exact input files each
    /// branch replaces, in the same order as `branches`.
    pub rewrite_inputs: Vec<IcebergFrozenRewriteBranchInput>,
    /// Present exactly on a copy-on-write mutation: what each branch replaces
    /// and what it re-reads, in the same order as `branches`.
    pub copy_on_write: Vec<IcebergCowBranchRecipe>,
    /// The branches, in ordinal order.
    pub branches: Vec<IcebergWriteBranchPlan>,
}

/// Plan the canonical data-plus-delete shape.
///
/// This is today's behaviour, unchanged: one data branch, plus one delete
/// branch when the signed input is row-level. Neither branch is routed, because
/// an ordinary write has exactly one thing to do with every row it is given.
pub(crate) fn plan_ordinary_branches(
    flavor: IcebergWriteFlavor,
    material: &IcebergSessionMaterial,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    // An equality delete writes delete files and nothing else, so it is the one
    // ordinary shape with no data branch at all. Sealing one beside it would
    // give SQL somewhere to send data rows the statement never produces.
    if matches!(
        material.input,
        ConnectorWriteInputShape::EqualityDelete { .. }
    ) {
        return plan_equality_delete_branches(flavor, material);
    }
    let mut branches = vec![IcebergWriteBranchPlan::Data {
        plan: material.data_plan(material.input.clone()),
        route: None,
    }];
    if let Some(branch) = ordinary_delete_branch(&material.input) {
        branches.push(IcebergWriteBranchPlan::Delete {
            plan: material.delete_plan(branch, material.input.clone())?,
            route: None,
        });
    }
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        document_publication: None,
        rewrite_inputs: Vec::new(),
        copy_on_write: Vec::new(),
        branches,
    })
}

/// Plan the one branch an equality delete seals.
///
/// The match key was resolved against the frozen table generation by
/// `begin_write`, because it needs the Iceberg schema to turn a column name into
/// a field id. Here it is only attached to the branch that writes it.
fn plan_equality_delete_branches(
    flavor: IcebergWriteFlavor,
    material: &IcebergSessionMaterial,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let recipe = material.equality.clone().ok_or_else(|| {
        invalid("Iceberg equality-delete session has no frozen equality match key")
    })?;
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        document_publication: None,
        rewrite_inputs: Vec::new(),
        copy_on_write: Vec::new(),
        branches: vec![IcebergWriteBranchPlan::EqualityDelete {
            plan: IcebergEqualityDeleteBranchPlan {
                output: IcebergWriterOutput::try_new(
                    IcebergFileFormat::Parquet,
                    parquet::basic::Compression::SNAPPY,
                    None,
                )?,
                recipe,
                input: material.input.clone(),
            },
            route: None,
        }],
    })
}

/// The delete branch an ordinary session's signed input calls for, if any.
pub(crate) const fn ordinary_delete_branch(
    input: &ConnectorWriteInputShape,
) -> Option<IcebergWriteBranch> {
    match input {
        ConnectorWriteInputShape::DeletionVector { .. } => Some(IcebergWriteBranch::DeletionVector),
        ConnectorWriteInputShape::PositionDelete { .. } => Some(IcebergWriteBranch::PositionDelete),
        _ => None,
    }
}

/// Plan a managed publication's branches.
///
/// The declared shape decides the structure, and only the structure. Both
/// shapes seal the *same* session: the technique, the empty-input disposition
/// and the durable provenance travel with it either way, because they decide
/// the single external commit and what an empty write means, and no writer
/// needs any of them.
///
/// * `Data` republishes rows wholesale, so it seals exactly one unrouted data
///   branch.
/// * `RowMutation` applies a change stream, so it seals the branches a row
///   mutation needs and SQL routes change events to them. Its branch structure
///   is planned by the same code an ordinary DML row mutation goes through --
///   a publication's change stream is not a second kind of row mutation, it is
///   the same one committed under a publication.
pub(crate) fn plan_managed_publication_branches(
    material: &IcebergSessionMaterial,
    publication: IcebergManagedPublicationFacts,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let branches =
        plan_publication_branch_list(material, publication.technique(), publication.shape())?;
    Ok(IcebergSessionFlavorPlan {
        flavor: IcebergWriteFlavor::ManagedPublication,
        publication: Some(publication),
        document_publication: None,
        rewrite_inputs: Vec::new(),
        copy_on_write: Vec::new(),
        branches,
    })
}

pub(crate) fn plan_document_publication_branches(
    material: &IcebergSessionMaterial,
    publication: IcebergDocumentPublicationFacts,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let branches =
        plan_publication_branch_list(material, publication.technique(), publication.shape())?;
    Ok(IcebergSessionFlavorPlan {
        flavor: IcebergWriteFlavor::ManagedPublication,
        publication: None,
        document_publication: Some(publication),
        rewrite_inputs: Vec::new(),
        copy_on_write: Vec::new(),
        branches,
    })
}

fn plan_publication_branch_list(
    material: &IcebergSessionMaterial,
    technique: ConnectorManagedPublicationTechnique,
    shape: ConnectorManagedPublicationShape,
) -> Result<Vec<IcebergWriteBranchPlan>, ConnectorError> {
    let branches = match shape {
        ConnectorManagedPublicationShape::Data => {
            if ordinary_delete_branch(&material.input).is_some() {
                return Err(unsupported(
                    "Iceberg managed publication publishes data files, not a row-level delete input",
                ));
            }
            vec![IcebergWriteBranchPlan::Data {
                plan: material.data_plan(material.input.clone()),
                route: None,
            }]
        }
        ConnectorManagedPublicationShape::InsertOnlyChangeStream => {
            // A full refresh republishes every row, so it has no change stream
            // to apply -- the same reason the row-mutation shape refuses it.
            if technique != ConnectorManagedPublicationTechnique::Incremental {
                return Err(unsupported(
                    "Iceberg full-refresh publication republishes rows and does not apply a change stream",
                ));
            }
            // An insert-only change stream supersedes nothing, so it seals a
            // data branch exactly like the ordinary publication shape. What it
            // needs beyond that is the route: its rows arrive as change events,
            // and SQL's change-stream compile requires every branch to declare
            // which effects it accepts.
            if ordinary_delete_branch(&material.input).is_some() {
                return Err(unsupported(
                    "Iceberg insert-only publication publishes data files, not a row-level delete input",
                ));
            }
            let ordinals = InputOrdinals::of(&material.input);
            let data_fields = material.input.fields();
            let route = route_facts(
                &material.table,
                RouteKey::new(
                    IcebergWriteFlavor::ManagedPublication,
                    IcebergWriteBranch::Data,
                    0,
                ),
                vec![ConnectorRowMutationEffect::Insert],
                &ordinals,
                data_fields.iter().copied(),
                &[],
                Vec::new(),
            )?;
            vec![IcebergWriteBranchPlan::Data {
                plan: material.data_plan(material.input.clone()),
                route: Some(route),
            }]
        }
        ConnectorManagedPublicationShape::RowMutation => {
            // A full refresh replaces every live row, so nothing it publishes
            // has a prior version for a change event to supersede. Applying a
            // change stream to it would stage deletes against an image the same
            // commit is about to discard.
            if technique != ConnectorManagedPublicationTechnique::Incremental {
                return Err(unsupported(
                    "Iceberg full-refresh publication republishes rows and does not apply a change stream",
                ));
            }
            // Copy-on-write rewrites whole data files, and a publication has no
            // match selection to cut those rewrites from. Left admitted it
            // would not fail closed: the publication's commit op would resolve
            // to a plain append and publish every after-image while the
            // before-images stayed live. It is refused here, before the row
            // mutation is planned, so the refusal names the publication's own
            // reason rather than the flavor-admission one.
            if let ConnectorWriteInputShape::RowLineage {
                row_identity_fields,
                ..
            } = &material.input
                && identity_names_are(
                    row_identity_fields,
                    ICEBERG_ROW_ID_COL,
                    ICEBERG_LAST_UPDATED_SEQ_COL,
                )
            {
                return Err(unsupported(
                    "Iceberg publication change stream requires a `_file`/`_pos` row identity; a copy-on-write refresh is not supported",
                ));
            }
            plan_row_mutation_branches(material)?.branches
        }
    };
    Ok(branches)
}

/// One frozen rewrite group, together with what its branch's writer owns.
///
/// The group alone decides a data rewrite's branch. A position-delete rewrite
/// needs one thing more: its branch writes a deletion vector, and the writer
/// resolves a data file's partition and row count through the merge target the
/// session froze for it. Those come from the table metadata, so `begin_write`
/// freezes them and the pure branch planner only attaches them.
#[derive(Clone, Debug)]
pub(crate) struct IcebergFrozenRewriteBranch {
    /// The group this branch rewrites, and therefore exactly what its commit
    /// replaces.
    pub group: IcebergFrozenRewriteGroupV1,
    /// The data files this branch's delete writer owns, each frozen with *no*
    /// old reference to merge. Empty on a data rewrite, which seals no delete
    /// branch at all.
    ///
    /// A rewrite reads the old artifacts through its own scan and republishes
    /// their positions, so there is nothing left for the writer to merge —
    /// handing it the references would make it read the same Puffin blobs a
    /// second time, and its commit retires them either way.
    pub delete_targets: Vec<IcebergOldDeleteMergeTarget>,
}

/// Plan a distributed rewrite's branches: one branch per frozen group.
///
/// The ordinal is the group's query-local name. Which *kind* of branch it seals
/// is the frozen operation's decision, not the input's: a data rewrite seals a
/// data branch and refuses a row-level delete input, a position-delete rewrite
/// seals a deletion-vector branch and refuses anything else. Reading it off the
/// input alone would let a caller turn one rewrite into the other by signing a
/// different shape, and its commit would then retire the wrong half of the
/// frozen set.
///
/// Nothing about the group is copied into a data branch's writer recipe — a
/// rewrite writer's job is to write new data files for the rows it is handed,
/// and which old files those rows came from is a planning fact the frontend
/// already holds.
pub(crate) fn plan_distributed_rewrite_branches(
    material: &IcebergSessionMaterial,
    shape: ConnectorDistributedRewriteShape,
    groups: &[IcebergFrozenRewriteBranch],
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let flavor = match shape {
        ConnectorDistributedRewriteShape::DataFiles { .. } => {
            if ordinary_delete_branch(&material.input).is_some() {
                return Err(unsupported(
                    "Iceberg distributed rewrite republishes data files, not a row-level delete input",
                ));
            }
            IcebergWriteFlavor::DistributedRewrite
        }
        ConnectorDistributedRewriteShape::PositionDeletes { .. } => {
            // A repacked deletion vector is a v3 Puffin artifact; on an earlier
            // format version the rewrite would have to write a Parquet position
            // delete instead, which replaces nothing the plan selected.
            if material.table.format_version() < 3 {
                return Err(unsupported(
                    "Iceberg position-delete rewrite requires a format-version 3 table",
                ));
            }
            if !matches!(
                material.input,
                ConnectorWriteInputShape::DeletionVector { .. }
            ) {
                return Err(unsupported(
                    "Iceberg position-delete rewrite republishes deletion vectors, not a data input",
                ));
            }
            IcebergWriteFlavor::DistributedRewritePositionDeletes
        }
    };
    // A rewrite that found nothing to rewrite still seals one branch: the
    // session has to exist so it can terminate, and its empty prepared set is
    // what makes it a no-op rather than a failure. Its frozen input is empty
    // for the same reason, and the branch count stays one per input.
    let empty = [IcebergFrozenRewriteBranch {
        group: IcebergFrozenRewriteGroupV1::default(),
        delete_targets: Vec::new(),
    }];
    let groups = if groups.is_empty() {
        &empty[..]
    } else {
        groups
    };
    let rewrite_inputs = groups
        .iter()
        .map(|branch| frozen_rewrite_branch_input(shape, &branch.group))
        .collect::<Result<Vec<_>, _>>()?;
    let branches = groups
        .iter()
        .map(|branch| match shape {
            ConnectorDistributedRewriteShape::DataFiles { .. } => {
                Ok(IcebergWriteBranchPlan::Data {
                    plan: material.data_plan(material.input.clone()),
                    route: None,
                })
            }
            ConnectorDistributedRewriteShape::PositionDeletes { .. } => {
                Ok(IcebergWriteBranchPlan::Delete {
                    plan: material.rewrite_delete_plan(
                        IcebergWriteBranch::DeletionVector,
                        material.input.clone(),
                        branch.delete_targets.clone(),
                    )?,
                    route: None,
                })
            }
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        document_publication: None,
        rewrite_inputs,
        copy_on_write: Vec::new(),
        branches,
    })
}

/// The exact live files one frozen group's branch replaces.
///
/// A data rewrite retires the group's data files together with the delete
/// artifacts the group was proven to own, because the rows those deletions
/// removed are already absent from the files the branch writes. Leaving an
/// owned delete artifact live would re-apply it to rows that no longer exist.
///
/// A position-delete rewrite retires only the delete artifacts its group
/// selected. Its data files stay live and are named anyway, because they are
/// what the replacement deletion vectors must cover: the commit proves the
/// staged artifacts reference exactly this set.
fn frozen_rewrite_branch_input(
    shape: ConnectorDistributedRewriteShape,
    group: &IcebergFrozenRewriteGroupV1,
) -> Result<IcebergFrozenRewriteBranchInput, ConnectorError> {
    let delete_paths = match shape {
        ConnectorDistributedRewriteShape::DataFiles { .. } => {
            group.owned_data_delete_files.iter().cloned().collect()
        }
        ConnectorDistributedRewriteShape::PositionDeletes { .. } => group
            .selected_position_delete_files
            .iter()
            .cloned()
            .collect(),
    };
    IcebergFrozenRewriteBranchInput::try_new(
        group
            .data_files
            .iter()
            .map(|file| EntryIdentity::DataFile {
                path: file.path.clone(),
            })
            .collect(),
        delete_paths,
    )
}

/// How many branches a row mutation needs, and what each one accepts.
///
/// The signed input decides it, because the input is what says which halves of
/// a change event SQL can supply:
///
/// * a position-delete or deletion-vector input carries only a row identity, so
///   the mutation is delete-only and needs exactly one delete branch;
/// * a row-lineage input whose identity is `_file`/`_pos` carries both halves,
///   so it is a merge-on-read mutation: the delete branch consumes the
///   before-image identity and the data branch consumes the after-image values.
///   A `Replace` reaches both, which is why both accept it;
/// * a row-lineage input whose identity is `_row_id`/`_last_updated_sequence_number`
///   is a copy-on-write mutation: one data branch rewrites whole files, so it
///   sees every change event that touches a file it rewrites.
pub(crate) fn plan_row_mutation_branches(
    material: &IcebergSessionMaterial,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let ordinals = InputOrdinals::of(&material.input);
    match &material.input {
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
        | ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => {
            let branch = ordinary_delete_branch(&material.input)
                .ok_or_else(|| invalid("Iceberg row-level input has no delete branch"))?;
            let flavor = match branch {
                IcebergWriteBranch::DeletionVector => IcebergWriteFlavor::RowMutationDeletionVector,
                _ => IcebergWriteFlavor::RowMutationPositionDelete,
            };
            let route = route_facts(
                &material.table,
                RouteKey::new(flavor, branch, 0),
                vec![ConnectorRowMutationEffect::Delete],
                &ordinals,
                identity_fields.iter().chain(partition_source_fields),
                partition_source_fields,
                Vec::new(),
            )?;
            Ok(IcebergSessionFlavorPlan {
                flavor,
                publication: None,
                document_publication: None,
                rewrite_inputs: Vec::new(),
                copy_on_write: Vec::new(),
                branches: vec![IcebergWriteBranchPlan::Delete {
                    plan: material.delete_plan(branch, material.input.clone())?,
                    route: Some(route),
                }],
            })
        }
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => {
            if identity_names_are(row_identity_fields, ICEBERG_FILE_COL, ICEBERG_POS_COL) {
                plan_merge_on_read_branches(material, data_fields, row_identity_fields, &ordinals)
            } else if identity_names_are(
                row_identity_fields,
                ICEBERG_ROW_ID_COL,
                ICEBERG_LAST_UPDATED_SEQ_COL,
            ) {
                // A copy-on-write mutation cannot be planned from the input
                // alone: its branch count follows the files its match selection
                // touched, and only the copy-on-write flavor carries that
                // selection. Admitting it here would seal one branch for a
                // statement that rewrites several files and silently attribute
                // every file's replacement rows to the first one.
                Err(unsupported(
                    "Iceberg copy-on-write row mutation must be admitted through the copy-on-write session flavor",
                ))
            } else {
                Err(unsupported(
                    "Iceberg row mutation requires a `_file`/`_pos` or `_row_id`/`_last_updated_sequence_number` row identity",
                ))
            }
        }
        ConnectorWriteInputShape::Data { .. } | ConnectorWriteInputShape::EqualityDelete { .. } => {
            Err(unsupported(
                "Iceberg row mutation requires a position-delete, deletion-vector, or row-lineage input",
            ))
        }
    }
}

/// Merge-on-read: a deletion-vector branch and a data branch over one input.
///
/// Merge-on-read is admitted only on a row-lineage table, so its delete half is
/// the v3 deletion-vector writer rather than a Parquet position-delete file.
fn plan_merge_on_read_branches(
    material: &IcebergSessionMaterial,
    data_fields: &[ConnectorWriteFieldBinding],
    row_identity_fields: &[ConnectorWriteFieldBinding],
    ordinals: &InputOrdinals,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    if material.table.format_version() < 3 {
        return Err(unsupported(
            "Iceberg merge-on-read row mutation requires a format-version 3 table",
        ));
    }
    let flavor = IcebergWriteFlavor::RowMutationDeletionVector;
    let data_input = ConnectorWriteInputShape::Data {
        fields: data_fields.to_vec(),
    };
    data_input.validate()?;
    let delete_input = ConnectorWriteInputShape::DeletionVector {
        identity_fields: row_identity_fields.to_vec(),
        partition_source_fields: Vec::new(),
    };
    delete_input.validate()?;
    let data_route = route_facts(
        &material.table,
        RouteKey::new(flavor, IcebergWriteBranch::Data, 0),
        vec![
            ConnectorRowMutationEffect::Replace,
            ConnectorRowMutationEffect::Insert,
        ],
        ordinals,
        data_fields.iter(),
        &[],
        Vec::new(),
    )?;
    // Every change event that touches one old data file has to reach one
    // physical delete writer: Iceberg permits a single deletion vector per data
    // file, and two writers each staging one for the same file is refused as a
    // corrupt prepared set. Declaring no partition field made that true the
    // blunt way -- one writer for the whole branch -- so a merge-on-read
    // mutation over a partitioned table gathered every delete onto one driver.
    //
    // `_file` is the exclusivity key itself: it names the very file the deletion
    // vector supersedes, so hashing by it puts every row of a file on one writer
    // and spreads distinct files across all of them. Legacy hashed by the
    // *before-image* partition columns and relied on "a data file lives in
    // exactly one partition" to get the same guarantee -- this is that guarantee
    // without the indirection, and finer grained. It is also the only key
    // available: the signed row-lineage input carries after-image data columns
    // and the row identity, and no before-image partition column at all, so
    // hashing by partition here would route two rows of one file to two writers
    // whenever an update moves a row across partitions.
    let file_identity = row_identity_fields
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case(ICEBERG_FILE_COL))
        .cloned()
        .ok_or_else(|| {
            invalid("Iceberg merge-on-read row identity is missing its `_file` column")
        })?;
    let delete_route = route_facts(
        &material.table,
        RouteKey::new(flavor, IcebergWriteBranch::DeletionVector, 1),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace,
        ],
        ordinals,
        row_identity_fields.iter(),
        std::slice::from_ref(&file_identity),
        Vec::new(),
    )?;
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        document_publication: None,
        rewrite_inputs: Vec::new(),
        copy_on_write: Vec::new(),
        branches: vec![
            IcebergWriteBranchPlan::Data {
                plan: material.data_plan(data_input),
                route: Some(data_route),
            },
            IcebergWriteBranchPlan::Delete {
                plan: material.delete_plan(IcebergWriteBranch::DeletionVector, delete_input)?,
                route: Some(delete_route),
            },
        ],
    })
}

/// Copy-on-write: one data branch per frozen recipe.
///
/// A copy-on-write mutation rewrites whole data files, and each rewritten file
/// is its own branch: the branch re-reads exactly that file and its commit
/// replaces exactly that file. Sealing one branch for the whole mutation would
/// give every file's replacement rows the same writer, and the prepared write
/// set cannot notice — every fragment would name a target the session really
/// did seal. The branch order is the recipe order, so a branch's ordinal is the
/// only name it needs.
///
/// The net-new rows of a folded `MERGE` insert reach the trailing append branch,
/// which replaces nothing. Its route is the only one that accepts an insert, so
/// SQL cannot route a net-new row into a file rewrite.
pub(crate) fn plan_copy_on_write_branches(
    material: &IcebergSessionMaterial,
    recipes: &[IcebergCowBranchRecipe],
    match_contract: &ConnectorMutationMatchContract,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let flavor = IcebergWriteFlavor::RowMutationCopyOnWrite;
    if recipes.is_empty() {
        return Err(invalid(
            "Iceberg copy-on-write session must seal at least one frozen branch",
        ));
    }
    let ConnectorWriteInputShape::RowLineage { data_fields, .. } = &material.input else {
        return Err(unsupported(
            "Iceberg copy-on-write mutation requires a row-lineage input",
        ));
    };
    // A net-new row has no prior version, so the append branch writes the
    // target's own columns and lets the commit mint its lineage. A rewrite
    // branch re-emits rows that already have one, so it carries the lineage
    // columns through unchanged -- writing a fresh id for them would break the
    // row identity the rewrite exists to preserve.
    let append_input = ConnectorWriteInputShape::Data {
        fields: data_fields.clone(),
    };
    append_input.validate()?;
    let rewrite_selection_bindings: Arc<[ConnectorWriteSelectionBinding]> =
        copy_on_write_selection_bindings(&material.input, &material.input, match_contract)?.into();
    let append_selection_bindings: Arc<[ConnectorWriteSelectionBinding]> =
        copy_on_write_selection_bindings(&material.input, &append_input, match_contract)?.into();
    let ordinals = InputOrdinals::of(&material.input);
    let mut branches = Vec::with_capacity(recipes.len());
    for (index, recipe) in recipes.iter().enumerate() {
        let (effects, input, selection_bindings) = match recipe.input() {
            IcebergCowBranchInput::Rewrite { .. } => (
                vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                ],
                material.input.clone(),
                Arc::clone(&rewrite_selection_bindings),
            ),
            IcebergCowBranchInput::Append => (
                vec![ConnectorRowMutationEffect::Insert],
                append_input.clone(),
                Arc::clone(&append_selection_bindings),
            ),
        };
        let ordinal = u32::try_from(index)
            .map_err(|_| invalid("Iceberg copy-on-write session exceeds its branch bound"))?;
        let route = route_facts(
            &material.table,
            RouteKey::new(flavor, IcebergWriteBranch::Data, ordinal),
            effects,
            &ordinals,
            input.fields().into_iter(),
            &[],
            selection_bindings,
        )?;
        branches.push(IcebergWriteBranchPlan::Data {
            plan: material.data_plan(input),
            route: Some(route),
        });
    }
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        document_publication: None,
        rewrite_inputs: Vec::new(),
        copy_on_write: recipes.to_vec(),
        branches,
    })
}

/// Freeze the exact bridge from this session's writer tokens to the match
/// contract's independently signed selection tokens. Target columns bind by
/// provider-owned target ordinal; Iceberg lineage roles bind inside the
/// provider's own vocabulary. The returned order is the route input order.
fn copy_on_write_selection_bindings(
    session_input: &ConnectorWriteInputShape,
    route_input: &ConnectorWriteInputShape,
    contract: &ConnectorMutationMatchContract,
) -> Result<Vec<ConnectorWriteSelectionBinding>, ConnectorError> {
    let ConnectorWriteInputShape::RowLineage {
        data_fields,
        row_identity_fields,
    } = session_input
    else {
        return Err(invalid(
            "Iceberg copy-on-write selection bindings require a row-lineage session input",
        ));
    };
    if data_fields.len() != contract.after_fields().len() {
        return Err(invalid(
            "Iceberg copy-on-write writer data width differs from its signed after-image",
        ));
    }
    let compatible = |writer: &ConnectorWriteFieldBinding, selection: &arrow::datatypes::Field| {
        writer.field().data_type() == selection.data_type()
            && (!selection.is_nullable() || writer.field().is_nullable())
    };
    let mut by_writer = std::collections::BTreeMap::new();
    for (writer, selection) in data_fields.iter().zip(contract.after_fields()) {
        if !compatible(writer, selection.field()) {
            return Err(invalid(
                "Iceberg copy-on-write writer data type differs from its signed after-image",
            ));
        }
        by_writer.insert(
            writer.token(),
            ConnectorWriteSelectionBinding::new(
                writer.token(),
                selection.token(),
                selection.target_ordinal(),
                ConnectorWriteSelectionBindingRole::AfterImage,
            ),
        );
    }
    for writer in row_identity_fields {
        if writer.field().name() == ICEBERG_LAST_UPDATED_SEQ_COL {
            if writer.field().data_type() != &arrow::datatypes::DataType::Int64
                || !writer.field().is_nullable()
            {
                return Err(invalid(
                    "Iceberg copy-on-write inherited written-version field must be nullable INT64",
                ));
            }
            by_writer.insert(
                writer.token(),
                ConnectorWriteSelectionBinding::provider_derived(
                    writer.token(),
                    ConnectorWriteProviderDerivedValue::Inherit,
                ),
            );
            continue;
        }
        if writer.field().name() != ICEBERG_ROW_ID_COL {
            return Err(invalid(
                "Iceberg copy-on-write writer carries an unknown row-lineage role",
            ));
        }
        let mut matches = contract
            .identity_fields()
            .iter()
            .filter(|field| field.field().name() == writer.field().name());
        let selection = matches.next().ok_or_else(|| {
            invalid("Iceberg copy-on-write match contract omits a writer lineage role")
        })?;
        let nullable_is_compatible =
            !selection.field().is_nullable() || writer.field().is_nullable();
        let uniqueness_is_compatible = contract.uniqueness_tokens().contains(&selection.token());
        if matches.next().is_some()
            || writer.field().data_type() != selection.field().data_type()
            || !nullable_is_compatible
            || !uniqueness_is_compatible
        {
            return Err(invalid(
                "Iceberg copy-on-write writer lineage role is ambiguous or differs from its signed type and uniqueness role",
            ));
        }
        by_writer.insert(
            writer.token(),
            ConnectorWriteSelectionBinding::new(
                writer.token(),
                selection.token(),
                selection.source_ordinal(),
                ConnectorWriteSelectionBindingRole::Identity,
            ),
        );
    }
    route_input
        .fields()
        .into_iter()
        .map(|writer| {
            by_writer.get(&writer.token()).copied().ok_or_else(|| {
                invalid("Iceberg copy-on-write route carries a foreign writer token")
            })
        })
        .collect()
}

fn identity_names_are(fields: &[ConnectorWriteFieldBinding], first: &str, second: &str) -> bool {
    fields
        .iter()
        .any(|field| field.field().name().eq_ignore_ascii_case(first))
        && fields
            .iter()
            .any(|field| field.field().name().eq_ignore_ascii_case(second))
}

/// Where each signed field sits in the input row SQL builds.
///
/// One row carries every branch's columns; a branch reads its own subset, and
/// this is the only thing that says which positions those are.
struct InputOrdinals {
    tokens: Vec<ConnectorWriteFieldToken>,
}

impl InputOrdinals {
    fn of(input: &ConnectorWriteInputShape) -> Self {
        Self {
            tokens: input
                .fields()
                .into_iter()
                .map(ConnectorWriteFieldBinding::token)
                .collect(),
        }
    }

    fn ordinal_of(&self, token: ConnectorWriteFieldToken) -> Result<u32, ConnectorError> {
        let index = self
            .tokens
            .iter()
            .position(|candidate| *candidate == token)
            .ok_or_else(|| {
                invalid("Iceberg row-mutation route names a field the signed input does not carry")
            })?;
        u32::try_from(index)
            .map_err(|_| invalid("Iceberg row-mutation route input ordinal overflows"))
    }
}

/// Everything that names one branch inside its session.
///
/// The route key is derived from exactly these facts plus the table generation:
/// no operation, cohort, or attempt identity takes part, so two writes of the
/// same shape against the same generation route the same way and what
/// distinguishes them belongs to whoever owns their external effect.
#[derive(Clone, Copy)]
struct RouteKey {
    flavor: IcebergWriteFlavor,
    branch: IcebergWriteBranch,
    ordinal: u32,
}

impl RouteKey {
    const fn new(flavor: IcebergWriteFlavor, branch: IcebergWriteBranch, ordinal: u32) -> Self {
        Self {
            flavor,
            branch,
            ordinal,
        }
    }

    fn route_id(self, table: &IcebergWriteTableFacts) -> ConnectorWriteRouteId {
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.iceberg.write-stack.route.v1\0");
        hasher.update(table.table_uuid().as_bytes());
        hasher.update([0]);
        hasher.update(table.target_ref().as_bytes());
        hasher.update([0]);
        hasher.update(self.flavor.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.branch.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.ordinal.to_be_bytes());
        ConnectorWriteRouteId::from_bytes(hasher.finalize().into())
    }
}

/// Derive one branch's routing facts.
fn route_facts<'a>(
    table: &IcebergWriteTableFacts,
    key: RouteKey,
    accepted_effects: Vec<ConnectorRowMutationEffect>,
    ordinals: &InputOrdinals,
    consumed: impl Iterator<Item = &'a ConnectorWriteFieldBinding>,
    partition_source_fields: &[ConnectorWriteFieldBinding],
    selection_bindings: impl Into<Arc<[ConnectorWriteSelectionBinding]>>,
) -> Result<ConnectorWriteRouteFacts, ConnectorError> {
    let input_ordinals = consumed
        .map(|binding| {
            ordinals
                .ordinal_of(binding.token())
                .map(|ordinal| ConnectorMutationRouteInput::new(binding.token(), ordinal))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let partition_fields = partition_source_fields
        .iter()
        .map(ConnectorWriteFieldBinding::token)
        .collect::<Vec<_>>();
    ConnectorWriteRouteFacts::try_new(
        key.route_id(table),
        accepted_effects,
        input_ordinals,
        partition_fields,
        selection_bindings,
    )
}
