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

//! Provider-private logical target planning.
//!
//! Every Iceberg write flavor is expressed as a small, dense set of logical
//! write targets. The neutral write stack sees only ordinals and input shapes;
//! the cohort and route vocabulary the provider uses internally never crosses
//! that boundary.

use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::write_stack::session::{
    ConnectorMutationContributionGroup, ConnectorWriteCohortRoutingBody,
    ConnectorWriteCohortRoutingProof, ConnectorWriteRewriteSource, ConnectorWriteRouteFacts,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorWriteAdmissionPurpose, ConnectorWriteInputShape,
};

use crate::commit::write_stack::copy_on_write::IcebergCowBranchRecipe;
use crate::commit::write_stack::domain::{
    IcebergDataBranchRecipe, IcebergDocumentPublicationFacts, IcebergEqualityDeleteRecipe,
    IcebergFrozenRewriteBranchInput, IcebergPositionRewriteTarget, IcebergSealedWriteTarget,
    IcebergSessionFacts, IcebergWriteBranch, IcebergWriteCommitSession, IcebergWriteFlavor,
    IcebergWriteSessionId, IcebergWriteTableFacts, IcebergWriterHandle, IcebergWriterOutput,
    invalid,
};
type IcebergWriteTargetPlanParts = (
    WriteTargetOrdinal,
    IcebergWriterHandle,
    ConnectorWriteInputShape,
    Option<ConnectorWriteRouteFacts>,
    Option<ConnectorWriteCohortRoutingProof>,
    Option<ConnectorWriteRewriteSource>,
    Option<[u8; 32]>,
    Option<ConnectorMutationContributionGroup>,
);

/// One logical target the sealed plan exposes.
#[derive(Clone, Debug)]
pub struct IcebergWriteTargetPlan {
    ordinal: WriteTargetOrdinal,
    branch: IcebergWriteBranch,
    handle: IcebergWriterHandle,
    input: ConnectorWriteInputShape,
    route: Option<ConnectorWriteRouteFacts>,
    routing_proof: Option<ConnectorWriteCohortRoutingProof>,
    rewrite_source: Option<ConnectorWriteRewriteSource>,
    rewrite_group_digest: Option<[u8; 32]>,
    position_materialization: Option<ConnectorMutationContributionGroup>,
}

impl IcebergWriteTargetPlan {
    pub const fn ordinal(&self) -> WriteTargetOrdinal {
        self.ordinal
    }
    pub const fn branch(&self) -> IcebergWriteBranch {
        self.branch
    }
    pub const fn handle(&self) -> &IcebergWriterHandle {
        &self.handle
    }
    pub const fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
    /// The routing facts of one row-mutation branch, present exactly when the
    /// session routes rows by change event.
    pub const fn route(&self) -> Option<&ConnectorWriteRouteFacts> {
        self.route.as_ref()
    }
    /// The read contract of one copy-on-write branch, present exactly when the
    /// branch produces its rows by re-reading the files it replaces.
    pub const fn rewrite_source(&self) -> Option<&ConnectorWriteRewriteSource> {
        self.rewrite_source.as_ref()
    }
    pub const fn routing_proof(&self) -> Option<&ConnectorWriteCohortRoutingProof> {
        self.routing_proof.as_ref()
    }
    pub(crate) fn with_creation_scope(
        mut self,
        scope: super::domain::IcebergCreationScope,
    ) -> Self {
        self.handle = self.handle.with_creation_scope(scope);
        self
    }

    pub(crate) fn with_position_materialization(
        mut self,
        recipe: super::domain::IcebergMutationReadRecipe,
        materialization: Option<(ConnectorMutationContributionGroup, ConnectorWriteInputShape)>,
    ) -> Result<Self, ConnectorError> {
        let group_id = materialization.as_ref().map(|(group, _)| group.id());
        self.handle = self.handle.try_with_mutation_read(recipe, group_id)?;
        if let Some((group, input)) = materialization {
            self.route = None;
            self.input = input;
            self.position_materialization = Some(group);
        }
        Ok(self)
    }

    pub(crate) fn with_rewrite_group_digest(mut self, digest: [u8; 32]) -> Self {
        self.rewrite_group_digest = Some(digest);
        self
    }
    pub const fn rewrite_group_digest(&self) -> Option<[u8; 32]> {
        self.rewrite_group_digest
    }
    pub fn into_parts(self) -> IcebergWriteTargetPlanParts {
        (
            self.ordinal,
            self.handle,
            self.input,
            self.route,
            self.routing_proof,
            self.rewrite_source,
            self.rewrite_group_digest,
            self.position_materialization,
        )
    }
}

/// The data branch every flavor opens.
#[derive(Clone, Debug)]
pub struct IcebergDataBranchPlan {
    pub output: IcebergWriterOutput,
    pub recipe: IcebergDataBranchRecipe,
    pub input: ConnectorWriteInputShape,
}

/// A position writer, optionally scoped to one maintenance selection.
#[derive(Clone, Debug)]
pub struct IcebergDeleteBranchPlan {
    pub branch: IcebergWriteBranch,
    pub output: IcebergWriterOutput,
    pub position_rewrite: Option<IcebergPositionRewriteTarget>,
    pub input: ConnectorWriteInputShape,
}

/// An equality-delete branch: it appends delete files matched on column values.
///
/// It owns no data file and freezes no old-delete reference, which is exactly
/// what separates it from [`IcebergDeleteBranchPlan`]: an equality delete
/// supersedes nothing and merges nothing.
#[derive(Clone, Debug)]
pub struct IcebergEqualityDeleteBranchPlan {
    pub output: IcebergWriterOutput,
    pub recipe: IcebergEqualityDeleteRecipe,
    pub input: ConnectorWriteInputShape,
}

/// Everything `begin_write` freezes before it seals a session.
#[derive(Clone, Debug)]
pub struct IcebergWriteSessionPlanInput {
    pub flavor: IcebergWriteFlavor,
    pub purpose: ConnectorWriteAdmissionPurpose,
    pub table: IcebergWriteTableFacts,
    pub base_version_digest: Option<[u8; 32]>,
    /// Present exactly on the staged-create flavor, whose target has no catalog
    /// entry to load.
    pub staged_metadata: Option<std::sync::Arc<crate::iceberg::spec::TableMetadata>>,
    pub data: IcebergDataBranchPlan,
    pub deletes: Vec<IcebergDeleteBranchPlan>,
}

/// One logical branch a session seals, in ordinal order.
///
/// [`plan_write_session`] seals a flavor's canonical data-plus-delete shape.
/// A flavor whose branch structure is decided per request — a row mutation
/// whose branch count follows the change events it must route, a distributed
/// rewrite with one branch per frozen group — states its branches here instead,
/// and the same unique-owner proof runs over them.
#[derive(Clone, Debug)]
pub enum IcebergWriteBranchPlan {
    Data {
        plan: IcebergDataBranchPlan,
        route: Option<ConnectorWriteRouteFacts>,
    },
    Delete {
        plan: IcebergDeleteBranchPlan,
        route: Option<ConnectorWriteRouteFacts>,
    },
    EqualityDelete {
        plan: IcebergEqualityDeleteBranchPlan,
        route: Option<ConnectorWriteRouteFacts>,
    },
}

impl IcebergWriteBranchPlan {
    pub const fn branch(&self) -> IcebergWriteBranch {
        match self {
            Self::Data { .. } => IcebergWriteBranch::Data,
            Self::Delete { plan, .. } => plan.branch,
            Self::EqualityDelete { .. } => IcebergWriteBranch::EqualityDelete,
        }
    }

    fn route(&self) -> Option<&ConnectorWriteRouteFacts> {
        match self {
            Self::Data { route, .. }
            | Self::Delete { route, .. }
            | Self::EqualityDelete { route, .. } => route.as_ref(),
        }
    }

    fn input(&self) -> &ConnectorWriteInputShape {
        match self {
            Self::Data { plan, .. } => &plan.input,
            Self::Delete { plan, .. } => &plan.input,
            Self::EqualityDelete { plan, .. } => &plan.input,
        }
    }

    fn position_rewrite(&self) -> Option<&IcebergPositionRewriteTarget> {
        match self {
            Self::Delete { plan, .. } => plan.position_rewrite.as_ref(),
            _ => None,
        }
    }

    fn writer_handle(
        &self,
        table: &IcebergWriteTableFacts,
    ) -> Result<IcebergWriterHandle, ConnectorError> {
        match self {
            Self::Data { plan, .. } => IcebergWriterHandle::try_new_data(
                table.clone(),
                plan.output.clone(),
                plan.recipe.clone(),
            ),
            Self::Delete { plan, .. } => {
                let handle = IcebergWriterHandle::try_new_delete(
                    plan.branch,
                    table.clone(),
                    plan.output.clone(),
                )?;
                match &plan.position_rewrite {
                    Some(target) => handle.try_with_position_rewrite(target.clone()),
                    None => Ok(handle),
                }
            }
            Self::EqualityDelete { plan, .. } => IcebergWriterHandle::try_new_equality_delete(
                table.clone(),
                plan.output.clone(),
                plan.recipe.clone(),
            ),
        }
    }
}

/// Everything a flavor that plans its own branches freezes before it seals a
/// session.
#[derive(Clone, Debug)]
pub struct IcebergBranchSessionPlanInput {
    pub flavor: IcebergWriteFlavor,
    pub purpose: ConnectorWriteAdmissionPurpose,
    pub table: IcebergWriteTableFacts,
    pub base_version_digest: Option<[u8; 32]>,
    /// Present exactly on an application-document publication.
    pub document_publication: Option<IcebergDocumentPublicationFacts>,
    /// Present exactly on the staged-create flavor: the frozen metadata of a
    /// target that has no catalog entry to load.
    pub staged_metadata: Option<std::sync::Arc<crate::iceberg::spec::TableMetadata>>,
    /// Present exactly on the distributed-rewrite flavor: the exact input files
    /// each sealed branch replaces, in the same order as `branches`.
    pub rewrite_inputs: Vec<IcebergFrozenRewriteBranchInput>,
    /// Present exactly on the copy-on-write flavor: what each sealed branch
    /// replaces and what it re-reads, in the same order as `branches`.
    pub copy_on_write: Vec<IcebergCowBranchRecipe>,
    /// Present only on a managed publication that replaces the target's default
    /// partitioning in the same external commit.
    pub(crate) repartition:
        Option<crate::commit::write_stack::repartition::IcebergPreparedRepartition>,
    /// The exact table generation every *writer* is built against.
    ///
    /// It differs from `table` only under an atomic partition replacement,
    /// where the writers must already write under the spec the commit is about
    /// to establish while the session's own compare-and-swap still has to match
    /// the generation the table currently holds.
    pub writer_table: Option<IcebergWriteTableFacts>,
    /// The branches this session seals, in ordinal order.
    pub branches: Vec<IcebergWriteBranchPlan>,
}

/// Seal one write session's logical target map from an explicit branch list.
///
/// Ordinals are assigned by position, so the branch order *is* the ordinal
/// order. The unique-owner proof runs before any handle is built, which is what
/// makes an unprovable routing fail with nothing written.
pub fn plan_branch_session(
    session_id: IcebergWriteSessionId,
    input: IcebergBranchSessionPlanInput,
) -> Result<(IcebergWriteCommitSession, Vec<IcebergWriteTargetPlan>), ConnectorError> {
    if input.branches.is_empty() {
        return Err(invalid(format!(
            "Iceberg {} write session must seal at least one branch",
            input.flavor.as_str()
        )));
    }
    let allowed = crate::commit::write_stack::domain::allowed_session_branches(
        input.flavor,
        input
            .document_publication
            .as_ref()
            .map(IcebergDocumentPublicationFacts::shape),
    );
    for plan in &input.branches {
        if !allowed.contains(&plan.branch()) {
            return Err(invalid(format!(
                "Iceberg {} flavor does not own a {} branch",
                input.flavor.as_str(),
                plan.branch().as_str()
            )));
        }
        if let IcebergWriteBranchPlan::Delete { plan, .. } = plan
            && !plan.branch.writes_deletes()
        {
            return Err(invalid(
                "Iceberg delete branch plan must name a delete branch",
            ));
        }
    }
    // Routing is a property of the whole session: a partially routed session
    // gives SQL somewhere to send some rows and nowhere to send the rest. The
    // neutral session refuses it too, but refusing here keeps the provider from
    // building handles for a plan that can never be sealed.
    let routed = input
        .branches
        .iter()
        .filter(|plan| plan.route().is_some())
        .count();
    if routed != 0 && routed != input.branches.len() {
        return Err(invalid(format!(
            "Iceberg {} write session routes some branches but not all",
            input.flavor.as_str()
        )));
    }

    let mut sealed = Vec::with_capacity(input.branches.len());
    for (index, plan) in input.branches.iter().enumerate() {
        let ordinal = u32::try_from(index)
            .map_err(|_| invalid("Iceberg write session exceeds its logical target bound"))?;
        sealed.push(IcebergSealedWriteTarget::new(
            WriteTargetOrdinal::try_new(ordinal)?,
            plan.branch(),
            plan.position_rewrite().cloned(),
        ));
    }

    // A copy-on-write branch's read contract is per branch, so a session
    // holding a different number of recipes than it sealed branches could not
    // say which file any one branch replaces or re-reads.
    if !input.copy_on_write.is_empty() && input.copy_on_write.len() != input.branches.len() {
        return Err(invalid(format!(
            "Iceberg {} write session froze {} copy-on-write recipes for {} sealed branches",
            input.flavor.as_str(),
            input.copy_on_write.len(),
            input.branches.len()
        )));
    }

    let writer_table = input.writer_table.as_ref().unwrap_or(&input.table);
    let mut plans = Vec::with_capacity(sealed.len());
    for (index, plan) in input.branches.iter().enumerate() {
        let routing_proof = match input.copy_on_write.get(index) {
            Some(recipe) => {
                let route = plan.route().ok_or_else(|| {
                    invalid("Iceberg copy-on-write branch omits its routing facts")
                })?;
                Some(ConnectorWriteCohortRoutingProof::try_new(
                route.route_id(),
                recipe.selection_digest(),
                recipe.selection_ordinals().to_vec(),
                match recipe.input() {
                    crate::commit::write_stack::copy_on_write::IcebergCowBranchInput::Rewrite {
                        ..
                    } => ConnectorWriteCohortRoutingBody::Rewrite,
                    crate::commit::write_stack::copy_on_write::IcebergCowBranchInput::Append => {
                        ConnectorWriteCohortRoutingBody::Append
                    }
                },
                )?)
            }
            None => None,
        };
        plans.push(IcebergWriteTargetPlan {
            ordinal: sealed[index].ordinal(),
            branch: plan.branch(),
            handle: plan.writer_handle(writer_table)?,
            input: plan.input().clone(),
            route: plan.route().cloned(),
            routing_proof,
            rewrite_group_digest: None,
            position_materialization: None,
            rewrite_source: input
                .copy_on_write
                .get(index)
                .and_then(IcebergCowBranchRecipe::rewrite_source)
                .cloned(),
        });
    }

    let handle = IcebergWriteCommitSession::try_new_sealed(
        session_id,
        input.table,
        input.flavor,
        IcebergSessionFacts {
            purpose: input.purpose,
            base_version_digest: input.base_version_digest,
            document_publication: input.document_publication,
            staged_metadata: input.staged_metadata,
            rewrite_inputs: input.rewrite_inputs,
            copy_on_write: input
                .copy_on_write
                .into_iter()
                .map(IcebergCowBranchRecipe::into_input)
                .collect(),
            repartition: input.repartition,
        },
        sealed,
    )?;
    Ok((handle, plans))
}

/// Seal one write session's logical target map.
///
/// Ordinal assignment follows [`IcebergWriteFlavor::branches`] exactly, so the
/// data branch is always ordinal 0 and a delete branch, when the flavor owns
/// one, is ordinal 1. The unique-owner proof runs before any target is built.
pub fn plan_write_session(
    session_id: IcebergWriteSessionId,
    input: IcebergWriteSessionPlanInput,
) -> Result<(IcebergWriteCommitSession, Vec<IcebergWriteTargetPlan>), ConnectorError> {
    let mut branches = Vec::with_capacity(1 + input.deletes.len());
    branches.push(IcebergWriteBranchPlan::Data {
        plan: input.data,
        route: None,
    });
    for delete in input.deletes {
        branches.push(IcebergWriteBranchPlan::Delete {
            plan: delete,
            route: None,
        });
    }
    // The canonical shape freezes no rewrite input, because it is cut from the
    // flavor alone rather than from a planned group set. A rewrite sealed this
    // way therefore replaces nothing: its empty prepared set terminates it as a
    // no-op, and a non-empty one is refused by the commit action rather than
    // retiring files no group named.
    let rewrite_inputs = if input.flavor.is_distributed_rewrite() {
        branches
            .iter()
            .map(|_| IcebergFrozenRewriteBranchInput::default())
            .collect()
    } else {
        Vec::new()
    };
    plan_branch_session(
        session_id,
        IcebergBranchSessionPlanInput {
            flavor: input.flavor,
            purpose: input.purpose,
            table: input.table,
            base_version_digest: input.base_version_digest,
            document_publication: None,
            staged_metadata: input.staged_metadata,
            rewrite_inputs,
            copy_on_write: Vec::new(),
            repartition: None,
            writer_table: None,
            branches,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::write_stack::test_support::{
        data_branch_plan, delete_branch_plan, table_facts,
    };

    #[test]
    fn a_flavor_seals_its_branches_as_dense_ordinals() {
        let (handle, plans) = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor: IcebergWriteFlavor::RowMutationDeletionVector,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                staged_metadata: None,
                data: data_branch_plan(),
                deletes: vec![delete_branch_plan(IcebergWriteBranch::DeletionVector, None)],
            },
        )
        .expect("plan");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].ordinal().get(), 0);
        assert_eq!(plans[0].branch(), IcebergWriteBranch::Data);
        assert_eq!(plans[1].ordinal().get(), 1);
        assert_eq!(plans[1].branch(), IcebergWriteBranch::DeletionVector);
        assert_eq!(handle.expected_targets().len(), 2);
    }

    #[test]
    fn an_append_flavor_cannot_seal_a_delete_branch() {
        let error = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor: IcebergWriteFlavor::Append,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                staged_metadata: None,
                data: data_branch_plan(),
                deletes: vec![delete_branch_plan(IcebergWriteBranch::DeletionVector, None)],
            },
        )
        .expect_err("flavor does not own the branch");
        assert!(error.message().contains("does not own a"));
    }
}
