// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use novarocks_sql::planning::query_execution::{SealedMvRewriteAction, SealedScanIdentity};

use crate::api::{
    ExactObjectBinding, MvCandidateFact, MvCandidateFactInput, MvPublicationId, QueryConsistency,
    RelationOccurrence, SealedExactBindingReceipts,
};

/// A candidate whose published inputs match the pre-rewrite query bindings.
/// It becomes executable only when description freezing also proves that the
/// final sealed plan reads `output` at `target_scan_node_id`.
#[derive(Clone, Debug)]
pub struct StrictMvCandidateMatch {
    publication_id: MvPublicationId,
    inputs: Arc<[ExactObjectBinding]>,
    output: ExactObjectBinding,
    rewrite_action: SealedMvRewriteAction,
}

impl StrictMvCandidateMatch {
    pub const fn publication_id(&self) -> MvPublicationId {
        self.publication_id
    }

    /// Published output that final description freezing must match to the
    /// selected scan node.
    pub const fn output_binding(&self) -> &ExactObjectBinding {
        &self.output
    }

    pub const fn target_scan(&self) -> SealedScanIdentity {
        self.rewrite_action.target()
    }

    pub fn input_bindings(&self) -> &[ExactObjectBinding] {
        &self.inputs
    }

    pub const fn rewrite_action(&self) -> &SealedMvRewriteAction {
        &self.rewrite_action
    }
}

/// Query-scoped proof that the optimizer-selected publication inputs match
/// the actual pre-rewrite SQL bindings. Construction is possible only through
/// the sealed receipt authority that minted those binding tokens.
pub struct SelectedMvQueryInputs {
    inputs: Arc<[ExactObjectBinding]>,
    rewrite_action: SealedMvRewriteAction,
}

impl SelectedMvQueryInputs {
    /// The exact final-plan scan the optimizer replaced with this publication.
    /// The opaque identity is read-only; only the proof functions can construct
    /// or consume the selected-input authority.
    pub const fn target(&self) -> SealedScanIdentity {
        self.rewrite_action.target()
    }
}

pub fn prove_selected_mv_query_inputs(
    consistency: QueryConsistency,
    receipts: &SealedExactBindingReceipts,
    rewrite_action: SealedMvRewriteAction,
) -> Result<SelectedMvQueryInputs, String> {
    if consistency != QueryConsistency::Strict {
        return Err(
            "UEA-1 currently validates MV candidate matches only at strict consistency".to_string(),
        );
    }
    let publication_inputs = rewrite_action.publication_inputs();
    let mapping = rewrite_action.input_mapping();
    if mapping.len() != publication_inputs.len() {
        return Err("MV selection does not exactly cover its publication inputs".to_string());
    }
    if mapping.iter().enumerate().any(|(index, selected)| {
        mapping[..index]
            .iter()
            .any(|other| other.occurrence() == selected.occurrence())
    }) {
        return Err("MV selection repeats a pre-rewrite query occurrence".to_string());
    }

    let mut inputs = vec![None; publication_inputs.len()];
    for selected in mapping {
        if selected.occurrence().binding() != selected.binding() {
            return Err("MV selection occurrence and binding token disagree".to_string());
        }
        let ordinal = selected.publication_input_ordinal();
        let expected = publication_inputs
            .get(ordinal)
            .ok_or_else(|| "MV selection names an unknown publication input".to_string())?;
        if inputs[ordinal].is_some() {
            return Err("MV selection repeats a publication input".to_string());
        }
        let actual = receipts.resolve(selected.binding())?;
        if !actual.matches_publication_relation(expected)? {
            return Err(
                "MV publication input does not match the actual pre-rewrite query binding"
                    .to_string(),
            );
        }
        inputs[ordinal] = Some(actual);
    }
    let inputs = inputs
        .into_iter()
        .map(|input| input.ok_or_else(|| "MV selection omits a publication input".to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SelectedMvQueryInputs {
        inputs: inputs.into(),
        rewrite_action,
    })
}

/// Complete the strict proof with the exact target receipt produced by final
/// scan negotiation. This closes both base-version and target-version races.
pub fn prove_selected_mv_target(
    selected: SelectedMvQueryInputs,
    target: &RelationOccurrence,
) -> Result<StrictMvCandidateMatch, String> {
    if target.occurrence() != selected.rewrite_action.target() {
        return Err("MV target receipt belongs to another final plan scan".to_string());
    }
    if target.sql_occurrence().binding() != target.sql_binding() {
        return Err("MV target SQL occurrence and binding token disagree".to_string());
    }
    if !target
        .binding()
        .matches_publication_relation(selected.rewrite_action.publication_target())?
    {
        return Err("MV target receipt does not match the published target revision".to_string());
    }
    let publication_id = MvPublicationId::try_new(selected.rewrite_action.publication_id())
        .ok_or_else(|| "MV publication identity is invalid".to_string())?;
    let candidate = MvCandidateFactInput::try_new(
        publication_id,
        selected.rewrite_action.definition_fingerprint(),
        selected.rewrite_action.source(),
        &selected.inputs,
        target.binding(),
    )
    .map(MvCandidateFact::retain)
    .ok_or_else(|| "MV selected candidate facts are incomplete".to_string())?;
    Ok(StrictMvCandidateMatch {
        publication_id,
        inputs: candidate.inputs().into(),
        output: candidate.output().clone(),
        rewrite_action: selected.rewrite_action,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{CatalogGeneration, ExactBindingReceiptStore, ObjectPath, ProviderFactFormat};

    fn format(kind: &str) -> ProviderFactFormat {
        ProviderFactFormat::try_new("iceberg-rest", kind).unwrap()
    }

    fn rewrite_action() -> SealedMvRewriteAction {
        novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap(),
        )
        .scan_contracts()
        .unwrap()[0]
            .mv_rewrite_action()
            .unwrap()
    }

    fn publication_binding(
        relation: &novarocks_sql::compiler::SqlMvRewritePublicationRelation,
    ) -> ExactObjectBinding {
        ExactObjectBinding::new_for_publication_test(
            ObjectPath::try_new(relation.table_fqn().split('.')).unwrap(),
            CatalogGeneration::try_new(format("catalog-generation/v1"), Arc::<[u8]>::from([1]))
                .unwrap(),
            relation,
        )
    }

    fn changed_publication_binding(
        relation: &novarocks_sql::compiler::SqlMvRewritePublicationRelation,
        object: &[u8],
        snapshot: i64,
    ) -> ExactObjectBinding {
        let object = novarocks_spi::connector::ConnectorTableObjectId::try_new(
            bytes::Bytes::copy_from_slice(object),
        )
        .unwrap();
        let changed = novarocks_sql::compiler::SqlMvRewritePublicationRelation::new(
            relation.table_fqn().to_string(),
            novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                relation.revision().object_identity().provider().clone(),
                &object,
                Some(snapshot),
            )
            .unwrap(),
        )
        .unwrap();
        publication_binding(&changed)
    }

    fn selected_inputs(
        action: SealedMvRewriteAction,
        input: ExactObjectBinding,
    ) -> Result<SelectedMvQueryInputs, String> {
        let binding = action.input_mapping()[0].binding();
        let allocator = novarocks_sql::binding::SqlTableBindingAllocator::try_new_for_test(
            binding.scope().get(),
        )?;
        let store = ExactBindingReceiptStore::new(&allocator);
        store.register_for_test(binding, input);
        prove_selected_mv_query_inputs(QueryConsistency::Strict, &store.seal(), action)
    }

    fn target_occurrence(action: &SealedMvRewriteAction) -> RelationOccurrence {
        let plan = novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap(),
        );
        let contract = plan.scan_contracts().unwrap().remove(0);
        RelationOccurrence::resolved(
            action.target(),
            contract.sql_occurrence(),
            contract.binding(),
            publication_binding(action.publication_target()),
        )
    }

    #[test]
    fn selected_proof_joins_publication_to_actual_input_and_target_receipts() {
        let action = rewrite_action();
        let input = publication_binding(&action.publication_inputs()[0]);
        let selected = selected_inputs(action.clone(), input).unwrap();
        let target = target_occurrence(&action);
        let strict = prove_selected_mv_target(selected, &target).unwrap();
        assert_eq!(strict.publication_id().bytes(), [7; 16]);
        assert_eq!(strict.target_scan(), action.target());
    }

    #[test]
    fn selected_proof_rejects_s101_s102_and_object_replacement() {
        let action = rewrite_action();
        let expected = &action.publication_inputs()[0];
        let same_object = expected.revision().object_identity().value();
        for mismatched in [
            changed_publication_binding(expected, same_object, 102),
            changed_publication_binding(expected, b"replacement-object", 101),
        ] {
            assert!(selected_inputs(action.clone(), mismatched).is_err());
        }
    }

    #[test]
    fn selected_proof_rejects_m1_m2_and_unreadable_target() {
        let action = rewrite_action();
        let selected = selected_inputs(
            action.clone(),
            publication_binding(&action.publication_inputs()[0]),
        )
        .unwrap();
        let plan = novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap(),
        );
        let contract = plan.scan_contracts().unwrap().remove(0);
        let wrong_target = RelationOccurrence::resolved(
            action.target(),
            contract.sql_occurrence(),
            contract.binding(),
            changed_publication_binding(
                action.publication_target(),
                action
                    .publication_target()
                    .revision()
                    .object_identity()
                    .value(),
                202,
            ),
        );
        assert!(prove_selected_mv_target(selected, &wrong_target).is_err());

        let selected = selected_inputs(
            action.clone(),
            publication_binding(&action.publication_inputs()[0]),
        )
        .unwrap();
        let unreadable = ExactObjectBinding::new_without_semantic_revision_for_test(
            ObjectPath::try_new(action.publication_target().table_fqn().split('.')).unwrap(),
            CatalogGeneration::try_new(format("catalog-generation/v1"), Arc::<[u8]>::from([1]))
                .unwrap(),
        );
        let unreadable_target = RelationOccurrence::resolved(
            action.target(),
            contract.sql_occurrence(),
            contract.binding(),
            unreadable,
        );
        assert!(prove_selected_mv_target(selected, &unreadable_target).is_err());
    }

    #[test]
    fn selected_proof_rejects_cross_query_self_join_and_reordered_mapping() {
        let action = rewrite_action();
        let foreign_allocator = novarocks_sql::binding::SqlTableBindingAllocator::try_new_for_test(
            std::num::NonZeroU64::new(99).unwrap(),
        )
        .unwrap();
        assert!(
            prove_selected_mv_query_inputs(
                QueryConsistency::Strict,
                &ExactBindingReceiptStore::new(&foreign_allocator).seal(),
                action.clone(),
            )
            .is_err()
        );

        let occurrence = action.input_mapping()[0].occurrence();
        for mapping in [
            vec![(occurrence, 0), (occurrence, 0)],
            vec![(occurrence, 1)],
        ] {
            let malformed = novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
                novarocks_sql::test_support::native_mv_rewritten_scan_plan_with_inputs(mapping)
                    .unwrap(),
            )
            .scan_contracts()
            .unwrap()
            .remove(0)
            .mv_rewrite_action()
            .unwrap();
            let actual = publication_binding(&malformed.publication_inputs()[0]);
            assert!(selected_inputs(malformed, actual).is_err());
        }
    }
}
