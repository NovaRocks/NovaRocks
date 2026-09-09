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

use std::{collections::BTreeSet, sync::Arc};

use novarocks_sql::planning::query_execution::{
    SealedMvRewriteAction, SealedPreparationPlanId, SealedScanIdentity,
};

use crate::api::{
    ExactObjectBinding, MvCandidateFact, MvPublicationId, QueryConsistency, RelationOccurrence,
};

/// Relation mapping asserted by the compiler. The explicit
/// publication-input ordinal prevents a same-name self join or reordered
/// definition input from being accepted through positional coincidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvInputMatch {
    occurrence: SealedScanIdentity,
    publication_input_ordinal: usize,
}

impl MvInputMatch {
    pub const fn new(occurrence: SealedScanIdentity, publication_input_ordinal: usize) -> Self {
        Self {
            occurrence,
            publication_input_ordinal,
        }
    }
    pub const fn occurrence(self) -> SealedScanIdentity {
        self.occurrence
    }
    pub const fn publication_input_ordinal(self) -> usize {
        self.publication_input_ordinal
    }
}

/// A candidate whose published inputs match the pre-rewrite query bindings.
/// It becomes executable only when description freezing also proves that the
/// final sealed plan reads `output` at `target_scan_node_id`.
#[derive(Clone, Debug)]
pub struct StrictMvCandidateMatch {
    publication_id: MvPublicationId,
    pre_rewrite_plan: SealedPreparationPlanId,
    input_mapping: Arc<[MvInputMatch]>,
    output: ExactObjectBinding,
    rewrite_action: SealedMvRewriteAction,
}

impl StrictMvCandidateMatch {
    pub const fn publication_id(&self) -> MvPublicationId {
        self.publication_id
    }

    pub fn input_mapping(&self) -> &[MvInputMatch] {
        &self.input_mapping
    }

    /// Published output that final description freezing must match to the
    /// selected scan node.
    pub const fn output_binding(&self) -> &ExactObjectBinding {
        &self.output
    }

    pub const fn target_scan(&self) -> SealedScanIdentity {
        self.rewrite_action.target()
    }

    pub const fn pre_rewrite_plan(&self) -> SealedPreparationPlanId {
        self.pre_rewrite_plan
    }

    pub const fn rewrite_action(&self) -> &SealedMvRewriteAction {
        &self.rewrite_action
    }
}

pub fn prove_strict_mv_candidate_match(
    consistency: QueryConsistency,
    query_occurrences: &[RelationOccurrence],
    input_mapping: &[MvInputMatch],
    candidate: &MvCandidateFact,
    rewrite_action: SealedMvRewriteAction,
) -> Result<StrictMvCandidateMatch, String> {
    if consistency != QueryConsistency::Strict {
        return Err(
            "UEA-1 currently validates MV candidate matches only at strict consistency".to_string(),
        );
    }
    if rewrite_action.definition_fingerprint() != candidate.definition_fingerprint() {
        return Err(
            "MV candidate definition does not match the compiler-proved relation".to_string(),
        );
    }
    if rewrite_action.publication_id() != candidate.publication_id().bytes() {
        return Err("MV candidate publication was not selected by the optimizer".to_string());
    }
    if rewrite_action.input_mapping().len() != input_mapping.len() {
        return Err("MV candidate mapping differs from the optimizer selection".to_string());
    }
    if input_mapping.len() != candidate.inputs().len() {
        return Err(
            "MV candidate input count does not match the selected query occurrences".to_string(),
        );
    }
    let query_occurrence_ids = query_occurrences
        .iter()
        .map(RelationOccurrence::occurrence)
        .collect::<BTreeSet<_>>();
    if query_occurrence_ids.len() != query_occurrences.len() {
        return Err("MV candidate match input repeats a query relation occurrence".to_string());
    }
    let pre_rewrite_plan = query_occurrences
        .first()
        .ok_or_else(|| "MV candidate match has no pre-rewrite query occurrence".to_string())?
        .occurrence()
        .plan();
    if query_occurrences
        .iter()
        .any(|occurrence| occurrence.occurrence().plan() != pre_rewrite_plan)
    {
        return Err(
            "MV candidate match mixes relation occurrences from different pre-rewrite plans"
                .to_string(),
        );
    }
    if rewrite_action.target().plan() == pre_rewrite_plan {
        return Err(
            "MV candidate match requires distinct pre-rewrite and final plan seals".to_string(),
        );
    }
    let mut seen_occurrences = BTreeSet::new();
    let mut seen_inputs = BTreeSet::new();
    for mapping in input_mapping.iter() {
        if !seen_occurrences.insert(mapping.occurrence) {
            return Err("MV candidate match repeats a query relation occurrence".to_string());
        }
        if !seen_inputs.insert(mapping.publication_input_ordinal) {
            return Err("MV candidate match repeats a publication input".to_string());
        }
        let query_binding = query_occurrences
            .iter()
            .find(|query| query.occurrence() == mapping.occurrence)
            .map(RelationOccurrence::binding)
            .ok_or_else(|| {
                "MV candidate match names an unknown query relation occurrence".to_string()
            })?;
        let published_binding = candidate
            .inputs()
            .get(mapping.publication_input_ordinal)
            .ok_or_else(|| "MV candidate match names an unknown publication input".to_string())?;
        if query_binding != published_binding {
            return Err(
                "MV candidate input does not exactly match the mapped query relation binding"
                    .to_string(),
            );
        }
        let selected = rewrite_action.input_mapping().iter().any(|selected| {
            let query = query_occurrences
                .iter()
                .find(|query| query.occurrence() == mapping.occurrence)
                .expect("query occurrence was resolved above");
            selected.binding() == query.sql_binding()
                && selected.occurrence() == query.sql_occurrence()
                && selected.publication_input_ordinal() == mapping.publication_input_ordinal
        });
        if !selected {
            return Err("MV candidate mapping was not selected by the optimizer".to_string());
        }
    }
    if seen_inputs.len() != candidate.inputs().len() {
        return Err("MV candidate match does not cover every publication input".to_string());
    }
    Ok(StrictMvCandidateMatch {
        publication_id: candidate.publication_id(),
        pre_rewrite_plan,
        input_mapping: input_mapping.into(),
        output: candidate.output().clone(),
        rewrite_action,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        CatalogGeneration, DataVersion, ObjectIdentity, ObjectPath, ProviderFactFormat,
    };

    fn format(kind: &str) -> ProviderFactFormat {
        ProviderFactFormat::try_new("iceberg-rest", kind).unwrap()
    }

    fn binding(table: &str, version: u8) -> ExactObjectBinding {
        ExactObjectBinding::new_for_test(
            ObjectPath::try_new(["ice", "ns", table]).unwrap(),
            CatalogGeneration::try_new(format("catalog-generation/v1"), Arc::<[u8]>::from([1]))
                .unwrap(),
            ObjectIdentity::try_new(format("table-uuid/v1"), Arc::<[u8]>::from([2])).unwrap(),
            DataVersion::try_new(format("snapshot-id/v1"), Arc::<[u8]>::from([version])).unwrap(),
        )
    }

    fn candidate(inputs: Vec<ExactObjectBinding>, output_version: u8) -> MvCandidateFact {
        candidate_with_identity([7; 16], [9; 32], inputs, output_version)
    }

    fn candidate_with_identity(
        publication_id: [u8; 16],
        definition_fingerprint: [u8; 32],
        inputs: Vec<ExactObjectBinding>,
        output_version: u8,
    ) -> MvCandidateFact {
        MvCandidateFact::try_new_for_test(
            MvPublicationId::try_new(publication_id).unwrap(),
            definition_fingerprint,
            "catalog.mv_orders:v1",
            &inputs,
            &binding("mv_orders", output_version),
        )
        .unwrap()
    }

    #[test]
    fn optimizer_selection_rejects_other_publication_and_definition() {
        let source_contracts = source_occurrences(1);
        let occurrence = source_contracts[0].identity();
        let source = binding("orders", 101);
        let queries = [RelationOccurrence::resolved(
            occurrence,
            source_contracts[0].sql_occurrence(),
            source_contracts[0].binding(),
            source.clone(),
        )];
        let mapping = [MvInputMatch::new(occurrence, 0)];

        for candidate in [
            candidate_with_identity([8; 16], [9; 32], vec![source.clone()], 7),
            candidate_with_identity([7; 16], [8; 32], vec![source.clone()], 7),
        ] {
            assert!(
                prove_strict_mv_candidate_match(
                    QueryConsistency::Strict,
                    &queries,
                    &mapping,
                    &candidate,
                    rewrite_action(),
                )
                .is_err()
            );
        }
    }

    fn source_occurrences(
        count: usize,
    ) -> Vec<novarocks_sql::planning::query_execution::SealedScanContract> {
        let plan = if count == 1 {
            novarocks_sql::test_support::native_scan_plan(
                novarocks_sql::test_support::NativeScanFixture::ConnectorRead,
            )
            .unwrap()
        } else {
            novarocks_sql::test_support::native_self_join_scan_plan().unwrap()
        };
        novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(plan)
            .scan_contracts()
            .unwrap()
            .into_iter()
            .take(count)
            .collect()
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

    #[test]
    fn strict_candidate_match_binds_inputs_and_exact_published_output() {
        let source = binding("orders", 101);
        let candidate = candidate(vec![source.clone()], 7);
        let source_contracts = source_occurrences(1);
        let occurrence = source_contracts[0].identity();
        let queries = [RelationOccurrence::resolved(
            occurrence,
            source_contracts[0].sql_occurrence(),
            source_contracts[0].binding(),
            source,
        )];
        let mapping = [MvInputMatch::new(occurrence, 0)];
        let action = rewrite_action();
        let matched = prove_strict_mv_candidate_match(
            QueryConsistency::Strict,
            &queries,
            &mapping,
            &candidate,
            action.clone(),
        )
        .unwrap();
        assert_eq!(matched.output_binding(), candidate.output());
        assert_eq!(matched.target_scan(), action.target());
    }

    #[test]
    fn strict_candidate_match_rejects_a_newer_query_binding() {
        let candidate = candidate(vec![binding("orders", 101)], 7);
        let source_contracts = source_occurrences(1);
        let occurrence = source_contracts[0].identity();
        let queries = [RelationOccurrence::resolved(
            occurrence,
            source_contracts[0].sql_occurrence(),
            source_contracts[0].binding(),
            binding("orders", 102),
        )];
        let mapping = [MvInputMatch::new(occurrence, 0)];
        assert!(
            prove_strict_mv_candidate_match(
                QueryConsistency::Strict,
                &queries,
                &mapping,
                &candidate,
                rewrite_action(),
            )
            .is_err()
        );
    }

    #[test]
    fn explicit_mapping_prevents_positional_self_join_mismatch() {
        let left = binding("orders", 101);
        let right = left.clone();
        let candidate = candidate(vec![left.clone(), right.clone()], 7);
        let contracts = source_occurrences(2);
        let occurrences = [contracts[0].identity(), contracts[1].identity()];
        let queries = [
            RelationOccurrence::resolved(
                occurrences[0],
                contracts[0].sql_occurrence(),
                contracts[0].binding(),
                left,
            ),
            RelationOccurrence::resolved(
                occurrences[1],
                contracts[1].sql_occurrence(),
                contracts[1].binding(),
                right,
            ),
        ];
        let swapped = [
            MvInputMatch::new(occurrences[0], 1),
            MvInputMatch::new(occurrences[1], 0),
        ];
        let optimizer_mapping = vec![
            (contracts[0].sql_occurrence(), 0),
            (contracts[1].sql_occurrence(), 1),
        ];
        let action = novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan_with_inputs(
                optimizer_mapping,
            )
            .unwrap(),
        )
        .scan_contracts()
        .unwrap()[0]
            .mv_rewrite_action()
            .unwrap();
        assert!(
            prove_strict_mv_candidate_match(
                QueryConsistency::Strict,
                &queries,
                &swapped,
                &candidate,
                action,
            )
            .is_err()
        );
    }
}
