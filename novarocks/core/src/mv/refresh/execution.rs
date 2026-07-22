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

use std::collections::{BTreeMap, BTreeSet};

use crate::mv::model::{AffectedTargetPartitions, MvStorageEngine, MvTarget};
use crate::mv::refresh::planning::{RefreshPlanContract, RefreshStateBaseline};
use crate::mv::refresh::snapshot::ExecutableRefreshDecision;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) struct RefreshExecutionObservation<'a> {
    pub(crate) backend: MvStorageEngine,
    pub(crate) mv_id: Option<i64>,
    pub(crate) target: &'a MvTarget,
    pub(crate) base_refs: &'a [TableIdentity],
    pub(crate) state_baseline: &'a RefreshStateBaseline,
    pub(crate) snapshot_pins: Option<&'a BTreeMap<String, Option<i64>>>,
}

#[derive(Debug)]
pub(crate) struct ValidatedRefreshExecution<'a> {
    contract: &'a RefreshPlanContract,
}

impl<'a> ValidatedRefreshExecution<'a> {
    pub(crate) fn decision(&self) -> ExecutableRefreshDecision {
        self.contract.decision
    }

    pub(crate) fn target(&self) -> &'a MvTarget {
        &self.contract.target
    }

    pub(crate) fn base_refs(&self) -> &'a [TableIdentity] {
        &self.contract.base_refs
    }

    pub(crate) fn state_baseline(&self) -> &'a RefreshStateBaseline {
        &self.contract.state_baseline
    }

    pub(crate) fn affected_partitions(&self) -> &'a AffectedTargetPartitions {
        &self.contract.affected_partitions
    }
}

pub(crate) fn validate_refresh_execution<'a>(
    contract: &'a RefreshPlanContract,
    observation: &RefreshExecutionObservation<'_>,
) -> Result<ValidatedRefreshExecution<'a>, String> {
    if contract.storage_engine != observation.backend {
        return Err(format!(
            "refresh execution backend mismatch: planned {}, observed {}",
            contract.storage_engine.backend_name(),
            observation.backend.backend_name()
        ));
    }
    if contract.mv_id != observation.mv_id {
        return Err(format!(
            "refresh execution mv id mismatch: planned {:?}, observed {:?}",
            contract.mv_id, observation.mv_id
        ));
    }
    if contract.target != *observation.target {
        return Err(format!(
            "refresh execution target mismatch: planned {}, observed {}",
            contract.target.display_name(),
            observation.target.display_name()
        ));
    }

    let contract_bases = unique_base_refs("planned", &contract.base_refs)?;
    let observed_bases = unique_base_refs("observed", observation.base_refs)?;
    if contract_bases != observed_bases {
        return Err(set_mismatch(
            "refresh execution base refs",
            &contract_bases,
            &observed_bases,
        ));
    }

    if contract.state_baseline != *observation.state_baseline {
        return Err(format!(
            "refresh execution state baseline mismatch: planned {:?}, observed {:?}",
            contract.state_baseline, observation.state_baseline
        ));
    }

    match &contract.state_baseline {
        RefreshStateBaseline::Pinless => {
            if !contract.snapshot_pins.is_empty() {
                return Err(
                    "pinless refresh execution contract must not contain planned snapshot pins"
                        .to_string(),
                );
            }
            if observation.snapshot_pins.is_some() {
                return Err(
                    "pinless refresh execution observation must not contain snapshot pins"
                        .to_string(),
                );
            }
        }
        RefreshStateBaseline::SnapshotBacked { .. } => {
            let planned_keys = contract
                .snapshot_pins
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            if planned_keys != contract_bases {
                return Err(set_mismatch(
                    "planned snapshot pin keys",
                    &contract_bases,
                    &planned_keys,
                ));
            }
            let observed_pins = observation.snapshot_pins.ok_or_else(|| {
                "snapshot-backed refresh execution observation is missing snapshot pins".to_string()
            })?;
            let observed_keys = observed_pins.keys().cloned().collect::<BTreeSet<_>>();
            if observed_keys != contract_bases {
                return Err(set_mismatch(
                    "observed snapshot pin keys",
                    &contract_bases,
                    &observed_keys,
                ));
            }
            for fqn in &contract_bases {
                let planned = contract.snapshot_pins.get(fqn).expect("validated key set");
                let observed = observed_pins.get(fqn).expect("validated key set");
                if planned != observed {
                    return Err(format!(
                        "refresh execution snapshot pin mismatch for {fqn}: planned {planned:?}, observed {observed:?}"
                    ));
                }
            }
        }
    }

    Ok(ValidatedRefreshExecution { contract })
}

pub(crate) fn dispatch_refresh_decision<T, E>(
    decision: ExecutableRefreshDecision,
    skip_empty: impl FnOnce() -> Result<T, E>,
    first_refresh: impl FnOnce() -> Result<T, E>,
    metadata_only: impl FnOnce() -> Result<T, E>,
    incremental: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    match decision {
        ExecutableRefreshDecision::SkipEmpty => skip_empty(),
        ExecutableRefreshDecision::FirstRefresh => first_refresh(),
        ExecutableRefreshDecision::MetadataOnly => metadata_only(),
        ExecutableRefreshDecision::Incremental => incremental(),
    }
}

fn unique_base_refs(source: &str, base_refs: &[TableIdentity]) -> Result<BTreeSet<String>, String> {
    let mut fqns = BTreeSet::new();
    for base_ref in base_refs {
        let fqn = base_ref.fqn();
        if !fqns.insert(fqn.clone()) {
            return Err(format!(
                "refresh execution {source} base refs contain duplicate {fqn}"
            ));
        }
    }
    Ok(fqns)
}

fn set_mismatch(label: &str, expected: &BTreeSet<String>, observed: &BTreeSet<String>) -> String {
    let missing = expected.difference(observed).cloned().collect::<Vec<_>>();
    let extra = observed.difference(expected).cloned().collect::<Vec<_>>();
    format!("{label} mismatch: missing {missing:?}, extra {extra:?}")
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use super::*;
    use crate::mv::model::{AffectedTargetPartitions, MvStorageEngine, MvTarget};
    use crate::mv::refresh::planning::{RefreshPlanContract, RefreshStateBaseline};
    use crate::mv::refresh::snapshot::ExecutableRefreshDecision;
    use novarocks_catalog::identifier::TableIdentity;

    fn table(name: &str) -> TableIdentity {
        TableIdentity::new("ice", "db", name)
    }

    fn target(name: &str) -> MvTarget {
        MvTarget {
            catalog: Some("ice".to_string()),
            database: "db".to_string(),
            name: name.to_string(),
        }
    }

    fn snapshot_baseline() -> RefreshStateBaseline {
        RefreshStateBaseline::SnapshotBacked {
            previous_snapshot_ids: BTreeMap::from([
                ("ice.db.left".to_string(), 1),
                ("ice.db.right".to_string(), 2),
            ]),
            previous_table_uuids: BTreeMap::from([
                ("ice.db.left".to_string(), "left-v1".to_string()),
                ("ice.db.right".to_string(), "right-v1".to_string()),
            ]),
            target_snapshot_id: Some(10),
            target_table_uuid: "target-v1".to_string(),
            definition_fingerprint: "definition-v1".to_string(),
        }
    }

    fn contract() -> RefreshPlanContract {
        RefreshPlanContract {
            mv_id: Some(42),
            target: target("mv"),
            storage_engine: MvStorageEngine::Iceberg,
            decision: ExecutableRefreshDecision::Incremental,
            state_baseline: snapshot_baseline(),
            base_refs: vec![table("left"), table("right")],
            snapshot_pins: BTreeMap::from([
                ("ice.db.left".to_string(), Some(3)),
                ("ice.db.right".to_string(), Some(4)),
            ]),
            affected_partitions: AffectedTargetPartitions::not_derived("test"),
        }
    }

    fn validate<'a>(
        contract: &'a RefreshPlanContract,
        backend: MvStorageEngine,
        mv_id: Option<i64>,
        target: &MvTarget,
        base_refs: &[TableIdentity],
        state_baseline: &RefreshStateBaseline,
        snapshot_pins: Option<&BTreeMap<String, Option<i64>>>,
    ) -> Result<ValidatedRefreshExecution<'a>, String> {
        validate_refresh_execution(
            contract,
            &RefreshExecutionObservation {
                backend,
                mv_id,
                target,
                base_refs,
                state_baseline,
                snapshot_pins,
            },
        )
    }

    #[test]
    fn rejects_snapshot_pin_drift() {
        let contract = contract();
        let mut observed_pins = contract.snapshot_pins.clone();
        observed_pins.insert("ice.db.left".to_string(), Some(99));

        let error = validate(
            &contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &contract.target,
            &contract.base_refs,
            &contract.state_baseline,
            Some(&observed_pins),
        )
        .unwrap_err();

        assert!(error.contains("snapshot pin"), "{error}");
        assert!(error.contains("ice.db.left"), "{error}");
    }

    #[test]
    fn accepts_base_ref_order_changes() {
        let contract = contract();
        let reordered = vec![table("right"), table("left")];

        validate(
            &contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &contract.target,
            &reordered,
            &contract.state_baseline,
            Some(&contract.snapshot_pins),
        )
        .expect("base reference order must not affect identity validation");
    }

    #[test]
    fn rejects_identity_and_baseline_drift_fail_closed() {
        let contract = contract();
        let wrong_target = target("other_mv");
        let duplicate_bases = vec![table("left"), table("left")];
        let replacement_bases = vec![table("left"), table("replacement")];
        let pinless = RefreshStateBaseline::Pinless;

        let cases = [
            (
                "backend",
                validate(
                    &contract,
                    MvStorageEngine::StarRocks,
                    Some(42),
                    &contract.target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "mv id",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(43),
                    &contract.target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "target",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &wrong_target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "duplicate",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &duplicate_bases,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "base refs",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &replacement_bases,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "state baseline",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &contract.base_refs,
                    &pinless,
                    Some(&contract.snapshot_pins),
                ),
            ),
        ];

        for (expected, result) in cases {
            let error = result.unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?} in {error:?}"
            );
        }
    }

    #[test]
    fn rejects_pin_key_and_option_value_drift() {
        let contract = contract();
        let mut missing = contract.snapshot_pins.clone();
        missing.remove("ice.db.right");
        let mut extra = contract.snapshot_pins.clone();
        extra.insert("ice.db.extra".to_string(), Some(5));
        let mut some_to_none = contract.snapshot_pins.clone();
        some_to_none.insert("ice.db.left".to_string(), None);

        for pins in [&missing, &extra, &some_to_none] {
            assert!(
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(pins),
                )
                .is_err()
            );
        }

        let mut none_planned = contract.clone();
        none_planned
            .snapshot_pins
            .insert("ice.db.left".to_string(), None);
        assert!(
            validate(
                &none_planned,
                MvStorageEngine::Iceberg,
                Some(42),
                &none_planned.target,
                &none_planned.base_refs,
                &none_planned.state_baseline,
                Some(&contract.snapshot_pins),
            )
            .unwrap_err()
            .contains("snapshot pin")
        );
    }

    #[test]
    fn rejects_planned_pin_keys_that_do_not_match_contract_bases() {
        let mut missing = contract();
        missing.snapshot_pins.remove("ice.db.right");
        let mut extra = contract();
        extra
            .snapshot_pins
            .insert("ice.db.extra".to_string(), Some(5));

        for contract in [&missing, &extra] {
            let error = validate(
                contract,
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                &contract.base_refs,
                &contract.state_baseline,
                Some(&contract.snapshot_pins),
            )
            .unwrap_err();
            assert!(error.contains("planned snapshot pin keys"), "{error}");
        }
    }

    #[test]
    fn rejects_duplicate_planned_and_observed_base_refs() {
        let mut duplicate_contract = contract();
        duplicate_contract.base_refs = vec![table("left"), table("left")];
        let error = validate(
            &duplicate_contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &duplicate_contract.target,
            &duplicate_contract.base_refs,
            &duplicate_contract.state_baseline,
            Some(&duplicate_contract.snapshot_pins),
        )
        .unwrap_err();
        assert!(
            error.contains("planned base refs contain duplicate"),
            "{error}"
        );

        let contract = contract();
        let duplicate_observed = vec![table("left"), table("left")];
        let error = validate(
            &contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &contract.target,
            &duplicate_observed,
            &contract.state_baseline,
            Some(&contract.snapshot_pins),
        )
        .unwrap_err();
        assert!(
            error.contains("observed base refs contain duplicate"),
            "{error}"
        );
    }

    #[test]
    fn rejects_each_snapshot_backed_baseline_field_drift() {
        let contract = contract();
        let RefreshStateBaseline::SnapshotBacked {
            previous_snapshot_ids,
            previous_table_uuids,
            target_snapshot_id,
            target_table_uuid,
            definition_fingerprint,
        } = snapshot_baseline()
        else {
            unreachable!()
        };

        let mut changed_previous_snapshots = previous_snapshot_ids.clone();
        changed_previous_snapshots.insert("ice.db.left".to_string(), 99);
        let mut changed_previous_uuids = previous_table_uuids.clone();
        changed_previous_uuids.insert("ice.db.left".to_string(), "left-v2".to_string());
        let drifts = [
            RefreshStateBaseline::SnapshotBacked {
                previous_snapshot_ids: changed_previous_snapshots,
                previous_table_uuids: previous_table_uuids.clone(),
                target_snapshot_id,
                target_table_uuid: target_table_uuid.clone(),
                definition_fingerprint: definition_fingerprint.clone(),
            },
            RefreshStateBaseline::SnapshotBacked {
                previous_snapshot_ids: previous_snapshot_ids.clone(),
                previous_table_uuids: changed_previous_uuids,
                target_snapshot_id,
                target_table_uuid: target_table_uuid.clone(),
                definition_fingerprint: definition_fingerprint.clone(),
            },
            RefreshStateBaseline::SnapshotBacked {
                previous_snapshot_ids: previous_snapshot_ids.clone(),
                previous_table_uuids: previous_table_uuids.clone(),
                target_snapshot_id: Some(11),
                target_table_uuid: target_table_uuid.clone(),
                definition_fingerprint: definition_fingerprint.clone(),
            },
            RefreshStateBaseline::SnapshotBacked {
                previous_snapshot_ids: previous_snapshot_ids.clone(),
                previous_table_uuids: previous_table_uuids.clone(),
                target_snapshot_id,
                target_table_uuid: "target-v2".to_string(),
                definition_fingerprint: definition_fingerprint.clone(),
            },
            RefreshStateBaseline::SnapshotBacked {
                previous_snapshot_ids,
                previous_table_uuids,
                target_snapshot_id,
                target_table_uuid,
                definition_fingerprint: "definition-v2".to_string(),
            },
        ];

        for drift in &drifts {
            let error = validate(
                &contract,
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                &contract.base_refs,
                drift,
                Some(&contract.snapshot_pins),
            )
            .unwrap_err();
            assert!(error.contains("state baseline"), "{error}");
        }
    }

    #[test]
    fn validation_reports_identity_failures_in_contract_order() {
        let contract = contract();
        let wrong_target = target("wrong");
        let wrong_bases = vec![table("wrong")];
        let pinless = RefreshStateBaseline::Pinless;
        let empty_pins = BTreeMap::new();

        let observations = [
            (
                MvStorageEngine::StarRocks,
                Some(99),
                &wrong_target,
                wrong_bases.as_slice(),
                &pinless,
                "backend",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(99),
                &wrong_target,
                wrong_bases.as_slice(),
                &pinless,
                "mv id",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(42),
                &wrong_target,
                wrong_bases.as_slice(),
                &pinless,
                "target",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                wrong_bases.as_slice(),
                &pinless,
                "base refs",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                contract.base_refs.as_slice(),
                &pinless,
                "state baseline",
            ),
        ];

        for (backend, mv_id, target, bases, baseline, expected) in observations {
            let error = validate(
                &contract,
                backend,
                mv_id,
                target,
                bases,
                baseline,
                Some(&empty_pins),
            )
            .unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?} in {error:?}"
            );
        }
    }

    #[test]
    fn pinless_contract_requires_empty_plans_and_no_observed_pin_map() {
        let mut contract = contract();
        contract.storage_engine = MvStorageEngine::StarRocks;
        contract.mv_id = None;
        contract.state_baseline = RefreshStateBaseline::Pinless;
        contract.snapshot_pins.clear();

        validate(
            &contract,
            MvStorageEngine::StarRocks,
            None,
            &contract.target,
            &contract.base_refs,
            &contract.state_baseline,
            None,
        )
        .expect("valid pinless contract should pass");

        let empty = BTreeMap::new();
        let error = validate(
            &contract,
            MvStorageEngine::StarRocks,
            None,
            &contract.target,
            &contract.base_refs,
            &contract.state_baseline,
            Some(&empty),
        )
        .unwrap_err();
        assert!(error.contains("pinless"), "{error}");
    }

    #[test]
    fn metadata_only_dispatches_only_metadata_closure() {
        let calls = RefCell::new(Vec::new());
        let result = dispatch_refresh_decision(
            ExecutableRefreshDecision::MetadataOnly,
            || {
                calls.borrow_mut().push("skip");
                Ok::<_, String>(0)
            },
            || {
                calls.borrow_mut().push("first");
                Ok(1)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(2)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(3)
            },
        )
        .unwrap();

        assert_eq!(result, 2);
        assert_eq!(*calls.borrow(), vec!["metadata"]);
    }

    #[test]
    fn dispatches_each_executable_decision_exactly_once() {
        for (decision, expected) in [
            (ExecutableRefreshDecision::SkipEmpty, "skip"),
            (ExecutableRefreshDecision::FirstRefresh, "first"),
            (ExecutableRefreshDecision::MetadataOnly, "metadata"),
            (ExecutableRefreshDecision::Incremental, "incremental"),
        ] {
            let calls = RefCell::new(Vec::new());
            let actual = dispatch_refresh_decision(
                decision,
                || {
                    calls.borrow_mut().push("skip");
                    Ok::<_, String>("skip")
                },
                || {
                    calls.borrow_mut().push("first");
                    Ok("first")
                },
                || {
                    calls.borrow_mut().push("metadata");
                    Ok("metadata")
                },
                || {
                    calls.borrow_mut().push("incremental");
                    Ok("incremental")
                },
            )
            .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(*calls.borrow(), vec![expected]);
        }
    }
}
