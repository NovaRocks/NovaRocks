// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Finite test-only expiry of inline MV documents and their old snapshots.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_mv_application::management::{
    EffectDisposition, EffectIdentity, EffectResponsibility, EffectScope, ManagementRequest,
    ManagementTimestamp,
};
use novarocks_mv_application::persistence::keyed_documents::decode_current_keyed_documents;
use novarocks_parser::ast::{CallStatement, LiteralKind, MaintenanceValue, ProcedureArgumentMode};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::{
    CatalogHandle, ConnectorCatalogMutationOperation, ConnectorControlResolver,
    ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
    ConnectorInstanceId, ConnectorMutationOperationId, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
    ConnectorTableResolution, ExternalMutationFinalization, FullKeyedDocumentView,
    KeyedDocumentObservationRequest, KeyedExpiry, KeyedMutation, KeyedMutationIntent,
    KeyedSnapshotObservation,
};
use uuid::Uuid;

use super::iceberg_refresh::IcebergMvCorePorts;
use crate::connector::mutation::{MutationDispatchState, ResolvedCatalogMutation};

const PROCEDURE: &str = "novarocks_mv_document_expiry";
const MAX_ARGUMENT_BYTES: usize = 4096;

#[derive(Debug, PartialEq, Eq)]
struct ExpiryCall {
    catalog: String,
    namespace: String,
    mv: String,
    keep_last: usize,
    token: String,
}

fn decode_call(call: &CallStatement) -> Result<Option<ExpiryCall>, String> {
    if !call
        .procedure
        .parts
        .last()
        .is_some_and(|part| part.value.eq_ignore_ascii_case(PROCEDURE))
    {
        return Ok(None);
    }
    let [catalog, system, _] = call.procedure.parts.as_slice() else {
        return Err(format!("{PROCEDURE} requires catalog.system.{PROCEDURE}"));
    };
    if catalog.value.is_empty() || !system.value.eq_ignore_ascii_case("system") {
        return Err(format!("{PROCEDURE} requires catalog.system.{PROCEDURE}"));
    }
    if call.argument_mode != ProcedureArgumentMode::Named || call.arguments.len() != 3 {
        return Err(format!(
            "{PROCEDURE} requires exactly table, keep_last and token named arguments"
        ));
    }
    let mut arguments = BTreeMap::new();
    for argument in &call.arguments {
        let name = argument
            .name
            .as_ref()
            .ok_or("MV document expiry arguments must be named")?
            .value
            .to_ascii_lowercase();
        if !matches!(name.as_str(), "table" | "keep_last" | "token")
            || arguments.insert(name, &argument.value).is_some()
        {
            return Err("MV document expiry has an unknown or duplicate argument".into());
        }
    }
    let text = |name: &str| -> Result<&str, String> {
        let Some(MaintenanceValue::Literal(literal)) = arguments.get(name).copied() else {
            return Err(format!(
                "MV document expiry {name} must be a string literal"
            ));
        };
        let LiteralKind::String(value) = &literal.kind else {
            return Err(format!(
                "MV document expiry {name} must be a string literal"
            ));
        };
        if value.is_empty() || value.len() > MAX_ARGUMENT_BYTES || value.contains(['\n', '\r']) {
            return Err(format!(
                "MV document expiry {name} is empty, oversized or contains a line break"
            ));
        }
        Ok(value)
    };
    let table = text("table")?;
    let parts: Vec<_> = table.split('.').collect();
    let [namespace, mv] = parts.as_slice() else {
        return Err("MV document expiry table must be exactly namespace.mv".into());
    };
    if namespace.is_empty() || mv.is_empty() || namespace.trim() != *namespace || mv.trim() != *mv {
        return Err("MV document expiry table must be exactly namespace.mv".into());
    }
    let Some(MaintenanceValue::Literal(literal)) = arguments.get("keep_last").copied() else {
        return Err("MV document expiry keep_last must be a positive integer literal".into());
    };
    let LiteralKind::Number(number) = &literal.kind else {
        return Err("MV document expiry keep_last must be a positive integer literal".into());
    };
    let keep_last = number
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .filter(|_| number.bytes().all(|b| b.is_ascii_digit()))
        .ok_or("MV document expiry keep_last must be a positive integer literal")?;
    Ok(Some(ExpiryCall {
        catalog: catalog.value.clone(),
        namespace: (*namespace).into(),
        mv: (*mv).into(),
        keep_last,
        token: text("token")?.into(),
    }))
}

/// Preserve every named ref head and the newest N snapshots on main's parent
/// chain. An already-expired parent ends that chain; an absent ref head or a
/// cycle cannot be planned safely.
fn plan_remove_snapshots(
    snapshots: &[KeyedSnapshotObservation],
    refs: &BTreeMap<String, i64>,
    current: Option<i64>,
    keep_last: usize,
) -> Result<BTreeSet<i64>, String> {
    if keep_last == 0 {
        return Err("MV document expiry keep_last must be positive".into());
    }
    let mut by_id = BTreeMap::new();
    for snapshot in snapshots {
        if by_id.insert(snapshot.snapshot_id, snapshot).is_some() {
            return Err("MV document expiry full observation has duplicate snapshot IDs".into());
        }
    }
    if refs.get("main").is_some_and(|main| Some(*main) != current) {
        return Err("MV document expiry main ref disagrees with Current".into());
    }
    let mut retain = BTreeSet::new();
    for head in refs.values() {
        if !by_id.contains_key(head) {
            return Err("MV document expiry ref head is absent from the full observation".into());
        }
        retain.insert(*head);
    }
    let mut next = current;
    let mut visited = BTreeSet::new();
    for _ in 0..keep_last {
        let Some(id) = next else { break };
        let Some(snapshot) = by_id.get(&id) else {
            break;
        };
        if !visited.insert(id) {
            return Err("MV document expiry main parent chain contains a cycle".into());
        }
        retain.insert(id);
        next = snapshot.parent;
    }
    Ok(by_id
        .keys()
        .filter(|id| !retain.contains(id))
        .copied()
        .collect())
}

pub(crate) fn try_execute_document_expiry(
    ports: &IcebergMvCorePorts,
    call: &CallStatement,
    _current_database: &str,
    context: &ConnectorRequestContext,
    on_reconciled_current: &dyn Fn(
        CatalogHandle,
        novarocks_mv_application::product::MvTarget,
        Uuid,
        ConnectorRequestContext,
    ) -> Result<(), String>,
    on_committed: &dyn Fn(
        CatalogHandle,
        novarocks_mv_application::product::MvTarget,
        Uuid,
        ConnectorRequestContext,
    ) -> Result<(), String>,
) -> Result<Option<StatementResult>, String> {
    let Some(call) = decode_call(call)? else {
        return Ok(None);
    };
    // Claim authorization before any provider work. Reconstructing this owner
    // or changing keep_last cannot renew a consumed target/token selection.
    let mut claim = ports
        .readiness()
        .test_controls()
        .ok_or("MV document expiry requires an active finite test grant")?
        .claim_document_expiry(&call.catalog, &call.namespace, &call.mv, &call.token)?;
    let target = novarocks_mv_application::product::MvTarget::from_parts(
        Some(&call.catalog),
        &call.namespace,
        &call.mv,
    );
    let instance = ConnectorInstanceId::parse(&call.catalog).map_err(|e| e.to_string())?;
    let table = ConnectorTableIdentity {
        instance_id: instance.clone(),
        namespace: Arc::from(call.namespace.as_str()),
        table: Arc::from(call.mv.as_str()),
    };
    let control = ConnectorControlResolver::acquire_current(ports.connector_control(), &instance)
        .map_err(|e| format!("acquire MV document expiry catalog: {e}"))?;
    let catalog = control
        .binding()
        .catalog_handle()
        .map_err(|e| format!("bind MV document expiry catalog: {e}"))?
        .clone();
    let binding = control
        .binding()
        .metadata()
        .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
            table: table.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context.clone(),
        })
        .map_err(|e| format!("bind MV document expiry target: {e}"))?;
    if binding.metadata.identity != table {
        return Err("MV document expiry provider bound another target".into());
    }
    let documents = control
        .derive_document_storage_lease()
        .map_err(|e| e.to_string())?;
    let request = |context: ConnectorRequestContext| KeyedDocumentObservationRequest {
        owner: documents.owner().clone(),
        catalog_handle: catalog.clone(),
        target: table.clone(),
        object_id: binding.object_id.clone(),
        context,
    };
    let entrance = ports.management_entrance()?.as_ref();
    let (initial, _, convergence) =
        match entrance.observe_keyed_admission(&documents, request(context.clone())) {
            Ok(observed) => observed,
            Err(error) => {
                entrance
                    .close_on_keyed_current_management_mismatch(
                        &documents,
                        request(context.clone().after_external_effect()),
                    )
                    .map_err(|e| e.to_string())?;
                return Err(format!(
                    "admit fresh MV document expiry observation: {error}"
                ));
            }
        };
    if convergence.reinstall_current_projection {
        on_reconciled_current(
            catalog.clone(),
            target.clone(),
            Uuid::now_v7(),
            context.clone().after_external_effect(),
        )?;
    }
    let dependencies = initial.management_dependencies(control.control_runtime_id());
    let mut management = entrance
        .acquire(
            ManagementRequest::try_new(
                catalog.clone(),
                table.clone(),
                Some(binding.object_id.clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .map_err(|e| format!("build MV document expiry admission: {e:?}"))?,
            || context.is_cancelled() || std::time::Instant::now() >= context.deadline(),
        )
        .map_err(|e| format!("acquire MV document expiry admission: {e:?}"))?;
    // FIFO admission may outlive another local writer. This exact fresh full
    // load and sealed token, not the pre-admission observation, become expiry's base.
    let admitted_context = context.clone().after_external_effect();
    let mut full = documents
        .observe_full_keyed_documents(request(admitted_context.clone()))
        .map_err(|e| format!("load full MV document expiry observation: {e}"))?;
    let convergence = entrance
        .resolve_pending_slot_changes(&documents, request(admitted_context.clone()))
        .map_err(|e| e.to_string())?;
    if convergence.reinstall_current_projection {
        full = documents
            .observe_full_keyed_documents(request(admitted_context.clone().after_external_effect()))
            .map_err(|e| e.to_string())?;
        on_reconciled_current(
            catalog.clone(),
            target.clone(),
            Uuid::now_v7(),
            admitted_context.clone().after_external_effect(),
        )?;
    }
    let observed = decode_current_keyed_documents(full.current.clone(), catalog.clone())
        .map_err(|e| format!("decode MV document expiry Current: {e}"))?;
    if observed.management_target != initial.management_target
        || observed.deployment_owner != *entrance.owner()
        || observed.process_incarnation != *entrance.incarnation()
        || observed.management_dependencies(control.control_runtime_id()) != dependencies
    {
        entrance
            .close_on_keyed_current_management_mismatch(
                &documents,
                request(admitted_context.clone().after_external_effect()),
            )
            .map_err(|e| e.to_string())?;
        return Err("MV document expiry target, owner, incarnation or dependencies changed during admission".into());
    }
    let remove_snapshots = plan_full_expiry(&full, call.keep_last)?;
    let operation_uuid = Uuid::now_v7();
    let operation_id = ConnectorMutationOperationId::from_bytes(*operation_uuid.as_bytes());
    let admission = documents
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                documents.owner().clone(),
                catalog.clone(),
                operation_id,
                table,
                Some(binding.object_id.clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                admitted_context.clone(),
            )
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    let intent = KeyedMutationIntent {
        admission,
        expected_metadata: full.current.metadata_version.clone(),
        expected_snapshot: full.current.current_snapshot,
        mutation: KeyedMutation::Expiry(KeyedExpiry {
            observation: full.token,
            remove_snapshots,
        }),
    };
    intent
        .validate_for(documents.owner(), operation_id)
        .map_err(|e| e.to_string())?;
    let mutation = control.derive_mutation_lease().map_err(|e| e.to_string())?;
    // The optional external intervention occurs after planning and before
    // mark_dispatched. The exact immutable full observation remains attached.
    claim.after_planning(&admitted_context)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch")?;
    let timestamp = u64::try_from(now.as_millis())
        .map(ManagementTimestamp::from_unix_millis)
        .map_err(|_| "system clock exceeds u64 milliseconds")?;
    management
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes(operation_id.to_bytes()),
            observed.management_target,
            entrance.incarnation().clone(),
            EffectScope::CATALOG_COMMIT,
            timestamp,
        ))
        .map_err(|e| format!("mark MV document expiry dispatched: {e:?}"))?;
    let resolved = crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
        &mutation,
        operation_id,
        ConnectorCatalogMutationOperation::KeyedDocuments { intent },
        admitted_context.clone(),
    );
    management
        .record_terminal(disposition(&resolved))
        .map_err(|e| format!("record MV document expiry terminal: {e:?}"))?;
    match resolved {
        ResolvedCatalogMutation::KnownCommitted(completed) => {
            on_committed(
                catalog,
                target,
                operation_uuid,
                context.clone().after_external_effect(),
            )?;
            if let ExternalMutationFinalization::Failed(error) = completed.finalization {
                return Err(format!(
                    "MV document expiry committed but finalization failed: {error}"
                ));
            }
            Ok(Some(StatementResult::Ok))
        }
        ResolvedCatalogMutation::KnownUncommitted { failure, cleanup } => Err(format!(
            "MV document expiry did not commit: {}",
            crate::connector::mutation::known_uncommitted_message(failure, &cleanup)
        )),
        ResolvedCatalogMutation::CommitUnknown { failure, .. } => Err(format!(
            "MV document expiry outcome is unknown; management remains unsettled: {failure}"
        )),
        ResolvedCatalogMutation::ContractFailure { error, .. } => {
            Err(format!("MV document expiry contract failure: {error}"))
        }
    }
}

fn plan_full_expiry(
    full: &FullKeyedDocumentView,
    keep_last: usize,
) -> Result<BTreeSet<i64>, String> {
    plan_remove_snapshots(
        &full.snapshots,
        &full.refs,
        full.current.current_snapshot,
        keep_last,
    )
}
fn disposition(resolved: &ResolvedCatalogMutation) -> EffectDisposition {
    match resolved {
        ResolvedCatalogMutation::KnownCommitted(_) => EffectDisposition::KnownCommitted,
        ResolvedCatalogMutation::KnownUncommitted { .. }
        | ResolvedCatalogMutation::ContractFailure {
            dispatch: MutationDispatchState::ConfirmedNotDispatched,
            ..
        } => EffectDisposition::KnownUncommitted,
        ResolvedCatalogMutation::CommitUnknown { .. }
        | ResolvedCatalogMutation::ContractFailure {
            dispatch: MutationDispatchState::PossiblyDispatched,
            ..
        } => EffectDisposition::CommitUnknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::ast::{MaintenanceStatement, Statement};

    fn call(sql: &str) -> CallStatement {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [Statement::Maintenance(MaintenanceStatement::Call(call))] = statements.as_slice()
        else {
            panic!("CALL")
        };
        call.clone()
    }
    #[test]
    fn document_expiry_call_is_strict_and_other_procedures_miss() {
        let valid = call(
            "CALL ice.system.novarocks_mv_document_expiry(table => 'sales.mv', keep_last => 2, token => 'once')",
        );
        assert_eq!(
            decode_call(&valid).unwrap(),
            Some(ExpiryCall {
                catalog: "ice".into(),
                namespace: "sales".into(),
                mv: "mv".into(),
                keep_last: 2,
                token: "once".into()
            })
        );
        assert!(
            decode_call(&call("CALL ice.system.other(table => 'sales.mv')"))
                .unwrap()
                .is_none()
        );
        for sql in [
            "CALL novarocks_mv_document_expiry(table => 'sales.mv', keep_last => 2, token => 'once')",
            "CALL ice.other.novarocks_mv_document_expiry(table => 'sales.mv', keep_last => 2, token => 'once')",
            "CALL ice.system.novarocks_mv_document_expiry('sales.mv', 2, 'once')",
            "CALL ice.system.novarocks_mv_document_expiry(table => 'mv', keep_last => 2, token => 'once')",
            "CALL ice.system.novarocks_mv_document_expiry(table => 'ice.sales.mv', keep_last => 2, token => 'once')",
            "CALL ice.system.novarocks_mv_document_expiry(table => 'sales.mv', keep_last => 0, token => 'once')",
            "CALL ice.system.novarocks_mv_document_expiry(table => 'sales.mv', keep_last => 1.5, token => 'once')",
            "CALL ice.system.novarocks_mv_document_expiry(table => 'sales.mv', keep_last => '2', token => 'once')",
            "CALL ice.system.novarocks_mv_document_expiry(table => 'sales.mv', keep_last => 2, token => '')",
            "CALL ice.system.novarocks_mv_document_expiry(table => 'sales.mv', keep_last => 2, directory => '/tmp')",
        ] {
            assert!(decode_call(&call(sql)).is_err(), "{sql}");
        }
        let mut duplicate = valid;
        duplicate.arguments[2].name = duplicate.arguments[0].name.clone();
        assert!(decode_call(&duplicate).is_err());
    }
    fn snapshot(id: i64, parent: Option<i64>) -> KeyedSnapshotObservation {
        KeyedSnapshotObservation {
            snapshot_id: id,
            parent,
            records: Vec::new(),
        }
    }
    #[test]
    fn document_expiry_preserves_all_ref_heads_and_main_latest_n() {
        let snapshots = vec![
            snapshot(1, None),
            snapshot(2, Some(1)),
            snapshot(3, Some(2)),
            snapshot(4, Some(3)),
            snapshot(5, Some(2)),
            snapshot(6, Some(5)),
        ];
        let refs = BTreeMap::from([("main".into(), 4), ("tag".into(), 1), ("branch".into(), 6)]);
        assert_eq!(
            plan_remove_snapshots(&snapshots, &refs, Some(4), 2).unwrap(),
            BTreeSet::from([2, 5])
        );
        assert_eq!(
            plan_remove_snapshots(&snapshots, &refs, Some(4), 1).unwrap(),
            BTreeSet::from([2, 3, 5])
        );
        assert_eq!(
            plan_remove_snapshots(&snapshots, &refs, Some(4), 20).unwrap(),
            BTreeSet::from([5])
        );
        // A removed ancestor is a normal finite-history boundary.
        assert!(
            plan_remove_snapshots(
                &[snapshot(4, Some(3))],
                &BTreeMap::from([("main".into(), 4)]),
                Some(4),
                2
            )
            .unwrap()
            .is_empty()
        );
        // Current is the main-chain head even if the provider's metadata has
        // no explicit main entry in its optional refs map.
        assert_eq!(
            plan_remove_snapshots(&snapshots, &BTreeMap::new(), Some(4), 2).unwrap(),
            BTreeSet::from([1, 2, 5, 6])
        );
        assert!(
            plan_remove_snapshots(&[], &BTreeMap::new(), None, 2)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn document_expiry_refuses_ambiguous_full_history() {
        let refs = BTreeMap::from([("main".into(), 2)]);
        assert!(
            plan_remove_snapshots(&[snapshot(2, None), snapshot(2, None)], &refs, Some(2), 1)
                .is_err()
        );
        assert!(plan_remove_snapshots(&[], &refs, Some(2), 1).is_err());
        assert!(plan_remove_snapshots(&[snapshot(2, None)], &refs, Some(3), 1).is_err());
        assert!(
            plan_remove_snapshots(
                &[snapshot(2, Some(1)), snapshot(1, Some(2))],
                &refs,
                Some(2),
                3
            )
            .is_err()
        );
        assert!(plan_remove_snapshots(&[], &BTreeMap::new(), None, 0).is_err());
    }
}
