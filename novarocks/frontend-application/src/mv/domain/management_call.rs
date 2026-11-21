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

//! The operator's view of MV management continuation, as SQL.
//!
//! These are deliberately three named procedures with fixed argument lists
//! rather than one general-purpose operation endpoint: each argument is part
//! of a decision an operator has to be able to state exactly, and an interface
//! that accepts arbitrary payloads makes that statement unreviewable.
//!
//! The target is three separate arguments because a dotted string would have
//! to be re-parsed, and an MV named with a dot would then be addressed as a
//! different object than the one the operator meant.

use std::sync::Arc;

use novarocks_mv_application::management::{
    DeploymentOwner, ManagementAuditAction, ManagementAuditOutcome, ManagementAuditRecord,
    ManagementAuditSink, ManagementContinuationService, ManagementTimestamp, MvManagementStatus,
    MvResumeDeclaration, ProcessIncarnation, ReadmissionChallenge, ReadmissionMode,
    ReadmissionPermit,
};
use novarocks_parser::ast::LiteralKind;
use novarocks_query_application::api::{QueryResult, build_utf8_table_query_result};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_query_application::sql::PreparedMaintenanceCall;
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorTableIdentity};

use crate::mv::domain::management_handover::MvOwnerHandoverOutcome;

const STATUS_PROCEDURE: &str = "novarocks_mv_management_status";
const RESUME_PROCEDURE: &str = "novarocks_mv_resume_management";
const SET_OWNER_PROCEDURE: &str = "novarocks_mv_set_owner";

/// The largest an operator-supplied text argument may be.
///
/// Evidence is a statement a human writes and another human reviews, not a
/// payload; a bound keeps one command from filling the audit record.
const MAX_ARGUMENT_BYTES: usize = 4096;
/// `novarocks_mv_set_owner` takes no operator declaration: it asserts nothing
/// about a writer elsewhere, so there is no statement to attribute. The audit
/// record says so rather than repeating the authenticated principal into the
/// field that exists to hold an operator's own claim.
const SET_OWNER_OPERATOR_REFERENCE: &str =
    "no operator declaration: novarocks_mv_set_owner acts on the authenticated session alone";

/// One MV named by its three parts, exactly as the operator wrote them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManagementCallTarget {
    pub(crate) catalog: String,
    pub(crate) database: String,
    pub(crate) name: String,
}

impl ManagementCallTarget {
    /// The same exact identity, for a caller outside this module.
    pub(crate) fn connector_table(&self) -> Result<ConnectorTableIdentity, String> {
        self.table()
    }

    fn table(&self) -> Result<ConnectorTableIdentity, String> {
        Ok(ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(&self.catalog)
                .map_err(|error| format!("parse MV management catalog identity: {error}"))?,
            namespace: Arc::from(self.database.as_str()),
            table: Arc::from(self.name.as_str()),
        })
    }
}

/// One decoded MV management procedure call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ManagementCall {
    Status {
        target: ManagementCallTarget,
    },
    Resume {
        target: ManagementCallTarget,
        challenge: String,
        old_incarnation: String,
        operator: String,
        evidence: String,
    },
    SetOwner {
        target: ManagementCallTarget,
        challenge: String,
        new_deployment: String,
    },
}

impl ManagementCall {
    /// Decode one of the three fixed shapes, or report that this CALL is not
    /// an MV management procedure at all.
    ///
    /// A procedure this module owns but cannot decode is an error, never a
    /// route miss: silently falling through would let a mistyped management
    /// statement be reported as an unknown procedure.
    pub(crate) fn try_decode(statement: &PreparedMaintenanceCall) -> Result<Option<Self>, String> {
        let Some((procedure, arity)) = owned_procedure(&statement.procedure) else {
            return Ok(None);
        };
        let arguments = positional_text_arguments(statement, procedure, arity)?;
        let target = ManagementCallTarget {
            catalog: arguments[0].clone(),
            database: arguments[1].clone(),
            name: arguments[2].clone(),
        };
        Ok(Some(match procedure {
            STATUS_PROCEDURE => Self::Status { target },
            RESUME_PROCEDURE => Self::Resume {
                target,
                challenge: arguments[3].clone(),
                old_incarnation: arguments[4].clone(),
                operator: arguments[5].clone(),
                evidence: arguments[6].clone(),
            },
            _ => Self::SetOwner {
                target,
                challenge: arguments[3].clone(),
                new_deployment: arguments[4].clone(),
            },
        }))
    }
}

/// Identify a management procedure before argument decoding.
pub(crate) fn owned_procedure(
    name: &novarocks_parser::ast::ObjectName,
) -> Option<(&'static str, usize)> {
    let [part] = name.parts.as_slice() else {
        return None;
    };
    match part.value.to_ascii_lowercase().as_str() {
        STATUS_PROCEDURE => Some((STATUS_PROCEDURE, 3)),
        RESUME_PROCEDURE => Some((RESUME_PROCEDURE, 7)),
        SET_OWNER_PROCEDURE => Some((SET_OWNER_PROCEDURE, 5)),
        _ => None,
    }
}

/// Execute one MV management procedure, or leave the CALL to its own consumer.
pub(crate) fn try_execute_management_call(
    continuation: Option<&Arc<ManagementContinuationService>>,
    audit: Option<&Arc<dyn ManagementAuditSink>>,
    operations: &dyn MvManagementOperations,
    session_principal: &str,
    statement: &PreparedMaintenanceCall,
) -> Result<Option<StatementResult>, String> {
    let Some(call) = ManagementCall::try_decode(statement)? else {
        return Ok(None);
    };
    let continuation = continuation.ok_or_else(|| {
        "MV management procedures require the serving product's management authority".to_string()
    })?;
    match call {
        ManagementCall::Status { target } => execute_status(continuation, &target)
            .map(StatementResult::Query)
            .map(Some),
        ManagementCall::Resume {
            target,
            challenge,
            old_incarnation,
            operator,
            evidence,
        } => execute_resume(
            continuation,
            require_audit(audit, RESUME_PROCEDURE)?,
            operations,
            session_principal,
            ResumeRequest {
                target,
                challenge,
                old_incarnation,
                operator,
                evidence,
            },
        )
        .map(StatementResult::Query)
        .map(Some),
        ManagementCall::SetOwner {
            target,
            challenge,
            new_deployment,
        } => execute_set_owner(
            continuation,
            require_audit(audit, SET_OWNER_PROCEDURE)?,
            operations,
            session_principal,
            SetOwnerRequest {
                target,
                challenge,
                new_deployment,
            },
        )
        .map(StatementResult::Query)
        .map(Some),
    }
}

/// A declaration is a statement nobody can check afterwards unless it was
/// written down, so it is refused outright where there is nowhere to write it.
fn require_audit<'a>(
    audit: Option<&'a Arc<dyn ManagementAuditSink>>,
    procedure: &str,
) -> Result<&'a Arc<dyn ManagementAuditSink>, String> {
    audit.ok_or_else(|| {
        format!(
            "{procedure} requires a management audit sink; set [mv_management].audit_log so a declaration can be recorded before it takes effect"
        )
    })
}

pub(crate) struct ResumeRequest {
    target: ManagementCallTarget,
    challenge: String,
    old_incarnation: String,
    operator: String,
    evidence: String,
}

/// The readmission a resume performs once its declaration is admitted.
///
/// It is a port because the provider observation it drives belongs to the
/// adapter, while the declaration that permits it belongs here.
pub(crate) trait MvManagementOperations: Send + Sync {
    fn readmit(
        &self,
        target: &ManagementCallTarget,
        previous_incarnation: ProcessIncarnation,
        permits: Vec<ReadmissionPermit>,
    ) -> Result<(), String>;

    /// Rewrite the target's managed owner. The adapter owns the exact
    /// connector generation and the observation the marker is replaced
    /// against; the challenge that paid for the statement was already spent
    /// by the management service.
    fn hand_over(
        &self,
        target: &ManagementCallTarget,
        new_owner: DeploymentOwner,
    ) -> Result<MvOwnerHandoverOutcome, String>;
}

/// Record the attempt, readmit, record the outcome.
///
/// The record comes first because a declaration that could not be written must
/// not act. The outcome is recorded whichever way it went: a declaration that
/// was accepted and then failed is exactly the case an operator later needs to
/// find.
fn execute_resume(
    continuation: &ManagementContinuationService,
    audit: &Arc<dyn ManagementAuditSink>,
    operations: &dyn MvManagementOperations,
    session_principal: &str,
    request: ResumeRequest,
) -> Result<QueryResult, String> {
    let table = request.target.table()?;
    let challenge = parse_challenge(&request.challenge)?;
    let old_incarnation = ProcessIncarnation::parse(&request.old_incarnation)
        .map_err(|error| format!("parse the declared old incarnation: {error:?}"))?;
    let mut record = ManagementAuditRecord {
        action: ManagementAuditAction::ResumeManagement,
        session_principal: session_principal.to_string(),
        operator_reference: request.operator.clone(),
        table: table.clone(),
        local_owner: continuation.local_owner().clone(),
        local_incarnation: continuation.local_incarnation().clone(),
        declared_old_incarnation: Some(old_incarnation.clone()),
        declared_new_owner: None,
        challenge,
        evidence_reference: request.evidence.clone(),
        outcome: ManagementAuditOutcome::Attempted,
    };
    audit.record(&record)?;

    let outcome = continuation
        .resume_target_on_declaration(
            &table,
            &MvResumeDeclaration {
                challenge,
                old_incarnation: old_incarnation.clone(),
                operator: request.operator,
                evidence: request.evidence,
                declared_at: now_management_timestamp()?,
            },
        )
        .map_err(|error| format!("admit the MV resume declaration: {error}"))
        .and_then(|permits| {
            let settled = permits.len();
            operations
                .readmit(&request.target, old_incarnation, permits)
                .map(|()| settled)
        });

    record.outcome = match &outcome {
        Ok(_) => ManagementAuditOutcome::Applied,
        Err(error) => ManagementAuditOutcome::Refused(error.clone()),
    };
    // The readmission already happened or already failed; a record that cannot
    // be written now cannot undo it, so it is reported rather than substituted
    // for the outcome.
    if let Err(error) = audit.record(&record) {
        tracing::warn!(%error, "recording the MV resume outcome failed");
    }
    let settled = outcome?;
    build_utf8_table_query_result(
        &[("Property", false), ("Value", true)],
        vec![
            row("Catalog", Some(request.target.catalog)),
            row("Database", Some(request.target.database)),
            row("Name", Some(request.target.name)),
            row("SettledEffects", Some(settled.to_string())),
            row(
                "Phase",
                Some(continuation.management_phase(&table).as_str().to_string()),
            ),
        ],
    )
}

pub(crate) struct SetOwnerRequest {
    target: ManagementCallTarget,
    challenge: String,
    new_deployment: String,
}

/// Record the attempt, hand the target over, record the outcome.
///
/// The challenge is spent here, before the adapter is asked to do anything:
/// a handover is the one statement that can move a target out of this
/// deployment's reach, so a stale or invented challenge must fail without
/// reaching the provider. The statement carries no operator declaration of its
/// own -- unlike a resume, it asserts nothing about a writer elsewhere -- so
/// the audit record says that plainly rather than borrowing the authenticated
/// principal as if it had been declared.
fn execute_set_owner(
    continuation: &ManagementContinuationService,
    audit: &Arc<dyn ManagementAuditSink>,
    operations: &dyn MvManagementOperations,
    session_principal: &str,
    request: SetOwnerRequest,
) -> Result<QueryResult, String> {
    let table = request.target.table()?;
    let challenge = parse_challenge(&request.challenge)?;
    let new_owner = DeploymentOwner::parse(&request.new_deployment)
        .map_err(|error| format!("parse the declared new deployment: {error:?}"))?;
    let mut record = ManagementAuditRecord {
        action: ManagementAuditAction::SetOwner,
        session_principal: session_principal.to_string(),
        operator_reference: SET_OWNER_OPERATOR_REFERENCE.to_string(),
        table: table.clone(),
        local_owner: continuation.local_owner().clone(),
        local_incarnation: continuation.local_incarnation().clone(),
        declared_old_incarnation: None,
        declared_new_owner: Some(new_owner.clone()),
        challenge,
        evidence_reference: format!(
            "handover of {} to deployment {} authorised by challenge {}",
            table_display(&table),
            new_owner.as_str(),
            request.challenge
        ),
        outcome: ManagementAuditOutcome::Attempted,
    };
    audit.record(&record)?;

    let outcome = continuation
        .spend_challenge(challenge)
        .map_err(|error| format!("admit the MV owner handover: {error}"))
        .and_then(|()| operations.hand_over(&request.target, new_owner.clone()));

    record.outcome = match &outcome {
        Ok(_) => ManagementAuditOutcome::Applied,
        Err(error) => ManagementAuditOutcome::Refused(error.clone()),
    };
    // The handover already happened or already failed; a record that cannot be
    // written now cannot undo it, so it is reported rather than substituted
    // for the outcome.
    if let Err(error) = audit.record(&record) {
        tracing::warn!(%error, "recording the MV set_owner outcome failed");
    }
    let outcome = outcome?;
    build_utf8_table_query_result(
        &[("Property", false), ("Value", true)],
        vec![
            row("Catalog", Some(request.target.catalog)),
            row("Database", Some(request.target.database)),
            row("Name", Some(request.target.name)),
            row("NewOwner", Some(new_owner.as_str().to_string())),
            row(
                "PreviousOwner",
                match &outcome {
                    MvOwnerHandoverOutcome::HandedOver { previous_owner } => {
                        Some(previous_owner.clone())
                    }
                },
            ),
            row(
                "Result",
                Some(
                    match &outcome {
                        MvOwnerHandoverOutcome::HandedOver { .. } => "HANDED_OVER",
                    }
                    .to_string(),
                ),
            ),
            row(
                "Phase",
                Some(continuation.management_phase(&table).as_str().to_string()),
            ),
        ],
    )
}

fn table_display(table: &novarocks_spi::connector::ConnectorTableIdentity) -> String {
    format!(
        "{}.{}.{}",
        table.instance_id.as_str(),
        table.namespace,
        table.table
    )
}

fn parse_challenge(value: &str) -> Result<ReadmissionChallenge, String> {
    uuid::Uuid::parse_str(value)
        .map(|value| ReadmissionChallenge::from_bytes(*value.as_bytes()))
        .map_err(|error| format!("parse the management challenge: {error}"))
}

fn now_management_timestamp() -> Result<ManagementTimestamp, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_string())?;
    u64::try_from(now.as_millis())
        .map(ManagementTimestamp::from_unix_millis)
        .map_err(|_| "system clock exceeds u64 milliseconds".to_string())
}

fn execute_status(
    continuation: &ManagementContinuationService,
    target: &ManagementCallTarget,
) -> Result<QueryResult, String> {
    let table = target.table()?;
    let status = continuation
        .status(&table, None, new_challenge())
        .map_err(|error| format!("read MV management status: {error}"))?;
    build_utf8_table_query_result(
        &[("Property", false), ("Value", true)],
        status_rows(&status),
    )
}

/// One row per fact an operator needs before deciding anything, including the
/// facts a following declaration must repeat back.
fn status_rows(status: &MvManagementStatus) -> Vec<Vec<Option<String>>> {
    let mut rows = vec![
        row(
            "Catalog",
            Some(status.table.instance_id.as_str().to_string()),
        ),
        row("Database", Some(status.table.namespace.to_string())),
        row("Name", Some(status.table.table.to_string())),
        row("Phase", Some(status.phase.as_str().to_string())),
        row("LocalOwner", Some(status.local_owner.as_str().to_string())),
        row(
            "LocalIncarnation",
            Some(status.local_incarnation.as_str().to_string()),
        ),
        row("UnsettledEffects", Some(status.unsettled.len().to_string())),
        row("PendingSlotChanges", Some(status.pending_slot_changes.to_string())),
        row("LatestSlotChangeConclusion", status.latest_slot_change_conclusion.as_ref().map(|c|match c {novarocks_spi::connector::document_storage::CommitConclusion::Committed(_)=>"Committed",novarocks_spi::connector::document_storage::CommitConclusion::NotCommitted(_)=>"NotCommitted",novarocks_spi::connector::document_storage::CommitConclusion::InvalidatedUndetermined=>"InvalidatedUndetermined",novarocks_spi::connector::document_storage::CommitConclusion::StillUnknown=>"StillUnknown"}.to_string())),
        row("Challenge", status.challenge.map(render_challenge)),
        row("RequiredEvidence", status.required_evidence.clone()),
        row(
            "HandoverAvailable",
            Some(
                if status.handover_available {
                    "YES"
                } else {
                    "NO"
                }
                .to_string(),
            ),
        ),
    ];
    for (index, effect) in status.unsettled.iter().enumerate() {
        let ordinal = index + 1;
        rows.push(row(
            &format!("UnsettledEffect{ordinal}Incarnation"),
            Some(effect.dispatching_incarnation.as_str().to_string()),
        ));
        rows.push(row(
            &format!("UnsettledEffect{ordinal}Continuation"),
            Some(
                match effect.mode {
                    ReadmissionMode::AutomaticWhenGuaranteed => "GUARANTEED_WINDOW",
                    ReadmissionMode::OperatorDeclarationOnly => "OPERATOR_DECLARATION",
                }
                .to_string(),
            ),
        ));
    }
    rows
}

fn row(property: &str, value: Option<String>) -> Vec<Option<String>> {
    vec![Some(property.to_string()), value]
}

fn render_challenge(challenge: ReadmissionChallenge) -> String {
    uuid::Uuid::from_bytes(challenge.to_bytes()).to_string()
}

/// A challenge is process-local and single-use, so a fresh random value per
/// status read is exactly what binds a later declaration to this read.
fn new_challenge() -> ReadmissionChallenge {
    ReadmissionChallenge::from_bytes(*uuid::Uuid::now_v7().as_bytes())
}

fn positional_text_arguments(
    statement: &PreparedMaintenanceCall,
    procedure: &str,
    arity: usize,
) -> Result<Vec<String>, String> {
    if statement.argument_mode == novarocks_sql::semantic::command::ProcedureArgumentModeSql::Named
    {
        return Err(format!(
            "{procedure} takes positional arguments; named arguments are not accepted"
        ));
    }
    if statement.arguments.len() != arity {
        return Err(format!(
            "{procedure} takes exactly {arity} arguments, but {} were supplied",
            statement.arguments.len()
        ));
    }
    statement
        .arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            if argument.name.is_some() {
                return Err(format!(
                    "{procedure} argument {} must be positional",
                    index + 1
                ));
            }
            let Some(literal) = argument.value.literal() else {
                return Err(format!(
                    "{procedure} argument {} must be a string literal",
                    index + 1
                ));
            };
            let LiteralKind::String(value) = &literal.kind else {
                return Err(format!(
                    "{procedure} argument {} must be a string literal",
                    index + 1
                ));
            };
            if value.is_empty() {
                return Err(format!(
                    "{procedure} argument {} must not be empty",
                    index + 1
                ));
            }
            if value.len() > MAX_ARGUMENT_BYTES {
                return Err(format!(
                    "{procedure} argument {} exceeds the {MAX_ARGUMENT_BYTES}-byte limit",
                    index + 1
                ));
            }
            Ok(value.clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::{ast::TrinoStatement, parser::parse_statement};

    fn call(source: &str) -> PreparedMaintenanceCall {
        let TrinoStatement::Call(statement) = parse_statement(source).expect("CALL should parse")
        else {
            panic!("expected one CALL statement for {source}");
        };
        PreparedMaintenanceCall::from_trino(&statement).unwrap()
    }

    fn decode(source: &str) -> Result<Option<ManagementCall>, String> {
        ManagementCall::try_decode(&call(source))
    }

    #[test]
    fn trino_call_retains_the_management_target_and_original_authority_gate() {
        use novarocks_parser::{ast::TrinoStatement, parser};

        let sql = "CALL novarocks_mv_management_status('ice', 'db.with.dots', 'orders.mv')";
        let TrinoStatement::Call(parsed) = parser::parse_statement(sql).unwrap() else {
            panic!("expected CALL");
        };
        let modern = PreparedMaintenanceCall::from_trino(&parsed).unwrap();
        let expected = ManagementCall::Status {
            target: ManagementCallTarget {
                catalog: "ice".into(),
                database: "db.with.dots".into(),
                name: "orders.mv".into(),
            },
        };
        assert_eq!(
            ManagementCall::try_decode(&modern).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            ManagementCall::try_decode(&call(sql)).unwrap(),
            Some(expected)
        );
        let result =
            try_execute_management_call(None, None, &UnreachableOperations, "root", &modern);
        let Err(error) = result else {
            panic!("expected missing management authority");
        };
        assert_eq!(
            error,
            "MV management procedures require the serving product's management authority"
        );
    }

    #[test]
    fn another_procedure_is_a_route_miss_not_an_error() {
        assert_eq!(
            decode("CALL ice.system.novarocks_imv_stateless_rebuild(\"table\" => 'db.mv')")
                .expect("another procedure decodes as a miss"),
            None
        );
    }

    #[test]
    fn a_target_is_three_arguments_so_a_dotted_name_is_never_reparsed() {
        assert_eq!(
            decode("CALL novarocks_mv_management_status('ice', 'db.with.dots', 'orders.mv')")
                .expect("decodes")
                .expect("is a management call"),
            ManagementCall::Status {
                target: ManagementCallTarget {
                    catalog: "ice".to_string(),
                    database: "db.with.dots".to_string(),
                    name: "orders.mv".to_string(),
                },
            }
        );
    }

    #[test]
    fn resume_carries_the_whole_declaration() {
        assert_eq!(
            decode(
                "CALL novarocks_mv_resume_management('ice', 'db', 'mv', 'chal', 'inc-a', \
                 'operator@example', 'controller confirmed the old FE exited')"
            )
            .expect("decodes")
            .expect("is a management call"),
            ManagementCall::Resume {
                target: ManagementCallTarget {
                    catalog: "ice".to_string(),
                    database: "db".to_string(),
                    name: "mv".to_string(),
                },
                challenge: "chal".to_string(),
                old_incarnation: "inc-a".to_string(),
                operator: "operator@example".to_string(),
                evidence: "controller confirmed the old FE exited".to_string(),
            }
        );
    }

    #[test]
    fn a_wrong_arity_is_refused_rather_than_partially_understood() {
        let error = decode("CALL novarocks_mv_set_owner('ice', 'db', 'mv', 'chal')")
            .expect_err("an incomplete owner handover must not decode");

        assert!(error.contains("exactly 5 arguments"), "{error}");
    }

    #[test]
    fn named_arguments_are_refused_so_positions_stay_reviewable() {
        let error =
            decode("CALL novarocks_mv_management_status(catalog => 'ice', db => 'd', name => 'm')")
                .expect_err("named arguments must not decode");

        assert!(error.contains("positional"), "{error}");
    }

    #[test]
    fn an_empty_argument_is_refused() {
        let error = decode("CALL novarocks_mv_management_status('ice', '', 'mv')")
            .expect_err("an empty target part must not decode");

        assert!(error.contains("must not be empty"), "{error}");
    }

    #[test]
    fn an_oversized_statement_is_refused_before_it_reaches_an_audit_record() {
        let evidence = "e".repeat(MAX_ARGUMENT_BYTES + 1);
        let error = decode(&format!(
            "CALL novarocks_mv_resume_management('ice', 'db', 'mv', 'chal', 'inc-a', 'op', \
             '{evidence}')"
        ))
        .expect_err("an oversized statement must not decode");

        assert!(error.contains("exceeds the"), "{error}");
    }

    struct UnreachableOperations;

    impl MvManagementOperations for UnreachableOperations {
        fn readmit(
            &self,
            _target: &ManagementCallTarget,
            _previous_incarnation: ProcessIncarnation,
            _permits: Vec<ReadmissionPermit>,
        ) -> Result<(), String> {
            unreachable!("no test here reaches a readmission")
        }

        fn hand_over(
            &self,
            _target: &ManagementCallTarget,
            _new_owner: DeploymentOwner,
        ) -> Result<MvOwnerHandoverOutcome, String> {
            unreachable!("no test here reaches a handover")
        }
    }

    #[test]
    fn a_set_owner_decodes_its_target_challenge_and_new_deployment() {
        let decoded =
            decode("CALL novarocks_mv_set_owner('ice', 'db', 'mv', 'ch-1', 'other-deployment')")
                .expect("set_owner should decode")
                .expect("set_owner is an MV management procedure");

        assert_eq!(
            decoded,
            ManagementCall::SetOwner {
                target: ManagementCallTarget {
                    catalog: "ice".to_string(),
                    database: "db".to_string(),
                    name: "mv".to_string(),
                },
                challenge: "ch-1".to_string(),
                new_deployment: "other-deployment".to_string(),
            }
        );
    }

    #[derive(Default)]
    struct RecordingAudit(std::sync::Mutex<Vec<ManagementAuditRecord>>);

    impl novarocks_mv_application::management::ManagementAuditSink for RecordingAudit {
        fn record(&self, record: &ManagementAuditRecord) -> Result<(), String> {
            record.validate()?;
            self.0.lock().expect("audit records").push(record.clone());
            Ok(())
        }
    }

    fn continuation_service() -> ManagementContinuationService {
        ManagementContinuationService::new(
            novarocks_mv_application::management::ManagementEntrance::new(
                DeploymentOwner::parse("deployment-a").expect("owner"),
                ProcessIncarnation::parse("incarnation-1").expect("incarnation"),
            ),
            novarocks_mv_application::management::RemoteEffectPolicy::default(),
        )
    }

    fn set_owner_call(new_deployment: &str, challenge: &str) -> PreparedMaintenanceCall {
        call(&format!(
            "CALL novarocks_mv_set_owner('ice', 'db', 'mv', '{challenge}', '{new_deployment}')"
        ))
    }

    #[test]
    fn same_owner_handover_still_requires_an_issued_challenge() {
        let audit: Arc<dyn ManagementAuditSink> = Arc::new(RecordingAudit::default());
        let error = try_execute_management_call(
            Some(&Arc::new(continuation_service())),
            Some(&audit),
            &UnreachableOperations,
            "root",
            &set_owner_call("deployment-a", "00000000-0000-7000-8000-000000000001"),
        )
        .expect_err("an unissued challenge cannot authorize same-owner handover");

        assert!(!error.contains("names this deployment"), "{error}");
    }

    #[test]
    fn a_challenge_this_process_never_issued_never_reaches_the_target() {
        let sink = Arc::new(RecordingAudit::default());
        let audit: Arc<dyn ManagementAuditSink> = sink.clone();
        let error = try_execute_management_call(
            Some(&Arc::new(continuation_service())),
            Some(&audit),
            // Reaching the adapter would panic, which is the assertion: an
            // unissued challenge must fail before the provider is touched.
            &UnreachableOperations,
            "root",
            &set_owner_call("deployment-b", "00000000-0000-7000-8000-000000000002"),
        )
        .expect_err("an unissued challenge cannot pay for a handover");

        assert!(error.contains("admit the MV owner handover"), "{error}");
        let records = sink.0.lock().expect("audit records");
        assert_eq!(records.len(), 2, "the attempt and its outcome are recorded");
        assert_eq!(records[0].action, ManagementAuditAction::SetOwner);
        assert_eq!(records[0].outcome, ManagementAuditOutcome::Attempted);
        assert!(
            matches!(records[1].outcome, ManagementAuditOutcome::Refused(_)),
            "{:?}",
            records[1].outcome
        );
        assert_eq!(
            records[1]
                .declared_new_owner
                .as_ref()
                .map(DeploymentOwner::as_str),
            Some("deployment-b")
        );
    }

    #[test]
    fn a_handover_without_an_audit_sink_is_refused() {
        let error = try_execute_management_call(
            Some(&Arc::new(continuation_service())),
            None,
            &UnreachableOperations,
            "root",
            &set_owner_call("deployment-b", "00000000-0000-7000-8000-000000000003"),
        )
        .expect_err("a declaration with nowhere to be written does not act");

        assert!(error.contains("audit"), "{error}");
    }

    #[test]
    fn a_management_call_without_the_serving_authority_says_so() {
        let error = try_execute_management_call(
            None,
            None,
            &UnreachableOperations,
            "root",
            &call("CALL novarocks_mv_management_status('ice', 'db', 'mv')"),
        )
        .expect_err("a product with no management authority cannot answer");

        assert!(error.contains("management authority"), "{error}");
    }
    #[test]
    fn same_owner_handover_reaches_the_operation_once_and_spends_its_challenge() {
        struct SameOwner(std::sync::atomic::AtomicUsize);
        impl MvManagementOperations for SameOwner {
            fn readmit(
                &self,
                _: &ManagementCallTarget,
                _: ProcessIncarnation,
                _: Vec<ReadmissionPermit>,
            ) -> Result<(), String> {
                unreachable!("handover only")
            }
            fn hand_over(
                &self,
                _: &ManagementCallTarget,
                owner: DeploymentOwner,
            ) -> Result<MvOwnerHandoverOutcome, String> {
                assert_eq!(owner.as_str(), "deployment-a");
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(MvOwnerHandoverOutcome::HandedOver {
                    previous_owner: owner.as_str().into(),
                })
            }
        }
        let continuation = Arc::new(continuation_service());
        let table = ManagementCallTarget {
            catalog: "ice".into(),
            database: "db".into(),
            name: "mv".into(),
        }
        .table()
        .unwrap();
        let challenge = new_challenge();
        let status = continuation.status(&table, None, challenge).unwrap();
        assert_eq!(status.challenge, Some(challenge));
        let rows = status_rows(&status);
        assert!(
            rows.iter()
                .any(|r| r == &row("PendingSlotChanges", Some("0".into())))
        );
        assert!(
            rows.iter()
                .any(|r| r == &row("LatestSlotChangeConclusion", None))
        );
        let statement = set_owner_call(
            "deployment-a",
            &uuid::Uuid::from_bytes(challenge.to_bytes()).to_string(),
        );
        let operations = SameOwner(std::sync::atomic::AtomicUsize::new(0));
        let audit: Arc<dyn ManagementAuditSink> = Arc::new(RecordingAudit::default());
        try_execute_management_call(
            Some(&continuation),
            Some(&audit),
            &operations,
            "root",
            &statement,
        )
        .unwrap();
        assert_eq!(operations.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            try_execute_management_call(
                Some(&continuation),
                Some(&audit),
                &operations,
                "root",
                &statement
            )
            .is_err()
        );
        assert_eq!(operations.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
