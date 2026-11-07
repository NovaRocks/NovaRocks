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

//! Exact submission data derived from already selected, resolved SQL inputs.
use crate::parser::{MV_DOCUMENT_OBSERVATION_TAG, validate_test_capability_tags};
use crate::session::split_sql_statements;
use anyhow::{Context, Result, bail, ensure};
use novarocks_parser::ast::{LiteralKind, MaintenanceStatement, MaintenanceValue, Statement};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub(crate) struct PlannedSubmission {
    pub run_token: String,
    pub sql_sha256: String,
    pub outer_attempt: usize,
    pub transient_attempt: usize,
    pub statement_index: usize,
}
impl PlannedSubmission {
    pub fn validate_sql(&self, sql: &str) -> Result<()> {
        ensure!(
            self.sql_sha256 == format!("{:x}", Sha256::digest(sql.as_bytes())),
            "submitted SQL differs from the frozen control target"
        );
        Ok(())
    }
}

pub(crate) fn plan_submissions(
    prefix: &str,
    sql: &str,
    outer_attempts: usize,
) -> Result<Vec<PlannedSubmission>> {
    ensure!(
        outer_attempts > 0,
        "controlled SQL requires a finite nonzero attempt count"
    );
    ensure!(
        !prefix.is_empty()
            && prefix.len() <= 64
            && prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid control target prefix"
    );
    let statements = split_sql_statements(sql)?;
    ensure!(
        !statements.is_empty(),
        "controlled SQL has no submitted statement"
    );
    let count = outer_attempts
        .checked_mul(2)
        .and_then(|attempts| attempts.checked_mul(statements.len()))
        .context("controlled submission count overflow")?;
    // This is a consumer allocation bound, not a larger product grant budget.
    ensure!(
        count <= 16384,
        "controlled SQL submission plan exceeds its finite allocation bound"
    );
    let mut targets = Vec::with_capacity(count);
    for outer_attempt in 0..outer_attempts {
        for transient_attempt in 0..2 {
            for (statement_index, statement) in statements.iter().enumerate() {
                let run_token =
                    format!("{prefix}-r{outer_attempt}-t{transient_attempt}-p{statement_index}");
                ensure!(
                    run_token.len() <= 128,
                    "controlled submission token exceeds its bound"
                );
                targets.push(PlannedSubmission {
                    run_token,
                    sql_sha256: format!("{:x}", Sha256::digest(statement.as_bytes())),
                    outer_attempt,
                    transient_attempt,
                    statement_index,
                });
            }
        }
    }
    Ok(targets)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvDocumentDemand {
    pub catalog: String,
    pub namespace: String,
    pub mv: String,
    pub level: String,
}

/// A nearby explicit tag grants observation intent; parsing only resolves the
/// selected CALL's literal target. Bare SQL or ordinary tags grant nothing.
pub(crate) fn tagged_mv_document_demands(
    tags: &[String],
    sql: &str,
) -> Result<Vec<MvDocumentDemand>> {
    validate_test_capability_tags(tags)?;
    if !tags.iter().any(|tag| tag == MV_DOCUMENT_OBSERVATION_TAG) {
        return Ok(vec![]);
    }
    let mut demands = Vec::new();
    for statement in novarocks_parser::parse(sql)
        .map_err(|error| anyhow::anyhow!("decode explicitly tagged MV document CALL: {error}"))?
    {
        let Statement::Maintenance(MaintenanceStatement::Call(call)) = statement else {
            continue;
        };
        let names = call
            .procedure
            .parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>();
        ensure!(
            names.len() == 3
                && names[1].eq_ignore_ascii_case("system")
                && names[2].eq_ignore_ascii_case("novarocks_imv_stateless_rebuild"),
            "MV document observation tag names an unsupported or unqualified procedure"
        );
        let mut arguments = std::collections::BTreeMap::new();
        for argument in call.arguments {
            let name = argument
                .name
                .context("MV document observation requires named literal arguments")?
                .value
                .to_ascii_lowercase();
            let MaintenanceValue::Literal(value) = argument.value else {
                bail!("MV document target argument is not a literal")
            };
            let LiteralKind::String(value) = value.kind else {
                bail!("MV document target argument is not a string")
            };
            ensure!(
                arguments.insert(name, value).is_none(),
                "MV document CALL repeats an argument"
            );
        }
        ensure!(
            arguments.len() == 2,
            "MV document CALL must declare its exact table and observation level"
        );
        let table = arguments
            .get("table")
            .context("MV document CALL omitted its table")?;
        let level = arguments
            .get("level")
            .context("MV document CALL omitted its level")?;
        ensure!(
            matches!(level.as_str(), "baseline" | "package" | "provenance"),
            "MV document tag cannot authorize destructive or unknown rebuild levels"
        );
        let parts = table.split('.').collect::<Vec<_>>();
        ensure!(
            parts.len() == 2 && parts.iter().all(|part| !part.is_empty()),
            "MV document table must explicitly name its namespace and MV"
        );
        demands.push(MvDocumentDemand {
            catalog: names[0].to_owned(),
            namespace: parts[0].to_owned(),
            mv: parts[1].to_owned(),
            level: level.clone(),
        });
    }
    ensure!(
        !demands.is_empty(),
        "MV document observation tag has no exact document CALL"
    );
    Ok(demands)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_submission_slots_distinguish_statement_and_retry_and_reject_changed_sql() {
        let sql = "SET @x=1; SELECT /*+ SET_VAR(query_timeout=9) */ ';' AS x";
        let submitted = split_sql_statements(sql).unwrap();
        let targets = plan_submissions("selected", sql, 2).unwrap();
        assert_eq!(targets.len(), 8);
        assert_eq!(
            targets
                .iter()
                .map(|target| &target.run_token)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            8
        );
        for target in targets {
            target
                .validate_sql(&submitted[target.statement_index])
                .unwrap();
            assert!(target.validate_sql("SELECT unrelated").is_err());
        }
        assert!(plan_submissions("selected", sql, 0).is_err());
        assert!(plan_submissions("selected", sql, usize::MAX).is_err());
    }
    #[test]
    fn explicit_mv_tag_resolves_only_literal_observation_targets_and_never_wipe() {
        let sql = "USE c.n; CALL c.system.novarocks_imv_stateless_rebuild(table => 'n.m', level => 'package'); CALL c.system.novarocks_imv_stateless_rebuild(table => 'n.m', level => 'provenance')";
        assert!(
            tagged_mv_document_demands(&["regression".into()], sql)
                .unwrap()
                .is_empty()
        );
        let tags = vec![MV_DOCUMENT_OBSERVATION_TAG.into()];
        let demands = tagged_mv_document_demands(&tags, sql).unwrap();
        assert_eq!(demands.len(), 2);
        assert_eq!(
            demands[0],
            MvDocumentDemand {
                catalog: "c".into(),
                namespace: "n".into(),
                mv: "m".into(),
                level: "package".into()
            }
        );
        assert_eq!(demands[1].level, "provenance");
        assert!(
            tagged_mv_document_demands(
                &tags,
                "CALL c.system.novarocks_imv_stateless_rebuild(table => 'n.m', level => 'full')"
            )
            .is_err()
        );
        assert!(tagged_mv_document_demands(&tags, "SELECT 1").is_err());
        assert!(
            tagged_mv_document_demands(
                &tags,
                "CALL system.novarocks_imv_stateless_rebuild(table => 'm', level => 'package')"
            )
            .is_err()
        );
    }
}
