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

//! Closed, runner-owned assertions over one completed EXPLAIN COSTS response.
//! Inputs are fixture policy and immutable display facts, never catalog reads.

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryStatsContract {
    tables: Vec<TableContract>,
    broadcast: BroadcastContract,
    payload: PayloadContract,
    hash_table: HashTableContract,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableContract {
    table_suffix: String,
    rows: u64,
    confidence: Confidence,
    source: Source,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
enum Confidence {
    Measured,
    Exact,
    Estimated,
    Fallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
enum Source {
    IcebergManifest,
    IcebergPuffin,
    ManagedLakeMetadata,
    StarRocksTableMetadata,
    ConnectorEstimate,
    Derived,
    Fallback,
    TestFixture,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BroadcastContract {
    distribution: BroadcastDistribution,
    join_kind: InnerJoin,
    verdict: FeasibleVerdict,
    forced: bool,
    backends: u32,
    risk_multiplier: f64,
    per_node_budget_bytes: u64,
    cluster_network_budget_bytes: u64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum BroadcastDistribution {
    #[serde(rename = "BROADCAST")]
    Broadcast,
}
#[derive(Clone, Copy, Debug, Deserialize)]
enum InnerJoin {
    #[serde(rename = "INNER")]
    Inner,
}
#[derive(Clone, Copy, Debug, Deserialize)]
enum FeasibleVerdict {
    #[serde(rename = "feasible")]
    Feasible,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PayloadContract {
    PositiveRange,
    Exact { bytes: f64 },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum HashTableContract {
    Exact {
        build_rows: u64,
        load_factor: f64,
        per_row_overhead_bytes: f64,
    },
    Bounded {
        max_build_rows: u64,
        load_factor: f64,
        per_row_overhead_bytes: f64,
    },
}

const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;

impl QueryStatsContract {
    pub fn parse(raw: &str) -> Result<Self> {
        let parsed: Self =
            serde_json::from_str(raw).context("invalid @query_stats_contract JSON")?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> Result<()> {
        let mut labels = BTreeSet::new();
        for table in &self.tables {
            ensure!(
                !table.table_suffix.is_empty()
                    && !table.table_suffix.chars().any(char::is_whitespace),
                "table_suffix must be a nonempty unambiguous label suffix"
            );
            ensure!(labels.insert(&table.table_suffix), "duplicate table_suffix");
        }
        let policy = &self.broadcast;
        // V1 describes an ordinary feasible INNER broadcast. Forced plans and
        // other join kinds require their own explicit contract, not defaults.
        let _ = (policy.distribution, policy.join_kind, policy.verdict);
        ensure!(
            !policy.forced,
            "query stats contract requires an unforced broadcast"
        );
        ensure!(policy.backends > 0, "backends must be positive");
        finite_positive(policy.risk_multiplier, "risk_multiplier")?;
        for (name, bytes) in [
            ("per_node_budget_bytes", policy.per_node_budget_bytes),
            (
                "cluster_network_budget_bytes",
                policy.cluster_network_budget_bytes,
            ),
        ] {
            ensure!(
                bytes > 0 && bytes <= MAX_EXACT_INTEGER,
                "{name} must be a positive exact f64 integer"
            );
        }
        if let PayloadContract::Exact { bytes } = self.payload {
            finite_positive(bytes, "exact payload")?;
        }
        let (rows, load_factor, overhead) = self.hash_parameters();
        ensure!(
            rows > 0 && rows <= MAX_EXACT_INTEGER,
            "hash row bound must be a positive exact f64 integer"
        );
        ensure!(
            load_factor.is_finite() && (0.5..=1.0).contains(&load_factor),
            "load_factor must be finite in the production [0.5,1.0] domain"
        );
        ensure!(
            overhead.is_finite() && overhead >= 0.0,
            "hash overhead must be finite and nonnegative"
        );
        Ok(())
    }

    fn hash_parameters(&self) -> (u64, f64, f64) {
        match self.hash_table {
            HashTableContract::Exact {
                build_rows,
                load_factor,
                per_row_overhead_bytes,
            } => (build_rows, load_factor, per_row_overhead_bytes),
            HashTableContract::Bounded {
                max_build_rows,
                load_factor,
                per_row_overhead_bytes,
            } => (max_build_rows, load_factor, per_row_overhead_bytes),
        }
    }

    /// Reject unsupported statements before the runner executes a fixture step.
    pub fn validate_sql(&self, sql: &str) -> Result<()> {
        use novarocks_parser::ast::{ExplainFormat, Statement};
        let statements =
            novarocks_parser::parse(sql).map_err(|error| anyhow::anyhow!("{error}"))?;
        ensure!(
            matches!(statements.as_slice(), [Statement::ExplainQuery(query)]
            if query.format == ExplainFormat::Costs && !query.logical),
            "@query_stats_contract requires exactly one physical EXPLAIN COSTS query"
        );
        Ok(())
    }

    pub fn verify(&self, sql: &str, response: &str) -> Result<()> {
        self.validate_sql(sql)?;
        let mut tables = Vec::new();
        let mut refs = BTreeSet::new();
        let mut decisions = Vec::new();
        for line in response.lines() {
            if let Some(body) = line.trim().strip_prefix("TABLE STATS ") {
                let fields = parse_fields(body, None)?;
                require_keys(&fields, &["ref", "table", "rows", "confidence", "source"])?;
                let stats_ref: u32 = fields["ref"].parse().context("invalid StatsRef")?;
                ensure!(refs.insert(stats_ref), "duplicate TABLE STATS ref");
                let rows: u64 = fields["rows"]
                    .parse()
                    .context("missing or invalid TABLE STATS row count")?;
                let confidence: Confidence = parse_enum(fields["confidence"])?;
                let source: Source = parse_enum(fields["source"])?;
                tables.push((
                    stats_ref,
                    fields["table"].to_string(),
                    rows,
                    confidence,
                    source,
                ));
            }
            if let Some((prefix, after)) = line.split_once("bcast[") {
                let (node_id, operator) = prefix
                    .trim_start()
                    .split_once(':')
                    .context("bcast decision has no physical node label")?;
                ensure!(
                    !node_id.is_empty()
                        && node_id.bytes().all(|byte| byte.is_ascii_digit())
                        && operator.starts_with("HASH JOIN (BROADCAST, INNER,"),
                    "bcast decision must belong to the same BROADCAST INNER join line"
                );
                let (body, suffix) = after
                    .split_once(']')
                    .context("unterminated bcast decision")?;
                ensure!(
                    !suffix.contains("bcast["),
                    "duplicate bcast decision on one join line"
                );
                let verdicts: Vec<_> = prefix
                    .split_whitespace()
                    .filter(|token| token.starts_with("bcast_verdict="))
                    .collect();
                ensure!(
                    verdicts.as_slice() == ["bcast_verdict=feasible"],
                    "missing, duplicate or mismatched broadcast verdict"
                );
                decisions.push(parse_fields(body, Some(','))?);
            }
        }
        ensure!(
            tables.len() == self.tables.len(),
            "TABLE STATS count differs from contract"
        );
        let mut matched = BTreeSet::new();
        for expected in &self.tables {
            let candidates: Vec<_> = tables
                .iter()
                .filter(|(_, label, ..)| label.ends_with(&expected.table_suffix))
                .collect();
            ensure!(
                candidates.len() == 1,
                "missing or ambiguous table suffix {}",
                expected.table_suffix
            );
            let (reference, _, rows, confidence, source) = candidates[0];
            ensure!(
                matched.insert(*reference),
                "one TABLE STATS row matched multiple expectations"
            );
            ensure!(
                *rows == expected.rows
                    && *confidence == expected.confidence
                    && *source == expected.source,
                "table {} row count/confidence/source do not match the same-line facts",
                expected.table_suffix
            );
        }
        ensure!(decisions.len() == 1, "expected exactly one bcast decision");
        let fields = &decisions[0];
        require_keys(
            fields,
            &[
                "verdict",
                "forced",
                "build_bytes",
                "hash_table_bytes",
                "backends",
                "fanout_bytes",
                "per_node_budget_bytes",
                "risk_multiplier",
            ],
        )?;
        ensure!(
            fields["verdict"] == "feasible" && fields["forced"] == "false",
            "broadcast is not unforced and feasible"
        );
        let backends: u32 = fields["backends"]
            .parse()
            .context("invalid backend count")?;
        let budget = number(fields, "per_node_budget_bytes")?;
        let risk = number(fields, "risk_multiplier")?;
        let payload = number(fields, "build_bytes")?;
        let hash = number(fields, "hash_table_bytes")?;
        let fanout = number(fields, "fanout_bytes")?;
        let policy = &self.broadcast;
        ensure!(
            backends == policy.backends
                && risk == policy.risk_multiplier
                && budget == policy.per_node_budget_bytes as f64,
            "broadcast topology/policy differs from fixture"
        );
        ensure!(
            fanout == (payload * f64::from(backends)) * risk,
            "broadcast fanout formula mismatch"
        );
        ensure!(
            hash * risk <= budget,
            "broadcast hash memory floor exceeds per-node budget"
        );
        ensure!(
            fanout <= policy.cluster_network_budget_bytes as f64,
            "broadcast network floor exceeds cluster budget"
        );
        if let PayloadContract::Exact { bytes } = self.payload {
            ensure!(
                payload == bytes,
                "payload differs from independently derived exact bytes"
            );
        }
        let (rows, load_factor, overhead) = self.hash_parameters();
        let lower = payload / load_factor;
        let upper = lower + rows as f64 * overhead;
        ensure!(
            lower.is_finite() && upper.is_finite(),
            "hash formula overflow"
        );
        match self.hash_table {
            HashTableContract::Exact { .. } => {
                ensure!(hash == upper, "exact hash table formula mismatch")
            }
            HashTableContract::Bounded { .. } => ensure!(
                hash >= lower && hash <= upper,
                "hash table outside source-derived row bounds"
            ),
        }
        Ok(())
    }
}

fn finite_positive(value: f64, name: &str) -> Result<()> {
    ensure!(
        value.is_finite() && value > 0.0,
        "{name} must be finite and positive"
    );
    Ok(())
}

fn number(fields: &BTreeMap<&str, &str>, name: &str) -> Result<f64> {
    let value: f64 = fields
        .get(name)
        .context("missing cost field")?
        .parse()
        .with_context(|| format!("invalid complete numeric field {name}"))?;
    finite_positive(value, name)?;
    Ok(value)
}

fn parse_enum<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value.to_string()))
        .context("unknown TABLE STATS enum")
}

fn parse_fields(body: &str, separator: Option<char>) -> Result<BTreeMap<&str, &str>> {
    let tokens: Vec<_> = match separator {
        Some(separator) => body.split(separator).map(str::trim).collect(),
        None => body.split_whitespace().collect(),
    };
    let mut fields = BTreeMap::new();
    for token in tokens {
        let (key, value) = token
            .split_once('=')
            .context("malformed statistics key/value")?;
        ensure!(
            !key.is_empty() && !value.is_empty() && !value.contains('='),
            "malformed statistics field"
        );
        ensure!(
            fields.insert(key, value).is_none(),
            "duplicate statistics field {key}"
        );
    }
    Ok(fields)
}

fn require_keys(fields: &BTreeMap<&str, &str>, required: &[&str]) -> Result<()> {
    ensure!(
        fields.len() == required.len() && required.iter().all(|key| fields.contains_key(key)),
        "statistics record has missing or unknown fields"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const SQL: &str = "EXPLAIN COSTS SELECT 1";

    fn exact_contract() -> String {
        r#"{"tables":[],"broadcast":{"distribution":"BROADCAST","join_kind":"INNER","verdict":"feasible","forced":false,"backends":3,"risk_multiplier":2,"per_node_budget_bytes":268435456,"cluster_network_budget_bytes":268435456},"payload":{"kind":"exact","bytes":80},"hash_table":{"kind":"exact","build_rows":10,"load_factor":0.75,"per_row_overhead_bytes":16}}"#.to_string()
    }

    fn line(payload: f64, hash: f64, fanout: f64) -> String {
        format!(
            "19:HASH JOIN (BROADCAST, INNER, eq: [p.k = b.k]) bcast_verdict=feasible bcast[verdict=feasible, forced=false, build_bytes={payload}, hash_table_bytes={hash}, backends=3, fanout_bytes={fanout}, per_node_budget_bytes=268435456, risk_multiplier=2]"
        )
    }

    fn positive_line() -> String {
        line(80.0, 80.0 / 0.75 + 10.0 * 16.0, (80.0 * 3.0) * 2.0)
    }

    #[test]
    fn exact_derived_formula_accepts_complete_roundtrip_fields() {
        QueryStatsContract::parse(&exact_contract())
            .unwrap()
            .verify(SQL, &positive_line())
            .unwrap();
    }

    #[test]
    fn complete_policy_and_numeric_corruption_is_rejected() {
        let contract = QueryStatsContract::parse(&exact_contract()).unwrap();
        let good = positive_line();
        for bad in [
            good.replace("fanout_bytes=480", "fanout_bytes=240"),
            good.replace("backends=3", "backends=1"),
            good.replace("risk_multiplier=2", "risk_multiplier=1"),
            good.replace("build_bytes=80", "build_bytes=0"),
            good.replace("build_bytes=80", "build_bytes=-1"),
            good.replace("build_bytes=80", "build_bytes=NaN"),
            good.replace("build_bytes=80", "build_bytes=inf"),
            good.replace("build_bytes=80", "build_bytes=80junk"),
            good.replace("forced=false", "forced=true"),
            good.replace("BROADCAST, INNER", "PARTITIONED, INNER"),
            good.replace("19:HASH JOIN", "19:PROJECT fake HASH JOIN"),
            good.replace("19:HASH JOIN", "not_a_node:HASH JOIN"),
            good.replace("hash_table_bytes=", "wrong_field="),
            good.replace("risk_multiplier=2", "risk_multiplier=2, risk_multiplier=2"),
            format!("{good}\n{good}"),
        ] {
            assert!(contract.verify(SQL, &bad).is_err(), "{bad}");
        }
        assert!(contract.verify("SELECT 1", &good).is_err());
        assert!(
            contract
                .verify("EXPLAIN COSTS SELECT 1; EXPLAIN COSTS SELECT 2", &good)
                .is_err()
        );
    }

    #[test]
    fn strict_json_schema_rejects_missing_unknown_duplicate_and_invalid_policy() {
        let good = exact_contract();
        for bad in [
            good.replace("\"tables\":[],", ""),
            good.replace("\"tables\":[]", "\"tables\":[],\"unknown\":0"),
            good.replace("\"tables\":[]", "\"tables\":[],\"tables\":[]"),
            good.replace("\"backends\":3", "\"backends\":3,\"backends\":3"),
            good.replace("\"backends\":3", "\"backends\":0"),
            good.replace("\"load_factor\":0.75", "\"load_factor\":0"),
            good.replace("\"verdict\":\"feasible\"", "\"verdict\":\"unknown\""),
        ] {
            assert!(QueryStatsContract::parse(&bad).is_err(), "{bad}");
        }
    }

    fn bounded_contract() -> String {
        exact_contract().replace("\"tables\":[]", r#""tables":[{"table_suffix":".db.build","rows":500000,"confidence":"Exact","source":"IcebergManifest"}]"#)
            .replace(r#""payload":{"kind":"exact","bytes":80}"#, r#""payload":{"kind":"positive_range"}"#)
            .replace(r#""kind":"exact","build_rows":10"#, r#""kind":"bounded","max_build_rows":500000"#)
    }

    #[test]
    fn supplied_non_eight_width_and_bounded_rows_use_independent_formula() {
        let contract = QueryStatsContract::parse(&bounded_contract()).unwrap();
        let table = "TABLE STATS ref=17 table=ice.db.build rows=500000 confidence=Exact source=IcebergManifest";
        for width in [20.25, 42.5] {
            let payload = 10.0 * width;
            let hash = payload / 0.75 + 10.0 * 16.0;
            let response = format!("{table}\n{}", line(payload, hash, (payload * 3.0) * 2.0));
            contract.verify(SQL, &response).unwrap();
            assert!(
                contract
                    .verify(
                        SQL,
                        &format!(
                            "{table}\n{}",
                            line(payload, hash / 3.0, payload * 3.0 * 2.0)
                        )
                    )
                    .is_err()
            );
        }
        assert!(
            contract
                .verify(
                    SQL,
                    &format!(
                        "{table}\n{}",
                        line(100_000_000.0, 140_000_000.0, 600_000_000.0)
                    )
                )
                .is_err()
        );
    }

    #[test]
    fn same_line_provenance_is_required_and_missing_duplicate_ambiguous_facts_fail() {
        let contract = QueryStatsContract::parse(&bounded_contract()).unwrap();
        let good = format!(
            "TABLE STATS ref=17 table=ice.db.build rows=500000 confidence=Exact source=IcebergManifest\n{}",
            positive_line()
        );
        for bad in [
            good.replace(" rows=500000", " rows=missing"),
            good.replace("confidence=Exact source=IcebergManifest", "confidence=Fallback source=Derived"),
            good.replace("confidence=Exact", "confidence=Unknown"),
            good.replace(" source=IcebergManifest", ""),
            good.replace("rows=500000", "rows=500000 rows=500000"),
            good.replace("\n", "\nTABLE STATS ref=18 table=other.db.build rows=500000 confidence=Exact source=IcebergManifest\n"),
            good.replace("source=IcebergManifest", "source=Derived\nsource=IcebergManifest"),
        ] { assert!(contract.verify(SQL, &bad).is_err(), "{bad}"); }
        let absent = QueryStatsContract::parse(&exact_contract()).unwrap();
        assert!(
            absent.verify(SQL, &good).is_err(),
            "source-free control must not acquire base TABLE STATS"
        );
    }
}
