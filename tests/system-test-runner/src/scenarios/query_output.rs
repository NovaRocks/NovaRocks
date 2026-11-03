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

use crate::actors::mysql as mysql_actor;
use crate::actors::mysql_stream::{MysqlPacket, MysqlStream};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::TestClusterOwner;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const IO_TIMEOUT_CAP: Duration = Duration::from_secs(10);
const TWO_ROW_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(SchemaOnce),
        Box::new(NonNegotiatedMultiStatement),
        Box::new(NegotiatedMultiResult),
        Box::new(ScalarSession),
    ]
}

/// A public MySQL result must have exactly one schema prefix, then its rows,
/// then one terminal success packet. The raw actor is intentional: a normal
/// client library drains these packets before a scenario can prove their wire
/// order or detect an accidental second schema prefix.
struct SchemaOnce;

/// A negotiated COM_QUERY batch emits one result per executable statement.
/// The first result must carry SERVER_MORE_RESULTS_EXISTS, while the final
/// result owns the single terminal success packet without that status bit.
struct NegotiatedMultiResult;

/// A client that did not negotiate both multi-statement and multi-result
/// capabilities remains on the strict single-statement protocol path.
struct NonNegotiatedMultiStatement;

/// Query-valued SET must publish its session value only after the native
/// Scalar record and terminal success, including empty and rejected input.
struct ScalarSession;

impl Scenario for ScalarSession {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_output",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-output/scalar-session"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = context
            .handle()
            .query_execution_resource_snapshot()?
            .context("cross-process harness did not expose the query-resource oracle")?;
        let timeout = context
            .remaining("open Scalar session client")?
            .min(IO_TIMEOUT_CAP);
        let mut client = mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
        client.query_drop("SET @m07_scalar = (SELECT 4)")?;
        ensure!(
            client.query_first::<i64, _>("SELECT @m07_scalar")? == Some(4),
            "Scalar session lost its numeric value"
        );
        client.query_drop(
            "SET @m07_scalar = (SELECT SUM(generate_series) FROM generate_series(1, 100))",
        )?;
        ensure!(
            client.query_first::<i64, _>("SELECT @m07_scalar")? == Some(5050),
            "Scalar session lost its aggregate value"
        );
        client.query_drop("SET @m07_scalar = (SELECT 4 WHERE FALSE)")?;
        ensure!(
            client.query_first::<Option<i64>, _>("SELECT @m07_scalar")? == Some(None),
            "empty Scalar input must publish NULL"
        );
        client.query_drop("SET @m07_scalar = (SELECT 'retained')")?;
        ensure!(
            client
                .query_drop("SET @m07_scalar = (SELECT generate_series FROM generate_series(1, 2))")
                .is_err(),
            "multirow Scalar input must fail"
        );
        ensure!(
            client
                .query_first::<String, _>("SELECT @m07_scalar")?
                .as_deref()
                == Some("retained"),
            "failed Scalar input must preserve the previously committed session value"
        );
        context.action(
            "verified native Scalar numeric, aggregate, empty, and failed session publication",
        );
        drop(client);
        let deadline = context.deadline();
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)
            .context("await Scalar session resource convergence")?;
        context.action("verified Scalar session resources converged after success and failure");
        Ok(())
    }
}

impl Scenario for NonNegotiatedMultiStatement {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_output",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-output/non-negotiated-multi-statement"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = context
            .handle()
            .query_execution_resource_snapshot()?
            .context("cross-process harness did not expose the query-resource oracle")?;
        let timeout = context
            .remaining("open non-negotiated raw MySQL client")?
            .min(IO_TIMEOUT_CAP);
        let mut stream = MysqlStream::connect(context.mysql_user(), context.mysql_port(), timeout)?;
        stream.send_query("SET query_timeout = 60; SELECT 1")?;
        let response = stream.read_packet("non-negotiated multi-statement rejection")?;
        ensure!(
            response.is_error(),
            "unnegotiated multi-statement query must be rejected before any result, got payload={:?}",
            response.payload()
        );
        context.action("verified unnegotiated multi-statement COM_QUERY is rejected");
        let deadline = context.deadline();
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)
            .context("await non-negotiated multi-statement resource convergence")?;
        context.action("verified rejected multi-statement resources converged");
        Ok(())
    }
}

impl Scenario for NegotiatedMultiResult {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_output",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-output/negotiated-multi-result"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = context
            .handle()
            .query_execution_resource_snapshot()?
            .context("cross-process harness did not expose the query-resource oracle")?;
        let timeout = context
            .remaining("open negotiated raw MySQL client")?
            .min(IO_TIMEOUT_CAP);
        let mut stream = MysqlStream::connect_with_multi_results(
            context.mysql_user(),
            context.mysql_port(),
            timeout,
        )?;
        stream.send_query("SET query_timeout = 60; SELECT 1")?;
        let first = stream.read_packet("first multi-result terminal")?;
        ensure!(
            first.is_result_terminator() && first.has_more_results(),
            "first statement must end with SERVER_MORE_RESULTS_EXISTS, got payload={:?}",
            first.payload()
        );

        let packets = [
            stream.read_packet("second result column count")?,
            stream.read_packet("second result column definition")?,
            stream.read_packet("second result metadata terminator")?,
            stream.read_packet("second result row")?,
            stream.read_packet("second result terminal")?,
        ];
        assert_packet_sequence(&packets)?;
        ensure!(packets[0].payload() == [1], "expected one result column");
        ensure_normal_packet(&packets[1], "second result column definition")?;
        ensure!(
            packets[2].is_result_terminator(),
            "expected metadata terminator"
        );
        ensure_normal_packet(&packets[3], "second result row")?;
        ensure!(
            packets[4].is_result_terminator() && !packets[4].has_more_results(),
            "final result must terminate without more-results, got payload={:?}",
            packets[4].payload()
        );
        context.action("verified negotiated multi-result wire order and terminal status flags");
        let deadline = context.deadline();
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)
            .context("await negotiated multi-result resource convergence")?;
        context.action("verified negotiated multi-result resources converged");
        Ok(())
    }
}

impl Scenario for SchemaOnce {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_output",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-output/schema-once"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = context
            .handle()
            .query_execution_resource_snapshot()?
            .context("cross-process harness did not expose the query-resource oracle")?;
        context.action("captured query-output resource baseline");

        let timeout = context
            .remaining("open raw MySQL query-output client")?
            .min(IO_TIMEOUT_CAP);
        let mut stream = MysqlStream::query(
            context.mysql_user(),
            context.mysql_port(),
            TWO_ROW_QUERY,
            timeout,
        )?;
        context.action("sent one-column two-row query through raw public MySQL stream");

        let packets = [
            stream.read_packet("result column count")?,
            stream.read_packet("result column definition")?,
            stream.read_packet("result metadata terminator")?,
            stream.read_packet("first result row")?,
            stream.read_packet("second result row")?,
            stream.read_packet("result terminal")?,
        ];
        assert_packet_sequence(&packets)?;
        ensure!(
            packets[0].sequence() == 1,
            "MySQL COM_QUERY response must begin at packet sequence 1, got {}",
            packets[0].sequence()
        );
        ensure!(
            packets[0].payload() == [1],
            "expected exactly one result column, got payload={:?}",
            packets[0].payload()
        );
        ensure_normal_packet(&packets[1], "single column definition")?;
        ensure!(
            packets[2].is_result_terminator(),
            "expected one metadata terminator, got payload={:?}",
            packets[2].payload()
        );
        ensure_normal_packet(&packets[3], "first result row")?;
        ensure_normal_packet(&packets[4], "second result row")?;
        ensure!(
            packets[5].is_result_terminator(),
            "expected one success terminal packet, got payload={:?}",
            packets[5].payload()
        );
        context.action("verified one schema prefix, two rows, and one terminal success packet");

        let deadline = context.deadline();
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)
            .context("await query-output resource convergence")?;
        context.action("verified query-output resources converged after terminal packet");
        Ok(())
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let actual = context.handle().be_count();
    ensure!(
        actual == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, but the runner launched 1FE+{actual}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

fn ensure_normal_packet(packet: &MysqlPacket, label: &str) -> Result<()> {
    ensure!(
        !packet.is_error() && !packet.is_result_terminator(),
        "expected {label}, got payload={:?}",
        packet.payload()
    );
    Ok(())
}

fn assert_packet_sequence(packets: &[MysqlPacket]) -> Result<()> {
    for pair in packets.windows(2) {
        ensure!(
            pair[1].sequence() == pair[0].sequence().wrapping_add(1),
            "MySQL result packet sequence is not contiguous: {} then {}",
            pair[0].sequence(),
            pair[1].sequence()
        );
    }
    Ok(())
}
