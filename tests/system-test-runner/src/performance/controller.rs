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

use super::manifest::{MixedWorkload, SlowOutputWorkload, Uea1WorkloadManifest, Window};
use super::metrics::{QuerySample, write_report};
use crate::actors::mysql as mysql_actor;
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::LaunchProfile;
use novarocks_cluster_harness::process_resources::ProcessResourceMonitor;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub enum PerformanceScenario {
    ShortConcurrent,
    Mixed,
    SlowOutput,
}

impl PerformanceScenario {
    pub fn name(self) -> &'static str {
        match self {
            Self::ShortConcurrent => "performance/uea1-short-concurrent",
            Self::Mixed => "performance/uea1-mixed",
            Self::SlowOutput => "performance/uea1-slow-output",
        }
    }
}

pub fn run(
    scenario: PerformanceScenario,
    context: &mut ScenarioContext,
    manifest: &Uea1WorkloadManifest,
) -> Result<()> {
    ensure!(
        context.launch_profile() == LaunchProfile::Performance,
        "{} requires --launch-profile performance",
        scenario.name()
    );
    let monitor = ProcessResourceMonitor::start(context.process_ids(), Duration::from_millis(100))?;
    let samples = match scenario {
        PerformanceScenario::ShortConcurrent => run_short(context, manifest)?,
        PerformanceScenario::Mixed => run_mixed(context, &manifest.mixed)?,
        PerformanceScenario::SlowOutput => run_slow_output(context, &manifest.slow_output)?,
    };
    let resource_path = context.scenario_root().join("process-resources.json");
    let resources = monitor.finish(&resource_path)?;
    ensure!(
        !resources.samples().is_empty(),
        "{} collected no process resource samples",
        scenario.name()
    );
    ensure!(
        !samples.is_empty() && samples.iter().any(|sample| sample.rows > 0),
        "{} produced no valid work",
        scenario.name()
    );
    write_report(
        context.scenario_root(),
        &manifest.sha256,
        scenario.name(),
        &samples,
        &[],
    )?;
    context.action(format!(
        "{} completed {} valid samples using manifest {}",
        scenario.name(),
        samples.len(),
        manifest.sha256
    ));
    Ok(())
}

fn run_short(
    context: &ScenarioContext,
    manifest: &Uea1WorkloadManifest,
) -> Result<Vec<QuerySample>> {
    let timeout = context.remaining("run short-query performance windows")?;
    let mut connection = mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
    for query in &manifest.short.queries {
        let explain = format!("EXPLAIN {query}");
        let text = connection
            .query_iter(explain)
            .context("explain fixed short performance query")?
            .map(|row| row.map(|row| format!("{row:?}")))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .join("\n");
        for expected in &manifest.short.plan_contains {
            ensure!(
                text.contains(expected),
                "fixed short query plan omitted required fragment {expected:?}: {text}"
            );
        }
    }
    drop(connection);

    let mut all = Vec::new();
    for window in &manifest.short.windows {
        all.extend(run_query_window(
            context.mysql_user(),
            context.mysql_port(),
            &manifest.short.queries,
            window,
            timeout,
            "short",
        )?);
    }
    Ok(all)
}

fn run_mixed(context: &ScenarioContext, workload: &MixedWorkload) -> Result<Vec<QuerySample>> {
    let timeout = context.remaining("run mixed performance window")?;
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_millis(workload.duration_ms);
    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::new();
    for client in 0..workload.foreground_clients {
        let stop = Arc::clone(&stop);
        let sender = sender.clone();
        let user = context.mysql_user().to_string();
        let sql = workload.foreground_sql.clone();
        let port = context.mysql_port();
        workers.push(thread::spawn(move || -> Result<()> {
            let mut connection = mysql_actor::connect(&user, port, timeout)?;
            while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
                sender.send(measure_query(
                    &mut connection,
                    "mixed-foreground",
                    client,
                    &sql,
                )?)?;
            }
            Ok(())
        }));
    }
    for (index, producer) in workload.producers.iter().cloned().enumerate() {
        let stop = Arc::clone(&stop);
        let sender = sender.clone();
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        workers.push(thread::spawn(move || -> Result<()> {
            let mut connection = mysql_actor::connect(&user, port, timeout)?;
            let mut completed = 0_u64;
            while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
                for statement in &producer.statements {
                    connection
                        .query_drop(statement)
                        .with_context(|| format!("run {} producer statement", producer.kind))?;
                }
                let sample = measure_query(
                    &mut connection,
                    &format!("mixed-{}", producer.kind),
                    index,
                    &producer.completion_query,
                )?;
                if sample.rows > 0 {
                    completed += 1;
                }
                sender.send(sample)?;
            }
            ensure!(
                completed >= producer.minimum_completions,
                "{} producer completed {completed} non-empty jobs, expected at least {}",
                producer.kind,
                producer.minimum_completions
            );
            Ok(())
        }));
    }
    drop(sender);
    while Instant::now() < deadline {
        thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(10)),
        );
    }
    stop.store(true, Ordering::Release);
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("mixed performance worker panicked"))??;
    }
    Ok(receiver.into_iter().collect())
}

fn run_query_window(
    user: &str,
    port: u16,
    queries: &[String],
    window: &Window,
    timeout: Duration,
    workload: &str,
) -> Result<Vec<QuerySample>> {
    let start = Arc::new(Barrier::new(window.concurrency));
    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::new();
    let warmup_ms = window.warmup_ms;
    let duration_ms = window.duration_ms;
    for client in 0..window.concurrency {
        let user = user.to_string();
        let queries = queries.to_vec();
        let start = Arc::clone(&start);
        let sender = sender.clone();
        let workload = workload.to_string();
        workers.push(thread::spawn(move || -> Result<()> {
            let mut connection = mysql_actor::connect(&user, port, timeout)?;
            let warmup_deadline = Instant::now() + Duration::from_millis(warmup_ms);
            let mut index = 0;
            while Instant::now() < warmup_deadline {
                let _ = measure_query(
                    &mut connection,
                    &format!("{workload}-warmup"),
                    client,
                    &queries[index % queries.len()],
                )?;
                index += 1;
            }
            start.wait();
            let deadline = Instant::now() + Duration::from_millis(duration_ms);
            while Instant::now() < deadline {
                let sample = measure_query(
                    &mut connection,
                    &workload,
                    client,
                    &queries[index % queries.len()],
                )?;
                sender.send(sample)?;
                index += 1;
            }
            Ok(())
        }));
    }
    drop(sender);
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("short performance worker panicked"))??;
    }
    Ok(receiver.into_iter().collect())
}

fn measure_query(
    connection: &mut mysql::Conn,
    workload: &str,
    client: usize,
    sql: &str,
) -> Result<QuerySample> {
    let started = Instant::now();
    let mut result = connection
        .query_iter(sql)
        .with_context(|| format!("run performance query {workload}"))?;
    let mut first_row = None;
    let mut rows = 0_u64;
    for row in result.by_ref() {
        row.with_context(|| format!("read performance query row {workload}"))?;
        first_row.get_or_insert_with(|| started.elapsed());
        rows += 1;
    }
    ensure!(rows > 0, "performance query {workload} returned no rows");
    Ok(QuerySample {
        workload: workload.to_string(),
        client,
        first_row_micros: first_row.expect("row count is positive").as_micros(),
        total_micros: started.elapsed().as_micros(),
        rows,
        outcome: "success".to_string(),
    })
}

fn run_slow_output(
    context: &ScenarioContext,
    workload: &SlowOutputWorkload,
) -> Result<Vec<QuerySample>> {
    let timeout = context.remaining("run slow-output performance window")?;
    let mut slow = connect_raw_mysql(context.mysql_user(), context.mysql_port(), timeout)?;
    send_query(&mut slow, &workload.query)?;
    let mut unread = connect_raw_mysql(context.mysql_user(), context.mysql_port(), timeout)?;
    send_query(&mut unread, &workload.query)?;

    let started = Instant::now();
    let deadline = started + Duration::from_millis(workload.duration_ms);
    let interval = Duration::from_millis(workload.interval_ms);
    slow.set_read_timeout(Some(interval.min(timeout)))?;
    let mut bytes = 0_u64;
    let mut first_byte = None;
    let mut buffer = vec![0_u8; workload.bytes_per_interval.min(64 * 1024)];
    while Instant::now() < deadline {
        let interval_deadline = Instant::now() + interval;
        let mut remaining = workload.bytes_per_interval;
        while remaining > 0 {
            let read_limit = remaining.min(buffer.len());
            let count = match slow.read(&mut buffer[..read_limit]) {
                Ok(0) => break,
                Ok(count) => count,
                Err(error)
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    break;
                }
                Err(error) => return Err(error).context("read throttled MySQL output"),
            };
            remaining -= count;
            bytes += count as u64;
            first_byte.get_or_insert_with(|| started.elapsed());
        }
        thread::sleep(interval_deadline.saturating_duration_since(Instant::now()));
    }
    drop(unread);
    let mut control = mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
    let control_rows: Vec<mysql::Row> = control.query("SELECT 1")?;
    ensure!(
        !control_rows.is_empty(),
        "control query made no progress while slow readers were active"
    );
    ensure!(bytes > 0, "slow-output client read no bytes");
    Ok(vec![QuerySample {
        workload: "slow-output".to_string(),
        client: 0,
        first_row_micros: first_byte
            .context("slow-output client read no first byte")?
            .as_micros(),
        total_micros: started.elapsed().as_micros(),
        rows: bytes,
        outcome: "success".to_string(),
    }])
}

fn connect_raw_mysql(user: &str, port: u16, timeout: Duration) -> Result<TcpStream> {
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&address, timeout)
        .with_context(|| format!("connect raw public MySQL client at {address}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let handshake = read_packet(&mut stream)?;
    ensure!(
        handshake.first().copied() == Some(10),
        "expected MySQL protocol v10 handshake"
    );
    let flags = CLIENT_LONG_PASSWORD
        | CLIENT_LONG_FLAG
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;
    let mut response = Vec::with_capacity(user.len() + 64);
    response.extend_from_slice(&flags.to_le_bytes());
    response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0_u8; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(0);
    response.extend_from_slice(b"mysql_native_password");
    response.push(0);
    write_packet(&mut stream, 1, &response)?;
    let auth = read_packet(&mut stream)?;
    if auth.first().copied() == Some(0xff) {
        bail!("raw MySQL authentication failed");
    }
    ensure!(
        auth.first().copied() == Some(0),
        "unexpected raw MySQL authentication response"
    );
    Ok(stream)
}

fn send_query(stream: &mut TcpStream, sql: &str) -> Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_packet(stream, 0, &payload)
}

fn read_packet(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header)?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    ensure!(
        length <= 16 * 1024 * 1024,
        "MySQL packet exceeds test bound"
    );
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet exceeds protocol limit");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}
