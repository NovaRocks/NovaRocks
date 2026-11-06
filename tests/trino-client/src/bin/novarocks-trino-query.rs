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

//! One-statement typed query tool for repository tests, not a product CLI.
use novarocks_test_trino_client::{
    CancelHandle, ClientConfig, ClientSession, StatementFailure, StatementResult, TrinoClient,
    TrinoValue,
};
use serde::Serialize;
use serde_json::json;
use std::{
    collections::BTreeMap,
    future::Future,
    io::{self, Read, Write},
    path::PathBuf,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    TextColumn,
    ScalarInteger,
    NoRows,
}
#[derive(Debug)]
struct Options {
    client: ClientConfig,
    session: ClientSession,
    timeout: Duration,
    mode: Mode,
    sql: Option<String>,
    session_output: Option<PathBuf>,
    observation_output: Option<PathBuf>,
}
fn arguments(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut args = args.into_iter();
    let mut values = BTreeMap::new();
    let mut properties = BTreeMap::new();
    while let Some(name) = args.next() {
        if !matches!(
            name.as_str(),
            "--host"
                | "--port"
                | "--user"
                | "--source"
                | "--time-zone"
                | "--timeout-secs"
                | "--mode"
                | "--sql"
                | "--catalog"
                | "--schema"
                | "--session"
                | "--session-output"
                | "--observation-output"
        ) {
            return Err(format!("unknown argument: {name}"));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {name}"))?;
        if name == "--session" {
            let (key, value) = value
                .split_once('=')
                .ok_or("--session requires name=value")?;
            if key.is_empty() {
                return Err("session property name is empty".into());
            }
            properties.insert(key.to_owned(), value.to_owned());
        } else if values.insert(name.clone(), value).is_some() {
            return Err(format!("duplicate argument: {name}"));
        }
    }
    let mut required = |name: &str| {
        values
            .remove(name)
            .ok_or_else(|| format!("required argument: {name}"))
    };
    let host = required("--host")?;
    let port = required("--port")?
        .parse::<u16>()
        .map_err(|_| "invalid --port")?;
    if port == 0 {
        return Err("--port must be nonzero".into());
    }
    let user = required("--user")?;
    let zone = required("--time-zone")?;
    let timeout = required("--timeout-secs")?
        .parse::<f64>()
        .map_err(|_| "invalid --timeout-secs")?;
    if !timeout.is_finite() || timeout <= 0.0 {
        return Err("--timeout-secs must be finite and positive".into());
    }
    let timeout = Duration::try_from_secs_f64(timeout).map_err(|_| "--timeout-secs overflows")?;
    if timeout.is_zero() {
        return Err("--timeout-secs is below timer precision".into());
    }
    let mode = match required("--mode")?.as_str() {
        "text-column" => Mode::TextColumn,
        "scalar-integer" => Mode::ScalarInteger,
        "no-rows" => Mode::NoRows,
        _ => return Err("--mode must be text-column, scalar-integer or no-rows".into()),
    };
    let mut session = ClientSession::new(zone);
    session.catalog = values.remove("--catalog");
    session.schema = values.remove("--schema");
    session.properties = properties;
    Ok(Options {
        client: ClientConfig {
            host,
            port,
            user,
            source: values
                .remove("--source")
                .unwrap_or_else(|| "novarocks-trino-query".into()),
        },
        session,
        timeout,
        mode,
        sql: values.remove("--sql"),
        session_output: values.remove("--session-output").map(PathBuf::from),
        observation_output: values.remove("--observation-output").map(PathBuf::from),
    })
}
fn render(result: &StatementResult, mode: Mode) -> Result<String, String> {
    match mode {
        Mode::NoRows => {
            if !result.is_completion()
                || !result.columns.is_empty()
                || !result.rows.is_empty()
                || result.update_count.is_some()
            {
                return Err("no-rows requires an empty command completion".into());
            }
            Ok(String::new())
        }
        Mode::ScalarInteger => {
            if !result.is_result_set()
                || result.columns.len() != 1
                || result.rows.len() != 1
                || result.rows[0].len() != 1
                || !matches!(
                    result.columns[0].type_signature.raw_type.as_str(),
                    "tinyint" | "smallint" | "integer" | "bigint"
                )
            {
                return Err(
                    "scalar-integer requires exactly one non-NULL integer result cell".into(),
                );
            }
            match &result.rows[0][0] {
                TrinoValue::Integer(value) => Ok(format!("{value}\n")),
                _ => Err("scalar-integer requires exactly one non-NULL integer result cell".into()),
            }
        }
        Mode::TextColumn => {
            if !result.is_result_set()
                || result.columns.len() != 1
                || !matches!(
                    result.columns[0].type_signature.raw_type.as_str(),
                    "varchar" | "char"
                )
            {
                return Err("text-column requires one text result column".into());
            }
            let mut output = String::new();
            for row in &result.rows {
                let [TrinoValue::Text(text)] = row.as_slice() else {
                    return Err("text-column requires one non-NULL text cell per row".into());
                };
                output.push_str(text);
                output.push('\n');
            }
            Ok(output)
        }
    }
}
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum FailureReason {
    Statement(StatementFailure),
    Local(BTreeMap<String, String>),
}
impl FailureReason {
    fn local(kind: &str, message: impl Into<String>) -> Self {
        Self::Local(BTreeMap::from([(kind.to_owned(), message.into())]))
    }
}
#[derive(Debug, Serialize)]
struct Failure {
    failure: FailureReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    statement_failure: Option<FailureReason>,
    cleanup_failures: Vec<FailureReason>,
    worker_joined: bool,
}
impl Failure {
    fn local(kind: &str, message: impl Into<String>) -> Self {
        Self {
            failure: FailureReason::local(kind, message),
            statement_failure: None,
            cleanup_failures: Vec::new(),
            worker_joined: false,
        }
    }
    fn statement(error: StatementFailure) -> Self {
        Self {
            failure: FailureReason::Statement(error),
            statement_failure: None,
            cleanup_failures: Vec::new(),
            worker_joined: false,
        }
    }
}
fn statement_worker(
    options: Options,
    sql: String,
    deadline: Instant,
    cancel_sender: oneshot::Sender<CancelHandle>,
    result_sender: oneshot::Sender<Result<String, Failure>>,
    retired: mpsc::Receiver<CancelHandle>,
) {
    // Every blocking Client is constructed/run here. Keep its original owner
    // until the coordinator returns the published cancellation clone, so its
    // final runtime Drop is also on this same ordinary worker thread.
    let mut result_sender = Some(result_sender);
    let result = (|| {
        let client = TrinoClient::new(options.client).map_err(Failure::statement)?;
        let mut session = options.session;
        let pending = client
            .submit(&session, &sql, deadline)
            .map_err(Failure::statement)?;
        let cancel = pending.cancel_handle();
        match cancel_sender.send(cancel) {
            Ok(()) => {}
            Err(cancel) => {
                let cleanup = cancel.delete(deadline);
                let mut failure = Failure::local(
                    "Coordinator",
                    "query coordinator exited before cancellation handoff",
                );
                if let Err(error) = cleanup {
                    failure
                        .cleanup_failures
                        .push(FailureReason::Statement(error));
                }
                return Err(failure);
            }
        }
        let result = pending.run(deadline).map_err(Failure::statement);
        let result = result.and_then(|(result, effects)| {
            session.apply(&effects);
            if let Some(path) = options.session_output {
                let value = json!({"catalog":session.catalog,"schema":session.schema,"properties":session.properties,"time_zone":session.time_zone});
                std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).map_err(|error| Failure::local("SessionOutput", error.to_string()))?;
            }
            let output = render(&result, options.mode).map_err(|error| Failure::local("Output", error))?;
            if let Some(path) = options.observation_output {
                // These are the shared client's original completed-result facts,
                // not a reconstructed page count or a second protocol observer.
                let value = json!({"query_id":result.query_id,"pages":result.pages,"update_type":result.update_type,"update_count":result.update_count});
                std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).map_err(|error| Failure::local("ObservationOutput", error.to_string()))?;
            }
            Ok(output)
        });
        // The result must be sent before waiting for the clone to return.
        let _ = result_sender
            .take()
            .expect("one original result sender")
            .send(result);
        if let Ok(cancel) = retired.recv() {
            drop(cancel);
        }
        drop(client);
        Ok(())
    })();
    // Setup failures precede the normal result handoff; dropping this channel
    // would hide their typed cause. The normal branch consumed its Sender.
    if let Err(error) = result
        && let Some(sender) = result_sender.take()
    {
        let _ = sender.send(Err(error));
    }
}

async fn coordinate(
    options: Options,
    sql: String,
    deadline: Instant,
    interrupted: impl Future<Output = String>,
) -> Result<String, Failure> {
    let (cancel_sender, mut cancel_receiver) = oneshot::channel();
    let (result_sender, mut result_receiver) = oneshot::channel();
    let (retire, retired) = mpsc::channel();
    let worker = thread::Builder::new()
        .name("trino-query-original".into())
        .spawn(move || {
            statement_worker(
                options,
                sql,
                deadline,
                cancel_sender,
                result_sender,
                retired,
            )
        })
        .map_err(|error| Failure::local("Worker", error.to_string()))?;
    tokio::pin!(interrupted);
    let mut cancel = None;
    let mut cancel_received = false;
    let mut signal = None;
    let mut cancellation = None;
    let mut result;
    loop {
        tokio::select! {
            received = &mut cancel_receiver, if !cancel_received => {
                cancel_received = true;
                cancel = received.ok();
            }
            received = &mut result_receiver => {
                result = received.unwrap_or_else(|_| Err(Failure::local("Worker", "original worker exited without its result")));
                break;
            }
            name = &mut interrupted, if signal.is_none() => { signal = Some(name); }
        }
        if signal.is_some()
            && cancellation.is_none()
            && let Some(handle) = cancel.take()
        {
            cancellation = Some(tokio::task::spawn_blocking(move || {
                let outcome = handle.delete(deadline);
                (handle, outcome)
            }));
        }
    }
    if !cancel_received {
        cancel = cancel_receiver.await.ok();
    }
    // A signal may race the final result or the handoff. Still cancel the
    // exact continuation if it remains, and join that original cancellation.
    if signal.is_some()
        && cancellation.is_none()
        && let Some(handle) = cancel.take()
    {
        cancellation = Some(tokio::task::spawn_blocking(move || {
            let outcome = handle.delete(deadline);
            (handle, outcome)
        }));
    }
    let mut cleanup = Vec::new();
    if let Some(job) = cancellation {
        match job.await {
            Ok((handle, outcome)) => {
                cancel = Some(handle);
                if let Err(error) = outcome {
                    cleanup.push(FailureReason::Statement(error));
                }
            }
            Err(_) => cleanup.push(FailureReason::local(
                "CancellationWorker",
                "original cancellation worker panicked",
            )),
        }
    }
    if let Some(handle) = cancel
        && let Err(error) = retire.send(handle)
    {
        // A panicked original worker may already have dropped its owner.
        // Dispose the final blocking-client clone outside the async task.
        tokio::task::spawn_blocking(move || drop(error.0))
            .await
            .ok();
    }
    drop(retire);
    let panicked = worker.join().is_err();
    if let Some(name) = signal {
        let mut failure = Failure::local("Interrupted", name);
        if let Err(error) = result {
            failure.statement_failure = Some(error.failure);
            failure.cleanup_failures.extend(error.cleanup_failures);
        }
        result = Err(failure);
    }
    if panicked && result.is_ok() {
        result = Err(Failure::local("Worker", "original worker panicked"));
    }
    if !cleanup.is_empty() && result.is_ok() {
        result = Err(Failure::local("Cleanup", "query cancellation failed"));
    }
    if let Err(error) = &mut result {
        error.worker_joined = true;
        error.cleanup_failures.extend(cleanup);
        if panicked {
            error
                .cleanup_failures
                .push(FailureReason::local("Worker", "original worker panicked"));
        }
    }
    result
}

fn main() {
    let started = Instant::now();
    let result = (|| {
        let mut args = std::env::args().skip(1).peekable();
        if args.peek().is_some_and(|value| value == "--help") {
            println!(
                "novarocks-trino-query --host HOST --port PORT --user USER --time-zone ZONE --timeout-secs SECONDS --mode text-column|scalar-integer|no-rows [--sql ONE_SQL] [--catalog CATALOG] [--schema SCHEMA] [--session NAME=VALUE] [--session-output PATH] [--observation-output PATH]; omitted --sql reads one SQL from stdin"
            );
            return Ok(String::new());
        }
        let mut options = arguments(args).map_err(|error| Failure::local("Usage", error))?;
        let sql = match options.sql.take() {
            Some(sql) => sql,
            None => {
                let mut sql = String::new();
                io::stdin()
                    .read_to_string(&mut sql)
                    .map_err(|error| Failure::local("Input", error.to_string()))?;
                sql
            }
        };
        if sql.trim().is_empty() {
            return Err(Failure::local(
                "Usage",
                "one nonempty SQL statement is required",
            ));
        }
        let deadline = started
            .checked_add(options.timeout)
            .ok_or_else(|| Failure::local("Usage", "timeout deadline overflows"))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| Failure::local("Coordinator", error.to_string()))?;
        runtime.block_on(async {
            use tokio::signal::unix::{SignalKind, signal};
            let mut interrupt = signal(SignalKind::interrupt()).map_err(|error| Failure::local("Signal", error.to_string()))?;
            let mut terminate = signal(SignalKind::terminate()).map_err(|error| Failure::local("Signal", error.to_string()))?;
            coordinate(options, sql, deadline, async move {
                tokio::select! { _ = interrupt.recv() => "SIGINT".into(), _ = terminate.recv() => "SIGTERM".into() }
            }).await
        })
    })();
    match result {
        Ok(output) => {
            if let Err(error) = io::stdout().write_all(output.as_bytes()) {
                eprintln!("{}", json!({"failure":{"Output":error.to_string()}}));
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("{}", serde_json::to_string(&error).unwrap());
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_test_trino_client::{Column, TypeSignature};
    use serde::Deserialize;
    use std::{
        net::{TcpListener, TcpStream},
        sync::atomic::{AtomicU64, Ordering},
    };

    fn options(port: u16, mode: Mode) -> Options {
        let mut session = ClientSession::new("UTC");
        session.catalog = Some("lake".into());
        session.schema = Some("ns".into());
        session.properties.insert("pipeline_dop".into(), "3".into());
        Options {
            client: ClientConfig {
                host: "127.0.0.1".into(),
                port,
                user: "root".into(),
                source: "tool-test".into(),
            },
            session,
            timeout: Duration::from_secs(5),
            mode,
            sql: None,
            session_output: None,
            observation_output: None,
        }
    }
    fn accept(listener: &TcpListener) -> TcpStream {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    return stream;
                }
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(1))
                }
                Err(error) => panic!("fixture did not receive the original request: {error}"),
            }
        }
    }
    fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
            assert!(bytes.len() < 65536);
        }
        let headers = String::from_utf8(bytes.clone()).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|n| n.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let old = bytes.len();
        bytes.resize(old + length, 0);
        stream.read_exact(&mut bytes[old..]).unwrap();
        String::from_utf8(bytes).unwrap()
    }
    fn reply(stream: &mut TcpStream, status: u16, body: &str, effects: bool) {
        let effects = if effects {
            "X-Trino-Set-Session: pipeline_dop=2\r\n"
        } else {
            ""
        };
        write!(stream, "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{effects}Connection: close\r\n\r\n{body}", body.len()).unwrap();
        stream.flush().unwrap();
    }
    fn server(pages: Vec<&'static str>) -> (u16, thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        (
            port,
            thread::spawn(move || {
                pages
                    .into_iter()
                    .enumerate()
                    .map(|(index, page)| {
                        let mut stream = accept(&listener);
                        let request = read_request(&mut stream);
                        reply(
                            &mut stream,
                            200,
                            &page.replace("$BASE", &format!("http://127.0.0.1:{port}")),
                            index == 2,
                        );
                        request
                    })
                    .collect()
            }),
        )
    }
    fn run(options: Options, sql: &str) -> Result<String, Failure> {
        let deadline = Instant::now() + options.timeout;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(coordinate(
                options,
                sql.into(),
                deadline,
                std::future::pending(),
            ))
    }
    #[test]
    fn one_statement_arguments_keep_session_and_semicolons_without_splitting() {
        let input = [
            "--host",
            "localhost",
            "--port",
            "8080",
            "--user",
            "root",
            "--time-zone",
            "Asia/Shanghai",
            "--timeout-secs",
            "2.5",
            "--mode",
            "text-column",
            "--sql",
            "SELECT 'a;b'",
            "--catalog",
            "ice",
            "--schema",
            "db",
            "--session",
            "pipeline_dop=4",
            "--observation-output",
            "/tmp/query-observation.json",
        ];
        let parsed = arguments(input.into_iter().map(str::to_owned)).unwrap();
        assert_eq!(parsed.sql.as_deref(), Some("SELECT 'a;b'"));
        assert_eq!(parsed.session.catalog.as_deref(), Some("ice"));
        assert_eq!(parsed.session.schema.as_deref(), Some("db"));
        assert_eq!(parsed.session.properties["pipeline_dop"], "4");
        assert_eq!(parsed.timeout, Duration::from_millis(2500));
        assert_eq!(
            parsed.observation_output,
            Some(PathBuf::from("/tmp/query-observation.json"))
        );
        assert!(arguments(["--host".into(), "localhost".into()]).is_err());
    }
    fn result(ty: &str, rows: Vec<Vec<TrinoValue>>) -> StatementResult {
        StatementResult {
            columns: vec![Column {
                name: "value".into(),
                type_name: ty.into(),
                type_signature: TypeSignature::simple(ty),
            }],
            rows,
            query_id: "q".into(),
            pages: 1,
            update_type: None,
            update_count: None,
        }
    }
    #[test]
    fn tool_failure_preserves_original_error_fields_and_numeric_source_text() {
        let original = r#"{"message":"original","errorCode":3,"errorName":"USER_CANCELED","errorType":"USER_ERROR","failureInfo":{"originalNumber":1.2300e+40}}"#;
        let error = serde_json::from_str(original).unwrap();
        let failure = Failure::statement(StatementFailure::Trino(error));
        assert_eq!(
            serde_json::to_string(&failure).unwrap(),
            format!(
                "{{\"failure\":{{\"Trino\":{original}}},\"cleanup_failures\":[],\"worker_joined\":false}}"
            )
        );
    }
    #[test]
    fn three_modes_require_original_typed_shape_cardinality_and_nonnull_values() {
        assert_eq!(
            render(
                &result("bigint", vec![vec![TrinoValue::Integer(0)]]),
                Mode::ScalarInteger
            )
            .unwrap(),
            "0\n"
        );
        for bad in [
            result("bigint", vec![vec![TrinoValue::Null]]),
            result("double", vec![vec![TrinoValue::Double(1.0)]]),
            result("varchar", vec![vec![TrinoValue::Text("1".into())]]),
            result("bigint", vec![]),
            result(
                "bigint",
                vec![vec![TrinoValue::Integer(1)], vec![TrinoValue::Integer(2)]],
            ),
        ] {
            assert!(render(&bad, Mode::ScalarInteger).is_err());
        }
        assert_eq!(
            render(
                &result(
                    "varchar",
                    vec![
                        vec![TrinoValue::Text("a\nb".into())],
                        vec![TrinoValue::Text("c".into())]
                    ]
                ),
                Mode::TextColumn
            )
            .unwrap(),
            "a\nb\nc\n"
        );
        assert!(
            render(
                &result("varchar", vec![vec![TrinoValue::Null]]),
                Mode::TextColumn
            )
            .is_err()
        );
        let mut command = result("bigint", vec![vec![TrinoValue::Integer(1)]]);
        command.update_type = Some("INSERT".into());
        assert!(render(&command, Mode::NoRows).is_err());
        command.columns.clear();
        command.rows.clear();
        command.update_type = Some("CREATE TABLE".into());
        assert_eq!(render(&command, Mode::NoRows).unwrap(), "");
        assert!(
            render(
                &result("bigint", vec![vec![TrinoValue::Integer(1)]]),
                Mode::NoRows
            )
            .is_err()
        );
    }
    #[test]
    fn shared_execution_consumes_all_explain_pages_and_effects_before_output() {
        static SERIAL: AtomicU64 = AtomicU64::new(0);
        let output = std::env::temp_dir().join(format!(
            "nr-trino-tool-effects-{}-{}.json",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let observation = output.with_extension("observation.json");
        let (port, server) = server(vec![
            r#"{"id":"q","nextUri":"$BASE/next/0"}"#,
            r#"{"id":"q","columns":[{"name":"plan","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[]}}],"data":[["first"],["second"]],"nextUri":"$BASE/next/1"}"#,
            r#"{"id":"q"}"#,
        ]);
        let mut options = options(port, Mode::TextColumn);
        options.session_output = Some(output.clone());
        options.observation_output = Some(observation.clone());
        assert_eq!(run(options, "EXPLAIN SELECT 1").unwrap(), "first\nsecond\n");
        #[derive(Deserialize)]
        struct SavedSession {
            properties: BTreeMap<String, String>,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        #[serde(deny_unknown_fields)]
        struct ExplainObservation {
            query_id: String,
            pages: usize,
            update_type: (),
            update_count: (),
        }
        let effects: SavedSession =
            serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        std::fs::remove_file(output).unwrap();
        assert_eq!(effects.properties["pipeline_dop"], "2");
        let observed: ExplainObservation =
            serde_json::from_slice(&std::fs::read(&observation).unwrap()).unwrap();
        std::fs::remove_file(observation).unwrap();
        assert_eq!(
            observed,
            ExplainObservation {
                query_id: "q".into(),
                pages: 3,
                update_type: (),
                update_count: (),
            }
        );
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /v1/statement "));
        assert!(requests[0].ends_with("EXPLAIN SELECT 1"));
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("x-trino-session: pipeline_dop=3")
        );
        assert!(requests[2].starts_with("GET /next/1 "));
    }
    #[test]
    fn late_typed_error_keeps_its_original_wire_object_and_produces_no_success_text() {
        let (port, server) = server(vec![
            r#"{"id":"q","nextUri":"$BASE/next/0"}"#,
            r#"{"id":"q","columns":[{"name":"plan","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[]}}],"data":[["partial"]],"nextUri":"$BASE/next/1"}"#,
            r#"{"id":"q","error":{"message":"actual late failure","errorCode":12,"errorName":"ALREADY_EXISTS","errorType":"USER_ERROR","extra":{"original":true}}}"#,
        ]);
        let error = run(options(port, Mode::TextColumn), "EXPLAIN SELECT 1").unwrap_err();
        let FailureReason::Statement(StatementFailure::Trino(original)) = &error.failure else {
            panic!("late failure lost its original typed Trino cause");
        };
        assert_eq!(original.error_code, 12);
        assert_eq!(
            serde_json::to_string(original).unwrap(),
            r#"{"message":"actual late failure","errorCode":12,"errorName":"ALREADY_EXISTS","errorType":"USER_ERROR","extra":{"original":true}}"#
        );
        assert!(error.worker_joined);
        assert_eq!(server.join().unwrap().len(), 3);
    }
    #[test]
    fn interruption_deletes_the_same_continuation_and_joins_the_original_worker() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let (arrived, signal) = oneshot::channel();
        let server = thread::spawn(move || {
            let mut post = accept(&listener);
            let request = read_request(&mut post);
            assert!(request.starts_with("POST /v1/statement "));
            reply(
                &mut post,
                200,
                &format!(r#"{{"id":"q","nextUri":"http://127.0.0.1:{port}/next/0"}}"#),
                false,
            );
            drop(post);
            let mut get = accept(&listener);
            let request = read_request(&mut get);
            assert!(request.starts_with("GET /next/0 "));
            arrived.send(()).unwrap();
            let mut delete = accept(&listener);
            let request = read_request(&mut delete);
            assert!(request.starts_with("DELETE /next/0 "));
            reply(&mut delete, 204, "", false);
            reply(
                &mut get,
                200,
                r#"{"id":"q","error":{"message":"actual cancel","errorCode":3,"errorName":"USER_CANCELED","errorType":"USER_ERROR"}}"#,
                false,
            );
        });
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(coordinate(
                options(port, Mode::TextColumn),
                "SELECT sleep(10)".into(),
                Instant::now() + Duration::from_secs(5),
                async move {
                    signal.await.unwrap();
                    "test-interrupt".into()
                },
            ))
            .unwrap_err();
        server.join().unwrap();
        let FailureReason::Local(local) = &error.failure else {
            panic!("interrupt lost its local cause");
        };
        assert_eq!(local["Interrupted"], "test-interrupt");
        let Some(FailureReason::Statement(StatementFailure::Trino(statement))) =
            &error.statement_failure
        else {
            panic!("interrupt lost the original typed statement failure");
        };
        assert_eq!(statement.error_code, 3);
        assert!(error.worker_joined);
        assert!(error.cleanup_failures.is_empty());
    }
}
