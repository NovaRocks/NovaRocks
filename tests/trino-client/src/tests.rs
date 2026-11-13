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

//! Protocol and decoder correctness tests use original HTTP and JSON text as
//! independent inputs. The fake server owns and joins its one bounded thread;
//! no NovaRocks service, external fixture or persistent resource is required.
use super::*;
use serde_json::value::RawValue;
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
}
struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: String,
    disconnect: bool,
}
impl Reply {
    fn json(body: &str) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: body.into(),
            disconnect: false,
        }
    }
    fn header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.into()));
        self
    }
}
struct FakeServer {
    port: u16,
    worker: Option<JoinHandle<Vec<Request>>>,
}
impl FakeServer {
    fn new(replies: Vec<Reply>) -> Self {
        Self::with_request_hook(replies, |_, _| false)
    }
    fn with_request_hook(
        replies: Vec<Reply>,
        mut before_reply: impl FnMut(&Request, &mut TcpStream) -> bool + Send + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let worker = thread::spawn(move || {
            let mut requests = Vec::new();
            for reply in replies {
                let until = Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                Instant::now() < until,
                                "fake server expected another request; received {requests:?}"
                            );
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(e) => panic!("fake accept: {e}"),
                    }
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let request = read_request(&mut stream);
                let close_without_response = before_reply(&request, &mut stream);
                requests.push(request);
                if close_without_response {
                    continue;
                }
                if reply.disconnect {
                    // A valid response prefix with an incomplete body exercises
                    // a read interruption, rather than a connection refusal.
                    stream.write_all(b"HTTP/1.1 200 Test\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"id\":").unwrap();
                    stream.flush().unwrap();
                    continue;
                }
                let body = reply
                    .body
                    .replace("$BASE", &format!("http://127.0.0.1:{port}"))
                    .replace("$USER_BASE", &format!("http://user@127.0.0.1:{port}"))
                    .replace("$HTTPS_BASE", &format!("https://127.0.0.1:{port}"))
                    .replace(
                        "$PORT_BASE",
                        &format!("http://127.0.0.1:{}", port.wrapping_add(1)),
                    );
                write!(stream,"HTTP/1.1 {} Test\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: application/json\r\n",reply.status,body.len()).unwrap();
                for (name, value) in reply.headers {
                    write!(stream, "{name}: {value}\r\n").unwrap();
                }
                write!(stream, "\r\n{body}").unwrap();
                stream.flush().unwrap();
            }
            requests
        });
        Self {
            port,
            worker: Some(worker),
        }
    }
    fn client(&self) -> TrinoClient {
        TrinoClient::new(ClientConfig {
            host: "127.0.0.1".into(),
            port: self.port,
            user: "root".into(),
            source: "typed-tests".into(),
        })
        .unwrap()
    }
    fn client_with_request_timeout_cap(&self, cap: Duration) -> TrinoClient {
        TrinoClient::new_with_request_timeout_cap(
            ClientConfig {
                host: "127.0.0.1".into(),
                port: self.port,
                user: "root".into(),
                source: "typed-tests".into(),
            },
            cap,
        )
        .unwrap()
    }
    fn finish(mut self) -> Vec<Request> {
        self.worker.take().unwrap().join().unwrap()
    }
}
impl Drop for FakeServer {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
fn read_request(stream: &mut TcpStream) -> Request {
    let mut raw = Vec::new();
    let mut byte = [0];
    while !raw.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        raw.push(byte[0]);
    }
    let head = String::from_utf8(raw).unwrap();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap().split_whitespace();
    let method = first.next().unwrap().into();
    let path = first.next().unwrap().into();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let len = headers
        .get("content-length")
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(0);
    let mut body = vec![0; len];
    stream.read_exact(&mut body).unwrap();
    Request {
        method,
        path,
        headers,
        body: String::from_utf8(body).unwrap(),
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn session() -> ClientSession {
    ClientSession::new("Asia/Shanghai")
}
fn queued() -> Reply {
    Reply::json(r#"{"id":"q","nextUri":"$BASE/next/0"}"#)
}
fn terminal() -> Reply {
    Reply::json(r#"{"id":"q"}"#)
}
fn sig(text: &str) -> TypeSignature {
    serde_json::from_str(text).unwrap()
}
fn cell(ty: &TypeSignature, text: &str) -> TrinoValue {
    decode_value(ty, RawValue::from_string(text.into()).unwrap().as_ref()).unwrap()
}

fn depth64_page(raw_type: &str) -> String {
    // Original wire text: 63 containers plus an integer leaf, with a NULL
    // sibling at every level. Do not serialize the client's typed model.
    let mut signature = r#"{"rawType":"integer","arguments":[]}"#.to_owned();
    let mut type_name = "integer".to_owned();
    let mut value = "42".to_owned();
    for _ in 0..63 {
        match raw_type {
            "array" => {
                signature = format!(
                    r#"{{"rawType":"array","arguments":[{{"kind":"TYPE","value":{signature}}}]}}"#
                );
                type_name = format!("array({type_name})");
                value = format!("[{value},null]");
            }
            "row" => {
                signature = format!(
                    r#"{{"rawType":"row","arguments":[{{"kind":"NAMED_TYPE","value":{{"fieldName":{{"name":"value"}},"typeSignature":{signature}}}}},{{"kind":"NAMED_TYPE","value":{{"fieldName":{{"name":"absent"}},"typeSignature":{{"rawType":"integer","arguments":[]}}}}}}]}}"#
                );
                type_name = format!("row(value {type_name},absent integer)");
                value = format!("[{value},null]");
            }
            "map" => {
                signature = format!(
                    r#"{{"rawType":"map","arguments":[{{"kind":"TYPE","value":{{"rawType":"varchar","arguments":[]}}}},{{"kind":"TYPE","value":{signature}}}]}}"#
                );
                type_name = format!("map(varchar,{type_name})");
                value = format!(r#"{{"value":{value},"absent":null}}"#);
            }
            _ => panic!("unsupported test shape"),
        }
    }
    format!(
        r#"{{"id":"q","columns":[{{"name":"deep","type":"{type_name}","typeSignature":{signature}}}],"data":[[{value}],[null]],"nextUri":"$BASE/next/deep"}}"#
    )
}

#[test]
fn depth64_array_row_and_map_pages_decode_on_post_and_get() {
    for raw_type in ["array", "row", "map"] {
        for initial_page in [true, false] {
            let page = Reply::json(&depth64_page(raw_type));
            let replies = if initial_page {
                vec![page, terminal()]
            } else {
                vec![queued(), page, terminal()]
            };
            let server = FakeServer::new(replies);
            let pending = server
                .client()
                .submit(&session(), "SELECT deep", deadline())
                .unwrap();
            let (result, effects) = pending.run(deadline()).unwrap();
            assert_eq!(result.query_id, "q");
            assert_eq!(result.pages, if initial_page { 2 } else { 3 });
            assert_eq!(effects, SessionEffects::default());
            assert_eq!(result.columns.len(), 1);
            assert_eq!(result.columns[0].name, "deep");
            assert_eq!(result.rows.len(), 2);
            assert_eq!(result.rows[0].len(), 1);
            assert_eq!(result.rows[1], vec![TrinoValue::Null]);

            // Inspect every decoded branch against the wire's independent
            // 63-container/42/NULL oracle, including map entry order and keys.
            let mut value = &result.rows[0][0];
            let mut signature = &result.columns[0].type_signature;
            for _ in 0..63 {
                assert_eq!(signature.raw_type, raw_type);
                match (raw_type, value) {
                    ("array", TrinoValue::Array(children)) | ("row", TrinoValue::Row(children)) => {
                        assert_eq!(children.len(), 2);
                        assert_eq!(children[1], TrinoValue::Null);
                        value = &children[0];
                        signature = match &signature.arguments[0] {
                            TypeArgument::Type(child) if raw_type == "array" => child,
                            TypeArgument::NamedType(child) if raw_type == "row" => {
                                &child.type_signature
                            }
                            other => panic!("unexpected signature argument: {other:?}"),
                        };
                    }
                    ("map", TrinoValue::Map(entries)) => {
                        assert_eq!(entries.len(), 2);
                        assert_eq!(entries[0].0, TrinoValue::Text("value".into()));
                        assert_eq!(
                            entries[1],
                            (TrinoValue::Text("absent".into()), TrinoValue::Null)
                        );
                        value = &entries[0].1;
                        signature = signature.type_argument(1).unwrap();
                    }
                    other => panic!("unexpected decoded shape: {other:?}"),
                }
            }
            assert_eq!(signature.raw_type, "integer");
            assert!(signature.arguments.is_empty());
            assert_eq!(*value, TrinoValue::Integer(42));
            let requests = server.finish();
            let methods = requests
                .iter()
                .map(|r| r.method.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                methods,
                if initial_page {
                    vec!["POST", "GET"]
                } else {
                    vec!["POST", "GET", "GET"]
                }
            );
            assert_eq!(requests.last().unwrap().path, "/next/deep");
        }
    }
}

#[test]
fn trailing_page_garbage_is_rejected_on_post_and_get() {
    for suffix in [" trailing", " {}"] {
        let malformed = format!(r#"{{"id":"q"}}{suffix}"#);
        let server = FakeServer::new(vec![Reply::json(&malformed)]);
        let error = server
            .client()
            .submit(&session(), "SELECT 1", deadline())
            .err()
            .expect("submission must reject trailing JSON");
        assert!(
            matches!(error, StatementFailure::Protocol(ref reason) if reason.contains("trailing characters")),
            "{error:?}"
        );
        assert_eq!(server.finish().len(), 1);

        let server = FakeServer::new(vec![
            queued(),
            Reply::json(&malformed),
            Reply {
                status: 204,
                ..Reply::json("")
            },
        ]);
        let pending = server
            .client()
            .submit(&session(), "SELECT 1", deadline())
            .unwrap();
        let error = pending.run(deadline()).unwrap_err();
        assert!(
            matches!(error, StatementFailure::Protocol(ref reason) if reason.contains("trailing characters")),
            "{error:?}"
        );
        let requests = server.finish();
        assert_eq!(
            requests
                .iter()
                .map(|r| r.method.as_str())
                .collect::<Vec<_>>(),
            vec!["POST", "GET", "DELETE"]
        );
        assert_eq!(requests[2].path, "/next/0");
    }
}

#[test]
fn pages_include_empty_polls_late_columns_and_the_final_acknowledgment() {
    let server = FakeServer::new(vec![
        queued(),
        Reply::json(r#"{"id":"q","data":[],"nextUri":"$BASE/next/1"}"#),
        Reply::json(
            r#"{"id":"q","columns":[{"name":"v","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}],"data":[[42]],"nextUri":"$BASE/next/2"}"#,
        ),
        terminal(),
    ]);
    let client = server.client();
    let pending = client.submit(&session(), "SELECT 42", deadline()).unwrap();
    assert_eq!(pending.query_id, "q");
    assert!(
        pending
            .initial_next_uri
            .as_ref()
            .unwrap()
            .ends_with("/next/0")
    );
    let (result, effects) = pending.run(deadline()).unwrap();
    assert_eq!(result.pages, 4);
    assert_eq!(result.rows, vec![vec![TrinoValue::Integer(42)]]);
    assert_eq!(effects, SessionEffects::default());
    let requests = server.finish();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        vec!["POST", "GET", "GET", "GET"]
    );
    assert_eq!(requests[0].body, "SELECT 42");
    assert_eq!(requests.last().unwrap().path, "/next/2");
    for r in requests {
        assert_eq!(r.headers["x-trino-time-zone"], "Asia/Shanghai");
        assert_eq!(
            r.headers["x-trino-client-capabilities"],
            CLIENT_CAPABILITIES
        );
        assert_eq!(r.headers["x-trino-user"], "root");
    }
}
#[test]
fn every_page_contributes_session_effects_and_only_success_applies_them() {
    for cap in [None, Some(Duration::from_secs(2))] {
        let server = FakeServer::new(vec![
            queued()
                .header("X-Trino-Set-Catalog", "cat")
                .header("X-Trino-Set-Session", "a=A%2CB"),
            Reply::json(r#"{"id":"q","nextUri":"$BASE/next/1"}"#)
                .header("X-Trino-Set-Schema", "db")
                .header("X-Trino-Clear-Session", "old")
                .header("X-Trino-Set-Session", "a=C+value"),
            terminal().header("X-Trino-Set-Session", "last=hello%2Bworld"),
        ]);
        let mut state = session();
        state.properties.insert("old".into(), "gone".into());
        let client = match cap {
            Some(cap) => server.client_with_request_timeout_cap(cap),
            None => server.client(),
        };
        client.execute(&mut state, "SET a=1", deadline()).unwrap();
        assert_eq!(state.catalog.as_deref(), Some("cat"));
        assert_eq!(state.schema.as_deref(), Some("db"));
        assert_eq!(
            state.properties,
            BTreeMap::from([
                ("a".into(), "C value".into()),
                ("last".into(), "hello+world".into())
            ])
        );
        server.finish();
    }
}

#[test]
fn failed_statement_discards_accumulated_effects_and_preserves_error_fields() {
    let server = FakeServer::new(vec![
        queued().header("X-Trino-Set-Catalog", "changed"),
        Reply::json(
            r#"{"id":"q","error":{"message":"[sql.analyze.no_table] at line 2 column 3","errorCode":7,"errorName":"NO_TABLE","errorType":"USER_ERROR"}}"#,
        ),
    ]);
    let mut state = session();
    let original = state.clone();
    let error = server
        .client()
        .execute(&mut state, "bad", deadline())
        .unwrap_err();
    assert_eq!(state, original);
    assert_eq!(
        error.to_string(),
        "TrinoError { name=NO_TABLE type=USER_ERROR code=7 }: [sql.analyze.no_table] at line 2 column 3"
    );
    server.finish();
}
#[test]
fn unsupported_session_change_headers_are_rejected_on_any_page() {
    for header in [
        "X-Trino-Set-Path",
        "X-Trino-Set-Role",
        "X-Trino-Added-Prepare",
        "X-Trino-Deallocated-Prepare",
        "X-Trino-Started-Transaction-Id",
        "X-Trino-Clear-Transaction-Id",
        "X-Trino-Set-Authorization-User",
        "X-Trino-Reset-Authorization-User",
    ] {
        let server = FakeServer::new(vec![terminal().header(header, "value")]);
        let error = server
            .client()
            .submit(&session(), "SELECT 1", deadline())
            .unwrap()
            .run(deadline())
            .unwrap_err();
        assert!(
            matches!(error, StatementFailure::UnsupportedSessionHeader(_)),
            "{header}: {error:?}"
        );
        server.finish();
    }
}
#[test]
fn schema_without_catalog_sends_the_default_catalog_and_form_encoded_properties() {
    let server = FakeServer::new(vec![terminal()]);
    let mut state = session();
    state.schema = Some("db".into());
    state.properties.insert("sql_mode".into(), "a,b +中".into());
    server
        .client()
        .execute(&mut state, "SELECT 1", deadline())
        .unwrap();
    let requests = server.finish();
    assert_eq!(requests[0].headers["x-trino-catalog"], "default_catalog");
    assert_eq!(requests[0].headers["x-trino-schema"], "db");
    assert_eq!(
        requests[0].headers["x-trino-session"],
        "sql_mode=a%2Cb+%2B%E4%B8%AD"
    );
}
#[test]
fn http_status_failures_remain_distinct_from_transport_and_trino_errors() {
    for status in [400, 401, 403, 404, 410] {
        let server = FakeServer::new(vec![Reply {
            status,
            ..Reply::json("denied")
        }]);
        let error = server
            .client()
            .submit(&session(), "SELECT 1", deadline())
            .err()
            .unwrap();
        assert_eq!(
            error,
            StatementFailure::Http {
                status,
                body: "denied".into()
            }
        );
        server.finish();
    }
}
#[test]
fn a_disconnected_continuation_is_not_retried() {
    let server = FakeServer::new(vec![
        queued(),
        Reply {
            disconnect: true,
            ..terminal()
        },
    ]);
    let error = server
        .client()
        .submit(&session(), "SELECT 1", deadline())
        .unwrap()
        .run(deadline())
        .unwrap_err();
    assert!(matches!(error, StatementFailure::Transport(_)), "{error:?}");
    assert!(error.to_string().contains("Trino transport failure"));
    assert_eq!(server.finish().len(), 2);
}
#[test]
fn cancel_handle_can_be_used_on_another_thread_and_sends_the_time_zone() {
    let server = FakeServer::new(vec![
        queued(),
        Reply {
            status: 204,
            ..Reply::json("")
        },
        Reply::json(
            r#"{"id":"q","error":{"message":"canceled","errorCode":1,"errorName":"USER_CANCELED","errorType":"USER_ERROR"}}"#,
        ),
    ]);
    let pending = server
        .client()
        .submit(&session(), "SELECT 1", deadline())
        .unwrap();
    let cancel = pending.cancel_handle();
    thread::spawn(move || cancel.delete(deadline()).unwrap())
        .join()
        .unwrap();
    assert!(
        matches!(pending.run(deadline()),Err(StatementFailure::Trino(TrinoError{error_name,..})) if error_name=="USER_CANCELED")
    );
    let requests = server.finish();
    assert_eq!(requests[1].method, "DELETE");
    assert_eq!(requests[1].headers["x-trino-time-zone"], "Asia/Shanghai");
}
#[test]
fn an_expired_run_deadline_best_effort_cancels_without_starting_execution() {
    let server = FakeServer::new(vec![
        queued(),
        Reply {
            status: 204,
            ..Reply::json("")
        },
    ]);
    let pending = server
        .client()
        .submit(&session(), "SELECT 1", deadline())
        .unwrap();
    assert_eq!(
        pending
            .run(Instant::now() - Duration::from_millis(1))
            .unwrap_err(),
        StatementFailure::Deadline
    );
    let requests = server.finish();
    assert_eq!(requests[1].method, "DELETE");
}
#[test]
fn foreign_continuation_uris_are_refused_before_contact() {
    for uri in [
        "http://evil.example:8080/next",
        "https://127.0.0.1:8080/next",
        "$USER_BASE/next",
        "$HTTPS_BASE/next",
        "$PORT_BASE/next",
        "$BASE/next#fragment",
    ] {
        let server = FakeServer::new(vec![Reply::json(&format!(
            r#"{{"id":"q","nextUri":"{uri}"}}"#
        ))]);
        assert!(matches!(
            server.client().submit(&session(), "SELECT 1", deadline()),
            Err(StatementFailure::Protocol(_))
        ));
        server.finish();
    }
}
#[test]
fn column_changes_and_data_width_are_explicit_protocol_failures() {
    for later in [
        r#"{"id":"q","columns":[{"name":"other","type":"bigint","typeSignature":{"rawType":"bigint"}}]}"#,
        r#"{"id":"q","data":[[1,2]]}"#,
    ] {
        let first = Reply::json(
            r#"{"id":"q","columns":[{"name":"v","type":"bigint","typeSignature":{"rawType":"bigint"}}],"nextUri":"$BASE/next"}"#,
        );
        let server = FakeServer::new(vec![first, Reply::json(later)]);
        assert!(matches!(
            server
                .client()
                .submit(&session(), "SELECT 1", deadline())
                .unwrap()
                .run(deadline()),
            Err(StatementFailure::Protocol(_))
        ));
        server.finish();
    }
}
#[test]
fn ieee_floats_are_correctly_rounded_from_original_number_text() {
    for text in [
        "0.09090909090909091",
        "1.4000000000000001",
        "1.2345678901234567",
        "-0.0",
        "4.9406564584124654e-324",
        "1.7976931348623157e308",
    ] {
        let TrinoValue::Double(value) = cell(&TypeSignature::simple("double"), text) else {
            panic!("double")
        };
        assert_eq!(
            value.to_bits(),
            text.parse::<f64>().unwrap().to_bits(),
            "{text}"
        );
    }
    for text in [
        "0.09090909090909091",
        "1.4000000000000001",
        "-0.0",
        "1.401298464324817e-45",
        "3.4028234663852886e38",
    ] {
        let TrinoValue::Real(value) = cell(&TypeSignature::simple("real"), text) else {
            panic!("real")
        };
        assert_eq!(
            value.to_bits(),
            text.parse::<f32>().unwrap().to_bits(),
            "{text}"
        );
    }
    assert!(
        matches!(cell(&TypeSignature::simple("double"),r#""NaN""#),TrinoValue::Double(v) if v.is_nan())
    );
    assert!(
        matches!(cell(&TypeSignature::simple("real"),r#""Infinity""#),TrinoValue::Real(v) if v==f32::INFINITY)
    );
}
#[test]
fn map_decoding_preserves_document_order_and_typed_keys() {
    let ty = sig(
        r#"{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"integer"}},{"kind":"TYPE","value":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}}]}"#,
    );
    assert_eq!(
        cell(&ty, r#"{"2":"y","1":"x"}"#),
        TrinoValue::Map(vec![
            (TrinoValue::Integer(2), TrinoValue::Text("y".into())),
            (TrinoValue::Integer(1), TrinoValue::Text("x".into()))
        ])
    );
}
#[test]
fn arrays_rows_binary_text_and_variant_are_decoded_without_an_untyped_tree() {
    let row = sig(
        r#"{"rawType":"row","arguments":[{"kind":"NAMED_TYPE","value":{"fieldName":{"name":"a"},"typeSignature":{"rawType":"array","arguments":[{"kind":"TYPE","value":{"rawType":"boolean"}}]}}},{"kind":"NAMED_TYPE","value":{"fieldName":null,"typeSignature":{"rawType":"varbinary"}}}]}"#,
    );
    assert_eq!(
        cell(&row, r#"[[true,null,false],"AP8="]"#),
        TrinoValue::Row(vec![
            TrinoValue::Array(vec![
                TrinoValue::Boolean(true),
                TrinoValue::Null,
                TrinoValue::Boolean(false)
            ]),
            TrinoValue::Binary(vec![0, 255])
        ])
    );
    assert_eq!(
        cell(&TypeSignature::simple("variant"), r#"{"z": 1,"a":[2]}"#),
        TrinoValue::Variant(r#"{"z": 1,"a":[2]}"#.into())
    );
    assert_eq!(
        cell(&TypeSignature::simple("varchar"), r#""NULL""#),
        TrinoValue::Text("NULL".into())
    );
    assert_eq!(
        cell(&TypeSignature::simple("unknown"), "null"),
        TrinoValue::Null
    );
}
// Logical depth 64 is a supported NovaRocks result. The independent wire
// text below expands each ARRAY level to three JSON containers and each ROW
// level to four, so neither may depend on Serde's default JSON recursion cap.
fn depth64_pages(named_rows: bool) {
    let mut wire = r#"{"rawType":"integer","arguments":[]}"#.to_owned();
    let mut expected_signature = TypeSignature::simple("integer");
    let mut expected_value = TrinoValue::Integer(7);
    let mut type_name = "integer".to_owned();
    for _ in 0..63 {
        if named_rows {
            wire = format!(
                r#"{{"rawType":"row","arguments":[{{"kind":"NAMED_TYPE","value":{{"fieldName":{{"name":"v"}},"typeSignature":{wire}}}}}]}}"#,
            );
            expected_signature = TypeSignature {
                raw_type: "row".into(),
                arguments: vec![TypeArgument::NamedType(NamedTypeSignature {
                    field_name: Some(RowFieldName { name: "v".into() }),
                    type_signature: expected_signature,
                })],
            };
            expected_value = TrinoValue::Row(vec![expected_value]);
            type_name = format!("row(v {type_name})");
        } else {
            wire =
                format!(r#"{{"rawType":"array","arguments":[{{"kind":"TYPE","value":{wire}}}]}}"#,);
            expected_signature = TypeSignature {
                raw_type: "array".into(),
                arguments: vec![TypeArgument::Type(expected_signature)],
            };
            expected_value = TrinoValue::Array(vec![expected_value]);
            type_name = format!("array({type_name})");
        }
    }
    let value = format!("{}7{}", "[".repeat(63), "]".repeat(63));
    let columns = format!(r#"[{{"name":"deep","type":"{type_name}","typeSignature":{wire}}}]"#,);
    let first = format!(
        r#"{{"id":"q","columns":{columns},"data":[[null],[{value}]],"nextUri":"$BASE/next/deep"}}"#,
    );
    let second = format!(r#"{{"id":"q","columns":{columns},"data":[[{value}],[null]]}}"#);
    let server = FakeServer::new(vec![Reply::json(&first), Reply::json(&second)]);
    let result = server
        .client()
        .execute(&mut session(), "SELECT deep", deadline())
        .unwrap();
    assert_eq!(result.pages, 2);
    assert_eq!(
        result.columns,
        vec![Column {
            name: "deep".into(),
            type_name,
            type_signature: expected_signature,
        }]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![TrinoValue::Null],
            vec![expected_value.clone()],
            vec![expected_value],
            vec![TrinoValue::Null],
        ]
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, "/next/deep");
}
#[test]
fn array64_signatures_and_values_decode_on_initial_and_continuation_pages() {
    depth64_pages(false);
}
#[test]
fn row64_signatures_and_values_decode_on_initial_and_continuation_pages() {
    depth64_pages(true);
}
#[test]
fn signature_arguments_keep_original_tags_order_and_refusals() {
    let arguments: Vec<TypeArgument> = serde_json::from_str(
        r#"[{"value":12,"kind":"LONG"},{"kind":"TYPE","value":{"rawType":"integer"}},{"value":{"typeSignature":{"rawType":"boolean"},"fieldName":null},"kind":"NAMED_TYPE"}]"#,
    ).unwrap();
    assert_eq!(
        arguments,
        vec![
            TypeArgument::Long(12),
            TypeArgument::Type(TypeSignature::simple("integer")),
            TypeArgument::NamedType(NamedTypeSignature {
                field_name: None,
                type_signature: TypeSignature::simple("boolean"),
            }),
        ]
    );
    for wire in [
        r#"{"kind":"OTHER","value":0}"#,
        r#"{"kind":"LONG","value":-1}"#,
        r#"{"kind":"TYPE","value":{"rawType":"integer","rawType":"bigint"}}"#,
        r#"{"kind":"NAMED_TYPE","value":{"fieldName":null}}"#,
    ] {
        assert!(
            serde_json::from_str::<TypeArgument>(wire).is_err(),
            "{wire}"
        );
    }
    let unsupported = sig(r#"{"rawType":"uuid"}"#);
    assert!(unsupported.validate().is_err());
}

#[test]
fn unsupported_signatures_are_rejected_even_for_null_cells() {
    for ty in [
        TypeSignature::simple("uuid"),
        TypeSignature::simple("decimal"),
        TypeSignature::simple("array"),
    ] {
        assert!(decode_value(&ty, RawValue::from_string("null".into()).unwrap().as_ref()).is_err());
    }
}
#[test]
fn http_error_preview_truncates_at_a_character_boundary() {
    let error = StatementFailure::Http {
        status: 400,
        body: "中".repeat(501),
    };
    let text = error.to_string();
    assert_eq!(text.matches('中').count(), 500);
}

#[test]
fn textual_scalar_types_keep_the_exact_wire_content_and_declared_parameters() {
    for signature in [
        sig(
            r#"{"rawType":"decimal","arguments":[{"kind":"LONG","value":38},{"kind":"LONG","value":18}]}"#,
        ),
        sig(r#"{"rawType":"char","arguments":[{"kind":"LONG","value":8}]}"#),
        sig(r#"{"rawType":"timestamp","arguments":[{"kind":"LONG","value":6}]}"#),
        sig(r#"{"rawType":"timestamp with time zone","arguments":[{"kind":"LONG","value":9}]}"#),
        TypeSignature::simple("date"),
        TypeSignature::simple("time"),
        TypeSignature::simple("json"),
    ] {
        assert_eq!(
            cell(&signature, r#""  exact\\text 中  ""#),
            TrinoValue::Text("  exact\\text 中  ".into())
        );
        assert_eq!(cell(&signature, "null"), TrinoValue::Null);
    }
}
#[test]
fn post_error_is_a_structured_query_error_without_starting_a_continuation() {
    let server = FakeServer::new(vec![Reply::json(
        r#"{"id":"q","error":{"message":"rejected","errorCode":2,"errorName":"NOT_SUPPORTED","errorType":"USER_ERROR"}}"#,
    )]);
    assert!(matches!(
        server.client().submit(&session(), "SELECT 1", deadline()),
        Err(StatementFailure::Trino(TrinoError { error_code: 2, .. }))
    ));
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn unsupported_session_header_cancels_the_registered_continuation() {
    let server = FakeServer::new(vec![
        queued().header("X-Trino-Set-Path", "unsupported"),
        Reply {
            status: 204,
            ..Reply::json("")
        },
    ]);
    let mut state = session();
    let original = state.clone();
    let error = server
        .client()
        .execute(&mut state, "SELECT 1", deadline())
        .unwrap_err();
    assert!(matches!(
        error,
        StatementFailure::UnsupportedSessionHeader(_)
    ));
    assert_eq!(state, original);
    let requests = server.finish();
    assert_eq!(requests[1].method, "DELETE");
    assert_eq!(requests[1].path, "/next/0");
}
#[test]
fn local_decode_refusal_cancels_the_latest_valid_uri_without_masking_its_error() {
    let first = Reply::json(
        r#"{"id":"q","columns":[{"name":"v","type":"bigint","typeSignature":{"rawType":"bigint"}}],"nextUri":"$BASE/next/0"}"#,
    );
    let changed = Reply::json(
        r#"{"id":"q","columns":[{"name":"changed","type":"bigint","typeSignature":{"rawType":"bigint"}}],"nextUri":"$BASE/next/1"}"#,
    );
    let server = FakeServer::new(vec![
        first,
        changed,
        Reply {
            status: 500,
            ..Reply::json("cancel failed")
        },
    ]);
    let error = server
        .client()
        .submit(&session(), "SELECT 1", deadline())
        .unwrap()
        .run(deadline())
        .unwrap_err();
    assert!(
        matches!(error,StatementFailure::Protocol(message) if message.contains("columns changed"))
    );
    let requests = server.finish();
    assert_eq!(requests[2].method, "DELETE");
    assert_eq!(requests[2].path, "/next/1");
}

#[test]
fn observed_initial_error_retains_all_structured_error_fields() {
    const ERROR: &str = r#"{"vendorFact":{"z": 1.2300e+9, "kept": true, "a": -0.0},"message":"full original message","errorCode":7,"errorName":"NO_TABLE","errorType":"USER_ERROR","errorLocation":{"lineNumber":2,"columnNumber":3},"sqlState":"42S02","failureInfo":{"type":"Original","message":"full nested detail"}}"#;
    let response = format!(r#"{{"id":"rejected","error":{ERROR}}}"#);
    let server = FakeServer::new(vec![Reply::json(&response)]);
    let mut traces = vec![];
    let outcome =
        server
            .client()
            .submit_observed(&session(), "SELECT missing", deadline(), |trace| {
                traces.push(serde_json::to_value(trace).unwrap());
            });
    let Err(error) = outcome else {
        panic!("expected the original statement error");
    };
    assert_eq!(
        error.to_string(),
        "TrinoError { name=NO_TABLE type=USER_ERROR code=7 }: full original message"
    );
    let StatementFailure::Trino(ref original) = error else {
        panic!("expected the original structured Trino error");
    };
    assert_eq!(serde_json::to_string(original).unwrap(), ERROR);
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0]["query_id"], "rejected");
    assert_eq!(traces[0]["pages"], 1);
    assert_eq!(traces[0]["state"], "failed");
    let error = &traces[0]["failure"]["Trino"];
    assert_eq!(
        error["errorLocation"],
        serde_json::json!({"lineNumber":2,"columnNumber":3})
    );
    assert_eq!(error["sqlState"], "42S02");
    assert_eq!(
        error["failureInfo"],
        serde_json::json!({"type":"Original","message":"full nested detail"})
    );
    assert_eq!(error["vendorFact"]["kept"], true);
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn observed_later_error_keeps_first_uri_page_count_and_update_type() {
    let server = FakeServer::new(vec![
        queued(),
        Reply::json(r#"{"id":"q","updateType":"INSERT","data":[],"nextUri":"$BASE/next/1"}"#),
        Reply::json(
            r#"{"id":"q","error":{"message":"original terminal refusal","errorCode":7,"errorName":"NO_TABLE","errorType":"USER_ERROR"}}"#,
        ),
    ]);
    let mut traces = vec![];
    let pending = server
        .client()
        .submit_observed(
            &session(),
            "INSERT INTO missing SELECT 1",
            deadline(),
            |trace| {
                traces.push(serde_json::to_value(trace).unwrap());
            },
        )
        .unwrap();
    let outcome = pending.run_observed(deadline(), |trace| {
        traces.push(serde_json::to_value(trace).unwrap());
    });
    assert!(matches!(outcome, Err(StatementFailure::Trino(_))));
    assert_eq!(traces.len(), 2);
    assert_eq!(traces[0]["state"], "submitted");
    assert_eq!(traces[1]["state"], "failed");
    assert_eq!(traces[1]["pages"], 3);
    assert_eq!(traces[1]["query_id"], "q");
    assert_eq!(traces[0]["initial_next_uri"], traces[1]["initial_next_uri"]);
    assert!(
        traces[1]["initial_next_uri"]
            .as_str()
            .unwrap()
            .ends_with("/next/0")
    );
    assert_eq!(traces[1]["update_type"], "INSERT");
    assert_eq!(
        traces[1]["failure"]["Trino"]["message"],
        "original terminal refusal"
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec!["POST", "GET", "GET"]
    );
}

#[test]
fn observed_command_finishes_without_a_tabular_result() {
    let server = FakeServer::new(vec![
        queued(),
        Reply::json(r#"{"id":"q","updateType":"SET SESSION"}"#),
    ]);
    let mut traces = vec![];
    let pending = server
        .client()
        .submit_observed(&session(), "SET query_timeout=7", deadline(), |trace| {
            traces.push(serde_json::to_value(trace).unwrap());
        })
        .unwrap();
    let (result, effects) = pending
        .run_observed(deadline(), |trace| {
            traces.push(serde_json::to_value(trace).unwrap());
        })
        .unwrap();
    assert!(!result.has_columns());
    assert_eq!(result.pages, 2);
    assert_eq!(effects, SessionEffects::default());
    assert_eq!(traces.len(), 2);
    assert_eq!(traces[1]["state"], "finished");
    assert_eq!(traces[1]["pages"], 2);
    assert_eq!(traces[1]["update_type"], "SET SESSION");
    assert!(traces[1]["failure"].is_null());
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn observed_local_refusal_keeps_progress_and_original_cancellation() {
    let server = FakeServer::new(vec![
        queued(),
        Reply::json(r#"{"id":"q","updateType":"DELETE","nextUri":"$BASE/next/1"}"#)
            .header("X-Trino-Set-Path", "unsupported"),
        Reply {
            status: 204,
            ..Reply::json("")
        },
    ]);
    let pending = server
        .client()
        .submit(&session(), "DELETE FROM t", deadline())
        .unwrap();
    let mut traces = vec![];
    let outcome = pending.run_observed(deadline(), |trace| {
        traces.push(serde_json::to_value(trace).unwrap());
    });
    assert!(matches!(
        outcome,
        Err(StatementFailure::UnsupportedSessionHeader(_))
    ));
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0]["query_id"], "q");
    assert_eq!(traces[0]["pages"], 2);
    assert_eq!(traces[0]["update_type"], "DELETE");
    assert_eq!(traces[0]["state"], "failed");
    assert!(
        traces[0]["initial_next_uri"]
            .as_str()
            .unwrap()
            .ends_with("/next/0")
    );
    assert!(traces[0]["failure"]["UnsupportedSessionHeader"].is_string());
    let requests = server.finish();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec!["POST", "GET", "DELETE"]
    );
    assert_eq!(requests[2].path, "/next/1");
}

#[test]
fn observed_interrupted_response_retains_identity_without_a_retry() {
    let server = FakeServer::new(vec![
        queued(),
        Reply {
            disconnect: true,
            ..terminal()
        },
    ]);
    let pending = server
        .client()
        .submit(&session(), "SELECT 1", deadline())
        .unwrap();
    let mut traces = vec![];
    let outcome = pending.run_observed(deadline(), |trace| {
        traces.push(serde_json::to_value(trace).unwrap());
    });
    assert!(matches!(outcome, Err(StatementFailure::Transport(_))));
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0]["query_id"], "q");
    assert_eq!(traces[0]["pages"], 1);
    assert_eq!(traces[0]["state"], "failed");
    assert!(traces[0]["failure"]["Transport"].is_string());
    assert!(
        traces[0]["initial_next_uri"]
            .as_str()
            .unwrap()
            .ends_with("/next/0")
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[1].method, "GET");
}

#[test]
fn table_execute_keeps_metric_rows_update_type_and_terminal_ack() {
    let server = FakeServer::new(vec![
        queued(),
        Reply::json(
            r#"{"id":"q","updateType":"ALTER TABLE EXECUTE","columns":[{"name":"metric_name","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}},{"name":"metric_value","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}],"data":[["rewritten_data_files_count",3],["removed_delete_files_count",0],["added_data_files_count",1]],"nextUri":"$BASE/next/1"}"#,
        ),
        Reply::json(r#"{"id":"q","updateType":"ALTER TABLE EXECUTE"}"#),
    ]);
    let (result, _) = server
        .client()
        .submit(&session(), "ALTER TABLE t OPTIMIZE", deadline())
        .unwrap()
        .run(deadline())
        .unwrap();
    assert!(result.is_result_set());
    assert!(!result.is_completion());
    assert_eq!(result.update_type.as_deref(), Some("ALTER TABLE EXECUTE"));
    assert_eq!(result.update_count, None);
    assert_eq!(result.pages, 3);
    assert_eq!(
        result.rows,
        vec![
            vec![
                TrinoValue::Text("rewritten_data_files_count".into()),
                TrinoValue::Integer(3)
            ],
            vec![
                TrinoValue::Text("removed_delete_files_count".into()),
                TrinoValue::Integer(0)
            ],
            vec![
                TrinoValue::Text("added_data_files_count".into()),
                TrinoValue::Integer(1)
            ],
        ]
    );
    let requests = server.finish();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["POST", "GET", "GET"]
    );
    assert_eq!(requests[2].path, "/next/1");
}

#[test]
fn counted_completion_rows_and_trace_retain_zero_through_terminal_ack() {
    for n in [0, 3] {
        let data = format!(
            r#"{{"id":"q","updateType":"INSERT","columns":[{{"name":"rows","type":"bigint","typeSignature":{{"rawType":"bigint","arguments":[]}}}}],"data":[[{n}]],"updateCount":{n},"nextUri":"$BASE/next/2"}}"#
        );
        let end = format!(r#"{{"id":"q","updateType":"INSERT","updateCount":{n}}}"#);
        let server = FakeServer::new(vec![
            queued(),
            Reply::json(r#"{"id":"q","updateType":"INSERT","nextUri":"$BASE/next/1"}"#),
            Reply::json(&data),
            Reply::json(&end),
        ]);
        let mut traces = vec![];
        let pending = server
            .client()
            .submit(&session(), "INSERT INTO t VALUES (1)", deadline())
            .unwrap();
        let (result, _) = pending
            .run_observed(deadline(), |trace| {
                traces.push(serde_json::to_value(trace).unwrap())
            })
            .unwrap();
        assert!(result.is_completion());
        assert!(!result.is_result_set());
        assert_eq!(result.update_count, Some(n));
        assert_eq!(result.rows, vec![vec![TrinoValue::Integer(n as i64)]]);
        assert_eq!(traces[0]["update_count"], n);
        assert_eq!(result.pages, 4);
        let requests = server.finish();
        assert_eq!(
            requests
                .iter()
                .map(|r| r.method.as_str())
                .collect::<Vec<_>>(),
            ["POST", "GET", "GET", "GET"]
        );
        assert_eq!(requests[3].path, "/next/2");
    }
}
#[test]
fn unknown_completion_is_null_and_rows_named_query_remains_result_set() {
    for command in [false, true] {
        let kind = if command {
            r#", "updateType":"ANALYZE""#
        } else {
            ""
        };
        let row = format!(
            r#"{{"id":"q"{kind},"columns":[{{"name":"rows","type":"bigint","typeSignature":{{"rawType":"bigint","arguments":[]}}}}],"data":[[null]],"nextUri":"$BASE/next/1"}}"#
        );
        let end = format!(r#"{{"id":"q"{kind}}}"#);
        let server = FakeServer::new(vec![queued(), Reply::json(&row), Reply::json(&end)]);
        let (result, _) = server
            .client()
            .submit(&session(), "fixture", deadline())
            .unwrap()
            .run(deadline())
            .unwrap();
        assert_eq!(result.update_count, None);
        assert_eq!(result.rows, vec![vec![TrinoValue::Null]]);
        assert_eq!(result.is_completion(), command);
        assert_eq!(result.is_result_set(), !command);
        assert_eq!(server.finish().len(), 3);
    }
}

/// The peer waits for actual connection retirement, never a scheduled sleep.
/// A missing client timeout reaches the socket's finite observation timeout and fails.
fn await_timed_out_request_close(stream: &mut TcpStream) {
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte) {
        Ok(0) => (),
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => (),
        result => {
            panic!("timed out HTTP request did not retire its original connection: {result:?}")
        }
    }
}

#[test]
fn request_timeout_cap_applies_to_actual_post_get_and_delete() {
    for method in ["POST", "GET", "DELETE"] {
        let replies = match method {
            "POST" => vec![Reply::json("")],
            "GET" => vec![
                queued(),
                Reply::json(""),
                Reply {
                    status: 204,
                    ..Reply::json("")
                },
            ],
            "DELETE" => vec![
                queued(),
                Reply {
                    status: 204,
                    ..Reply::json("")
                },
            ],
            _ => unreachable!(),
        };
        let server = FakeServer::with_request_hook(replies, move |request, stream| {
            if request.method == method {
                await_timed_out_request_close(stream);
                true
            } else {
                false
            }
        });
        let client = server.client_with_request_timeout_cap(Duration::from_millis(150));
        let original_deadline = Instant::now() + Duration::from_secs(5);
        let result = match method {
            "POST" => client
                .submit(&session(), "SELECT 1", original_deadline)
                .map(|_| ()),
            "GET" => client
                .submit(&session(), "SELECT 1", original_deadline)
                .unwrap()
                .run(original_deadline)
                .map(|_| ()),
            "DELETE" => {
                let pending = client
                    .submit(&session(), "SELECT 1", original_deadline)
                    .unwrap();
                pending.cancel_handle().delete(original_deadline)
            }
            _ => unreachable!(),
        };
        let before_original_deadline = Instant::now() < original_deadline;
        let requests = server.finish();
        assert!(
            matches!(result, Err(StatementFailure::Deadline)),
            "{method}: {result:?}"
        );
        assert!(
            before_original_deadline,
            "the request cap did not beat the original global deadline"
        );
        let expected: &[&str] = match method {
            "POST" => &["POST"],
            // PendingStatement retains its existing best-effort cancellation owner.
            "GET" => &["POST", "GET", "DELETE"],
            "DELETE" => &["POST", "DELETE"],
            _ => unreachable!(),
        };
        assert_eq!(
            requests
                .iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn original_global_deadline_wins_with_or_without_a_longer_request_cap() {
    for cap in [None, Some(Duration::from_secs(3))] {
        let server = FakeServer::with_request_hook(vec![Reply::json("")], |request, stream| {
            assert_eq!(request.method, "POST");
            await_timed_out_request_close(stream);
            true
        });
        let client = match cap {
            Some(cap) => server.client_with_request_timeout_cap(cap),
            None => server.client(),
        };
        let started = Instant::now();
        let original_deadline = started + Duration::from_millis(100);
        let result = client.submit(&session(), "SELECT 1", original_deadline);
        let before_longer_cap = Instant::now() < started + Duration::from_secs(3);
        let requests = server.finish();
        assert!(matches!(result, Err(StatementFailure::Deadline)));
        assert!(before_longer_cap);
        assert_eq!(requests.len(), 1);
    }
}

#[test]
fn a_zero_request_timeout_cap_is_refused_before_client_construction() {
    let result = TrinoClient::new_with_request_timeout_cap(
        ClientConfig {
            host: "127.0.0.1".into(),
            port: 12345,
            user: "root".into(),
            source: "typed-tests".into(),
        },
        Duration::ZERO,
    );
    assert!(matches!(result, Err(StatementFailure::Protocol(_))));
}
