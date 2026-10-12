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
//! Freeze complete original renderer observations before sharing it with JSON_OBJECT.
use super::Selection;
use super::parse_json_core::{ParseJsonObserver, evaluate_selected};
use arrow_array::{Array, StringArray};
use serde_json::json;

struct Trace {
    events: String,
    stop: Option<usize>,
}
impl ParseJsonObserver for Trace {
    type Error = &'static str;
    fn step(&mut self) -> Result<(), Self::Error> {
        self.event('s')
    }
    fn boundary(&mut self) -> Result<(), Self::Error> {
        self.event('b')
    }
}
impl Trace {
    fn event(&mut self, event: char) -> Result<(), &'static str> {
        self.events.push(event);
        if self.stop == Some(self.events.len()) {
            Err("original observer interruption")
        } else {
            Ok(())
        }
    }
}
#[test]
fn parse_json_renderer_trace_original_complete_values_events_and_every_interruption() {
    let input = StringArray::from(vec![
        Some("{\"z\":[1,true,null,\"é中\\n\"],\"a\":{\"b\":2,\"a\":false}}"),
        None,
        Some("ordinary text"),
        Some("null"),
        Some("0.50"),
        Some("[\"\\t\",{\"z\":3,\"a\":1}]"),
    ]);
    let mut records = Vec::new();
    for rows in [&[0, 1, 2, 3, 4, 5][..], &[0, 2, 5][..], &[][..]] {
        let selection = Selection::try_sparse(input.len(), rows).unwrap();
        let mut trace = Trace {
            events: String::new(),
            stop: None,
        };
        let output = evaluate_selected(&input, selection, |_, row| row, &mut trace).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        let values = (0..output.len())
            .map(|row| (!output.is_null(row)).then(|| output.value(row)))
            .collect::<Vec<_>>();
        for stop in 1..=trace.events.len() {
            let mut interrupted = Trace {
                events: String::new(),
                stop: Some(stop),
            };
            let error =
                evaluate_selected(&input, selection, |_, row| row, &mut interrupted).unwrap_err();
            assert_eq!(error, "original observer interruption");
            assert_eq!(interrupted.events, trace.events[..stop]);
        }
        records.push(json!({"selection":rows,"values":values,"complete_events":trace.events}));
    }
    assert_eq!(
        records[0]["values"],
        json!([
            "{\"a\": {\"a\": false, \"b\": 2}, \"z\": [1, true, null, \"é中\\n\"]}",
            null,
            null,
            "null",
            "0.5",
            "[\"\\t\", {\"a\": 1, \"z\": 3}]"
        ])
    );
    assert_eq!(
        json!(records),
        serde_json::from_str::<serde_json::Value>(ORIGINAL_RENDERER_TRACE).unwrap()
    );
    if let Ok(path) = std::env::var("C16_ORIGINAL_PARSE_TRACE_OUT") {
        std::fs::write(path, serde_json::to_string_pretty(&records).unwrap() + "\n").unwrap();
    }
}

// Complete original output and callback order, recorded before extraction.
const ORIGINAL_RENDERER_TRACE: &str = r###"[{"complete_events":"bbsbbsbbbbbsbbbbbbbbsbbbbbsbbbbbbbbsbbsbbbbbbbbbbsbbbbbbsbbbbbbbbbbsbbssbbbbsbbsbbsbbsbbsbbsbbbbbbbbbbsbbsbbbsbbsbbbbsbbsbbbbbbsbbsbbssbbbbsbbsbbbbbsbbbbbbbbsbbbbsbbbbbbbbbbsbbbbbbbbbbbb","selection":[0,1,2,3,4,5],"values":["{\"a\": {\"a\": false, \"b\": 2}, \"z\": [1, true, null, \"é中\\n\"]}",null,null,"null","0.5","[\"\\t\", {\"a\": 1, \"z\": 3}]"]},{"complete_events":"bbsbbsbbbbbsbbbbbbbbsbbbbbsbbbbbbbbsbbsbbbbbbbbbbsbbbbbbsbbbbbbbbbbsbbssbbbbsbbsbbsbbsbbsbbsbbbbbbbbbbsbbbsbbsbbssbbbbsbbsbbbbbsbbbbbbbbsbbbbsbbbbbbbbbbsbbbbbbbbbbbb","selection":[0,2,5],"values":["{\"a\": {\"a\": false, \"b\": 2}, \"z\": [1, true, null, \"é中\\n\"]}",null,"[\"\\t\", {\"a\": 1, \"z\": 3}]"]},{"complete_events":"bbbb","selection":[],"values":[]}]"###;
