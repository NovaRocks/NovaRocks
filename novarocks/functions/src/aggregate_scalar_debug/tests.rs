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

use super::*;
use arrow_buffer::i256;

#[derive(Debug, Eq, PartialEq)]
enum Event {
    Step,
    Boundary,
    Digits(FixedDigitsEvent),
}
struct Stop(usize);
struct Observer {
    stop: Option<usize>,
    events: Vec<Event>,
}
impl Observer {
    fn observe(&mut self, event: Event) -> Result<(), Stop> {
        let ordinal = self.events.len();
        self.events.push(event);
        if self.stop == Some(ordinal) {
            Err(Stop(ordinal))
        } else {
            Ok(())
        }
    }
}
impl ScalarDebugObserver for Observer {
    type Error = Stop;
    fn step(&mut self) -> Result<(), Stop> {
        self.observe(Event::Step)
    }
    fn boundary(&mut self) -> Result<(), Stop> {
        self.observe(Event::Boundary)
    }
    fn fixed_digits_event(&mut self, event: FixedDigitsEvent) -> Result<(), Stop> {
        self.observe(Event::Digits(event))
    }
}
fn fixture() -> AggScalarValue {
    use AggScalarValue::*;
    List(vec![
        Some(Bool(true)),
        None,
        Some(Map(vec![(
            Some(Decimal256(i256::MIN)),
            Some(Struct(vec![
                Some(Utf8("é\0\n\"\\".into())),
                Some(Binary(vec![0, 128, 255])),
                None,
            ])),
        )])),
        Some(Int64(i64::MIN)),
    ])
}

#[test]
fn c16_p1_scalar_debug_owned_every_callback_preserves_first_cause_and_stops() {
    let value = fixture();
    let mut original = Observer {
        stop: None,
        events: Vec::new(),
    };
    let mut complete = String::new();
    assert!(
        write_observed(
            ScalarDebugRoot::Scalar(&value),
            &mut complete,
            &mut original
        )
        .is_ok()
    );
    assert_eq!(complete, format!("{value:?}"));
    assert!(
        original
            .events
            .iter()
            .filter(|e| matches!(e, Event::Digits(_)))
            .count()
            <= 701
    );
    for stop in 0..original.events.len() {
        let mut observer = Observer {
            stop: Some(stop),
            events: Vec::new(),
        };
        let mut out = String::new();
        let result = write_observed(ScalarDebugRoot::Scalar(&value), &mut out, &mut observer);
        assert!(matches!(result, Err(ScalarDebugFailure::Control(Stop(at))) if at == stop));
        assert_eq!(observer.events, original.events[..=stop]);
        assert!(complete.starts_with(&out));
    }
}

struct FaultWriter {
    limit: usize,
    written: usize,
    failed: bool,
    after_failure: usize,
}
impl Write for FaultWriter {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.failed {
            self.after_failure += 1;
            return Err(fmt::Error);
        }
        if text.len() > self.limit - self.written {
            self.failed = true;
            Err(fmt::Error)
        } else {
            self.written += text.len();
            Ok(())
        }
    }
}
#[test]
fn c16_p1_scalar_debug_owned_writer_fault_has_no_next_write_or_footer() {
    let value = fixture();
    let full = format!("{value:?}");
    for limit in [0, 1, 4, 20, full.len() / 2, full.len() - 1] {
        let mut observer = Observer {
            stop: None,
            events: Vec::new(),
        };
        let mut writer = FaultWriter {
            limit,
            written: 0,
            failed: false,
            after_failure: 0,
        };
        let result = write_observed(ScalarDebugRoot::Scalar(&value), &mut writer, &mut observer);
        assert!(matches!(result, Err(ScalarDebugFailure::Formatting)));
        assert!(writer.failed);
        assert_eq!(writer.after_failure, 0);
    }
}

#[test]
fn c16_p1_scalar_debug_owned_inner_roots_match_original_vectors_without_enum_label() {
    use AggScalarValue::*;
    let values = vec![None, Some(Decimal256(i256::MAX)), Some(fixture())];
    let pairs = vec![
        (None, Some(Decimal256(i256::ZERO))),
        (Some(fixture()), None),
    ];
    for (root, expected) in [
        (ScalarDebugRoot::Sequence(&values), format!("{values:?}")),
        (ScalarDebugRoot::Pairs(&pairs), format!("{pairs:?}")),
    ] {
        let mut out = String::new();
        let mut observer = Observer {
            stop: None,
            events: Vec::new(),
        };
        assert!(write_observed(root, &mut out, &mut observer).is_ok());
        assert_eq!(out, expected);
    }
}
