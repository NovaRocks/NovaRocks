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

fn parse(raw: &str) -> Option<i32> {
    parse_trino_date32_observed::<std::convert::Infallible>(raw, |_| Ok(())).unwrap()
}
#[test]
fn trino_profile_matches_primary_joda_source_chain_values_and_date32_bounds() {
    // Independent expected values from exact-declared Joda2.15.0 source chain,
    // not calculated by this proposed Rust Gregorian implementation.
    for (text, expected) in [
        ("1970-01-01", 0),
        ("1969-12-31", -1),
        ("2000-02-29", 11016),
        ("0000-01-01", -719528),
        ("-1-01-01", -719893),
        ("1-1-1", -719162),
        ("2023-1-2", 19359),
        ("+2023-1-2", 19359),
        ("-0-1-1", -719528),
        ("000002023-1-2", 19359),
        ("-5877641-06-23", i32::MIN),
        ("+5881580-07-11", i32::MAX),
    ] {
        assert_eq!(parse(text), Some(expected), "{text}");
    }
    for text in [
        "",
        " ",
        "1900-02-29",
        "0000002023-1-2",
        "20230102",
        "2023-01-02T00:00:00",
        "2023-01-02Z",
        "2023-01-02+00:00",
        "2023-01-02 x",
        "2023-+1-2",
        "2023-001-2",
        "2023-1-002",
        "999999999-1-1",
        "-999999999-1-1",
        "2023-13-1",
        "2023-2-29",
        "-5877641-06-22",
        "+5881580-07-12",
        "--2023-1-2",
        "+-2023-1-2",
        "2023-1-+2",
        "２０２３-01-02",
        "2023/01/02",
    ] {
        assert_eq!(parse(text), None, "{text:?}");
    }
}
#[test]
fn trino_date_whitespace_is_java_character_whitespace_and_no_input_quota() {
    for ch in [
        '\t', '\n', '\u{000b}', '\u{000c}', '\r', '\u{001c}', '\u{001d}', '\u{001e}', '\u{001f}',
        ' ', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}',
        '\u{2006}', '\u{2008}', '\u{2009}', '\u{200a}', '\u{2028}', '\u{2029}', '\u{205f}',
        '\u{3000}',
    ] {
        assert_eq!(
            parse(&format!("{ch}1970-01-01{ch}")),
            Some(0),
            "U+{:04X}",
            ch as u32
        );
    }
    for ch in [
        '\0', '\u{0085}', '\u{00a0}', '\u{180e}', '\u{2007}', '\u{202f}', '\u{feff}',
    ] {
        assert_eq!(
            parse(&format!("{ch}1970-01-01")),
            None,
            "U+{:04X}",
            ch as u32
        );
    }
    let raw = format!("{}1970-01-01{}", " ".repeat(20000), " ".repeat(20000));
    let mut steps = 0;
    assert_eq!(
        parse_trino_date32_observed::<std::convert::Infallible>(&raw, |_| {
            steps += 1;
            Ok(())
        })
        .unwrap(),
        Some(0)
    );
    assert!(steps >= 40000);
}
#[test]
fn legacy_date_profile_keeps_its_original_compact_input_and_chrono_range() {
    assert!(parse_date("20230102").is_some());
    assert_eq!(parse("20230102"), None);
    assert_eq!(parse_date("+5881580-07-11"), None);
    assert_eq!(parse("+5881580-07-11"), Some(i32::MAX));
}
#[test]
fn trino_date_observation_refusal_is_original_and_stops_without_a_tail() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct OriginalRefusal;
    for raw in [
        format!("{}2023-1-2{}", " ".repeat(128), " ".repeat(128)),
        "+5881580-07-11".into(),
        "2023-02-29".into(),
    ] {
        let mut count = 0;
        let _ = parse_trino_date32_observed::<OriginalRefusal>(&raw, |_| {
            count += 1;
            Ok(())
        });
        for stop in 0..count {
            let mut calls = 0;
            let result = parse_trino_date32_observed(&raw, |_| {
                assert!(calls <= stop, "callback after original refusal");
                let at = calls;
                calls += 1;
                if at == stop {
                    Err(OriginalRefusal)
                } else {
                    Ok(())
                }
            });
            assert_eq!(result, Err(OriginalRefusal));
            assert_eq!(calls, stop + 1);
        }
    }
}

#[test]
fn date32_components_match_independent_localdate_wire_boundaries_epoch_and_bce() {
    for (days, expected) in [
        (i32::MIN, (-5877641, 6, 23)),
        (i32::MIN + 1, (-5877641, 6, 24)),
        (i32::MAX - 1, (5881580, 7, 10)),
        (i32::MAX, (5881580, 7, 11)),
        (0, (1970, 1, 1)),
        (-719893, (-1, 1, 1)),
        (-719528, (0, 1, 1)),
        (-719162, (1, 1, 1)),
        (11016, (2000, 2, 29)),
    ] {
        let actual = date32_to_gregorian(days);
        assert_eq!(actual, expected);
        let text = format!("{}-{:02}-{:02}", actual.0, actual.1, actual.2);
        assert_eq!(parse(&text), Some(days));
    }
}
