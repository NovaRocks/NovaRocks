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

//! Unicode whitespace boundaries shared by the legacy and selected adapters.
//! The returned span borrows the input; the observer owns refusal and control.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StringTrimOp {
    Left,
    Right,
    Both,
}

/// Return the maximal span retained by the selected whitespace operation.
/// Observe each original character after its predicate and before its cursor update.
pub fn trimmed_observed<E>(
    op: StringTrimOp,
    text: &str,
    mut visit: impl FnMut() -> Result<(), E>,
) -> Result<&str, E> {
    let mut start = 0usize;
    let mut end = text.len();
    // str::trim{,_start,_end} uses this same char::is_whitespace predicate.
    // Each visited UTF-8 scalar consumes at most four bytes; source boundaries
    // come from the original iterator, with no temporary text or side index.
    if matches!(op, StringTrimOp::Left | StringTrimOp::Both) {
        for ch in text.chars() {
            let whitespace = ch.is_whitespace();
            visit()?;
            if !whitespace {
                break;
            }
            start += ch.len_utf8();
        }
    }
    if matches!(op, StringTrimOp::Right | StringTrimOp::Both) {
        for ch in text[start..].chars().rev() {
            let whitespace = ch.is_whitespace();
            visit()?;
            if !whitespace {
                break;
            }
            end -= ch.len_utf8();
        }
    }
    Ok(&text[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[test]
    fn string_trim_core_std_fixed_bytes_and_valid_borrowed_boundaries() {
        let white = "\t\n\u{b}\u{c}\r \u{85}\u{a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}";
        let text = [
            String::new(),
            white.into(),
            format!("{white}x{white}"),
            "\0".into(),
            " \0 ".into(),
            "\u{200b}\u{180e}\u{feff}\u{1c}".into(),
            "e\u{301}👩‍💻".into(),
            format!("{}x{}", "\u{2003}".repeat(1025), "\u{3000}".repeat(1025)),
        ];
        for s in &text {
            for op in [StringTrimOp::Both, StringTrimOp::Left, StringTrimOp::Right] {
                let expected = match op {
                    StringTrimOp::Both => s.trim(),
                    StringTrimOp::Left => s.trim_start(),
                    StringTrimOp::Right => s.trim_end(),
                };
                let actual = trimmed_observed(op, s, || Ok::<(), Infallible>(())).unwrap();
                assert_eq!(actual.as_bytes(), expected.as_bytes());
                let offset = (actual.as_ptr() as usize)
                    .checked_sub(s.as_ptr() as usize)
                    .unwrap();
                assert!(offset <= s.len() && offset + actual.len() <= s.len());
                assert!(s.is_char_boundary(offset) && s.is_char_boundary(offset + actual.len()));
                assert_eq!(&s[offset..offset + actual.len()], actual);
            }
        }
        for (op, expected) in [
            (StringTrimOp::Both, "x"),
            (StringTrimOp::Left, "x\u{a0}"),
            (StringTrimOp::Right, "\u{2003}x"),
        ] {
            assert_eq!(
                trimmed_observed(op, "\u{2003}x\u{a0}", || Ok::<(), Infallible>(())).unwrap(),
                expected
            );
        }
    }

    #[derive(Debug, PartialEq)]
    struct Refusal {
        at: usize,
    }
    #[test]
    fn string_trim_core_original_visits_and_generic_first_refusal() {
        for (s, op, count) in [
            ("", StringTrimOp::Both, 0),
            ("x", StringTrimOp::Both, 2),
            ("x", StringTrimOp::Left, 1),
            ("x", StringTrimOp::Right, 1),
            ("\u{2003}\u{3000}", StringTrimOp::Both, 2),
            (" x ", StringTrimOp::Both, 4),
        ] {
            let mut visited = 0;
            trimmed_observed(op, s, || {
                visited += 1;
                Ok::<(), Infallible>(())
            })
            .unwrap();
            assert_eq!(visited, count);
        }
        let text = format!("{}x{}", "\u{2003}".repeat(1025), "\u{3000}".repeat(1025));
        for op in [StringTrimOp::Both, StringTrimOp::Left, StringTrimOp::Right] {
            let mut count = 0;
            trimmed_observed(op, &text, || {
                count += 1;
                Ok::<(), Infallible>(())
            })
            .unwrap();
            for stop in 0..count {
                let mut trace = Vec::new();
                let result = trimmed_observed(op, &text, || {
                    let at = trace.len();
                    assert!(at <= stop, "observer called after refusal");
                    trace.push(at);
                    if at == stop {
                        Err(Refusal { at })
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(Refusal { at: stop }));
                assert_eq!(trace, (0..=stop).collect::<Vec<_>>());
            }
        }
    }
}
