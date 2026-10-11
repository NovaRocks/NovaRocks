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

//! Borrowed UTF-8 row measurement and ordered emission shared by both evaluators.
//! Readers, observation and output storage remain owned by each caller.

const MAX_ROW_BYTES: usize = 1_048_576;

/// The original observation after a measurement branch or an emitted part.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConcatObservation {
    SeparatorMeasured,
    ValueMeasured,
    PartEnd,
}

/// Measure CONCAT, stopping row reads at the first NULL or exceeded byte cap.
pub fn concat_length_observed<'s, C, E>(
    arguments: usize,
    context: &mut C,
    mut read: impl FnMut(&mut C, usize) -> Result<Option<&'s str>, E>,
) -> Result<Option<usize>, E> {
    let mut length = 0;
    for index in 0..arguments {
        let Some(text) = read(context, index)? else {
            return Ok(None);
        };
        if text.len() > MAX_ROW_BYTES - length {
            return Ok(None);
        }
        length += text.len();
    }
    Ok(Some(length))
}

/// Emit the already measured, successful CONCAT row without another probe.
pub fn emit_concat_observed<'s, C, E>(
    arguments: usize,
    context: &mut C,
    mut read: impl FnMut(&mut C, usize) -> Result<&'s str, E>,
    mut emit: impl FnMut(&mut C, &'s str) -> Result<(), E>,
    mut observe: impl FnMut(&mut C, ConcatObservation) -> Result<(), E>,
) -> Result<(), E> {
    for index in 0..arguments {
        let text = read(context, index)?;
        emit(context, text)?;
        observe(context, ConcatObservation::PartEnd)?;
    }
    Ok(())
}

/// Measure CONCAT_WS, retaining the caller's masked-tail access contract.
pub fn concat_ws_length_observed<'s, C, E>(
    arguments: usize,
    context: &mut C,
    mut read: impl FnMut(&mut C, usize) -> Result<Option<&'s str>, E>,
    mut visit_masked: impl FnMut(&mut C, usize) -> Result<(), E>,
    mut observe: impl FnMut(&mut C, ConcatObservation) -> Result<(), E>,
) -> Result<Option<usize>, E> {
    let separator = read(context, 0)?;
    let mut length = 0;
    let mut has_value = false;
    let mut oversize = false;
    for index in 1..arguments {
        if separator.is_none() || oversize {
            visit_masked(context, index)?;
        } else if let Some(value) = read(context, index)?
            && let Some(separator) = separator
        {
            if has_value {
                if separator.len() > MAX_ROW_BYTES - length {
                    oversize = true;
                } else {
                    length += separator.len();
                }
                observe(context, ConcatObservation::SeparatorMeasured)?;
            }
            if !oversize {
                if value.len() > MAX_ROW_BYTES - length {
                    oversize = true;
                } else {
                    length += value.len();
                }
                has_value = true;
                observe(context, ConcatObservation::ValueMeasured)?;
            }
        }
        observe(context, ConcatObservation::PartEnd)?;
    }
    Ok((separator.is_some() && !oversize).then_some(length))
}

/// Emit a successful CONCAT_WS row, including separators between empty parts.
pub fn emit_concat_ws_observed<'s, C, E>(
    arguments: usize,
    context: &mut C,
    mut separator: impl FnMut(&mut C) -> Result<&'s str, E>,
    mut read: impl FnMut(&mut C, usize) -> Result<Option<&'s str>, E>,
    mut emit: impl FnMut(&mut C, &'s str) -> Result<(), E>,
    mut observe: impl FnMut(&mut C, ConcatObservation) -> Result<(), E>,
) -> Result<(), E> {
    let separator = separator(context)?;
    let mut has_value = false;
    for index in 1..arguments {
        if let Some(value) = read(context, index)? {
            if has_value {
                emit(context, separator)?;
            }
            emit(context, value)?;
            has_value = true;
        }
        observe(context, ConcatObservation::PartEnd)?;
    }
    Ok(())
}

#[cfg(test)]
mod string_concat_core_tests {
    use super::*;
    use std::convert::Infallible;
    fn never<T>(result: Result<T, Infallible>) -> T {
        match result {
            Ok(value) => value,
            Err(never) => match never {},
        }
    }
    #[test]
    fn string_concat_core_fixed_null_bytes_and_order_oracle() {
        for (values, expected) in [
            (vec![Some("a"), Some(""), Some("é\0")], Some("aé\0")),
            (vec![Some("a"), None, Some("unread")], None),
            (vec![], Some("")),
        ] {
            let length = never(concat_length_observed(values.len(), &mut (), |_, i| {
                Ok(values[i])
            }));
            assert_eq!(length, expected.map(str::len));
            if let Some(expected) = expected {
                let mut out = String::new();
                never(emit_concat_observed(
                    values.len(),
                    &mut out,
                    |_, i| Ok(values[i].unwrap()),
                    |out, text| {
                        out.push_str(text);
                        Ok(())
                    },
                    |_, _| Ok(()),
                ));
                assert_eq!(out, expected);
            }
        }
        for (values, expected) in [
            (vec![Some(","), None, Some(""), Some("x")], Some(",x")),
            (vec![Some("|"), None, None], Some("")),
            (vec![None, Some("unread")], None),
            (
                vec![Some("🧪"), Some("é\0"), Some("你好")],
                Some("é\0🧪你好"),
            ),
        ] {
            let length = never(concat_ws_length_observed(
                values.len(),
                &mut (),
                |_, i| Ok(values[i]),
                |_, _| Ok(()),
                |_, _| Ok(()),
            ));
            assert_eq!(length, expected.map(str::len));
            if let Some(expected) = expected {
                let mut out = String::new();
                never(emit_concat_ws_observed(
                    values.len(),
                    &mut out,
                    |_| Ok(values[0].unwrap()),
                    |_, i| Ok(values[i]),
                    |out, text| {
                        out.push_str(text);
                        Ok(())
                    },
                    |_, _| Ok(()),
                ));
                assert_eq!(out, expected);
            }
        }
    }
    #[test]
    fn string_concat_core_masked_visit_refusal_and_byte_cap_are_exact() {
        let oversized = "x".repeat(1_048_577);
        let exact = "x".repeat(1_048_576);
        assert_eq!(
            never(concat_length_observed(1, &mut (), |_, _| Ok(Some(
                exact.as_str()
            )))),
            Some(1_048_576)
        );
        assert_eq!(
            never(concat_length_observed(2, &mut (), |_, i| if i == 0 {
                Ok(Some(oversized.as_str()))
            } else {
                panic!("read after exceeded cap")
            })),
            None
        );
        assert_eq!(
            never(concat_ws_length_observed(
                3,
                &mut (),
                |_, i| Ok(match i {
                    0 => Some(oversized.as_str()),
                    1 => Some("solo"),
                    _ => None,
                }),
                |_, _| Ok(()),
                |_, _| Ok(())
            )),
            Some(4)
        );
        let mut visits = vec![];
        let refused = concat_ws_length_observed(
            4,
            &mut visits,
            |visits, i| {
                visits.push(i);
                if i == 0 {
                    Ok(None)
                } else {
                    panic!("required read after NULL separator")
                }
            },
            |visits, i| {
                visits.push(i);
                if i == 2 {
                    Err("masked refusal")
                } else {
                    Ok(())
                }
            },
            |_, _| Ok(()),
        );
        assert_eq!(refused, Err("masked refusal"));
        assert_eq!(visits, [0, 1, 2]);
        let mut pieces = vec![];
        let refused = emit_concat_ws_observed(
            4,
            &mut pieces,
            |_| Ok("-"),
            |_, _| Ok(Some("x")),
            |pieces, text| {
                pieces.push(text);
                if text == "-" {
                    Err("sink refusal")
                } else {
                    Ok(())
                }
            },
            |_, _| Ok(()),
        );
        assert_eq!(refused, Err("sink refusal"));
        assert_eq!(pieces, ["x", "-"]);
    }
}
