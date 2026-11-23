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

use super::search::{RegexpMatch, RegexpSearch};
use crate::{KernelFailure, kernel_input::EvaluationCheckpoints};

#[derive(Debug)]
pub(crate) enum RegexpValueFailure {
    Data(&'static str),
    Kernel(KernelFailure),
}

impl From<KernelFailure> for RegexpValueFailure {
    fn from(error: KernelFailure) -> Self {
        Self::Kernel(error)
    }
}

impl RegexpSearch<'_, '_> {
    pub(crate) fn count(
        &mut self,
        source: &str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<i64, KernelFailure> {
        let mut count = 0i64;
        let mut cursor = Some(0);
        while let Some(start) = cursor {
            let Some(found) = self.find(source, start, work)? else {
                break;
            };
            count = count
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
            cursor = found.next_start(source);
        }
        Ok(count)
    }

    pub(crate) fn position(
        &mut self,
        source: &str,
        start: i32,
        occurrence: i32,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<i32, RegexpValueFailure> {
        if start < 1 {
            return Err(RegexpValueFailure::Data(
                "start position cannot be smaller than 1",
            ));
        }
        if occurrence < 1 {
            return Err(RegexpValueFailure::Data(
                "occurrence cannot be smaller than 1",
            ));
        }
        let Some(start) = offset_at(source, start as usize - 1, work)? else {
            return Ok(-1);
        };
        let mut cursor = Some(start);
        for index in 1..=occurrence {
            let Some(at) = cursor else { break };
            let Some(found) = self.find(source, at, work)? else {
                break;
            };
            if index == occurrence {
                let mut position = 1usize;
                for _ in source[..found.span().start].chars() {
                    work.step()?;
                    position += 1;
                }
                return i32::try_from(position).map_err(|_| {
                    RegexpValueFailure::Data("regexp position is outside the integer range")
                });
            }
            cursor = found.next_start(source);
        }
        Ok(-1)
    }

    fn checked_group(&self, group: i64) -> Result<usize, RegexpValueFailure> {
        if group < 0 {
            return Err(RegexpValueFailure::Data("capture group cannot be negative"));
        }
        usize::try_from(group)
            .ok()
            .filter(|group| *group < self.program.names.len())
            .ok_or(RegexpValueFailure::Data("capture group does not exist"))
    }

    pub(crate) fn extract<'s>(
        &mut self,
        source: &'s str,
        group: i64,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<&'s str>, RegexpValueFailure> {
        let group = self.checked_group(group)?;
        let found = self.find(source, 0, work)?;
        Ok(found.and_then(|found| found.capture(group).map(|span| &source[span])))
    }

    /// Emit one result for each match, including an absent optional group.
    pub(crate) fn extract_all<'s>(
        &mut self,
        source: &'s str,
        group: i64,
        work: &mut EvaluationCheckpoints<'_>,
        mut emit: impl FnMut(
            Option<&'s str>,
            &mut EvaluationCheckpoints<'_>,
        ) -> Result<(), KernelFailure>,
    ) -> Result<(), RegexpValueFailure> {
        let group = self.checked_group(group)?;
        let mut cursor = Some(0);
        while let Some(start) = cursor {
            let Some(found) = self.find(source, start, work)? else {
                break;
            };
            emit(found.capture(group).map(|span| &source[span]), work)?;
            cursor = found.next_start(source);
        }
        Ok(())
    }

    /// The output owner admits and observes copies through `emit`. It must
    /// discard this row's staged output if a replacement reference fails.
    pub(crate) fn replace(
        &mut self,
        source: &str,
        replacement: &str,
        work: &mut EvaluationCheckpoints<'_>,
        mut emit: impl FnMut(&[u8], &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
    ) -> Result<(), RegexpValueFailure> {
        let mut cursor = Some(0);
        let mut end = 0;
        while let Some(start) = cursor {
            let Some(found) = self.find(source, start, work)? else {
                break;
            };
            let span = found.span();
            emit(&source.as_bytes()[end..span.start], work)?;
            found.replace_match(source.as_bytes(), replacement.as_bytes(), work, &mut emit)?;
            end = span.end;
            cursor = found.next_start(source);
        }
        emit(&source.as_bytes()[end..], work)?;
        Ok(())
    }
}

fn offset_at(
    source: &str,
    position: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    for (index, (offset, _)) in source.char_indices().enumerate() {
        work.step()?;
        if index == position {
            return Ok(Some(offset));
        }
    }
    Ok(None)
}

impl RegexpMatch<'_, '_> {
    fn replace_match(
        &self,
        source: &[u8],
        replacement: &[u8],
        work: &mut EvaluationCheckpoints<'_>,
        emit: &mut impl FnMut(&[u8], &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
    ) -> Result<(), RegexpValueFailure> {
        let mut cursor = 0;
        while let Some(byte) = replacement.get(cursor).copied() {
            work.step()?;
            cursor += 1;
            match byte {
                b'\\' => {
                    let next = replacement.get(cursor).ok_or(RegexpValueFailure::Data(
                        "trailing escape in regexp replacement",
                    ))?;
                    cursor += 1;
                    emit(std::slice::from_ref(next), work)?;
                }
                b'$' => {
                    let (group, end) = self.replacement_group(replacement, cursor, work)?;
                    cursor = end;
                    if let Some(span) = self.capture(group) {
                        emit(&source[span], work)?;
                    }
                }
                _ => emit(std::slice::from_ref(&byte), work)?,
            }
        }
        Ok(())
    }

    fn replacement_group(
        &self,
        replacement: &[u8],
        at: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(usize, usize), RegexpValueFailure> {
        let group_len = self.names.len();
        let mut cursor = at;
        if replacement.get(cursor) == Some(&b'{') {
            cursor += 1;
            let start = cursor;
            while let Some(byte) = replacement.get(cursor) {
                work.step()?;
                if *byte == b'}' {
                    break;
                }
                cursor += 1;
            }
            // Joni looks up even a name that runs to the end of the template.
            let name = &replacement[start..cursor];
            let end = (cursor + 1).min(replacement.len());
            for (group, candidate) in self.names.iter().enumerate().skip(1) {
                work.step()?;
                if let Some(candidate) = candidate
                    && equal_name(candidate.as_bytes(), name, work)?
                {
                    return Ok((group, end));
                }
            }
            return Err(RegexpValueFailure::Data(
                "unknown named group in regexp replacement",
            ));
        }

        let Some(mut group) = digit(replacement.get(cursor)) else {
            return Err(RegexpValueFailure::Data(
                "invalid capture reference in regexp replacement",
            ));
        };
        if group >= group_len {
            return Err(RegexpValueFailure::Data(
                "unknown group in regexp replacement",
            ));
        }
        cursor += 1;
        while let Some(digit) = digit(replacement.get(cursor)) {
            work.step()?;
            let Some(extended) = group
                .checked_mul(10)
                .and_then(|prefix| prefix.checked_add(digit))
            else {
                break;
            };
            if extended >= group_len {
                break;
            }
            group = extended;
            cursor += 1;
        }
        Ok((group, cursor))
    }
}

fn digit(byte: Option<&u8>) -> Option<usize> {
    let byte = *byte?;
    byte.is_ascii_digit().then(|| usize::from(byte - b'0'))
}

fn equal_name(
    candidate: &[u8],
    name: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    if candidate.len() != name.len() {
        return Ok(false);
    }
    for (left, right) in candidate.iter().zip(name) {
        work.step()?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
