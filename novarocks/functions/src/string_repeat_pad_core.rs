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
//! ONE original REPEAT/SPACE/PAD row program with explicit storage projections.
//! Original projection keeps its infallible String/Vec policies. Selected
//! projection owns only borrowed characters and its existing compact output.
use std::convert::Infallible;
pub const MAX_STRING_BYTES: usize = 1_048_576;
#[derive(Clone, Copy, Debug)]
pub enum RepeatPlan<'a> {
    Null,
    Empty,
    Repeated {
        text: &'a str,
        count: usize,
        bytes: usize,
    },
}
impl RepeatPlan<'_> {
    pub fn bytes(self) -> Option<usize> {
        match self {
            Self::Null => None,
            Self::Empty => Some(0),
            Self::Repeated { bytes, .. } => Some(bytes),
        }
    }
}
pub fn repeat_plan<'a>(text: impl FnOnce() -> &'a str, n: i64) -> RepeatPlan<'a> {
    if n < 0 {
        return RepeatPlan::Empty;
    }
    let s = text();
    if s.is_empty() || n == 0 {
        return RepeatPlan::Empty;
    }
    let n_u128 = n as u128;
    let bytes_u128 = s.len() as u128;
    if bytes_u128.saturating_mul(n_u128) > MAX_STRING_BYTES as u128 {
        return RepeatPlan::Null;
    }
    RepeatPlan::Repeated {
        text: s,
        count: n as usize,
        bytes: s.len() * (n as usize),
    }
}
pub trait RepeatWriter {
    type Error;
    type Output;
    fn empty(&mut self) -> Result<Self::Output, Self::Error>;
    fn repeat(
        &mut self,
        text: &str,
        count: usize,
        bytes: usize,
    ) -> Result<Self::Output, Self::Error>;
}
pub fn render_repeat<W: RepeatWriter>(
    plan: RepeatPlan<'_>,
    writer: &mut W,
) -> Result<Option<W::Output>, W::Error> {
    match plan {
        RepeatPlan::Null => Ok(None),
        RepeatPlan::Empty => writer.empty().map(Some),
        RepeatPlan::Repeated { text, count, bytes } => writer.repeat(text, count, bytes).map(Some),
    }
}
struct OriginalRepeatWriter;
impl RepeatWriter for OriginalRepeatWriter {
    type Error = Infallible;
    type Output = String;
    fn empty(&mut self) -> Result<String, Infallible> {
        Ok(String::new())
    }
    fn repeat(&mut self, s: &str, n: usize, _bytes: usize) -> Result<String, Infallible> {
        Ok(s.repeat(n))
    }
}
pub fn repeat_original(plan: RepeatPlan<'_>) -> Option<String> {
    match render_repeat(plan, &mut OriginalRepeatWriter) {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}
pub trait SourceCharacters {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn iter(&self) -> impl Iterator<Item = char>;
}
pub trait CyclicCharacters {
    type Error;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get(&mut self, index: usize) -> Result<char, Self::Error>;
}
impl SourceCharacters for Vec<char> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn iter(&self) -> impl Iterator<Item = char> {
        self.as_slice().iter().copied()
    }
}
impl CyclicCharacters for Vec<char> {
    type Error = Infallible;
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn get(&mut self, index: usize) -> Result<char, Infallible> {
        Ok(self[index])
    }
}
/// This family-private output projection owns storage, never the PAD rules.
/// A selected projection may stream into its already-admitted compact carrier;
/// the original projection keeps the original fill and composed String order.
pub trait PadProjection<'a> {
    type Error;
    type Source: SourceCharacters;
    type Pad: CyclicCharacters<Error = Self::Error>;
    type Fill;
    type Output;
    type Emission;
    fn source(&mut self, text: &'a str) -> Result<Self::Source, Self::Error>;
    fn pad(&mut self, text: &'a str) -> Result<Self::Pad, Self::Error>;
    fn prefix(&mut self, chars: impl Iterator<Item = char>) -> Result<Self::Output, Self::Error>;
    fn new_fill(&mut self, source: &str, left: bool) -> Result<Self::Fill, Self::Error>;
    fn push_fill(&mut self, fill: &mut Self::Fill, ch: char) -> Result<(), Self::Error>;
    fn compose_left(&mut self, fill: Self::Fill, source: &str)
    -> Result<Self::Output, Self::Error>;
    fn compose_right(
        &mut self,
        source: &str,
        fill: Self::Fill,
    ) -> Result<Self::Output, Self::Error>;
    fn result_len(&self, result: &Self::Output) -> usize;
    fn emit(&mut self, result: Option<Self::Output>) -> Result<Self::Emission, Self::Error>;
}
pub fn pad_into<'a, P: PadProjection<'a>>(
    s: &'a str,
    target_len: i64,
    pad: impl FnOnce() -> &'a str,
    left: bool,
    projection: P,
) -> Result<P::Emission, P::Error> {
    pad_into_with_limit(s, target_len, pad, left, Some(MAX_STRING_BYTES), projection)
}

pub(crate) fn pad_into_with_limit<'a, P: PadProjection<'a>>(
    s: &'a str,
    target_len: i64,
    pad: impl FnOnce() -> &'a str,
    left: bool,
    byte_limit: Option<usize>,
    mut projection: P,
) -> Result<P::Emission, P::Error> {
    let Ok(target_len) = usize::try_from(target_len) else {
        return projection.emit(None);
    };
    if byte_limit.is_some_and(|limit| target_len > limit) {
        return projection.emit(None);
    }
    let pad = pad();
    let source_chars = projection.source(s)?;
    let source_len = source_chars.len();
    let result = if target_len <= source_len || pad.is_empty() {
        projection.prefix(source_chars.iter().take(target_len))?
    } else {
        let mut pad_chars = projection.pad(pad)?;
        let needed = target_len - source_len;
        let mut fill = projection.new_fill(s, left)?;
        for idx in 0..needed {
            let index = idx % pad_chars.len();
            let ch = pad_chars.get(index)?;
            projection.push_fill(&mut fill, ch)?;
        }
        if left {
            projection.compose_left(fill, s)?
        } else {
            projection.compose_right(s, fill)?
        }
    };
    if byte_limit.is_some_and(|limit| projection.result_len(&result) > limit) {
        projection.emit(None)
    } else {
        projection.emit(Some(result))
    }
}
pub struct OriginalPadProjection<'out> {
    out: &'out mut Vec<Option<String>>,
}
impl<'out> OriginalPadProjection<'out> {
    pub fn new(out: &'out mut Vec<Option<String>>) -> Self {
        Self { out }
    }
}
impl<'a> PadProjection<'a> for OriginalPadProjection<'_> {
    type Error = Infallible;
    type Source = Vec<char>;
    type Pad = Vec<char>;
    type Fill = String;
    type Output = String;
    type Emission = ();
    fn source(&mut self, s: &'a str) -> Result<Vec<char>, Infallible> {
        Ok(s.chars().collect())
    }
    fn pad(&mut self, pad: &'a str) -> Result<Vec<char>, Infallible> {
        Ok(pad.chars().collect())
    }
    fn prefix(&mut self, chars: impl Iterator<Item = char>) -> Result<String, Infallible> {
        Ok(chars.collect())
    }
    fn new_fill(&mut self, _source: &str, _left: bool) -> Result<String, Infallible> {
        Ok(String::new())
    }
    fn push_fill(&mut self, fill: &mut String, ch: char) -> Result<(), Infallible> {
        fill.push(ch);
        Ok(())
    }
    fn compose_left(&mut self, fill: String, s: &str) -> Result<String, Infallible> {
        let mut composed = String::with_capacity(fill.len() + s.len());
        composed.push_str(&fill);
        composed.push_str(s);
        Ok(composed)
    }
    fn compose_right(&mut self, s: &str, fill: String) -> Result<String, Infallible> {
        let mut composed = String::with_capacity(s.len() + fill.len());
        composed.push_str(s);
        composed.push_str(&fill);
        Ok(composed)
    }
    fn result_len(&self, result: &String) -> usize {
        result.len()
    }
    fn emit(&mut self, result: Option<String>) -> Result<(), Infallible> {
        self.out.push(result);
        Ok(())
    }
}

#[cfg(test)]
#[path = "string_repeat_pad_core_tests.rs"]
mod tests;
