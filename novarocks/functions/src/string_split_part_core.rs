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
//! ONE original SPLIT_PART directional search and rendering program.
//! The original collection policy remains infallible; selected projection owns
//! no temporary result String. Observation never selects a substring.
use std::convert::Infallible;
pub trait SplitProjection<'a> {
    type Error;
    type Output;
    fn empty(&mut self) -> Result<Self::Output, Self::Error>;
    fn text(&mut self, text: &'a str) -> Result<Self::Output, Self::Error>;
    fn character(&mut self, tail: &'a str, ch: char) -> Result<Self::Output, Self::Error>;
    fn observe_character(&mut self, ch: char) -> Result<(), Self::Error>;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}
pub fn project<'a, P: SplitProjection<'a>>(
    s: &'a str,
    delim: &str,
    idx: i64,
    projection: &mut P,
) -> Result<P::Output, P::Error> {
    if idx == 0 {
        return projection.empty();
    }
    if delim.is_empty() {
        if idx > s.len() as i64 {
            return projection.empty();
        }
        let mut h = 0usize;
        let mut num = 0i64;
        let target = idx.saturating_sub(1);
        while h < s.len() && num < target {
            let Some(ch) = s[h..].chars().next() else {
                return projection.empty();
            };
            h += ch.len_utf8();
            num += 1;
            projection.observe_character(ch)?;
        }
        if h >= s.len() {
            return projection.empty();
        }
        return match s[h..].chars().next() {
            Some(ch) => {
                projection.observe_character(ch)?;
                projection.character(&s[h..], ch)
            }
            None => projection.empty(),
        };
    }
    if let Some(v) = split_index_impl(s, delim, idx, projection)? {
        projection.text(v)
    } else if idx == 1 || idx == -1 {
        projection.text(s)
    } else {
        projection.empty()
    }
}
fn split_index_impl<'a, P: SplitProjection<'a>>(
    haystack: &'a str,
    delimiter: &str,
    part_number: i64,
    projection: &mut P,
) -> Result<Option<&'a str>, P::Error> {
    if part_number > 0 {
        split_index_positive(haystack, delimiter, part_number, projection)
    } else {
        split_index_negative(haystack, delimiter, part_number, projection)
    }
}
fn split_index_positive<'a, P: SplitProjection<'a>>(
    haystack: &'a str,
    delimiter: &str,
    part_number: i64,
    projection: &mut P,
) -> Result<Option<&'a str>, P::Error> {
    let haystack_bytes = haystack.as_bytes();
    let delimiter_bytes = delimiter.as_bytes();
    let delimiter_len = delimiter_bytes.len() as isize;
    let mut pre_offset = -delimiter_len;
    let mut offset = -delimiter_len;
    let mut num = 0i64;
    while num < part_number {
        pre_offset = offset;
        let search_start = (offset + delimiter_len) as usize;
        if search_start > haystack_bytes.len() {
            break;
        }
        let search_slice = &haystack_bytes[search_start..];
        if let Some(pos_rel) = find_subslice(search_slice, delimiter_bytes, projection)? {
            offset = (search_start + pos_rel) as isize;
            num += 1;
        } else {
            offset = haystack_bytes.len() as isize;
            num = if num == 0 { 0 } else { num + 1 };
            projection.step()?;
            break;
        }
        projection.step()?;
    }
    Ok(if num == part_number {
        let start = (pre_offset + delimiter_len) as usize;
        let end = offset as usize;
        Some(&haystack[start..end])
    } else {
        None
    })
}
fn split_index_negative<'a, P: SplitProjection<'a>>(
    haystack: &'a str,
    delimiter: &str,
    part_number: i64,
    projection: &mut P,
) -> Result<Option<&'a str>, P::Error> {
    // Preserve the original unchecked negation, including raw i64::MIN debug panic.
    let Some(target) = usize::try_from(-part_number).ok() else {
        return Ok(None);
    };
    if target == 0 {
        return Ok(None);
    }
    let mut offset = haystack.len() as isize;
    let mut pre_offset = offset;
    let mut num = 0usize;
    let mut search_scope = haystack;
    while num <= target && offset >= 0 {
        projection.flush()?;
        let found = search_scope.rfind(delimiter);
        projection.flush()?;
        if let Some(found) = found {
            offset = found as isize;
            num += 1;
            if num == target {
                projection.step()?;
                break;
            }
            pre_offset = offset;
            offset -= 1;
            search_scope = &haystack[..pre_offset as usize];
        } else {
            offset = -1;
            projection.step()?;
            break;
        }
        projection.step()?;
    }
    if offset == -1 && num != 0 {
        num += 1;
    }
    projection.step()?;
    Ok(if num == target {
        if offset == -1 {
            Some(&haystack[..pre_offset as usize])
        } else {
            let start = offset as usize + delimiter.len();
            let end = pre_offset as usize;
            Some(&haystack[start..end])
        }
    } else {
        None
    })
}
fn find_subslice<'a, P: SplitProjection<'a>>(
    haystack: &[u8],
    needle: &[u8],
    projection: &mut P,
) -> Result<Option<usize>, P::Error> {
    find_subslice_observed(haystack, needle, || projection.step())
}

/// Search for an exact byte delimiter while observing the actual comparisons.
pub(crate) fn find_subslice_observed<E>(
    haystack: &[u8],
    needle: &[u8],
    mut step: impl FnMut() -> Result<(), E>,
) -> Result<Option<usize>, E> {
    if needle.is_empty() {
        return Ok(Some(0));
    }
    if haystack.len() < needle.len() {
        return Ok(None);
    }
    let mut failure = None;
    let found = haystack.windows(needle.len()).position(|window| {
        match equal_window(window, needle, &mut step) {
            Ok(equal) => equal,
            Err(error) => {
                failure = Some(error);
                // Stop the one original position search immediately. This
                // sentinel is never published: its originating error wins.
                true
            }
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(found),
    }
}
/// ONE byte equality author. This is the original slice-equality semantics
/// with its primitive comparisons exposed to the selected observation policy.
/// The old v1 library Eq implementation changes locally; search/order/storage
/// remain original. No second comparison follows this result.
fn equal_window<E>(
    window: &[u8],
    needle: &[u8],
    step: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    if window.len() != needle.len() {
        return Ok(false);
    }
    for (left, right) in window.iter().zip(needle) {
        let same = left == right;
        step()?;
        if !same {
            step()?;
            return Ok(false);
        }
    }
    step()?;
    Ok(true)
}
struct OriginalProjection;
impl<'a> SplitProjection<'a> for OriginalProjection {
    type Error = Infallible;
    type Output = String;
    fn empty(&mut self) -> Result<String, Infallible> {
        Ok(String::new())
    }
    fn text(&mut self, text: &'a str) -> Result<String, Infallible> {
        Ok(text.to_string())
    }
    fn character(&mut self, _tail: &'a str, ch: char) -> Result<String, Infallible> {
        Ok(ch.to_string())
    }
    fn observe_character(&mut self, _ch: char) -> Result<(), Infallible> {
        Ok(())
    }
    fn step(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}
pub fn original(s: &str, delim: &str, idx: i64) -> String {
    match project(s, delim, idx, &mut OriginalProjection) {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}
