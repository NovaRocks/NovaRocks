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

mod temporal_zone_ids;
use temporal_zone_ids::{ZONE_IDS, ZONES_BY_NAME};

const OFFSET_LIMIT_MINUTES: i16 = 14 * 60;
const MIN_OFFSET_MINUTES: i16 = -OFFSET_LIMIT_MINUTES;

const UTC_NAMES: &[&str] = &[
    "GMT",
    "GMT0",
    "GMT+0",
    "GMT-0",
    "Etc/GMT",
    "Etc/GMT0",
    "Etc/GMT+0",
    "Etc/GMT-0",
    "UT",
    "UT+0",
    "UT-0",
    "Etc/UT",
    "Etc/UT+0",
    "Etc/UT-0",
    "UTC",
    "UTC+0",
    "UTC-0",
    "Etc/UTC",
    "Etc/UTC+0",
    "Etc/UTC-0",
    "+0000",
    "+00:00",
    "-0000",
    "-00:00",
    "Z",
    "Zulu",
    "UCT",
    "Greenwich",
    "Universal",
    "Etc/Universal",
    "Etc/UCT",
];
const PREFIXES: &[&str] = &["Etc/GMT", "Etc/UTC", "Etc/UT", "GMT", "UTC", "UT"];

/// A stable key from the frozen Trino time-zone index. Different indexed names
/// retain different keys even when they share identical wall-clock rules.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TimeZoneKey(u16);

impl TimeZoneKey {
    pub const UTC: Self = Self(0);

    /// Position is the frozen wire key, including reserved positions.
    pub const fn index_material() -> &'static [&'static str] {
        ZONE_IDS
    }

    pub fn from_raw(raw: u16) -> Option<Self> {
        ZONE_IDS.get(usize::from(raw)).filter(|id| !id.is_empty())?;
        Some(Self(raw))
    }

    pub const fn raw(self) -> u16 {
        self.0
    }

    pub fn name(self) -> &'static str {
        ZONE_IDS[usize::from(self.0)]
    }

    pub const fn from_offset_minutes(minutes: i16) -> Option<Self> {
        // The contiguous non-zero offsets are the first entries in the Trino index.
        match minutes {
            MIN_OFFSET_MINUTES..=-1 => Some(Self((minutes + OFFSET_LIMIT_MINUTES + 1) as u16)),
            0 => Some(Self::UTC),
            1..=OFFSET_LIMIT_MINUTES => Some(Self((minutes + OFFSET_LIMIT_MINUTES) as u16)),
            _ => None,
        }
    }

    pub const fn fixed_offset_minutes(self) -> Option<i16> {
        match self.0 {
            0 => Some(0),
            code @ 1..=840 => Some(code as i16 - (OFFSET_LIMIT_MINUTES + 1)),
            code @ 841..=1680 => Some(code as i16 - OFFSET_LIMIT_MINUTES),
            _ => None,
        }
    }

    /// Coerce header, property, and SQL zone arguments using Trino's indexed
    /// lookup and normalization. Literal parsing also applies its own grammar.
    pub fn parse(name: &str) -> Option<Self> {
        if let Ok(position) =
            ZONES_BY_NAME.binary_search_by_key(&name, |id| ZONE_IDS[usize::from(*id)])
        {
            return Some(Self(ZONES_BY_NAME[position]));
        }
        if UTC_NAMES.contains(&name) {
            return Some(Self::UTC);
        }
        for prefix in PREFIXES {
            let Some(offset) = name.strip_prefix(prefix) else {
                continue;
            };
            if !offset.starts_with(['+', '-']) {
                return None;
            }
            if let Some((hour, minute)) = offset.split_once(':') {
                return prefixed_offset(prefix, hour.parse().ok()?, minute.parse().ok()?);
            }
            if offset.len() <= 3 {
                return prefixed_offset(prefix, offset.parse().ok()?, 0);
            }
            // JDK-normalized prefixed offsets retain their prefix. Except for
            // zero, the resulting ID has no entry in the Trino index.
            return (prefix.len() < 4 && iso_offset_seconds(offset)? == 0).then_some(Self::UTC);
        }
        let seconds = iso_offset_seconds(name)?;
        if seconds % 60 != 0 {
            return None;
        }
        Self::from_offset_minutes((seconds / 60).try_into().ok()?)
    }
}

fn prefixed_offset(prefix: &str, mut hour: i16, minute: i16) -> Option<TimeZoneKey> {
    if !(0..=59).contains(&minute) {
        return None;
    }
    if prefix == "Etc/GMT" {
        hour = hour.checked_neg()?;
    }
    let minutes = hour.checked_mul(60)?;
    let signed_minute = if hour < 0 { -minute } else { minute };
    TimeZoneKey::from_offset_minutes(minutes.checked_add(signed_minute)?)
}

/// Java's ISO ZoneOffset grammar also admits seconds; Trino key admission
/// accepts the result only when seconds are zero and minutes lie in range.
pub(crate) fn iso_offset_seconds(text: &str) -> Option<i32> {
    let (&sign, body) = text.as_bytes().split_first()?;
    let sign = match sign {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    fn decimal(bytes: &[u8]) -> Option<i32> {
        bytes.iter().try_fold(0, |result, digit| {
            digit
                .is_ascii_digit()
                .then(|| result * 10 + i32::from(digit - b'0'))
        })
    }
    let (hour, minute, second) = match body {
        h @ [..] if h.len() <= 2 && !h.is_empty() => (decimal(h)?, 0, 0),
        [a, b, c, d] => (decimal(&[*a, *b])?, decimal(&[*c, *d])?, 0),
        [a, b, b':', c, d] => (decimal(&[*a, *b])?, decimal(&[*c, *d])?, 0),
        [a, b, c, d, e, f] => (
            decimal(&[*a, *b])?,
            decimal(&[*c, *d])?,
            decimal(&[*e, *f])?,
        ),
        [a, b, b':', c, d, b':', e, f] => (
            decimal(&[*a, *b])?,
            decimal(&[*c, *d])?,
            decimal(&[*e, *f])?,
        ),
        _ => return None,
    };
    if hour > 18 || minute > 59 || second > 59 {
        return None;
    }
    let seconds = hour * 3600 + minute * 60 + second;
    (seconds <= 18 * 3600).then_some(sign * seconds)
}
