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

use novarocks_type_contract::{PICOS_PER_SECOND, TemporalMicros, TimeZoneKey};

const MATERIAL: &[u8] = include_bytes!("temporal_zone_rules.bin");
const HEADER_BYTES: usize = 48;
const TABLE_HEADER_BYTES: usize = 20;
const TRANSITION_BYTES: usize = 12;
const KEY_BYTES: usize = 4;
const INDEX_BYTES: usize = 4;
// The largest offset supported by java.time.ZoneOffset is 18 hours.
const MAX_JDK_OFFSET_MILLIS: i64 = 18 * 60 * 60 * 1000;

/// The actual library selected by the temporal overload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalZoneSource {
    Jdk,
    Joda,
}

/// JDK literal validation and string casts have distinct gap behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JdkGapPolicy {
    Reject,
    Advance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ZoneRuleError {
    OutOfRange,
    NonexistentLocalTime,
}

impl std::fmt::Display for ZoneRuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::OutOfRange => "Time zone conversion exceeds the supported instant range",
            Self::NonexistentLocalTime => "Local time does not exist in the selected time zone",
        })
    }
}

impl std::error::Error for ZoneRuleError {}

/// Generated rule bytes. The plan-contract digest also binds the stable
/// zone-index material and the interpretation of these records.
pub const TEMPORAL_ZONE_RULE_INTERPRETATION: &[u8] =
    b"jdk-tzdb2026b-literal-strict-and-text-advance; joda-2.15-lenient-and-offset-preservation/v1";

pub fn temporal_zone_rule_material() -> &'static [u8] {
    MATERIAL
}

pub fn offset_seconds(
    zone: TimeZoneKey,
    source: TemporalZoneSource,
    utc_millis: i64,
) -> Result<i32, ZoneRuleError> {
    RuleTable::get(zone, source).offset_seconds(utc_millis)
}

pub fn utc_to_local(
    zone: TimeZoneKey,
    source: TemporalZoneSource,
    utc: TemporalMicros,
) -> Result<TemporalMicros, ZoneRuleError> {
    let seconds = offset_seconds(zone, source, utc.floor_millis())?;
    TemporalMicros::from_picos(utc.as_picos() + i128::from(seconds) * i128::from(PICOS_PER_SECOND))
        .ok_or(ZoneRuleError::OutOfRange)
}

pub fn jdk_local_to_utc(
    zone: TimeZoneKey,
    local: TemporalMicros,
    gap_policy: JdkGapPolicy,
) -> Result<TemporalMicros, ZoneRuleError> {
    let table = RuleTable::get(zone, TemporalZoneSource::Jdk);
    let millis = local.floor_millis();
    let mut start = millis
        .checked_sub(MAX_JDK_OFFSET_MILLIS)
        .ok_or(ZoneRuleError::OutOfRange)?;
    let end = millis
        .checked_add(MAX_JDK_OFFSET_MILLIS)
        .ok_or(ZoneRuleError::OutOfRange)?;
    let mut before = table.offset_seconds(start)?;
    loop {
        let next = table.next_transition(start)?;
        let candidate = millis - i64::from(before) * 1000;
        if candidate >= start && next.is_none_or(|next| candidate < next) {
            return local_minus_offset(local, before);
        }
        let Some(next) = next.filter(|next| *next <= end) else {
            return Err(ZoneRuleError::OutOfRange);
        };
        let after = table.offset_seconds(next)?;
        if after > before
            && (next + i64::from(before) * 1000..next + i64::from(after) * 1000).contains(&millis)
        {
            return match gap_policy {
                JdkGapPolicy::Reject => Err(ZoneRuleError::NonexistentLocalTime),
                JdkGapPolicy::Advance => local_minus_offset(local, before),
            };
        }
        start = next;
        before = after;
    }
}

/// Joda's lenient local conversion estimates the offset on the zero-offset
/// timeline. A preferred prior instant is used by calendar arithmetic when
/// that instant's offset is still valid at the new local timestamp.
pub fn joda_local_to_utc(
    zone: TimeZoneKey,
    local: TemporalMicros,
    preferred: Option<TemporalMicros>,
) -> Result<TemporalMicros, ZoneRuleError> {
    let table = RuleTable::get(zone, TemporalZoneSource::Joda);
    let millis = local.floor_millis();
    if let Some(prior) = preferred {
        let original_offset = table.offset_seconds(prior.floor_millis())?;
        let original_estimate = millis
            .checked_sub(i64::from(original_offset) * 1000)
            .ok_or(ZoneRuleError::OutOfRange)?;
        if table.offset_seconds(original_estimate)? == original_offset {
            return local_minus_offset(local, original_offset);
        }
    }

    let local_offset = table.offset_seconds(millis)?;
    let estimated = millis - i64::from(local_offset) * 1000;
    let actual_offset = table.offset_seconds(estimated)?;
    if actual_offset != local_offset && local_offset < 0 {
        let adjusted = millis - i64::from(actual_offset) * 1000;
        if table.next_transition(estimated)? != table.next_transition(adjusted)? {
            return local_minus_offset(local, local_offset);
        }
    }
    local_minus_offset(local, actual_offset)
}

fn local_minus_offset(
    local: TemporalMicros,
    offset_seconds: i32,
) -> Result<TemporalMicros, ZoneRuleError> {
    TemporalMicros::from_picos(
        local.as_picos() - i128::from(offset_seconds) * i128::from(PICOS_PER_SECOND),
    )
    .ok_or(ZoneRuleError::OutOfRange)
}

/// A borrowed view over a deduplicated generated transition record. Search
/// accesses each fixed-width record directly without expanding the history.
#[derive(Clone, Copy)]
struct RuleTable {
    offset_before_first: i32,
    cycle: Option<usize>,
    next_after_domain: Option<i64>,
    transitions: &'static [u8],
}

impl RuleTable {
    fn get(zone: TimeZoneKey, source: TemporalZoneSource) -> Self {
        let key_at = HEADER_BYTES + usize::from(zone.raw()) * KEY_BYTES;
        let table_at = key_at
            + match source {
                TemporalZoneSource::Jdk => 0,
                TemporalZoneSource::Joda => 2,
            };
        let table = usize::from(u16::from_be_bytes(material(table_at)));
        let offset_index = HEADER_BYTES + read_u32(40) as usize * KEY_BYTES;
        let records = offset_index + read_u32(44) as usize * INDEX_BYTES;
        let record = records + read_u32(offset_index + table * INDEX_BYTES) as usize;
        let count = read_u32(record + 8) as usize;
        let next_after_domain = read_i64(record + 12);
        let bytes = record + TABLE_HEADER_BYTES;
        Self {
            offset_before_first: i32::from_be_bytes(material(record)),
            cycle: usize::try_from(i32::from_be_bytes(material(record + 4))).ok(),
            next_after_domain: (next_after_domain != i64::MAX).then_some(next_after_domain),
            transitions: &MATERIAL[bytes..bytes + count * TRANSITION_BYTES],
        }
    }

    fn request_time(self, time: i64) -> Result<(i64, i64), ZoneRuleError> {
        if !(read_i64(8)..read_i64(16)).contains(&time) {
            return Err(ZoneRuleError::OutOfRange);
        }
        let start = read_i64(24);
        if self.cycle.is_some() && time >= start {
            let phase = start + (time - start).rem_euclid(read_i64(32));
            Ok((phase, time - phase))
        } else {
            Ok((time, 0))
        }
    }

    fn transition_time(self, index: usize) -> i64 {
        let start = index * TRANSITION_BYTES;
        i64::from_be_bytes(
            self.transitions[start..start + 8]
                .try_into()
                .expect("generated transition time"),
        )
    }

    fn next_index(self, time: i64) -> usize {
        let mut low = 0;
        let mut high = self.transitions.len() / TRANSITION_BYTES;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.transition_time(middle) <= time {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    fn offset_seconds(self, time: i64) -> Result<i32, ZoneRuleError> {
        let (phase, _) = self.request_time(time)?;
        let next = self.next_index(phase);
        if next == 0 {
            return Ok(self.offset_before_first);
        }
        let at = (next - 1) * TRANSITION_BYTES + 8;
        Ok(i32::from_be_bytes(
            self.transitions[at..at + 4]
                .try_into()
                .expect("generated transition offset"),
        ))
    }

    fn next_transition(self, time: i64) -> Result<Option<i64>, ZoneRuleError> {
        let (phase, cycle_delta) = self.request_time(time)?;
        let next = self.next_index(phase);
        if next < self.transitions.len() / TRANSITION_BYTES {
            return Ok(Some(self.transition_time(next) + cycle_delta));
        }
        if let Some(cycle) = self.cycle {
            return Ok(Some(
                self.transition_time(cycle) + cycle_delta + read_i64(32),
            ));
        }
        Ok(self.next_after_domain)
    }
}

fn material<const N: usize>(at: usize) -> [u8; N] {
    MATERIAL[at..at + N]
        .try_into()
        .expect("complete generated rule table")
}

fn read_i64(at: usize) -> i64 {
    i64::from_be_bytes(material(at))
}

fn read_u32(at: usize) -> u32 {
    u32::from_be_bytes(material(at))
}
