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

use novarocks_functions::temporal_zone_rules::*;
use novarocks_type_contract::{TemporalMicros, TimeZoneKey};

#[test]
fn temporal_zone_rules_distinguish_jdk_joda_and_future_cycles() {
    let accra = TimeZoneKey::parse("Africa/Accra").unwrap();
    let epoch_1900 = -2_208_988_800_000;
    assert_eq!(
        offset_seconds(accra, TemporalZoneSource::Jdk, epoch_1900),
        Ok(-968)
    );
    assert_eq!(
        offset_seconds(accra, TemporalZoneSource::Joda, epoch_1900),
        Ok(-52)
    );
    let ny = TimeZoneKey::parse("America/New_York").unwrap();
    let july_2100 = 4_118_126_400_000;
    for source in [TemporalZoneSource::Jdk, TemporalZoneSource::Joda] {
        let one_gregorian_cycle = 146_097 * 86_400_000;
        assert_eq!(offset_seconds(ny, source, july_2100), Ok(-14_400));
        assert_eq!(
            offset_seconds(ny, source, july_2100 + 2 * one_gregorian_cycle),
            Ok(-14_400)
        );
    }
}

#[test]
fn temporal_zone_rules_apply_the_resolved_gap_and_overlap_policies() {
    let ny = TimeZoneKey::parse("America/New_York").unwrap();
    let new_york_gap = TemporalMicros::whole_micros(1_710_037_800_000_000);
    assert_eq!(
        jdk_local_to_utc(ny, new_york_gap, JdkGapPolicy::Reject),
        Err(ZoneRuleError::NonexistentLocalTime)
    );
    let shifted = TemporalMicros::whole_micros(1_710_055_800_000_000);
    assert_eq!(
        jdk_local_to_utc(ny, new_york_gap, JdkGapPolicy::Advance),
        Ok(shifted)
    );
    assert_eq!(joda_local_to_utc(ny, new_york_gap, None), Ok(shifted));

    let berlin = TimeZoneKey::parse("Europe/Berlin").unwrap();
    let berlin_overlap = TemporalMicros::whole_micros(1_729_996_200_000_000);
    let early = TemporalMicros::whole_micros(1_729_989_000_000_000);
    let late = TemporalMicros::whole_micros(1_729_992_600_000_000);
    assert_eq!(
        jdk_local_to_utc(berlin, berlin_overlap, JdkGapPolicy::Reject),
        Ok(early)
    );
    assert_eq!(joda_local_to_utc(berlin, berlin_overlap, None), Ok(late));
    assert_eq!(
        joda_local_to_utc(berlin, berlin_overlap, Some(early)),
        Ok(early)
    );
}
