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

use std::cmp::Reverse;
use std::collections::BTreeMap;

use crate::commit::preparation::ManifestMergePolicy;
use crate::iceberg::Result;
use crate::iceberg::spec::ManifestContentType;

use super::{MergeCandidate, invalid};

pub(super) enum Layout<'a> {
    Keep(Vec<MergeCandidate<'a>>),
    Merge(Vec<MergeCandidate<'a>>),
}

pub(super) fn plan(
    candidates: Vec<MergeCandidate<'_>>,
    policy: ManifestMergePolicy,
) -> Result<Vec<Layout<'_>>> {
    if candidates.is_empty() || !policy.enabled {
        return Ok(vec![Layout::Keep(candidates)]);
    }
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    let mut first = [true; 2];
    for candidate in candidates {
        let content = match candidate.descriptor.content {
            ManifestContentType::Data => 0,
            ManifestContentType::Deletes => 1,
        };
        let weight = u64::try_from(candidate.descriptor.manifest_length).map_err(|_| {
            invalid(format!(
                "Negative manifest length: {}",
                candidate.descriptor.manifest_path
            ))
        })?;
        let group = groups
            .entry((content, Reverse(candidate.descriptor.partition_spec_id)))
            .or_default();
        group.push((candidate, weight, std::mem::take(&mut first[content])));
    }

    let mut result = Vec::new();
    for group in groups.into_values() {
        let mut bins = Vec::new();
        let mut bin = Vec::new();
        let mut weight = 0_u64;
        let mut contains_first = false;
        for (candidate, bytes, is_first) in group.into_iter().rev() {
            let fits = weight
                .checked_add(bytes)
                .is_some_and(|total| total <= policy.target_bytes);
            if !bin.is_empty() && !fits {
                bins.push(finish_bin(std::mem::take(&mut bin), contains_first, policy));
                weight = 0;
                contains_first = false;
            }
            weight += bytes;
            contains_first |= is_first;
            bin.push(candidate);
        }
        bins.push(finish_bin(bin, contains_first, policy));
        result.extend(bins.into_iter().rev());
    }
    Ok(result)
}

fn finish_bin(
    mut bin: Vec<MergeCandidate<'_>>,
    contains_first: bool,
    policy: ManifestMergePolicy,
) -> Layout<'_> {
    bin.reverse();
    if bin.len() == 1 || (contains_first && bin.len() < policy.min_count) {
        Layout::Keep(bin)
    } else {
        Layout::Merge(bin)
    }
}
