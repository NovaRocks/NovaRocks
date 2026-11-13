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

//! Bind reference inputs after final case selection and before cluster launch.
use anyhow::Result;
use novarocks_cluster_harness::reference_inputs::{ReferenceInput, ResolvedReference};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn bind_selected<F>(
    dry_run: bool,
    suites: impl IntoIterator<Item = (String, bool, Vec<ReferenceInput>)>,
    resolve: F,
) -> Result<BTreeMap<String, ResolvedReference>>
where
    F: FnOnce(&BTreeSet<ReferenceInput>) -> Result<ResolvedReference>,
{
    let selected = suites
        .into_iter()
        .filter(|(_, has_cases, inputs)| *has_cases && !inputs.is_empty())
        .collect::<Vec<_>>();
    let needed = selected
        .iter()
        .flat_map(|(_, _, inputs)| inputs.iter().copied())
        .collect::<BTreeSet<_>>();
    if dry_run || needed.is_empty() {
        return Ok(BTreeMap::new());
    }
    let reference = resolve(&needed)?;
    Ok(selected
        .into_iter()
        .map(|(name, _, inputs)| (name, reference.subset(&inputs.into_iter().collect())))
        .collect())
}

#[cfg(test)]
mod tests;
