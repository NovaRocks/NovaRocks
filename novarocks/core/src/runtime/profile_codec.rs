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

//! Native protocol adapter for execution-owned runtime profiles.

use novarocks_execution::runtime::profile::{
    ProfileCounter, ProfileNode, ProfileUnit, Profiler, RuntimeProfileTree,
    default_counter_strategy,
};
use novarocks_protocol::novarocks;

pub(crate) fn encode_native_runtime_profile(profiler: &Profiler) -> novarocks::RuntimeProfileTree {
    encode_runtime_profile_tree(&profiler.to_native_tree())
}

pub(crate) fn encode_runtime_profile_tree(
    tree: &RuntimeProfileTree,
) -> novarocks::RuntimeProfileTree {
    novarocks::RuntimeProfileTree {
        root: Some(encode_profile_node(&tree.root)),
    }
}

pub(crate) fn decode_runtime_profile_tree(
    tree: &novarocks::RuntimeProfileTree,
) -> Result<RuntimeProfileTree, String> {
    let root = tree
        .root
        .as_ref()
        .ok_or_else(|| "RuntimeProfileTree missing root".to_string())?;
    Ok(RuntimeProfileTree {
        root: decode_profile_node(root)?,
    })
}

pub(crate) fn encode_profile_unit_value(unit: ProfileUnit) -> i32 {
    encode_profile_unit(unit) as i32
}

fn encode_profile_unit(unit: ProfileUnit) -> novarocks::ProfileUnit {
    match unit {
        ProfileUnit::Unit => novarocks::ProfileUnit::Unit,
        ProfileUnit::CpuTicks => novarocks::ProfileUnit::CpuTicks,
        ProfileUnit::Bytes => novarocks::ProfileUnit::Bytes,
        ProfileUnit::TimeNs => novarocks::ProfileUnit::TimeNs,
        ProfileUnit::TimeMs => novarocks::ProfileUnit::TimeMs,
        ProfileUnit::TimeS => novarocks::ProfileUnit::TimeS,
        ProfileUnit::None => novarocks::ProfileUnit::None,
    }
}

fn decode_profile_unit(unit: i32) -> Result<ProfileUnit, String> {
    match novarocks::ProfileUnit::try_from(unit) {
        Ok(novarocks::ProfileUnit::Unit) => Ok(ProfileUnit::Unit),
        Ok(novarocks::ProfileUnit::CpuTicks) => Ok(ProfileUnit::CpuTicks),
        Ok(novarocks::ProfileUnit::Bytes) => Ok(ProfileUnit::Bytes),
        Ok(novarocks::ProfileUnit::TimeNs) => Ok(ProfileUnit::TimeNs),
        Ok(novarocks::ProfileUnit::TimeMs) => Ok(ProfileUnit::TimeMs),
        Ok(novarocks::ProfileUnit::TimeS) => Ok(ProfileUnit::TimeS),
        Ok(novarocks::ProfileUnit::None) => Ok(ProfileUnit::None),
        Ok(novarocks::ProfileUnit::Unspecified) => {
            Err("ProfileUnit is unspecified in native runtime profile".to_string())
        }
        Err(_) => Err(format!(
            "unknown ProfileUnit value {unit} in native runtime profile"
        )),
    }
}

fn encode_profile_node(node: &ProfileNode) -> novarocks::ProfileNode {
    novarocks::ProfileNode {
        name: node.name.clone(),
        node_id: node.node_id,
        counters: node.counters.iter().map(encode_profile_counter).collect(),
        info_strings: node.info_strings.clone().into_iter().collect(),
        children: node.children.iter().map(encode_profile_node).collect(),
    }
}

fn decode_profile_node(node: &novarocks::ProfileNode) -> Result<ProfileNode, String> {
    Ok(ProfileNode {
        name: node.name.clone(),
        node_id: node.node_id,
        counters: node
            .counters
            .iter()
            .map(decode_profile_counter)
            .collect::<Result<Vec<_>, _>>()?,
        info_strings: node
            .info_strings
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        children: node
            .children
            .iter()
            .map(decode_profile_node)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn encode_profile_counter(counter: &ProfileCounter) -> novarocks::Counter {
    novarocks::Counter {
        name: counter.name.clone(),
        parent_name: counter.parent_name.clone(),
        unit: encode_profile_unit(counter.unit) as i32,
        value: counter.value,
        min_value: counter.min_value,
        max_value: counter.max_value,
    }
}

fn decode_profile_counter(counter: &novarocks::Counter) -> Result<ProfileCounter, String> {
    let unit = decode_profile_unit(counter.unit)?;
    Ok(ProfileCounter {
        name: counter.name.clone(),
        parent_name: counter.parent_name.clone(),
        unit,
        strategy: default_counter_strategy(unit),
        value: counter.value,
        min_value: counter.min_value,
        max_value: counter.max_value,
    })
}

#[cfg(test)]
mod tests {
    use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};

    use super::decode_runtime_profile_tree;

    #[test]
    fn roundtrip_rebuilds_execution_profile_strategy() {
        let profile = RuntimeProfile::new("root");
        profile.counter_set("scan", ProfileUnit::TimeNs, 5);
        let tree = super::encode_native_runtime_profile(&profile);
        let decoded = decode_runtime_profile_tree(&tree).expect("decode profile");
        assert_eq!(decoded.root.counters[0].unit, ProfileUnit::TimeNs);
    }
}
