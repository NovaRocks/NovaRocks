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

//! Backend adapter from execution profiles to native terminal wire values.

use novarocks_execution::runtime::profile::{ProfileNode, ProfileUnit, RuntimeProfileTree};
use novarocks_proto::novarocks::{
    Counter, ProfileNode as WireProfileNode, ProfileUnit as WireUnit,
};

pub(crate) fn encode_runtime_profile_tree(
    tree: &RuntimeProfileTree,
) -> novarocks_proto::novarocks::RuntimeProfileTree {
    novarocks_proto::novarocks::RuntimeProfileTree {
        root: Some(encode_profile_node(&tree.root)),
    }
}

fn encode_profile_node(node: &ProfileNode) -> WireProfileNode {
    WireProfileNode {
        name: node.name.clone(),
        node_id: node.node_id,
        counters: node
            .counters
            .iter()
            .map(|counter| Counter {
                name: counter.name.clone(),
                parent_name: counter.parent_name.clone(),
                unit: encode_profile_unit(counter.unit) as i32,
                value: counter.value,
                min_value: counter.min_value,
                max_value: counter.max_value,
            })
            .collect(),
        info_strings: node.info_strings.clone().into_iter().collect(),
        children: node.children.iter().map(encode_profile_node).collect(),
    }
}

fn encode_profile_unit(unit: ProfileUnit) -> WireUnit {
    match unit {
        ProfileUnit::Unit => WireUnit::Unit,
        ProfileUnit::CpuTicks => WireUnit::CpuTicks,
        ProfileUnit::Bytes => WireUnit::Bytes,
        ProfileUnit::TimeNs => WireUnit::TimeNs,
        ProfileUnit::TimeMs => WireUnit::TimeMs,
        ProfileUnit::TimeS => WireUnit::TimeS,
        ProfileUnit::None => WireUnit::None,
    }
}
