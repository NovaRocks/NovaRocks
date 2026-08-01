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

//! Retired broker file-scan decoder.
//!
//! `FILE_SCAN_NODE` used a Core public binding carrying a concrete
//! `FileScanRange`.  That binding is not part of the frozen fragment kernel;
//! decode must therefore fail at the original protocol field path.

use std::collections::{BTreeMap, HashMap};

use crate::protocol::starrocks::decode::error::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::layout::Layout;
use crate::protocol::starrocks::decode::node::{Lowered, ScanRangeCarrier};
use crate::thrift::{descriptors, internal_service, plan_nodes, types};
use novarocks::exec::expr::ExprArena;
use novarocks::protocol::FieldPath;

#[derive(Clone, Debug)]
pub(crate) struct BrokerFileProgramFacts;

pub(crate) fn decode_broker_file_program_facts(
    nodes: &[plan_nodes::TPlanNode],
    _raw_ranges: &BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    _arena: &mut ExprArena,
    nodes_path: FieldPath,
    _raw_ranges_path: FieldPath,
) -> Result<BTreeMap<i32, BrokerFileProgramFacts>, StarRocksFragmentDecodeError> {
    if let Some((index, _)) = nodes
        .iter()
        .enumerate()
        .find(|(_, node)| node.node_type == plan_nodes::TPlanNodeType::FILE_SCAN_NODE)
    {
        return Err(StarRocksFragmentDecodeError::unsupported(
            nodes_path.index(index).field("node_type"),
            "FILE_SCAN_NODE is retired; broker file execution is not part of the fragment kernel",
        ));
    }
    Ok(BTreeMap::new())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn lower_file_scan_node(
    _node: &plan_nodes::TPlanNode,
    _desc_tbl: Option<&descriptors::TDescriptorTable>,
    _tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    _layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    _scan_ranges: Option<ScanRangeCarrier>,
    _program_facts: Option<&BrokerFileProgramFacts>,
    _arena: &mut ExprArena,
    _out_layout: Layout,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    Err(StarRocksFragmentDecodeError::unsupported(
        node_path.field("node_type"),
        "FILE_SCAN_NODE is retired; broker file execution is not part of the fragment kernel",
    ))
}
