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

//! Frontend-local adapters for terminal wire leaves.
//!
//! Protocol owns validation and the generated terminal values. These adapters
//! only bridge Frontend-owned SPI write facts and execution profile views.

use novarocks_execution::runtime::profile::{
    ProfileCounter, ProfileNode, ProfileUnit, RuntimeProfileTree, default_counter_strategy,
};
use novarocks_proto::novarocks;
#[cfg(test)]
use novarocks_proto::{common, plan};
#[cfg(test)]
use novarocks_spi::connector::{ConnectorStagedReportFrame, ConnectorWriterTerminalState};

#[cfg(test)]
const CONNECTOR_WRITER_TERMINAL_STAGED: u32 = 0;

#[cfg(test)]
pub(crate) fn encode_connector_staged_report_frame(
    frame: &ConnectorStagedReportFrame,
) -> novarocks::ConnectorStagedReportFrame {
    let writer = frame.writer();
    let fragment_instance_id = writer.fragment_instance_id();
    novarocks::ConnectorStagedReportFrame {
        contract_version: frame.version(),
        writer: Some(plan::ConnectorWriterIdentity {
            operation_id: writer.operation_id().to_bytes().to_vec(),
            cohort_id: writer.cohort_id().to_bytes().to_vec(),
            execution_query_id: writer.execution_id().query_id().to_vec(),
            execution_attempt_id: writer.execution_id().attempt_id(),
            fragment_instance_id: Some(common::UniqueId {
                hi: i64::from_be_bytes(
                    fragment_instance_id[..8]
                        .try_into()
                        .expect("fixed UUID prefix"),
                ),
                lo: i64::from_be_bytes(
                    fragment_instance_id[8..]
                        .try_into()
                        .expect("fixed UUID suffix"),
                ),
            }),
            fragment_id: writer.fragment_id(),
            backend_num: writer.backend_num(),
            sink_ordinal: writer.sink_ordinal(),
            connector_instance_id: writer.binding_key().instance_id.as_str().to_string(),
            connector_incarnation: writer.binding_key().incarnation.to_bytes().to_vec(),
        }),
        terminal_state: match frame.state() {
            ConnectorWriterTerminalState::Staged => CONNECTOR_WRITER_TERMINAL_STAGED,
            ConnectorWriterTerminalState::Aborted => 1,
            ConnectorWriterTerminalState::Failed => 2,
        },
        input_rows: frame.summary().input_rows,
        staged_bytes: frame.summary().staged_bytes,
        artifact_count: frame.summary().artifact_count,
        part_index: frame.part_index(),
        part_count: frame.part_count(),
        logical_payload_len: frame.logical_payload_len(),
        logical_payload_sha256: frame.logical_payload_digest().to_vec(),
        frame_payload: frame.frame_payload().to_vec(),
        frame_payload_sha256: frame.frame_payload_digest().to_vec(),
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

fn decode_profile_counter(counter: &novarocks::Counter) -> Result<ProfileCounter, String> {
    let unit = match novarocks::ProfileUnit::try_from(counter.unit) {
        Ok(novarocks::ProfileUnit::Unit) => ProfileUnit::Unit,
        Ok(novarocks::ProfileUnit::CpuTicks) => ProfileUnit::CpuTicks,
        Ok(novarocks::ProfileUnit::Bytes) => ProfileUnit::Bytes,
        Ok(novarocks::ProfileUnit::TimeNs) => ProfileUnit::TimeNs,
        Ok(novarocks::ProfileUnit::TimeMs) => ProfileUnit::TimeMs,
        Ok(novarocks::ProfileUnit::TimeS) => ProfileUnit::TimeS,
        Ok(novarocks::ProfileUnit::None) => ProfileUnit::None,
        Ok(novarocks::ProfileUnit::Unspecified) => {
            return Err("ProfileUnit is unspecified in native runtime profile".to_string());
        }
        Err(_) => {
            return Err(format!(
                "unknown ProfileUnit value {} in native runtime profile",
                counter.unit
            ));
        }
    };
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
