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

//! Backend-owned static scan-source contract decoding.

use std::collections::BTreeMap;

use novarocks::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind, ScanSourceContract};
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_protocol::plan;

pub(crate) fn decode_scan_source_contracts(
    root: &plan::DistributedNode,
    path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ScanSourceContract>, ProtocolError> {
    let mut assignments = BTreeMap::new();
    visit(root, path, &mut assignments)?;
    Ok(assignments)
}

fn visit(
    node: &plan::DistributedNode,
    path: FieldPath,
    assignments: &mut BTreeMap<FragmentNodeId, ScanSourceContract>,
) -> Result<(), ProtocolError> {
    if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
        && let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref()
    {
        let scan_path = path
            .clone()
            .field("payload")
            .field("physical")
            .field("scan");
        let table = scan.table.as_ref().ok_or_else(|| {
            error(
                scan_path.clone().field("table"),
                ProtocolErrorKind::MissingField,
                format!("native ScanNode node_id={} requires table", node.node_id),
            )
        })?;
        let source = table.source.as_ref().ok_or_else(|| {
            error(
                scan_path.clone().field("table").field("source"),
                ProtocolErrorKind::MissingField,
                format!("native ScanNode node_id={} requires source", node.node_id),
            )
        })?;
        let source = source.kind.as_ref().ok_or_else(|| {
            error(
                scan_path
                    .clone()
                    .field("table")
                    .field("source")
                    .field("kind"),
                ProtocolErrorKind::MissingField,
                format!(
                    "native ScanNode node_id={} requires source kind",
                    node.node_id
                ),
            )
        })?;
        let kind = match source {
            plan::scan_source::Kind::StarrocksTable(_) => ScanAssignmentKind::StarRocksTablet,
            _ => ScanAssignmentKind::File,
        };
        if assignments
            .insert(
                FragmentNodeId::new(node.node_id),
                ScanSourceContract::new(kind),
            )
            .is_some()
        {
            return Err(error(
                path.clone().field("node_id"),
                ProtocolErrorKind::InconsistentFields,
                format!("native plan has duplicate scan node_id={}", node.node_id),
            ));
        }
    }
    for (index, child) in node.children.iter().enumerate() {
        visit(
            child,
            path.clone().field("children").index(index),
            assignments,
        )?;
    }
    Ok(())
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}

#[cfg(test)]
mod tests {
    use super::decode_scan_source_contracts;
    use novarocks::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
    use novarocks::protocol::FieldPath;
    use novarocks_protocol::plan;

    #[test]
    fn classifies_connector_read_as_file_assignment() {
        let root = plan::DistributedNode {
            node_id: 17,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    table: Some(plan::TableDef {
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::ConnectorRead(
                                plan::ConnectorReadSource::default(),
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };

        let contracts =
            decode_scan_source_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect("decode scan contract");

        assert_eq!(
            contracts
                .get(&FragmentNodeId::new(17))
                .map(|contract| contract.assignment_kind()),
            Some(ScanAssignmentKind::File)
        );
    }

    #[test]
    fn preserves_missing_scan_source_path() {
        let root = plan::DistributedNode {
            node_id: 17,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    table: Some(plan::TableDef::default()),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };

        let error =
            decode_scan_source_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect_err("missing source must fail");

        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root.payload.physical.scan.table.source (missing field): native ScanNode node_id=17 requires source"
        );
    }
}
