// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Closed summary vocabulary projection. Parents admit original backing and
//! every output array before invoking these emitters; no grant is acquired here.

use crate::physical_node_v2::{self as node, NodeCodecError};
use novarocks_physical_plan as p;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::CompileCheckpoints;

pub(crate) fn encode_kind(kind: p::SummaryKind) -> i32 {
    match kind {
        p::SummaryKind::WriteCounts => wire::SummaryKind::WriteCounts as i32,
        p::SummaryKind::DeleteVerification => wire::SummaryKind::DeleteVerification as i32,
    }
}
pub(crate) fn decode_kind(raw: i32) -> Result<p::SummaryKind, &'static str> {
    match wire::SummaryKind::try_from(raw) {
        Ok(wire::SummaryKind::WriteCounts) => Ok(p::SummaryKind::WriteCounts),
        Ok(wire::SummaryKind::DeleteVerification) => Ok(p::SummaryKind::DeleteVerification),
        _ => Err("summary kind is absent or unknown"),
    }
}
pub(crate) fn encode_field(field: p::SummaryField) -> wire::SummaryField {
    wire::SummaryField {
        kind: encode_kind(field.kind()),
        ordinal: Some(field.ordinal() as u32),
    }
}
pub(crate) fn decode_field(field: &wire::SummaryField) -> Result<p::SummaryField, &'static str> {
    let kind = decode_kind(field.kind)?;
    let ordinal = field.ordinal.ok_or("summary field ordinal is absent")?;
    let ordinal =
        usize::try_from(ordinal).map_err(|_| "summary field ordinal is unrepresentable")?;
    p::SummaryField::from_ordinal(kind, ordinal)
        .ok_or("summary field ordinal differs from its kind")
}
pub(crate) fn encode_port(port: p::FragmentOutputPort) -> i32 {
    match port {
        p::FragmentOutputPort::Root => wire::FragmentOutputPort::Root as i32,
        p::FragmentOutputPort::Summary(p::SummaryKind::WriteCounts) => {
            wire::FragmentOutputPort::SummaryWriteCounts as i32
        }
        p::FragmentOutputPort::Summary(p::SummaryKind::DeleteVerification) => {
            wire::FragmentOutputPort::SummaryDeleteVerification as i32
        }
    }
}
pub(crate) fn decode_port(raw: i32) -> Result<p::FragmentOutputPort, &'static str> {
    match wire::FragmentOutputPort::try_from(raw) {
        Ok(wire::FragmentOutputPort::Root) => Ok(p::FragmentOutputPort::Root),
        Ok(wire::FragmentOutputPort::SummaryWriteCounts) => {
            Ok(p::FragmentOutputPort::Summary(p::SummaryKind::WriteCounts))
        }
        Ok(wire::FragmentOutputPort::SummaryDeleteVerification) => Ok(
            p::FragmentOutputPort::Summary(p::SummaryKind::DeleteVerification),
        ),
        _ => Err("fragment source port is absent, retired or unknown"),
    }
}

pub(crate) fn encode_columns(
    columns: p::SummaryColumns,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SummaryColumns, NodeCodecError> {
    let mut values = node::reserve(columns.values().len(), work)?;
    for id in columns.values() {
        work.step()?;
        values.push(id.get());
    }
    Ok(wire::SummaryColumns {
        kind: encode_kind(columns.kind()),
        value_ids: values,
    })
}
pub(crate) fn decode_columns(
    columns: &wire::SummaryColumns,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::SummaryColumns, NodeCodecError> {
    let value = decode_columns_shape(columns)?;
    for _ in &columns.value_ids {
        work.step()?;
    }
    Ok(value)
}
/// Fixed closed-width vocabulary check; no traversal or allocation precedes admission.
pub(crate) fn decode_columns_shape(
    columns: &wire::SummaryColumns,
) -> Result<p::SummaryColumns, NodeCodecError> {
    let kind = decode_kind(columns.kind).map_err(node::invalid)?;
    if columns.value_ids.len() != kind.fields().len() {
        return Err(node::invalid("summary field count differs from its kind"));
    }
    Ok(match kind {
        p::SummaryKind::WriteCounts => p::SummaryColumns::WriteCounts([
            p::ValueId::new(columns.value_ids[0]),
            p::ValueId::new(columns.value_ids[1]),
            p::ValueId::new(columns.value_ids[2]),
        ]),
        p::SummaryKind::DeleteVerification => p::SummaryColumns::DeleteVerification([
            p::ValueId::new(columns.value_ids[0]),
            p::ValueId::new(columns.value_ids[1]),
            p::ValueId::new(columns.value_ids[2]),
            p::ValueId::new(columns.value_ids[3]),
        ]),
    })
}

pub(crate) fn encode_output(
    output: p::SummaryOutput,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SummaryOutput, NodeCodecError> {
    let author = match output.author {
        p::SummaryAuthor::RouterSink => wire::summary_author::Kind::RouterSink(Empty {}),
        p::SummaryAuthor::Node(node) => wire::summary_author::Kind::NodeId(node.get()),
    };
    work.step()?;
    Ok(wire::SummaryOutput {
        edge_id: Some(output.edge.get()),
        columns: Some(encode_columns(output.columns, work)?),
        author: Some(wire::SummaryAuthor { kind: Some(author) }),
    })
}
pub(crate) fn decode_output(
    output: &wire::SummaryOutput,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::SummaryOutput, NodeCodecError> {
    let edge = output
        .edge_id
        .ok_or_else(|| node::invalid("summary edge is absent"))?;
    let columns = output
        .columns
        .as_ref()
        .ok_or_else(|| node::invalid("summary columns are absent"))?;
    let author = output
        .author
        .as_ref()
        .and_then(|author| author.kind.as_ref())
        .ok_or_else(|| node::invalid("summary author is absent"))?;
    let author = match author {
        wire::summary_author::Kind::RouterSink(_) => p::SummaryAuthor::RouterSink,
        wire::summary_author::Kind::NodeId(node) => p::SummaryAuthor::Node(p::NodeId::new(*node)),
    };
    work.step()?;
    Ok(p::SummaryOutput {
        edge: p::EdgeId::new(edge),
        columns: decode_columns(columns, work)?,
        author,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn field_vocabulary_keeps_zero_ordinal_present_and_kinds_independent() {
        for (field, kind, ordinal) in [
            (p::SummaryField::Inserted, 1, 0),
            (p::SummaryField::Replaced, 1, 1),
            (p::SummaryField::Deleted, 1, 2),
            (p::SummaryField::Requested, 2, 0),
            (p::SummaryField::Matched, 2, 1),
            (p::SummaryField::InsufficientKeys, 2, 2),
            (p::SummaryField::InsufficientFingerprint, 2, 3),
        ] {
            let wire = wire::SummaryField {
                kind,
                ordinal: Some(ordinal),
            };
            assert_eq!(encode_field(field), wire);
            assert_eq!(decode_field(&wire), Ok(field));
        }
        for (kind, ordinal) in [
            (0, Some(0)),
            (3, Some(0)),
            (1, None),
            (1, Some(3)),
            (2, Some(4)),
        ] {
            assert!(decode_field(&wire::SummaryField { kind, ordinal }).is_err());
        }
    }
    #[test]
    fn both_closed_layouts_preserve_ids_in_exact_order() {
        for (kind, ids) in [(1, vec![0, 4, u32::MAX]), (2, vec![0, 4, u32::MAX, 17])] {
            let columns = decode_columns_shape(&wire::SummaryColumns {
                kind,
                value_ids: ids.clone(),
            })
            .unwrap();
            assert_eq!(
                columns
                    .values()
                    .iter()
                    .map(|id| id.get())
                    .collect::<Vec<_>>(),
                ids
            );
            assert_eq!(encode_kind(columns.kind()), kind);
        }
        for (kind, ids) in [(1, vec![0, 1, 2, 3]), (2, vec![0, 1, 2]), (0, vec![])] {
            assert!(
                decode_columns_shape(&wire::SummaryColumns {
                    kind,
                    value_ids: ids
                })
                .is_err()
            );
        }
    }
    #[test]
    fn outputs_round_trip_both_kinds_and_uncertified_node_author_identity() {
        struct Control;
        impl novarocks_type_contract::PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        for output in [
            p::SummaryOutput {
                edge: p::EdgeId::new(0),
                columns: p::SummaryColumns::write_counts(
                    p::ValueId::new(0),
                    p::ValueId::new(7),
                    p::ValueId::new(u32::MAX),
                ),
                author: p::SummaryAuthor::RouterSink,
            },
            p::SummaryOutput {
                edge: p::EdgeId::new(u32::MAX),
                columns: p::SummaryColumns::delete_verification(
                    p::ValueId::new(0),
                    p::ValueId::new(7),
                    p::ValueId::new(8),
                    p::ValueId::new(u32::MAX),
                ),
                author: p::SummaryAuthor::Node(p::NodeId::new(u32::MAX)),
            },
        ] {
            let mut work = CompileCheckpoints::try_new(
                &Control,
                novarocks_type_contract::CompilePhase::Encode,
            )
            .unwrap();
            let wire = encode_output(output, &mut work).unwrap();
            work.finish().unwrap();
            assert_eq!(wire.columns.as_ref().unwrap().value_ids[0], 0);
            if output.kind() == p::SummaryKind::DeleteVerification {
                assert_eq!(wire.columns.as_ref().unwrap().kind, 2);
                assert_eq!(
                    wire.columns.as_ref().unwrap().value_ids,
                    [0, 7, 8, u32::MAX]
                );
                assert_eq!(
                    wire.author.as_ref().unwrap().kind,
                    Some(wire::summary_author::Kind::NodeId(u32::MAX))
                );
            }
            let mut work = CompileCheckpoints::try_new(
                &Control,
                novarocks_type_contract::CompilePhase::Decode,
            )
            .unwrap();
            assert_eq!(decode_output(&wire, &mut work).unwrap(), output);
            work.finish().unwrap();
        }
    }
}
