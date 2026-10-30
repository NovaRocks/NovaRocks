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

//! The existing Apply reply grammar's allocation bounds, before prost.

use super::{ResourcePreflightError, ScanResult, Value, checked_increment, finish, for_fields};
use crate::{
    TransportBudget,
    descriptor::MAX_SPLIT_PLAN_NODES,
    domain::{MAX_DOMAIN_UPDATES, MAX_OPEN_EDGES},
};
use novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES;

/// Reuses the production batch, domain, context-membership and terminal-profile
/// limits. Malformed protobuf is still interpreted only by prost. Repeated
/// singular messages share counters because prost merges their repeated fields.
pub fn check_operation_reply(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    if raw.len() > crate::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES {
        return Err(ResourcePreflightError(
            "operation reply exceeds Native decoded-message limit",
        ));
    }
    let mut receipts = 0;
    finish(for_fields(raw, |field, value| {
        if let (1, Value::Bytes(receipt)) = (field, value) {
            checked_increment(
                &mut receipts,
                TransportBudget::DEFAULT.max_batch_items(),
                "operation reply exceeds batch item bound",
            )?;
            let mut ack = AckCounts::default();
            for_fields(receipt, |field, value| {
                if let (2, Value::Bytes(verdict)) = (field, value) {
                    ack.verdict(verdict)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }))
}

#[derive(Default)]
struct AckCounts {
    create_domains: usize,
    update_domains: usize,
    context_domains: usize,
    quiesce_tasks: usize,
    profile_sections: [usize; 4],
}
impl AckCounts {
    fn verdict(&mut self, raw: &[u8]) -> ScanResult {
        for_fields(raw, |field, value| {
            let Value::Bytes(ack) = value else {
                return Ok(());
            };
            match field {
                5 => task_ack(ack, &mut self.create_domains),
                6 => task_ack(ack, &mut self.update_domains),
                7 => for_fields(ack, |field, value| {
                    if let (4, Value::Bytes(_)) = (field, value) {
                        // The actual typed context decoder uses the same 256
                        // bound as task domains; its older IDL comment says 16.
                        checked_increment(
                            &mut self.context_domains,
                            MAX_DOMAIN_UPDATES,
                            "context acknowledgement exceeds domain bound",
                        )?;
                    }
                    Ok(())
                }),
                8 => for_fields(ack, |field, value| {
                    if let (5, Value::Bytes(telemetry)) = (field, value) {
                        self.telemetry(telemetry)?;
                    }
                    Ok(())
                }),
                11 => for_fields(ack, |field, value| {
                    if let (3, Value::Bytes(_)) = (field, value) {
                        checked_increment(
                            &mut self.quiesce_tasks,
                            TransportBudget::DEFAULT.max_tasks_per_context(),
                            "quiesce acknowledgement exceeds context membership bound",
                        )?;
                    }
                    Ok(())
                }),
                _ => Ok(()),
            }
        })
    }
    fn telemetry(&mut self, raw: &[u8]) -> ScanResult {
        for_fields(raw, |field, value| {
            if let (1, Value::Bytes(profile)) = (field, value) {
                for_fields(profile, |field, value| {
                    if (2..=5).contains(&field) && matches!(value, Value::Bytes(_)) {
                        checked_increment(
                            &mut self.profile_sections[(field - 2) as usize],
                            QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES,
                            "terminal profile exceeds section bound",
                        )?;
                    }
                    Ok(())
                })?;
            }
            Ok(())
        })
    }
}
fn task_ack(raw: &[u8], domains: &mut usize) -> ScanResult {
    for_fields(raw, |field, value| {
        if let (2, Value::Bytes(domain)) = (field, value) {
            checked_increment(
                domains,
                MAX_DOMAIN_UPDATES,
                "task acknowledgement exceeds domain bound",
            )?;
            let mut nodes = 0;
            let mut edges = 0;
            for_fields(domain, |field, value| match (field, value) {
                (1, Value::Bytes(split)) => for_fields(split, |field, value| {
                    if let (1, Value::Bytes(_)) = (field, value) {
                        checked_increment(
                            &mut nodes,
                            MAX_SPLIT_PLAN_NODES,
                            "split acknowledgement exceeds plan-node bound",
                        )?;
                    }
                    Ok(())
                }),
                (3, Value::Bytes(open)) => for_fields(open, |field, value| {
                    if field == 1 {
                        match value {
                            Value::Varint => checked_increment(
                                &mut edges,
                                MAX_OPEN_EDGES,
                                "edge acknowledgement exceeds edge bound",
                            )?,
                            Value::Bytes(packed) => {
                                let mut cursor = super::Cursor::new(packed);
                                while !cursor.is_empty() {
                                    cursor.varint()?;
                                    checked_increment(
                                        &mut edges,
                                        MAX_OPEN_EDGES,
                                        "edge acknowledgement exceeds edge bound",
                                    )?;
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(())
                }),
                _ => Ok(()),
            })?;
            Ok(())
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_models::novarocks as wire;
    use prost::Message;

    #[test]
    fn repeated_singular_ack_messages_share_the_original_domain_bound() {
        // Two occurrences of the same oneof message are merged by prost.
        // Each is below the bound; the actual decoded union exceeds it.
        let ack = wire::UpdateTaskAck {
            accepted_domains: vec![wire::TaskDomainReceipt::default(); MAX_DOMAIN_UPDATES / 2 + 1],
            ..Default::default()
        }
        .encode_to_vec();
        fn message(field: u8, payload: &[u8]) -> Vec<u8> {
            let mut out = vec![field << 3 | 2];
            let mut length = payload.len();
            while length >= 128 {
                out.push((length as u8) | 128);
                length >>= 7;
            }
            out.push(length as u8);
            out.extend_from_slice(payload);
            out
        }
        let mut verdict = message(6, &ack);
        verdict.extend(message(6, &ack));
        let raw = message(1, &message(2, &verdict));
        let decoded = wire::ApplyTaskOperationsResponse::decode(raw.as_slice()).unwrap();
        let Some(wire::task_operation_receipt::Result::Verdict(verdict)) =
            &decoded.receipts[0].result
        else {
            panic!("verdict");
        };
        let Some(wire::task_operation_verdict::Ack::UpdateTask(ack)) = &verdict.ack else {
            panic!("update");
        };
        assert_eq!(ack.accepted_domains.len(), MAX_DOMAIN_UPDATES + 2);
        assert!(check_operation_reply(&raw).is_err());
    }
}
