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
//! Synchronous data-plane handlers delegated by the Backend RPC service.

use std::fmt;

use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
};
use novarocks_proto_models as proto;
use novarocks_types::UniqueId;

/// Exactly the addressing fields one inbound exchange frame carries.
///
/// The frame has no query, attempt, or task identity in it, so every field
/// here is a kernel key or a wire counter. An authority resolves those against
/// its own frozen facts; nothing downstream may derive authority from them.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ExchangeRouteQuery {
    pub destination_fragment_instance_id: UniqueId,
    pub destination_node_id: i32,
    pub source_fragment_instance_id: UniqueId,
    pub sender_ordinal: u32,
    pub sender_count: u32,
}

/// What one exchange-ingress authority says about one inbound frame.
///
/// The question is three-valued on purpose. Two owners are wired at once --
/// the fragment-based query lifecycle and the task substrate -- and each holds
/// a disjoint set of destinations. Collapsing this into `Result` would make
/// "this destination is not mine" indistinguishable from "this route is
/// illegal", so a destination nobody holds would be reported as the refusal of
/// whichever authority happened to be asked, and a destination two authorities
/// hold would be settled by whichever answered first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeRouteClaim {
    /// This authority holds no destination matching the frame. It is not a
    /// permission and not a refusal: it is a statement of non-ownership.
    NotHeld,
    /// This authority holds the destination and the frame's route is legal.
    Authorized,
    /// This authority holds the destination and the frame's route is illegal.
    Refused(String),
}

impl ExchangeRouteClaim {
    const fn claims_destination(&self) -> bool {
        matches!(self, Self::Authorized | Self::Refused(_))
    }
}

/// One owner of exchange destinations on this backend.
///
/// An authority answers only about destinations it holds. It never speaks for
/// another owner's destinations and never falls back to another owner's
/// verdict; the caller composes the answers.
pub trait ExchangeRouteAuthority: Send + Sync + 'static {
    /// A stable name used in refusal text, so an operator reading a rejected
    /// frame can tell which owner decided it.
    fn authority_name(&self) -> &'static str;

    fn claim_exchange_route(&self, query: ExchangeRouteQuery) -> ExchangeRouteClaim;
}

/// Why one inbound exchange frame is not admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ExchangeRouteRefusal {
    /// No authority is wired at all, so no owner could have been asked.
    NoAuthority,
    /// Every wired authority disclaimed the destination.
    NoOwner,
    /// The one authority that holds the destination refused the route.
    Refused {
        authority: &'static str,
        detail: String,
    },
    /// More than one authority holds the destination. This is a composition
    /// conflict, not a race to be won: whichever answer were taken, one owner
    /// would be executing frames the other believes it owns.
    Conflict { authorities: Vec<&'static str> },
}

impl fmt::Display for ExchangeRouteRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAuthority => {
                formatter.write_str("exchange ingress has no route authority on this backend")
            }
            Self::NoOwner => formatter
                .write_str("no active exchange owner on this backend holds this destination"),
            Self::Refused { authority, detail } => {
                write!(
                    formatter,
                    "{authority} holds this destination and refused the route: {detail}"
                )
            }
            Self::Conflict { authorities } => write!(
                formatter,
                "this destination is claimed by more than one exchange owner ({}), so no owner may admit the frame",
                authorities.join(", ")
            ),
        }
    }
}

/// Settles one frame against every wired authority, requiring exactly one
/// claimant.
///
/// Zero claimants is a refusal rather than a permission, and two claimants is
/// a refusal rather than a first-answer win, so the order the authorities are
/// wired in cannot change the answer.
fn settle_exchange_route(
    authorities: &[&dyn ExchangeRouteAuthority],
    query: ExchangeRouteQuery,
) -> Result<(), ExchangeRouteRefusal> {
    if authorities.is_empty() {
        return Err(ExchangeRouteRefusal::NoAuthority);
    }
    let mut claimants: Vec<(&'static str, ExchangeRouteClaim)> = Vec::new();
    for authority in authorities {
        let claim = authority.claim_exchange_route(query);
        if claim.claims_destination() {
            claimants.push((authority.authority_name(), claim));
        }
    }
    match claimants.len() {
        0 => Err(ExchangeRouteRefusal::NoOwner),
        1 => match claimants.pop().expect("one claimant") {
            (_, ExchangeRouteClaim::Authorized) => Ok(()),
            (authority, ExchangeRouteClaim::Refused(detail)) => {
                Err(ExchangeRouteRefusal::Refused { authority, detail })
            }
            (_, ExchangeRouteClaim::NotHeld) => {
                unreachable!("a disclaimed route is not collected as a claimant")
            }
        },
        _ => Err(ExchangeRouteRefusal::Conflict {
            authorities: claimants.into_iter().map(|(name, _)| name).collect(),
        }),
    }
}

fn ok_common_status() -> proto::common::Status {
    proto::common::Status {
        code: 0,
        message: String::new(),
    }
}

fn error_common_status(message: impl Into<String>) -> proto::common::Status {
    proto::common::Status {
        code: 1,
        message: message.into(),
    }
}

pub fn handle_transmit_chunk(
    receiver_port: &dyn ExchangeReceiverPort,
    authorities: &[&dyn ExchangeRouteAuthority],
    params: proto::novarocks::ExchangeRequest,
) -> proto::novarocks::ExchangeResponse {
    let mut response = proto::novarocks::ExchangeResponse {
        ack_sequence: params.sequence,
        status: Some(ok_common_status()),
    };

    let destination_fragment_instance_id = UniqueId::new(params.finst_id_hi, params.finst_id_lo);
    let source_fragment_instance_id =
        UniqueId::new(params.source_finst_id_hi, params.source_finst_id_lo);
    if destination_fragment_instance_id == UniqueId::new(0, 0)
        || source_fragment_instance_id == UniqueId::new(0, 0)
    {
        response.status = Some(error_common_status(
            "exchange ingress requires non-zero source and destination fragment instance IDs",
        ));
        return response;
    }
    if params.node_id < 0 {
        response.status = Some(error_common_status(
            "exchange ingress requires a non-negative destination node ID",
        ));
        return response;
    }
    if params.sender_count == 0 || params.sender_ordinal >= params.sender_count {
        response.status = Some(error_common_status(
            "exchange ingress requires a sender ordinal below a non-zero sender count",
        ));
        return response;
    }
    if let Err(refusal) = settle_exchange_route(
        authorities,
        ExchangeRouteQuery {
            destination_fragment_instance_id,
            destination_node_id: params.node_id,
            source_fragment_instance_id,
            sender_ordinal: params.sender_ordinal,
            sender_count: params.sender_count,
        },
    ) {
        response.status = Some(error_common_status(format!(
            "exchange ingress route rejected: {refusal}"
        )));
        return response;
    }

    let key = ExchangeReceiverKey {
        fragment_instance_id: destination_fragment_instance_id,
        node_id: params.node_id,
    };
    let frame = ExchangeReceiverFrame {
        source_fragment_instance_id,
        sender_ordinal: params.sender_ordinal,
        sender_count: params.sender_count,
        sender_id: params.sender_id,
        backend_number: params.be_number,
        sequence: params.sequence,
        eos: params.eos,
        payload: params.payload,
    };
    if let Err(err) = receiver_port.push(key, frame) {
        response.status = Some(error_common_status(format!(
            "exchange ingress failed: {err}"
        )));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::{
        ExchangeRouteAuthority, ExchangeRouteClaim, ExchangeRouteQuery, handle_transmit_chunk,
    };
    use novarocks_execution::runtime::fragment::io::UnavailableExchangeReceiverPort;
    use novarocks_proto_models as proto;

    /// An authority with a fixed answer, so a composition test can state
    /// exactly which owners claim a destination.
    struct FixedAuthority {
        name: &'static str,
        claim: ExchangeRouteClaim,
    }

    impl FixedAuthority {
        fn new(name: &'static str, claim: ExchangeRouteClaim) -> Self {
            Self { name, claim }
        }
    }

    impl ExchangeRouteAuthority for FixedAuthority {
        fn authority_name(&self) -> &'static str {
            self.name
        }

        fn claim_exchange_route(&self, _query: ExchangeRouteQuery) -> ExchangeRouteClaim {
            self.claim.clone()
        }
    }

    fn frame() -> proto::novarocks::ExchangeRequest {
        proto::novarocks::ExchangeRequest {
            finst_id_hi: 1,
            finst_id_lo: 2,
            node_id: 7,
            source_finst_id_hi: 3,
            source_finst_id_lo: 4,
            sender_ordinal: 0,
            sender_count: 1,
            sender_id: 11,
            be_number: 0,
            eos: false,
            sequence: 42,
            payload: vec![0xff],
        }
    }

    fn transmit(
        authorities: &[&dyn ExchangeRouteAuthority],
        request: proto::novarocks::ExchangeRequest,
    ) -> proto::common::Status {
        handle_transmit_chunk(&UnavailableExchangeReceiverPort, authorities, request)
            .status
            .expect("status")
    }

    #[test]
    fn exchange_route_rejection_happens_before_receiver_delivery() {
        let lifecycle = FixedAuthority::new(
            "the fragment query lifecycle",
            ExchangeRouteClaim::Refused("the route is absent from the manifest".to_string()),
        );
        let response = handle_transmit_chunk(
            &UnavailableExchangeReceiverPort,
            &[&lifecycle as &dyn ExchangeRouteAuthority],
            frame(),
        );

        assert_eq!(response.ack_sequence, 42);
        let status = response.status.expect("status");
        assert_eq!(status.code, 1);
        assert!(status.message.contains("route rejected"));
    }

    #[test]
    fn pre_lnp9_exchange_shape_is_rejected_before_route_authorization() {
        let lifecycle = FixedAuthority::new(
            "the fragment query lifecycle",
            ExchangeRouteClaim::Refused("unreachable".to_string()),
        );
        let status = transmit(
            &[&lifecycle as &dyn ExchangeRouteAuthority],
            proto::novarocks::ExchangeRequest {
                finst_id_hi: 1,
                finst_id_lo: 2,
                node_id: 7,
                sender_id: 11,
                be_number: 0,
                eos: false,
                sequence: 42,
                payload: vec![0xff],
                ..Default::default()
            },
        );

        assert_eq!(status.code, 1);
        assert!(status.message.contains("requires non-zero source"));
    }

    /// Catches a data plane that authorizes a frame from a single authority's
    /// silence. Before the task substrate was wired as its own authority the
    /// lifecycle owner answered for every destination, so a task-protocol
    /// destination -- which it has no manifest for -- was reported as a route
    /// absent from every manifest, and every distributed query with an
    /// exchange failed.
    #[test]
    fn a_destination_no_authority_holds_is_refused_as_an_absent_owner() {
        let lifecycle =
            FixedAuthority::new("the fragment query lifecycle", ExchangeRouteClaim::NotHeld);
        let tasks = FixedAuthority::new("the task substrate", ExchangeRouteClaim::NotHeld);
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );

        assert_eq!(status.code, 1);
        assert!(
            status
                .message
                .contains("no active exchange owner on this backend holds this destination"),
            "unexpected refusal: {}",
            status.message
        );
        // A destination nobody holds must not name an owner: the wire may not
        // tell "not created yet" from "not yours".
        assert!(!status.message.contains("the task substrate"));
        assert!(!status.message.contains("the fragment query lifecycle"));
    }

    /// The task substrate's claim is what admits a task-protocol frame, and it
    /// admits it while the lifecycle owner disclaims the same destination.
    #[test]
    fn a_destination_one_authority_holds_is_settled_by_that_authority() {
        let lifecycle =
            FixedAuthority::new("the fragment query lifecycle", ExchangeRouteClaim::NotHeld);
        let tasks = FixedAuthority::new("the task substrate", ExchangeRouteClaim::Authorized);
        // The receiver port is unavailable, so an authorized frame fails at
        // delivery rather than at authorization. That is the proof the route
        // was admitted.
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );
        assert_eq!(status.code, 1);
        assert!(
            status.message.contains("exchange ingress failed"),
            "unexpected status: {}",
            status.message
        );
        assert!(!status.message.contains("route rejected"));
    }

    /// A refusal must say which owner decided it, because the two owners
    /// refuse for unrelated reasons and an operator reading only "absent from
    /// every manifest" would be told the wrong thing on the task path.
    #[test]
    fn the_holding_authority_is_named_in_its_own_refusal() {
        let lifecycle =
            FixedAuthority::new("the fragment query lifecycle", ExchangeRouteClaim::NotHeld);
        let tasks = FixedAuthority::new(
            "the task substrate",
            ExchangeRouteClaim::Refused("its topology froze 3 senders".to_string()),
        );
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );

        assert_eq!(status.code, 1);
        assert!(
            status.message.contains("the task substrate")
                && status.message.contains("its topology froze 3 senders"),
            "unexpected refusal: {}",
            status.message
        );
    }

    /// Two owners holding one destination is a composition conflict. Taking
    /// either answer would let one owner run frames the other believes it
    /// owns, so the gate closes -- and the order the owners are wired in must
    /// not change that.
    #[test]
    fn two_authorities_holding_one_destination_close_the_gate_rather_than_race() {
        let lifecycle = FixedAuthority::new(
            "the fragment query lifecycle",
            ExchangeRouteClaim::Authorized,
        );
        let tasks = FixedAuthority::new("the task substrate", ExchangeRouteClaim::Authorized);
        for order in [
            [
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            [
                &tasks as &dyn ExchangeRouteAuthority,
                &lifecycle as &dyn ExchangeRouteAuthority,
            ],
        ] {
            let status = transmit(&order, frame());
            assert_eq!(status.code, 1);
            assert!(
                status
                    .message
                    .contains("claimed by more than one exchange owner"),
                "unexpected refusal: {}",
                status.message
            );
        }

        // One authorizing and one refusing is the same conflict: a refusal is
        // a claim on the destination, so it does not lose to a permission.
        let refusing = FixedAuthority::new(
            "the task substrate",
            ExchangeRouteClaim::Refused("its topology froze 3 senders".to_string()),
        );
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &refusing as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );
        assert_eq!(status.code, 1);
        assert!(
            status
                .message
                .contains("claimed by more than one exchange owner"),
            "unexpected refusal: {}",
            status.message
        );
    }

    #[test]
    fn a_data_plane_with_no_authority_admits_nothing() {
        let status = transmit(&[], frame());
        assert_eq!(status.code, 1);
        assert!(
            status
                .message
                .contains("exchange ingress has no route authority"),
            "unexpected refusal: {}",
            status.message
        );
    }

    #[test]
    fn an_invalid_sender_set_is_rejected_before_any_authority_is_asked() {
        struct PanickingAuthority;

        impl ExchangeRouteAuthority for PanickingAuthority {
            fn authority_name(&self) -> &'static str {
                "the panicking owner"
            }

            fn claim_exchange_route(&self, _query: ExchangeRouteQuery) -> ExchangeRouteClaim {
                unreachable!("a structurally invalid sender set never reaches an authority")
            }
        }

        let authority = PanickingAuthority;
        for (sender_ordinal, sender_count) in [(0, 0), (2, 2), (5, 3)] {
            let status = transmit(
                &[&authority as &dyn ExchangeRouteAuthority],
                proto::novarocks::ExchangeRequest {
                    sender_ordinal,
                    sender_count,
                    ..frame()
                },
            );
            assert_eq!(status.code, 1);
            assert!(
                status
                    .message
                    .contains("requires a sender ordinal below a non-zero sender count"),
                "unexpected refusal for ordinal {sender_ordinal} of {sender_count}: {}",
                status.message
            );
        }
    }
}
