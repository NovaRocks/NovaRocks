//! Validated, role-neutral values a native query addresses a backend with.
//!
//! `QueryControlEndpoint` is the endpoint half of a backend descriptor;
//! `RuntimeFilterContribution` is the runtime-filter installation a query
//! context carries. Both outlived the participant manifest this module was
//! named for.

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::novarocks;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryControlEndpoint {
    raw: novarocks::QueryControlEndpoint,
}

impl QueryControlEndpoint {
    /// Constructs a generated endpoint before applying the canonical
    /// lifecycle validation. This is a convenience for role-local assembly
    /// and tests; the generated message remains the stored representation.
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::QueryControlEndpoint {
            host: host.into(),
            port: u32::from(port),
        })
    }

    pub fn parse(raw: novarocks::QueryControlEndpoint) -> Result<Self, ProtocolError> {
        if raw.host.trim().is_empty() {
            return Err(invalid(
                FieldPath::root("query_control_endpoint").field("host"),
                "query control endpoint host must not be empty",
            ));
        }
        if raw.port == 0 {
            return Err(invalid(
                FieldPath::root("query_control_endpoint").field("port"),
                "query control endpoint port must be nonzero",
            ));
        }
        if raw.port > u32::from(u16::MAX) {
            return Err(out_of_range(
                FieldPath::root("query_control_endpoint").field("port"),
                "query control endpoint port exceeds u16 range",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlEndpoint {
        &self.raw
    }

    pub fn host(&self) -> &str {
        &self.raw.host
    }

    pub const fn port(&self) -> u16 {
        self.raw.port as u16
    }
}

/// Opaque, validated generated runtime-filter contribution.
///
/// The lifecycle and install payloads deliberately remain generated values:
/// Backend owns their semantic decoding, while this contract owns only the
/// participant-manifest carrier shape.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterContribution {
    raw: novarocks::RuntimeFilterContribution,
}

impl RuntimeFilterContribution {
    pub fn parse(raw: novarocks::RuntimeFilterContribution) -> Result<Self, ProtocolError> {
        if raw.participant_id == 0 {
            return Err(invalid(
                FieldPath::root("runtime_filter_contribution").field("participant_id"),
                "runtime filter participant id must be nonzero",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::RuntimeFilterContribution {
        &self.raw
    }

    pub const fn participant_id(&self) -> u32 {
        self.raw.participant_id
    }
}

fn out_of_range(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::OutOfRange, detail)
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

#[cfg(test)]
mod tests {
    use super::{QueryControlEndpoint, RuntimeFilterContribution};
    use crate::ProtocolErrorKind;
    use novarocks_proto_models::novarocks;

    #[test]
    fn endpoint_rejects_every_unaddressable_value() {
        for (host, port, kind, field) in [
            ("   ", 9030u32, ProtocolErrorKind::InvalidValue, "host"),
            ("127.0.0.1", 0, ProtocolErrorKind::InvalidValue, "port"),
            (
                "127.0.0.1",
                u32::from(u16::MAX) + 1,
                ProtocolErrorKind::OutOfRange,
                "port",
            ),
        ] {
            let error = QueryControlEndpoint::parse(novarocks::QueryControlEndpoint {
                host: host.into(),
                port,
            })
            .expect_err("an unaddressable endpoint must not parse");
            assert_eq!(error.kind(), kind);
            assert_eq!(
                error.path().to_string(),
                format!("query_control_endpoint.{field}")
            );
        }
    }

    #[test]
    fn endpoint_retains_the_exact_generated_message() {
        let endpoint = QueryControlEndpoint::new("10.0.0.7", 9060).expect("addressable endpoint");
        assert_eq!(endpoint.host(), "10.0.0.7");
        assert_eq!(endpoint.port(), 9060);
        assert_eq!(
            QueryControlEndpoint::parse(endpoint.as_proto().clone()).expect("reparse"),
            endpoint
        );
    }

    /// The lifecycle and install payloads stay generated values: only the
    /// participant id is this carrier's own fact, so only it is validated.
    #[test]
    fn contribution_validates_its_participant_id_and_nothing_below_it() {
        let error = RuntimeFilterContribution::parse(novarocks::RuntimeFilterContribution {
            participant_id: 0,
            ..Default::default()
        })
        .expect_err("an unattributed contribution must not parse");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            error.path().to_string(),
            "runtime_filter_contribution.participant_id"
        );

        let contribution = RuntimeFilterContribution::parse(novarocks::RuntimeFilterContribution {
            participant_id: 7,
            ..Default::default()
        })
        .expect("an attributed contribution parses without decoding its payloads");
        assert_eq!(contribution.participant_id(), 7);
        assert!(contribution.as_proto().install.is_none());
    }
}
