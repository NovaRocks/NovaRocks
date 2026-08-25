//! Small, copyable identities used as lifecycle registry keys.

use std::cmp::Ordering;

use novarocks_types::QueryId;

use super::error::ContractError;
use crate::{common, novarocks};

/// Nonzero ordinal for one physical attempt of a logical query.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AttemptId(u64);

impl AttemptId {
    pub fn new(value: u64) -> Result<Self, ContractError> {
        if value == 0 {
            return Err(ContractError::invalid_value("attempt id must be nonzero"));
        }
        Ok(Self(value))
    }

    /// Validates an attempt ordinal taken from `QueryExecutionId.attempt_id`.
    pub fn try_from_proto(value: u64) -> Result<Self, ContractError> {
        Self::new(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn to_proto(self) -> u64 {
        self.0
    }
}

/// Immutable identity for one physical execution attempt of a logical query.
///
/// This deliberately remains a small `Copy` value rather than a
/// newtype-over-proto: both role-local registries use it as a map key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryExecutionId {
    query_id: QueryId,
    attempt_id: AttemptId,
}

impl QueryExecutionId {
    pub fn new(query_id: QueryId, attempt_id: AttemptId) -> Result<Self, ContractError> {
        if query_id.high() == 0 && query_id.low() == 0 {
            return Err(ContractError::invalid_value("query id must be nonzero"));
        }
        Ok(Self {
            query_id,
            attempt_id,
        })
    }

    /// Validates the generated wire identity without creating a parallel DTO.
    ///
    /// The check order intentionally matches the former lifecycle decoder:
    /// the required `query_id` is checked first, then the attempt ordinal, and
    /// finally the all-zero query-id invariant.
    pub fn try_from_proto(src: &novarocks::QueryExecutionId) -> Result<Self, ContractError> {
        let query_id = src
            .query_id
            .as_ref()
            .ok_or_else(|| ContractError::invalid_value("query id is required"))?;
        Self::new(
            QueryId::new(query_id.hi, query_id.lo),
            AttemptId::try_from_proto(src.attempt_id)?,
        )
    }

    pub fn to_proto(self) -> novarocks::QueryExecutionId {
        novarocks::QueryExecutionId {
            query_id: Some(common::UniqueId {
                hi: self.query_id.high(),
                lo: self.query_id.low(),
            }),
            attempt_id: self.attempt_id.to_proto(),
        }
    }

    pub const fn query_id(self) -> QueryId {
        self.query_id
    }

    pub const fn attempt_id(self) -> AttemptId {
        self.attempt_id
    }
}

impl From<QueryExecutionId> for novarocks::QueryExecutionId {
    fn from(value: QueryExecutionId) -> Self {
        value.to_proto()
    }
}

impl TryFrom<&novarocks::QueryExecutionId> for QueryExecutionId {
    type Error = ContractError;

    fn try_from(value: &novarocks::QueryExecutionId) -> Result<Self, Self::Error> {
        Self::try_from_proto(value)
    }
}

impl Ord for QueryExecutionId {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.query_id.high(), self.query_id.low(), self.attempt_id).cmp(&(
            other.query_id.high(),
            other.query_id.low(),
            other.attempt_id,
        ))
    }
}

impl PartialOrd for QueryExecutionId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::{AttemptId, QueryExecutionId};
    use crate::common;
    use crate::lifecycle::error::ContractErrorCode;
    use crate::novarocks;
    use novarocks_types::QueryId;

    #[test]
    fn round_trips_a_query_execution_identity_through_proto() {
        let identity = QueryExecutionId::new(
            QueryId::new(11, 12),
            AttemptId::new(3).expect("nonzero attempt"),
        )
        .expect("nonzero query id");

        let encoded = identity.to_proto();
        assert_eq!(QueryExecutionId::try_from_proto(&encoded), Ok(identity));
        assert_eq!(novarocks::QueryExecutionId::from(identity), encoded);
    }

    #[test]
    fn rejects_invalid_identity_values_in_decoder_order() {
        let missing_query_id = QueryExecutionId::try_from_proto(&novarocks::QueryExecutionId {
            attempt_id: 1,
            ..Default::default()
        })
        .expect_err("query id is required");
        assert_eq!(missing_query_id.code(), ContractErrorCode::InvalidValue);
        assert_eq!(missing_query_id.detail(), "query id is required");

        let zero_attempt_precedes_the_zero_query_id_check =
            QueryExecutionId::try_from_proto(&novarocks::QueryExecutionId {
                query_id: Some(common::UniqueId { hi: 0, lo: 0 }),
                attempt_id: 0,
            })
            .expect_err("attempt is validated first");
        assert_eq!(
            zero_attempt_precedes_the_zero_query_id_check.detail(),
            "attempt id must be nonzero"
        );

        let zero_query_id = QueryExecutionId::try_from_proto(&novarocks::QueryExecutionId {
            query_id: Some(common::UniqueId { hi: 0, lo: 0 }),
            attempt_id: 1,
        })
        .expect_err("zero query id is invalid");
        assert_eq!(zero_query_id.detail(), "query id must be nonzero");
    }
}
