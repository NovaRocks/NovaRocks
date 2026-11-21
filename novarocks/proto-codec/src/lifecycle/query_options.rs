//! Wire query options shared by lifecycle participants.

use novarocks_proto_models::novarocks;

/// A `novarocks.QueryOptions` contract value.
///
/// The generated message is the sole representation. Runtime defaults and
/// runtime-owned execution options are deliberately not materialized here.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryOptions {
    raw: novarocks::QueryOptions,
}

impl QueryOptions {
    /// Retains the exact generated message without applying runtime defaults.
    ///
    /// Scalar values and optional-field presence remain unchanged. This
    /// constructor does not fill defaults or translate to an
    /// Execution-owned runtime options structure.
    pub const fn from_proto(raw: novarocks::QueryOptions) -> Self {
        Self { raw }
    }

    /// Returns the exact generated message.
    pub const fn as_proto(&self) -> &novarocks::QueryOptions {
        &self.raw
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::QueryOptions;
    use novarocks_proto_models::novarocks;

    #[test]
    fn preserves_the_exact_generated_query_options_message() {
        let raw = novarocks::QueryOptions {
            runtime_filter_wait_timeout_ms: Some(0),
            group_concat_max_len: Some(0),
            ..Default::default()
        };

        let parsed = QueryOptions::from_proto(raw);

        assert_eq!(parsed.as_proto(), &raw);
        assert_eq!(parsed.as_proto().encode_to_vec(), [64, 0, 80, 0]);
    }
}
