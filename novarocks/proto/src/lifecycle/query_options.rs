//! Validated wire query options shared by lifecycle participants.

use super::error::ContractError;
use crate::novarocks;

/// A parsed `novarocks.QueryOptions` contract value.
///
/// The generated message is the sole representation. Runtime defaults and
/// runtime-owned execution options are deliberately not materialized here.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryOptions {
    raw: novarocks::QueryOptions,
}

impl QueryOptions {
    /// Validates wire-level bounds and spill-option self-consistency.
    ///
    /// Zero-valued scalar options retain their existing native meaning of
    /// "unset". This constructor does not fill defaults or translate to an
    /// Execution-owned runtime options structure.
    pub fn parse(raw: novarocks::QueryOptions) -> Result<Self, ContractError> {
        validate_spill_options(&raw)?;
        Ok(Self { raw })
    }

    /// Returns the exact generated message accepted at the boundary.
    pub const fn as_proto(&self) -> &novarocks::QueryOptions {
        &self.raw
    }
}

fn validate_spill_options(raw: &novarocks::QueryOptions) -> Result<(), ContractError> {
    if !raw.enable_spill {
        return Ok(());
    }

    let spill = raw
        .spill_options
        .as_ref()
        .ok_or_else(|| ContractError::invalid_value("enable_spill=true requires spill_options"))?;
    match spill.spill_mode {
        0..=2 => {}
        3 => {
            return Err(ContractError::invalid_value(
                "spill_mode RANDOM is not supported yet",
            ));
        }
        value => {
            return Err(ContractError::invalid_value(format!(
                "unknown spill_mode value {value}"
            )));
        }
    }
    if !spill.spill_mem_limit_threshold.is_finite() {
        return Err(ContractError::invalid_value(
            "spill_mem_limit_threshold must be finite",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::QueryOptions;
    use crate::lifecycle::error::ContractErrorCode;
    use crate::novarocks;

    #[test]
    fn preserves_the_exact_generated_query_options_message() {
        let raw = novarocks::QueryOptions {
            batch_size: 4096,
            runtime_filter_wait_timeout_ms: Some(0),
            group_concat_max_len: Some(0),
            ..Default::default()
        };

        let parsed = QueryOptions::parse(raw).expect("valid wire options");

        assert_eq!(parsed.as_proto(), &raw);
        assert_eq!(
            parsed.as_proto().encode_to_vec(),
            [8, 128, 32, 64, 0, 80, 0]
        );
    }

    #[test]
    fn rejects_enabled_spill_without_its_required_options() {
        let error = QueryOptions::parse(novarocks::QueryOptions {
            enable_spill: true,
            ..Default::default()
        })
        .expect_err("spill options are required when spilling is enabled");

        assert_eq!(error.code(), ContractErrorCode::InvalidValue);
        assert_eq!(error.detail(), "enable_spill=true requires spill_options");
    }

    #[test]
    fn rejects_unknown_and_unsupported_spill_modes() {
        for (spill_mode, expected) in [
            (3, "spill_mode RANDOM is not supported yet"),
            (99, "unknown spill_mode value 99"),
        ] {
            let error = QueryOptions::parse(novarocks::QueryOptions {
                enable_spill: true,
                spill_options: Some(novarocks::SpillOptions {
                    spill_mode,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect_err("invalid spill mode");
            assert_eq!(error.code(), ContractErrorCode::InvalidValue);
            assert_eq!(error.detail(), expected);
        }
    }

    #[test]
    fn rejects_non_finite_enabled_spill_thresholds() {
        let error = QueryOptions::parse(novarocks::QueryOptions {
            enable_spill: true,
            spill_options: Some(novarocks::SpillOptions {
                spill_mem_limit_threshold: f64::NAN,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect_err("non-finite values cannot enter a canonical contract");

        assert_eq!(error.code(), ContractErrorCode::InvalidValue);
        assert_eq!(error.detail(), "spill_mem_limit_threshold must be finite");
    }

    #[test]
    fn retains_ignored_spill_details_when_spilling_is_disabled() {
        let raw = novarocks::QueryOptions {
            enable_spill: false,
            spill_options: Some(novarocks::SpillOptions {
                spill_mode: 99,
                ..Default::default()
            }),
            ..Default::default()
        };

        let parsed = QueryOptions::parse(raw)
            .expect("disabled spilling preserves the existing ignored-field semantics");
        assert_eq!(parsed.as_proto(), &raw);
    }
}
