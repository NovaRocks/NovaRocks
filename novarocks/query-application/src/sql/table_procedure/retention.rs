// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::Span;
use novarocks_spi::connector::ConnectorMaintenanceDuration;
use novarocks_type_contract::{CompilePhase, MAX_UNOBSERVED_COMPILE_WORK, PureCompileControl};

use super::{QueryServiceError, invalid_procedure_argument};

/// Duration decoded under the original table-procedure argument budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcedureRetentionDuration(ConnectorMaintenanceDuration);

impl ProcedureRetentionDuration {
    pub fn parse(
        source: &str,
        span: Span,
        text: &str,
        control: &dyn PureCompileControl,
    ) -> Result<Self, QueryServiceError> {
        let checkpoint = |units| {
            control
                .checkpoint(CompilePhase::Validate, units)
                .map_err(QueryServiceError::from_compile_control)
        };
        let invalid = || {
            invalid_procedure_argument(
                source,
                span,
                "Invalid table procedure retention_threshold duration",
            )
        };
        checkpoint(0)?;
        for chunk in text.as_bytes().chunks(MAX_UNOBSERVED_COMPILE_WORK as usize) {
            let ascii = chunk.is_ascii();
            checkpoint(chunk.len() as u32)?;
            if !ascii {
                return Err(invalid());
            }
        }
        let duration = ConnectorMaintenanceDuration::parse(text);
        checkpoint(0)?;
        duration.map(Self).ok_or_else(invalid)
    }

    pub fn rounded_millis(self) -> Option<i64> {
        self.0.rounded_millis()
    }

    pub const fn connector_duration(self) -> ConnectorMaintenanceDuration {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_validation_preserves_the_original_control_refusal() {
        use novarocks_type_contract::CompileControlError;

        struct RefuseWork;
        impl PureCompileControl for RefuseWork {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::Validate);
                if units == 0 {
                    Ok(())
                } else {
                    Err(CompileControlError::ResourceExhausted)
                }
            }
        }

        let source = "unsupported duration";
        let error = ProcedureRetentionDuration::parse(
            source,
            Span::new(0, source.len()),
            source,
            &RefuseWork,
        )
        .unwrap_err();
        assert_eq!(
            error.compile_control_error(),
            Some(CompileControlError::ResourceExhausted),
        );
    }

    #[test]
    fn duration_units_preserve_reference_numeric_and_whitespace_rules() {
        for (text, millis) in [
            ("7d", Some(604_800_000)),
            (" 1.5 h \t", Some(5_400_000)),
            ("500us", Some(1)),
            ("499999ns", Some(0)),
            ("+1e-3s", Some(1)),
            ("1.e0s", Some(1000)),
            ("0x1.8p2fms", Some(6)),
            ("\0-0x1p-1075s", Some(0)),
            ("\0-0x1.0000000000001p-1075s", None),
            ("+1e0s ", None),
            ("0x1p2ms ", None),
            ("-1d", None),
            ("1h\u{a0}", None),
            ("１s", None),
            ("1H", None),
            ("1e999d", None),
            ("9223372036854775808ms", Some(i64::MAX)),
            ("9223372036854777856ms", None),
        ] {
            assert_eq!(
                ConnectorMaintenanceDuration::parse(text)
                    .and_then(ConnectorMaintenanceDuration::rounded_millis),
                millis,
                "{text:?}"
            );
        }
    }
}
