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

//! Statement-local statistics publication facts.
use std::fmt;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationFact {
    /// The Provider completed without applying an external mutation.
    NoOp,
    KnownUncommitted,
    KnownCommitted,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsFailure {
    pub error: novarocks_query_application::session_error::QueryServiceError,
}

/// One bounded observation, not the originating Provider resource owner.
/// The actual Session keeps that original cause until its real close.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct StatisticsPublicationFailure {
    fact: novarocks_failure_contract::FailureFact,
    compile_control: Option<novarocks_type_contract::CompileControlProvenance>,
    diagnostic: [u8; novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES],
    diagnostic_len: u16,
}
impl StatisticsPublicationFailure {
    pub const PUBLIC_MESSAGE_BYTES: usize =
        "statistics publication failed (reason 65535:65535)".len();
    pub fn public_message(&self) -> String {
        use fmt::Write;
        let mut message = String::with_capacity(Self::PUBLIC_MESSAGE_BYTES);
        write!(&mut message, "{self}").expect("fixed String write");
        debug_assert!(message.len() <= Self::PUBLIC_MESSAGE_BYTES);
        message
    }

    /// Called outside repository locks while borrowing the actual Provider
    /// outcome. Formatting is diagnostic only and cannot change its facts.
    pub fn capture(
        fact: novarocks_failure_contract::FailureFact,
        compile_control: Option<novarocks_type_contract::CompileControlProvenance>,
        diagnostic: &dyn fmt::Display,
    ) -> Self {
        struct Writer<'a> {
            bytes: &'a mut [u8],
            len: usize,
        }
        impl fmt::Write for Writer<'_> {
            fn write_str(&mut self, text: &str) -> fmt::Result {
                let mut len = text.len().min(self.bytes.len() - self.len);
                while !text.is_char_boundary(len) {
                    len -= 1;
                }
                self.bytes[self.len..self.len + len].copy_from_slice(&text.as_bytes()[..len]);
                self.len += len;
                if len == text.len() {
                    Ok(())
                } else {
                    Err(fmt::Error)
                }
            }
        }
        let mut record = Self {
            fact,
            compile_control,
            diagnostic: [0; novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES],
            diagnostic_len: 0,
        };
        let mut writer = Writer {
            bytes: &mut record.diagnostic,
            len: 0,
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = fmt::write(&mut writer, format_args!("{diagnostic}"));
        }));
        record.diagnostic_len = writer.len as u16;
        record
    }
    pub const fn fact(&self) -> novarocks_failure_contract::FailureFact {
        self.fact
    }
    pub const fn compile_control(
        &self,
    ) -> Option<novarocks_type_contract::CompileControlProvenance> {
        self.compile_control
    }
    /// Private process diagnostics. Never pass this text to SHOW or SQL errors.
    pub fn local_diagnostic(&self) -> &str {
        std::str::from_utf8(&self.diagnostic[..usize::from(self.diagnostic_len)])
            .expect("bounded formatter copied complete UTF-8 characters")
    }
}
impl fmt::Display for StatisticsPublicationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "statistics publication failed (reason {})",
            self.fact.reason()
        )
    }
}
impl fmt::Debug for StatisticsPublicationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Even incidental Debug does not publish the private diagnostic.
        f.debug_struct("StatisticsPublicationFailure")
            .field("fact", &self.fact)
            .field("compile_control", &self.compile_control)
            .finish()
    }
}

/// Only legal publication combinations cross the product boundary. Pre-dispatch
/// failure is StatisticsAttemptError, never a fabricated publication verdict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationOutcome {
    NoOp {
        finalization_failure: Option<StatisticsPublicationFailure>,
        cleanup_failure: Option<StatisticsPublicationFailure>,
    },
    KnownCommitted {
        finalization_failure: Option<StatisticsPublicationFailure>,
        cleanup_failure: Option<StatisticsPublicationFailure>,
    },
    KnownUncommitted {
        failure: StatisticsPublicationFailure,
        cleanup_failure: Option<StatisticsPublicationFailure>,
    },
    CommitUnknown {
        failure: StatisticsPublicationFailure,
    },
}
impl StatisticsPublicationOutcome {
    pub const fn fact(&self) -> StatisticsPublicationFact {
        match self {
            Self::NoOp { .. } => StatisticsPublicationFact::NoOp,
            Self::KnownCommitted { .. } => StatisticsPublicationFact::KnownCommitted,
            Self::KnownUncommitted { .. } => StatisticsPublicationFact::KnownUncommitted,
            Self::CommitUnknown { .. } => StatisticsPublicationFact::CommitUnknown,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsAttemptError {
    Failed(StatisticsFailure),
    Stale(StatisticsFailure),
    Cancelled(StatisticsFailure),
}

impl StatisticsAttemptError {
    pub(crate) fn failure(&self) -> StatisticsFailure {
        match self {
            Self::Failed(e) | Self::Stale(e) | Self::Cancelled(e) => e.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn bounded_publication_observation_keeps_facts_but_not_original_custody() {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, FailureRowPosition,
            OuterFailureProjection, ResourceDimension, ResourceFacts,
        };
        use novarocks_type_contract::{CompileControlError, CompileControlProvenance};
        use std::num::NonZeroU16;
        #[derive(Debug)]
        struct Original {
            fact: FailureFact,
            dropped: Arc<AtomicUsize>,
        }
        impl fmt::Display for Original {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("private-token=secret;")?;
                for _ in 0..1024 {
                    f.write_str("原")?;
                }
                Ok(())
            }
        }
        impl std::error::Error for Original {}
        impl FailureProjection for Original {
            fn failure_fact(&self) -> FailureFact {
                self.fact
            }
        }
        impl OuterFailureProjection for Original {}
        impl Drop for Original {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let fact = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(
                NonZeroU16::new(u16::MAX).unwrap(),
                NonZeroU16::new(u16::MAX).unwrap(),
            ),
        )
        .with_resource(ResourceFacts {
            request_scope: None,
            constraint_scope: None,
            dimension: ResourceDimension::Bytes,
            requested: Some(99),
            required_growth: Some(9),
            limit: Some(90),
            live: Some(81),
            committed: Some(3),
        })
        .with_row_position(FailureRowPosition {
            selected_ordinal: 7,
            batch_row: 11,
        });
        let dropped = Arc::new(AtomicUsize::new(0));
        let original = CompileControlError::from_domain(Original {
            fact,
            dropped: dropped.clone(),
        });
        let record = StatisticsPublicationFailure::capture(
            original.failure_fact(),
            Some(original.provenance()),
            &original,
        );
        assert_eq!(record.fact(), fact);
        assert_eq!(
            record.compile_control(),
            Some(CompileControlProvenance::Source(fact))
        );
        assert!(record.local_diagnostic().contains("private-token=secret"));
        assert!(
            record.local_diagnostic().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES
        );
        assert!(record.local_diagnostic().ends_with('原'));
        assert!(!record.public_message().contains("secret"));
        assert!(!format!("{record:?}").contains("secret"));
        assert!(
            record.public_message().len() <= StatisticsPublicationFailure::PUBLIC_MESSAGE_BYTES
        );
        let historical = record;
        drop(original);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "history must not pin the Provider tree"
        );
        assert_eq!(historical.fact(), fact);
        assert_eq!(
            historical.compile_control(),
            Some(CompileControlProvenance::Source(fact))
        );
    }

    #[test]
    fn diagnostic_formatter_panic_cannot_change_publication_facts() {
        use novarocks_failure_contract::FailureProjection;
        struct PanickingDiagnostic;
        impl fmt::Display for PanickingDiagnostic {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("private-prefix")?;
                panic!("diagnostic formatter failure")
            }
        }
        let original = novarocks_type_contract::CompileControlError::DeadlineExceeded;
        let record = StatisticsPublicationFailure::capture(
            original.failure_fact(),
            Some(original.provenance()),
            &PanickingDiagnostic,
        );
        assert_eq!(record.fact(), original.failure_fact());
        assert_eq!(record.compile_control(), Some(original.provenance()));
        assert_eq!(record.local_diagnostic(), "private-prefix");
        assert!(!record.public_message().contains("private"));
    }
}
