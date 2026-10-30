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

//! Move-only COW begin failure custody on the original command worker.

use super::cow_necessary_before_begin;
use novarocks_spi::connector::ConnectorError;
use std::fmt::{self, Write};

// The frozen MySQL startup profile publishes at most 16 KiB of diagnostics.
// A complete UTF-8 boundary can add at most three bytes; terminal byte
// truncation then sees the same prefix as the previous unbounded formatting.
const WIRE_DIAGNOSTIC_BYTES: usize = 16 * 1024;

pub struct CowFailure {
    cause: Cause,
    // Test-only final field observes exit after the original cause fields drop.
    #[cfg(test)]
    exit_probe: Option<ExitProbe>,
    // Only pre-provider failure uses this holder. Checked provider failures
    // already carry the same neutral guard inside their move-only envelope.
    original: Option<novarocks_spi::connector::ConnectorOriginalResultScope>,
}
enum Cause {
    BeforeBegin(cow_necessary_before_begin::Error),
    Lease(ConnectorError),
    Provider(ConnectorError),
    CheckedBegin(novarocks_spi::connector::ConnectorCowBeginFailure),
    OriginalScope(novarocks_spi::connector::OriginalResultCheckError),
    Construction(crate::query_execution::dml::cow_closed_ast::BuildError),
    MissingCatalogIdentity,
}
impl CowFailure {
    fn new(cause: Cause) -> Self {
        Self {
            cause,
            #[cfg(test)]
            exit_probe: None,
            original: None,
        }
    }
    pub(super) fn before_begin(error: cow_necessary_before_begin::Error) -> Self {
        Self::new(Cause::BeforeBegin(error))
    }
    pub(crate) fn lease(error: ConnectorError) -> Self {
        Self::new(Cause::Lease(error))
    }
    pub(crate) fn provider(error: ConnectorError) -> Self {
        Self::new(Cause::Provider(error))
    }
    pub(crate) fn missing_catalog_identity() -> Self {
        Self::new(Cause::MissingCatalogIdentity)
    }

    pub(super) fn construction(
        error: crate::query_execution::dml::cow_closed_ast::BuildError,
        original: novarocks_spi::connector::ConnectorOriginalResultScope,
    ) -> Self {
        let mut failure = Self::new(Cause::Construction(error));
        failure.original = Some(original);
        failure
    }

    pub(crate) fn original_scope(
        error: novarocks_spi::connector::OriginalResultCheckError,
    ) -> Self {
        Self::new(Cause::OriginalScope(error))
    }

    pub(crate) fn checked_begin(error: novarocks_spi::connector::ConnectorCowBeginFailure) -> Self {
        Self::new(Cause::CheckedBegin(error))
    }
    pub(crate) fn missing_catalog_identity_bound(
        original: novarocks_spi::connector::ConnectorOriginalResultScope,
    ) -> Self {
        let mut failure = Self::missing_catalog_identity();
        failure.original = Some(original);
        failure
    }

    // Frontend command boundaries invoke this inside the original synchronous
    // closure, including the fail-closed CTAS mapper for an invalid COW error.
    // The original WorkOwner/window still live in execute_synchronous_stage;
    // no raw cause enters its output receipt.
    pub(crate) fn retire_on_original_worker(
        self,
        terminal: Option<&novarocks_spi::connector::LakePublicationTerminal>,
    ) -> String {
        let presentation = self.borrowed_presentation(terminal);
        drop(self); // Actual raw cause exit precedes publication of this String.
        presentation
    }

    /// The supervised source retains the move-only cause while publishing the
    /// same bounded client projection. This borrows no erased formatter.
    pub(crate) fn borrowed_presentation(
        &self,
        terminal: Option<&novarocks_spi::connector::LakePublicationTerminal>,
    ) -> String {
        let mut out = DiagnosticPrefix(String::with_capacity(WIRE_DIAGNOSTIC_BYTES + 3));
        out.push("Executor: ");
        match &self.cause {
            Cause::Lease(error) => {
                out.push("derive connector write-stack lease: ");
                out.connector(error);
            }
            Cause::Provider(error) => {
                out.push("begin connector write session: ");
                out.connector(error);
            }
            Cause::CheckedBegin(failure) => {
                use novarocks_spi::connector::ConnectorCowBeginCause;
                match failure.cause() {
                    ConnectorCowBeginCause::Provider(error) => {
                        out.push("begin connector write session: ");
                        out.connector(error);
                    }
                    ConnectorCowBeginCause::OriginalResult(error) => {
                        // Scope emits only these closed first-party causes.
                        // Preserve their original presentation without invoking
                        // an erased source formatter or reconstructing cause.
                        let source = std::error::Error::source(error);
                        if let Some(source) =
                            source.and_then(|s| s.downcast_ref::<ConnectorError>())
                        {
                            out.connector(source);
                        } else if let Some(source) = source
                            .and_then(|s| s.downcast_ref::<novarocks_workload_control::WorkError>())
                        {
                            let _ = write!(&mut out, "{source}");
                        } else {
                            let _ = write!(&mut out, "{error}");
                        }
                    }
                }
            }
            Cause::Construction(error) => {
                use crate::query_execution::dml::cow_closed_ast::BuildError;
                match error {
                    BuildError::Original(error) => out.original(error),
                    _ => {
                        let _ = write!(&mut out, "{error}");
                    }
                }
            }
            Cause::OriginalScope(error) => out.original(error),
            Cause::MissingCatalogIdentity => {
                out.push("connector write lease has no immutable catalog runtime identity")
            }
            Cause::BeforeBegin(error) => match error {
                cow_necessary_before_begin::Error::Control(error) => out.connector(error),
                cow_necessary_before_begin::Error::Scope(error) => {
                    // Concrete WorkError is a closed first-party enum. Its
                    // implementation reads only finite enums/static labels.
                    let _ = write!(&mut out, "{error}");
                }
                cow_necessary_before_begin::Error::ExistingCoverage(error) => out.push(error),
                cow_necessary_before_begin::Error::MissingAdmission => {
                    out.push("COW construction requires the original Internal result binding")
                }
            },
        }
        if let Some(terminal) = terminal {
            let target = terminal.target();
            let _ = write!(
                &mut out,
                " (lake publication id={} family={} target={}.{}",
                terminal.header().publication_id(),
                terminal.header().family(),
                target.catalog(),
                target.namespace()
            );
            if let Some(table) = target.table() {
                let _ = write!(&mut out, ".{table}");
            }
            if let Some(reference) = target.reference() {
                let _ = write!(&mut out, "@{reference}");
            }
            let _ = write!(
                &mut out,
                " disposition={:?} next_action={:?} do_not_retry={})",
                terminal.disposition(),
                terminal.next_action(),
                terminal.do_not_retry()
            );
        }

        out.0
    }
    #[cfg(test)]
    pub(crate) fn with_exit_probe(mut self, probe: impl FnOnce() + Send + 'static) -> Self {
        self.exit_probe = Some(ExitProbe(Some(Box::new(probe))));
        self
    }
    #[cfg(test)]
    fn original_connector(&self) -> Option<&ConnectorError> {
        match &self.cause {
            Cause::Lease(e) | Cause::Provider(e) => Some(e),
            Cause::BeforeBegin(cow_necessary_before_begin::Error::Control(e)) => Some(e),
            _ => None,
        }
    }
}
impl fmt::Debug for CowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CowFailure(original cause retained)")
    }
}
impl fmt::Display for CowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("copy-on-write begin failed")
    }
}
impl std::error::Error for CowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Cause::BeforeBegin(error) => Some(error),
            Cause::Lease(error) | Cause::Provider(error) => Some(error),
            Cause::CheckedBegin(error) => Some(error),
            Cause::OriginalScope(error) => Some(error),
            Cause::Construction(error) => Some(error),
            Cause::MissingCatalogIdentity => None,
        }
    }
}
struct DiagnosticPrefix(String);
impl DiagnosticPrefix {
    fn push(&mut self, text: &str) {
        if self.0.len() >= WIRE_DIAGNOSTIC_BYTES {
            return;
        }
        let mut take = (WIRE_DIAGNOSTIC_BYTES - self.0.len()).min(text.len());
        while !text.is_char_boundary(take) {
            take += 1;
        }
        self.0.push_str(&text[..take]);
    }
    fn original(&mut self, error: &novarocks_spi::connector::OriginalResultCheckError) {
        let source = std::error::Error::source(error);
        if let Some(error) = source.and_then(|e| e.downcast_ref::<ConnectorError>()) {
            self.connector(error);
        } else if let Some(error) =
            source.and_then(|e| e.downcast_ref::<novarocks_workload_control::WorkError>())
        {
            let _ = write!(self, "{error}");
        } else {
            let _ = write!(self, "{error}");
        }
    }
    fn connector(&mut self, error: &ConnectorError) {
        // This concrete, sealed Display is audited in connector-contract/error.rs:
        // closed kind + borrowed message + optional private cleanup_context.
        // No arbitrary Error formatter or nested provider formatter is called.
        // The original retryable/table-binding fields remain on error until drop;
        // this slice preserves the existing Internal/publication terminal route.
        let _ = write!(self, "{error}");
    }
}
impl fmt::Write for DiagnosticPrefix {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.push(text);
        Ok(())
    }
}
#[cfg(test)]
struct ExitProbe(Option<Box<dyn FnOnce() + Send>>);
#[cfg(test)]
impl Drop for ExitProbe {
    fn drop(&mut self) {
        if let Some(probe) = self.0.take() {
            probe();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{ConnectorErrorKind, ConnectorTableObjectBindingFailure};
    use std::error::Error;

    #[test]
    fn provider_move_keeps_original_allocation_and_all_typed_fields() {
        let error = ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Replaced,
            "original object replaced",
        )
        .with_cleanup_context("original cleanup context");
        let ptr = error.message().as_ptr();
        let expected = error.clone();
        let failure = CowFailure::provider(error);
        assert_eq!(
            failure.original_connector().unwrap().message().as_ptr(),
            ptr
        );
        assert_eq!(failure.original_connector(), Some(&expected));
        assert!(
            failure
                .source()
                .unwrap()
                .downcast_ref::<ConnectorError>()
                .is_some()
        );
        assert_eq!(
            failure.retire_on_original_worker(None),
            "Executor: begin connector write session: InvalidRequest: original object replaced (cleanup: original cleanup context)"
        );
    }
    #[test]
    fn actual_dml_conversion_keeps_move_only_cause_until_query_claim() {
        let error = ConnectorError::new(ConnectorErrorKind::PermissionDenied, "original denied")
            .with_cleanup_context("provider cleanup");
        let ptr = error.message().as_ptr();
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = exited.clone();
        let failure = CowFailure::provider(error)
            .with_exit_probe(move || probe.store(true, std::sync::atomic::Ordering::SeqCst));
        let mut dml = crate::dml::error::DmlExecutionError::Cow(failure).into_dml_error(None);
        let retained = dml.source().unwrap().downcast_ref::<CowFailure>().unwrap();
        assert_eq!(
            retained.original_connector().unwrap().message().as_ptr(),
            ptr
        );
        assert_eq!(
            retained.original_connector().unwrap().kind(),
            ConnectorErrorKind::PermissionDenied
        );
        assert!(!exited.load(std::sync::atomic::Ordering::SeqCst));
        let failure = dml.take_cow_failure().unwrap();
        assert!(dml.source().is_none());
        assert_eq!(
            failure.original_connector().unwrap().message().as_ptr(),
            ptr
        );
        let message = failure.retire_on_original_worker(None);
        assert!(exited.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            message,
            "Executor: begin connector write session: PermissionDenied: original denied (cleanup: provider cleanup)"
        );
    }
    #[test]
    fn retryable_raw_error_is_not_reconstructed_or_reclassified() {
        let error = ConnectorError::new(ConnectorErrorKind::Unavailable, "provider-first")
            .with_retryable_before_progress();
        let ptr = error.message().as_ptr();
        let failure = CowFailure::lease(error);
        let original = failure.original_connector().unwrap();
        assert_eq!(original.message().as_ptr(), ptr);
        assert_eq!(original.kind(), ConnectorErrorKind::Unavailable);
        assert!(original.retryable_before_progress());
        assert_eq!(
            failure.retire_on_original_worker(None),
            "Executor: derive connector write-stack lease: Unavailable: provider-first"
        );
    }
    #[test]
    fn scope_control_remains_original_work_error_until_retirement() {
        let work = novarocks_workload_control::WorkError::Cancelled(
            novarocks_workload_control::CancellationReason::DeadlineExceeded,
        );
        let failure =
            CowFailure::before_begin(cow_necessary_before_begin::Error::Scope(work.clone()));
        let before = failure
            .source()
            .unwrap()
            .downcast_ref::<cow_necessary_before_begin::Error>()
            .unwrap();
        assert_eq!(
            before
                .source()
                .unwrap()
                .downcast_ref::<novarocks_workload_control::WorkError>(),
            Some(&work)
        );
        assert_eq!(
            failure.retire_on_original_worker(None),
            "Executor: Work is cancelled: DeadlineExceeded"
        );
    }
    #[test]
    fn bounded_unicode_projection_keeps_original_wire_prefix_and_exits_raw_cause() {
        let error = ConnectorError::new(ConnectorErrorKind::Cancelled, "界".repeat(40_000))
            .with_cleanup_context("not in the wire prefix");
        let legacy = format!("Executor: begin connector write session: {error}");
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = exited.clone();
        let failure = CowFailure::provider(error)
            .with_exit_probe(move || observed.store(true, std::sync::atomic::Ordering::SeqCst));
        let message = failure.retire_on_original_worker(None);
        assert!(exited.load(std::sync::atomic::Ordering::SeqCst));
        assert!(message.len() <= WIRE_DIAGNOSTIC_BYTES + 3);
        assert_eq!(
            &message.as_bytes()[..WIRE_DIAGNOSTIC_BYTES],
            &legacy.as_bytes()[..WIRE_DIAGNOSTIC_BYTES]
        );
    }
}
