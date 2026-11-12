// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//! One cleanup phase for the original driver's still-unsealed object journal.
//! The existing Provider policy supplies the independent time bound. The same actor
//! retains the original capabilities and exact journal; the phase grants no
//! writer, Task, publication, capacity or storage authority.

use super::{StagedPathJournal, error};
use crate::access_binding::IcebergReadBinding;
use crate::commit::operation::OperationLimits;
use crate::iceberg::io::FileIO;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
};

pub(super) struct UnsealedWriterCleanup {
    binding: IcebergReadBinding,
    original: ConnectorRequestContext,
    location: String,
    phase: Option<CleanupPhase>,
    stream_cleanup: Option<novarocks_spi::connector::ConnectorIoCleanupFuture>,
}
struct CleanupPhase {
    request: ConnectorRequestContext,
    file_io: FileIO,
    objects_per_turn: usize,
    completed_in_turn: usize,
}
impl UnsealedWriterCleanup {
    pub(super) fn new(
        binding: IcebergReadBinding,
        original: ConnectorRequestContext,
        location: impl Into<String>,
    ) -> Self {
        Self {
            binding,
            original,
            location: location.into(),
            phase: None,
            stream_cleanup: None,
        }
    }
    fn phase(&mut self, journal: &StagedPathJournal) -> &mut CleanupPhase {
        let original = &self.original;
        let binding = &self.binding;
        let location = &self.location;
        self.phase.get_or_insert_with(|| {
            // The original journal already owns this obligation. Activate the
            // existing Provider policy once, not a new Work deadline and not
            // the async actor's diagnostic watchdog.
            let budget = OperationLimits::default().cleanup;
            let request = journal.cleanup_request(original);
            let file_io = crate::fs_io::build_file_io_for_location(
                location,
                binding.for_request(request.clone()),
            );
            CleanupPhase {
                request,
                file_io,
                objects_per_turn: budget.objects,
                completed_in_turn: 0,
            }
        })
    }
    pub(super) async fn run(&mut self, journal: &StagedPathJournal) -> Result<(), ConnectorError> {
        let mut stream_error = None;
        loop {
            if self.stream_cleanup.is_none() {
                self.stream_cleanup = journal.take_stream_cleanup();
            }
            let Some(cleanup) = self.stream_cleanup.as_mut() else {
                break;
            };
            // The original upload must finish aborting before deleting its objects.
            if let Err(error) = cleanup.await {
                stream_error.get_or_insert(error);
            }
            self.stream_cleanup = None;
        }
        while let Some(path) = journal.last_owned() {
            let phase = self.phase(journal);
            phase.request.check_active()?;
            let result = tokio::time::timeout_at(
                phase.request.deadline().into(),
                phase.file_io.delete(&path),
            )
            .await
            .map_err(|_| {
                error(
                    ConnectorErrorKind::DeadlineExceeded,
                    "Iceberg writer cleanup exhausted its original time budget",
                )
            })?;
            result.map_err(|source| {
                error(
                    ConnectorErrorKind::Internal,
                    format!("cleanup staged Iceberg writer file {path}: {source}"),
                )
            })?;
            // Failure, timeout or dropped waiting is not deletion evidence.
            // Only the successful original call removes this exact path.
            journal
                .confirm_deleted(&path)
                .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            phase.completed_in_turn += 1;
            if phase.completed_in_turn == phase.objects_per_turn {
                // The commit owner's object count bounds a cleanup ROUND,
                // not this writer's legitimate total object inventory. Yield
                // between rounds without resetting the one cleanup deadline.
                phase.completed_in_turn = 0;
                tokio::task::yield_now().await;
            }
        }
        if let Some(error) = stream_error {
            return Err(error);
        }
        if let Some((path, original)) = journal.first_stream_exit_failure() {
            return Err(error(
                ConnectorErrorKind::Unavailable,
                format!("Iceberg staged object {path} has unproven stream exit: {original}"),
            ));
        }
        if journal.has_unproven() {
            return Err(error(
                ConnectorErrorKind::Internal,
                "Iceberg staged object ownership remains unproven",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
