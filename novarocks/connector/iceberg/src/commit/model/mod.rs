// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Immutable logical intent and complete, single-owner publication requests.

mod artifacts;
mod dependency;
mod fields;
mod identity;
mod intent;
mod outcome;
mod request;

pub use artifacts::{
    ArtifactClass, ArtifactKind, ArtifactLedger, ArtifactRecord, ArtifactWriteState,
    ArtifactWriter, AttemptArtifacts, CleanupScope,
};
pub use dependency::{Dependency, ValidationInput};
pub use fields::{AddedContent, EntryFacts, FrozenEntry, SeqField};
pub use identity::{
    AttemptToken, EntryIdentity, ObjectIdentity, OperationAuthority, OperationToken,
};
pub use intent::{
    FileChanges, IsolationLevel, OperationIntent, OperationIntentParts, StartSnapshot, TableTarget,
};
pub use outcome::{
    CleanupRemainingReason, IcebergCleanupReport, PublicationOutcome, PublicationReport,
    RemainingArtifact,
};
pub use request::{
    BaseIdentity, FrozenRequest, FrozenRequestParts, RequestShape, StagedCreateIdentity,
};

fn invalid(message: impl Into<String>) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}
