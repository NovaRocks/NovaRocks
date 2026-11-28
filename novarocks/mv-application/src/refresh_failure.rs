// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

/// Product disposition of a materialized-view refresh failure.
///
/// The caller retains the exact original failure and uses this classification
/// to choose its publication, recovery or retry action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRefreshFailureClass {
    InconsistentContent,
    IncompleteEvidence,
    PublicationNotApplied,
    AdmissionRefused,
    NotStarted,
    MarkerOutcomeUnknown,
    TemporaryShortage,
    BindingChanged,
    RecoveryUnavailable,
    InvalidDataOrFormat,
}
