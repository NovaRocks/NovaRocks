// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to you under the
// Apache License, Version 2.0.

//! Provider-neutral terminal transaction handoff for distributed writers.
//!
//! The query coordinator owns collection and completeness validation.  Once
//! it produces a [`ConnectorWriteCompletion`], this module is the sole core
//! path that invokes the retained FE control capability.  It deliberately
//! returns the SPI outcome unchanged: provider-specific journal mapping is a
//! caller concern and must not reintroduce an Iceberg carrier into core.

use novarocks_spi::connector::{ConnectorError, ConnectorWriteReceipt, ExternalMutationOutcome};

use crate::query_execution::outcome::ConnectorWriteCompletion;

/// Commit a complete staged writer manifest through exactly the FE generation
/// that planned it.  No registry lookup, generation substitution, or payload
/// reconstruction is permitted here.
pub(crate) fn commit(
    completion: &ConnectorWriteCompletion,
) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
    completion
        .session()
        .commit(completion.commit_context().clone())
}
