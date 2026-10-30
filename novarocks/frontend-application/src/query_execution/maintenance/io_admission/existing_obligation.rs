// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! A weak continuation of this MIO's registered metadata obligation.
//! It is not a publication verdict, grant, or replacement physical owner.

use super::MetadataIoAdmission;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorOperationControlView, ConnectorRequestContext,
    ConnectorStopView,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    time::Instant,
};

#[derive(Clone)]
pub(crate) struct ExistingMetadataObligation {
    source: Weak<MetadataIoAdmission>,
    control: ConnectorOperationControlView,
    stop: ConnectorStopView,
}
impl ExistingMetadataObligation {
    pub(super) fn capture(
        source: &Arc<MetadataIoAdmission>,
        root: &WorkScope,
        context: &ConnectorRequestContext,
    ) -> Result<Self, SharedFailureCause> {
        source
            .check_existing_metadata_obligation_context(root, context)
            .map_err(admission_failure)?;
        let token = Self {
            source: Arc::downgrade(source),
            control: context.original_operation_control(),
            stop: context.stop().clone(),
        };
        token.check_active(root)?;
        Ok(token)
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.control.deadline()
    }

    pub(crate) fn check_active(&self, root: &WorkScope) -> Result<(), SharedFailureCause> {
        self.control.check_active().map_err(|cause| {
            SharedFailureCause::from_static_detail(cause, "existing metadata obligation stopped")
        })?;
        let source = self
            .source
            .upgrade()
            .ok_or_else(|| admission_failure(WorkError::Released))?;
        // Recheck the original positive incarnation and exact Root on every
        // phase. A retained token or successful UI result is not authority.
        source
            .check_existing_metadata_obligation(root)
            .map_err(admission_failure)
    }

    pub(crate) fn stopped(&self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        self.stop.stopped()
    }
}

fn admission_failure(cause: WorkError) -> SharedFailureCause {
    SharedFailureCause::from_static_detail(cause, "existing metadata obligation admission refused")
}
