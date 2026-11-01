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

//! The ingress resource gate for compiled-package carriers.
//!
//! A process that composed the compiled-package interpreter receives each
//! task's static plan as the exact bytes of one physical package, the
//! `FrozenFragment.package` carrier. Only the backend that wins a task's
//! creation interprets those bytes, but the package's generated resource
//! preflight runs here, for every decoded create and before any owner
//! classifies the operation's identity. A replay is answered without its body
//! ever being prepared, yet it still cannot carry a package past this
//! boundary that the receiver would refuse to allocate for.
//!
//! The gate proves resource bounds only. The one-carrier law, the package's
//! semantics and every compile refusal stay with the winner's interpreter, so
//! a carrier without a package passes here and is refused there. Task codec
//! keeps no plan-codec dependency: the generated model is consulted only by
//! this Native adapter module. A plan-tree process composes no gate.

use std::sync::Arc;

use novarocks_plan_codec::resource_preflight_v2::{
    DecodeProjectionLimits, FragmentDecodeResourceModel, ResourceModelError,
};
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::operation::{DecodedCreateTask, DecodedOperation};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use prost::Message;

/// The generated package preflight over every create's package carrier.
pub struct StaticPackageAdmission {
    model: Arc<FragmentDecodeResourceModel>,
    limits: DecodeProjectionLimits,
}

impl StaticPackageAdmission {
    /// `model` is the process's one decode model, shared with the package
    /// receiver. `limits` must be that receiver's own generated-layout
    /// admission (`PackageDecodeLimits::wire`), so a package this gate admits
    /// is one the winner's first decode step admits too.
    pub fn new(model: Arc<FragmentDecodeResourceModel>, limits: DecodeProjectionLimits) -> Self {
        Self { model, limits }
    }

    /// Bounds the package of every decoded create in one ordinary batch.
    ///
    /// The first refusal refuses the whole batch, exactly as the codec's own
    /// carrier bound does, so no item of it reaches its owner.
    pub(crate) fn admit_ordinary_batch(
        &self,
        operations: &[Option<DecodedOperation>],
        path: FieldPath,
    ) -> Result<(), ProtocolError> {
        for (index, operation) in operations.iter().enumerate() {
            let Some(DecodedOperation::CreateTask(create)) = operation else {
                continue;
            };
            self.admit_create(
                create,
                path.clone()
                    .field("operations")
                    .index(index)
                    .field("create_task")
                    .field("frozen_fragment")
                    .field("package"),
            )?;
        }
        Ok(())
    }

    fn admit_create(
        &self,
        create: &DecodedCreateTask,
        path: FieldPath,
    ) -> Result<(), ProtocolError> {
        // Prost stays the only carrier interpreter, and the package field is
        // generated as a zero-copy view of the received bytes. A carrier it
        // cannot read holds no package to bound; the winner refuses it.
        let Ok(carrier) =
            proto::FrozenFragment::decode(create.input().static_fragment().to_bytes())
        else {
            return Ok(());
        };
        if carrier.package.is_empty() {
            return Ok(());
        }
        self.model
            .preflight(&carrier.package, self.limits, &IngressPreflightControl)
            .map(|_| ())
            .map_err(|error| package_refusal(path, error))
    }
}

/// The package preflight extends the ingress decode, which observes neither a
/// clock nor a cancellation. The existing carrier byte admission bounds this
/// scan; decoder depth and checked arithmetic remain implementation boundaries.
/// Resource projection here grants no allocation and adds no separate quota.
struct IngressPreflightControl;

impl PureCompileControl for IngressPreflightControl {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn package_refusal(path: FieldPath, error: ResourceModelError) -> ProtocolError {
    match error {
        ResourceModelError::Control(cause) => ProtocolError::new(
            path,
            ProtocolErrorKind::CompileControl(cause.clone()),
            cause.to_string(),
        ),
        // A schema refusal is fail-closed rather than waved through: the
        // model that admitted nothing cannot admit this package either.
        other => ProtocolError::new(
            path,
            ProtocolErrorKind::OutOfRange,
            format!("fragment package exceeds its receiver resource admission: {other}"),
        ),
    }
}
