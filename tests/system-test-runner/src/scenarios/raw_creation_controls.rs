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

//! Direct Native observation for the existing raw fixture's original identity.
use super::RawBackendSession;
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::test_sessions::{
    ClusterTestSessionPlan, RoleLaunchReference, RoleSlot,
};
use novarocks_failpoint::{session::DomainGrant, target_binding::RoleProcessReceipt};
use novarocks_proto_models::common;
use novarocks_types::{BackendProcessId, FrontendProcessId};
use serde_json::json;
use std::{path::Path, sync::Mutex, time::Instant};

struct Frozen {
    identity: (common::UniqueId, FrontendProcessId),
    reference: RoleLaunchReference,
    deadline: Instant,
    selected: bool,
}
#[derive(Default)]
pub(super) struct RawCreationObservationOwner(Mutex<Option<Frozen>>);
impl RawCreationObservationOwner {
    pub fn freeze(
        &self,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("raw creation observation owner poisoned"))?;
        ensure!(owner.is_none(), "raw creation observation already frozen");
        ensure!(cluster_size == 3, "raw creation requires native 1FE+3BE");
        let controlled = binary;
        // Allocate through the authoritative fixture producer, exactly once.
        let identity = RawBackendSession::declare_identity();
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let root = plan.reserve_transport("native-raw-observations")?;
        let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
        plan.authorize_ordinary_launch(&frontend, binary.to_path_buf())?;
        let backend = plan.reserve_launch(RoleSlot::Backend(0))?;
        let parameters = json!({"action":novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),"root":root,"backend_index":0,
            "target_session_id":backend.session_id,"max_events":16384,
            "query_execution":{"query_id_hi":identity.0.hi,"query_id_lo":identity.0.lo,"attempt_id":1}});
        ensure!(
            serde_json::to_vec(&parameters)?.len() <= 4096,
            "raw Native observation parameters exceed bounded input"
        );
        plan.authorize_launch(
            &backend,
            vec![DomainGrant {
                domain: "native-observation".into(),
                grant_id: "raw-grpc-q1".into(),
                parameters,
            }],
            vec![
                ("novarocks-server".into(), "test-controls".into()),
                ("novarocks-native-adapter".into(), "test-controls".into()),
            ],
        )?;
        plan.set_launch_binary(&backend, controlled.to_path_buf())?;
        for index in 1..cluster_size {
            let reference = plan.reserve_launch(RoleSlot::Backend(index))?;
            plan.authorize_ordinary_launch(&reference, binary.to_path_buf())?;
        }
        plan.validate_selected_intents(cluster_size)?;
        *owner = Some(Frozen {
            identity,
            reference: backend,
            deadline,
            selected: false,
        });
        Ok(plan)
    }
    pub fn take_identity(
        &self,
        deadline: Instant,
    ) -> Result<(common::UniqueId, FrontendProcessId)> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("raw creation observation owner poisoned"))?;
        let frozen = owner
            .as_mut()
            .context("raw creation observation was not frozen before launch")?;
        ensure!(!frozen.selected, "raw creation identity already selected");
        ensure!(
            frozen.deadline == deadline && Instant::now() < deadline,
            "raw creation original deadline changed or expired"
        );
        frozen.selected = true;
        Ok(frozen.identity)
    }
    pub fn verify_backend(&self, actual: BackendProcessId) -> Result<()> {
        let owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("raw creation observation owner poisoned"))?;
        let frozen = owner
            .as_ref()
            .context("raw creation observation was not frozen")?;
        ensure!(
            Instant::now() < frozen.deadline,
            "raw creation original deadline expired"
        );
        let receipt = RoleProcessReceipt::read_private(&frozen.reference.process_receipt)?;
        ensure!(
            receipt.session_id == frozen.reference.session_id,
            "raw creation receipt session mismatch"
        );
        ensure!(
            receipt.backend_process()? == actual,
            "raw creation target differs from its actual declared backend process"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_fixture_query_is_declared_once_and_process_refinement_is_exact() {
        let binary = std::env::current_exe().unwrap();
        let owner = RawCreationObservationOwner::default();
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let plan = owner.freeze(3, &binary, deadline).unwrap();
        let original = owner.0.lock().unwrap().as_ref().unwrap().identity;
        assert_eq!(original.0.hi, 0x5b5b);
        assert_eq!(owner.take_identity(plan.deadline()).unwrap(), original);
        assert!(owner.take_identity(plan.deadline()).is_err());
        let process = BackendProcessId::new_v7();
        let reference = owner.0.lock().unwrap().as_ref().unwrap().reference.clone();
        RoleProcessReceipt {
            schema_version: 1,
            session_id: reference.session_id.clone(),
            launch_id: "raw-test-launch".into(),
            role: novarocks_failpoint::session::SessionRole::Be,
            process_id: process.to_string(),
        }
        .write_private(&reference.process_receipt)
        .unwrap();
        assert!(owner.verify_backend(process).is_ok());
        assert!(owner.verify_backend(BackendProcessId::new_v7()).is_err());
    }
}
