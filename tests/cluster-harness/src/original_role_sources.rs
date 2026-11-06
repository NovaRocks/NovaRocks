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

//! Immutable observations borrow only the original live role/log owners.
use super::{BackendTopologyRow, process_resources};
use super::{CrossProcessCluster, TOPOLOGY_IO_TIMEOUT_CAP, query_frontend_backend_topology};
use anyhow::{Context, Result, ensure};
use novarocks_types::BackendProcessId;
use std::time::Instant;

impl CrossProcessCluster {
    /// One original FE durable snapshot, checked before and after the caller's visit.
    pub fn with_original_frontend_log_snapshot<T>(
        &self,
        original_deadline: Instant,
        visitor: impl FnOnce(&mut dyn std::io::Read, u64) -> Result<T>,
    ) -> Result<T> {
        ensure!(
            Instant::now() < original_deadline,
            "original FE log clock expired before snapshot"
        );
        ensure!(
            self.fe_log_history.is_empty(),
            "original role observer refuses FE replacement history"
        );
        let process = &self.fe_process;
        let identity = &self.fe_launch_identity;
        ensure!(
            process.is_running()? && process.pid() == identity.pid,
            "original role observer requires original live FE child"
        );
        process_resources::recheck_process_launch_identity(identity)?;
        let result = process
            .log_source()
            .with_bounded_snapshot_reader(super::root_observation_identity::SCAN_BYTES, visitor);
        let after = (|| -> Result<()> {
            ensure!(
                process.is_running()?,
                "original FE exited during log snapshot"
            );
            process_resources::recheck_process_launch_identity(identity)?;
            ensure!(
                Instant::now() < original_deadline,
                "original FE log clock expired after snapshot"
            );
            Ok(())
        })();
        match (result, after) {
            (Err(primary), Err(secondary)) => Err(primary.context(secondary)),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(_), Err(secondary)) => Err(secondary),
            (Ok(value), Ok(())) => Ok(value),
        }
    }
    /// One strict live descriptor inventory; no drain fallback and no root/task request.
    #[cfg(unix)]
    pub fn original_backend_process_ids(
        &self,
        original_deadline: Instant,
    ) -> Result<[novarocks_types::BackendProcessId; 3]> {
        ensure!(
            self.be_processes.len() == 3
                && self.be_launch_identities.len() == 3
                && self.be_grpc_ports.len() == 3
                && self.be_log_history.len() == 3
                && self.fe_log_history.is_empty()
                && self.be_log_history.iter().all(String::is_empty),
            "exact root observer requires three original backend owners without replacement history"
        );
        ensure!(
            Instant::now() < original_deadline,
            "original root descriptor clock expired before query"
        );
        let before = self.recheck_live_process_launch_identities()?;
        let timeout = original_deadline
            .saturating_duration_since(Instant::now())
            .min(TOPOLOGY_IO_TIMEOUT_CAP);
        ensure!(
            !timeout.is_zero(),
            "original root descriptor clock expired before query"
        );
        let rows =
            query_frontend_backend_topology(&self.target_host, self.runtime.fe_http_port, timeout)?;
        ensure!(
            Instant::now() < original_deadline,
            "original root descriptor clock expired after query"
        );
        let processes = original_backend_inventory(&rows, &self.be_grpc_ports)?;
        ensure!(
            self.recheck_live_process_launch_identities()? == before,
            "original role instance changed during descriptor query"
        );
        ensure!(
            Instant::now() < original_deadline,
            "original root descriptor clock expired after role recheck"
        );
        Ok(processes)
    }

    /// Visits only an original backend's already-pinned durable log, no all-log/history API.
    #[cfg(unix)]
    pub fn with_original_backend_log_snapshot<T>(
        &self,
        index: usize,
        max_bytes: u64,
        original_deadline: Instant,
        visitor: impl FnOnce(&mut dyn std::io::Read, u64) -> Result<T>,
    ) -> Result<T> {
        ensure!(
            self.be_processes.len() == 3
                && self.be_launch_identities.len() == 3
                && index < 3
                && self.be_log_history.len() == 3
                && self.be_log_history[index].is_empty(),
            "original backend log owner was replaced or is missing"
        );
        ensure!(
            max_bytes == 2 * 1024 * 1024,
            "original backend log observation cap changed"
        );
        ensure!(
            Instant::now() < original_deadline,
            "original root log clock expired before snapshot"
        );
        let process = &self.be_processes[index];
        let identity = &self.be_launch_identities[index];
        ensure!(
            process.pid() == identity.pid && process.is_running()?,
            "original backend log process is not the frozen live child"
        );
        process_resources::recheck_process_launch_identity(identity)?;
        let result = process
            .log_source()
            .with_bounded_snapshot_reader(max_bytes, visitor);
        let after = (|| -> Result<()> {
            ensure!(
                process.is_running()?,
                "original backend exited during log snapshot"
            );
            process_resources::recheck_process_launch_identity(identity)?;
            ensure!(
                Instant::now() < original_deadline,
                "original root log clock expired after snapshot"
            );
            Ok(())
        })();
        match (result, after) {
            (Err(primary), Err(secondary)) => Err(primary.context(secondary)),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(_), Err(secondary)) => Err(secondary),
            (Ok(value), Ok(())) => Ok(value),
        }
    }
}

fn original_backend_inventory(
    rows: &[BackendTopologyRow],
    ports: &[u16],
) -> Result<[BackendProcessId; 3]> {
    ensure!(
        ports.len() == 3 && ports[0] != ports[1] && ports[0] != ports[2] && ports[1] != ports[2],
        "original backend endpoint inventory is not three distinct endpoints"
    );
    ensure!(
        rows.iter().filter(|row| row.is_eligible_live()).count() == 3,
        "exact root observer requires exactly three actual live eligible descriptors"
    );
    let mut processes = [None; 3];
    for (index, port) in ports.iter().enumerate() {
        let mut matches = rows
            .iter()
            .filter(|row| row.grpc_port == *port && row.is_eligible_live());
        let row = matches
            .next()
            .context("actual original backend has no live eligible descriptor")?;
        ensure!(
            matches.next().is_none(),
            "actual original backend endpoint has multiple live descriptors"
        );
        let process: BackendProcessId = row.process_id.parse()?;
        ensure!(
            process.to_string() == row.process_id && !processes.contains(&Some(process)),
            "actual original backend UUID is noncanonical or duplicate"
        );
        processes[index] = Some(process);
    }
    Ok([
        processes[0].context("missing first backend")?,
        processes[1].context("missing second backend")?,
        processes[2].context("missing third backend")?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rows() -> Vec<BackendTopologyRow> {
        (0..3)
            .map(|index| BackendTopologyRow {
                process_id: BackendProcessId::new_v7().to_string(),
                grpc_port: 19000 + index,
                state: "Live".to_owned(),
                alive: true,
                scheduled_fragments: 0,
                build_identity: "actual-build".to_owned(),
                native_compatibility_id: "actual-island".to_owned(),
                status_detail: String::new(),
            })
            .collect()
    }
    #[test]
    fn descriptor_inventory_uses_actual_endpoints_in_launch_order() {
        let mut rows = rows();
        rows.reverse();
        let ids = original_backend_inventory(&rows, &[19000, 19001, 19002]).unwrap();
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(
                id.to_string(),
                rows.iter()
                    .find(|row| row.grpc_port == 19000 + index as u16)
                    .unwrap()
                    .process_id
            );
        }
    }
    #[test]
    fn missing_extra_duplicate_or_unhealthy_descriptors_refuse() {
        for alteration in 0..9 {
            let mut rows = rows();
            let mut ports = [19000, 19001, 19002];
            match alteration {
                0 => {
                    rows.pop();
                }
                1 => {
                    rows.push(rows[0].clone());
                }
                2 => rows[1].process_id = rows[0].process_id.clone(),
                3 => rows[1].grpc_port = rows[0].grpc_port,
                4 => rows[1].state = "Draining".to_owned(),
                5 => rows[1].alive = false,
                6 => rows[1].status_detail = "not eligible".to_owned(),
                7 => ports[1] = ports[0],
                _ => rows[1].process_id = "01900000-0000-7000-8000-00000000000A".to_owned(),
            }
            assert!(
                original_backend_inventory(&rows, &ports).is_err(),
                "alteration={alteration}"
            );
        }
    }
}
