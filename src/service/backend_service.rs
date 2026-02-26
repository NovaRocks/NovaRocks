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
use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use threadpool::ThreadPool;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryInputProtocolFactory, TBinaryOutputProtocol,
    TBinaryOutputProtocolFactory, TInputProtocolFactory, TOutputProtocolFactory,
};
use thrift::server::TProcessor;
use thrift::transport::{
    TBufferedReadTransport, TBufferedReadTransportFactory, TBufferedWriteTransport,
    TBufferedWriteTransportFactory, TIoChannel, TReadTransportFactory, TTcpChannel,
    TWriteTransportFactory,
};

use crate::common::thrift::thrift_named_json;
use crate::connector::starrocks::lake::{create_lake_tablet_from_req, execute_alter_tablet_task};
use crate::frontend_service::{FrontendServiceSyncClient, TFrontendServiceSyncClient};
use crate::master_service;
use crate::novarocks_config::config as novarocks_app_config;
use crate::runtime::starlet_shard_registry;
use crate::service::{disk_report, stream_load};
use crate::{
    agent_service,
    backend_service::{
        BackendServiceSyncHandler, BackendServiceSyncProcessor, TExportTaskRequest,
        TGetTabletsInfoRequest, TGetTabletsInfoResult, TRoutineLoadTask, TStreamLoadChannel,
        TTabletStatResult,
    },
    internal_service, starrocks_external_service,
    status::TStatus,
    status_code::TStatusCode,
    types,
};

#[derive(Debug, Clone)]
pub struct BackendServiceConfig {
    pub host: String,
    pub be_port: u16,
}

#[derive(Clone, Debug)]
struct BackendHandler {
    peer: Option<std::net::SocketAddr>,
}

fn stub_status(method: &str) -> TStatus {
    TStatus::new(
        TStatusCode::NOT_IMPLEMENTED_ERROR,
        Some(vec![format!("novarocks BackendService stub: {method}")]),
    )
}

fn ok_status() -> TStatus {
    TStatus::new(TStatusCode::OK, None)
}

fn internal_error_status(message: String) -> TStatus {
    TStatus::new(TStatusCode::INTERNAL_ERROR, Some(vec![message]))
}

fn next_report_version() -> i64 {
    static REPORT_VERSION: AtomicI64 = AtomicI64::new(1);
    REPORT_VERSION.fetch_add(1, Ordering::AcqRel)
}

const CREATE_TABLET_ADD_SHARD_WAIT_MS: u64 = 1_500;
const CREATE_TABLET_ADD_SHARD_POLL_MS: u64 = 25;
const ALTER_FAILFAST_ERROR_REPORT_TIMES: usize = 3;

fn submit_task_worker_pool() -> &'static ThreadPool {
    static POOL: OnceLock<ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| ThreadPool::with_name("BackendService submit_task".to_string(), 8))
}

fn json_summary<T: thrift::protocol::TSerializable>(v: &T) -> String {
    match thrift_named_json(v) {
        Ok(s) => {
            if s.len() > 2048 {
                format!("{}...<truncated>", &s[..2048])
            } else {
                s
            }
        }
        Err(e) => format!("<json_error:{e}>"),
    }
}

fn build_backend_for_finish_task() -> Result<types::TBackend, String> {
    let cfg = novarocks_app_config().map_err(|e| e.to_string())?;
    let mut host = cfg.server.host.trim().to_string();
    if host.is_empty() || host == "0.0.0.0" {
        host = "127.0.0.1".to_string();
    }
    Ok(types::TBackend::new(
        host,
        cfg.server.be_port as i32,
        cfg.server.http_port as i32,
    ))
}

fn send_finish_task(
    task: &agent_service::TAgentTaskRequest,
    task_status: TStatus,
) -> Result<(), String> {
    let fe_addr = disk_report::latest_fe_addr().ok_or_else(|| {
        "missing FE address for finish_task (heartbeat not received yet)".to_string()
    })?;
    let addr: SocketAddr = format!("{}:{}", fe_addr.hostname, fe_addr.port)
        .parse()
        .map_err(|e| format!("invalid FE address for finish_task: {e}"))?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|e| format!("connect FE finish_task failed: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan) = channel
        .split()
        .map_err(|e| format!("split thrift channel for finish_task failed: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_trans, true);
    let o_prot = TBinaryOutputProtocol::new(o_trans, true);
    let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);

    let report_version = next_report_version();
    let request = master_service::TFinishTaskRequest::new(
        build_backend_for_finish_task()?,
        task.task_type,
        task.signature,
        task_status,
        Some(report_version),
        None::<Vec<master_service::TTabletInfo>>,
        None::<i64>,
        None::<i64>,
        None::<i64>,
        None::<String>,
        None::<Vec<types::TTabletId>>,
        None::<Vec<String>>,
        None::<BTreeMap<types::TTabletId, Vec<String>>>,
        None::<Vec<types::TTabletId>>,
        None::<i64>,
        None::<i64>,
        None::<Vec<master_service::TTabletVersionPair>>,
        None::<Vec<master_service::TTabletVersionPair>>,
        None::<types::TSnapshotInfo>,
    );
    tracing::debug!(
        signature = task.signature,
        task_type = ?task.task_type,
        report_version = ?request.report_version,
        req = %json_summary(&request),
        "BackendService.finish_task sending request"
    );
    let result = client.finish_task(request).map_err(|e| {
        format!(
            "FE finish_task rpc failed for signature={}: {e}",
            task.signature
        )
    })?;
    if result.status.status_code != TStatusCode::OK {
        return Err(format!(
            "FE finish_task returned non-OK for signature={}: {:?}",
            task.signature, result.status
        ));
    }
    tracing::debug!(
        signature = task.signature,
        task_type = ?task.task_type,
        report_version,
        "BackendService.finish_task reported"
    );
    Ok(())
}

fn execute_create_tablet_task(task: &agent_service::TAgentTaskRequest) -> Result<(), String> {
    let req = task
        .create_tablet_req
        .as_ref()
        .ok_or_else(|| "create_tablet task missing create_tablet_req".to_string())?;
    let tablet_type = req
        .tablet_type
        .unwrap_or(agent_service::TTabletType::TABLET_TYPE_DISK);
    if tablet_type != agent_service::TTabletType::TABLET_TYPE_LAKE {
        return Err(format!(
            "unsupported create_tablet tablet_type={:?} for tablet_id={} (only TABLET_TYPE_LAKE is supported)",
            tablet_type, req.tablet_id
        ));
    }

    let shard_info = wait_for_starlet_add_shard(req.tablet_id).ok_or_else(|| {
        format!(
            "missing shard path from Starlet AddShard cache for create_tablet tablet_id={} after waiting {}ms",
            req.tablet_id, CREATE_TABLET_ADD_SHARD_WAIT_MS
        )
    })?;
    create_lake_tablet_from_req(req, &shard_info.full_path, shard_info.s3)
}

fn execute_alter_task(task: &agent_service::TAgentTaskRequest) -> Result<(), String> {
    let req = task
        .alter_tablet_req_v2
        .as_ref()
        .ok_or_else(|| "alter task missing alter_tablet_req_v2".to_string())?;
    execute_alter_tablet_task(req)
}

fn execute_update_tablet_meta_info_task(
    task: &agent_service::TAgentTaskRequest,
) -> Result<(), String> {
    if task.update_tablet_meta_info_req.is_none() {
        return Err("update_tablet_meta_info task missing update_tablet_meta_info_req".to_string());
    }
    Err("update_tablet_meta_info is unsupported in NovaRocks Lake SCHEMA_CHANGE V1".to_string())
}

fn wait_for_starlet_add_shard(tablet_id: i64) -> Option<starlet_shard_registry::StarletShardInfo> {
    let started_at = Instant::now();
    loop {
        let mut infos = starlet_shard_registry::select_infos(&[tablet_id]);
        if let Some(info) = infos.remove(&tablet_id) {
            if started_at.elapsed() >= Duration::from_millis(CREATE_TABLET_ADD_SHARD_POLL_MS) {
                tracing::info!(
                    tablet_id,
                    waited_ms = started_at.elapsed().as_millis(),
                    "resolved AddShard path for create_tablet after waiting"
                );
            }
            return Some(info);
        }
        if started_at.elapsed() >= Duration::from_millis(CREATE_TABLET_ADD_SHARD_WAIT_MS) {
            return None;
        }
        thread::sleep(Duration::from_millis(CREATE_TABLET_ADD_SHARD_POLL_MS));
    }
}

fn process_submit_task(peer: Option<std::net::SocketAddr>, task: agent_service::TAgentTaskRequest) {
    let create_tablet_id = task.create_tablet_req.as_ref().map(|r| r.tablet_id);
    let drop_tablet_id = task.drop_tablet_req.as_ref().map(|r| r.tablet_id);
    tracing::info!(
        peer = ?peer,
        signature = task.signature,
        task_type = ?task.task_type,
        create_tablet_id = ?create_tablet_id,
        drop_tablet_id = ?drop_tablet_id,
        "BackendService.submit_tasks accepted task"
    );

    let task_result = execute_backend_task(&task);

    if let Err(task_err) = &task_result {
        tracing::warn!(
            peer = ?peer,
            signature = task.signature,
            task_type = ?task.task_type,
            error = %task_err,
            "BackendService.submit_tasks task execution failed"
        );
    }

    let task_error = task_result.err();
    let finish_report_times = task_error
        .as_deref()
        .map(|err| finish_task_report_times_for_error(task.task_type, err))
        .unwrap_or(1);
    for report_attempt in 0..finish_report_times {
        let task_status = match task_error.as_ref() {
            Some(err) => internal_error_status(err.clone()),
            None => ok_status(),
        };
        if let Err(err) = send_finish_task(&task, task_status) {
            tracing::warn!(
                peer = ?peer,
                signature = task.signature,
                task_type = ?task.task_type,
                report_attempt,
                error = %err,
                "BackendService.submit_tasks failed to report finish_task"
            );
            break;
        }
    }
}

fn finish_task_report_times_for_error(task_type: types::TTaskType, error: &str) -> usize {
    if task_type != types::TTaskType::ALTER {
        return 1;
    }
    if error.contains("unsupported") || error.contains("does not support") {
        return ALTER_FAILFAST_ERROR_REPORT_TIMES;
    }
    1
}

fn execute_backend_task(task: &agent_service::TAgentTaskRequest) -> Result<(), String> {
    match task.task_type {
        types::TTaskType::CREATE => {
            execute_create_tablet_task(&task).map_err(|err| format!("create_tablet failed: {err}"))
        }
        types::TTaskType::ALTER => {
            execute_alter_task(&task).map_err(|err| format!("alter task failed: {err}"))
        }
        types::TTaskType::UPDATE_TABLET_META_INFO => execute_update_tablet_meta_info_task(&task)
            .map_err(|err| format!("update_tablet_meta_info failed: {err}")),
        other => Err(format!(
            "unsupported backend task_type={other:?} in submit_tasks"
        )),
    }
}

impl BackendServiceSyncHandler for BackendHandler {
    fn handle_exec_plan_fragment(
        &self,
        params: internal_service::TExecPlanFragmentParams,
    ) -> thrift::Result<internal_service::TExecPlanFragmentResult> {
        let _ = params;
        Ok(internal_service::TExecPlanFragmentResult::new(
            Some(stub_status("exec_plan_fragment")),
            None,
        ))
    }

    fn handle_cancel_plan_fragment(
        &self,
        params: internal_service::TCancelPlanFragmentParams,
    ) -> thrift::Result<internal_service::TCancelPlanFragmentResult> {
        let _ = params;
        Ok(internal_service::TCancelPlanFragmentResult::new(Some(
            stub_status("cancel_plan_fragment"),
        )))
    }

    fn handle_transmit_data(
        &self,
        params: internal_service::TTransmitDataParams,
    ) -> thrift::Result<internal_service::TTransmitDataResult> {
        let _ = params;
        Ok(internal_service::TTransmitDataResult::new(
            Some(stub_status("transmit_data")),
            None,
            None,
            None,
        ))
    }

    fn handle_fetch_data(
        &self,
        params: internal_service::TFetchDataParams,
    ) -> thrift::Result<internal_service::TFetchDataResult> {
        let _ = params;
        let empty_batch = crate::data::TResultBatch::new(vec![], false, 0, None);
        Ok(internal_service::TFetchDataResult::new(
            empty_batch,
            true,
            0,
            Some(stub_status("fetch_data")),
        ))
    }

    fn handle_submit_tasks(
        &self,
        tasks: Vec<agent_service::TAgentTaskRequest>,
    ) -> thrift::Result<agent_service::TAgentResult> {
        let tasks_len = tasks.len();
        if tasks_len > 0 {
            let worker_pool = submit_task_worker_pool();
            for task in tasks {
                let peer = self.peer;
                worker_pool.execute(move || process_submit_task(peer, task));
            }
        }

        tracing::debug!(
            peer = ?self.peer,
            tasks_len,
            "Received BackendService.submit_tasks and queued tasks"
        );
        Ok(agent_service::TAgentResult::new(
            ok_status(),
            None,
            None,
            None,
        ))
    }

    fn handle_make_snapshot(
        &self,
        snapshot_request: agent_service::TSnapshotRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(
            peer = ?self.peer,
            req = %json_summary(&snapshot_request),
            "Received BackendService.make_snapshot"
        );
        Ok(agent_service::TAgentResult::new(
            stub_status("make_snapshot"),
            None,
            None,
            None,
        ))
    }

    fn handle_release_snapshot(
        &self,
        snapshot_path: String,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(
            peer = ?self.peer,
            snapshot_path = %snapshot_path,
            "Received BackendService.release_snapshot"
        );
        Ok(agent_service::TAgentResult::new(
            stub_status("release_snapshot"),
            None,
            None,
            None,
        ))
    }

    fn handle_publish_cluster_state(
        &self,
        request: agent_service::TAgentPublishRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(
            peer = ?self.peer,
            req = %json_summary(&request),
            "Received BackendService.publish_cluster_state"
        );
        Ok(agent_service::TAgentResult::new(
            stub_status("publish_cluster_state"),
            None,
            None,
            None,
        ))
    }

    fn handle_submit_etl_task(
        &self,
        request: agent_service::TMiniLoadEtlTaskRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(
            peer = ?self.peer,
            req = %json_summary(&request),
            "Received BackendService.submit_etl_task"
        );
        Ok(agent_service::TAgentResult::new(
            stub_status("submit_etl_task"),
            None,
            None,
            None,
        ))
    }

    fn handle_get_etl_status(
        &self,
        request: agent_service::TMiniLoadEtlStatusRequest,
    ) -> thrift::Result<agent_service::TMiniLoadEtlStatusResult> {
        tracing::debug!(
            peer = ?self.peer,
            req = %json_summary(&request),
            "Received BackendService.get_etl_status"
        );
        Ok(agent_service::TMiniLoadEtlStatusResult::new(
            stub_status("get_etl_status"),
            types::TEtlState::UNKNOWN,
            None,
            None,
            None,
        ))
    }

    fn handle_delete_etl_files(
        &self,
        request: agent_service::TDeleteEtlFilesRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(
            peer = ?self.peer,
            req = %json_summary(&request),
            "Received BackendService.delete_etl_files"
        );
        Ok(agent_service::TAgentResult::new(
            stub_status("delete_etl_files"),
            None,
            None,
            None,
        ))
    }

    fn handle_submit_export_task(&self, request: TExportTaskRequest) -> thrift::Result<TStatus> {
        tracing::debug!(
            peer = ?self.peer,
            req = %json_summary(&request),
            "Received BackendService.submit_export_task"
        );
        Ok(stub_status("submit_export_task"))
    }

    fn handle_get_export_status(
        &self,
        task_id: types::TUniqueId,
    ) -> thrift::Result<internal_service::TExportStatusResult> {
        tracing::debug!(
            peer = ?self.peer,
            task_id = %json_summary(&task_id),
            "Received BackendService.get_export_status"
        );
        Ok(internal_service::TExportStatusResult::new(
            stub_status("get_export_status"),
            types::TExportState::UNKNOWN,
            None,
        ))
    }

    fn handle_erase_export_task(&self, task_id: types::TUniqueId) -> thrift::Result<TStatus> {
        tracing::debug!(
            peer = ?self.peer,
            task_id = %json_summary(&task_id),
            "Received BackendService.erase_export_task"
        );
        Ok(stub_status("erase_export_task"))
    }

    fn handle_get_tablet_stat(&self) -> thrift::Result<TTabletStatResult> {
        tracing::debug!(peer = ?self.peer, "Received BackendService.get_tablet_stat");
        Ok(TTabletStatResult::new(std::collections::BTreeMap::new()))
    }

    fn handle_submit_routine_load_task(
        &self,
        tasks: Vec<TRoutineLoadTask>,
    ) -> thrift::Result<TStatus> {
        tracing::debug!(
            peer = ?self.peer,
            tasks_len = tasks.len(),
            "Received BackendService.submit_routine_load_task"
        );
        Ok(stub_status("submit_routine_load_task"))
    }

    fn handle_finish_stream_load_channel(
        &self,
        stream_load_channel: TStreamLoadChannel,
    ) -> thrift::Result<TStatus> {
        tracing::debug!(
            peer = ?self.peer,
            channel = %json_summary(&stream_load_channel),
            "Received BackendService.finish_stream_load_channel"
        );
        Ok(stream_load::finish_stream_load_channel(
            stream_load_channel.label.as_deref(),
            stream_load_channel.table_name.as_deref(),
            stream_load_channel.channel_id,
        ))
    }

    fn handle_open_scanner(
        &self,
        params: starrocks_external_service::TScanOpenParams,
    ) -> thrift::Result<starrocks_external_service::TScanOpenResult> {
        tracing::debug!(
            peer = ?self.peer,
            params = %json_summary(&params),
            "Received BackendService.open_scanner"
        );
        Ok(starrocks_external_service::TScanOpenResult::new(
            stub_status("open_scanner"),
            None,
            None,
        ))
    }

    fn handle_get_next(
        &self,
        params: starrocks_external_service::TScanNextBatchParams,
    ) -> thrift::Result<starrocks_external_service::TScanBatchResult> {
        tracing::debug!(
            peer = ?self.peer,
            params = %json_summary(&params),
            "Received BackendService.get_next"
        );
        Ok(starrocks_external_service::TScanBatchResult::new(
            stub_status("get_next"),
            None,
            None,
        ))
    }

    fn handle_close_scanner(
        &self,
        params: starrocks_external_service::TScanCloseParams,
    ) -> thrift::Result<starrocks_external_service::TScanCloseResult> {
        tracing::debug!(
            peer = ?self.peer,
            params = %json_summary(&params),
            "Received BackendService.close_scanner"
        );
        Ok(starrocks_external_service::TScanCloseResult::new(
            stub_status("close_scanner"),
        ))
    }

    fn handle_get_tablets_info(
        &self,
        request: TGetTabletsInfoRequest,
    ) -> thrift::Result<TGetTabletsInfoResult> {
        tracing::debug!(
            peer = ?self.peer,
            req = %json_summary(&request),
            "Received BackendService.get_tablets_info"
        );
        Ok(TGetTabletsInfoResult::new(
            stub_status("get_tablets_info"),
            None,
            None,
        ))
    }
}

pub fn start_backend_service(config: BackendServiceConfig) -> Result<(), String> {
    let host = if config.host.is_empty() {
        "0.0.0.0".to_string()
    } else {
        config.host.clone()
    };

    let addr = format!("{}:{}", host, config.be_port);
    let addr_for_log = addr.clone();

    tracing::info!("Starting BackendService on {}", addr);

    thread::Builder::new()
        .name("backend-service".to_string())
        .spawn(move || {
            tracing::info!("BackendService listening on {}", addr_for_log);

            let listener = match TcpListener::bind(&addr) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("BackendService bind error: {}", e);
                    return;
                }
            };

            let worker_pool = ThreadPool::with_name("BackendService processor".to_owned(), 4);

            for stream in listener.incoming() {
                match stream {
                    Ok(s) => {
                        worker_pool.execute(move || {
                            let peer = s.peer_addr().ok();

                            let mut first8 = [0u8; 8];
                            let n = s
                                .set_read_timeout(Some(Duration::from_millis(50)))
                                .and_then(|_| s.peek(&mut first8))
                                .unwrap_or(0);
                            let _ = s.set_read_timeout(None);

                            let channel = TTcpChannel::with_stream(s);
                            let (r_chan, w_chan) = match channel.split() {
                                Ok(v) => v,
                                Err(_) => return,
                            };

                            let r_tran = TBufferedReadTransportFactory::new().create(Box::new(r_chan));
                            let mut i_prot = TBinaryInputProtocolFactory::new().create(r_tran);

                            let w_tran =
                                TBufferedWriteTransportFactory::new().create(Box::new(w_chan));
                            let mut o_prot = TBinaryOutputProtocolFactory::new().create(w_tran);

                            let handler = BackendHandler { peer };
                            let processor = BackendServiceSyncProcessor::new(handler);

                            loop {
                                match processor.process(&mut *i_prot, &mut *o_prot) {
                                    Ok(()) => {}
                                    Err(thrift::Error::Transport(ref te))
                                        if te.kind == thrift::TransportErrorKind::EndOfFile => break,
                                    Err(thrift::Error::Protocol(ref pe))
                                        if pe.kind == thrift::ProtocolErrorKind::BadVersion => {
                                            tracing::warn!(
                                                peer = ?peer,
                                                first_bytes = ?&first8[..n.min(first8.len())],
                                                thrift_message = %pe.message,
                                                "ProtocolError(BadVersion) on be_port"
                                            );
                                            break;
                                        }
                                    Err(other) => {
                                        tracing::warn!(peer = ?peer, "processor error: {:?}", other);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("failed to accept remote connection: {}", e);
                        continue;
                    }
                }
            }
        })
        .map_err(|e| format!("Failed to spawn backend service thread: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TSerializable};
    use thrift::transport::{TBufferChannel, TIoChannel};

    use super::{execute_backend_task, finish_task_report_times_for_error};
    use crate::{agent_service, descriptors, types};

    fn test_task(task_type: types::TTaskType) -> agent_service::TAgentTaskRequest {
        agent_service::TAgentTaskRequest {
            protocol_version: agent_service::TAgentServiceVersion::V1,
            task_type,
            signature: 1,
            priority: None,
            create_tablet_req: None,
            drop_tablet_req: None,
            alter_tablet_req: None,
            clone_req: None,
            push_req: None,
            cancel_delete_data_req: None,
            resource_info: None,
            storage_medium_migrate_req: None,
            check_consistency_req: None,
            upload_req: None,
            download_req: None,
            snapshot_req: None,
            release_snapshot_req: None,
            clear_remote_file_req: None,
            publish_version_req: None,
            clear_alter_task_req: None,
            clear_transaction_task_req: None,
            move_dir_req: None,
            recover_tablet_req: None,
            alter_tablet_req_v2: None,
            recv_time: None,
            update_tablet_meta_info_req: None,
            drop_auto_increment_map_req: None,
            compaction_req: None,
            remote_snapshot_req: None,
            replicate_snapshot_req: None,
            update_schema_req: None,
            compaction_control_req: None,
        }
    }

    fn test_tablet_schema() -> agent_service::TTabletSchema {
        agent_service::TTabletSchema {
            short_key_column_count: 1,
            schema_hash: 2001,
            keys_type: types::TKeysType::DUP_KEYS,
            storage_type: types::TStorageType::COLUMN,
            columns: vec![descriptors::TColumn {
                column_name: "c1".to_string(),
                column_type: None,
                aggregation_type: None,
                is_key: Some(true),
                is_allow_null: Some(false),
                default_value: None,
                is_bloom_filter_column: None,
                define_expr: None,
                is_auto_increment: Some(false),
                col_unique_id: Some(1),
                has_bitmap_index: None,
                agg_state_desc: None,
                index_len: None,
                type_desc: None,
            }],
            bloom_filter_fpp: None,
            indexes: None,
            is_in_memory: None,
            id: Some(7),
            sort_key_idxes: Some(vec![0]),
            sort_key_unique_ids: Some(vec![1]),
            schema_version: Some(1),
            compression_type: None,
            compression_level: None,
        }
    }

    fn thrift_binary_serialize<T: TSerializable>(value: &T) -> Vec<u8> {
        let channel = TBufferChannel::with_capacity(0, 1024);
        let (_, w) = channel.split().expect("split TBufferChannel");
        let mut protocol = TBinaryOutputProtocol::new(w, true);
        value
            .write_to_out_protocol(&mut protocol)
            .expect("thrift binary serialize");
        protocol.transport.write_bytes()
    }

    fn thrift_binary_deserialize<T: TSerializable>(bytes: &[u8]) -> T {
        let mut channel = TBufferChannel::with_capacity(bytes.len(), 1024);
        channel.set_readable_bytes(bytes);
        let (r, _) = channel.split().expect("split TBufferChannel");
        let mut protocol = TBinaryInputProtocol::new(r, true);
        T::read_from_in_protocol(&mut protocol).expect("thrift binary deserialize")
    }

    #[test]
    fn execute_backend_task_fails_fast_for_alter_without_request() {
        let task = test_task(types::TTaskType::ALTER);
        let err = execute_backend_task(&task).expect_err("alter without req must fail");
        assert!(
            err.contains("alter task failed"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn execute_backend_task_fails_fast_for_update_meta_task() {
        let mut task = test_task(types::TTaskType::UPDATE_TABLET_META_INFO);
        task.update_tablet_meta_info_req = Some(agent_service::TUpdateTabletMetaInfoReq::new(
            None::<Vec<agent_service::TTabletMetaInfo>>,
            Some(agent_service::TTabletType::TABLET_TYPE_LAKE),
            Some(99_i64),
        ));
        let err = execute_backend_task(&task).expect_err("update_meta must fail in V1");
        assert!(
            err.contains("update_tablet_meta_info failed"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn execute_backend_task_fails_fast_for_unsupported_task_type() {
        let task = test_task(types::TTaskType::PUSH);
        let err = execute_backend_task(&task).expect_err("unsupported task must fail");
        assert!(
            err.contains("unsupported backend task_type"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn finish_task_report_times_for_error_retries_unsupported_alter() {
        let reports = finish_task_report_times_for_error(
            types::TTaskType::ALTER,
            "alter task failed: alter task unsupported alter_job_type=TAlterJobType(0)",
        );
        assert_eq!(reports, 3);
    }

    #[test]
    fn finish_task_report_times_for_error_keeps_single_report_for_non_alter() {
        let reports = finish_task_report_times_for_error(
            types::TTaskType::CREATE,
            "create_tablet failed: something failed",
        );
        assert_eq!(reports, 1);
    }

    #[test]
    fn alter_req_v2_roundtrip_with_base_tablet_read_schema() {
        let schema = test_tablet_schema();
        let req = agent_service::TAlterTabletReqV2::new(
            11_i64,
            22_i64,
            101_i32,
            202_i32,
            Some(7_i64),
            None::<Vec<agent_service::TAlterMaterializedViewParam>>,
            Some(agent_service::TTabletType::TABLET_TYPE_LAKE),
            Some(333_i64),
            None::<agent_service::TAlterTabletMaterializedColumnReq>,
            None::<i64>,
            None::<crate::internal_service::TQueryGlobals>,
            None::<crate::internal_service::TQueryOptions>,
            None::<Vec<descriptors::TColumn>>,
            Some(agent_service::TAlterJobType::SCHEMA_CHANGE),
            None::<descriptors::TDescriptorTable>,
            None::<crate::exprs::TExpr>,
            None::<Vec<String>>,
            Some(schema.clone()),
        );

        let bytes = thrift_binary_serialize(&req);
        let decoded: agent_service::TAlterTabletReqV2 = thrift_binary_deserialize(&bytes);
        let decoded_schema = decoded
            .base_tablet_read_schema
            .expect("base_tablet_read_schema should be preserved");
        assert_eq!(decoded_schema.schema_hash, schema.schema_hash);
        assert_eq!(decoded_schema.columns.len(), schema.columns.len());
    }

    #[test]
    fn alter_req_v2_roundtrip_without_base_tablet_read_schema() {
        let req = agent_service::TAlterTabletReqV2::new(
            31_i64,
            32_i64,
            301_i32,
            302_i32,
            Some(9_i64),
            None::<Vec<agent_service::TAlterMaterializedViewParam>>,
            Some(agent_service::TTabletType::TABLET_TYPE_LAKE),
            Some(444_i64),
            None::<agent_service::TAlterTabletMaterializedColumnReq>,
            None::<i64>,
            None::<crate::internal_service::TQueryGlobals>,
            None::<crate::internal_service::TQueryOptions>,
            None::<Vec<descriptors::TColumn>>,
            Some(agent_service::TAlterJobType::SCHEMA_CHANGE),
            None::<descriptors::TDescriptorTable>,
            None::<crate::exprs::TExpr>,
            None::<Vec<String>>,
            None::<agent_service::TTabletSchema>,
        );

        let bytes = thrift_binary_serialize(&req);
        let decoded: agent_service::TAlterTabletReqV2 = thrift_binary_deserialize(&bytes);
        assert!(
            decoded.base_tablet_read_schema.is_none(),
            "base_tablet_read_schema should stay None when not provided"
        );
    }
}
