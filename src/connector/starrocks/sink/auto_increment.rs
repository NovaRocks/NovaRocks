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

use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{
    ReadHalf as TReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel,
    TTcpChannel, WriteHalf as TWriteHalf,
};

use crate::frontend_service::{FrontendServiceSyncClient, TFrontendServiceSyncClient};
use crate::{frontend_service, status_code, types};

const FE_AUTO_INCREMENT_TIMEOUT_SECS: u64 = 20;

#[derive(Clone, Copy, Debug)]
struct AutoIncrementInterval {
    next: i64,
    end: i64,
}

static AUTO_INCREMENT_INTERVALS: OnceLock<Mutex<HashMap<i64, AutoIncrementInterval>>> =
    OnceLock::new();

fn interval_cache() -> &'static Mutex<HashMap<i64, AutoIncrementInterval>> {
    AUTO_INCREMENT_INTERVALS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn with_frontend_client<T>(
    fe_addr: &types::TNetworkAddress,
    f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> Result<T, String>,
) -> Result<T, String> {
    let addr: SocketAddr = format!("{}:{}", fe_addr.hostname, fe_addr.port)
        .parse()
        .map_err(|e| format!("invalid FE address for auto increment: {e}"))?;
    let stream =
        TcpStream::connect_timeout(&addr, Duration::from_secs(FE_AUTO_INCREMENT_TIMEOUT_SECS))
            .map_err(|e| format!("connect FE failed for auto increment: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(FE_AUTO_INCREMENT_TIMEOUT_SECS)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(FE_AUTO_INCREMENT_TIMEOUT_SECS)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan): (TReadHalf<TTcpChannel>, TWriteHalf<TTcpChannel>) = channel
        .split()
        .map_err(|e| format!("split FE thrift channel failed for auto increment: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_trans, true);
    let o_prot = TBinaryOutputProtocol::new(o_trans, true);
    let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);
    f(&mut client)
}

fn request_new_interval(
    fe_addr: &types::TNetworkAddress,
    table_id: i64,
    rows: usize,
) -> Result<AutoIncrementInterval, String> {
    if table_id <= 0 {
        return Err(format!(
            "invalid table_id for auto increment allocation: {table_id}"
        ));
    }
    if rows == 0 {
        return Err("auto increment allocation rows cannot be zero".to_string());
    }
    let rows_i64 = i64::try_from(rows)
        .map_err(|_| format!("auto increment allocation rows overflow: {rows}"))?;
    let request = frontend_service::TAllocateAutoIncrementIdParam {
        table_id: Some(table_id),
        rows: Some(rows_i64),
    };
    let response = with_frontend_client(fe_addr, |client| {
        client
            .alloc_auto_increment_id(request)
            .map_err(|e| format!("alloc_auto_increment_id RPC failed: {e}"))
    })?;
    let status = response
        .status
        .as_ref()
        .ok_or_else(|| "alloc_auto_increment_id response missing status".to_string())?;
    if status.status_code != status_code::TStatusCode::OK {
        let detail = status
            .error_msgs
            .as_ref()
            .map(|v| v.join("; "))
            .unwrap_or_default();
        return Err(format!(
            "alloc_auto_increment_id failed: status={:?}, error={}",
            status.status_code, detail
        ));
    }
    let start = response
        .auto_increment_id
        .ok_or_else(|| "alloc_auto_increment_id response missing auto_increment_id".to_string())?;
    let allocated_rows = response
        .allocated_rows
        .ok_or_else(|| "alloc_auto_increment_id response missing allocated_rows".to_string())?;
    if allocated_rows <= 0 {
        return Err(format!(
            "alloc_auto_increment_id returned invalid allocated_rows={allocated_rows}"
        ));
    }
    let end = start.checked_add(allocated_rows).ok_or_else(|| {
        format!(
            "auto increment interval overflow: start={} allocated_rows={}",
            start, allocated_rows
        )
    })?;
    Ok(AutoIncrementInterval { next: start, end })
}

pub(crate) fn allocate_auto_increment_ids(
    fe_addr: &types::TNetworkAddress,
    table_id: i64,
    rows: usize,
) -> Result<Vec<i64>, String> {
    if rows == 0 {
        return Ok(Vec::new());
    }
    if table_id <= 0 {
        return Err(format!("invalid table_id for auto increment: {table_id}"));
    }

    let mut result = Vec::with_capacity(rows);
    let mut remaining = rows;
    let mut guard = interval_cache()
        .lock()
        .map_err(|_| "lock auto increment cache failed".to_string())?;

    while remaining > 0 {
        let usable = guard
            .get_mut(&table_id)
            .and_then(|interval| (interval.next < interval.end).then_some(interval));
        if let Some(interval) = usable {
            let available = usize::try_from(interval.end - interval.next).unwrap_or(0);
            if available > 0 {
                let take = remaining.min(available);
                let start = interval.next;
                let end = start
                    .checked_add(i64::try_from(take).map_err(|_| {
                        format!("auto increment take size overflow: table_id={table_id}, take={take}")
                    })?)
                    .ok_or_else(|| {
                        format!(
                            "auto increment range overflow while assigning ids: table_id={table_id}, start={start}, take={take}"
                        )
                    })?;
                result.extend(start..end);
                interval.next = end;
                remaining -= take;
                continue;
            }
        }

        let request_rows = remaining.max(1024);
        let interval = request_new_interval(fe_addr, table_id, request_rows)?;
        guard.insert(table_id, interval);
    }

    Ok(result)
}
