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
use std::time::Duration;

use reqwest::blocking::Client;

use crate::frontend_service::{self, TTrackingLoadInfo};
use crate::types;

use super::SchemaScanContext;
use super::chunk_builder::{SchemaRow, SchemaValue, normalize_column_key};
use super::frontend::with_frontend_client;

const TRACKING_LOG_TIMEOUT_SECS: u64 = 5;

pub(crate) fn fetch_rows(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    if ctx.label.is_none() && ctx.job_id.unwrap_or_default() <= 0 {
        return Ok(Vec::new());
    }

    let request = frontend_service::TGetLoadsParams::new(
        ctx.db.clone(),
        ctx.job_id,
        None::<i64>,
        ctx.label.clone(),
        ctx.type_.clone(),
    );
    let response = match with_frontend_client(fe_addr, |client| {
        client
            .get_tracking_loads(request)
            .map_err(|err| err.to_string())
    }) {
        Ok(response) => response,
        Err(err) if err.contains("load_tracking_logs must specify label or job_id") => {
            return Ok(Vec::new());
        }
        Err(err) => return Err(err),
    };
    let http_client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(TRACKING_LOG_TIMEOUT_SECS))
        .build()
        .map_err(|err| format!("build tracking log http client failed: {err}"))?;
    response
        .tracking_loads
        .unwrap_or_default()
        .iter()
        .map(|info| build_tracking_row(info, &http_client))
        .collect()
}

fn build_tracking_row(info: &TTrackingLoadInfo, http_client: &Client) -> Result<SchemaRow, String> {
    let mut row = SchemaRow::new();
    let job_id = info.job_id.unwrap_or_default();
    row.insert(normalize_column_key("ID"), SchemaValue::Int64(job_id));
    row.insert(normalize_column_key("JOB_ID"), SchemaValue::Int64(job_id));
    row.insert(
        normalize_column_key("LABEL"),
        SchemaValue::Utf8(info.label.clone().unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("DATABASE_NAME"),
        SchemaValue::Utf8(info.db.clone().unwrap_or_default()),
    );
    if let Some(load_type) = info.load_type.as_ref() {
        row.insert(
            normalize_column_key("TYPE"),
            SchemaValue::Utf8(load_type.clone()),
        );
    }
    if let Some(urls) = info.urls.as_ref() {
        let logs = urls
            .iter()
            .map(|url| fetch_tracking_log(url, http_client))
            .collect::<Vec<_>>();
        row.insert(
            normalize_column_key("TRACKING_LOG"),
            SchemaValue::Utf8(logs.join("\n")),
        );
    }
    Ok(row)
}

fn fetch_tracking_log(url: &str, http_client: &Client) -> String {
    match http_client.get(url).send() {
        Ok(response) => match response.error_for_status() {
            Ok(response) => response
                .text()
                .unwrap_or_else(|err| format!("Failed to read {url}: {err}")),
            Err(err) => format!("Failed to access {url} err: {err}"),
        },
        Err(err) => format!("Failed to access {url} err: {err}"),
    }
}
