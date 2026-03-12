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
use crate::common::types::format_uuid;
use crate::frontend_service::{self, TFrontendServiceSyncClient};
use crate::service::disk_report;
use crate::service::frontend_rpc::{
    FrontendRpcCallOptions, FrontendRpcError, FrontendRpcKind, FrontendRpcManager,
};
use crate::{internal_service, status, status_code, types};

use super::SchemaScanContext;

const FE_TIMEOUT_SECS: u64 = 5;

pub(crate) fn resolve_frontend_addr(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Option<types::TNetworkAddress> {
    fe_addr.cloned().or_else(disk_report::latest_fe_addr)
}

pub(crate) fn with_frontend_client<T>(
    fe_addr: Option<&types::TNetworkAddress>,
    f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> Result<T, String>,
) -> Result<T, String> {
    let fe_addr = resolve_frontend_addr(fe_addr).ok_or_else(|| {
        "missing FE address for schema scan (coord is absent and heartbeat cache is empty)"
            .to_string()
    })?;
    let mut f = Some(f);
    FrontendRpcManager::shared()
        .call_with_options(
            FrontendRpcKind::SchemaQuery,
            &fe_addr,
            FrontendRpcCallOptions {
                transport_retries: 0,
            },
            |client| {
                f.take()
                    .expect("schema FE RPC closure is consumed once per request")(
                    client
                )
                .map_err(FrontendRpcError::from_message_guess)
            },
        )
        .map_err(|err| err.to_string())
}

pub(crate) fn forward_show_result(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
    sql: &str,
) -> Result<frontend_service::TShowResultSet, String> {
    let current_user_ident = effective_current_user_ident(ctx);
    let user_roles = current_user_ident
        .as_ref()
        .and_then(|ident| ident.current_role_ids.clone());
    let query_id = random_unique_id();
    let request = frontend_service::TMasterOpRequest::new(
        schema_scan_user(ctx),
        ctx.db.clone().unwrap_or_default(),
        sql.to_string(),
        None::<types::TResourceInfo>,
        None::<String>,
        None::<i64>,
        Some(FE_TIMEOUT_SECS as i32),
        ctx.user_ip.clone().or_else(|| ctx.ip.clone()),
        None::<String>,
        None::<i64>,
        None::<i64>,
        None::<i64>,
        None::<bool>,
        current_user_ident,
        None::<i32>,
        None::<internal_service::TQueryOptions>,
        ctx.catalog_name.clone(),
        Some(query_id.clone()),
        Some(true),
        None::<String>,
        user_roles,
        None::<i32>,
        Some(format_uuid(query_id.hi, query_id.lo)),
        ctx.thread_id
            .and_then(|thread_id| i32::try_from(thread_id).ok()),
        None::<i64>,
        Some(true),
        None::<i64>,
    );
    let response = with_frontend_client(fe_addr, |client| {
        client.forward(request).map_err(|err| err.to_string())
    })?;
    if let Some(error_msg) = response
        .error_msg
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Err(error_msg.to_string());
    }
    response
        .result_set
        .ok_or_else(|| format!("FE forward `{sql}` did not return a show result set"))
}

pub(crate) fn build_auth_info(ctx: &SchemaScanContext) -> frontend_service::TAuthInfo {
    frontend_service::TAuthInfo::new(
        ctx.db.clone(),
        ctx.user.clone(),
        ctx.user_ip.clone().or_else(|| ctx.ip.clone()),
        effective_current_user_ident(ctx),
        ctx.catalog_name.clone(),
    )
}

pub(crate) fn effective_current_user_ident(
    ctx: &SchemaScanContext,
) -> Option<types::TUserIdentity> {
    if let Some(ident) = ctx.current_user_ident.clone() {
        return Some(ident);
    }

    let username = schema_scan_user(ctx);
    let host = ctx
        .user_ip
        .clone()
        .or_else(|| ctx.ip.clone())
        .unwrap_or_else(|| "%".to_string());
    Some(types::TUserIdentity::new(
        Some(username),
        Some(host),
        Some(false),
        Some(false),
        None::<types::TUserRoles>,
    ))
}

pub(crate) fn extract_db_name(full_name: &str) -> String {
    full_name
        .split_once(':')
        .map(|(_, db_name)| db_name.to_string())
        .unwrap_or_else(|| full_name.to_string())
}

pub(crate) fn ensure_ok_status(status: Option<&status::TStatus>, op: &str) -> Result<(), String> {
    let Some(status) = status else {
        return Ok(());
    };
    if status.status_code == status_code::TStatusCode::OK {
        return Ok(());
    }
    let msg = status
        .error_msgs
        .as_ref()
        .and_then(|msgs| msgs.first())
        .cloned()
        .unwrap_or_else(|| format!("{op} failed with status {:?}", status.status_code));
    Err(msg)
}

fn schema_scan_user(ctx: &SchemaScanContext) -> String {
    ctx.user
        .clone()
        .or_else(|| {
            ctx.current_user_ident
                .as_ref()
                .and_then(|ident| ident.username.clone())
        })
        .unwrap_or_else(|| "root".to_string())
}

fn random_unique_id() -> types::TUniqueId {
    types::TUniqueId::new(rand::random::<i64>(), rand::random::<i64>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_nodes;

    fn make_context() -> SchemaScanContext {
        SchemaScanContext {
            table_name: "task_runs".to_string(),
            db: Some("db1".to_string()),
            table: None,
            wild: None,
            user: Some("alice".to_string()),
            ip: Some("10.0.0.8".to_string()),
            port: None,
            thread_id: None,
            user_ip: Some("10.0.0.9".to_string()),
            current_user_ident: None,
            catalog_name: None,
            table_id: None,
            partition_id: None,
            tablet_id: None,
            txn_id: None,
            job_id: None,
            label: None,
            type_: None,
            state: None,
            limit: None,
            log_start_ts: None,
            log_end_ts: None,
            log_level: None,
            log_pattern: None,
            log_limit: None,
            frontends: Vec::<plan_nodes::TFrontend>::new(),
        }
    }

    #[test]
    fn effective_current_user_ident_preserves_explicit_identity() {
        let mut ctx = make_context();
        ctx.current_user_ident = Some(types::TUserIdentity::new(
            Some("bob".to_string()),
            Some("192.168.1.2".to_string()),
            Some(false),
            Some(false),
            None::<types::TUserRoles>,
        ));

        let ident = effective_current_user_ident(&ctx).expect("user identity");
        assert_eq!(ident.username.as_deref(), Some("bob"));
        assert_eq!(ident.host.as_deref(), Some("192.168.1.2"));
    }

    #[test]
    fn effective_current_user_ident_synthesizes_missing_identity() {
        let ctx = make_context();

        let ident = effective_current_user_ident(&ctx).expect("user identity");
        assert_eq!(ident.username.as_deref(), Some("alice"));
        assert_eq!(ident.host.as_deref(), Some("10.0.0.9"));
        assert_eq!(ident.is_domain, Some(false));
        assert_eq!(ident.is_ephemeral, Some(false));
    }
}
