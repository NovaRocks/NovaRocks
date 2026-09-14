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

//! Production supervision for the native Backend role.

use std::future::Future;
use std::time::Duration;

use novarocks_native_adapter::BackendDataRuntime;
use novarocks_native_adapter::backend_application::{
    BackendApplicationError, BackendApplicationHost, BackendServerConfig,
};

const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Runs one already-composed Backend role until its process owner requests
/// shutdown or one of the host-owned listeners reports a failure.
pub async fn run_until_shutdown<F>(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let mut host = BackendApplicationHost::open(config, data_runtime)?;
    println!("{}", host.ready_marker());
    tokio::pin!(shutdown);

    let primary = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            _ = tokio::time::sleep(SUPERVISION_POLL_INTERVAL) => match host.poll_failure() {
                Ok(Some(error)) | Err(error) => break Err(error),
                Ok(None) => {}
            },
        }
    };
    let primary = match primary {
        Ok(()) => match host.poll_failure() {
            Ok(Some(error)) | Err(error) => Err(error),
            Ok(None) => Ok(()),
        },
        Err(error) => Err(error),
    };
    host.begin_drain();
    match (primary, host.shutdown()) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(primary), Err(shutdown)) => Err(primary.with_cleanup_context(shutdown)),
    }
}
