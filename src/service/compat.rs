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
use std::ffi::CString;

#[repr(C)]
struct NovaRocksCompatConfig {
    host: *const std::os::raw::c_char,
    heartbeat_port: u16,
    brpc_port: u16,
    debug_exec_batch_plan_json: u8,
    log_level: u8,
}

unsafe extern "C" {
    fn novarocks_compat_start(
        cfg: *const NovaRocksCompatConfig,
        err_buf: *mut std::os::raw::c_char,
        err_buf_len: i32,
    ) -> i32;
    fn novarocks_compat_stop();
}

#[derive(Debug, Clone)]
pub struct CompatConfig<'a> {
    pub host: &'a str,
    pub heartbeat_port: u16,
    pub brpc_port: u16,
    pub debug_exec_batch_plan_json: bool,
    pub log_level: u8,
}

#[derive(Debug)]
pub struct CompatError {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for CompatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "compat start failed (code={}): {}",
            self.code, self.message
        )
    }
}

impl std::error::Error for CompatError {}

pub fn start(cfg: &CompatConfig<'_>) -> Result<(), CompatError> {
    let host = CString::new(cfg.host).map_err(|e| CompatError {
        code: -1,
        message: format!("invalid host string: {e}"),
    })?;

    let native_cfg = NovaRocksCompatConfig {
        host: host.as_ptr(),
        heartbeat_port: cfg.heartbeat_port,
        brpc_port: cfg.brpc_port,
        debug_exec_batch_plan_json: u8::from(cfg.debug_exec_batch_plan_json),
        log_level: cfg.log_level,
    };

    let mut err_buf = vec![0i8; 512];
    let code =
        unsafe { novarocks_compat_start(&native_cfg, err_buf.as_mut_ptr(), err_buf.len() as i32) };
    if code == 0 {
        Ok(())
    } else {
        let message = unsafe { std::ffi::CStr::from_ptr(err_buf.as_ptr()) }
            .to_string_lossy()
            .trim()
            .to_string();
        Err(CompatError { code, message })
    }
}

pub fn stop() {
    unsafe { novarocks_compat_stop() }
}
