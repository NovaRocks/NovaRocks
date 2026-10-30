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

//! Complete capture from independently monitored file-backed output.
use crate::{
    ManagedCommand,
    monitor_client::{Material, OwnedMonitor},
    unix,
};
use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::process::Output;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct CapturedCommandOutcome {
    pub output: Output,
    pub timed_out: bool,
    pub failures: Vec<String>,
    group_exit_observed: bool,
    material: Arc<Material>,
}

impl CapturedCommandOutcome {
    /// True only when the waited original monitor observed the entire local
    /// payload group gone. This says nothing about remote producer work.
    pub fn group_exit_observed(&self) -> bool {
        self.group_exit_observed
    }

    /// Release ordinary diagnostics after the caller consumes all assertions.
    pub fn release_diagnostics(&self) -> Result<()> {
        ensure!(
            !self.timed_out && self.failures.is_empty(),
            "cannot release failed command diagnostics"
        );
        self.material.release();
        Ok(())
    }
    pub fn diagnostic_root(&self) -> &std::path::Path {
        &self.material.path
    }
}

pub fn capture_command_until(
    command: ManagedCommand,
    mut stdout: File,
    mut stderr: File,
    deadline: Instant,
) -> Result<CapturedCommandOutcome> {
    ensure!(
        Instant::now() < deadline,
        "captured command original deadline expired before spawn"
    );
    ensure!(
        stdout.metadata()?.is_file() && stderr.metadata()?.is_file(),
        "captured command output must be an owned regular file"
    );
    let mut owner = OwnedMonitor::spawn("captured command", command, deadline)?;
    let mut timed_out = false;
    let mut failures = vec![];
    let final_record = match owner.wait(deadline) {
        Ok(value) => value,
        Err(primary) => {
            timed_out = Instant::now() >= deadline;
            failures.push(format!("captured command original wait: {primary:#}"));
            owner.send(b'K')?;
            owner
                .wait(Instant::now() + Duration::from_secs(10))
                .context("actually settle captured command after original wait failed")?
        }
    };
    let mut output = Output {
        status: final_record.status,
        stdout: vec![],
        stderr: vec![],
    };
    for (name, original, target, bytes) in [
        ("stdout", &owner.stdout, &mut stdout, &mut output.stdout),
        ("stderr", &owner.stderr, &mut stderr, &mut output.stderr),
    ] {
        let result = (|| -> Result<()> {
            ensure!(
                Instant::now() < deadline,
                "original deadline expired reading captured output; run-owned original retained"
            );
            let metadata = original.metadata()?;
            let path_metadata =
                std::fs::symlink_metadata(owner.material.path.join(format!("{name}.log")))?;
            ensure!(
                path_metadata.is_file()
                    && metadata.ino() == path_metadata.ino()
                    && metadata.dev() == path_metadata.dev(),
                "original captured output path was replaced"
            );
            let mut source = original.try_clone()?;
            source.seek(SeekFrom::Start(0))?;
            target.seek(SeekFrom::Start(0))?;
            target.set_len(0)?;
            let mut buffer = [0u8; 65536];
            loop {
                ensure!(
                    Instant::now() < deadline,
                    "original deadline expired reading captured output; run-owned original retained"
                );
                let read = source.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                target.write_all(&buffer[..read])?;
                bytes.extend_from_slice(&buffer[..read]);
            }
            target.flush()?;
            Ok(())
        })();
        if let Err(error) = result {
            timed_out |= Instant::now() >= deadline;
            failures.push(format!(
                "read original captured {name}: {error:#}; authoritative run output={}",
                owner.material.path.join(format!("{name}.log")).display()
            ));
        }
    }
    let material = Arc::clone(&owner.material);
    // The waiter completed before material can ever be released.
    drop(owner);
    let _ = unix::POLL;
    Ok(CapturedCommandOutcome {
        output,
        timed_out,
        failures,
        group_exit_observed: final_record.group_exit_observed,
        material,
    })
}
