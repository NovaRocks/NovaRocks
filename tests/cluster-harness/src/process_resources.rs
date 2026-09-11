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

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
#[cfg(not(target_os = "linux"))]
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, UNIX_EPOCH};

#[cfg(target_os = "macos")]
const PROC_PIDTBSDINFO: i32 = 3;

#[cfg(target_os = "macos")]
#[repr(C)]
struct ProcBsdInfo {
    pbi_flags: u32,
    pbi_status: u32,
    pbi_xstatus: u32,
    pbi_pid: u32,
    pbi_ppid: u32,
    pbi_uid: u32,
    pbi_gid: u32,
    pbi_ruid: u32,
    pbi_rgid: u32,
    pbi_svuid: u32,
    pbi_svgid: u32,
    rfu_1: u32,
    pbi_comm: [i8; 16],
    pbi_name: [i8; 32],
    pbi_nfiles: u32,
    pbi_pgid: u32,
    pbi_pjobc: u32,
    e_tdev: u32,
    e_tpgid: u32,
    pbi_nice: i32,
    pbi_start_tvsec: u64,
    pbi_start_tvusec: u64,
}

#[cfg(target_os = "macos")]
#[link(name = "proc")]
unsafe extern "C" {
    fn proc_pidinfo(
        pid: i32,
        flavor: i32,
        arg: u64,
        buffer: *mut std::ffi::c_void,
        buffer_size: i32,
    ) -> i32;
    fn proc_pidpath(pid: i32, buffer: *mut std::ffi::c_void, buffer_size: u32) -> i32;
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExecutableFileIdentity {
    pub canonical_path: PathBuf,
    pub sha256: String,
    pub size_bytes: u64,
    pub modified_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProcessLaunchIdentity {
    pub role: String,
    pub pid: u32,
    pub process_start_token: String,
    pub executable: ExecutableFileIdentity,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProcessResourceIdentity {
    pub pid: u32,
    pub process_start_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterProcessIds {
    pub frontend: u32,
    pub backends: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterProcessIdentities {
    pub frontend: ProcessResourceIdentity,
    pub backends: Vec<ProcessResourceIdentity>,
}

impl ClusterProcessIds {
    fn role_pid_map(&self) -> Result<BTreeMap<String, u32>> {
        let mut processes = BTreeMap::new();
        processes.insert("fe".to_string(), self.frontend);
        for (index, pid) in self.backends.iter().copied().enumerate() {
            processes.insert(format!("be-{index}"), pid);
        }
        if processes.values().any(|pid| *pid == 0) {
            bail!("process resource identity contains pid zero");
        }
        let unique_pids = processes.values().copied().collect::<BTreeSet<_>>();
        if unique_pids.len() != processes.len() {
            bail!("process resource identity assigns one pid to multiple roles");
        }
        Ok(processes)
    }

    fn role_process_map(&self) -> Result<BTreeMap<String, ProcessResourceIdentity>> {
        let processes = self
            .role_pid_map()?
            .into_iter()
            .map(|(role, pid)| {
                let process_start_token = read_process_start_token(pid)
                    .with_context(|| format!("capture process start token for role {role}"))?;
                Ok((
                    role,
                    ProcessResourceIdentity {
                        pid,
                        process_start_token,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        validate_role_process_map(processes)
    }
}

impl ClusterProcessIdentities {
    pub fn from_launch_identities(
        frontend: &ProcessLaunchIdentity,
        backends: &[ProcessLaunchIdentity],
    ) -> Result<Self> {
        if frontend.role != "fe" {
            bail!(
                "frontend launch identity has unexpected role {}",
                frontend.role
            );
        }
        let backends = backends
            .iter()
            .enumerate()
            .map(|(index, identity)| {
                let expected = format!("be-{index}");
                if identity.role != expected {
                    bail!(
                        "backend launch identity at index {index} has unexpected role {}",
                        identity.role
                    );
                }
                Ok(ProcessResourceIdentity {
                    pid: identity.pid,
                    process_start_token: identity.process_start_token.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let identities = Self {
            frontend: ProcessResourceIdentity {
                pid: frontend.pid,
                process_start_token: frontend.process_start_token.clone(),
            },
            backends,
        };
        identities.role_process_map()?;
        Ok(identities)
    }

    fn role_process_map(&self) -> Result<BTreeMap<String, ProcessResourceIdentity>> {
        let mut processes = BTreeMap::new();
        processes.insert("fe".to_string(), self.frontend.clone());
        for (index, identity) in self.backends.iter().cloned().enumerate() {
            processes.insert(format!("be-{index}"), identity);
        }
        validate_role_process_map(processes)
    }
}

fn validate_role_process_map(
    processes: BTreeMap<String, ProcessResourceIdentity>,
) -> Result<BTreeMap<String, ProcessResourceIdentity>> {
    if processes
        .values()
        .any(|identity| identity.pid == 0 || identity.process_start_token.is_empty())
    {
        bail!("process resource identity contains an empty pid or start token");
    }
    let unique_pids = processes
        .values()
        .map(|identity| identity.pid)
        .collect::<BTreeSet<_>>();
    if unique_pids.len() != processes.len() {
        bail!("process resource identity assigns one pid to multiple roles");
    }
    Ok(processes)
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProcessResourceSample {
    pub elapsed_millis: u128,
    pub role: String,
    pub pid: u32,
    pub process_start_token: String,
    pub rss_bytes: Option<u64>,
    pub threads: Option<u64>,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct ProcessResourceHighWater {
    pub rss_bytes: u64,
    pub threads: Option<u64>,
}

/// Samples process resources without changing the process under observation.
///
/// Missing thread counters remain `None`; callers must not turn unavailable
/// operating-system data into a successful zero-valued measurement.
pub struct ProcessResourceSampler {
    started: Instant,
    run_id: String,
    processes: BTreeMap<String, ProcessResourceIdentity>,
    samples: Vec<ProcessResourceSample>,
}

pub struct ProcessResourceMonitor {
    started: Instant,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<ProcessResourceSampler>>>,
}

impl ProcessResourceMonitor {
    pub fn start(
        ids: ClusterProcessIds,
        run_id: impl Into<String>,
        interval: Duration,
    ) -> Result<Self> {
        let identities = ids.role_process_map()?;
        Self::start_with_processes(identities, run_id, interval)
    }

    pub fn start_with_identities(
        identities: ClusterProcessIdentities,
        run_id: impl Into<String>,
        interval: Duration,
    ) -> Result<Self> {
        let processes = identities.role_process_map()?;
        Self::start_with_processes(processes, run_id, interval)
    }

    fn start_with_processes(
        processes: BTreeMap<String, ProcessResourceIdentity>,
        run_id: impl Into<String>,
        interval: Duration,
    ) -> Result<Self> {
        if interval.is_zero() {
            bail!("process resource sampling interval must be positive");
        }
        let run_id = run_id.into();
        if run_id.is_empty() {
            bail!("process resource sampling requires a run identity");
        }
        let started = Instant::now();
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::clone(&stopped);
        let worker_started = started;
        let worker = thread::Builder::new()
            .name("process-resource-monitor".to_string())
            .spawn(move || {
                let mut sampler = ProcessResourceSampler::new_at(worker_started, run_id, processes);
                while !worker_stopped.load(Ordering::Acquire) {
                    sampler.sample_cluster()?;
                    thread::sleep(interval);
                }
                Ok(sampler)
            })
            .context("spawn process resource monitor")?;
        Ok(Self {
            started,
            stopped,
            worker: Some(worker),
        })
    }

    /// Returns a timestamp in the same monotonic time domain as every sample.
    pub fn elapsed_millis(&self) -> u128 {
        self.started.elapsed().as_millis()
    }

    pub fn finish(mut self, path: &Path) -> Result<ProcessResourceSampler> {
        self.stopped.store(true, Ordering::Release);
        let sampler = self
            .worker
            .take()
            .context("process resource monitor already finished")?
            .join()
            .map_err(|_| anyhow::anyhow!("process resource monitor panicked"))??;
        sampler.write_json(path)?;
        Ok(sampler)
    }
}

impl Drop for ProcessResourceMonitor {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl ProcessResourceSampler {
    #[cfg(test)]
    fn new() -> Self {
        let pid = std::process::id();
        let processes = BTreeMap::from([(
            "fe".to_string(),
            ProcessResourceIdentity {
                pid,
                process_start_token: read_process_start_token(pid)
                    .expect("capture current process start token"),
            },
        )]);
        Self::new_at(Instant::now(), "test-run".to_string(), processes)
    }

    fn new_at(
        started: Instant,
        run_id: String,
        processes: BTreeMap<String, ProcessResourceIdentity>,
    ) -> Self {
        Self {
            started,
            run_id,
            processes,
            samples: Vec::new(),
        }
    }

    pub fn sample_cluster(&mut self) -> Result<()> {
        for (role, identity) in &self.processes {
            verify_process_start_token(identity.pid, &identity.process_start_token)
                .with_context(|| format!("verify process identity before sampling role {role}"))?;
            let sample = match read_process_resources(identity.pid) {
                Ok((rss_bytes, threads)) => ProcessResourceSample {
                    elapsed_millis: self.started.elapsed().as_millis(),
                    role: role.clone(),
                    pid: identity.pid,
                    process_start_token: identity.process_start_token.clone(),
                    rss_bytes: Some(rss_bytes),
                    threads,
                    unavailable_reason: None,
                },
                Err(error) => ProcessResourceSample {
                    elapsed_millis: self.started.elapsed().as_millis(),
                    role: role.clone(),
                    pid: identity.pid,
                    process_start_token: identity.process_start_token.clone(),
                    rss_bytes: None,
                    threads: None,
                    unavailable_reason: Some(format!("{error:#}")),
                },
            };
            verify_process_start_token(identity.pid, &identity.process_start_token)
                .with_context(|| format!("verify process identity after sampling role {role}"))?;
            self.samples.push(sample);
        }
        Ok(())
    }

    pub fn samples(&self) -> &[ProcessResourceSample] {
        &self.samples
    }

    pub fn high_water(&self, role: &str) -> Option<ProcessResourceHighWater> {
        let matching = self
            .samples
            .iter()
            .filter(|sample| sample.role == role)
            .filter_map(|sample| sample.rss_bytes.map(|rss| (sample, rss)));
        let mut high = ProcessResourceHighWater::default();
        let mut found = false;
        for (sample, rss_bytes) in matching {
            found = true;
            high.rss_bytes = high.rss_bytes.max(rss_bytes);
            high.threads = match (high.threads, sample.threads) {
                (Some(current), Some(next)) => Some(current.max(next)),
                (None, Some(next)) => Some(next),
                (current, None) => current,
            };
        }
        found.then_some(high)
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        #[derive(Serialize)]
        struct ProcessResourceEnvelope<'a> {
            schema_version: u32,
            run_id: &'a str,
            processes: &'a BTreeMap<String, ProcessResourceIdentity>,
            samples: &'a [ProcessResourceSample],
        }
        let envelope = ProcessResourceEnvelope {
            schema_version: 2,
            run_id: &self.run_id,
            processes: &self.processes,
            samples: &self.samples,
        };
        let bytes =
            serde_json::to_vec_pretty(&envelope).context("serialize process resource envelope")?;
        fs::write(path, bytes)
            .with_context(|| format!("write process resource samples {}", path.display()))
    }
}

pub fn freeze_executable_identity(path: &Path) -> Result<ExecutableFileIdentity> {
    let canonical_path = fs::canonicalize(path)
        .with_context(|| format!("canonicalize executable path {}", path.display()))?;
    let metadata = fs::metadata(&canonical_path)
        .with_context(|| format!("inspect executable {}", canonical_path.display()))?;
    if !metadata.is_file() {
        bail!(
            "executable path is not a regular file: {}",
            canonical_path.display()
        );
    }
    let modified_unix_nanos = metadata
        .modified()
        .with_context(|| format!("read executable mtime {}", canonical_path.display()))?
        .duration_since(UNIX_EPOCH)
        .with_context(|| {
            format!(
                "executable mtime predates Unix epoch: {}",
                canonical_path.display()
            )
        })?
        .as_nanos()
        .try_into()
        .context("executable mtime does not fit u64 Unix nanoseconds")?;
    let mut file = File::open(&canonical_path)
        .with_context(|| format!("open executable {}", canonical_path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash executable {}", canonical_path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let metadata_after = fs::metadata(&canonical_path)
        .with_context(|| format!("reinspect executable {}", canonical_path.display()))?;
    let modified_after = metadata_after
        .modified()
        .with_context(|| format!("reread executable mtime {}", canonical_path.display()))?
        .duration_since(UNIX_EPOCH)
        .with_context(|| {
            format!(
                "executable mtime predates Unix epoch: {}",
                canonical_path.display()
            )
        })?
        .as_nanos();
    if metadata_after.len() != metadata.len() || modified_after != u128::from(modified_unix_nanos) {
        bail!(
            "executable changed while hashing: {}",
            canonical_path.display()
        );
    }
    Ok(ExecutableFileIdentity {
        canonical_path,
        sha256: format!("{:x}", hasher.finalize()),
        size_bytes: metadata.len(),
        modified_unix_nanos,
    })
}

pub fn capture_process_launch_identity(
    role: impl Into<String>,
    pid: u32,
    executable: &ExecutableFileIdentity,
) -> Result<ProcessLaunchIdentity> {
    let role = role.into();
    if role.is_empty() {
        bail!("process launch identity requires a role");
    }
    if pid == 0 {
        bail!("process launch identity requires a nonzero pid");
    }
    let current_executable = freeze_executable_identity(&executable.canonical_path)?;
    if current_executable != *executable {
        bail!("executable changed across process spawn for role {role}");
    }
    let process_start_token = read_process_start_token(pid)
        .with_context(|| format!("capture process start token for role {role}"))?;
    verify_live_executable_path(pid, &executable.canonical_path)
        .with_context(|| format!("verify live executable path for role {role}"))?;
    verify_process_start_token(pid, &process_start_token)
        .with_context(|| format!("recheck process start token for role {role}"))?;
    Ok(ProcessLaunchIdentity {
        role,
        pid,
        process_start_token,
        executable: executable.clone(),
    })
}

pub fn recheck_process_launch_identity(
    expected: &ProcessLaunchIdentity,
) -> Result<ProcessLaunchIdentity> {
    verify_process_start_token(expected.pid, &expected.process_start_token)
        .with_context(|| format!("verify live process instance for role {}", expected.role))?;
    verify_live_executable_path(expected.pid, &expected.executable.canonical_path)
        .with_context(|| format!("verify live executable path for role {}", expected.role))?;
    let current_executable = freeze_executable_identity(&expected.executable.canonical_path)?;
    if current_executable != expected.executable {
        bail!(
            "live executable contents or metadata changed for role {}",
            expected.role
        );
    }
    verify_process_start_token(expected.pid, &expected.process_start_token)
        .with_context(|| format!("recheck live process instance for role {}", expected.role))?;
    Ok(expected.clone())
}

fn verify_process_start_token(pid: u32, expected: &str) -> Result<()> {
    let observed = read_process_start_token(pid)?;
    if observed != expected {
        bail!("process start token changed for pid {pid}: expected={expected} observed={observed}");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn read_process_start_token(pid: u32) -> Result<String> {
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .context("read Linux boot identity")?;
    let stat_path = format!("/proc/{pid}/stat");
    let stat =
        fs::read_to_string(&stat_path).with_context(|| format!("read process stat {stat_path}"))?;
    let command_end = stat
        .rfind(')')
        .context("Linux process stat omitted command terminator")?;
    let fields = stat
        .get(command_end + 1..)
        .context("slice Linux process stat fields")?
        .split_whitespace()
        .collect::<Vec<_>>();
    let start_ticks = fields
        .get(19)
        .context("Linux process stat omitted starttime")?;
    start_ticks
        .parse::<u64>()
        .context("parse Linux process starttime")?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        bail!("Linux boot identity is empty");
    }
    Ok(format!("linux:{boot_id}:{start_ticks}"))
}

#[cfg(target_os = "macos")]
pub fn read_process_start_token(pid: u32) -> Result<String> {
    let pid: i32 = pid
        .try_into()
        .context("process pid does not fit macOS pid_t")?;
    let mut info = std::mem::MaybeUninit::<ProcBsdInfo>::zeroed();
    let expected_size: i32 = std::mem::size_of::<ProcBsdInfo>()
        .try_into()
        .context("proc_bsdinfo size does not fit c_int")?;
    // SAFETY: `info` points to writable storage of exactly `expected_size`
    // bytes and libproc initializes that storage on a full-size return.
    let returned = unsafe {
        proc_pidinfo(
            pid,
            PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            expected_size,
        )
    };
    if returned != expected_size {
        bail!(
            "proc_pidinfo could not read complete birth identity for pid {pid}: returned={returned} expected={expected_size}"
        );
    }
    // SAFETY: the exact-size return above states that libproc initialized the
    // complete `proc_bsdinfo` structure.
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != pid as u32 || info.pbi_start_tvsec == 0 {
        bail!("proc_pidinfo returned invalid birth identity for pid {pid}");
    }
    Ok(format!(
        "macos:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn read_process_start_token(_pid: u32) -> Result<String> {
    bail!("process start identity is unsupported on this operating system")
}

#[cfg(target_os = "linux")]
fn live_process_executable_path(pid: u32) -> Result<PathBuf> {
    let path = fs::read_link(format!("/proc/{pid}/exe"))
        .with_context(|| format!("read live executable path for pid {pid}"))?;
    fs::canonicalize(&path)
        .with_context(|| format!("canonicalize live executable path {}", path.display()))
}

#[cfg(target_os = "macos")]
fn live_process_executable_path(pid: u32) -> Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    const PROC_PIDPATHINFO_MAXSIZE: usize = 4096;
    let pid: i32 = pid
        .try_into()
        .context("process pid does not fit macOS pid_t")?;
    let mut buffer = vec![0_u8; PROC_PIDPATHINFO_MAXSIZE];
    // SAFETY: `buffer` is writable for the supplied size and remains alive for
    // the entire libproc call.
    let returned = unsafe {
        proc_pidpath(
            pid,
            buffer.as_mut_ptr().cast(),
            buffer
                .len()
                .try_into()
                .context("proc_pidpath buffer size does not fit u32")?,
        )
    };
    if returned <= 0 {
        bail!("proc_pidpath could not read executable path for pid {pid}");
    }
    let returned: usize = returned
        .try_into()
        .context("proc_pidpath returned a negative byte count")?;
    if returned > buffer.len() {
        bail!("proc_pidpath returned an oversized executable path for pid {pid}");
    }
    buffer.truncate(returned);
    while buffer.last() == Some(&0) {
        buffer.pop();
    }
    if buffer.is_empty() {
        bail!("proc_pidpath returned an empty executable path for pid {pid}");
    }
    let path = PathBuf::from(std::ffi::OsString::from_vec(buffer));
    fs::canonicalize(&path)
        .with_context(|| format!("canonicalize live executable path {}", path.display()))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn live_process_executable_path(_pid: u32) -> Result<PathBuf> {
    bail!("live executable identity is unsupported on this operating system")
}

fn verify_live_executable_path(pid: u32, expected: &Path) -> Result<()> {
    let observed = live_process_executable_path(pid)?;
    if observed != expected {
        bail!(
            "live process executable path differs for pid {pid}: expected={} observed={}",
            expected.display(),
            observed.display()
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_process_resources(pid: u32) -> Result<(u64, Option<u64>)> {
    let status_path = format!("/proc/{pid}/status");
    let status = fs::read_to_string(&status_path)
        .with_context(|| format!("read process status {status_path}"))?;
    let mut rss_kib = None;
    let mut threads = None;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            rss_kib = value.split_whitespace().next().and_then(|v| v.parse().ok());
        } else if let Some(value) = line.strip_prefix("Threads:") {
            threads = value.trim().parse().ok();
        }
    }
    let rss_kib: u64 = rss_kib.context("process status omitted VmRSS")?;
    Ok((rss_kib.saturating_mul(1024), threads))
}

#[cfg(target_os = "macos")]
fn read_process_resources(pid: u32) -> Result<(u64, Option<u64>)> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .context("run ps for process RSS")?;
    if !output.status.success() {
        bail!("ps could not sample pid {pid}");
    }
    let text = String::from_utf8(output.stdout).context("ps returned non-UTF8 RSS output")?;
    let rss_kib: u64 = text.trim().parse().context("parse ps RSS")?;
    let thread_output = Command::new("ps")
        .args(["-M", &pid.to_string()])
        .output()
        .context("run ps for process threads")?;
    let threads = thread_output.status.success().then(|| {
        String::from_utf8_lossy(&thread_output.stdout)
            .lines()
            .count()
            .saturating_sub(1) as u64
    });
    Ok((rss_kib.saturating_mul(1024), threads))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_process_resources(pid: u32) -> Result<(u64, Option<u64>)> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-o", "nlwp=", "-p", &pid.to_string()])
        .output()
        .context("run ps for process resources")?;
    if !output.status.success() {
        bail!("ps could not sample pid {pid}");
    }
    let text = String::from_utf8(output.stdout).context("ps returned non-UTF8 output")?;
    let mut fields = text.split_whitespace();
    let rss_kib: u64 = fields
        .next()
        .context("ps omitted RSS")?
        .parse()
        .context("parse ps RSS")?;
    let threads = fields.next().and_then(|value| value.parse().ok());
    Ok((rss_kib.saturating_mul(1024), threads))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_current_process_and_reports_high_water() {
        let mut sampler = ProcessResourceSampler::new();
        sampler.sample_cluster().expect("sample current process");
        let high = sampler.high_water("fe").expect("high water exists");
        assert!(high.rss_bytes > 0);
        assert_eq!(sampler.samples().len(), 1);
        assert!(sampler.samples()[0].unavailable_reason.is_none());
    }

    #[test]
    fn monitor_stops_and_persists_samples() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-process-resources-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create sample directory");
        let monitor = ProcessResourceMonitor::start(
            ClusterProcessIds {
                frontend: std::process::id(),
                backends: Vec::new(),
            },
            "run-1",
            Duration::from_millis(1),
        )
        .expect("start monitor");
        thread::sleep(Duration::from_millis(5));
        let sampler = monitor
            .finish(&root.join("samples.json"))
            .expect("finish monitor");
        assert!(!sampler.samples().is_empty());
        assert!(root.join("samples.json").is_file());
        let document: serde_json::Value = serde_json::from_slice(
            &fs::read(root.join("samples.json")).expect("read resource envelope"),
        )
        .expect("decode resource envelope");
        assert_eq!(
            document
                .as_object()
                .expect("resource envelope object")
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["processes", "run_id", "samples", "schema_version"])
        );
        assert_eq!(document["run_id"], "run-1");
        assert_eq!(document["schema_version"], 2);
        assert_eq!(document["processes"]["fe"]["pid"], std::process::id());
        let start_token = document["processes"]["fe"]["process_start_token"]
            .as_str()
            .expect("process start token");
        assert!(!start_token.is_empty());
        for sample in document["samples"].as_array().expect("resource samples") {
            let role = sample["role"].as_str().expect("sample role");
            let pid = sample["pid"].as_u64().expect("sample pid");
            assert_eq!(document["processes"][role]["pid"].as_u64(), Some(pid));
            assert_eq!(
                document["processes"][role]["process_start_token"],
                sample["process_start_token"]
            );
        }
        fs::remove_dir_all(root).expect("remove sample directory");
    }

    #[test]
    fn missing_or_reused_process_identity_fails_sampling() {
        let mut sampler = ProcessResourceSampler::new_at(
            Instant::now(),
            "missing-run".to_string(),
            BTreeMap::from([(
                "fe".to_string(),
                ProcessResourceIdentity {
                    pid: std::process::id(),
                    process_start_token: "wrong-birth-instance".to_string(),
                },
            )]),
        );
        let error = sampler
            .sample_cluster()
            .expect_err("changed process identity must fail sampling");
        assert!(error.to_string().contains("before sampling role fe"));
        assert!(sampler.samples().is_empty());
    }

    #[test]
    fn rejects_ambiguous_process_identity() {
        let pid = std::process::id();
        let error = ClusterProcessIds {
            frontend: pid,
            backends: vec![pid],
        }
        .role_pid_map()
        .expect_err("one pid cannot represent two process roles");
        assert!(error.to_string().contains("multiple roles"));
    }

    #[test]
    fn freezes_exact_frontend_and_backend_role_map() {
        let processes = ClusterProcessIds {
            frontend: 11,
            backends: vec![21, 22, 23],
        }
        .role_pid_map()
        .expect("freeze role map");
        assert_eq!(
            processes,
            BTreeMap::from([
                ("be-0".to_string(), 21),
                ("be-1".to_string(), 22),
                ("be-2".to_string(), 23),
                ("fe".to_string(), 11),
            ])
        );
    }

    #[test]
    fn process_start_token_is_stable_for_one_live_instance() {
        let pid = std::process::id();
        let first = read_process_start_token(pid).expect("read first process start token");
        let second = read_process_start_token(pid).expect("read second process start token");
        assert_eq!(first, second);
        assert!(!first.is_empty());
        assert_ne!(first, pid.to_string());
    }

    #[test]
    fn launch_identity_binds_live_process_and_executable_image() {
        let executable = std::env::current_exe().expect("resolve current test executable");
        let frozen = freeze_executable_identity(&executable).expect("freeze executable identity");
        assert_eq!(frozen.sha256.len(), 64);
        assert!(frozen.size_bytes > 0);
        assert!(frozen.modified_unix_nanos > 0);
        let identity = capture_process_launch_identity("test", std::process::id(), &frozen)
            .expect("capture current launch identity");
        assert_eq!(identity.executable, frozen);
        assert_eq!(
            recheck_process_launch_identity(&identity).expect("recheck live launch identity"),
            identity
        );
    }

    #[test]
    fn live_recheck_rejects_changed_executable_identity() {
        let executable = std::env::current_exe().expect("resolve current test executable");
        let frozen = freeze_executable_identity(&executable).expect("freeze executable identity");
        let mut identity = capture_process_launch_identity("test", std::process::id(), &frozen)
            .expect("capture current launch identity");
        identity.executable.sha256 = "0".repeat(64);
        let error = recheck_process_launch_identity(&identity)
            .expect_err("changed executable identity must fail recheck");
        assert!(error.to_string().contains("contents or metadata changed"));
    }

    #[test]
    fn resource_identities_preserve_frozen_launch_birth_tokens() {
        let executable = std::env::current_exe().expect("resolve current test executable");
        let frozen = freeze_executable_identity(&executable).expect("freeze executable identity");
        let frontend = capture_process_launch_identity("fe", std::process::id(), &frozen)
            .expect("capture frontend identity");
        let identities = ClusterProcessIdentities::from_launch_identities(&frontend, &[])
            .expect("build exact resource identities");
        assert_eq!(identities.frontend.pid, frontend.pid);
        assert_eq!(
            identities.frontend.process_start_token,
            frontend.process_start_token
        );
        let role_map = identities
            .role_process_map()
            .expect("freeze resource role map");
        assert_eq!(role_map["fe"], identities.frontend);
    }
}
