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
use std::fs;
use std::path::Path;
#[cfg(not(target_os = "linux"))]
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterProcessIds {
    pub frontend: u32,
    pub backends: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProcessResourceSample {
    pub elapsed_millis: u128,
    pub role: String,
    pub pid: u32,
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
    samples: Vec<ProcessResourceSample>,
}

pub struct ProcessResourceMonitor {
    started: Instant,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<ProcessResourceSampler>>>,
}

impl ProcessResourceMonitor {
    pub fn start(ids: ClusterProcessIds, interval: Duration) -> Result<Self> {
        if interval.is_zero() {
            bail!("process resource sampling interval must be positive");
        }
        let started = Instant::now();
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::clone(&stopped);
        let worker_started = started;
        let worker = thread::Builder::new()
            .name("process-resource-monitor".to_string())
            .spawn(move || {
                let mut sampler = ProcessResourceSampler::new_at(worker_started);
                while !worker_stopped.load(Ordering::Acquire) {
                    sampler.sample_cluster(&ids)?;
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

impl Default for ProcessResourceSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessResourceSampler {
    pub fn new() -> Self {
        Self::new_at(Instant::now())
    }

    fn new_at(started: Instant) -> Self {
        Self {
            started,
            samples: Vec::new(),
        }
    }

    pub fn sample_cluster(&mut self, ids: &ClusterProcessIds) -> Result<()> {
        self.sample("fe", ids.frontend)?;
        for (index, pid) in ids.backends.iter().copied().enumerate() {
            self.sample(format!("be-{index}"), pid)?;
        }
        Ok(())
    }

    pub fn sample(&mut self, role: impl Into<String>, pid: u32) -> Result<()> {
        let role = role.into();
        match read_process_resources(pid) {
            Ok((rss_bytes, threads)) => self.samples.push(ProcessResourceSample {
                elapsed_millis: self.started.elapsed().as_millis(),
                role,
                pid,
                rss_bytes: Some(rss_bytes),
                threads,
                unavailable_reason: None,
            }),
            Err(error) => self.samples.push(ProcessResourceSample {
                elapsed_millis: self.started.elapsed().as_millis(),
                role,
                pid,
                rss_bytes: None,
                threads: None,
                unavailable_reason: Some(format!("{error:#}")),
            }),
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
        let bytes =
            serde_json::to_vec_pretty(&self.samples).context("serialize process samples")?;
        fs::write(path, bytes)
            .with_context(|| format!("write process resource samples {}", path.display()))
    }
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
        sampler
            .sample("test", std::process::id())
            .expect("sample current process");
        let high = sampler.high_water("test").expect("high water exists");
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
            Duration::from_millis(1),
        )
        .expect("start monitor");
        thread::sleep(Duration::from_millis(5));
        let sampler = monitor
            .finish(&root.join("samples.json"))
            .expect("finish monitor");
        assert!(!sampler.samples().is_empty());
        assert!(root.join("samples.json").is_file());
        fs::remove_dir_all(root).expect("remove sample directory");
    }

    #[test]
    fn missing_process_is_recorded_without_a_zero_measurement() {
        let mut sampler = ProcessResourceSampler::new();
        sampler
            .sample("missing", u32::MAX)
            .expect("record unavailable process");
        let sample = &sampler.samples()[0];
        assert_eq!(sample.rss_bytes, None);
        assert!(sample.unavailable_reason.is_some());
        assert!(sampler.high_water("missing").is_none());
    }
}
