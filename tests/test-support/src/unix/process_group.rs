// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Native observation while the original group leader remains unreaped.
use std::{io, process::Child};

#[cfg(target_os = "linux")]
pub fn running(child: &Child) -> io::Result<bool> {
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().parse::<u32>().is_err() {
            continue;
        }
        let text = match std::fs::read_to_string(entry.path().join("stat")) {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if linux_stat_in_live_group(&text, child.id())? {
            return Ok(true);
        }
    }
    Ok(false)
}

// Linux do_task_stat initializes pgid to -1 and only replaces it while
// lock_task_sighand succeeds. A concurrently exiting task can retain that
// signed sentinel; it cannot belong to the original positive, pinned PGID.
#[cfg(any(target_os = "linux", test))]
fn linux_stat_in_live_group(text: &str, expected_group: u32) -> io::Result<bool> {
    let (_, fields) = text
        .rsplit_once(')')
        .ok_or_else(|| super::error("invalid native process stat"))?;
    let fields: Vec<_> = fields.split_whitespace().collect();
    let group: i32 = fields
        .get(2)
        .ok_or_else(|| super::error("missing native process group"))?
        .parse()
        .map_err(super::error)?;
    if group < -1 {
        return Err(super::error("invalid native process group"));
    }
    Ok(i64::from(group) == i64::from(expected_group)
        && !matches!(fields.first(), Some(&"Z" | &"X")))
}

#[cfg(target_os = "macos")]
pub fn running(child: &Child) -> io::Result<bool> {
    use std::ffi::c_void;
    unsafe extern "C" {
        fn proc_listpids(kind: u32, group: u32, buffer: *mut c_void, size: i32) -> i32;
        fn proc_pidinfo(pid: i32, flavor: i32, arg: u64, buffer: *mut c_void, size: i32) -> i32;
    }
    // Darwin proc_bsdshortinfo from sys/proc_info.h. All fields have native
    // uint32_t/uid_t/gid_t layout; PROC_PIDT_SHORTBSDINFO=13, SZOMB=5.
    #[repr(C)]
    struct ShortInfo {
        pid: u32,
        parent: u32,
        group: u32,
        status: u32,
        command: [u8; 16],
        remaining: [u32; 8],
    }
    let mut capacity = 64;
    loop {
        let mut pids = vec![0i32; capacity];
        let size = (pids.len() * size_of::<i32>()) as i32;
        // SAFETY: writable aligned PID array of the exact declared size;
        // PROC_PGRP_ONLY=2 limits observation to the unreaped leader's group.
        let bytes = unsafe { proc_listpids(2, child.id(), pids.as_mut_ptr().cast(), size) };
        if bytes < 0 {
            return Err(io::Error::last_os_error());
        }
        if bytes == size {
            if capacity >= 65536 {
                return Err(super::error("process group enumeration limit exceeded"));
            }
            capacity *= 2;
            continue;
        }
        if bytes % size_of::<i32>() as i32 != 0 {
            return Err(super::error("invalid native process group enumeration"));
        }
        for pid in pids
            .into_iter()
            .take(bytes as usize / size_of::<i32>())
            .filter(|pid| *pid > 0)
        {
            // SAFETY: every field is an integer/byte; zero initializes valid output storage.
            let mut info: ShortInfo = unsafe { std::mem::zeroed() };
            let size = size_of::<ShortInfo>() as i32;
            // SAFETY: writable aligned storage matches the native declared ABI.
            let length =
                unsafe { proc_pidinfo(pid, 13, 0, (&mut info as *mut ShortInfo).cast(), size) };
            if length != size {
                let error = io::Error::last_os_error();
                if matches!(error.raw_os_error(), Some(3)) {
                    continue;
                }
                return Err(super::error(format!(
                    "observe native process group member: {error}"
                )));
            }
            if info.group == child.id() && info.status != 5 {
                return Ok(true);
            }
        }
        return Ok(false);
    }
}

#[cfg(test)]
mod tests {
    use super::linux_stat_in_live_group;

    // CI maintainers exercise the same raw-stat decoder as the native Linux
    // monitor. Kernel signed fields are the oracle, independent of local /proc.
    fn stat(state: &str, group: &str) -> String {
        format!("4812 (worker with ) name) {state} 47 {group} 47 0 -1 4194304 0 0 0\n")
    }

    #[test]
    fn linux_signed_group_sentinels_do_not_match_the_original_group() {
        for group in ["-1", "0"] {
            assert!(!linux_stat_in_live_group(&stat("R", group), 4812).unwrap());
        }
        assert!(linux_stat_in_live_group(&stat("R", "4812"), 4812).unwrap());
        assert!(!linux_stat_in_live_group(&stat("S", "4813"), 4812).unwrap());
    }

    #[test]
    fn linux_exited_members_do_not_keep_the_original_group_running() {
        for state in ["Z", "X"] {
            assert!(!linux_stat_in_live_group(&stat(state, "4812"), 4812).unwrap());
        }
        assert!(linux_stat_in_live_group(&stat("S", "4812"), 4812).unwrap());
    }

    #[test]
    fn linux_invalid_group_fields_still_fail_observation() {
        for raw in [
            stat("R", "not-a-group"),
            stat("R", "2147483648"),
            stat("R", "-2"),
            "4812 (worker) R 47".into(),
            "4812 worker R 47 4812".into(),
        ] {
            assert!(linux_stat_in_live_group(&raw, 4812).is_err(), "{raw}");
        }
    }
}
