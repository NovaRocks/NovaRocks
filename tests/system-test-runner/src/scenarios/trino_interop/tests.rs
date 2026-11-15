// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file for additional
// information. Licensed under the Apache License, Version 2.0.

use super::*;
use std::fs::File;
use std::time::Duration;

#[test]
fn probe_and_cleanup_observe_the_original_payload_group_exit() {
    for mode in ["encoding-conformance", "cleanup"] {
        let root = tempfile::tempdir().unwrap();
        let original_work = Instant::now() + Duration::from_secs(5);
        let original_complete = original_work + Duration::from_secs(5);
        let mut command = ManagedCommand::new("sh");
        command.args(["-c", "exit 0"]);
        let capture_deadline =
            bind_probe_lifecycle(&mut command, mode, original_work, original_complete);
        assert_eq!(capture_deadline, original_complete);
        let outcome = novarocks_test_support::capture_command_until(
            command,
            File::create(root.path().join("stdout")).unwrap(),
            File::create(root.path().join("stderr")).unwrap(),
            capture_deadline,
        )
        .unwrap();
        assert!(outcome.output.status.success());
        assert!(!outcome.timed_out && outcome.failures.is_empty());
        assert!(outcome.group_exit_observed(), "{mode}");
        outcome.release_diagnostics().unwrap();
    }
}
