#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Exercise publication against actual logger files without running CI gates."""

from pathlib import Path
import os
import re
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[3]
PUBLISHER = REPO_ROOT / "tools/ci/publish-ci-logs.py"
WRAPPER = REPO_ROOT / "tools/ci/local-full-ci.sh"
LIMIT = 1024 * 1024


class PublishCiLogsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.logs = self.root / "input"
        self.logs.mkdir()

    def invoke(self, attempt="1", logs=None, step_summary=None, artifact_dir=None):
        artifact = artifact_dir or self.root / f"artifact-{attempt}"
        step = step_summary or self.root / f"step-{attempt}.md"
        output = self.root / f"output-{attempt}"
        result = subprocess.run(
            [sys.executable, str(PUBLISHER), "--log-root", str(logs or self.logs),
             "--artifact-dir", str(artifact), "--step-summary", str(step),
             "--github-output", str(output)],
            capture_output=True, timeout=15,
        )
        return result, artifact, step, output

    def assert_locator(self, output, artifact):
        lines = output.read_text().splitlines()
        self.assertEqual(len(lines), 3)
        delimiter = lines[0].removeprefix("artifact_dir<<")
        self.assertTrue(delimiter)
        self.assertEqual(lines[1], str(artifact.absolute()))
        self.assertEqual(lines[2], delimiter)

    def actual_stages(self, code):
        run = self.logs / f"run-{code}"
        run.mkdir()
        script = r'''
set +e
source "$1" --source-only
CI_RUN_DIR="$2"
CI_SUMMARY="$CI_RUN_DIR/summary.md"
ci_init_summary_state
ci_set_repo_context "$REPO_ROOT" fixture fixture-sha
ci_render_summary RUNNING
run_fail_fast_stage "fixture stage" "fixture.log" bash -c '
  for ((i=0; i<60; i++)); do printf "line-%02d\n" "$i"; done
  printf "fixture diagnostic\n" >&2
  exit "$1"
' _ "$3"
printf "following gate\n" >"$CI_RUN_DIR/following"
run_fail_fast_stage "following stage" "following.log" printf "success\n"
ci_render_summary PASS
'''
        result = subprocess.run(
            ["bash", "-c", script, "fixture", str(WRAPPER), str(run), str(code)],
            cwd=REPO_ROOT, capture_output=True, timeout=15,
        )
        return result, run

    def test_original_stage_success_and_logs_are_preserved(self):
        original, run = self.actual_stages(0)
        self.assertEqual(original.returncode, 0, original.stderr)
        summary = (run / "summary.md").read_bytes()
        self.assertIn(b"- Status: PASS", summary)
        self.assertIn(b"| following stage | PASS |", summary)
        before = {p.relative_to(self.logs): p.read_bytes()
                  for p in self.logs.rglob("*") if p.is_file()}
        result, artifact, step, output = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_locator(output, artifact)
        self.assertIn(summary, result.stdout)
        self.assertIn(summary, step.read_bytes())
        for relative, contents in before.items():
            self.assertEqual((artifact / "logs" / relative).read_bytes(), contents)
            self.assertEqual((self.logs / relative).read_bytes(), contents)

    def test_original_fail_fast_exit_and_full_diagnostics(self):
        for code in (37, 75):
            with self.subTest(code=code):
                original, run = self.actual_stages(code)
                self.assertEqual(original.returncode, code, original.stderr)
                self.assertFalse((run / "following").exists())
                self.assertFalse((run / "following.log").exists())
                summary = (run / "summary.md").read_bytes()
                log = (run / "fixture.log").read_bytes()
                self.assertIn(b"- Status: FAIL", summary)
                self.assertIn(b"| fixture stage | FAIL |", summary)
                self.assertIn(b"fixture diagnostic", summary)
                tail = b"\n".join(log.splitlines()[-40:])
                self.assertIn(tail, summary)
                self.assertNotIn(b"line-00\n", summary)
                self.assertIn(b"line-00\n", log)
                self.assertTrue(log.startswith(b"+ bash -c "))
                result, artifact, step, output = self.invoke(str(code))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assert_locator(output, artifact)
                self.assertIn(summary, result.stdout)
                self.assertIn(summary, step.read_bytes())
                self.assertEqual((artifact / "logs" / run.name / "fixture.log").read_bytes(), log)

    def test_multiple_summaries_and_binary_unicode_logs(self):
        summaries = []
        for name, status in (("a", "FAIL"), ("b", "RUNNING"), ("c", "PASS")):
            run = self.logs / name
            run.mkdir()
            text = f"# Original {name}\nStatus: {status}\n".encode()
            (run / "summary.md").write_bytes(text)
            summaries.append(text)
        payload = bytes(range(256)) + "Unicode: \u6d4b\u8bd5\n".encode()
        (self.logs / "a" / "binary.log").write_bytes(payload)
        result, artifact, step, _ = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        positions = [result.stdout.index(text) for text in summaries]
        self.assertEqual(positions, sorted(positions))
        for text in summaries:
            self.assertIn(text, step.read_bytes())
        self.assertEqual((artifact / "logs/a/binary.log").read_bytes(), payload)

    def test_missing_empty_and_stage_only_logs_do_not_infer_gate_status(self):
        for name in ("missing", "empty", "early"):
            with self.subTest(name=name):
                logs = self.root / name
                if name != "missing":
                    logs.mkdir()
                if name == "early":
                    (logs / "early.log").write_bytes(b"early stage output\n")
                result, artifact, step, output = self.invoke(name, logs=logs)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assert_locator(output, artifact)
                status = (artifact / "publication-status.txt").read_text()
                self.assertIn("Original gate verdict is not inferred.", status)
                self.assertNotIn("Status: PASS", status)
                self.assertNotIn("Status: FAIL", status)
                self.assertIn(b"missing" if name == "missing" else b"No CI summary", step.read_bytes())
                if name == "early":
                    self.assertEqual((artifact / "logs/early.log").read_bytes(), b"early stage output\n")

    def test_only_non_hidden_regular_files_are_copied(self):
        external = self.root / "outside"
        external.mkdir()
        (external / "private.log").write_bytes(b"not in log domain")
        (self.logs / "ordinary.log").write_bytes(b"ordinary")
        (self.logs / ".hidden").write_bytes(b"hidden")
        (self.logs / ".directory").mkdir()
        (self.logs / ".directory/file").write_bytes(b"hidden directory")
        (self.logs / "file-link").symlink_to(external / "private.log")
        (self.logs / "directory-link").symlink_to(external, target_is_directory=True)
        os.mkfifo(self.logs / "pipe")
        result, artifact, _, _ = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        paths = {p.relative_to(artifact / "logs").as_posix()
                 for p in (artifact / "logs").rglob("*")}
        self.assertEqual(paths, {"ordinary.log"})

    def test_existing_staging_is_rejected_without_locator(self):
        artifact = self.root / "existing"
        artifact.mkdir()
        (artifact / "unknown").write_bytes(b"untouched")
        result, _, _, output = self.invoke(artifact_dir=artifact)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())
        self.assertEqual((artifact / "unknown").read_bytes(), b"untouched")
        self.assertFalse((artifact / "publication-status.txt").exists())

    def test_nested_paths_and_symbolic_log_root_are_rejected(self):
        cases = [
            (self.logs, self.logs / "child"),
            (self.logs / "child", self.logs),
            (self.logs, self.logs),
        ]
        link = self.root / "input-link"
        link.symlink_to(self.logs, target_is_directory=True)
        cases.append((link, self.root / "new-artifact"))
        for index, (logs, artifact) in enumerate(cases):
            with self.subTest(index=index):
                result, _, _, output = self.invoke(str(index), logs=logs, artifact_dir=artifact)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

    def test_post_creation_summary_io_failure_preserves_owned_locator(self):
        (self.logs / "summary.md").write_bytes(b"original summary")
        step = self.root / "step-directory"
        step.mkdir()
        result, artifact, _, output = self.invoke(step_summary=step)
        self.assertNotEqual(result.returncode, 0)
        self.assert_locator(output, artifact)
        self.assertIn(b"CI log publication failed:", result.stderr)
        self.assertEqual((artifact / "logs/summary.md").read_bytes(), b"original summary")
        status = (artifact / "publication-status.txt").read_text()
        self.assertIn("Snapshot: complete", status)
        self.assertIn("Summary publication: failed", status)

    @unittest.skipIf(os.geteuid() == 0, "root bypasses the unreadable-file fixture")
    def test_copy_io_failure_reports_partial_owned_snapshot(self):
        (self.logs / "a.log").write_bytes(b"first complete file")
        unreadable = self.logs / "z.log"
        unreadable.write_bytes(b"unreadable file")
        unreadable.chmod(0)
        try:
            result, artifact, _, output = self.invoke()
        finally:
            unreadable.chmod(0o600)
        self.assertNotEqual(result.returncode, 0)
        self.assert_locator(output, artifact)
        self.assertEqual((artifact / "logs/a.log").read_bytes(), b"first complete file")
        self.assertFalse((artifact / "logs/z.log").exists())
        status = (artifact / "publication-status.txt").read_text()
        self.assertIn("Snapshot: partial", status)
        self.assertIn("Files copied: 1", status)
        self.assertIn("Summary publication: failed", status)

    def test_step_summary_exact_limit_and_overflow_keep_full_snapshot(self):
        summary = self.logs / "summary.md"
        summary.write_bytes(b"x" * 128)
        result, _, step, _ = self.invoke("measure")
        self.assertEqual(result.returncode, 0, result.stderr)
        overhead = len(step.read_bytes()) - 128
        prefix = b"existing summary\n"
        for excess in (0, 1):
            with self.subTest(excess=excess):
                contents = b"x" * (LIMIT - len(prefix) - overhead + excess)
                summary.write_bytes(contents)
                step = self.root / f"bounded-{excess}"
                step.write_bytes(prefix)
                result, artifact, _, _ = self.invoke(f"limit-{excess}", step_summary=step)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue(step.read_bytes().startswith(prefix))
                self.assertLessEqual(step.stat().st_size, LIMIT)
                self.assertIn(contents, result.stdout)
                self.assertEqual((artifact / "logs/summary.md").read_bytes(), contents)
                if excess == 0:
                    self.assertEqual(step.stat().st_size, LIMIT)
                    self.assertIn(contents, step.read_bytes())
                else:
                    self.assertIn(b"were not appended in full", step.read_bytes())

    def test_full_existing_summary_is_never_overwritten(self):
        (self.logs / "summary.md").write_bytes(b"new summary")
        step = self.root / "full.md"
        previous = b"p" * LIMIT
        step.write_bytes(previous)
        result, _, _, _ = self.invoke(step_summary=step)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(step.read_bytes(), previous)
        self.assertIn(b"No space remains", result.stdout)

    def test_console_preserves_command_text_in_fresh_disabled_intervals(self):
        original = b"::error::literal diagnostic\n::set-output name=x::literal\n"
        (self.logs / "summary.md").write_bytes(original)
        tokens = []
        for attempt in ("one", "two"):
            result, _, step, _ = self.invoke(attempt)
            self.assertEqual(result.returncode, 0, result.stderr)
            token = re.search(rb"^::stop-commands::([0-9a-f]+)\n", result.stdout).group(1)
            tokens.append(token)
            end = b"\n::" + token + b"::\n"
            self.assertTrue(result.stdout.endswith(end))
            self.assertLess(result.stdout.index(original), result.stdout.index(end))
            self.assertIn(original, step.read_bytes())
        self.assertNotEqual(tokens[0], tokens[1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
