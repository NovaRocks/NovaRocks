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

"""Publish existing CI diagnostics without interpreting their gate verdicts."""

import argparse
import os
from pathlib import Path
import shutil
import stat
import sys
import uuid


STEP_SUMMARY_LIMIT = 1024 * 1024


def regular_files(root):
    for entry in sorted(os.scandir(root), key=lambda item: item.name):
        if entry.name.startswith("."):
            continue
        mode = entry.stat(follow_symlinks=False).st_mode
        path = Path(entry.path)
        if stat.S_ISDIR(mode):
            yield from regular_files(path)
        elif stat.S_ISREG(mode):
            yield path


def summary_heading(relative):
    return f"\n## CI summary: {relative.as_posix()}\n\n".encode("utf-8")


def publish_summaries(summaries, notice, step_summary):
    token = uuid.uuid4().hex
    console = sys.stdout.buffer
    console.write(f"::stop-commands::{token}\n".encode())
    try:
        for relative, path in summaries:
            console.write(summary_heading(relative))
            with path.open("rb") as source:
                shutil.copyfileobj(source, console)
            console.write(b"\n")
        console.write(notice)
        if step_summary is None:
            return

        previous_size = step_summary.stat().st_size if step_summary.exists() else 0
        payload_size = len(notice) + sum(
            len(summary_heading(relative)) + path.stat().st_size + 1
            for relative, path in summaries
        )
        if previous_size + payload_size <= STEP_SUMMARY_LIMIT:
            with step_summary.open("ab") as destination:
                for relative, path in summaries:
                    destination.write(summary_heading(relative))
                    with path.open("rb") as source:
                        shutil.copyfileobj(source, destination)
                    destination.write(b"\n")
                destination.write(notice)
        else:
            limit_notice = (
                b"\nCI summaries were not appended in full: the step summary "
                b"would exceed 1 MiB. Full files are in the owned log snapshot; "
                b"remote availability requires a successful artifact upload.\n"
            )
            console.write(limit_notice)
            if previous_size + len(limit_notice) <= STEP_SUMMARY_LIMIT:
                with step_summary.open("ab") as destination:
                    destination.write(limit_notice)
            else:
                console.write(b"No space remains for a step-summary notice.\n")
    finally:
        console.write(f"\n::{token}::\n".encode())
        console.flush()


def publish(log_root, artifact_dir, step_summary=None, github_output=None):
    source_root = log_root.absolute()
    destination_root = artifact_dir.absolute()
    source_resolved = source_root.resolve()
    destination_resolved = destination_root.resolve()
    if (
        source_resolved == destination_resolved
        or source_resolved in destination_resolved.parents
        or destination_resolved in source_resolved.parents
    ):
        raise ValueError("CI log and artifact directories must not contain each other")

    if source_root.is_symlink():
        raise ValueError("CI log root must be a directory without a symbolic link")
    missing = not source_root.exists()
    if not missing and not stat.S_ISDIR(source_root.lstat().st_mode):
        raise ValueError("CI log root must be a directory without a symbolic link")
    destination_root.mkdir(parents=True, exist_ok=False)
    status_path = destination_root / "publication-status.txt"
    copied = 0
    snapshot = "missing" if missing else "partial"

    def write_status(publication):
        status_path.write_text(
            f"Snapshot: {snapshot}\nFiles copied: {copied}\n"
            f"Summary publication: {publication}\n"
            "Scope: non-hidden regular files; symbolic links are not followed.\n"
            "Original gate verdict is not inferred.\n",
            encoding="utf-8",
        )

    try:
        write_status("pending")
        if github_output is not None:
            delimiter = uuid.uuid4().hex
            with github_output.open("a", encoding="utf-8") as output:
                output.write(
                    f"artifact_dir<<{delimiter}\n{destination_root}\n{delimiter}\n"
                )

        summaries = []
        if not missing:
            for source in regular_files(source_root):
                relative = source.relative_to(source_root)
                destination = destination_root / "logs" / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
                copied += 1
                if source.name == "summary.md":
                    summaries.append((relative, destination))
            snapshot = "complete"
        write_status("pending")

        if missing:
            notice = b"\nCI log directory is missing; original gate status is unknown.\n"
        elif not summaries:
            notice = b"\nNo CI summary was found; available regular logs were copied.\n"
        else:
            notice = b"\nCI summaries are reproduced without changing gate verdicts.\n"
        publish_summaries(summaries, notice, step_summary)
        write_status("complete")
    except (OSError, ValueError):
        try:
            write_status("failed")
        except OSError:
            pass
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log-root", type=Path, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--step-summary", type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    try:
        publish(args.log_root, args.artifact_dir, args.step_summary, args.github_output)
    except (OSError, ValueError) as error:
        print(f"CI log publication failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
