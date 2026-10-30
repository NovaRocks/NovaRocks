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

"""Atomic, immutable worktree configuration; no service ownership or references."""
from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import shutil
import uuid

import fixture_runtime as runtime

SCHEMA = 2
# Presentation changes get a fresh immutable identity without rewriting files
# still pinned by an existing consumer. Readers verify older schema-2 records.
RENDERER_REVISION = 4


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(entry: Path):
    entry = entry.resolve()
    pointer = entry / "published"
    if not pointer.is_symlink():
        if pointer.exists() or any((entry / n).exists() or (entry / n).is_symlink() for n in runtime.ENTRY_FILES):
            raise runtime.RuntimeFailure("RuntimeStateUnreadable", "entry has no publication pointer")
        return None
    directory = pointer.resolve(strict=False)
    if directory.parent != (entry / "publications").resolve():
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "publication points outside its entry")
    metadata = runtime.read_json(directory / "entry.json")
    if metadata.get("schema") != SCHEMA or metadata.get("entry_root") != str(entry):
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "obsolete or foreign entry; prepare it explicitly")
    if runtime.digest(metadata) != directory.name:
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "publication metadata changed")
    files = runtime.read_json(directory / "files.json")
    if set(files) != set(runtime.ENTRY_FILES):
        raise runtime.RuntimeFailure("IncompletePublication", "publication file set differs")
    for name, expected in files.items():
        path = directory / name
        if not path.is_file() or path.is_symlink() or _hash(path) != expected:
            raise runtime.RuntimeFailure("IncompletePublication", name)
    return {**metadata, "published_dir": str(directory)}


def publish(entry: Path, metadata, renderer, *, hook=lambda name: None):
    """Caller holds the entry lock, and the machine lock for ready publications."""
    entry = entry.resolve()
    value = copy.deepcopy(metadata)
    value.pop("published_dir", None)
    value.update(schema=SCHEMA, entry_root=str(entry), renderer_revision=RENDERER_REVISION)
    config = value['config']
    if config.get('update_current'):
        current = Path(config['current_link'])
        if current.parent.resolve() != entry.parent or (current.exists() and not current.is_symlink()):
            raise runtime.RuntimeFailure('RuntimeIdentityMismatch', 'invalid current entry locator')
    identity = runtime.digest(value)
    directory = entry / "publications" / identity
    staging = directory.with_name(".prepare-" + uuid.uuid4().hex)
    staging.mkdir(parents=True, mode=0o700)
    context = {**value, "env_id": value["worktree"], "publication_dir": str(directory),
               "stable_runtime_dir": str(entry)}
    try:
        runtime.atomic_json(staging / "entry.json", value)
        renderer(context, staging)
        hashes = {}
        for name in runtime.ENTRY_FILES:
            path = staging / name
            if not path.is_file() or path.is_symlink():
                raise runtime.RuntimeFailure("IncompletePublication", name)
            hashes[name] = _hash(path)
        runtime.atomic_json(staging / "files.json", hashes)
        for path in staging.iterdir():
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
        runtime.fsync_directory(staging)
        hook("publication.prepared")
        if directory.exists():
            if runtime.read_json(directory / "entry.json") != value or runtime.read_json(directory / "files.json") != hashes:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "immutable publication differs")
            for name, expected in hashes.items():
                if (directory / name).is_symlink() or _hash(directory / name) != expected:
                    raise runtime.RuntimeFailure("IncompletePublication", name)
        else:
            os.rename(staging, directory)
            runtime.fsync_directory(directory.parent)
        for name in runtime.ENTRY_FILES:
            link = entry / name
            target = "published/" + name
            if link.is_symlink() and os.readlink(link) == target:
                continue
            temporary = entry / (".link-" + uuid.uuid4().hex)
            temporary.symlink_to(target)
            os.replace(temporary, link)
        runtime.fsync_directory(entry)
        hook("publication.before_swap")
        temporary = entry / (".published-" + uuid.uuid4().hex)
        temporary.symlink_to(Path("publications") / identity)
        os.replace(temporary, entry / "published")
        runtime.fsync_directory(entry)
        config = value["config"]
        if config.get("update_current"):
            current = Path(config["current_link"])
            if current.parent.resolve() != entry.parent or (current.exists() and not current.is_symlink()):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "invalid current entry locator")
            temporary = current.with_name(".current-" + uuid.uuid4().hex)
            temporary.symlink_to(entry.name)
            os.replace(temporary, current)
            runtime.fsync_directory(current.parent)
        return {**value, "published_dir": str(directory)}
    finally:
        if staging.exists():
            shutil.rmtree(staging)
