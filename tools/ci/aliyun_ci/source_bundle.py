# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Self-contained exact committed source delivery for local remote validation.

CI maintainers own this functional source transport. Its independent oracle is
Git commit/tree identity and SHA-256 of every tracked blob, including symlinks.
No repository configuration, remotes, credentials or ignored files are copied.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess


def _git(repo, *arguments, input=None):
    environment = os.environ.copy()
    environment.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_NO_REPLACE_OBJECTS='1', GIT_TERMINAL_PROMPT='0')
    return subprocess.run(['git', '-C', str(repo), *arguments], input=input,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, env=environment).stdout


def _identity(repo):
    return (_git(repo, 'rev-parse', 'HEAD').decode().strip(),
            _git(repo, 'rev-parse', 'HEAD^{tree}').decode().strip())


def _clean(repo):
    if _git(repo, 'status', '--porcelain=v1', '--untracked-files=normal'):
        raise ValueError('source worktree must be clean, including untracked files')


def _members(repo, sha):
    rows = _git(repo, 'ls-tree', '-rz', '--full-tree', sha).split(b'\0')
    parsed = []
    for row in rows:
        if not row:
            continue
        metadata, raw_path = row.split(b'\t', 1)
        mode, kind, oid = metadata.decode().split()
        path = raw_path.decode('utf-8')
        if kind != 'blob' or mode not in ('100644', '100755', '120000'):
            raise ValueError('source bundle requires tracked regular files or symlinks; submodules are unsupported')
        parsed.append((mode, oid, path))
    data = _git(repo, 'cat-file', '--batch', input=''.join(oid + '\n' for _, oid, _ in parsed).encode())
    offset, members = 0, []
    for mode, oid, path in parsed:
        end = data.index(b'\n', offset)
        actual, kind, length = data[offset:end].decode().split()
        size = int(length)
        if actual != oid or kind != 'blob':
            raise ValueError('Git object identity differs while reading source')
        offset = end + 1
        blob = data[offset:offset + size]
        offset += size + 1
        members.append({'path': path, 'mode': mode, 'size': size,
                        'sha256': hashlib.sha256(blob).hexdigest()})
    return members


def create(repo, output_dir, sha=None):
    """Create exclusively after preflight; the requested SHA must be clean HEAD."""
    repo = Path(repo).resolve(strict=True)
    if _git(repo, 'rev-parse', '--show-toplevel').decode().strip() != str(repo):
        raise ValueError('source must be the repository worktree root')
    _clean(repo)
    actual, tree = _identity(repo)
    sha = sha or actual
    if not re.fullmatch(r'[0-9a-f]{40}', sha) or sha != actual:
        raise ValueError('requested source SHA must equal the complete clean HEAD')
    members = _members(repo, sha)
    destination = Path(output_dir)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    bundle = destination / 'source.bundle'
    _git(repo, 'bundle', 'create', str(bundle.resolve()), 'HEAD')
    _clean(repo)
    if _identity(repo) != (sha, tree):
        raise ValueError('source identity changed while creating bundle')
    with bundle.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    manifest = {'schema': 1, 'sha': sha, 'tree': tree, 'bundle_sha256': digest, 'members': members}
    (destination / 'source.json').write_text(json.dumps(manifest, sort_keys=True) + '\n')
    return manifest


def import_bundle(bundle, manifest, destination):
    """Import without local config/remote, then verify every checked out blob."""
    bundle, destination = Path(bundle).resolve(strict=True), Path(destination)
    if manifest.get('schema') != 1 or not re.fullmatch(r'[0-9a-f]{40}', manifest.get('sha', '')):
        raise ValueError('unsupported source manifest or commit identity')
    with bundle.open('rb') as source:
        if hashlib.file_digest(source, 'sha256').hexdigest() != manifest['bundle_sha256']:
            raise ValueError('source bundle SHA-256 differs')
    destination.mkdir(mode=0o750, parents=True, exist_ok=False)
    _git(destination, 'init', '--quiet')
    _git(destination, 'bundle', 'verify', str(bundle))
    _git(destination, 'fetch', '--quiet', '--no-tags', str(bundle), 'HEAD')
    _git(destination, '-c', 'core.hooksPath=/dev/null', 'checkout', '--quiet', '--detach', manifest['sha'])
    if _identity(destination) != (manifest['sha'], manifest['tree']):
        raise ValueError('imported source commit/tree differs')
    actual = _members(destination, manifest['sha'])
    if actual != manifest['members']:
        raise ValueError('imported Git source member manifest differs')
    for row in actual:
        relative = PurePosixPath(row['path'])
        if relative.is_absolute() or '..' in relative.parts or relative.parts[0] == '.git':
            raise ValueError('invalid source member path')
        path = destination / row['path']
        data = os.fsencode(os.readlink(path)) if row['mode'] == '120000' else path.read_bytes()
        if len(data) != row['size'] or hashlib.sha256(data).hexdigest() != row['sha256']:
            raise ValueError(f"checked out source differs: {row['path']}")
        if row['mode'] != '120000' and bool(path.stat().st_mode & 0o111) != (row['mode'] == '100755'):
            raise ValueError(f"checked out source executable mode differs: {row['path']}")
    _clean(destination)
    return {'sha': manifest['sha'], 'tree': manifest['tree'], 'members_verified': len(actual)}
