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

"""Archive original attempt files and verify them outside the disposable worker.

CI maintainers own this low-cost functional asset. It never interprets Rust
assertions, retries workloads, or turns archive completeness into test success.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import gzip
import hashlib
import io
import json
import os
import pwd
import shutil
import stat
import subprocess
import sys
from pathlib import Path, PurePosixPath
import tarfile
import tempfile
import time
import http.client
from urllib.parse import urlsplit

from .ports import MANIFEST_SCHEMA
from .instance.swap import SWAP_ROOT


def _digest_file(path: Path) -> str:
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def _safe_name(name: str) -> bool:
    path = PurePosixPath(name)
    return bool(name) and not path.is_absolute() and '..' not in path.parts and str(path) == name


class _HashingReader:
    def __init__(self, source):
        self.source = source
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        data = self.source.read(size)
        self.digest.update(data)
        return data


def _files(root: Path, prefix: str):
    if root.resolve().is_relative_to(SWAP_ROOT.resolve()):
        raise ValueError('private worker swap storage must never be archived')
    if root.is_symlink():
        raise ValueError(f'archive input is a symbolic link: {root}')
    if root.is_file():
        yield root, prefix
        return
    if not root.is_dir():
        raise FileNotFoundError(root)
    for path in sorted(root.rglob('*')):
        if path.resolve().is_relative_to(SWAP_ROOT.resolve()):
            raise ValueError('private worker swap storage must never be archived')
        if path.is_symlink():
            raise ValueError(f'archive input contains a symbolic link: {path}')
        if path.is_file():
            yield path, f'{prefix}/{path.relative_to(root).as_posix()}'
        elif not path.is_dir():
            raise ValueError(f'archive input is not a regular file: {path}')


def pack(attempt_root: str | Path, diagnostics: dict[str, str | Path] | None = None,
         *, kind: str = 'final', output: str | Path | None = None) -> Path:
    """Write one archive exclusively. Hash exactly the bytes placed into tar.

    Diagnostics are explicit non-secret paths. Callers must never pass JIT,
    instance-role material or bootstrap files containing presigned URLs.
    """
    if kind not in ('final', 'interrupted'):
        raise ValueError('archive kind must be final or interrupted')
    if Path(attempt_root).is_symlink():
        raise ValueError('archive input is a symbolic link')
    attempt = Path(attempt_root).resolve(strict=True)
    if output is None:
        directory = Path(tempfile.mkdtemp(prefix='novarocks-ci-handoff-'))
        destination = directory / f'{kind}.tar.gz'
    else:
        destination = Path(output).resolve()
    if destination.is_relative_to(attempt):
        raise ValueError('archive output must be outside the attempt input')
    inputs = list(_files(attempt, 'attempt'))
    for name, path in sorted((diagnostics or {}).items()):
        if not _safe_name(name):
            raise ValueError('invalid diagnostic archive name')
        inputs.extend(_files(Path(path), f'diagnostics/{name}'))
    names = [name for _, name in inputs]
    if len(names) != len(set(names)):
        raise ValueError('archive members are not unique')
    manifest = {'schema': MANIFEST_SCHEMA, 'kind': kind, 'complete': kind == 'final', 'members': []}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('xb') as raw:
        with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w|') as archive:
                for path, name in inputs:
                    with path.open('rb') as source:
                        size = source.seek(0, 2)
                        source.seek(0)
                        reader = _HashingReader(source)
                        info = tarfile.TarInfo(name)
                        info.size = size
                        info.mode = 0o600
                        archive.addfile(info, reader)
                    manifest['members'].append({'path': name, 'size': size, 'sha256': reader.digest.hexdigest()})
                data = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()
                info = tarfile.TarInfo('MANIFEST.json')
                info.size = len(data)
                info.mode = 0o600
                archive.addfile(info, io.BytesIO(data))
    return destination


def verify(source: bytes | str | Path) -> dict:
    """Validate all member bytes and the gzip trailer without extracting files."""
    fact = {'present': True, 'kind': None, 'complete': False, 'manifest_ok': False,
            'missing_members': [], 'errors': [], 'archive_sha256': None}
    if not isinstance(source, bytes) and not Path(source).exists():
        return dict(fact, present=False, errors=['archive absent'])
    try:
        with ExitStack() as stack:
            raw = io.BytesIO(source) if isinstance(source, bytes) else stack.enter_context(Path(source).open('rb'))
            fact['archive_sha256'] = hashlib.file_digest(raw, 'sha256').hexdigest()
            raw.seek(0)
            compressed = stack.enter_context(gzip.GzipFile(fileobj=raw, mode='rb'))
            archive = stack.enter_context(tarfile.open(fileobj=compressed, mode='r:'))
            actual, manifest = {}, None
            for member in archive:
                if not member.isfile() or not _safe_name(member.name):
                    raise ValueError('archive contains an unsafe or non-file member')
                if member.name in actual or (member.name == 'MANIFEST.json' and manifest is not None):
                    raise ValueError('duplicate archive member')
                extracted = archive.extractfile(member)
                if member.name == 'MANIFEST.json':
                    manifest = json.load(extracted)
                else:
                    actual[member.name] = {'path': member.name, 'size': member.size,
                                           'sha256': hashlib.file_digest(extracted, 'sha256').hexdigest()}
            # Tar EOF can precede the gzip checksum; consume it before accepting.
            while compressed.read(1024 * 1024):
                pass
            if not isinstance(manifest, dict) or manifest.get('schema') != MANIFEST_SCHEMA:
                raise ValueError('missing or unsupported archive manifest')
            if manifest.get('kind') not in ('final', 'interrupted') or type(manifest.get('complete')) is not bool:
                raise ValueError('invalid archive manifest facts')
            fact.update(kind=manifest['kind'], complete=manifest['complete'])
            declared = manifest.get('members')
            if not isinstance(declared, list) or any(not isinstance(row, dict) for row in declared):
                raise ValueError('invalid member manifest')
            expected = {row.get('path'): row for row in declared}
            if len(expected) != len(declared) or any(not isinstance(name, str) or not _safe_name(name) for name in expected):
                raise ValueError('invalid or duplicate manifest path')
            fact['missing_members'] = sorted(set(expected) - set(actual))
            if fact['missing_members']:
                fact['errors'].append('manifest members are absent')
            if set(actual) - set(expected):
                fact['errors'].append('archive has unmanifested members')
            for name in set(expected) & set(actual):
                if expected[name] != actual[name]:
                    fact['errors'].append(f'member size or SHA-256 differs: {name}')
            fact['manifest_ok'] = not fact['errors']
    except (OSError, EOFError, tarfile.TarError, ValueError, KeyError, TypeError) as error:
        fact['errors'].append(str(error))
    return fact


def upload(archive: str | Path, url: str, *, timeout: float = 60) -> dict:
    path = Path(archive)
    digest = _digest_file(path)
    # OSS signs canonical content headers. http.client streams the exact bytes
    # without urllib's implicit Content-Type, including an empty header line.
    endpoint = urlsplit(url)
    if endpoint.scheme not in ('https', 'http') or not endpoint.hostname:
        raise ValueError('archive upload requires an HTTP capability')
    transport = http.client.HTTPSConnection if endpoint.scheme == 'https' else http.client.HTTPConnection
    connection = transport(endpoint.hostname, endpoint.port, timeout=timeout)
    target = endpoint.path or '/'
    if endpoint.query:
        target += '?' + endpoint.query
    try:
        with path.open('rb') as source:
            connection.request('PUT', target, body=source,
                headers={'Content-Length': str(path.stat().st_size)})
            response = connection.getresponse()
            if response.status not in (200, 201, 204):
                raise OSError(f'archive upload returned HTTP {response.status}')
            response.read()
    finally:
        connection.close()
    return {'uploaded': True, 'kind': verify(path)['kind'], 'size': path.stat().st_size, 'sha256': digest}


def _protected_output(path):
    """Require the root caller to control every parent used after validation."""
    destination = Path(path).absolute()
    # Resolve parent links once, then operate on the validated canonical path.
    destination = destination.parent.resolve(strict=True) / destination.name
    for parent in (destination.parent, *destination.parent.parents):
        facts = parent.stat()
        writable = facts.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        if facts.st_uid != 0 or (writable and not facts.st_mode & stat.S_ISVTX):
            raise ValueError('privileged archive output requires root-controlled parents')
    return destination


def _pack_as_owner(attempt_root, diagnostics=None, *, kind, uid, gid, output=None, timeout=60):
    """Receive bytes from an ordinary-account packer into a private parent file.

    The parent never opens an archive path chosen by the workload. Only the
    child traverses workload paths, with no bootstrap or upload capability.
    """
    if kind not in ('final', 'interrupted'):
        raise ValueError('archive kind must be final or interrupted')
    directory = Path(tempfile.mkdtemp(prefix='novarocks-ci-handoff-private-'))
    os.chmod(directory, 0o700)
    destination = directory / f'{kind}.tar.gz' if output is None else (
        _protected_output(output) if os.geteuid() == 0 else Path(output))
    account = pwd.getpwuid(uid)
    package_root = Path(__file__).resolve().parent.parent
    environment = {'HOME': account.pw_dir, 'USER': account.pw_name,
                   'LOGNAME': account.pw_name, 'PATH': '/usr/local/bin:/usr/bin:/bin',
                   'PYTHONPATH': str(package_root), 'PYTHONDONTWRITEBYTECODE': '1'}
    script = """import json,shutil,sys
from aliyun_ci.handoff import pack
request=json.load(sys.stdin)
archive=pack(request['attempt'],request['diagnostics'],kind=request['kind'])
try:
    with archive.open('rb') as source:
        shutil.copyfileobj(source,sys.stdout.buffer)
    sys.stdout.buffer.flush()
finally:
    archive.unlink(missing_ok=True)
    archive.parent.rmdir()
"""
    options = {'cwd': str(package_root), 'env': environment,
               'stdin': subprocess.PIPE}
    if os.geteuid() == 0:
        options.update(user=uid, group=gid, extra_groups=[])
    elif os.geteuid() != uid or os.getegid() != gid:
        raise ValueError('cannot pack as the attempt account')
    request = {'attempt': str(Path(attempt_root).absolute()), 'kind': kind,
               'diagnostics': {name: str(Path(path).absolute()) for name,path in (diagnostics or {}).items()}}
    # Stderr is a parent-owned file, not a second unbounded pipe. Keep the real
    # failure locally without putting arbitrary workload text into task status.
    with (directory / 'packer.stderr').open('xb') as errors, destination.open('xb') as target:
        os.fchmod(target.fileno(), 0o600)
        # The ordinary child inherits only this write descriptor; it never
        # chooses a parent-readable path and receives no upload capability.
        process = subprocess.Popen([sys.executable, '-c', script], stdout=target, stderr=errors, **options)
        try:
            process.communicate(json.dumps(request).encode(), timeout=timeout)
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait()
            raise
        if process.returncode != 0:
            raise RuntimeError(f'ordinary-account archive process exited {process.returncode}; diagnostics: {directory}')
    fact = verify(destination)
    if not fact['manifest_ok']:
        raise ValueError('ordinary-account archive failed member verification')
    return destination


def pack_upload(attempt_root, diagnostics=None, *, kind, url, output=None, timeout=60):
    started = time.monotonic()
    attempt = Path(attempt_root)
    if attempt.is_symlink():
        raise ValueError('archive input is a symbolic link')
    owner = attempt.stat()
    if os.geteuid() == 0 and owner.st_uid != 0:
        archive = _pack_as_owner(attempt, diagnostics, kind=kind, uid=owner.st_uid, gid=owner.st_gid, output=output, timeout=timeout)
    else:
        archive = pack(attempt, diagnostics, kind=kind, output=output)
    remaining = timeout - (time.monotonic() - started)
    if remaining <= 0:
        raise TimeoutError('archive preparation exhausted the handoff deadline')
    return dict(upload(archive, url, timeout=remaining), archive=str(archive))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('pack-upload', 'verify'))
    parser.add_argument('--attempt', type=Path)
    parser.add_argument('--diagnostic', action='append', default=[], metavar='NAME=PATH')
    parser.add_argument('--kind', choices=('final', 'interrupted'), default='final')
    parser.add_argument('--bootstrap', type=Path, default=Path('/run/novarocks-ci/bootstrap.json'))
    parser.add_argument('--archive', type=Path)
    args = parser.parse_args(argv)
    if args.command == 'verify':
        if args.archive is None:
            parser.error('--archive is required for verify')
        result = verify(args.archive)
        print(json.dumps(result, sort_keys=True))
        return 0 if result['manifest_ok'] else 1
    config = json.loads(args.bootstrap.read_text())
    diagnostics = dict(item.split('=', 1) for item in args.diagnostic)
    result = pack_upload(args.attempt or config['attempt_root'], diagnostics,
        kind=args.kind, url=config['upload_urls'][args.kind], output=args.archive)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
