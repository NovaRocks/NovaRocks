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

"""Prepare a clean image using the same exact allocation and release owners."""
from __future__ import annotations
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from .plan import derive, parse_time
from .lifecycle import create_once, find_run, wait_running, describe_instances, owned
from .teardown import delete_exact


def image_key(config, image_id, name):
    if not re.fullmatch(r'[A-Za-z0-9-]+', image_id):
        raise ValueError('invalid image identifier')
    return f"{config['oss']['prefix']}/images/{image_id}/{name}"


def run_command(config, plan, ecs, instance_id, script, *, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    timeout = int((parse_time(plan.deadline)-now()).total_seconds())
    if timeout <= 0:
        raise ValueError('image command deadline has expired')
    result = ecs.call('RunCommand', RegionId=config['region'], InstanceId=[instance_id],
                      Type='RunShellScript', CommandContent=base64.b64encode(script.encode()).decode(),
                      ContentEncoding='Base64', Timeout=timeout, TerminationMode='ProcessTree')
    invocation = result['InvokeId']
    while now() < parse_time(plan.deadline):
        value = ecs.call('DescribeInvocationResults', RegionId=config['region'], InvokeId=invocation,
                         InstanceId=instance_id, ContentEncoding='Base64')
        rows = value.get('Invocation', {}).get('InvocationResults', {}).get('InvocationResult', [])
        if rows:
            if len(rows) != 1 or rows[0].get('InstanceId') != instance_id or rows[0].get('InvokeId') != invocation:
                raise ValueError('cloud assistant result identity mismatch')
            row = rows[0]
            status = row.get('InvocationStatus')
            if status in ('Success', 'Failed', 'Error', 'Timeout', 'Cancelled', 'Terminated', 'Invalid', 'Aborted'):
                if status != 'Success' or row.get('ExitCode') != 0:
                    raise RuntimeError(f'cloud assistant {invocation} ended {status}, exit {row.get("ExitCode")}')
                if row.get('Dropped', 0):
                    raise ValueError('cloud assistant output was truncated')
                return base64.b64decode(row.get('Output', ''), validate=True).decode()
        sleep(5)
    raise TimeoutError(f'cloud assistant {invocation} did not exit before deadline')


def wait_stopped(config, plan, ecs, instance_id, *, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    while now() < parse_time(plan.deadline):
        rows = describe_instances(config, ecs, instance_id=instance_id)
        if len(rows) != 1:
            raise ValueError('builder disappeared before image creation')
        owned(config, rows[0], plan.tags)
        if rows[0]['Status'] == 'Stopped':
            return
        sleep(5)
    raise TimeoutError('builder did not stop before deadline')


def scrub_command_history(directory='/usr/local/share/aliyun-assist/work/script'):
    """Remove capability-bearing scripts from this disposable image builder."""
    root=Path(directory)
    needle=b'x-oss-' + b'signature='
    removed=[]
    if root.is_dir():
        for path in root.iterdir():
            if path.is_file() and not path.is_symlink() and needle in path.read_bytes():
                path.unlink()
                removed.append(path.name)
        if any(needle in path.read_bytes() for path in root.iterdir()
               if path.is_file() and not path.is_symlink()):
            raise ValueError('builder command history still contains an object capability')
    return {'removed_scripts': sorted(removed), 'remaining_capabilities': 0}


def build_image(config, plan, ecs, oss, *, preparation_sha, source_archive: bytes,
                now=None, sleep=time.sleep, nextest=None):
    if plan.role != 'builder':
        raise ValueError('image creation requires a builder identity')
    if not re.fullmatch(r'[0-9a-f]{40}', preparation_sha):
        raise ValueError('preparation SHA must be complete')
    now = now or (lambda: datetime.now(timezone.utc))
    key = f"{config['oss']['prefix']}/builders/{plan.run_id}/{plan.run_attempt}/source.tar"
    oss.put(key, source_archive, write_once=True)
    url = oss.presign('GET', key, plan.deadline)
    digest = hashlib.sha256(source_archive).hexdigest()
    build_log_key = key.removesuffix('source.tar') + 'build.log'
    build_log_url = oss.presign('PUT', build_log_key, plan.deadline)
    instance_id = None
    try:
        instance_id = create_once(config, plan, ecs, config['base_image_id'], now=now,
                                  user_data={'role': 'builder', 'deadline': plan.deadline})
        wait_running(config, plan, ecs, instance_id, config['base_image_id'], now=now, sleep=sleep)
        # This command contains only a public-source, bounded read capability.
        tool_commands = []
        tool_argument = ''
        if nextest is not None:
            tool_commands = [f'curl --fail --location --output \"$work/nextest.tar.gz\" {shlex.quote(nextest["url"])}',
                f'printf \"%s  %s\\n\" {shlex.quote(nextest["sha256"])} \"$work/nextest.tar.gz\" | sha256sum -c -']
            tool_argument = ' \"$work/nextest.tar.gz\"'
        script = '\n'.join(['set -eu', 'work=$(mktemp -d /var/tmp/novarocks-image.XXXXXX)',
            'trap \'rm -rf "$work"\' EXIT',
            f'curl --fail --location --output "$work/source.tar" {shlex.quote(url)}',
            f'printf "%s  %s\\n" {shlex.quote(digest)} "$work/source.tar" | sha256sum -c -',
            'mkdir "$work/source"', 'tar -xf "$work/source.tar" -C "$work/source"',
            *tool_commands,
            f'set +e; "$work/source/tools/ci/aliyun_ci/image/build-image.sh" "$work/source" {preparation_sha}{tool_argument} > /var/log/novarocks-ci-image-build.log 2>&1',
            'build_exit=$?; set -e',
            f'curl --fail --silent --show-error --header \'Content-Type:\' --request PUT --data-binary @/var/log/novarocks-ci-image-build.log {shlex.quote(build_log_url)} >/dev/null',
            'exit "$build_exit"'])
        run_command(config, plan, ecs, instance_id, script, now=now, sleep=sleep)
        build_log = oss.get(build_log_key)
        facts = json.loads(run_command(config, plan, ecs, instance_id,
            'cat /opt/novarocks-ci/preparation-facts.json', now=now, sleep=sleep))
        if facts.get('preparation_sha') != preparation_sha or facts.get('os') != 'ubuntu-26.04' or facts.get('arch') != 'amd64':
            raise ValueError('builder preparation identity mismatch')
        hygiene = json.loads(run_command(config, plan, ecs, instance_id,
            'cd /opt/novarocks-ci; /opt/novarocks-ci/venv/bin/python -c \"import json; from aliyun_ci.image import scrub_command_history; print(json.dumps(scrub_command_history()))\"',
            now=now, sleep=sleep))
        ecs.call('StopInstance', RegionId=config['region'], InstanceId=instance_id, ForceStop=False)
        wait_stopped(config, plan, ecs, instance_id, now=now, sleep=sleep)
        response = ecs.call('CreateImage', RegionId=config['region'], InstanceId=instance_id,
            ImageName=f'g16-{plan.run_id}-{plan.run_attempt}', ClientToken=plan.client_token,
            Tag=[{'Key': key, 'Value': value} for key, value in sorted(plan.tags.items())])
        image_id = response['ImageId']
        print('AliyunCIImage: ' + json.dumps({'image_id': image_id, 'region': config['region'],
              'run_id': plan.run_id, 'run_attempt': plan.run_attempt}), file=sys.stderr, flush=True)
        while now() < parse_time(plan.deadline):
            result = ecs.call('DescribeImages', RegionId=config['region'], ImageId=image_id,
                              ImageOwnerAlias='self', Status='ALL')
            rows = result.get('Images', {}).get('Image', [])
            if not rows:
                sleep(5)
                continue
            if len(rows) != 1 or rows[0].get('ImageId') != image_id:
                raise ValueError('created image readback identity mismatch')
            if rows[0]['Status'] == 'Available':
                release = dict(facts, image_id=image_id, region=config['region'], command_history_hygiene=hygiene,
                               staged_nextest=None if nextest is None else {k:nextest[k] for k in ('key','sha256')},
                               build_log={'key': build_log_key, 'sha256': hashlib.sha256(build_log).hexdigest()},
                               builder_run={'run_id': plan.run_id, 'run_attempt': plan.run_attempt,
                                            'instance_id': instance_id})
                oss.put(image_key(config, image_id, 'release.json'),
                        json.dumps(release, sort_keys=True).encode(), write_once=True)
                return release
            if rows[0]['Status'] not in ('Creating', 'Waiting'):
                raise RuntimeError(f'created image {image_id} ended {rows[0]["Status"]}')
            sleep(5)
        raise TimeoutError(f'image {image_id} did not become Available before deadline')
    finally:
        # Also recover a created instance when RunInstances returned an unknown outcome.
        if instance_id is None:
            existing = find_run(config, plan, ecs)
            instance_id = existing['InstanceId'] if existing else None
        if instance_id:
            delete_exact(config, ecs, instance_id, plan.tags, deadline=plan.deadline, now=now, sleep=sleep)


def promote_image(config, oss, image_id, summary_key):
    release = json.loads(oss.get(image_key(config, image_id, 'release.json')))
    raw = oss.get(summary_key)
    summary = json.loads(raw)
    if release.get('image_id') != image_id or summary.get('image_id') != image_id:
        raise ValueError('image validation summary identity mismatch')
    if summary.get('tools_check') is not True:
        raise ValueError('image tools check has not passed')
    value = {'schema': 1, 'image_id': image_id,
             'validation_run': {'run_id': summary['run_id'], 'run_attempt': summary['run_attempt']},
             'summary_key': summary_key, 'summary_sha256': hashlib.sha256(raw).hexdigest()}
    oss.put(image_key(config, image_id, 'ready.json'), json.dumps(value, sort_keys=True).encode(), write_once=True)
    return value
