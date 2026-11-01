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

"""Local caller: exact source, one task, independent evidence and release.

CI maintainers own these platform correctness operations. This module neither
selects Rust tests nor interprets assertions or retries product failures.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import tarfile
import time
from . import cloud_command
from .handoff import verify
from .lifecycle import create_once, wait_running, describe_instances, owned
from .plan import RunPlan, identity_tags, parse_time, timestamp
from .ports import APIError
from .start import publish_bootstrap
from .teardown import teardown_run, native_archive_facts

ATTEMPT = '/var/lib/novarocks-ci/attempt'
MISSING = ('NoSuchKey', 'NotFound', '404')


def _save(path, value, *, exclusive=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(value, indent=2, sort_keys=True) + '\n'
    if exclusive:
        with path.open('x') as target:
            target.write(text)
    else:
        # Interrupted writes retain the last durable observation.
        temporary = path.with_suffix(path.suffix + '.new')
        with temporary.open('w') as target:
            target.write(text)
            target.flush()
            import os
            os.fsync(target.fileno())
        temporary.replace(path)


def _update(path, *, require_absent=None, **updates):
    # A second operator may cancel while the original caller is waiting. Merge
    # only this observation's fields so the original cancellation identity survives.
    import fcntl
    lock = Path(path).with_suffix(Path(path).suffix + '.lock')
    with lock.open('a') as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX)
        value = json.loads(Path(path).read_text())
        if require_absent is not None and require_absent in value:
            raise ValueError('original command dispatch already recorded; observe that identity')
        if 'observation' in updates:
            prior = value.get('observation', {})
            current = dict(prior, **updates['observation'])
            if current.get('task') is None and prior.get('task') is not None:
                current['task'] = prior['task']
            updates['observation'] = current
        value.update(updates)
        _save(path, value)
        return value


def load_record(config, path):
    record = json.loads(Path(path).read_text())
    if record.get('schema') != 1 or record.get('config') != config.data:
        raise ValueError('local record does not match this deployment configuration')
    plan = RunPlan(**record['plan'])
    if plan.execution_kind != 'local-rust' or plan.role != 'worker':
        raise ValueError('record is not a local Rust worker')
    expected = identity_tags(config, plan.run_id, plan.run_attempt, plan.role)
    if any(plan.tags.get(key) != value for key, value in expected.items()):
        raise ValueError('record run identity does not match deployment')
    return record, plan


def _observer_script(plan):
    identity = repr({'run_id': plan.run_id, 'run_attempt': plan.run_attempt})
    return """set -eu
cd /opt/novarocks-ci
/opt/novarocks-ci/venv/bin/python - <<'PY'
import json,time
from pathlib import Path
expected = """ + identity + """
path = Path('/run/novarocks-ci/local-status.json')
while True:
    if path.exists():
        value = json.loads(path.read_text())
        if any(value.get('identity', {}).get(k) != v for k,v in expected.items()):
            raise ValueError('local task status identity mismatch')
        if value.get('state') == 'complete':
            print(json.dumps(value, sort_keys=True))
            break
    time.sleep(2)
PY
"""


def _task(observation, plan):
    if not observation.get('terminal') or observation.get('status') != 'Success' or observation.get('exit_code') != 0:
        return None
    if observation.get('dropped'):
        return None
    value = json.loads(observation.get('output') or '{}')
    if not isinstance(value, dict) or value.get('state') != 'complete':
        raise ValueError('observer returned no complete original task status')
    if any(value.get('identity', {}).get(k) != v for k,v in
           {'run_id': plan.run_id, 'run_attempt': plan.run_attempt}.items()):
        raise ValueError('original task result identity mismatch')
    return value


def start_local(config, plan, ecs, oos, oss, *, image_id, source, record_path,
                now=None, sleep=time.sleep):
    if config.data.get('execution_kind') != 'local-rust' or plan.execution_kind != 'local-rust':
        raise ValueError('local-run requires explicit local-rust configuration and plan')
    now = now or (lambda: datetime.now(timezone.utc))
    source = Path(source)
    manifest_bytes = (source / 'source.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    bundle = (source / 'source.bundle').read_bytes()
    if hashlib.sha256(bundle).hexdigest() != manifest['bundle_sha256']:
        raise ValueError('source bundle SHA-256 differs from manifest')
    release = json.loads(oss.get(f"{config['oss']['prefix']}/images/{image_id}/release.json"))
    if release.get('image_id') != image_id or release.get('region') != config['region']:
        raise ValueError('image release identity does not match request')
    cargo_registry = config.data.get('cargo_registry', 'crates-io')
    supported = release.get('local_rust_cargo_registries', ['crates-io'])
    if not isinstance(supported, list) or any(not isinstance(item, str) for item in supported):
        raise ValueError('image Cargo registry capabilities are invalid')
    if cargo_registry not in supported:
        raise ValueError(f'image does not support local Cargo registry {cargo_registry}')
    swap_gib = config.data.get('swap_gib', 0)
    swap_choices = release.get('local_rust_swap_gib', [0])
    if not isinstance(swap_choices, list) or any(type(item) is not int for item in swap_choices):
        raise ValueError('image swap capabilities are invalid')
    if swap_gib not in swap_choices:
        raise ValueError(f'image does not support local swap_gib {swap_gib}')
    source_key = f"{config['oss']['prefix']}/runs/{plan.run_id}/{plan.run_attempt}/source.bundle"
    manifest_key = source_key.removesuffix('.bundle') + '.json'
    capability_deadline = timestamp(parse_time(plan.deadline) - timedelta(
        minutes=config['deadline']['teardown_reserve_minutes']))
    # Compute all capabilities before allocation; unknown/short credential
    # expiration cannot leave a newly allocated machine behind.
    source_url = oss.presign('GET', source_key, capability_deadline)
    manifest_url = oss.presign('GET', manifest_key, capability_deadline)
    upload_urls = {kind: oss.presign('PUT', plan.oss_keys[kind], capability_deadline)
                   for kind in ('final', 'interrupted')}
    record = {'schema': 1, 'config': config.data, 'plan': plan.to_dict(), 'image_id': image_id,
              'source': manifest, 'source_key': source_key, 'instance_id': None, 'phase': 'prepared'}
    _save(record_path, record, exclusive=True)
    oss.put(source_key, bundle, write_once=True)
    oss.put(manifest_key, manifest_bytes, write_once=True)
    record['phase'] = 'allocating'
    _save(record_path, record)
    instance_id = create_once(config, plan, ecs, image_id, now=now)
    record.update(instance_id=instance_id, phase='allocated')
    _save(record_path, record)
    wait_running(config, plan, ecs, instance_id, image_id, now=now, sleep=sleep)
    work_deadline = timestamp(parse_time(plan.deadline) - timedelta(minutes=(
        config['deadline']['teardown_reserve_minutes'] + config['deadline']['upload_reserve_minutes'])))
    payload = {'execution_kind': 'local-rust', 'task': 'rust-workspace',
               'source': {key: manifest[key] for key in ('sha', 'tree', 'bundle_sha256')} |
                         {'url': source_url, 'manifest_url': manifest_url,
                          'manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest()},
               'deadline': capability_deadline,
               'work_deadline': work_deadline,
               'build_jobs': config.data.get('build_jobs'),
               'cargo_registry': cargo_registry,
               'swap_gib': swap_gib,
               'attempt_root': ATTEMPT, 'upload_urls': upload_urls, 'watcher': config['watcher'],
               'identity': {'run_id': plan.run_id, 'run_attempt': plan.run_attempt,
                            'label': plan.label, 'image_id': image_id, 'instance_id': instance_id}}
    record['phase'] = 'publishing-bootstrap'
    _save(record_path, record)
    publish_bootstrap(config, plan, oos, payload)
    observer_deadline = timestamp(parse_time(plan.deadline) - timedelta(
        minutes=config['deadline']['teardown_reserve_minutes']))
    def persist(command):
        _update(record_path, command=command, phase='observing')
    cloud_command.dispatch(config, ecs, instance_id, _observer_script(plan), observer_deadline,
                           persist=persist, now=now, sleep=sleep)
    return json.loads(Path(record_path).read_text())


def _check_instance(config, plan, ecs, record):
    if not record.get('instance_id'):
        return None
    values = describe_instances(config, ecs, instance_id=record['instance_id'])
    if not values:
        return False
    if len(values) != 1 or values[0].get('InstanceId') != record['instance_id']:
        raise ValueError('local run exact instance lookup mismatch')
    owned(config, values[0], plan.tags)
    if values[0].get('ImageId') != record['image_id']:
        raise ValueError('local run image identity mismatch')
    return True


def status_local(config, ecs, record_path):
    record, plan = load_record(config, record_path)
    present = _check_instance(config, plan, ecs, record)
    command = record.get('command')
    observation = cloud_command.observe(config, ecs, command) if command else None
    task = _task(observation, plan) if observation else None
    value = {'instance_present': present, 'cloud_command': observation, 'task': task}
    return _update(record_path, observation=value)['observation']


def wait_local(config, ecs, record_path, *, now=None, sleep=time.sleep):
    record, plan = load_record(config, record_path)
    _check_instance(config, plan, ecs, record)
    if not record.get('command'):
        raise ValueError('record has no accepted command; use collect/teardown to recover this original run')
    observation = cloud_command.wait(config, ecs, record['command'], now=now, sleep=sleep)
    value = {'cloud_command': observation, 'task': _task(observation, plan)}
    return _update(record_path, observation=value,
        phase='observed-terminal' if observation.get('terminal') else 'observing')['observation']


def _cancel_script(plan, upload_reserve_seconds):
    expected = repr({'run_id': plan.run_id, 'run_attempt': plan.run_attempt})
    return """set -eu
cd /opt/novarocks-ci
/opt/novarocks-ci/venv/bin/python - <<'PY'
import json,os,pwd,time
from datetime import datetime,timedelta,timezone
from pathlib import Path
from aliyun_ci.instance.watcher import cancel_batch
from aliyun_ci.plan import parse_time
root=Path('/run/novarocks-ci')
expected=""" + expected + """
upload_reserve_seconds=""" + str(upload_reserve_seconds) + """
bootstrap=json.loads((root/'bootstrap.json').read_text())
if bootstrap.get('execution_kind') != 'local-rust' or any(bootstrap['identity'].get(k)!=v for k,v in expected.items()):
    raise ValueError('local cancellation identity mismatch')
status_path=root/'local-status.json'
def completed_task():
    if not status_path.exists():
        return None
    value=json.loads(status_path.read_text())
    if any(value.get('identity',{}).get(k)!=v for k,v in expected.items()):
        raise ValueError('local cancellation status identity mismatch')
    return value if value.get('state') == 'complete' else None

task=completed_task()
if task is not None:
    print(json.dumps({'cancellation':{'signal_sent':False,'exited':task.get('batch',{}).get('exited'),'reason':'original supervisor complete'},
                      'task':task,'task_completion_observed':True},sort_keys=True))
    raise SystemExit(0)
# This original marker is persisted before SIGTERM, including cancellation
# during source preparation, before the native PID has been created.
marker=root/'local-cancel.json'
fd=os.open(marker,os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as output:
    json.dump({'identity':expected,'reason':'local-cancel'},output)
started=datetime.now(timezone.utc)
budget=bootstrap['watcher']['cancel_budget_seconds']
end=min(parse_time(bootstrap['deadline']),started+timedelta(seconds=budget+upload_reserve_seconds))
cancel_budget=min(budget,max(0,(end-datetime.now(timezone.utc)).total_seconds()))
result=cancel_batch(root,cancel_budget,uid=pwd.getpwnam('runner').pw_uid)
while True:
    task=completed_task()
    if task is not None or datetime.now(timezone.utc) >= end:
        break
    time.sleep(min(0.2,max(0,(end-datetime.now(timezone.utc)).total_seconds())))
print(json.dumps({'cancellation':result,'task':task,'task_completion_observed':task is not None},sort_keys=True))
PY
"""


def _retain_cancellation(config, plan, record_path, observation):
    if (not observation.get('terminal') or observation.get('status') != 'Success' or
            observation.get('exit_code') != 0 or observation.get('dropped')):
        return observation
    result = json.loads(observation.get('output') or '{}')
    if not isinstance(result, dict) or not isinstance(result.get('cancellation'), dict):
        raise ValueError('cancellation command returned no original cancellation fact')
    task = result.get('task')
    if task is not None:
        # Reuse the same complete-task identity check as the original observer;
        # command termination alone never proves native completion.
        checked = dict(observation, output=json.dumps(task))
        task = _task(checked, plan)
    value = dict(observation, native_cancellation=result['cancellation'], task=task,
                 task_completion_observed=task is not None)
    _update(record_path, observation={'cancellation_cloud_command': value, 'task': task})
    return value


def cancel_local(config, ecs, record_path, *, now=None, sleep=time.sleep):
    record, plan = load_record(config, record_path)
    if not record.get('instance_id'):
        return {'signal_sent': False, 'reason': 'no instance was recorded', 'instance_id': None}
    if _check_instance(config, plan, ecs, record) is False:
        return {'signal_sent': False, 'reason': 'instance already absent'}
    if record.get('cancellation'):
        observation = cloud_command.observe(config, ecs, record['cancellation'])
        return _retain_cancellation(config, plan, record_path, observation)
    script = _cancel_script(plan, config['deadline']['upload_reserve_minutes'] * 60)
    deadline = timestamp(parse_time(plan.deadline) - timedelta(
        minutes=config['deadline']['teardown_reserve_minutes']))
    def persist(command):
        _update(record_path, require_absent='cancellation' if command['dispatch_state'] == 'pending' else None,
                cancellation=command)
    command = cloud_command.dispatch(config, ecs, record['instance_id'], script, deadline,
                                     persist=persist, now=now, sleep=sleep)
    observation = cloud_command.wait(config, ecs, command, now=now, sleep=sleep)
    return _retain_cancellation(config, plan, record_path, observation)


def collect_local(config, oss, record_path, output_dir):
    record, plan = load_record(config, record_path)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    archives, native = {}, {}
    for kind in ('final', 'interrupted'):
        key = plan.oss_keys[kind]
        try:
            raw = oss.get(key)
        except APIError as error:
            archives[kind] = {'present': False if error.code in MISSING else None, 'error': error.code}
            continue
        fact = verify(raw)
        archives[kind] = dict(fact, key=key)
        destination = output / f'{kind}.tar.gz'
        if destination.exists():
            if destination.read_bytes() != raw:
                raise ValueError('existing collected archive differs from original OSS object')
        else:
            with destination.open('xb') as target:
                target.write(raw)
        if not fact['manifest_ok'] or fact.get('kind') != kind:
            archives[kind]['key_matches_kind'] = fact.get('kind') == kind
            continue
        native[kind] = native_archive_facts(raw, kind=kind, key=key, digest=fact['archive_sha256'])
        # verify() accepted only safe regular members. Preserve all original
        # bytes under a distinct archive-kind directory.
        with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
            for member in archive:
                target = output / kind / member.name
                target.parent.mkdir(parents=True, exist_ok=True)
                data = archive.extractfile(member).read()
                if target.exists():
                    if target.read_bytes() != data:
                        raise ValueError('existing collected member differs from original archive')
                else:
                    with target.open('xb') as file:
                        file.write(data)
    value = {'archives': archives, 'native': native, 'output_dir': str(output.resolve())}
    _update(record_path, collection=value)
    return value


def finish_local(config, ecs, oos, oss, record_path, *, output_dir=None, now=None, sleep=time.sleep):
    record, plan = load_record(config, record_path)
    collect_error = None
    try:
        collection = collect_local(config, oss, record_path,
            output_dir or Path(record_path).parent / 'results')
    except Exception as error:
        collection, collect_error = None, str(error)
    finally:
        # Shared teardown always executes, including missing/corrupt evidence.
        summary = teardown_run(config, plan, ecs, oos, oss, None,
            instance_id=record.get('instance_id'), image_id=record['image_id'], now=now, sleep=sleep)
    record, _ = load_record(config, record_path)
    summary['local_collection'] = collection
    if collect_error:
        summary['local_collection_error'] = collect_error
    summary['local_task'] = record.get('observation', {}).get('task')
    summary['source'] = record['source']
    _update(record_path, summary=summary, phase='finished')
    return summary


def result_exit_code(summary):
    task = summary.get('local_task') or {}
    archives = (summary.get('local_collection') or {}).get('archives', {})
    complete = any(fact.get('manifest_ok') and fact.get('present') and fact.get('kind') == kind
                   for kind, fact in archives.items())
    platform_ok = (summary.get('compute_terminal') == 'released' and not summary.get('leftovers') and
                   complete and summary.get('rust_result') is not None and summary.get('tools_check') is True)
    return 0 if platform_ok and type(task.get('exit_code')) is int and task['exit_code'] == 0 else 1
