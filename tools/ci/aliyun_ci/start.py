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

"""One-shot spot allocation. Unknown create outcomes keep their original token."""
from __future__ import annotations
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import time
from .ports import APIError
from .plan import identity_tags, parse_time
from .github_api import generate_jit, runners, validate_target

from .lifecycle import (NO_STOCK, UNKNOWN, NOT_FOUND, tags_of, tag_query,
    describe_instances, owned, find_run, create_once, verify_instance, wait_running)

def publish_bootstrap(config, plan, oos, payload):
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
    encoded = base64.b64encode(raw).decode('ascii')
    parts = [encoded[i:i+4096] for i in range(0, len(encoded), 4096)]
    for index, part in enumerate(parts):
        oos.call('CreateSecretParameter', RegionId=config['region'],
                 Name=f'{plan.secret_name}-part-{index:06d}', Type='Secret', Value=part, Tags=json.dumps(plan.tags))
    root = {'schema': 1, 'parts': len(parts), 'sha256': hashlib.sha256(raw).hexdigest()}
    oos.call('CreateSecretParameter', RegionId=config['region'], Name=plan.secret_name,
             Type='Secret', Value=json.dumps(root, sort_keys=True), Tags=json.dumps(plan.tags))


# Design: ADR-0204 (docs/adr/ADR-0204-one-shot-spot-ci-and-independent-evidence.md)
def start_run(config, plan, ecs, oos, oss, github, *, sha, image_id, runner,
              mode='rust', now=None, sleep=time.sleep, wait_online=True):
    if mode not in ('rust', 'probe'):
        raise ValueError('unsupported run mode')
    if not re.fullmatch(r'[0-9a-fA-F]{40}', sha):
        raise ValueError('sha must be a complete 40-hex commit')
    # A local pre-merge boot probe dispatches no test job. Its preparation
    # commit can still be unpublished; remote workload admission applies only
    # to GitHub runs, which actually check out and execute the requested SHA.
    if plan.tags['origin'] == 'github':
        validate_target(config, github, sha)
    release = json.loads(oss.get(f"{config['oss']['prefix']}/images/{image_id}/release.json"))
    if release.get('image_id') != image_id or release.get('region') != config['region']:
        raise ValueError('image release identity does not match request')
    upload_urls = {kind: oss.presign('PUT', plan.oss_keys[kind], plan.deadline) for kind in ('final', 'interrupted')}
    instance_id = create_once(config, plan, ecs, image_id, now=now)
    try:
        wait_running(config, plan, ecs, instance_id, image_id, now=now, sleep=sleep)
    except Exception:
        # Ownership is independently checked before cleanup; preserve the original mismatch.
        from .teardown import delete_exact
        delete_exact(config, ecs, instance_id, {key: value for key, value in plan.tags.items() if key != 'deadline'},
                     deadline=plan.deadline, now=now, sleep=sleep)
        raise
    jit = generate_jit(config, plan, github, instance_id=instance_id)
    payload = {'execution_kind': 'github-runner', 'jit': jit, 'runner': runner, 'deadline': plan.deadline,
        'attempt_root': '/var/lib/novarocks-ci/attempt',
        'upload_urls': upload_urls,
        'watcher': config['watcher'], 'identity': {'run_id': plan.run_id, 'run_attempt': plan.run_attempt,
            'label': plan.label, 'image_id': image_id, 'instance_id': instance_id}}
    publish_bootstrap(config, plan, oos, payload)
    if wait_online:
        now = now or (lambda: datetime.now(timezone.utc))
        while now() < parse_time(plan.deadline):
            if any(row['name'] == plan.label and row['status'] == 'online' for row in runners(config, github)):
                break
            sleep(5)
        else:
            raise TimeoutError(f'runner {plan.label} did not come online before deadline')
    return {'instance_id': instance_id, 'label': plan.label, 'image_id': image_id,
            'deadline': plan.deadline, 'test_timeout_minutes': plan.test_timeout_minutes,
            'plan': plan.to_dict(), 'sha': sha, 'mode': mode}
