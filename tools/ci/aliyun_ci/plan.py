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

"""Run identity and absolute deadlines, independent of cloud SDKs."""
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import re
import secrets
from .config import DeploymentConfig


def timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError('UTC-aware time is required')
    return value.astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('UTC-aware time is required')
    return parsed.astimezone(timezone.utc)


def identity_tags(config, run_id, run_attempt, role='worker'):
    if not re.fullmatch(r'(?:[0-9]+|local-[A-Za-z0-9-]+)', str(run_id)):
        raise ValueError('invalid run_id')
    if type(run_attempt) is not int or run_attempt < 1:
        raise ValueError('run_attempt must be a positive integer')
    if role not in ('worker', 'builder'):
        raise ValueError('invalid role')
    return {'deployment': config['deployment_id'], 'repository': config['repository'],
            'run_id': str(run_id), 'run_attempt': str(run_attempt), 'role': role,
            'origin': 'local' if str(run_id).startswith('local-') else 'github'}


@dataclass(frozen=True)
class RunPlan:
    run_id: str
    run_attempt: int
    client_token: str
    tags: dict
    label: str
    deadline: str
    auto_release: str
    test_timeout_minutes: int
    oss_keys: dict
    secret_name: str
    role: str
    execution_kind: str = 'github-runner'

    def to_dict(self):
        return asdict(self)


def derive(config: DeploymentConfig, run_id: str, run_attempt: int, now: datetime,
           *, nonce: str | None = None, role: str = 'worker', execution_kind: str | None = None) -> RunPlan:
    execution_kind = execution_kind or ('builder' if role == 'builder' else config.data.get('execution_kind', 'github-runner'))
    if execution_kind not in ('builder', 'local-rust', 'github-runner'):
        raise ValueError('unsupported execution kind')
    tags = identity_tags(config, run_id, run_attempt, role)
    token = f"g16-{config['deployment_id']}-{run_id}-{run_attempt}"
    # ECS permits 64 ASCII bytes, including the candidate suffix.
    if len((token + '-c' + str(len(config['candidates']) - 1)).encode('ascii')) > 64:
        raise ValueError('derived ClientToken exceeds the ECS 64-byte limit')
    duration = config['deadline']
    raw_deadline = now + timedelta(minutes=sum(duration.values()))
    # ECS stores AutoReleaseTime at minute precision; round up to preserve time.
    deadline_value = raw_deadline.replace(second=0, microsecond=0)
    if raw_deadline > deadline_value:
        deadline_value += timedelta(minutes=1)
    deadline = timestamp(deadline_value)
    tags['deadline'] = deadline
    if execution_kind != 'github-runner':
        tags['execution_kind'] = execution_kind
    label = f'g16-{run_id}-{run_attempt}-{nonce or secrets.token_hex(6)}'
    root = f"{config['oss']['prefix']}/runs/{run_id}/{run_attempt}"
    return RunPlan(str(run_id), run_attempt, token, tags, label, deadline, deadline,
                   duration['work_minutes'] + duration['upload_reserve_minutes'],
                   {kind: f'{root}/{kind}.tar.gz' for kind in ('final', 'interrupted')} |
                   {'summary': f'{root}/summary.json'},
                   f"g16-{config['deployment_id'].encode().hex()}-{run_id}-{run_attempt}", role, execution_kind)
