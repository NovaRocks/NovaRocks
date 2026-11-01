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

"""Inspect deployment-owned overdue resources; no display-name deletion authority."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import re
from .plan import derive, parse_time
from .start import describe_instances, owned, tags_of, tag_query
from .teardown import delete_exact, delete_bootstrap, delete_run_runners, secret_parameters
from .github_api import runners
from .ports import APIError


def sweep(config, ecs, oos, oss, github, *, now=None, sleep=None):
    now = now or (lambda: datetime.now(timezone.utc))
    current = now()
    query = {'deployment': config['deployment_id'], 'repository': config['repository']}
    runs, anomalies = {}, []
    local_only = config.data.get('execution_kind') == 'local-rust'
    grace = timedelta(minutes=config['janitor']['grace_minutes'])
    for instance in describe_instances(config, ecs, tags=query):
        if any(tags_of(instance).get(key) != value for key, value in query.items()):
            continue
        if local_only and tags_of(instance).get('execution_kind') not in ('local-rust', 'builder'):
            continue
        tags = owned(config, instance, query)
        if local_only and tags.get('execution_kind') not in ('local-rust', 'builder'):
            continue
        if current <= parse_time(tags['deadline']) + grace:
            continue
        plan = derive(config, tags['run_id'], int(tags['run_attempt']), current, role=tags['role'], execution_kind=tags.get('execution_kind', 'github-runner'))
        plan = replace(plan, deadline=tags['deadline'], auto_release=tags['deadline'], tags=tags)
        kwargs = {'now': now}
        if sleep is not None:
            kwargs['sleep'] = sleep
        try:
            delete_exact(config, ecs, instance['InstanceId'], tags, **kwargs)
            anomalies.append({'kind': 'overdue-instance', 'instance_id': instance['InstanceId'], 'terminal': 'released'})
        except Exception as error:
            anomalies.append({'kind': 'overdue-instance', 'instance_id': instance['InstanceId'], 'terminal': 'unknown', 'error': str(error)})
        runs[(plan.run_id, plan.run_attempt)] = plan
    for parameter in secret_parameters(config, oos):
        tags = parameter.get('Tags', {})
        if isinstance(tags, str):
            tags = json.loads(tags)
        if any(tags.get(key) != value for key, value in query.items()) or 'deadline' not in tags:
            continue
        if local_only and tags.get('execution_kind') not in ('local-rust', 'builder'):
            continue
        if current <= parse_time(tags['deadline']) + grace:
            continue
        plan = derive(config, tags['run_id'], int(tags['run_attempt']), current, role=tags['role'], execution_kind=tags.get('execution_kind', 'github-runner'))
        plan = replace(plan, deadline=tags['deadline'], auto_release=tags['deadline'], tags=tags)
        if parameter['Name'] != plan.secret_name and not re.fullmatch(re.escape(plan.secret_name) + r'-part-[0-9]{6}', parameter['Name']):
            continue
        runs[(plan.run_id, plan.run_attempt)] = plan
    # Runner labels carry the original resource facts even after cloud-side release.
    for row in (runners(config, github) if config.data.get('execution_kind', 'github-runner') == 'github-runner' else []):
        from .github_api import runner_tags
        tags = runner_tags(row)
        if any(tags.get(key) != value for key, value in query.items()):
            continue
        if tags.get('account_id') != str(config['account_id']) or tags.get('region') != config['region']:
            continue
        if row.get('status') != 'offline':
            continue
        required = ('run_id', 'run_attempt', 'deadline', 'instance_id', 'role', 'origin')
        if any(key not in tags for key in required):
            anomalies.append({'kind': 'runner', 'name': row['name'], 'terminal': 'unknown', 'error': 'missing ownership labels'})
            continue
        if current <= parse_time(tags['deadline']) + grace:
            continue
        expected = {key: tags[key] for key in ('deployment', 'repository', *required) if key != 'instance_id'}
        cloud = describe_instances(config, ecs, instance_id=tags['instance_id'])
        if cloud:
            # The existing instance is handled by exact cloud ownership above.
            owned(config, cloud[0], expected)
            anomalies.append({'kind': 'runner', 'name': row['name'], 'terminal': 'unknown', 'error': 'instance still present'})
            continue
        plan = derive(config, tags['run_id'], int(tags['run_attempt']), current, role=tags['role'], execution_kind=tags.get('execution_kind', 'github-runner'))
        plan = replace(plan, label=row['name'], deadline=tags['deadline'], auto_release=tags['deadline'], tags=expected)
        runs[(plan.run_id, plan.run_attempt)] = plan
    for plan in runs.values():
        try:
            delete_bootstrap(config, plan, oos)
            if plan.execution_kind == 'github-runner':
                delete_run_runners(config, plan, github)
            try:
                oss.get(plan.oss_keys['summary'])
                missing_summary = False
            except APIError as error:
                if error.code not in ('NoSuchKey', 'NotFound', '404'):
                    raise
                missing_summary = True
            anomalies.append({'kind': 'overdue-run', 'run_id': plan.run_id,
                              'run_attempt': plan.run_attempt, 'missing_teardown_summary': missing_summary})
        except Exception as error:
            anomalies.append({'kind': 'overdue-run', 'run_id': plan.run_id, 'error': str(error)})
    return {'observed_at': current.isoformat(), 'anomalies': anomalies}
