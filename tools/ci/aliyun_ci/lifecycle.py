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

"""Shared exact ECS allocation, ownership and reclamation for every caller."""
from __future__ import annotations
import base64
from datetime import datetime, timedelta, timezone
import json
import time
from .ports import APIError
from .plan import identity_tags, parse_time

NO_STOCK = {'OperationDenied.NoStock', 'Zone.NotOnSale'}
UNKNOWN = {'TransportUnknown', 'RequestTimeout', 'InternalError'}
NOT_FOUND = {'SecretParameterNotFound', 'InvalidSecretParameter.NotFound', 'ParameterNotFound'}


def tags_of(instance):
    return {item['TagKey']: item['TagValue'] for item in instance.get('Tags', {}).get('Tag', [])}


def tag_query(tags):
    return [{'Key': key, 'Value': value} for key, value in sorted(tags.items())]


def describe_instances(config, ecs, *, tags=None, instance_id=None):
    page, result = 1, []
    while True:
        params = {'RegionId': config['region'], 'PageSize': 100, 'PageNumber': page}
        if tags:
            params['Tag'] = tag_query(tags)
        if instance_id:
            params['InstanceIds'] = json.dumps([instance_id])
        response = ecs.call('DescribeInstances', **params)
        rows = response.get('Instances', {}).get('Instance', [])
        result.extend(rows)
        if len(result) >= response.get('TotalCount', len(result)):
            return result
        if not rows:
            raise ValueError('truncated ECS list returned no instances')
        page += 1


def owned(config, instance, expected_tags):
    actual = tags_of(instance)
    if instance.get('RegionId') != config['region'] or any(actual.get(k) != v for k, v in expected_tags.items()):
        raise ValueError(f"instance ownership mismatch: {instance.get('InstanceId')}")
    # The account is established by STS before constructing the service adapters.
    for field in ('deployment', 'repository', 'run_id', 'run_attempt', 'role', 'origin', 'deadline'):
        if field not in actual:
            raise ValueError(f'instance is missing ownership field {field}')
    parse_time(actual['deadline'])
    identity_tags(config, actual['run_id'], int(actual['run_attempt']), actual['role'])
    return actual


def find_run(config, plan, ecs):
    exact = {key: value for key, value in plan.tags.items() if key != 'deadline'}
    values = describe_instances(config, ecs, tags=exact)
    for instance in values:
        owned(config, instance, exact)
    if len(values) > 1:
        raise ValueError('multiple instances claim one run; refusing to choose one')
    return values[0] if values else None


def create_once(config, plan, ecs, image_id, *, now=None, user_data=None):
    now = now or (lambda: datetime.now(timezone.utc))
    existing = find_run(config, plan, ecs)
    if existing:
        return existing['InstanceId']
    if parse_time(plan.auto_release) < now() + timedelta(minutes=30):
        raise ValueError('auto release is less than 30 minutes from actual allocation')
    pointer = user_data or {'region': config['region'], 'secret_name': plan.secret_name,
                           'deadline': plan.deadline}
    for index, candidate in enumerate(config['candidates']):
        token = f'{plan.client_token}-c{index}'
        parameters = dict(RegionId=config['region'], ZoneId=candidate['zone'],
            VSwitchId=candidate['vswitch_id'], InstanceType=candidate['instance_type'],
            ImageId=image_id, SecurityGroupId=config['security_group_id'],
            RamRoleName=config['instance_ram_role'], InstanceChargeType='PostPaid',
            SpotStrategy='SpotAsPriceGo', SpotDuration=1, SpotInterruptionBehavior='Terminate',
            Amount=1, ClientToken=token, Tag=tag_query(plan.tags),
            AutoReleaseTime=plan.auto_release, SystemDisk={'Size': config['system_disk_gib'], 'Category': 'cloud_essd'},
            InternetMaxBandwidthOut=config['internet_max_bandwidth_out_mbps'], InternetChargeType='PayByTraffic',
            UserData=base64.b64encode(json.dumps(pointer, sort_keys=True).encode()).decode())
        modify = False
        for invocation in range(3):
            try:
                response = ecs.call('RunInstances', **parameters)
                ids = response.get('InstanceIdSets', {}).get('InstanceIdSet', [])
                if len(ids) != 1:
                    raise ValueError('RunInstances did not return exactly one instance')
                instance_id = ids[0]
                if modify:
                    ecs.call('ModifyInstanceAutoReleaseTime', RegionId=config['region'],
                             InstanceId=instance_id, AutoReleaseTime=plan.auto_release)
                return instance_id
            except APIError as error:
                if error.code in NO_STOCK:
                    break
                if error.code == 'InvalidAutoReleaseTime.Malformed' and not modify:
                    parameters.pop('AutoReleaseTime')
                    modify = True
                    continue
                if error.code not in UNKNOWN:
                    raise
            except (TimeoutError, ConnectionError):
                pass
            # Recover the same request; never move to a new candidate after an unknown result.
            if invocation == 0:
                continue
            existing = find_run(config, plan, ecs)
            if existing:
                instance_id = existing['InstanceId']
                if modify:
                    ecs.call('ModifyInstanceAutoReleaseTime', RegionId=config['region'],
                             InstanceId=instance_id, AutoReleaseTime=plan.auto_release)
                return instance_id
            raise APIError('CreateOutcomeUnknown', f'cannot resolve ClientToken {token}')
    raise APIError('NoCapacity', 'all configured spot candidates reported no stock')


def verify_instance(config, plan, instance, image_id):
    owned(config, instance, plan.tags)
    if instance.get('ImageId') != image_id or parse_time(instance.get('AutoReleaseTime', '')) != parse_time(plan.auto_release):
        raise ValueError(f"instance image or auto release mismatch: {instance['InstanceId']}")
    return instance


def wait_running(config, plan, ecs, instance_id, image_id, *, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    while now() < parse_time(plan.deadline):
        values = describe_instances(config, ecs, instance_id=instance_id)
        if values:
            instance = verify_instance(config, plan, values[0], image_id)
            if instance.get('Status') == 'Running':
                return instance
            if instance.get('Status') not in ('Pending', 'Starting', 'Stopped'):
                raise ValueError(f"unexpected instance status: {instance.get('Status')}")
        sleep(5)
    raise TimeoutError(f'instance {instance_id} did not reach Running before deadline')



def delete_exact(config, ecs, instance_id, expected_tags, *, deadline=None, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    # Cleanup gets its own observation budget even when the work deadline expired.
    cleanup_end = now() + timedelta(minutes=config['deadline']['teardown_reserve_minutes'])
    values = describe_instances(config, ecs, instance_id=instance_id)
    if not values:
        return {'instance_id': instance_id, 'terminal': 'released'}
    if len(values) != 1 or values[0].get('InstanceId') != instance_id:
        raise ValueError('exact instance lookup returned an unexpected identity')
    owned(config, values[0], expected_tags)
    try:
        ecs.call('DeleteInstance', RegionId=config['region'], InstanceId=instance_id, Force=True)
    except (APIError, TimeoutError, ConnectionError) as error:
        if isinstance(error, APIError) and error.code not in ('InvalidInstanceId.NotFound', 'TransportUnknown', 'RequestTimeout'):
            raise
    while now() < cleanup_end:
        values = describe_instances(config, ecs, instance_id=instance_id)
        if not values:
            return {'instance_id': instance_id, 'terminal': 'released'}
        owned(config, values[0], expected_tags)
        sleep(5)
    raise APIError('ReleaseUnconfirmed', f'instance still present or unknown: {instance_id}')
