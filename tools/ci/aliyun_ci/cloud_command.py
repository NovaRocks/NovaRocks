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

"""One dispatch, exact Cloud Assistant invocation observations; no implicit replay."""
from __future__ import annotations
import base64
from datetime import datetime, timezone
import time
from .plan import parse_time
from .ports import APIError

TERMINAL = frozenset(('Success', 'Failed', 'Error', 'Timeout', 'Cancelled', 'Terminated', 'Invalid', 'Aborted'))


def dispatch(config, ecs, instance_id, script, deadline, *, persist, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    timeout = int((parse_time(deadline) - now()).total_seconds())
    if timeout <= 0:
        raise ValueError('cloud command deadline has expired')
    command = {'instance_id': instance_id, 'deadline': deadline, 'dispatch_state': 'pending', 'invoke_id': None}
    persist(command)
    try:
        # A documented not-online refusal proves no command was accepted. All
        # transport/unknown outcomes remain unknown and are never resubmitted.
        while True:
            timeout = int((parse_time(deadline) - now()).total_seconds())
            if timeout <= 0:
                raise TimeoutError('Cloud Assistant dispatch deadline has expired')
            try:
                response = ecs.call('RunCommand', RegionId=config['region'], InstanceId=[instance_id],
                    Type='RunShellScript', CommandContent=base64.b64encode(script.encode()).decode(),
                    ContentEncoding='Base64', Timeout=timeout, TerminationMode='ProcessTree')
                break
            except APIError as error:
                if error.code not in ('InvalidInstance.NotConnected', 'InvalidInstance.NotOnline'):
                    raise
                if now() >= parse_time(deadline):
                    raise TimeoutError('Cloud Assistant did not come online before deadline') from None
                sleep(5)
        invocation = response.get('InvokeId')
        if not isinstance(invocation, str) or not invocation:
            raise ValueError('RunCommand returned no InvokeId')
        command.update(invoke_id=invocation, dispatch_state='accepted')
        persist(command)
        return command
    except BaseException as error:
        command.update(dispatch_state='unknown', error=type(error).__name__)
        persist(command)
        raise


def observe(config, ecs, command):
    if command.get('dispatch_state') != 'accepted' or not command.get('invoke_id'):
        return {'terminal': False, 'status': 'DispatchUnknown', 'exit_code': None,
                'invoke_id': command.get('invoke_id'), 'instance_id': command['instance_id']}
    invocation, instance = command['invoke_id'], command['instance_id']
    response = ecs.call('DescribeInvocationResults', RegionId=config['region'], InvokeId=invocation,
        InstanceId=instance, ContentEncoding='Base64')
    rows = response.get('Invocation', {}).get('InvocationResults', {}).get('InvocationResult', [])
    if not rows:
        return {'terminal': False, 'status': 'Pending', 'exit_code': None,
                'invoke_id': invocation, 'instance_id': instance}
    if len(rows) != 1 or rows[0].get('InstanceId') != instance or rows[0].get('InvokeId') != invocation:
        raise ValueError('Cloud Assistant result identity mismatch')
    row = rows[0]
    terminal = row.get('InvocationStatus') in TERMINAL
    output = base64.b64decode(row.get('Output', ''), validate=True).decode() if terminal else None
    return {'terminal': terminal, 'status': row.get('InvocationStatus'), 'exit_code': row.get('ExitCode'),
            'invoke_id': invocation, 'instance_id': instance, 'dropped': row.get('Dropped', 0), 'output': output}


def wait(config, ecs, command, *, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    if command.get('dispatch_state') != 'accepted':
        return observe(config, ecs, command)
    while now() < parse_time(command['deadline']):
        result = observe(config, ecs, command)
        if result['terminal']:
            return result
        sleep(5)
    raise TimeoutError('Cloud Assistant original command has no terminal observation before deadline')
