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

"""ROS change-set preparation and explicit application. Rendering is entirely offline."""
from pathlib import Path
import json
import hashlib
from .ports import APIError


def render(config, parameters=None):
    template = json.loads((Path(__file__).parent / 'infra/foundation.ros.json').read_text())
    if config.data.get('execution_kind') == 'local-rust':
        for name in ('GitHubOidc', 'ControlRole'):
            template['Resources'].pop(name)
        template['Parameters'].pop('GitHubOidcFingerprint')
        template['Outputs'].pop('ControlRoleArn')
    values = {'DeploymentId': config['deployment_id'], 'Repository': config['repository'],
              'BucketName': config['oss']['bucket'], 'ObjectPrefix': config['oss']['prefix'],
              'SecretPrefix': 'g16-' + config['deployment_id'].encode().hex() + '-',
              'InstanceRoleName': config['instance_ram_role'],
              'ExistingVSwitchId': config['candidates'][0]['vswitch_id'],
              'ZoneId': config['candidates'][0]['zone']}
    if parameters:
        unknown = set(parameters) - set(template['Parameters'])
        if unknown:
            raise ValueError('unknown ROS parameters: ' + ', '.join(sorted(unknown)))
        values.update(parameters)
    return {'template': template, 'parameters': values,
            'template_sha256': hashlib.sha256(json.dumps(template, sort_keys=True).encode()).hexdigest()}


def plan_deployment(config, ros, *, stack_name, parameters, create=False):
    candidate = render(config, parameters)
    missing = [name for name, declaration in candidate['template']['Parameters'].items()
               if name not in candidate['parameters'] and 'Default' not in declaration]
    if missing:
        raise ValueError('explicit ROS parameters are required: ' + ', '.join(missing))
    response = ros.call('CreateChangeSet', RegionId=config['region'], StackName=stack_name,
        ChangeSetName='g16-' + candidate['template_sha256'][:20], ChangeSetType='CREATE' if create else 'UPDATE',
        TemplateBody=json.dumps(candidate['template']),
        Parameters=[{'ParameterKey': key, 'ParameterValue': str(value)} for key, value in sorted(candidate['parameters'].items())])
    return {'change_set_id': response['ChangeSetId'], 'stack_id': response.get('StackId'),
            'rendered': candidate}


def inspect_change_set(config, ros, change_set_id):
    return ros.call('GetChangeSet', RegionId=config['region'], ChangeSetId=change_set_id)


def reviewed_change_digest(change):
    """Hash reviewed cloud facts; RequestId identifies the observation transport only."""
    stable = {key: value for key, value in change.items() if key != 'RequestId'}
    return hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()


def apply_deployment(config, ros, *, change_set_id, reviewed_digest):
    # Bind application to the exact reviewed cloud change-set representation.
    change = inspect_change_set(config, ros, change_set_id)
    actual = reviewed_change_digest(change)
    if actual != reviewed_digest:
        raise ValueError('change set differs from the reviewed representation')
    if change.get('Status') != 'CREATE_COMPLETE':
        raise ValueError('change set is not ready for execution')
    return ros.call('ExecuteChangeSet', RegionId=config['region'], ChangeSetId=change_set_id)


def check_deployment(config, ros, *, stack_id, variables=None):
    response = ros.call('GetStack', RegionId=config['region'], StackId=stack_id)
    if response.get('Status') not in ('CREATE_COMPLETE', 'UPDATE_COMPLETE'):
        raise ValueError('ROS stack is not in a successful terminal state')
    outputs = {item['OutputKey']: item['OutputValue'] for item in response.get('Outputs', [])}
    expected = {'BucketName': config['oss']['bucket'], 'ObjectPrefix': config['oss']['prefix'],
                'SecurityGroupId': config['security_group_id'], 'InstanceRoleName': config['instance_ram_role']}
    if any(str(outputs.get(key)) != str(value) for key, value in expected.items()):
        raise ValueError('stack outputs do not match deployment configuration')
    if config.data.get('execution_kind') == 'local-rust':
        return {'matched': True, 'outputs': outputs}
    if variables is None:
        raise ValueError('GitHub deployment requires repository variables')
    if variables.get('ALIYUN_CI_ROLE_ARN') != outputs.get('ControlRoleArn'):
        raise ValueError('repository role variable does not match stack output')
    encoded = variables.get('ALIYUN_CI_CONFIG_JSON')
    if isinstance(encoded, str):
        encoded = json.loads(encoded)
    if encoded != config.data:
        raise ValueError('repository deployment configuration differs from local configuration')
    return {'matched': True, 'outputs': outputs}
