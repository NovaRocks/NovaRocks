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

"""Explicit offline plans and operator commands; SDK imports stay lazy."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import os
import re
from pathlib import Path
import secrets
import sys
import subprocess
from .config import load_config
from .plan import RunPlan, derive, parse_time


def add_run_options(parser):
    parser.add_argument('--run-id')
    parser.add_argument('--run-attempt', type=int, default=1)
    parser.add_argument('--now')
    parser.add_argument('--nonce')
    parser.add_argument('--role', choices=('worker', 'builder'), default='worker')
    parser.add_argument('--plan', type=Path, help='Non-secret start output, if available')


def load_plan(config, args):
    if args.plan and args.plan.exists():
        value = json.loads(args.plan.read_text())
        value = value.get('plan', value)
        plan = RunPlan(**value)
        from .plan import identity_tags
        expected = identity_tags(config, plan.run_id, plan.run_attempt, plan.role)
        if any(plan.tags.get(key) != value for key, value in expected.items()):
            raise ValueError('saved plan does not belong to this deployment')
        return plan
    run_id = args.run_id or ('local-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + secrets.token_hex(2))
    return derive(config, run_id, args.run_attempt,
                  parse_time(args.now) if args.now else datetime.now(timezone.utc),
                  nonce=args.nonce, role=args.role)


def retain_plan(args, plan):
    # Write non-secret identity before the first cloud side effect; failure may
    # prevent normal job outputs, and local auto-generated run IDs must survive.
    serialized = json.dumps(plan.to_dict(), sort_keys=True)
    print('AliyunCIRunPlan: ' + serialized, file=sys.stderr, flush=True)
    if args.output:
        path = args.output.with_suffix('.plan.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x') as target: target.write(serialized + '\n')


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument('--config', type=Path, required=True)
    root.add_argument('--output', type=Path)
    root.add_argument('--github-token-env', default='GH_TOKEN', help='Name of explicitly provided token environment variable')
    sub = root.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-config')
    run = sub.add_parser('plan-run'); add_run_options(run)
    start = sub.add_parser('start'); add_run_options(start)
    start.add_argument('--sha', required=True)
    start.add_argument('--image-id', required=True)
    start.add_argument('--mode', choices=('rust', 'probe'), default='rust')
    teardown = sub.add_parser('teardown'); add_run_options(teardown)
    teardown.add_argument('--instance-id')
    teardown.add_argument('--image-id')
    teardown.add_argument('--rust-result', help='Original test step result JSON')
    teardown.add_argument('--tools-check', choices=('true', 'false'))
    sub.add_parser('janitor')
    sub.add_parser('runner-sync')
    local = sub.add_parser('local-run'); add_run_options(local)
    local.add_argument('--source', type=Path, default=Path.cwd())
    local.add_argument('--sha')
    local.add_argument('--image-id', required=True)
    local.add_argument('--record', type=Path, required=True)
    local.add_argument('--prepare-only', action='store_true')
    for name in ('local-status', 'local-cancel', 'local-collect'):
        item=sub.add_parser(name)
        item.add_argument('--record', type=Path, required=True)
        if name == 'local-collect':
            item.add_argument('--destination', type=Path, required=True)

    image = sub.add_parser('image'); add_run_options(image)
    image.set_defaults(role='builder')
    image.add_argument('--preparation-sha', required=True)
    image.add_argument('--source-archive', type=Path, required=True)
    promote = sub.add_parser('promote-image')
    promote.add_argument('--image-id', required=True)
    promote.add_argument('--summary-key', required=True)
    deploy = sub.add_parser('deploy')
    deploy.add_argument('operation', choices=('render', 'plan', 'inspect', 'apply', 'check'))
    deploy.add_argument('--parameters', type=Path)
    deploy.add_argument('--stack-name')
    deploy.add_argument('--stack-id')
    deploy.add_argument('--create', action='store_true')
    deploy.add_argument('--change-set-id')
    deploy.add_argument('--reviewed-digest')
    deploy.add_argument('--variables', type=Path)
    return root


def execute(config, args):
    if args.command == 'validate-config': return {'valid': True}, 0
    if args.command == 'plan-run': return load_plan(config, args).to_dict(), 0
    if args.command == 'deploy' and args.operation == 'render':
        from .deploy import render
        return render(config, json.loads(args.parameters.read_text()) if args.parameters else None), 0
    if args.command.startswith('local-'):
        if config.data.get('execution_kind') != 'local-rust':
            raise ValueError('local commands require execution_kind local-rust')
        if args.command == 'local-run':
            from .source_bundle import create
            source_dir = args.record.parent / (args.record.stem + '-source')
            manifest = create(args.source, source_dir, sha=args.sha)
            plan=load_plan(config,args)
            if args.prepare_only:
                return {'plan':plan.to_dict(),'source':manifest,'source_directory':str(source_dir)},0
    from .sdk import adapters
    services = adapters(config)
    if args.command.startswith('local-'):
        from .local_run import start_local, wait_local, status_local, collect_local, cancel_local, finish_local, result_exit_code
        ecs,oos,oss=(services[key] for key in ('ecs','oos','oss'))
        if args.command == 'local-status':
            return status_local(config,ecs,args.record),0
        if args.command == 'local-cancel':
            return cancel_local(config,ecs,args.record),0
        if args.command == 'local-collect':
            return collect_local(config,oss,args.record,args.destination),0
        control_error = None
        try:
            start_local(config,plan,ecs,oos,oss,image_id=args.image_id,source=source_dir,record_path=args.record)
            wait_local(config,ecs,args.record)
        except (KeyboardInterrupt, Exception) as error:
            control_error = {'error': type(error).__name__}
            if args.record.exists():
                try:
                    cancel_local(config,ecs,args.record)
                except Exception as cancel_error:
                    control_error['cancel_error'] = type(cancel_error).__name__
        finally:
            if args.record.exists():
                result=finish_local(config,ecs,oos,oss,args.record)
                if control_error:
                    result['control_error'] = control_error
                    # Retain control failure alongside confirmed release even
                    # when the original observer could not report a terminal.
                    from .local_run import _update
                    _update(args.record, summary=result)
        if control_error:
            if not args.record.exists():
                raise RuntimeError('local request failed before cloud identity was persisted')
            return result,1
        return result,result_exit_code(result)
    if args.command == 'deploy':
        from . import deploy
        ros = services['ros']
        if args.operation == 'plan':
            if not args.stack_name: raise ValueError('--stack-name is required')
            value = deploy.plan_deployment(config, ros, stack_name=args.stack_name, create=args.create,
                     parameters=json.loads(args.parameters.read_text()) if args.parameters else {})
        elif args.operation == 'inspect':
            if not args.change_set_id: raise ValueError('--change-set-id is required')
            value = deploy.inspect_change_set(config, ros, args.change_set_id)
            value = {'change_set': value, 'reviewed_digest': deploy.reviewed_change_digest(value)}
        elif args.operation == 'apply':
            if not args.change_set_id or not args.reviewed_digest:
                raise ValueError('--change-set-id and --reviewed-digest are required')
            value = deploy.apply_deployment(config, ros, change_set_id=args.change_set_id, reviewed_digest=args.reviewed_digest)
        else:
            if not args.stack_id: raise ValueError('--stack-id is required')
            value = deploy.check_deployment(config, ros, stack_id=args.stack_id, variables=json.loads(args.variables.read_text()) if args.variables else None)
        return value, 0
    oss, ecs, oos = (services[key] for key in ('oss', 'ecs', 'oos'))
    if args.command == 'image':
        from .image import build_image
        repo = Path(__file__).resolve().parents[3]
        expected = subprocess.check_output(['git', '-C', str(repo), 'archive', '--format=tar', args.preparation_sha])
        if args.source_archive.read_bytes() != expected:
            raise ValueError('source archive differs from the exact preparation commit')
        plan = load_plan(config, args)
        retain_plan(args, plan)
        from .image_tools import stage_nextest
        nextest = stage_nextest(config, oss, expected, plan.deadline)
        return build_image(config, plan, ecs, oss,
                 preparation_sha=args.preparation_sha, source_archive=expected, nextest=nextest), 0
    if args.command == 'promote-image':
        from .image import promote_image
        return promote_image(config, oss, args.image_id, args.summary_key), 0
    github = None
    if config.data.get('execution_kind', 'github-runner') == 'github-runner':
        from .github_api import GitHub
        github = GitHub(os.environ.get(args.github_token_env))
    if args.command == 'runner-sync':
        from .runner_sync import sync_runner
        return sync_runner(config, github, oss), 0
    if args.command == 'janitor':
        from .janitor import sweep
        value = sweep(config, ecs, oos, oss, github)
        return value, int(any(item.get('terminal') == 'unknown' or 'error' in item for item in value['anomalies']))
    plan = load_plan(config, args)
    if args.command == 'start':
        retain_plan(args, plan)
        from .runner_sync import current_runner
        from .start import start_run
        if not re.fullmatch(r'[0-9a-fA-F]{40}', args.sha):
            raise ValueError('sha must be a complete 40-hex commit')
        if plan.tags['origin'] == 'local':
            repo = Path(__file__).resolve().parents[3]
            resolved = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', '--verify',
                                                 args.sha + '^{commit}'], text=True).strip()
            if resolved.lower() != args.sha.lower():
                raise ValueError('local probe SHA does not identify the exact preparation commit')
        runner = current_runner(config, oss)
        runner['url'] = oss.presign('GET', runner['key'], plan.deadline)
        value = start_run(config, plan, ecs, oos, oss, github, sha=args.sha,
                          image_id=args.image_id, runner=runner, mode=args.mode)
        return value, 0
    from .teardown import teardown_run
    value = teardown_run(config, plan, ecs, oos, oss, github, instance_id=args.instance_id,
        image_id=args.image_id, rust_result=json.loads(args.rust_result) if args.rust_result else None,
        tools_check=None if args.tools_check is None else args.tools_check == 'true')
    return value, int(value['compute_terminal'] != 'released' or bool(value['leftovers']) or value.get('summary_publication', {}).get('published') is False)


def main(argv=None):
    command_parser = parser()
    args = command_parser.parse_args(argv)
    try:
        value, code = execute(load_config(args.config), args)
        serialized = json.dumps(value, indent=2, sort_keys=True)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open('x') as target: target.write(serialized + '\n')
        # Only non-secret outputs may cross job boundaries.
        if args.command == 'start' and os.environ.get('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'], 'a') as target:
                for key in ('instance_id', 'label', 'deadline', 'test_timeout_minutes', 'image_id'):
                    target.write(f'{key}={value[key]}\n')
                target.write('plan=' + json.dumps(value['plan'], sort_keys=True) + '\n')
        print(serialized)
        return code
    except (ValueError, OSError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as error:
        command_parser.exit(2, f'AliyunCIError: {error}\n')
