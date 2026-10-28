#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run one private mc script under the original machine and caller deadlines."""
import argparse
import json
import os
from pathlib import Path
import sys

import fixture_runtime as runtime
import machine_runtime as machine
from scratch import RUN_STORAGE_ENV, RunBudget, RunStorageScope, ScratchIdentity, ScratchSweeper


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--daemon', required=True)
    parser.add_argument('--expected-version', required=True)
    parser.add_argument('--expected-generation', required=True)
    parser.add_argument('--context-file', type=Path)
    parser.add_argument('--script-file', type=Path)
    parser.add_argument('--cleanup', action='store_true')
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument('--cleanup-run', action='store_true')
    operations.add_argument('--oss-operation', action='store_true')
    operations.add_argument('--benchmark-oss', action='store_true')
    args = parser.parse_args(argv)
    runtime.install_signal_handlers()
    try:
        if args.cleanup and (args.oss_operation or args.benchmark_oss):
            raise runtime.RuntimeFailure('RunBudgetRejected', 'SDK work cannot use the cleanup reserve')
        raw = args.context_file.read_text() if args.context_file else os.environ.get('NOVA_FIXTURE_RUN_CONTEXT', '')
        if len(raw) > 16384:
            raise runtime.RuntimeFailure('RunBudgetRejected', 'run context exceeds its input bound')
        context = json.loads(raw)
        identity = ScratchIdentity.from_dict(context.get('identity'))
        owner = machine.MachineOwner(args.root, daemon=args.daemon)
        state = owner.read_saved_state()
        if not state:
            raise runtime.RuntimeFailure('MachineUnavailable', 'machine has no saved authority')
        budget = RunBudget.admit(state, identity, inherited=context, cleanup=args.cleanup)
        deadline = budget.deadline - (0 if args.cleanup else budget.cleanup_grace_seconds)
        if args.script_file:
            with args.script_file.open() as source:
                script = source.read(65537)
        else:
            script = sys.stdin.read(65537)
        if len(script) > 65536 or not script:
            raise runtime.RuntimeFailure('InvalidConfiguration', 'mc script is empty or exceeds its input bound')
        with owner.ready_snapshot({'version': args.expected_version}, deadline=deadline,
                                  expected_generation=args.expected_generation) as snapshot:
            inherited_storage = os.environ.get(RUN_STORAGE_ENV)
            if inherited_storage is not None:
                if len(inherited_storage) > 16384:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'run storage exceeds its input bound')
                RunStorageScope.from_dict(json.loads(inherited_storage)).validate_against(identity, snapshot)
            if args.cleanup_run:
                if not args.cleanup:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'exact run cleanup requires the original cleanup phase')
                request = json.loads(script)
                if ScratchIdentity.from_dict(request.get('identity')) != identity:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'cleanup identity differs from original context')
                scope = RunStorageScope.from_dict(request.get('storage'))
                scope.validate_against(identity, snapshot)
                report = ScratchSweeper(snapshot, owner.backend).cleanup_run(scope, request.get('users'), deadline=deadline)
                if not report.get('complete'):
                    sys.stderr.write(json.dumps(report) + '\n')
                    raise runtime.RuntimeFailure('ScratchCleanupIncomplete', 'exact run cleanup did not complete')
                output = json.dumps(report) + '\n'
            elif args.benchmark_oss:
                import benchmark_oss
                output = benchmark_oss.run_operation(snapshot, owner.backend, json.loads(script), deadline=deadline)
            elif args.oss_operation:
                from machine_operation import validate_sdk_arguments
                request = json.loads(script)
                if not isinstance(request, dict) or set(request) != {'storage', 'args'}:
                    raise runtime.RuntimeFailure('InvalidScratchScope', 'OSS request requires its exact run projection')
                scope = RunStorageScope.from_dict(request['storage']).validate_against(identity, snapshot)
                if scope.storage_owner.get('provider') != 'aliyun-oss':
                    raise runtime.RuntimeFailure('InvalidScratchScope', 'OSS work requires a cloud run projection')
                arguments = request['args']
                if not isinstance(arguments, list) or len(arguments) < 3 or not scope.belongs(arguments[2]):
                    raise runtime.RuntimeFailure('InvalidScratchScope', 'OSS request leaves the original run')
                arguments = list(arguments)
                if arguments[2].startswith('s3a://'):
                    arguments[2] = 's3://' + arguments[2][6:]
                output = owner.backend.run_oss(snapshot, validate_sdk_arguments(arguments), deadline=deadline)
            else:
                if snapshot.get('config', {}).get('object_store') is not None:
                    raise runtime.RuntimeFailure('CloudIamUnsupported', 'cloud runs require an explicit OSS operation')
                output = owner.backend.run_mc(snapshot, script, deadline=deadline)
        sys.stdout.write(output)
        return 0
    except runtime.RuntimeFailure as error:
        print(error, file=sys.stderr)
        return 75 if error.code in machine.BLOCKED else 1
    except (OSError, ValueError, TypeError, AttributeError):
        print('RunBudgetRejected: invalid private mc request', file=sys.stderr)
        return 75
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
