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
"""Exclusive development attempts and read-only cleanup confirmation."""
from dataclasses import asdict
import copy
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys

LIB = Path(__file__).resolve().parents[1] / 'lib'
sys.path.insert(0, str(LIB))
import identity_snapshot
import invocation_record
from .compose import compose
from .dispositions import Dispositions
from .materials import read_attempt, read_rust, read_sql, sql_scope_identity, read_monitor_cleanup, _sql_case_cleanup_failed
from .model import Attempt, Scope, UnitKey
from .record import write_record
from .select import (select, changed_paths, sql_case_arguments,
                     RUST_AGGREGATE, RUST_AGGREGATE_DEFERRED)
from .declarations import native_selector_requests, validate_declarations
from .scope import required_scopes


def exclusive(parent, label='attempt'):
    parent = Path(parent).expanduser().resolve()
    parent.mkdir(parents=True, exist_ok=True)
    root = parent / (datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + label + '-' + secrets.token_hex(5))
    root.mkdir(mode=0o700)
    return root


def default_root(repo):
    state = Path(os.environ.get('XDG_STATE_HOME') or Path.home() / '.local/state')
    repo = Path(repo).resolve()
    return state / 'novarocks/verification' / (repo.name.lower() + '-' + hashlib.sha256(str(repo).encode()).hexdigest()[:12])


def json_command(argv, repo):
    return json.loads(subprocess.check_output([str(value) for value in argv], cwd=repo, text=True))


def prepare(repo, root):
    output = exclusive(Path(root) / 'preparations', 'native')
    with (output / 'prepare.log').open('xb') as log:
        subprocess.run([sys.executable, str(Path(repo) / 'tools/ci/test-run.py'), 'prepare', '--output', str(output / 'inputs')], cwd=repo, stdout=log, stderr=subprocess.STDOUT, check=True)
    return output / 'inputs'


def inventory(repo, inputs):
    inputs = Path(inputs)
    receipt = json.loads((inputs / 'artifacts.json').read_text())
    for entry in receipt.values():
        if identity_snapshot.digest(entry['path']) != entry['sha256']:
            raise ValueError('prepared artifact changed: ' + entry['path'])
    metadata = json.loads((inputs / 'cargo-metadata.json').read_text())
    sql = json_command([receipt['novarocks-sql-test']['path'], '--list-declarations'], repo)
    system = json_command([receipt['novarocks-system-tests']['path'], '--list-declarations'], repo)
    stages = json_command(['bash', str(Path(repo) / 'tools/ci/local-full-ci.sh'), '--list-stages'], repo)
    return {'metadata':metadata,'sql':sql,'system':system,'stages':stages,'receipt':receipt,'inputs':str(inputs),
            'repo':str(Path(repo).resolve())}


def selection(repo, base, native):
    rules = json.loads((Path(repo) / 'tools/ci/verification/impact-rules.json').read_text())
    return select(changed_paths(repo, base), native['metadata'], native['sql'], native['system'], native['stages'], rules, repo)


def native_list(inputs, expression=None, ignored=None):
    inputs = Path(inputs)
    command = json.loads((inputs / 'selected-tests.command.json').read_text())
    # G6a command files contain the exact native argv (not a shell string).
    cwd = command.get('cwd') if isinstance(command, dict) else None
    if isinstance(command, dict):
        command = command['argv']
    # G6a owns the prefix through its JSON output option. All trailing native
    # selection arguments (including positional names and partitions) are
    # replaced here so a narrowed preparation cannot hide verification units.
    original=list(command)
    marker=original.index('--message-format')
    if original[marker+1]!='json':raise ValueError('native list preparation is not JSON')
    command=original[:marker+2]
    prepared=json.loads((inputs/'prepared.json').read_text())
    command+=['--profile',prepared['profile']]
    if expression:
        command += ['-E', expression]
    if ignored:
        command += ['--run-ignored', ignored]
    return json.loads(subprocess.check_output(command, cwd=cwd, text=True))


def ignored_inventory(native, listing):
    """The native selector, rather than Python, resolves every ignored rule."""
    requests = native_selector_requests(native['metadata'])
    results = {request['key']: native_list(native['inputs'], request['filterset'], 'only')
               for request in requests}
    bindings = json.loads((Path(native['inputs']) / 'test-artifacts.json').read_text())
    checked = validate_declarations(native['metadata'], listing, bindings, results)
    if not checked['ok']:
        raise ValueError('invalid native Rust declarations: ' + '; '.join(checked['errors']))
    request_by_key = {request['key']: request for request in requests}
    packages = {package['id']: package for package in native['metadata']['packages']}
    units = {}
    for coverage in checked['coverage']:
        request = request_by_key[coverage['declarations'][0]]
        declaration = packages[request['package_id']]['metadata']['novarocks']['test']['ignored'][request['index']]
        units[coverage['unit']] = dict(declaration, package=request['package'], filterset=request['filterset'])
    native['ignored_units'] = units
    return units


def java_rest_owner(repo):
    path = Path(repo) / 'docker/iceberg-rest/tests/java_rest_inputs.py'
    spec = importlib.util.spec_from_file_location('_java_rest_inputs', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def java_rest_group(repo, values, ignored, bindings, listing):
    owned = [scope for scope in values if ignored.get(str(scope.units[0]), {}).get('input-owner') == 'iceberg-java-rest-interop']
    if not owned:
        return values
    owned.sort(key=lambda scope: str(scope.units[0]))
    owner = java_rest_owner(repo)
    names = [_rust_binding(scope, bindings, listing)[1] for scope in owned]
    owner.validate_group(names)
    if any(scope.identity != owned[0].identity for scope in owned):
        raise ValueError('Java REST owner members have different native artifact/input bindings')
    group = Scope(owner.SCOPE, tuple(scope.units[0] for scope in owned), owned[0].identity,
                  'isolated', all(scope.inputs_declared for scope in owned),
                  any(scope.selected for scope in owned), 'heavy')
    return [scope for scope in values if scope not in owned] + [group]


def rust_environment(repo, native):
    return dict(os.environ, NOVAROCKS_BIN=native['receipt']['novarocks']['path'], NOVAROCKS_WORKSPACE_ROOT=str(Path(repo).resolve()))


# Design: ADR-0186 (docs/adr/ADR-0186-layered-verification-and-read-only-evidence-composition.md)
def scopes(repo, native, chosen):
    """Build obligations from native declarations before reading any attempt."""
    inputs = Path(native['inputs'])
    listing = native_list(inputs, ignored='all')
    ignored = ignored_inventory(native, listing)
    native['regular_inventory'] = listing
    selected = native_list(inputs, chosen['rust_filterset'], 'all') if chosen['rust_filterset'] else {'rust-suites':{}}
    env = rust_environment(repo, native)
    identity_inputs=exclusive(inputs.parent/'identity-inputs','target')
    for filename in ('artifacts.json','cargo-metadata.json','test-artifacts.json','prepared.json','nextest.toml'):
        (identity_inputs/filename).symlink_to(inputs/filename)
    identity_listing = copy.deepcopy(listing)
    for binary, suite in identity_listing['rust-suites'].items():
        for name, case in suite['testcases'].items():
            declaration = ignored.get('rust-test:' + binary + '::' + name)
            retained = not case['ignored'] or (declaration and declaration['class'] == 'heavy')
            # This target-only list is never represented as execution evidence.
            case['filter-match'] = {'status':'matches' if retained else 'mismatch'}
    write_record(identity_inputs/'selected-tests.json',identity_listing)
    native['identity_inputs'] = str(identity_inputs)
    snapshots = identity_snapshot.rust_snapshot(repo, identity_inputs, env, 'nextest')
    metadata = {value['id']:value for value in native['metadata']['packages']}
    bindings = json.loads((inputs / 'test-artifacts.json').read_text())['binaries']
    result = []
    for binary, suite in listing['rust-suites'].items():
        package = metadata[bindings[binary]['package-id']]
        declaration = (package.get('metadata') or {}).get('novarocks', {}).get('test', {})
        declared = all(key in declaration for key in ('runtime-inputs','env-inputs'))
        for name, case in suite['testcases'].items():
            classification = ignored.get('rust-test:' + binary + '::' + name)
            if case.get('ignored') and (not classification or classification['class'] != 'heavy'):
                continue
            key = binary + '::' + name
            unit = UnitKey('rust-test', key)
            value = snapshots.get(key)
            wanted = selected.get('rust-suites', {}).get(binary, {}).get('testcases', {}).get(name, {}).get('filter-match', {}).get('status') == 'matches'
            result.append(Scope(str(unit),(unit,),value['inputs'] if value else None,'isolated',declared,wanted,
                                'heavy' if case.get('ignored') else 'regular'))
    chosen['resolved_heavy_ignored'] = [dict(ignored[str(scope.units[0])], unit=str(scope.units[0]), selected=scope.selected)
                                      for scope in result if scope.tier == 'heavy']
    result = java_rest_group(repo, result, ignored, bindings, listing)
    doc = identity_snapshot.rust_snapshot(repo, inputs, env, 'doctest')['workspace']
    result.append(Scope('rust-doctest:workspace',(UnitKey('rust-doctest','workspace'),),doc['inputs'],'isolated',doc['declared'],chosen['doctest']))
    selected_sql = {value['name'] for value in chosen['sql_suites']}
    for suite in native['sql']['suites']:
        reuse = suite.get('reuse', {})
        units = tuple(UnitKey('sql-case',suite['name']+'/'+case['case']) for case in suite['cases'])
        if not units:
            continue
        # Future private runtime identities are unknown before the producer has
        # actually created them. No historical e2e identity is guessed here.
        for group in ([unit] for unit in units) if reuse.get('scope') == 'case' and reuse.get('initial_state') else [units]:
            key = str(group[0]) if len(group)==1 and reuse.get('scope')=='case' else 'sql-suite:'+suite['name']
            result.append(Scope(key,tuple(group),None,reuse.get('initial_state'),reuse.get('env_inputs') is not None,
                suite['name'] in selected_sql,'heavy' if suite.get('explicit_only') else 'regular',bool(suite.get('disabled_reason'))))
    selected_system={value['name'] for value in chosen['system_scenarios']}
    for item in native['system']['scenarios']:
        unit=UnitKey('system-scenario',item['name']);declaration=item['declaration']
        spec=invocation_record.execution_spec('system-scenario', system_argv(native,item,'/unused/output'), system_spec(native,item))
        identity=invocation_record.snapshot(repo,spec,env)
        result.append(Scope(str(unit),(unit,),identity,declaration.get('initial_state'),declaration.get('env_inputs') is not None,
            item['name'] in selected_system,'regular' if item.get('default') else 'heavy',bool(item.get('paused'))))
    selected_stages={value['name'] for value in chosen['stages']}
    for item in native['stages']['stages']:
        mode = 'loom' if item['name']=='memory-model:loom' else 'miri' if item['name']=='memory-model:miri' else 'default'
        unit=UnitKey('stage',item['name'],mode)
        spec=stage_spec(repo,native,item,'/unused/output')
        spec=invocation_record.execution_spec('stage',stage_argv(repo,native,item,'/unused/output'),spec)
        identity=invocation_record.snapshot(repo,spec,env)
        deferred = dict(RUST_AGGREGATE_DEFERRED) if item['name'] == RUST_AGGREGATE else None
        result.append(Scope(str(unit),(unit,),identity,'immutable',True,
            item['name'] in selected_stages and deferred is None,
            'heavy' if item['tier']=='full' else 'regular',deferred=deferred))
    return result


def system_argv(native,item,output):
    repo=Path(native.get('repo', Path(__file__).resolve().parents[3]))
    # Match the native shell owner's argv order as well as its effective inputs.
    argv = [native['receipt']['novarocks-system-tests']['path'],
        '--binary',native['receipt']['novarocks']['path']]
    for role in ('compatible','other-island'):
        binding = native.get('system_variants', {}).get(role)
        if binding:
            if identity_snapshot.digest(binding['path']) != binding['sha256']:
                raise ValueError('prepared System variant changed: ' + role)
            argv += ['--' + role + '-binary', binding['path']]
    argv += ['--config',str(repo/'tools/ci/fixtures/system-scenarios-base.toml'),
        '--artifact-root',str(output),'--cluster-size','3','--timeout-secs','900','--only',item['name']]
    return argv


def stage_spec(repo,native,item,output):
    argv=stage_argv(repo,native,item,output)
    argv[2]='--describe-stage'
    descriptor=json_command(argv,repo)
    if (descriptor.get('schema')!=1 or descriptor.get('name')!=item['name']
            or descriptor.get('argv')!=descriptor.get('spec',{}).get('native_command')):
        raise ValueError('invalid native stage command descriptor')
    spec=descriptor['spec']
    if 'inputs' in item:spec['inputs']=item['inputs']
    return spec


def stage_argv(repo,native,item,output):
    return ['bash',str(Path(repo)/'tools/ci/local-full-ci.sh'),'--run-stage',item['name'],str(Path(native['inputs'])/'artifacts.json'),str(output)]


def system_spec(native,item):
    declaration=item['declaration']
    # execution_spec captures the actual runner, programs and config from argv.
    return {'inputs':[],'env_inputs':declaration.get('env_inputs') or [],'declaration':declaration,
            'declared':declaration.get('env_inputs') is not None}


def prepare_system_variants(repo, native, chosen, root):
    """Reuse the owner helper's variant builds and exact main-graph restoration."""
    items = [item for item in chosen['system_scenarios'] if not item.get('paused')]
    if not items:
        native['system_variants_prepared']=True
        return
    argv = [native['receipt']['novarocks-system-tests']['path'], '--list-binary-requirements']
    for item in items:
        argv += ['--only', item['name']]
    requirements = subprocess.check_output(argv, cwd=repo, text=True).splitlines()
    if not requirements or any(role not in {'primary','compatible','other-island'} for role in requirements):
        raise ValueError('invalid native System program requirements')
    if set(requirements) == {'primary'}:
        native['system_variants_prepared']=True
        return
    output = exclusive(Path(root) / 'preparations', 'system-variants')
    (output / 'requirements.txt').write_text('\n'.join(requirements) + '\n')
    shell = r'''source "$1/tools/ci/lib/system_scenarios.sh"
REPO_ROOT="$1"
NOVA_CI_ARTIFACT_RECEIPT="$2"
export NOVA_CI_ARTIFACT_RECEIPT
ci_prepare_system_scenario_binaries "$3" dev "$4/fixtures" "$5" "$4/build.log" "$4/requirements.txt" || exit $?
python3 - "$4/bindings.json" "$SYSTEM_SCENARIO_PRIMARY_BINARY" "$SYSTEM_SCENARIO_COMPATIBLE_BINARY" "$SYSTEM_SCENARIO_OTHER_ISLAND_BINARY" "$1/tools/ci/lib" <<'BINDINGS'
import json,sys
sys.path.insert(0,sys.argv[5])
from identity_snapshot import digest
value={role:{'path':path,'sha256':digest(path)} for role,path in zip(['primary','compatible','other-island'],sys.argv[2:5]) if path}
with open(sys.argv[1],'x') as stream:json.dump(value,stream,indent=2)
BINDINGS
'''
    command = ['bash','-c',shell,'system-variants',str(Path(repo).resolve()),str(Path(native['inputs'])/'artifacts.json'),
               native['receipt']['novarocks']['path'],str(output),output.name]
    write_record(output/'command.json', {'argv':command,'cwd':str(repo)})
    with (output/'stdout.log').open('xb') as out, (output/'stderr.log').open('xb') as err:
        result = subprocess.run(command, cwd=repo, env=rust_environment(repo,native), stdout=out, stderr=err)
    write_record(output/'exit.json', {'code':result.returncode})
    if result.returncode:
        raise ValueError('native System variant preparation failed; see ' + str(output))
    bindings = json.loads((output/'bindings.json').read_text())
    if any(role not in bindings for role in set(requirements)):
        raise ValueError('native System helper did not bind all required programs')
    native['system_variants'] = {role:binding for role,binding in bindings.items() if role != 'primary'}
    native['receipt'] = json.loads((Path(native['inputs'])/'artifacts.json').read_text())
    native['system_variants_prepared']=True


def refresh_development_programs(repo, native, output):
    """Let the existing main-graph producer renew its own receipt after variants."""
    inputs=Path(native['inputs'])
    output=Path(output)/'development-program-refresh'
    output.mkdir(parents=True,exist_ok=False)
    # Preserve the previous canonical inputs and raw Cargo streams before their
    # owning producer updates artifacts.json in place. Never infer new hashes.
    shutil.copytree(inputs,output/'before',symlinks=True)
    shell = ('source "$1/tools/ci/lib/build_capabilities.sh"; REPO_ROOT="$1"; '
             'ci_prepare_main "$2/artifacts.json" && ci_capture_main_builds "$2/artifacts.json" "$3/build-info"')
    argv=['bash','-c',shell,'refresh-development-programs',str(Path(repo).resolve()),str(inputs),str(output)]
    write_record(output/'command.json',{'argv':argv,'cwd':str(repo),
        'previous_inputs':str(inputs),'previous_receipt':native['receipt'],
        'meaning':'Original main-graph producer only; no test execution or target identity rewrite.'})
    code=None
    try:
        with (output/'stdout.log').open('xb') as out,(output/'stderr.log').open('xb') as err:
            code=subprocess.run(argv,cwd=repo,env=rust_environment(repo,native),stdout=out,stderr=err).returncode
    finally:
        write_record(output/'exit.json',{'code':code})
        shutil.copytree(inputs,output/'after',symlinks=True)
    if code:
        raise ValueError('development main-program refresh failed; see '+str(output))
    receipt=json.loads((inputs/'artifacts.json').read_text())
    if set(receipt)!=set(native['receipt']):
        raise ValueError('development main-program refresh changed the bound program set')
    for entry in receipt.values():
        if identity_snapshot.digest(entry['path'])!=entry['sha256']:
            raise ValueError('refreshed development artifact changed: '+entry['path'])
    native['receipt']=receipt


def invoke(root,repo,kind,name,units,argv,spec,materials,environment=None):
    environment=dict(os.environ if environment is None else environment)
    started=invocation_record.start(root,repo,kind,name,[str(unit) for unit in units],[str(a) for a in argv],spec,environment)
    log=Path(root)/'logs'/('{:04d}.log'.format(json.loads(started.read_text())['seq']))
    log.parent.mkdir(parents=True,exist_ok=True)
    files=invocation_record.command_inputs([str(a) for a in argv])
    def binary(path):
        return {'path':str(path),'sha256':identity_snapshot.digest(path) if Path(path).is_file() else None}
    primary=files.get('--binary') or environment.get('NOVAROCKS_BIN')
    binaries={'primary':binary(primary) if primary else None,
              'variants':[dict(binary(path),role=role.removeprefix('--').removesuffix('-binary'))
                          for role,path in files.items() if role in {'--compatible-binary','--other-island-binary'}]}
    code=None
    spawn_error=None
    try:
        with log.open('xb') as stream:
            try:
                code=subprocess.run([str(a) for a in argv],cwd=repo,env=environment,stdout=stream,stderr=subprocess.STDOUT).returncode
            except OSError as error:
                spawn_error={'type':type(error).__name__,'message':str(error)}
                stream.write(('executor did not start: '+str(error)+'\n').encode())
    finally:
        if code is not None:
            invocation_record.finish(started,code,str(log),materials,binaries,environment)
        else:
            # No native exit exists. Preserve that fact rather than assigning
            # a shell-style 127 to a program which never ran.
            value=json.loads(started.read_text())
            saved_spec=json.loads((started.parent/'.inputs'/started.name).read_text())
            try:
                end=invocation_record.snapshot(repo,saved_spec,environment)
            except (OSError,ValueError) as error:
                end={'error':str(error)}
            value.update(ended_at=invocation_record.now(),exit={'code':None,'signal_inferred':None},
                         execution={'started':False,'error':spawn_error},log=str(log),materials=materials,
                         binaries={'primary':None,'variants':[]},inputs_end=end,
                         inputs_stable=value['inputs_start']==end,
                         cleanup={'source':'none','refs':[],'summary':'ok'})
            write_record(started.with_name(started.name.replace('.started.json','.json')),value)
    return code if code is not None else 127


def confirmations(attempt):
    result=[]
    for path in sorted(Path(attempt).glob('cleanup-confirmation*.json')):
        value=json.loads(path.read_text())
        if value.get('schema')!=1 or value.get('attempt')!=Path(attempt).name:
            raise ValueError('cleanup confirmation belongs to another attempt or schema: '+str(path))
        result.extend(value.get('confirmations',[]))
    return result


def owned_attempt(root, declarations=(), current=False):
    """Read only native batch roots referenced by this attempt's own records."""
    root=Path(root).resolve()
    result=read_attempt(root,declarations,current=current)
    seen={(root/'rust/execution-index.json').resolve()}
    indices=set(root.glob('rust-heavy-*/execution-index.json'))
    if (root/'execution-index.json').is_file():indices.add(root/'execution-index.json')
    for path in sorted((root/'invocations').glob('*.json')):
        if path.name.endswith('.started.json'):
            continue
        record=json.loads(path.read_text())
        ref=record.get('materials',{}).get('execution_index')
        if ref:
            if isinstance(ref,dict):ref=ref['path']
            path_ref=Path(ref)
            indices.add(path_ref if path_ref.is_absolute() else path.parent/path_ref)
    for index in sorted(indices):
        index=index.resolve()
        if index in seen:
            continue
        if root.resolve() not in index.parents:
            result.errors.append('native batch reference is outside its owning attempt: '+str(index))
            continue
        seen.add(index)
        observations,errors=read_rust(index.parent,result.id,declarations)
        namespace=str(index.parent.relative_to(root)) if root in index.parents else str(index.parent)
        for observation in observations:
            observation.lifecycle=namespace+'/'+observation.lifecycle
        if index.parent.name == 'rust-heavy-java-rest':
            status_path = root / 'java-rest-inputs/owner-result.json'
            command_returned = None
            try:
                status = json.loads(status_path.read_text())
                summary = status.get('cleanup', {}).get('summary', 'unknown')
                command_returned = status.get('command_returned')
                if summary not in ('ok', 'violation') or (command_returned is not True and summary != 'violation'):
                    summary = 'unknown'
            except (OSError, ValueError):
                summary = 'unknown'
            for observation in observations:
                native_cleanup = observation.cleanup
                observation.detail['native_cleanup'] = native_cleanup
                observation.detail['java_rest_owner_cleanup'] = dict(
                    summary=summary, command_returned=command_returned)
                if 'violation' in (native_cleanup, summary):
                    observation.cleanup = 'violation'
                elif native_cleanup != 'ok' or summary != 'ok':
                    observation.cleanup = 'unknown'
                else:
                    observation.cleanup = 'ok'
                observation.detail['java_rest_owner_result'] = str(status_path)
        result.observations.extend(observations)
        result.errors.extend(errors)
    return result


def _is_attempt_root(path):
    path=Path(path)
    return path.is_dir() and ((path/'selection.json').is_file() or
        ((path/'identity/source-start.json').is_file() and ((path/'execution-index.json').is_file() or
         (path/'rust/execution-index.json').is_file() or (path/'invocations').is_dir())))


def compose_root(root,scope_values,target,layer='dev',current=None,extra_attempts=()):
    root=Path(root).resolve()
    root.mkdir(parents=True,exist_ok=True)
    target = _java_rest_target(root, scope_values, target)
    attempts=[];confirmed=[]
    candidates=([root] if _is_attempt_root(root) else [])
    candidates.extend(child for child in sorted(root.iterdir()) if _is_attempt_root(child))
    seen=set()
    for child in candidates:
        if child.resolve() in seen:continue
        seen.add(child.resolve())
        attempt=owned_attempt(child,scope_values,current=child.name==current)
        attempts.append(attempt);confirmed.extend(confirmations(child))
    attempts.extend(extra_attempts)
    disposition=Dispositions.read(Path(__file__).resolve().parents[1]/'baselines/dispositions.json')
    target=dict(target,current_attempt=current)
    value=compose(target,layer,attempts,scope_values,disposition,confirmed)
    value['evidence_class']='local-rehearsal' if layer=='admission' else 'local-development'
    if layer=='admission':
        rehearsal_scopes=copy.deepcopy(required_scopes('admission',scope_values))
        for scope in rehearsal_scopes:
            scope.selected=True
            scope.deferred=None
        rehearsal=compose(target,'dev',attempts,rehearsal_scopes,disposition,confirmed)
        value['local_rehearsal']={
            'membership':'all regular units before admission quarantine exclusions',
            'qualifies_for_admission':False,
            'layer_ok_semantics':'development composition of all regular units, including quarantined units',
            'admission_membership_exclusions':{str(unit):entry
                for scope in rehearsal_scopes for unit in scope.units
                for entry in [disposition.excluded(unit)] if entry},
            'layer_ok':rehearsal['layer_ok'],'scopes':rehearsal['scopes']}
    if layer == 'dev':
        # Read deferred-stage observations through the same evidence rules. They
        # do not create a second execution obligation or a synthetic stage PASS.
        deferred = [copy.deepcopy(scope) for scope in scope_values if scope.deferred and not scope.selected]
        for scope in deferred:
            scope.selected = True
        audit = compose(target, 'dev', attempts, deferred, disposition, confirmed)
        value['deferred_scopes'] = audit['scopes']
        if any(item['verdict'] in {'failed', 'inconsistent'} or item['cleanup_obligations']
               for item in audit['scopes']):
            value['layer_ok'] = False
    value['resume_exceptions'] = []
    for child in candidates:
        selection_file = child/'selection.json'
        if selection_file.is_file():
            exception = json.loads(selection_file.read_text()).get('resume_exception')
            if exception:
                value['resume_exceptions'].append({'attempt':child.name,
                    'selection':str(selection_file), 'exception':exception})
    value['unselected_obligations']=unselected_obligations(scope_values)
    output=exclusive(root/'compositions','composition')/'composition.json'
    write_record(output,value)
    return value,output


def _resource_checks(path, run_command):
    """A referenced registration is required evidence, including failed reads."""
    path=Path(path)
    try:
        value=json.loads(path.read_text())
    except (OSError,ValueError) as error:
        return [{'ref':str(path),'ok':False,'error':str(error)}]
    if (value.get('schema')!=1 or not all(isinstance(value.get(key),str) and value[key]
            for key in ('owner','id','label')) or value.get('state') not in {'registered','released','cleanup_failed'}):
        return [{'ref':str(path),'ok':False,'reason':'unsupported external resource registration'}]
    if value['state']=='released':
        return [{'ref':str(path),'ok':True,'state':'released','source':'resource-owner'}]
    label='label=com.docker.compose.project='+value['label']
    checks=[]
    for command in (['docker','ps','-aq','--filter',label],['docker','network','ls','-q','--filter',label],
                    ['docker','volume','ls','-q','--filter',label]):
        try:
            result=run_command(command,capture_output=True,text=True,check=False)
            checks.append({'ref':str(path),'argv':command,'exit':result.returncode,
                           'empty':not result.stdout.strip(),'ok':result.returncode==0 and not result.stdout.strip()})
        except (OSError,subprocess.SubprocessError) as error:
            checks.append({'ref':str(path),'argv':command,'ok':False,'error':str(error)})
    return checks


def _resolve_ref(base, ref):
    if isinstance(ref,dict):ref=ref['path']
    path=Path(ref)
    return path if path.is_absolute() else Path(base)/path


def _producer_cleanup_checks(observation, attempt, run_command, cache):
    invocation=observation.detail.get('invocation',{})
    if invocation.get('execution',{}).get('started') is False and invocation['execution'].get('error'):
        return [{'ref':observation.ref,'ok':True,'source':'executor','reason':'native process did not start'}]
    refs=list(invocation.get('cleanup',{}).get('refs',[]))
    materials=invocation.get('materials',{})
    checks=[]
    base=Path(observation.ref.split('#',1)[0]).parent
    def registration(ref):
        path=_resolve_ref(base,ref).resolve()
        if path not in cache:cache[path]=_resource_checks(path,run_command)
        return cache[path]
    def monitor(ref):
        path=_resolve_ref(base,ref)
        status=read_monitor_cleanup(path)
        return {'ref':str(path),'ok':status=='ok','status':status,'source':'monitor'}
    def resources(values):
        result=[]
        for value in values:
            ref=value.get('registration')
            if not ref:
                result.append({'ok':False,'reason':'resource registration reference is missing'})
            else:
                result.extend(registration(ref))
        return result
    if observation.unit.kind=='system-scenario':
        ref=materials.get('teardown')
        if not ref:
            return [{'ref':observation.ref,'ok':False,'reason':'System teardown reference is missing'}]
        path=_resolve_ref(base,ref)
        try:
            value=json.loads(path.read_text())
            if value.get('schema')!=1:raise ValueError('unsupported System teardown schema')
            fixture,cluster=value['fixture'],value['cluster']
            resource_values=fixture.get('resources',[])
            resource_checks=resources(resource_values)
            checks.extend(resource_checks)
            fixture_status=fixture['teardown']['status']
            checks.append({'ref':str(path),'domain':'fixture','ok':fixture_status=='ok' or
                           (fixture_status=='failed' and bool(resource_values) and all(c['ok'] for c in resource_checks))})
            if cluster.get('partial_cleanup')=='monitor' or cluster['shutdown']=='failed':
                monitors=materials.get('monitors',[])
                checks.extend(monitor(ref) for ref in monitors)
                if not monitors:checks.append({'ref':str(path),'domain':'cluster','ok':False,'reason':'exact monitor references are missing'})
            else:
                checks.append({'ref':str(path),'domain':'cluster','ok':cluster['shutdown'] in {'ok','not_applicable'}})
            producer_refs={str(_resolve_ref(base,ref))}
            producer_refs.update(str(_resolve_ref(base,r['registration'])) for r in resource_values if r.get('registration'))
            producer_refs.update(str(_resolve_ref(base,r)) for r in materials.get('monitors',[]))
            for ref in refs:
                path_ref=_resolve_ref(base,ref)
                if str(path_ref) not in producer_refs:
                    checks.append({'ref':str(path_ref),'ok':False,'reason':'unclassified required cleanup reference'})
        except (OSError,ValueError,KeyError,TypeError) as error:
            checks.append({'ref':str(path),'ok':False,'error':str(error)})
        return checks
    if observation.unit.kind=='sql-case':
        path=Path(observation.ref.split('#',1)[0])
        try:
            raw=path.read_bytes()
            if not raw.endswith(b'\n'):raise ValueError('unfinished SQL result record')
            records=[json.loads(line) for line in raw.splitlines()]
            suite_name,case_name=observation.unit.id.split('/',1)
            if observation.ref.partition('#')[2]!=observation.unit.id:
                raise ValueError('SQL cleanup reference does not identify the observed case')
            cases=[value for value in records if value.get('kind')=='case'
                   and value.get('suite')==suite_name and value.get('case')==case_name
                   and value.get('lifecycle_id')==observation.lifecycle]
            closures=[value for value in records if value.get('kind')=='suite'
                      and value.get('suite')==suite_name
                      and value.get('lifecycle_id')==observation.lifecycle]
            if len(cases)!=1 or len(closures)!=1:
                checks.append({'ref':observation.ref,'domain':'case/suite','ok':False,
                               'reason':'exact SQL case and suite cleanup lifecycle records are missing or ambiguous'})
            else:
                if _sql_case_cleanup_failed(cases[0].get('failure')):
                    checks.append({'ref':observation.ref,'domain':'case','ok':False,
                                   'reason':'case cleanup failed; no exact independent case cleanup confirmation is available'})
                suite_status=closures[0].get('cleanup',{}).get('status')
                if suite_status!='ok':
                    checks.append({'ref':observation.ref,'domain':'suite','ok':False,
                                   'reason':'suite cleanup failed or is unknown; no exact independent suite cleanup confirmation is available'})
            fixtures=[value for value in records if value.get('kind')=='fixture']
            servers=[value for value in records if value.get('kind')=='server']
            if not fixtures or (any(value.get('kind')=='selection' for value in records) and not servers):
                checks.append({'ref':str(path),'ok':False,'reason':'SQL cleanup producer records are missing'})
            for fixture in fixtures:
                resource_values=fixture.get('resources',[])
                resource_checks=resources(resource_values);checks.extend(resource_checks)
                checks.append({'ref':str(path),'domain':'fixture','ok':fixture['shutdown']['status']!='failed' or
                               (bool(resource_values) and all(c['ok'] for c in resource_checks))})
            for server in servers:
                cleanup=server['cleanup'];status=cleanup['status']
                if status!='failed':
                    checks.append({'ref':str(path),'domain':'server','ok':status in {'ok','not_launched','external'}})
                elif cleanup.get('residual_pids'):
                    for pid in cleanup['residual_pids']:
                        try:
                            os.kill(int(pid),0)
                            absent=False
                        except ProcessLookupError:absent=True
                        except (PermissionError,ValueError):absent=False
                        checks.append({'ref':str(path),'domain':'server','pid':pid,'ok':absent})
                else:
                    checks.append({'ref':str(path),'domain':'server','ok':False,'reason':'failed Server cleanup has no exact residual process facts'})
        except (OSError,ValueError,KeyError,TypeError) as error:
            checks.append({'ref':str(path),'ok':False,'error':str(error)})
        return checks
    if observation.unit.kind.startswith('rust-'):
        raw_path=Path(observation.ref.split('#',1)[0])
        root=raw_path.parent
        while root!=root.parent and not (root/'execution-index.json').is_file():root=root.parent
        try:
            index=json.loads((root/'execution-index.json').read_text())
            key='doctest_cleanup' if observation.unit.kind=='rust-doctest' else 'rust_cleanup'
            return [monitor(_resolve_ref(root,index['materials'][key]))]
        except (OSError,ValueError,KeyError) as error:
            return [{'ref':str(raw_path),'ok':False,'error':str(error)}]
    for ref in refs:
        path=_resolve_ref(base,ref)
        try:value=json.loads(path.read_text())
        except (OSError,ValueError):value=None
        if isinstance(value,dict) and all(key in value for key in ('owner','id','label')):
            checks.extend(registration(ref))
            continue
        status=read_monitor_cleanup(path)
        if status=='ok':checks.append({'ref':str(path),'ok':True,'source':'monitor'})
        else:checks.append({'ref':str(path),'ok':False,'status':status,'reason':'required cleanup reference is missing or unconfirmed'})
    return checks or [{'ref':observation.ref,'ok':False,'reason':'no exact cleanup facts are available'}]


def cleanup_confirmation(attempt,observation=None,run_command=subprocess.run):
    """Only observe exact resources named by producer materials; never clean them."""
    attempt=Path(attempt).resolve()
    if not _is_attempt_root(attempt):
        raise ValueError('cleanup confirmation requires an existing verification attempt')
    raw=owned_attempt(attempt)
    if not raw.observations:
        raise ValueError('cleanup confirmation requires recorded native observations')
    confirmations_out=[];cache={}
    for obs in raw.observations:
        if obs.cleanup=='ok':continue
        checks=_producer_cleanup_checks(obs,attempt,run_command,cache)
        complete=bool(checks) and all(item['ok'] for item in checks)
        confirmations_out.append({'schema':1,'attempt':attempt.name,'scope':obs.scope,'lifecycle':obs.lifecycle,'observation':obs.ref,
            'complete':complete,'summary':'ok' if complete else 'unknown','checks':checks})
    value={'schema':1,'attempt':attempt.name,'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'confirmations':confirmations_out}
    if observation is not None:
        value['manual_observation']={'path':str(Path(observation).resolve()),'sha256':identity_snapshot.digest(observation),'content':Path(observation).read_text()}
    path=attempt/'cleanup-confirmation.json'
    if path.exists():path=attempt/('cleanup-confirmation-'+secrets.token_hex(6)+'.json')
    write_record(path,value)
    return value,path


def scope_dict(scope):
    value=asdict(scope)
    value['units']=[str(unit) for unit in scope.units]
    return value


def pending_scopes(composition):
    return {item['key'] for item in composition['scopes'] if item['verdict'] not in {'satisfied','known_failure','excluded'} or item['cleanup_obligations']}


def unselected_obligations(scope_values):
    return [{'key':scope.key,'reason':(scope.deferred or {}).get('reason','outside this development impact selection'),
             'backstop':(scope.deferred or {}).get('backstop','full verification' if scope.tier=='heavy' else 'admission verification'),
             'suspended':scope.suspended} for scope in scope_values if not scope.selected]


def requirement_checks(requirements, environment, native, run_command=subprocess.run):
    """Observe declared prerequisites without provisioning external resources.

    Configuration checks only establish that the fixture was supplied. The
    native owner retains live readiness, exact input validation and cleanup.
    """
    checks=[]
    configured={
        'fixture:iceberg-rest':('NOVAROCKS_ICEBERG_REST_URI','NOVA_ENV_REST_ENV_FILE'),
        'fixture:minio':('AWS_S3_ENDPOINT','AWS_S3_ACCESS_KEY_ID','AWS_S3_SECRET_ACCESS_KEY'),
        'fixture:iceberg-rest-publication':('UEA7_TEST_CONTROL_URI',),
        'fixture:aliyun-ecs-oss':('NOVA_OSS_TEST_CONFIG',),
        'dataset:iceberg-java-oracle':('NOVAROCKS_UEA4G_ORACLE_RECEIPT','NOVAROCKS_UEA4G_ORACLE_OUTPUT'),
    }
    for requirement in requirements:
        check={'requirement':requirement,'ok':False}
        if requirement=='binary:novarocks':
            binding=native['receipt']['novarocks']
            try:check['ok']=identity_snapshot.digest(binding['path'])==binding['sha256']
            except OSError as error:check['error']=str(error)
        elif requirement in {'toolchain:java','fixture:docker'}:
            argv=['java','-version'] if requirement=='toolchain:java' else ['docker','info','--format','{{.ServerVersion}}']
            check['argv']=argv
            try:
                result=run_command(argv,env=environment,capture_output=True,text=True,check=False,timeout=15)
                check.update(exit=result.returncode,ok=result.returncode==0)
            except (OSError,subprocess.SubprocessError) as error:check['error']=str(error)
        elif requirement in configured:
            names=configured[requirement]
            missing=[name for name in names if not environment.get(name)]
            for name in names:
                if name.endswith(('_ENV_FILE','_RECEIPT')) and environment.get(name) and not Path(environment[name]).is_file():
                    missing.append(name)
                if name == 'NOVA_OSS_TEST_CONFIG' and environment.get(name):
                    path=Path(environment[name])
                    if not path.is_absolute() or not path.is_file():
                        missing.append(name)
            check.update(ok=not missing,missing=sorted(set(missing)),basis='declared effective configuration; native owner validates runtime facts')
        else:
            check['reason']='no native availability contract for this requirement'
        checks.append(check)
    return checks


def _rust_binding(scope, bindings, listing=None):
    native_id=scope.units[0].id
    candidates=[]
    for binary in bindings:
        if not native_id.startswith(binary+'::'):
            continue
        name=native_id[len(binary)+2:]
        if listing is None or name in listing['rust-suites'].get(binary,{}).get('testcases',{}):
            candidates.append((binary,name))
    if len(candidates)!=1:
        raise ValueError('native Rust unit has no unique binary/test binding: '+str(scope.units[0]))
    return candidates[0]


def _rust_expression(scope, bindings, metadata, listing=None):
    if len(scope.units) > 1:
        return ' | '.join(_rust_expression(Scope(str(unit), (unit,)), bindings, metadata, listing)
                          for unit in scope.units)
    binary,name=_rust_binding(scope,bindings,listing)
    binding=bindings[binary]
    package=metadata[binding['package-id']]['name']
    for other,value in bindings.items():
        if (other!=binary and value['package-id']==binding['package-id']
                and value['binary-name']==binding['binary-name']
                and (listing is None or name in listing['rust-suites'].get(other,{}).get('testcases',{}))):
            raise ValueError('native package/binary/test selector is not unique: '+str(scope.units[0]))
    return '(package(={}) & binary(={}) & test(={}))'.format(package,binding['binary-name'],name)


def _regular_expression(scopes, bindings, metadata, inputs, listing):
    """Compress native binary sets, then verify exact native membership."""
    wanted = {str(unit) for scope in scopes for unit in scope.units}
    groups = {}
    for scope in scopes:
        binary,name = _rust_binding(scope, bindings, listing)
        groups.setdefault(binary, []).append(name)
    expressions = []
    for binary, names in sorted(groups.items()):
        binding = bindings[binary]
        package = metadata[binding['package-id']]['name']
        prefix = '(package(={}) & binary(={})'.format(package, binding['binary-name'])
        all_regular = {name for name, case in listing['rust-suites'][binary]['testcases'].items()
                       if not case.get('ignored')}
        if set(names) == all_regular:
            expressions.append(prefix+')')
        else:
            included = prefix+' & ('+' | '.join('test(={})'.format(name) for name in sorted(names))+'))'
            excluded = prefix+' & not ('+' | '.join('test(={})'.format(name) for name in sorted(all_regular-set(names)))+'))'
            # Resume may omit only a few already satisfied tests from a large
            # binary. Keep the shorter exact set expression below argv limits.
            expressions.append(min((included, excluded), key=lambda value: len(value.encode())))
    expression = ' | '.join(expressions)
    verified = native_list(inputs, expression)
    actual = {'rust-test:'+binary+'::'+name
              for binary, suite in verified['rust-suites'].items()
              for name, case in suite['testcases'].items()
              if not case.get('ignored') and case.get('filter-match', {}).get('status') == 'matches'}
    if actual != wanted:
        raise ValueError('compressed native filter changes requested regular scope membership')
    return expression, sorted(actual)


def _rust_batch(repo, attempt, output, expression, environment, ignored=False, input_owner=None):
    argv=[sys.executable,str(repo/'tools/ci/test-run.py'),'batch','--output',str(output),'--','-E',expression]
    if ignored:argv+=['--run-ignored','only']
    if input_owner is not None:argv+=['--test-threads','1']
    command=attempt/(output.name+'.command.json')
    write_record(command,{'argv':argv,'cwd':str(repo)})
    with (attempt/(output.name+'.log')).open('xb') as log:
        try:
            if input_owner is None:
                result=subprocess.run(argv,cwd=repo,env=environment,stdout=log,stderr=subprocess.STDOUT)
                code=result.returncode
            else:
                code=input_owner.run(argv, stdout=log, stderr=subprocess.STDOUT)
        except OSError as error:
            code=None
            log.write(('executor did not start: '+str(error)+'\n').encode())
    write_record(attempt/(output.name+'.exit.json'),{'code':code})
    return code


def _java_rest_batch(repo, root, attempt, native, scope, bindings, metadata, env):
    owner_module = java_rest_owner(repo)
    listing = native.get('regular_inventory')
    names = [_rust_binding(Scope(str(unit), (unit,)), bindings, listing)[1] for unit in scope.units]
    owner_module.validate_group(names)
    expression = _rust_expression(scope, bindings, metadata, listing)
    # Native nextest, rather than a Python selector parser, proves whole-group
    # membership before admission starts the shared run's original deadline.
    selected = native_list(native['inputs'], expression, 'only')
    actual = {'rust-test:' + binary + '::' + name
              for binary, suite in selected['rust-suites'].items()
              for name, case in suite['testcases'].items()
              if case.get('filter-match', {}).get('status') == 'matches'}
    if actual != {str(unit) for unit in scope.units}:
        raise ValueError('Java REST native selection differs from its complete owner group')
    with owner_module.own_inputs(repo, attempt / 'java-rest-inputs', env) as owner:
        declarations = [native['ignored_units'][str(unit)] for unit in scope.units]
        requirements = declarations[0]['requires']
        if any(value['requires'] != requirements or value.get('input-owner') != owner_module.OWNER
               for value in declarations):
            raise ValueError('Java REST owner group declarations differ')
        checks = requirement_checks(requirements, owner.environment, native)
        write_record(attempt / 'java-rest-prerequisites.json', dict(schema=1, checks=checks))
        if not checks or not all(check['ok'] for check in checks):
            raise ValueError('Java REST original owner prerequisites are unavailable')
        snapshots = identity_snapshot.rust_snapshot(repo, native['identity_inputs'], owner.environment, 'nextest')
        payloads = [snapshots[unit.id]['inputs'] for unit in scope.units]
        if any(value != payloads[0] for value in payloads):
            raise ValueError('Java REST owner inputs do not bind one complete native group')
        declaration = dict(schema=1, phase='before-execution', scope=scope.key,
                           units=[str(unit) for unit in scope.units],
                           static_identity=owner_module.static_identity(payloads[0]), identity=payloads[0],
                           owner_result=str(owner.root / 'owner-result.json'), files=owner.input_files)
        path = attempt / 'java-rest-target.json'
        write_record(path, declaration)
        # A declared intended target, written before executing nextest. Compose
        # never learns target identity from test outcomes or generated receipts.
        identity_snapshot.atomic_json(root / 'java-rest-target.json',
                                      dict(path=str(path), sha256=identity_snapshot.digest(path)))
        scope.identity = payloads[0]
        output = attempt / 'rust-heavy-java-rest'
        code = _rust_batch(repo, attempt, output, expression,
                           owner.environment, ignored=True, input_owner=owner)
        if code == 0:
            observations, errors = read_rust(output, attempt.name, (scope,))
            if (errors or {item.unit for item in observations if item.unit.kind == 'rust-test'} != set(scope.units)
                    or any(item.result != 'pass' or not item.record_complete or not item.terminated
                           or not item.identity_confirmed or item.identity != scope.identity or item.cleanup != 'ok'
                           for item in observations if item.unit.kind == 'rust-test')):
                raise ValueError('Java REST complete native result/cleanup collection is missing')
        return code


def _java_rest_target(root, scopes, target):
    pointer = Path(root) / 'java-rest-target.json'
    owned = [scope for scope in scopes if scope.key == 'rust-owner:iceberg-java-rest-interop']
    if not owned or not pointer.is_file():
        return target
    ref = json.loads(pointer.read_text())
    path = Path(ref['path']).resolve()
    if Path(root).resolve() not in path.parents or identity_snapshot.digest(path) != ref['sha256']:
        raise ValueError('Java REST pre-execution target reference differs')
    saved = json.loads(path.read_text())
    scope = owned[0]
    module = java_rest_owner(Path(__file__).resolve().parents[3])
    if (saved.get('schema') != 1 or saved.get('phase') != 'before-execution'
            or saved.get('scope') != scope.key or saved.get('units') != [str(unit) for unit in scope.units]):
        raise ValueError('Java REST target declaration differs from its complete group')
    if scope.identity is None or saved['static_identity'] != module.static_identity(scope.identity):
        return target
    try:
        if any(identity_snapshot.digest(path) != digest for path, digest in saved['files'].items()):
            return target
    except OSError:
        return target
    result = dict(target, identities=dict(target.get('identities', {})))
    result['identities'][scope.key] = saved['identity']
    return result


def execute(repo,root,native,chosen,scope_values,pending=None):
    repo=Path(repo).resolve();root=Path(root).resolve()
    env=rust_environment(repo,native)
    selected=[scope for scope in scope_values if scope.selected and not scope.suspended and not scope.deferred
              and (pending is None or scope.key in pending)]
    bindings=json.loads((Path(native['inputs'])/'test-artifacts.json').read_text())['binaries']
    metadata={package['id']:package for package in native['metadata']['packages']}
    exception=None
    candidate_refusals=[]
    # An explicit resume plus a fresh composition is required. A caller-provided
    # pending set alone cannot establish that another scope was satisfied.
    if pending is not None and {scope.key for scope in selected} == {'rust-doctest:workspace'}:
        prior,prior_path=compose_root(root,scope_values,{'identities':{scope.key:scope.identity for scope in scope_values}})
        if pending_scopes(prior) == {'rust-doctest:workspace'}:
            satisfied={item['key']:item for item in prior['scopes'] if item['verdict']=='satisfied'
                       and item['accepted_execution'] and not item['cleanup_obligations']}
            candidates=sorted((scope for scope in scope_values if scope.selected and not scope.suspended
                               and scope.tier=='regular' and scope.units[0].kind=='rust-test'
                               and scope.key in satisfied),key=lambda scope:scope.key)
            for repeated in candidates:
                try:
                    expression=_rust_expression(repeated,bindings,metadata,native.get('regular_inventory'))
                except ValueError as error:
                    candidate_refusals.append({'scope':repeated.key,'error':str(error)})
                    continue
                selected.append(repeated)
                exception={'kind':'doctest-only-native-batch',
                    'reason':'only workspace doctest remains; native G6a batch requires nonempty nextest',
                    'pending_scopes':['rust-doctest:workspace'],'repeated_satisfied_scopes':[repeated.key],
                    'actual_filterset':expression,
                    'prior_composition':str(prior_path),
                    'accepted_execution':satisfied[repeated.key]['accepted_execution']}
                break
    attempt=exclusive(root)
    if exception:
        exception['batch_output']=str(attempt/'rust')
    selected_keys={scope.key for scope in selected}
    write_record(attempt/'selection.json',{'schema':1,'selection':chosen,
        'scopes':[scope_dict(scope) for scope in scope_values],
        'executed_scope_keys':sorted(selected_keys),'unselected_obligations':unselected_obligations(scope_values),
        'resume_exception':exception,'resume_candidate_refusals':candidate_refusals})
    errors=[]
    source_start=None
    try:
        source_start=identity_snapshot.source_snapshot(repo)
        identity_snapshot.atomic_json(attempt/'identity/source-start.json',source_start)
    except (OSError,subprocess.SubprocessError) as error:
        errors.append({'kind':'source-start','error':str(error)})
    rust=[scope for scope in selected if scope.units[0].kind=='rust-test']
    regular=[scope for scope in rust if scope.tier=='regular']
    heavy=[scope for scope in rust if scope.tier=='heavy']
    doc=any(scope.units[0].kind=='rust-doctest' for scope in selected)
    workspace=next((item for item in chosen['stages'] if item['name']=='workspace nextest and doctests'
                    and 'stage:'+item['name'] in selected_keys),None)
    batch_attempted=False
    system_ready=True
    def record_error(kind,name,code=None,error=None):
        value={'kind':kind,'name':name}
        if code is not None:value['exit']=code
        if error is not None:value['error']=str(error)
        errors.append(value)
    def run_stage(item):
        mode='loom' if item['name']=='memory-model:loom' else 'miri' if item['name']=='memory-model:miri' else 'default'
        unit=UnitKey('stage',item['name'],mode)
        output=attempt/'stage-runtime'/hashlib.sha256(item['name'].encode()).hexdigest()[:12]
        output.mkdir(parents=True,exist_ok=False)
        spec=stage_spec(repo,native,item,output)
        materials={}
        if item['name']=='workspace nextest and doctests':
            materials={'execution_index':str(output/'rust-stage/execution-index.json'),
                       'execution_identity':[str(output/'rust-stage/identity/nextest.json'),str(output/'rust-stage/identity/doctest.json')]}
        code=invoke(attempt,repo,'stage',item['name'],[unit],stage_argv(repo,native,item,output),spec,materials,env)
        if code:record_error('stage',item['name'],code=code)
        if item['name']=='native-compatibility' and code is not None:
            # Its native owner restores main using a stage-local receipt. Renew
            # the development owner's receipt through that same build producer;
            # frozen scopes and all previous observations remain unchanged.
            try:
                refresh_development_programs(repo,native,output)
            except (OSError,ValueError,subprocess.SubprocessError) as error:
                record_error('artifact-preparation','development programs after native-compatibility',error=error)
            else:
                env.update(rust_environment(repo,native))
    try:
        system_items=[item for item in chosen['system_scenarios'] if 'system-scenario:'+item['name'] in selected_keys]
        if system_items and not native.get('system_variants_prepared'):
            try:
                prepare_system_variants(repo,native,dict(chosen,system_scenarios=system_items),root)
            except (OSError,ValueError,subprocess.SubprocessError) as error:
                system_ready=False
                record_error('system-preparation','selected native programs',error=error)
        if workspace:
            # This is an actual full-workspace command. A selected rdeps batch
            # can never stand in for the native orchestration stage.
            try:run_stage(workspace)
            except (OSError,ValueError,subprocess.SubprocessError) as error:record_error('stage',workspace['name'],error=error)
            batch_attempted=True
        elif regular:
            batch_attempted=True
            try:
                if not exception and native.get('regular_inventory') is not None:
                    expression,verified=_regular_expression(regular,bindings,metadata,Path(native['inputs']),native['regular_inventory'])
                    write_record(attempt/'regular-filter.json',{'schema':1,'actual_filterset':expression,
                        'requested_scopes':sorted(scope.key for scope in regular),'native_selected_scopes':verified,
                        'membership_equal':True,'filter_utf8_bytes':len(expression.encode())})
                else:
                    expression=' | '.join(_rust_expression(scope,bindings,metadata,native.get('regular_inventory')) for scope in regular)
                code=_rust_batch(repo,attempt,attempt/'rust',expression,env)
                if code!=0:record_error('rust-batch','regular native tests',code=code,error='executor did not start' if code is None else None)
            except (OSError,ValueError,subprocess.SubprocessError) as error:
                record_error('rust-batch','regular native tests',error=error)
        groups={}
        for scope in heavy:
            if scope.key == 'rust-owner:iceberg-java-rest-interop':
                try:
                    code = _java_rest_batch(repo, root, attempt, native, scope, bindings, metadata, env)
                    batch_attempted = True
                    if code != 0:
                        record_error('rust-heavy', scope.key, code=code)
                except (OSError, ValueError, subprocess.SubprocessError,
                        java_rest_owner(repo).fixture_run.runtime.RuntimeFailure) as error:
                    record_error('rust-heavy', scope.key, error=error)
                continue
            declaration=native.get('ignored_units',{}).get(str(scope.units[0]))
            if not declaration or declaration.get('class')!='heavy':
                record_error('rust-heavy',str(scope.units[0]),error='native heavy declaration is unavailable')
                continue
            requirements=declaration.get('requires',[])
            checks=requirement_checks(requirements,env,native)
            checked={'schema':1,'unit':str(scope.units[0]),'requires':requirements,'checks':checks,
                     'ok':bool(checks) and all(check['ok'] for check in checks)}
            path=attempt/'prerequisites'/(hashlib.sha256(str(scope.units[0]).encode()).hexdigest()[:16]+'.json')
            write_record(path,checked)
            if not checked['ok']:
                record_error('rust-heavy',str(scope.units[0]),error='declared prerequisite is unavailable or unconfirmed; see '+str(path))
                continue
            groups.setdefault(tuple(sorted(requirements)),[]).append(scope)
        for index,group in enumerate(groups.values(),1):
            output=attempt/('rust-heavy-%04d'%index)
            try:
                expression=' | '.join(_rust_expression(scope,bindings,metadata,native.get('regular_inventory')) for scope in group)
                code=_rust_batch(repo,attempt,output,expression,env,ignored=True)
                batch_attempted=True
                if code!=0:record_error('rust-heavy',output.name,code=code,error='executor did not start' if code is None else None)
            except (OSError,ValueError,subprocess.SubprocessError) as error:
                record_error('rust-heavy',output.name,error=error)
        if doc and not batch_attempted:
            reason='doctest-only resume has no eligible satisfied regular nextest scope, or other target obligations remain'
            write_record(attempt/'doctest-only-gap.json',{'schema':1,'scope':'rust-doctest:workspace',
                'status':'not_run','reason':reason})
            record_error('rust-doctest','workspace',error=reason)
        for declaration in chosen['sql_suites']:
            name=declaration['name']
            group=[scope for scope in selected if scope.units[0].kind=='sql-case' and scope.units[0].id.startswith(name+'/')]
            if not group:continue
            try:
                cases=None
                if declaration.get('reuse',{}).get('scope')=='case':cases=[scope.units[0].id.split('/',1)[1] for scope in group]
                output=attempt/'sql'/(name+'.results.jsonl');output.parent.mkdir(exist_ok=True)
                config=env.get('NOVAROCKS_SQL_TEST_CONFIG')
                if not config:
                    record_error('sql-suite',name,error='NOVAROCKS_SQL_TEST_CONFIG is required');continue
                argv=[native['receipt']['novarocks-sql-test']['path'],'--config',config,'--suite',name,'--mode','verify','--results-file',str(output),'--cluster-mode','cross-process','--cluster-size','3','-j','1']+sql_case_arguments(declaration,cases)
                if declaration.get('prepared_dataset') is not None:
                    argv+=['--cluster-launch-profile','performance']
                units=[unit for scope in group for unit in scope.units]
                spec={'inputs':[],'files':{'runner':argv[0],'server':env['NOVAROCKS_BIN'],'config':config},'env_inputs':declaration.get('reuse',{}).get('env_inputs') or [],'declared':declaration.get('reuse',{}).get('env_inputs') is not None}
                code=invoke(attempt,repo,'sql-suite',name,units,argv,spec,{'sql_results':str(output)},env)
                if code:record_error('sql-suite',name,code=code)
            except (OSError,ValueError,subprocess.SubprocessError) as error:record_error('sql-suite',name,error=error)
        if system_ready:
            for item in system_items:
                try:
                    output=attempt/'system'
                    argv=system_argv(native,item,output)
                    scene=output/item['name'].replace('/','-')
                    code=invoke(attempt,repo,'system-scenario',item['name'],[UnitKey('system-scenario',item['name'])],argv,system_spec(native,item),
                        {'evidence':str(scene/'scenario-evidence.json'),'teardown':str(scene/'scenario-teardown.json')},env)
                    if code:record_error('system-scenario',item['name'],code=code)
                except (OSError,ValueError,subprocess.SubprocessError) as error:record_error('system-scenario',item['name'],error=error)
        for item in chosen['stages']:
            if item is workspace or item['name']=='workspace nextest and doctests':continue
            mode='loom' if item['name']=='memory-model:loom' else 'miri' if item['name']=='memory-model:miri' else 'default'
            if str(UnitKey('stage',item['name'],mode)) not in selected_keys:continue
            try:run_stage(item)
            except (OSError,ValueError,subprocess.SubprocessError) as error:record_error('stage',item['name'],error=error)
    finally:
        try:identity_snapshot.atomic_json(attempt/'identity/source-end.json',identity_snapshot.source_snapshot(repo))
        except (OSError,subprocess.SubprocessError) as error:record_error('source-end','attempt source',error=error)
        write_record(attempt/'execution.json',{'schema':1,'errors':errors})
    current=owned_attempt(attempt,scope_values,current=True)
    identities={scope.key:scope.identity for scope in scope_values}
    for obs in current.observations:
        if obs.unit.kind=='sql-case' and obs.identity is not None:identities[obs.scope]=obs.identity
    result,path=compose_root(root,scope_values,{'identities':identities},current=attempt.name)
    result['execution_errors']=errors
    write_record(attempt/'result.json',{'schema':1,'composition':str(path),'execution_errors':errors})
    return result,path,attempt
