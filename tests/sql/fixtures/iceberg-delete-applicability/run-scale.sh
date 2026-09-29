#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.


set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
manifest=""
output=""
namespace=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest) manifest="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    --namespace) namespace="$2"; shift 2 ;;
    *) echo "usage: $0 --manifest FILE --output NEW_DIR [--namespace NAME]" >&2; exit 2 ;;
  esac
done
[[ -n "$manifest" && -n "$output" ]] || { echo "--manifest and --output are required" >&2; exit 2; }
[[ -f "$manifest" && ! -e "$output" ]] || { echo "Manifest must exist and output must be new" >&2; exit 2; }
namespace="${namespace:-uea4g_scale_$(python3 -c 'import uuid; print(uuid.uuid4().hex)')}"
[[ "$namespace" =~ ^[a-zA-Z0-9_]+$ ]] || { echo "Invalid namespace" >&2; exit 2; }
NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' \
  "${NOVA_ENV_REST_ENV_FILE:-$WORKSPACE_ROOT/docker/iceberg-rest/runtime/current/env.sh}")"
export NOVA_ENV_REST_ENV_FILE
[[ -f "$NOVA_ENV_REST_ENV_FILE" ]] || { echo "Missing resolved fixture environment" >&2; exit 1; }
mkdir -p "$output"
output="$(cd "$output" && pwd)"
python3 - "$SCRIPT_DIR" "$manifest" "$output" "$namespace" <<'PYTHON'
import hashlib,json,pathlib,sys
source,manifest,out,namespace=map(pathlib.Path,sys.argv[1:])
namespace=str(namespace)
config=json.loads(manifest.read_text())
assert config['status'].startswith('frozen'), 'Matrix must be confirmed and frozen first'
digest=hashlib.sha256(manifest.read_bytes()).hexdigest()
assert manifest.with_name('manifest.sha256').read_text().split()[0]==digest, 'Frozen manifest digest changed'
(out/'manifest.json').write_bytes(manifest.read_bytes())
files=[source/'generate.scala',source/'scale.scala']
(out/'source-sha256.json').write_text(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2)+'\n')
script='\n'.join(p.read_text() for p in files)+'\ntry {\n'
script+=f'  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, {json.dumps(namespace)}, "scale")\n'
script+=f'  DeleteApplicabilityScale.run(DeleteApplicabilityFixture.mapper.readTree({json.dumps(json.dumps(config,separators=(",",":")))}))\n'
script+='  println("UEA4G_SCALE_COMPLETE")\n} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }\n'
(out/'fixture.scala').write_text(script)
PYTHON
if ! "$WORKSPACE_ROOT/docker/iceberg-rest/spark-shell.sh" "$output/fixture.scala" >"$output/spark.log" 2>&1; then
  grep -v '^UEA4G_RECEIPT ' "$output/spark.log" | tail -100 >&2
  exit 1
fi
python3 - "$output" <<'PYTHON'
import collections,hashlib,json,pathlib,re,sys
root=pathlib.Path(sys.argv[1]);config=json.loads((root/'manifest.json').read_text())
text=re.sub(r'\x1b\[[0-9;]*m','',(root/'spark.log').read_text())
assert 'UEA4G_SCALE_COMPLETE' in text.splitlines(), 'Missing exact completion marker; Scala failed'
records=[json.loads(line.removeprefix('UEA4G_RECEIPT ')) for line in text.splitlines() if line.startswith('UEA4G_RECEIPT ')]
runtime=[r for r in records if r['record']=='runtime'];assert len(runtime)==1 and '1.11.0' in runtime[0]['iceberg']
cases=[r for r in records if r['record']=='scale-case'];expected={c['case']:c for c in config['cases']}
assert len(cases)==len(expected)==config['case_count'] and {c['case'] for c in cases}==set(expected)
plans=[r for r in records if r['record']=='scale-plan-file']
members=[r for r in records if r['record']=='scale-delete-member']
artifacts=[r for r in records if r['record']=='scale-artifact'];paths={r['path'] for r in artifacts}
assert len(paths)==len(artifacts)
for a in artifacts:
 assert a['size']>0 and re.fullmatch('[0-9a-f]{64}',a['sha256']) and 'base64' not in a
for c in cases:
 assert c['java_planFiles']==c['java_row_read']=='success' and c['exact_bag_checked']
 assert c['java_oracle']==c['independent_oracle'] and c['metadata'] in paths
 own=[r for r in plans if r['case']==c['case']];dictionary={r['member_id']:r for r in members if r['case']==c['case']}
 assert len(own)==expected[c['case']]['data_files']==c['plan_file_count']
 assert len({r['data_ordinal'] for r in own})==len(own)
 assert len(dictionary)==c['delete_dictionary_size']
 for p in own:
  assert p['data']['path'] in paths and len(p['delete_member_ids'])==p['delete_list_size']
  assert all(member in dictionary for member in p['delete_member_ids'])
 for m in dictionary.values(): assert m['content']['path'] in paths
complete=[r for r in records if r['record']=='scale-complete'];assert len(complete)==1 and complete[0]['case_count']==len(cases) and complete[0]['artifact_count']==len(artifacts)
for filename,items in [('java-plans.jsonl',plans),('java-delete-members.jsonl',members),('artifacts.jsonl',artifacts)]:
 (root/filename).write_text(''.join(json.dumps(r,separators=(',',':'))+'\n' for r in items))
receipt={'matrix_sha256':hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest(),'runtime':runtime[0],'case_count':len(cases),'cases':cases,'artifact_count':len(artifacts),'artifacts':'artifacts.jsonl','java_plans':'java-plans.jsonl','java_delete_members':'java-delete-members.jsonl','checkpoints':[r for r in records if r['record']=='scale-checkpoint'],'source_sha256':json.loads((root/'source-sha256.json').read_text()),'performance_measured':False}
(root/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
for c in cases: print(c['case'], 'files='+str(c['plan_file_count']), 'visible_rows='+str(c['java_oracle']['row_count']))
print('UEA4G_SCALE_FIXTURE_OK receipt='+str(root/'receipt.json'))
PYTHON
