#!/usr/bin/env bash
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

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_dir=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$fixture_dir/../.." && pwd)

# Resolve once. The caller owns fixture startup; this driver never changes bindings or inputs.
publication=$(python3 - "$fixture_dir" <<'PY'
import json
from pathlib import Path
import sys

try:
    publication = (Path(sys.argv[1]) / 'runtime/current/published').resolve(strict=True)
    manifest = json.loads((publication / 'manifest.json').read_text())
    if not manifest.get('ready') or not manifest.get('fixture_inputs', {}).get('verified'):
        raise ValueError('publication is not ready with verified inputs')
    if not (publication / 'env.sh').is_file():
        raise ValueError('publication has no immutable environment')
except (OSError, ValueError) as error:
    print(f'BLOCKED: pinned fixture publication required: {error}', file=sys.stderr)
    sys.exit(75)
print(publication)
PY
)
source "$publication/env.sh"
if [[ "$NOVA_ENV_REST_ENV_FILE" != "$publication/env.sh" ]]; then
    echo "FAIL: sourced environment differs from pinned publication" >&2
    exit 1
fi

input_store=$(python3 - "$publication/manifest.json" <<'PY'
import json
from pathlib import Path
import sys
manifest = json.loads(Path(sys.argv[1]).read_text())
print(Path(manifest['fixture_inputs']['bom']).parent)
PY
)
# Read-only verification; missing pinned inputs stay BLOCKED. No provision/download/READY rewrite.
"$repo_root/docker/fixture-inputs/verify.sh" --store "$input_store" --repo-root "$repo_root" --consumer iceberg-rest
python3 - "$publication/manifest.json" "$input_store/bom.json" <<'PY'
import json
from pathlib import Path
import sys
manifest = json.loads(Path(sys.argv[1]).read_text())
bom = json.loads(Path(sys.argv[2]).read_text())
if manifest['fixture_inputs']['lock_sha256'] != bom['lock_sha256']:
    print('BLOCKED: pinned publication differs from verified fixture input lock', file=sys.stderr)
    sys.exit(75)
PY

: "${NOVAROCKS_ICEBERG_REST_URI:?pinned REST URI required}"
: "${NOVAROCKS_ICEBERG_REST_WAREHOUSE:?pinned REST warehouse required}"
: "${AWS_S3_ENDPOINT:?pinned S3 endpoint required}"
: "${AWS_S3_ACCESS_KEY_ID:?pinned S3 access key required}"
: "${AWS_S3_SECRET_ACCESS_KEY:?pinned S3 secret required}"

run_id=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
export NR_IRU5_INTEROP_NAMESPACE="iru5_${run_id}"
export NR_IRU5_INTEROP_ARTIFACT_DIR="$NOVA_ENV_RUNTIME_DIR/iru5-java-rest-$run_id"
mkdir "$NR_IRU5_INTEROP_ARTIFACT_DIR"
artifact="$NR_IRU5_INTEROP_ARTIFACT_DIR/runner.log"
python3 - "$publication/manifest.json" "$NR_IRU5_INTEROP_ARTIFACT_DIR/fixture.json" "$publication" <<'PY'
import json
from pathlib import Path
import sys
manifest = json.loads(Path(sys.argv[1]).read_text())
facts = {
    'publication': sys.argv[3],
    'fixture_input_lock_sha256': manifest['fixture_inputs']['lock_sha256'],
    'boundary': 'Java REST metadata interoperability; native 1FE+3BE SQL acceptance is separate',
}
Path(sys.argv[2]).write_text(json.dumps(facts, indent=2) + '\n')
PY

cd "$repo_root"
cargo test --locked --offline --profile dev-opt -p novarocks-connector-iceberg \
    --lib iru5_java_rest_ -- --ignored --test-threads=1 --nocapture 2>&1 | tee "$artifact"

# An unwired ignored module would otherwise produce a misleading zero-test success.
python3 - "$artifact" <<'PY'
from pathlib import Path
import re
import sys
text = Path(sys.argv[1]).read_text()
expected = (
    'iru5_java_rest_three_shapes_historic_spec_and_composition',
    'iru5_java_rest_d12_inheritance_and_physical_rewrite',
    'iru5_java_rest_two_canonical_clients_real_cas',
    'iru5_java_rest_attempt_runner_reloads_and_reprepares_after_real_cas',
)
for name in expected:
    if not re.search(r'^test [^\n]*' + re.escape(name) + r' \.\.\. ok$', text, re.MULTILINE):
        raise SystemExit(f'FAIL: expected ignored interop test did not pass: {name}')
if not re.search(r'test result: ok\. 4 passed; 0 failed; 0 ignored;', text):
    raise SystemExit('FAIL: expected exactly four enabled interop tests')
PY
printf 'IRU-5 Java REST interoperability artifacts: %s\n' "$NR_IRU5_INTEROP_ARTIFACT_DIR"
