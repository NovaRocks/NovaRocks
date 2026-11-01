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
# Run only on the disposable Ubuntu builder, as root. No cloud credentials are inputs.
source_root=$(realpath "${1:?source directory is required}")
preparation_sha=${2:?preparation SHA is required}
[[ "$preparation_sha" =~ ^[0-9a-f]{40}$ ]] || { echo 'Invalid preparation SHA' >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo 'Image preparation requires root' >&2; exit 2; }
if ! id runner >/dev/null 2>&1; then
  useradd --create-home --home-dir /var/lib/novarocks-ci/runner --shell /bin/bash runner
fi
# Seed the native owner's pinned cache from the operator's verified OSS copy.
# The installer still checks both archive and executable digests from its inputs.
if [ -n "${3:-}" ]; then
  nextest_cache=$(python3 - "$source_root" "$3" <<'PY_NEXTEST'
import hashlib,json,sys
from pathlib import Path
item=json.loads((Path(sys.argv[1])/'tools/dev/linux-env.inputs.json').read_text())['nextest']
if hashlib.file_digest(Path(sys.argv[2]).open('rb'),'sha256').hexdigest()!=item['sha256']:
    raise SystemExit('Staged nextest archive SHA-256 differs')
print('nextest-'+item['version']+'-'+item['sha256'])
PY_NEXTEST
)
  install -d -o runner -g runner -m 0755 /var/lib/novarocks-ci/runner/.cache/novarocks/linux-env
  install -o runner -g runner -m 0644 "$3" "/var/lib/novarocks-ci/runner/.cache/novarocks/linux-env/$nextest_cache"
fi
install -d -m 0755 /opt/novarocks-ci /opt/novarocks-ci/tools-source/tools
install -d -g runner -m 0750 /var/log/novarocks-ci
install -g runner -m 0640 /dev/null /var/log/novarocks-ci/bootstrap.log
cp -a "$source_root/tools/dev" /opt/novarocks-ci/tools-source/tools/
cp "$source_root/rust-toolchain.toml" /opt/novarocks-ci/tools-source/
"$source_root/tools/dev/linux-env.sh" prepare --user runner \
  --profile "$source_root/tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json" --tools-only
apt-get install --yes python3-venv iptables libicu-dev libkrb5-3 liblttng-ust-dev zlib1g
python3 -m venv /opt/novarocks-ci/venv
/opt/novarocks-ci/venv/bin/pip install --require-hashes -r "$source_root/tools/ci/aliyun_ci/requirements.lock"
cp -a "$source_root/tools/ci/aliyun_ci" /opt/novarocks-ci/
find /opt/novarocks-ci -type d -name __pycache__ -exec rm -rf {} +
install -m 0644 "$source_root"/tools/ci/aliyun_ci/instance/systemd/*.service /etc/systemd/system/
printf '%s\n' "$preparation_sha" > /opt/novarocks-ci/preparation-sha
chown -R root:root /opt/novarocks-ci
chmod -R go-w /opt/novarocks-ci
# Include the installed target dependency edges in Linux verification.
systemctl daemon-reload
systemctl enable novarocks-ci-bootstrap.service
# Validate the actual Linux service vocabulary and privilege boundary before capture.
systemd-analyze verify /etc/systemd/system/novarocks-ci-bootstrap.service \
  /etc/systemd/system/novarocks-local-rust.service \
  /etc/systemd/system/novarocks-runner.service \
  /etc/systemd/system/novarocks-spot-watcher.service
(
  cd /opt/novarocks-ci
  /opt/novarocks-ci/venv/bin/python -m unittest \
    aliyun_ci.tests.test_handoff.HandoffTests.test_root_packer_cannot_read_root_only_diagnostic
)
"$source_root/tools/ci/aliyun_ci/image/harden.sh"
/opt/novarocks-ci/tools-source/tools/dev/linux-env.sh check --user runner \
  --profile /opt/novarocks-ci/tools-source/tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json --tools-only \
  > /opt/novarocks-ci/tools-check.json
python3 - "$source_root" "$preparation_sha" <<'PY_FACTS'
import hashlib, json, os, platform, subprocess, sys
from pathlib import Path
sys.path.insert(0, '/opt/novarocks-ci')
from aliyun_ci.instance.local_rust import LOCAL_CARGO_REGISTRIES
from aliyun_ci.instance.swap import LOCAL_SWAP_GIB, SWAP_ROOT, kernel_snapshot
if SWAP_ROOT.exists() or SWAP_ROOT.is_symlink():
    raise SystemExit('Image capture refuses private worker swap storage')
builder_swap = kernel_snapshot()
print('BuilderSwapReadback: '+json.dumps(builder_swap,sort_keys=True),flush=True)
if builder_swap['swaps'] or builder_swap['meminfo']['SwapTotal_kib'] != 0:
    raise SystemExit('Image capture refuses active swap')
for executable in ('/usr/bin/dd', '/usr/bin/findmnt', '/usr/sbin/mkswap', '/usr/sbin/swapon'):
    if not Path(executable).is_file() or not os.access(executable, os.X_OK):
        raise SystemExit('Image swap executable is missing: '+executable)
root=Path(sys.argv[1])
digest=lambda path:hashlib.sha256((root/path).read_bytes()).hexdigest()
facts={'schema':1,'preparation_sha':sys.argv[2], 'os':'ubuntu-26.04','arch':'amd64',
       'local_rust_cargo_registries':list(LOCAL_CARGO_REGISTRIES),
       'local_rust_swap_gib':list(LOCAL_SWAP_GIB),
       'builder_swap':builder_swap,
       'tools_profile_digest':digest('tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json'),
       'inputs_digest':digest('tools/dev/linux-env.inputs.json'),
       'platform_lock_digest':digest('tools/ci/aliyun_ci/requirements.lock'),
       'observed':{'python':platform.python_version(),'kernel':platform.release(),
          'runner_native_packages':subprocess.check_output(['dpkg-query','-W','-f=${Package}=${Version}\\n',
            'libicu-dev','libkrb5-3','liblttng-ust-dev','zlib1g'],text=True)},
       'tools_check':json.loads(Path('/opt/novarocks-ci/tools-check.json').read_text())}
Path('/opt/novarocks-ci/preparation-facts.json').write_text(json.dumps(facts,sort_keys=True)+'\n')
PY_FACTS
# Remove build-only source and machine identity before the image is captured.
rm -rf -- "$source_root"
rm -rf /root/.cache/pip /root/.cargo /root/.rustup
cloud-init clean --logs --machine-id
rm -f /etc/ssh/ssh_host_*
