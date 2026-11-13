#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

java25_home=${JAVA25_HOME:-}
if [[ -z "$java25_home" || ! -x "$java25_home/bin/java" ]]; then
  echo "The declared Trino reference requires JDK 25; set JAVA25_HOME to a JDK 25 installation" >&2
  exit 1
fi
java_major=$("$java25_home/bin/java" -version 2>&1 | sed -n '1s/.*version "\([0-9]*\).*/\1/p')
test "$java_major" = 25 || {
  echo "JAVA25_HOME is not JDK 25: $java25_home" >&2
  exit 1
}

repo_root=$(cd "$(dirname "$0")/../../../.." && pwd)
trino_version=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["images"]["trino"]["version"])' \
  "$repo_root/docker/fixture-inputs/versions.json")
source_root="$repo_root/tests/datasketches-tck/interop/trino"
fixture_root="$repo_root/tests/datasketches-tck/fixtures"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-trino-theta-interop.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

mkdir -p "$work_dir/src/main/java/org/apache/novarocks/tck"
cp "$source_root/pom.xml" "$work_dir/pom.xml"
cp "$source_root/src/VerifyTrinoThetaInterop.java" \
  "$work_dir/src/main/java/org/apache/novarocks/tck/VerifyTrinoThetaInterop.java"

JAVA_HOME="$java25_home" PATH="$java25_home/bin:$PATH" \
  mvn -q -f "$work_dir/pom.xml" compile dependency:build-classpath \
  -Dtrino.version="$trino_version" -Dmdep.outputFile="$work_dir/classpath"

trino_output="$work_dir/trino_long_disjoint_n1000.sk"
"$java25_home/bin/java" -cp "$work_dir/target/classes:$(<"$work_dir/classpath")" \
  org.apache.novarocks.tck.VerifyTrinoThetaInterop \
  "$fixture_root/theta/rust_quickselect_n1000_ordered_v3.sk" \
  "$fixture_root/theta/rust_quickselect_n100000_ordered_v4.sk" \
  "$trino_output"

cargo run --quiet --locked -p novarocks-datasketches-tck \
  --bin verify_theta_interop -- \
  "$trino_output" 1000 \
  "$fixture_root/theta/rust_quickselect_n1000_ordered_v3.sk" 2000
