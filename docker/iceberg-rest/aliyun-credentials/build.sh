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
source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
artifacts="${1:?verified artifact directory is required}"
output="${2:?new output directory is required}"
mkdir "$output"
temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
classpath=""
while IFS= read -r name; do
  [[ -n "$name" ]] || continue
  test -f "$artifacts/$name"
  cp "$artifacts/$name" "$output/$name"
  classpath="${classpath:+$classpath:}$artifacts/$name"
done < "$source_dir/runtime-jars.txt"
for name in iceberg-aws-bundle-1.11.0.jar aws-java-sdk-bundle-1.12.262.jar; do
  test -f "$artifacts/$name"
  classpath="$classpath:$artifacts/$name"
done
find "$source_dir/src/main/java" -name '*.java' -print > "$temporary/main-sources"
mkdir "$temporary/classes"
javac --release 11 -cp "$classpath" -d "$temporary/classes" @"$temporary/main-sources"
jar --create --file "$output/novarocks-aliyun-credentials.jar" -C "$temporary/classes" .
if [[ "${3:-}" == --test ]]; then
  find "$source_dir/src/test/java" -name '*.java' -print > "$temporary/test-sources"
  javac --release 11 -cp "$classpath:$temporary/classes" -d "$temporary/classes" @"$temporary/test-sources"
  java -cp "$classpath:$temporary/classes" org.novarocks.fixture.auth.ProviderContractTest
  java -cp "$output/*:$artifacts/iceberg-aws-bundle-1.11.0.jar:$temporary/classes" \
    org.novarocks.fixture.auth.V2OnlyContractTest
fi
