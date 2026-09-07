#!/usr/bin/env bash
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

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 OUTPUT_DIR" >&2
  exit 2
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-datasketches-java.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

mkdir -p "$work_dir/src/main/java/org/apache/novarocks/tck"
cp "$script_dir/pom.xml" "$work_dir/pom.xml"
cp "$script_dir/src/GenerateFixtures.java" \
  "$work_dir/src/main/java/org/apache/novarocks/tck/GenerateFixtures.java"
cp "$script_dir/src/GenerateIcebergThetaVectors.java" \
  "$work_dir/src/main/java/org/apache/novarocks/tck/GenerateIcebergThetaVectors.java"

mvn -q -f "$work_dir/pom.xml" compile dependency:build-classpath \
  -Dmdep.outputFile="$work_dir/classpath"
java_jar=$(tr ':' '\n' < "$work_dir/classpath" | grep '/datasketches-java-6.2.0.jar$')
memory_jar=$(tr ':' '\n' < "$work_dir/classpath" | grep '/datasketches-memory-3.0.2.jar$')
iceberg_api_jar=$(tr ':' '\n' < "$work_dir/classpath" | grep '/iceberg-api-1.10.0.jar$')
test "$(shasum -a 256 "$java_jar" | cut -d ' ' -f 1)" = \
  "1b55103e1f7564150a0867eca4ce3bca13cd5935a32c199a5e738f8c5c24901a"
test "$(shasum -a 256 "$memory_jar" | cut -d ' ' -f 1)" = \
  "a3dbdec4de16bf2b0a4c9b1b253bd4064d587675fc76063f8972cdfa104c66cb"
test "$(shasum -a 256 "$iceberg_api_jar" | cut -d ' ' -f 1)" = \
  "627061d401dba9d1a8cada2da6394640c2e803102bb676fb34e5f65503cf2c51"
java -cp "$work_dir/target/classes:$(<"$work_dir/classpath")" \
  org.apache.novarocks.tck.GenerateFixtures "$1"
java -cp "$work_dir/target/classes:$(<"$work_dir/classpath")" \
  org.apache.novarocks.tck.GenerateIcebergThetaVectors "$1"
