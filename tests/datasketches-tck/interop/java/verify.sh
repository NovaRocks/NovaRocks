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

repo_root=$(cd "$(dirname "$0")/../../../.." && pwd)
source_root="$repo_root/tests/datasketches-tck/interop/java"
fixture_root="$repo_root/tests/datasketches-tck/fixtures"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-java-theta-interop.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

mkdir -p "$work_dir/src/main/java/org/apache/novarocks/tck"
cp "$source_root/pom.xml" "$work_dir/pom.xml"
cp "$source_root/src/VerifyBidirectionalTheta.java" \
  "$work_dir/src/main/java/org/apache/novarocks/tck/VerifyBidirectionalTheta.java"

mvn -q -f "$work_dir/pom.xml" compile dependency:build-classpath \
  -Dmdep.outputFile="$work_dir/classpath"
java_jar=$(tr ':' '\n' < "$work_dir/classpath" | grep '/datasketches-java-6.2.0.jar$')
memory_jar=$(tr ':' '\n' < "$work_dir/classpath" | grep '/datasketches-memory-3.0.2.jar$')
test "$(shasum -a 256 "$java_jar" | cut -d ' ' -f 1)" = \
  "1b55103e1f7564150a0867eca4ce3bca13cd5935a32c199a5e738f8c5c24901a"
test "$(shasum -a 256 "$memory_jar" | cut -d ' ' -f 1)" = \
  "a3dbdec4de16bf2b0a4c9b1b253bd4064d587675fc76063f8972cdfa104c66cb"

java_output="$work_dir/java_disjoint_n1000.sk"
java -cp "$work_dir/target/classes:$(<"$work_dir/classpath")" \
  org.apache.novarocks.tck.VerifyBidirectionalTheta "$fixture_root" "$java_output"

cargo run --quiet --locked -p novarocks-datasketches-tck \
  --bin verify_theta_interop -- \
  "$java_output" 1000 \
  "$fixture_root/theta/rust_quickselect_n1000_ordered_v3.sk" 2000
