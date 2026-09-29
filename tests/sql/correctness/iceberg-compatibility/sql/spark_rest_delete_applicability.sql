-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- @sequential=true

-- query 1
-- @result_contains=UEA4G_FIXTURE_OK
shell: set -eu
receipt_dir="$(mktemp -d "${TMPDIR:-/tmp}/novarocks-delete-applicability-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-delete-applicability/run.sh" --mode positive --namespace nr_compat_${suite_uuid0} --prefix g02_${uuid0} --output "$receipt_dir"

-- query 2
SELECT id, p, value
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.g02_${uuid0}_same_commit_position
ORDER BY id, value;

-- query 3
SELECT id, p, value
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.g02_${uuid0}_same_commit_equality
ORDER BY id, value;

-- query 4
SELECT id, p, value
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.g02_${uuid0}_same_commit_dv
ORDER BY id, value;

-- query 5
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.g02_${uuid0}_same_commit_position FORCE;

-- query 6
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.g02_${uuid0}_same_commit_equality FORCE;

-- query 7
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.g02_${uuid0}_same_commit_dv FORCE;
