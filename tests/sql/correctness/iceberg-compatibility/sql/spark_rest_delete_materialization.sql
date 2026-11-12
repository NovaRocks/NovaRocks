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
root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/rwm-6/delete-materialization-${uuid0}"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-delete-applicability/run.sh" --mode mutation-seed --namespace ${case_db} --prefix rwm6_${uuid0} --output "$root/seed"

-- query 2
SELECT id, p, value
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm6_${uuid0}_materialization_history
ORDER BY id;

-- query 3
-- @skip_result_check=true
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm6_${uuid0}_materialization_history WHERE id IN (2,9);

-- query 4
-- @result_contains=UEA4G_FIXTURE_OK
shell: set -eu
root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/rwm-6/delete-materialization-${uuid0}"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-delete-applicability/run.sh" --mode mutation-check --namespace ${case_db} --prefix rwm6_${uuid0} --output "$root/check" --mutation-seed-receipt "$root/seed/receipt.json"

-- query 5
SELECT id, p, value
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm6_${uuid0}_materialization_history
ORDER BY id;

-- query 6
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm6_${uuid0}_materialization_history FORCE;
