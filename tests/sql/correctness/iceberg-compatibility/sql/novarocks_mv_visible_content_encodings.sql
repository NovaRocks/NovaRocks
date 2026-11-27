-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,visible_bag,parquet_dictionary,array,map,struct
-- Official SDK files prove physical Parquet dictionary/plain label pages.
-- They do not claim an Arrow DictionaryArray reached the matcher.
-- Source and target content bags are independently checked in Spark, including
-- nullable/empty recursive values and array order; native refresh owns apply.

-- query 1
-- @skip_result_check=true
-- @result_contains=VISIBLE_CONTENT_ENCODINGS_READY
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
"$workspace/tests/sql/fixtures/mv-visible-content-encodings/run.sh" \
  visible initialize "${case_db}" "$workspace/logs/sql-mv-content-interop/${uuid0}"

-- query 2
-- @skip_result_check=true
USE iceberg_compat_${suite_uuid0}.${case_db};
SET SESSION enable_materialized_view_rewrite = false;
CREATE MATERIALIZED VIEW encoded_mv WITH (refresh_policy = 'MANUAL') AS
SELECT label, items, attrs, rec FROM encoded_source;
REFRESH MATERIALIZED VIEW encoded_mv;

-- query 3
-- @skip_result_check=true
-- @result_contains=VISIBLE_CONTENT_ENCODINGS_OBSERVED
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
"$workspace/tests/sql/fixtures/mv-visible-content-encodings/run.sh" \
  visible initial "${case_db}" "$workspace/logs/sql-mv-content-interop/${uuid0}"

-- query 4
-- @skip_result_check=true
-- @result_contains=VISIBLE_CONTENT_ENCODINGS_CHANGED
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
"$workspace/tests/sql/fixtures/mv-visible-content-encodings/run.sh" \
  visible mutate "${case_db}" "$workspace/logs/sql-mv-content-interop/${uuid0}"

-- query 5
-- @skip_result_check=true
-- @result_contains=MV refresh: ContentLocate
EXPLAIN REFRESH MATERIALIZED VIEW encoded_mv;
REFRESH MATERIALIZED VIEW encoded_mv;

-- query 6
-- @skip_result_check=true
-- @result_contains=VISIBLE_CONTENT_ENCODINGS_OBSERVED
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
"$workspace/tests/sql/fixtures/mv-visible-content-encodings/run.sh" \
  visible incremental "${case_db}" "$workspace/logs/sql-mv-content-interop/${uuid0}"

-- query 7
-- @skip_result_check=true
SET SESSION "iceberg_compat_${suite_uuid0}".incremental_refresh_enabled = false;
REFRESH MATERIALIZED VIEW encoded_mv;
RESET SESSION "iceberg_compat_${suite_uuid0}".incremental_refresh_enabled;

-- query 8
-- @skip_result_check=true
-- @result_contains=VISIBLE_CONTENT_ENCODINGS_OBSERVED
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
"$workspace/tests/sql/fixtures/mv-visible-content-encodings/run.sh" \
  visible full "${case_db}" "$workspace/logs/sql-mv-content-interop/${uuid0}"

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.encoded_mv;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.encoded_source;
