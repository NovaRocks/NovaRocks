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
-- @tags=session,trc5c,session_registry
-- Required typed session values, bounded evaluation and retired syntax.
-- Expected rules come from fixed Trino 484 source and approved NovaRocks differences.

-- query 1
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION trc_unknown_property = 1;

-- query 2
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET trc_unknown_property = 1;

-- query 3
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION enable_global_runtime_filter = 'true';

-- query 4
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION enable_global_runtime_filter = 1;

-- query 5
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = '4';

-- query 6
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = 8.5;

-- query 7
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = 2147483648;

-- query 8
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = 2e0;

-- query 9
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = NULL;

-- query 10
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = (SELECT 1);

-- query 11
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = DEFAULT;

-- query 12
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION enable_global_runtime_filter = off;

-- query 13
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION runtime_filter_scan_wait_time = -1;

-- query 14
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION autocommit = true;

-- query 15
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION catalog = 'x';

-- query 16
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION time_zone = 'UTC';

-- query 17
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET autocommit = 1;

-- query 18
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET PASSWORD = 'x';

-- query 19
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET ROLE = admin;

-- query 20
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION recursive_cte_max_depth = 0;

-- query 21
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION query_mem_limit = 0;

-- query 22
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION query_mem_limit = -1;

-- query 23
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET SESSION enable_global_runtime_filter = on;

-- query 24
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SET GLOBAL pipeline_dop = 1;

-- query 25
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SET GLOBAL trc_unknown_property = 1;

-- query 26
SET SESSION pipeline_dop = 1 + 2;

-- query 27
SET SESSION pipeline_dop = CAST(4 AS tinyint);

-- query 28
SET SESSION global_runtime_filter_probe_min_selectivity = 1;

-- query 29
SET SESSION global_runtime_filter_probe_min_selectivity = CAST(1 AS bigint);

-- query 30
SET SESSION enable_global_runtime_filter = CAST('true' AS boolean);

-- query 31
SET SESSION enable_global_runtime_filter = NOT false;

-- query 32
SET SESSION sql_mode = concat('3', '2');

-- query 33
SET SESSION query_mem_limit = 1073741824;

-- query 34
SET SESSION recursive_cte_max_depth = 10;

-- query 35
-- Disable optional folding; every following SET still requires the original compiled constant evaluator.
SET SESSION disable_optimizer_rules = 'FoldConstant';

-- query 36
SET SESSION pipeline_dop = 1 + 2;

-- query 37
SET SESSION sql_mode = concat('3', '2');

-- query 38
SET SESSION pipeline_dop = CASE WHEN true THEN 3 ELSE 1 / 0 END;

-- query 39
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = CASE WHEN false THEN 3 ELSE 1 / 0 END;

-- query 40
SET SESSION enable_global_runtime_filter = false AND (1 / 0 = 1);

-- query 41
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION enable_global_runtime_filter = true AND (1 / 0 = 1);

-- query 42
SET SESSION enable_global_runtime_filter = true OR (1 / 0 = 1);

-- query 43
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION enable_global_runtime_filter = false OR (1 / 0 = 1);

-- query 44
SET SESSION pipeline_dop = COALESCE(3, 1 / 0);

-- query 45
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION pipeline_dop = COALESCE(CAST(NULL AS integer), 1 / 0);

-- query 46
SET SESSION disable_optimizer_rules = '';

-- query 47
SET SESSION recursive_cte_max_depth = 2;

-- query 48
-- NovaRocks preserves its existing depth cap: the result is 1..2, not an over-depth error.
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 10
)
SELECT n FROM numbers ORDER BY n;

-- query 49
-- The existing root hint overrides the admitted session depth without changing it.
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 10
)
SELECT /*+ SET_VAR(recursive_cte_max_depth = 4) */ n FROM numbers ORDER BY n;

-- query 50
SET SESSION recursive_cte_max_depth = 5;

-- query 51
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 10
)
SELECT n FROM numbers ORDER BY n;

-- query 52
SELECT /*+ SET_VAR(query_mem_limit = 1073741824) */ 1;
