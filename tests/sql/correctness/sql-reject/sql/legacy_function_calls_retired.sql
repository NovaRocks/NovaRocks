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

-- Retired calls are rejected by public function resolution.
-- query 1
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT md5sum('x');

-- query 2
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT multi_distinct_count(1);

-- query 3
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT multi_distinct_sum(1) OVER ();

-- query 4
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT session_number(1, 2) OVER (ORDER BY 1);

-- query 5
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT grouping_id(n) FROM (VALUES (1)) AS t(n) GROUP BY ROLLUP(n);

-- query 6
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT ds_hll_accumulate(1);

-- query 7
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT __iceberg_transform_bucket(1, 8);

-- query 8
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT map_apply((k,v) -> ROW(k,v), MAP(ARRAY[1], ARRAY[2]));

-- query 9
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_sum(ARRAY[1]);

-- query 10
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT * FROM TABLE(generate_series(1, 2));

-- query 11
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_sortby(ARRAY[1,2], ARRAY[2,1]);

-- query 12
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT named_struct('value', 1);

-- query 13
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_length(ARRAY[1]);

-- query 14
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_repeat(1, 3);

-- query 15
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_map(x -> x, ARRAY[1]);

-- query 16
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_filter(x -> TRUE, ARRAY[1]);

-- query 17
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_generate(1, 2);

-- query 18
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT map_from_arrays(ARRAY[1], ARRAY[2]);

-- query 19
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT assert_true(TRUE);

-- query 20
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT assert_true(FALSE, 'internal check');
