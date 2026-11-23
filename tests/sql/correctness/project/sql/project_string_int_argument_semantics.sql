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

-- @order_sensitive=true
-- @tags=project,string,int_args
-- SQL integer arguments coerce to the BIGINT function profiles.
-- query 1
SELECT split_part('a,b,c', ',', 3) AS third_field,
       split_part('abc', '', 1) AS empty_separator_first,
       split_part('abc', '', 4) AS empty_separator_missing,
       substr('abc', 1, 2) AS first_two,
       substr('abc', -2, 2) AS last_two,
       substr('abc', 1, 0) AS empty_substring,
       lpad('ab', 1, 'x') AS lpad_shrink,
       rpad('ab', 1, 'x') AS rpad_shrink,
       split('a,b,', ',') AS trailing_empty_field;

-- query 2
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT split_part('a,b,c', ',', -1);

-- query 3
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT split_part('a,b,c', ',', 0);

-- query 4
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT lpad('ab', 1, '');

-- query 5
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT lpad('ab', -1, 'x');

-- query 6
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT rpad('ab', 1, '');

-- query 7
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT rpad('ab', -1, 'x');

-- query 8
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT split('abc', '');

-- query 9
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT split('a,b', ',', 0);
