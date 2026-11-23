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

-- query 1
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT group_concat('a');

-- query 2
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT string_agg('a', '|');

-- query 3
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_agg_distinct(1);

-- query 4
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT max_n(1, 2);

-- query 5
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT approx_top_k(1);

-- query 6
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION group_concat_max_len = 10000;
