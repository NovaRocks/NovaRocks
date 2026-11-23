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

-- Migrated from dev/test/sql/test_function/T/test_named_argments
-- Test Objective:
-- 1. Validate named arguments for table function sequence (START, STOP, STEP).
-- 2. Validate argument reordering with named arguments.
-- 3. Validate error handling for invalid named arguments (unknown param names, syntax errors, missing required params, duplicates).
-- 4. Validate positional arguments still work correctly.

-- query 1
select * from TABLE(sequence(start=>2, stop=>5));

-- query 2
select * from TABLE(sequence(stop=>5, start=>2));

-- query 3
select * from TABLE(sequence(start=>2, stop=>5, step=>2));

-- query 4
select * from TABLE(sequence(step=>2, start=>2, stop=>5));

-- query 5
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
select * from TABLE(sequence(start=>2, finish=>5));

-- query 6
select * from TABLE(sequence(stop=>5,start=>2, step => 2));

-- query 7
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
select * from TABLE(sequence(start=>2, =>5));

-- query 8
-- @expect_trino_error=INVALID_ARGUMENTS,USER_ERROR
select * from TABLE(sequence(start=>2, 5));

-- query 9
-- @expect_trino_error=MISSING_ARGUMENT,USER_ERROR
select * from TABLE(sequence(start=>2));

-- query 10
select * from TABLE(sequence(stop=>2));

-- query 11
-- @expect_trino_error=MISSING_ARGUMENT,USER_ERROR
select * from TABLE(sequence(step=>2));

-- query 12
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
select * from TABLE(sequence(start=>2, stop=>null));

-- query 13
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
select * from TABLE(sequence(start=>2, "STOP"=>null));

-- query 14
-- @expect_trino_error=INVALID_ARGUMENTS,USER_ERROR
select * from TABLE(sequence(start=>2, stop=>4,step->3));

-- query 15
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
select * from TABLE(sequence(start=>2, step=>2, stop=>4,start=>3));

-- query 16
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
select * from TABLE(sequence(start=>2, step=>2, stop=>4,stop=>6));

-- query 17
select * from TABLE(sequence(1, 5));

-- query 18
select * from TABLE(sequence(1, 5, 2));

-- query 19
-- @expect_trino_error=MISSING_ARGUMENT,USER_ERROR
select * from TABLE(sequence(1));
