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
-- @tags=complex,array

-- query 1
SELECT array_sort(ARRAY[3, 1, 2, NULL]) AS sorted_arr,
       array_join(ARRAY['a', 'b', NULL], ',') AS joined_arr;

-- query 2
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT array_join(ARRAY[JSON '[1]'], ',');

-- query 3
SELECT TRY(array_join(ARRAY[JSON '[1]'], ',')) AS element_error,
       array_join(ARRAY[JSON 'null', JSON '"one"'], ',', 'missing') AS replaced_null;

-- query 4
-- @expect_trino_error=OPERATOR_NOT_FOUND,USER_ERROR
SELECT array_join(ARRAY[ARRAY[1]], ',');
