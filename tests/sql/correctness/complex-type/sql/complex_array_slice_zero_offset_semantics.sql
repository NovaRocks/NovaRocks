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
-- Valid starts use Trino indexing. Slice length is clamped to the remaining items.

-- query 1
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT slice(ARRAY[1, 2, 3], 0, 2);

-- query 2
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT slice(ARRAY[1, 2, 3], 0);

-- query 3
SELECT slice(ARRAY[1, 2, 3], -2, 2) AS sliced_negative,
       slice(ARRAY[1, 2, 3], 1, 2) AS sliced_positive,
       slice(ARRAY[1, 2, 3], 2, BIGINT '9223372036854775807') AS clipped_length;
