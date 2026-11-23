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
-- @tags=filter,comparison,child_type
-- Numeric comparisons use their common Trino input type.

-- query 1
SELECT abs(1 - 2) = 0 AS abs_eq_zero,
       CAST(1.25 AS DOUBLE) > 1 AS dbl_gt_int,
       CAST(1 AS DECIMAL(10,2)) = 1 AS dec_eq_int;

-- query 2
SELECT CAST(TRUE AS INTEGER) = 1 AS bool_eq_one,
       CAST(FALSE AS INTEGER) < 1 AS bool_lt_one;

-- query 3
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT TRUE = 1;

-- query 4
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT FALSE < 1;
