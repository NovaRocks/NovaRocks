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
-- @tags=d04,project,date,interval,null_semantics
-- Conversion failures caught by TRY_CAST supply a typed NULL to calendar
-- arithmetic. Numeric and character operands have no DATE shift overload.

-- query 1
SELECT
    TRY_CAST('abcd' AS DATE) + INTERVAL '1' DAY AS invalid_day_source,
    TRY_CAST('abcd' AS DATE) + INTERVAL '1' MONTH AS invalid_month_source,
    DATE '2024-01-01' + CAST(NULL AS INTERVAL DAY) AS null_delta,
    DATE '2024-01-31' + INTERVAL '1' MONTH AS next_month,
    DATE '2024-01-01' + INTERVAL '1' DAY AS next_day,
    DATE '2024-01-01' - INTERVAL '1' DAY AS previous_day;

-- query 2
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT DATE '2024-01-01' + 'x';

-- query 3
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT DATE '2024-01-01' + 1;

-- query 4
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT DATE '2024-01-01' - 1;
