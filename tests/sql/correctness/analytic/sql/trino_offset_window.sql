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

-- query 1
SELECT x, lag(x, n, BIGINT '99') OVER (ORDER BY x) AS prior,
       lead(x, n, BIGINT '99') OVER (ORDER BY x) AS next_value
FROM (VALUES (10, 0), (20, 1), (30, 2)) t(x,n)
ORDER BY x;

-- query 2
-- @expect_trino_error=MISSING_ORDER_BY,USER_ERROR
SELECT lag(x) OVER () FROM (VALUES 1) t(x);

-- query 3
-- @expect_trino_error=INVALID_WINDOW_FRAME,USER_ERROR
SELECT lead(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (VALUES 1) t(x);

-- query 4
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT lag(x, CAST(NULL AS BIGINT)) OVER (ORDER BY x) FROM (VALUES 1, 2) t(x);

-- query 5
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT lead(x, BIGINT '-1') OVER (ORDER BY x) FROM (VALUES 1, 2) t(x);
