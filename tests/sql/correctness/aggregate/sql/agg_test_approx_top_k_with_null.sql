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
-- Values and types captured from Trino 484.

-- query 1
SELECT approx_most_frequent(2, x, 10) AS counts FROM (VALUES CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) t(x);

-- query 2
SELECT approx_most_frequent(2, x, 10) AS counts FROM (VALUES CAST(NULL AS BIGINT), BIGINT '3', BIGINT '3') t(x);

-- query 3
SELECT g, approx_most_frequent(2, x, 10) AS counts FROM (VALUES (1, CAST(NULL AS VARCHAR)), (2, 'b'), (2, 'b'), (2, 'c')) t(g,x) GROUP BY g ORDER BY g;

-- query 4
SELECT approx_most_frequent(0, x, -1) AS counts FROM (VALUES CAST(NULL AS BIGINT)) t(x);

-- query 5
-- Reject the invalid histogram capacity at argument admission.
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_most_frequent(2, x, 0) AS counts FROM (VALUES BIGINT '9') t(x);
