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
SELECT approx_most_frequent(3, x, 100001) AS counts FROM (VALUES BIGINT '7', BIGINT '7', BIGINT '8') t(x);

-- query 2
SELECT approx_most_frequent(5, x, 1) AS counts FROM (VALUES BIGINT '7', BIGINT '7', BIGINT '7') t(x);

-- query 3
SELECT approx_most_frequent(2, x, 10) AS counts FROM (VALUES 'x', 'x', 'z') t(x);

-- query 4
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_most_frequent(1, x, 10) AS counts FROM (VALUES BIGINT '7') t(x);

-- query 5
-- Reject the invalid histogram capacity at argument admission.
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_most_frequent(2, x, 2147483648) AS counts FROM (VALUES BIGINT '7') t(x);
