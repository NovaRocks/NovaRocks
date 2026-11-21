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
-- @tags=project,cast,time
-- TIME defaults to millisecond precision. A parsed timestamp can project its
-- clock at an explicit precision. TRY_CAST captures invalid time text.

-- query 1
SELECT
    CAST('00:00:00' AS TIME) AS t0,
    TRY_CAST('1970-01-01 01:01:01' AS TIME) AS t_dt_literal,
    CAST(TIMESTAMP '1970-01-01 01:01:01.000000' AS TIME(6)) AS t_from_datetime;

-- query 2
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(DATE '2020-01-02' AS TIME);

-- query 3
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('1970-01-01 01:01:01' AS TIME);

-- query 4
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('24:00:00' AS TIME);
