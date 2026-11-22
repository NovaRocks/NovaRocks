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

-- Retired declarations fail before a cast, calculation, or table write.

-- query 1
-- @expect_trino_error=TYPE_NOT_FOUND,USER_ERROR
CREATE TABLE ${case_db}.retired_n (id INTEGER, d DECIMAL(38,15), n LARGEINT) WITH (format_version = 3);

-- query 2
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(DECIMAL '1.25' AS DECIMAL(38,15)) + CAST('170141183460469231731687303715884105727' AS LARGEINT);

-- query 3
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(DECIMAL '1.25' AS DECIMAL(38,15)) - CAST('-170141183460469231731687303715884105728' AS LARGEINT);

-- query 4
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(1 AS DECIMAL(38,15)) * CAST(1 AS LARGEINT);

-- query 5
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(1 AS DECIMAL(38,15)) / CAST(1 AS LARGEINT);

-- query 6
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(1 AS DECIMAL(38,15)) % CAST(1 AS LARGEINT);
