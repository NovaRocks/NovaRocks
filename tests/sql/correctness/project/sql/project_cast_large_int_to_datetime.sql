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

-- Migrated from dev/test/sql/test_cast/T/test_cast_to_datetime
-- Numeric values have no cast to TIMESTAMP, including through TRY_CAST.
-- The DECIMAL literals preserve the original magnitudes beyond signed BIGINT.

-- query 1
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(DECIMAL '-18446744073709551494' AS timestamp(6));

-- query 2
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(0 AS timestamp(6));

-- query 3
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT TRY_CAST(1 AS timestamp(6));

-- query 4
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(DECIMAL '18446744073709551615' AS timestamp(6));
