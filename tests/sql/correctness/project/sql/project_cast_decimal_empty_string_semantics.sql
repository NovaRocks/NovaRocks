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

-- Missing decimal text raises a conversion error and TRY_CAST returns NULL.

-- query 1
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('' AS DECIMAL(10,2));

-- query 2
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(' ' AS DECIMAL(10,2));

-- query 3
SELECT TRY_CAST('' AS DECIMAL(10,2)) AS empty_dec, TRY_CAST(' ' AS DECIMAL(10,2)) AS blank_dec, CAST('0' AS DECIMAL(10,2)) AS zero_dec, CAST(NULL AS DECIMAL(10,2)) AS null_dec;
