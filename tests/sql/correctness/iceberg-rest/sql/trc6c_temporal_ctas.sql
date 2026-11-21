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
-- The REST catalog stages the exact format-v3 target declaration for CTAS.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.trc6c_ctas_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.trc6c_ctas_${uuid0}.temporal_v3
WITH (format_version = 3) AS SELECT
    TIMESTAMP '2024-01-01 00:00:00.1234567897' AS event,
    TIME '12:34:56.1234' AS clock,
    TIMESTAMP '2024-01-01 08:00:00.1234567894 Asia/Shanghai' AS zoned;

-- query 3
SELECT event, clock, zoned FROM iceberg_rest_${suite_uuid0}.trc6c_ctas_${uuid0}.temporal_v3;

-- query 4
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.trc6c_ctas_${uuid0}.temporal_v3;
DROP SCHEMA iceberg_rest_${suite_uuid0}.trc6c_ctas_${uuid0};
