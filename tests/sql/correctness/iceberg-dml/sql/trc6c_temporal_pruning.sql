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
-- Compare the same exact SQL predicates with the connector offer enabled and disabled.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc6c_epoch (id BIGINT, event TIMESTAMP(6))
WITH (format_version = 2, partitioning = ARRAY['event']);
INSERT INTO ${case_db}.trc6c_epoch VALUES ROW(1, TIMESTAMP '1969-12-31 23:59:59.999999');
INSERT INTO ${case_db}.trc6c_epoch VALUES ROW(2, TIMESTAMP '1970-01-01 00:00:00.000000');
INSERT INTO ${case_db}.trc6c_epoch VALUES ROW(3, TIMESTAMP '1970-01-01 00:00:00.000001');
SET SESSION enable_connector_static_predicate_pushdown = true;

-- query 2
SELECT id FROM ${case_db}.trc6c_epoch
WHERE event < TIMESTAMP '1970-01-01 00:00:00.0000004'
  AND event > TIMESTAMP '1969-12-31 23:59:59.999998999' ORDER BY id;

-- query 3
SELECT id FROM ${case_db}.trc6c_epoch
WHERE event < TIMESTAMP '1969-12-31 23:59:59.9999996' ORDER BY id;

-- query 4
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN
SELECT id FROM ${case_db}.trc6c_epoch
WHERE event < TIMESTAMP '1969-12-31 23:59:59.9999989' ORDER BY id;

-- query 5
SET SESSION enable_connector_static_predicate_pushdown = false;

-- query 6
SELECT id FROM ${case_db}.trc6c_epoch
WHERE event < TIMESTAMP '1970-01-01 00:00:00.0000004'
  AND event > TIMESTAMP '1969-12-31 23:59:59.999998999' ORDER BY id;

-- query 7
SELECT id FROM ${case_db}.trc6c_epoch
WHERE event < TIMESTAMP '1969-12-31 23:59:59.9999996' ORDER BY id;

-- query 8
-- @be_log_contains=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN
SELECT id FROM ${case_db}.trc6c_epoch
WHERE event < TIMESTAMP '1969-12-31 23:59:59.9999989' ORDER BY id;

-- query 9
-- @skip_result_check=true
SET SESSION enable_connector_static_predicate_pushdown = true;
DROP TABLE ${case_db}.trc6c_epoch;
