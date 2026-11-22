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

-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE SCHEMA IF NOT EXISTS ${case_db};
USE ${case_db};

-- query 2
-- @kill_be_index=1
-- @restart_be_delay_ms=1500
-- @skip_result_check=true
SELECT 1;

-- query 3
-- @heartbeat_delay_ms=3000
-- @retry_count=5
-- @retry_interval_ms=2000
-- @result_contains=1000000
SELECT COUNT() FROM TABLE(sequence(1, 1000000)) AS generate_series(generate_series);
