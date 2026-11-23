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

-- query 1
SELECT
    typeof(TIMESTAMP '2023-03-07 11:22:33.1234567'),
    typeof(TIMESTAMP '2023-03-07 11:22:33.123 UTC'),
    typeof(TIME '11:22:33.1234567 +05:30');

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.temporal_typeof_input (
    local_value TIMESTAMP(6),
    zoned_value TIMESTAMP(6) WITH TIME ZONE
) WITH (format_version = 3);
INSERT INTO ${case_db}.temporal_typeof_input VALUES ROW(
    TIMESTAMP '2023-03-07 11:22:33.123456',
    TIMESTAMP '2023-03-07 11:22:33.123456 UTC'
);

-- query 3
SELECT typeof(local_value) AS local_type, typeof(zoned_value) AS zoned_type
FROM ${case_db}.temporal_typeof_input;
